/* Asynchronous replication implementation.
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */


#include "server.h"
#include "cluster.h"
#include "bio.h"

#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>

void replicationDiscardCachedMaster(void);
void replicationResurrectCachedMaster(connection *conn);
void replicationSendAck(void);
void putSlaveOnline(client *slave);
int cancelReplicationHandshake(void);

/* We take a global flag to remember if this instance generated an RDB
 * because of replication, so that we can remove the RDB file in case
 * the instance is configured to have no persistence. */
int RDBGeneratedByReplication = 0;

/* --------------------------- Utility functions ---------------------------- */

/* Return the pointer to a string representing the slave ip:listening_port
 * pair. Mostly useful for logging, since we want to log a slave using its
 * IP address and its listening port which is more clear for the user, for
 * example: "Closing connection with replica 10.1.2.3:6380".
 * 获取某个client在集群中的id
 * */
char *replicationGetSlaveName(client *c) {
    static char buf[NET_PEER_ID_LEN];
    char ip[NET_IP_STR_LEN];

    ip[0] = '\0';
    buf[0] = '\0';

    if (c->slave_ip[0] != '\0' ||
        connPeerToString(c->conn,ip,sizeof(ip),NULL) != -1)
    {
        /* Note that the 'ip' buffer is always larger than 'c->slave_ip'
         * */
        if (c->slave_ip[0] != '\0') memcpy(ip,c->slave_ip,sizeof(c->slave_ip));

        if (c->slave_listening_port)
            anetFormatAddr(buf,sizeof(buf),ip,c->slave_listening_port);
        else
            snprintf(buf,sizeof(buf),"%s:<unknown-replica-port>",ip);
    } else {
        // 将client->id 设置到buf中
        snprintf(buf,sizeof(buf),"client id #%llu",
            (unsigned long long) c->id);
    }
    return buf;
}

/* Plain unlink() can block for quite some time in order to actually apply
 * the file deletion to the filesystem. This call removes the file in a
 * background thread instead. We actually just do close() in the thread,
 * by using the fact that if there is another instance of the same file open,
 * the foreground unlink() will only remove the fs name, and deleting the
 * file's storage space will only happen once the last reference is lost.
 * 在后台进程中删除某个文件
 * */
int bg_unlink(const char *filename) {
    int fd = open(filename,O_RDONLY|O_NONBLOCK);
    if (fd == -1) {
        /* Can't open the file? Fall back to unlinking in the main thread. */
        // 减少文件引用计数 当计数值为0时自动删除文件
        return unlink(filename);
    } else {
        /* The following unlink() removes the name but doesn't free the
         * file contents because a process still has it open. */
        int retval = unlink(filename);
        // 代表本次减少引用计数失败了
        if (retval == -1) {
            /* If we got an unlink error, we just return it, closing the
             * new reference we have to the file. */
            int old_errno = errno;
            close(fd);  /* This would overwrite our errno. So we saved it. */
            errno = old_errno;
            return -1;
        }
        // 通过后台线程执行关闭文件的任务
        bioCreateBackgroundJob(BIO_CLOSE_FILE,(void*)(long)fd,NULL,NULL);
        return 0; /* Success. */
    }
}

/* ---------------------------------- MASTER -------------------------------- */

/*
 * 这里是初始化backlog的一些属性
 * backlog在这里是积压的意思   master会存储最近执行的一些command 便于slave节点的数据同步
 */
void createReplicationBacklog(void) {
    serverAssert(server.repl_backlog == NULL);
    // 按照最大存储的积压数据大小来申请缓冲区
    server.repl_backlog = zmalloc(server.repl_backlog_size);
    server.repl_backlog_histlen = 0;
    server.repl_backlog_idx = 0;

    /* We don't have any data inside our buffer, but virtually the first
     * byte we have is the next byte that will be generated for the
     * replication stream.
     * */
    server.repl_backlog_off = server.master_repl_offset+1;
}

/* This function is called when the user modifies the replication backlog
 * size at runtime. It is up to the function to both update the
 * server.repl_backlog_size and to resize the buffer and setup it so that
 * it contains the same data as the previous one (possibly less data, but
 * the most recent bytes, or the same data and more free space in case the
 * buffer is enlarged).
 * 重置backlog_size的大小
 * */
void resizeReplicationBacklog(long long newsize) {
    // 积压数据有一个最小的size 这里不能小于该值
    if (newsize < CONFIG_REPL_BACKLOG_MIN_SIZE)
        newsize = CONFIG_REPL_BACKLOG_MIN_SIZE;
    if (server.repl_backlog_size == newsize) return;

    server.repl_backlog_size = newsize;
    if (server.repl_backlog != NULL) {
        /* What we actually do is to flush the old buffer and realloc a new
         * empty one. It will refill with new data incrementally.
         * The reason is that copying a few gigabytes adds latency and even
         * worse often we need to alloc additional space before freeing the
         * old buffer.
         * 释放旧数据
         * */
        zfree(server.repl_backlog);
        server.repl_backlog = zmalloc(server.repl_backlog_size);
        server.repl_backlog_histlen = 0;
        server.repl_backlog_idx = 0;
        /* Next byte we have is... the next since the buffer is empty. */
        server.repl_backlog_off = server.master_repl_offset+1;
    }
}

/*
 * 清除积压数据
 */
void freeReplicationBacklog(void) {
    serverAssert(listLength(server.slaves) == 0);
    zfree(server.repl_backlog);
    server.repl_backlog = NULL;
}

/* Add data to the replication backlog.
 * This function also increments the global replication offset stored at
 * server.master_repl_offset, because there is no case where we want to feed
 * the backlog without incrementing the offset.
 * 将数据填充到backlog中
 * 当master需要将数据同步到其他slave时 会将这部分数据额外保留一份 这样当slave需要同步数据时 可以从这里快速获取
 * 并且基于此 repl_backlog 采用轮式算法节省内存 并且数据被覆盖也没有关系
 * @param ptr 数据指针
 * @param len 数据长度
 * */
void feedReplicationBacklog(void *ptr, size_t len) {
    unsigned char *p = ptr;

    // 更新本节点关于集群数据同步的偏移量
    server.master_repl_offset += len;

    /* This is a circular buffer, so write as much data we can at every
     * iteration and rewind the "idx" index if we reach the limit.
     * 通过轮式算法填充数据  backlog 数组实际上是一个轮式结构
     * */
    while(len) {
        // idx是轮结构的下标 这里是获取剩余空间
        size_t thislen = server.repl_backlog_size - server.repl_backlog_idx;

        // 本次会使用到的空间
        if (thislen > len) thislen = len;
        // 这里会先填满剩余空间 此时ptr可能还有剩余数据
        memcpy(server.repl_backlog+server.repl_backlog_idx,p,thislen);
        // 移动指针
        server.repl_backlog_idx += thislen;
        // 将idx重置 此时会覆盖旧数据
        if (server.repl_backlog_idx == server.repl_backlog_size)
            server.repl_backlog_idx = 0;
        len -= thislen;
        // 移动p指针 避免复制重复的数据
        p += thislen;
        // 这个应该是记录一个总长度
        server.repl_backlog_histlen += thislen;
    }
    // 总长度不能超过 backlog_size
    if (server.repl_backlog_histlen > server.repl_backlog_size)
        server.repl_backlog_histlen = server.repl_backlog_size;
    /* Set the offset of the first byte we have in the backlog.
     * 代表此时的数据同步需要从repl_backlog_off的位置开始
     * */
    server.repl_backlog_off = server.master_repl_offset -
                              server.repl_backlog_histlen + 1;
}

/* Wrapper for feedReplicationBacklog() that takes Redis string objects
 * as input.
 * 从master处接收到的是 redisObject 这里将数据处理后转换成 redisString 并存入到backlog中
 * */
void feedReplicationBacklogWithObject(robj *o) {
    char llstr[LONG_STR_SIZE];
    void *p;
    size_t len;

    if (o->encoding == OBJ_ENCODING_INT) {
        len = ll2string(llstr,sizeof(llstr),(long)o->ptr);
        p = llstr;
    } else {
        len = sdslen(o->ptr);
        p = o->ptr;
    }
    feedReplicationBacklog(p,len);
}

/* Propagate write commands to slaves, and populate the replication backlog
 * as well. This function is used if the instance is a master: we use
 * the commands received by our clients in order to create the replication
 * stream. Instead if the instance is a slave and has sub-slaves attached,
 * we use replicationFeedSlavesFromMasterStream()
 * 将本次操作同步到所有从节点
 * @param dictid 因为每个command都是以db为单位进行同步的 这里是当前要同步的db
 * */
void replicationFeedSlaves(list *slaves, int dictid, robj **argv, int argc) {
    listNode *ln;
    listIter li;
    int j, len;
    char llstr[LONG_STR_SIZE];

    /* If the instance is not a top level master, return ASAP: we'll just proxy
     * the stream of data we receive from our master instead, in order to
     * propagate *identical* replication stream. In this way this slave can
     * advertise the same replication ID as the master (since it shares the
     * master replication history and has the same backlog and offsets).
     * 非master节点 无法发送同步请求
     * */
    if (server.masterhost != NULL) return;

    /* If there aren't slaves, and there is no backlog buffer to populate,
     * we can return ASAP.
     * 此时还没有副本 所以不需要发送同步请求 也不需要填充数据到backlog
     * */
    if (server.repl_backlog == NULL && listLength(slaves) == 0) return;

    /* We can't have slaves attached and no backlog. */
    serverAssert(!(listLength(slaves) != 0 && server.repl_backlog == NULL));

    /* Send SELECT command to every slave if needed.
     * 代表本次要写入的db 与最近一次数据同步对应的db不同 需要额外执行一条selectCommand
     * */
    if (server.slaveseldb != dictid) {
        robj *selectcmd;

        /* For a few DBs we have pre-computed SELECT command. */
        if (dictid >= 0 && dictid < PROTO_SHARED_SELECT_CMDS) {
            selectcmd = shared.select[dictid];
        } else {
            int dictid_len;

            dictid_len = ll2string(llstr,sizeof(llstr),dictid);
            selectcmd = createObject(OBJ_STRING,
                sdscatprintf(sdsempty(),
                "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
                dictid_len, llstr));
        }

        /* Add the SELECT command into the backlog. */
        if (server.repl_backlog) feedReplicationBacklogWithObject(selectcmd);

        /* Send it to slaves. */
        listRewind(slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) continue;
            addReply(slave,selectcmd);
        }

        if (dictid < 0 || dictid >= PROTO_SHARED_SELECT_CMDS)
            decrRefCount(selectcmd);
    }
    // 更改server最近一次命令相关的db
    server.slaveseldb = dictid;

    /* Write the command to the replication backlog if any.
     * 代表此时有副本 需要额外存储一份数据到backlog中 这个数据不仅可以做部分数据同步 也可以作为在slave同步完rdb数据后记录新追加数据的容器
     * 因为是先发送rdb数据 再发送backlog数据 串行发送 所以不用担心数据会乱
     * */
    if (server.repl_backlog) {
        // 存储临时数据的容器
        char aux[LONG_STR_SIZE+3];

        /* Add the multi bulk reply length. */
        aux[0] = '*';
        // * 代表是一个bulk协议  然后写入bulk下每个子command数量
        len = ll2string(aux+1,sizeof(aux)-1,argc);
        aux[len+1] = '\r';
        aux[len+2] = '\n';

        // 先往backlog中写入一个前缀信息
        feedReplicationBacklog(aux,len+3);

        for (j = 0; j < argc; j++) {
            long objlen = stringObjectLen(argv[j]);

            /* We need to feed the buffer with the object as a bulk reply
             * not just as a plain string, so create the $..CRLF payload len
             * and add the final CRLF */
            aux[0] = '$';
            len = ll2string(aux+1,sizeof(aux)-1,objlen);
            aux[len+1] = '\r';
            aux[len+2] = '\n';
            feedReplicationBacklog(aux,len+3);
            // 将实际的数据体写入到backlog中
            feedReplicationBacklogWithObject(argv[j]);
            // 这里是写入 \r\n
            feedReplicationBacklog(aux+len+1,2);
        }
    }

    /* Write the command to every slave. 将该command写往每个slave节点 */
    listRewind(slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        /* Don't feed slaves that are still waiting for BGSAVE to start.
         * 此时该节点加入时对应的rdb数据还未生成 当切换成end状态时就代表开始生成rdb数据了 也就是从此时开始新执行的command可以加入到缓冲区了 同样串行执行不用担心数据的混乱
         * */
        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) continue;

        /* Feed slaves that are waiting for the initial SYNC (so these commands
         * are queued in the output buffer until the initial SYNC completes),
         * or are already in sync with the master. */

        // * 这里都只是将数据写入到缓冲区 并没有发送数据
        // 这个时候slave节点的数据可能还没有同步完成 就要看注册的writeHandler是什么了 在同步完数据后会将writeHandler恢复成通用的 这时才会将缓冲区内的数据发出

        /* Add the multi bulk length. */
        addReplyArrayLen(slave,argc);

        /* Finally any additional argument that was not stored inside the
         * static buffer if any (from j to argc).
         * */
        for (j = 0; j < argc; j++)
            addReplyBulk(slave,argv[j]);
    }
}

/* This is a debugging function that gets called when we detect something
 * wrong with the replication protocol: the goal is to peek into the
 * replication backlog and show a few final bytes to make simpler to
 * guess what kind of bug it could be.
 * 获取积压的数据中的最后一部分
 * */
void showLatestBacklog(void) {

    // 如果没有积压的数据直接返回
    if (server.repl_backlog == NULL) return;

    // 默认获取最后256byte的数据 如果此时数据总长度小于该值 按需获取
    long long dumplen = 256;
    if (server.repl_backlog_histlen < dumplen)
        dumplen = server.repl_backlog_histlen;

    /* Identify the first byte to dump.
     * 因为采用的是轮式算法 所以用 %server.repl_backlog_size的方式获取起始偏移量
     * */
    long long idx =
      (server.repl_backlog_idx + (server.repl_backlog_size - dumplen)) %
       server.repl_backlog_size;

    /* Scan the circular buffer to collect 'dumplen' bytes.
     * 这里就是拷贝一份副本数据
     * */
    sds dump = sdsempty();
    while(dumplen) {
        long long thislen =
            ((server.repl_backlog_size - idx) < dumplen) ?
            (server.repl_backlog_size - idx) : dumplen;

        dump = sdscatrepr(dump,server.repl_backlog+idx,thislen);
        dumplen -= thislen;
        idx = 0;
    }

    /* Finally log such bytes: this is vital debugging info to
     * understand what happened. */
    serverLog(LL_WARNING,"Latest backlog is: '%s'", dump);
    sdsfree(dump);
}

/* This function is used in order to proxy what we receive from our master
 * to our sub-slaves.
 * */
#include <ctype.h>
void replicationFeedSlavesFromMasterStream(list *slaves, char *buf, size_t buflen) {
    listNode *ln;
    listIter li;

    /* Debugging: this is handy to see the stream sent from master
     * to slaves. Disabled with if(0). */
    if (0) {
        printf("%zu:",buflen);
        for (size_t j = 0; j < buflen; j++) {
            printf("%c", isprint(buf[j]) ? buf[j] : '.');
        }
        printf("\n");
    }

    // 如果此时服务器有数据积压 留存一份数据
    if (server.repl_backlog) feedReplicationBacklog(buf,buflen);
    listRewind(slaves,&li);
    // 将数据发往每个slave节点
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        /* Don't feed slaves that are still waiting for BGSAVE to start. */
        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) continue;
        addReplyProto(slave,buf,buflen);
    }
}

/*
 * 某些特殊的信息会被发往监控模块 比如失败信息
 * @param c
 * @param monitors
 * @param dictid
 * @param argv
 * @param argc
 */
void replicationFeedMonitors(client *c, list *monitors, int dictid, robj **argv, int argc) {
    listNode *ln;
    listIter li;
    int j;
    sds cmdrepr = sdsnew("+");
    robj *cmdobj;
    struct timeval tv;

    gettimeofday(&tv,NULL);
    cmdrepr = sdscatprintf(cmdrepr,"%ld.%06ld ",(long)tv.tv_sec,(long)tv.tv_usec);
    if (c->flags & CLIENT_LUA) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d lua] ",dictid);
    } else if (c->flags & CLIENT_UNIX_SOCKET) {
        cmdrepr = sdscatprintf(cmdrepr,"[%d unix:%s] ",dictid,server.unixsocket);
    } else {
        cmdrepr = sdscatprintf(cmdrepr,"[%d %s] ",dictid,getClientPeerId(c));
    }

    for (j = 0; j < argc; j++) {
        if (argv[j]->encoding == OBJ_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long)argv[j]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr,(char*)argv[j]->ptr,
                        sdslen(argv[j]->ptr));
        }
        if (j != argc-1)
            cmdrepr = sdscatlen(cmdrepr," ",1);
    }
    cmdrepr = sdscatlen(cmdrepr,"\r\n",2);
    cmdobj = createObject(OBJ_STRING,cmdrepr);

    listRewind(monitors,&li);
    while((ln = listNext(&li))) {
        client *monitor = ln->value;
        addReply(monitor,cmdobj);
    }
    decrRefCount(cmdobj);
}

/* Feed the slave 'c' with the replication backlog starting from the
 * specified 'offset' up to the end of the backlog.
 * 读取积压的数据  发送到某个client
 * @param offset 代表从哪个偏移量开始
 * */
long long addReplyReplicationBacklog(client *c, long long offset) {
    long long j, skip, len;

    serverLog(LL_DEBUG, "[PSYNC] Replica request offset: %lld", offset);

    if (server.repl_backlog_histlen == 0) {
        serverLog(LL_DEBUG, "[PSYNC] Backlog history len is zero");
        return 0;
    }

    serverLog(LL_DEBUG, "[PSYNC] Backlog size: %lld",
             server.repl_backlog_size);
    serverLog(LL_DEBUG, "[PSYNC] First byte: %lld",
             server.repl_backlog_off);
    serverLog(LL_DEBUG, "[PSYNC] History len: %lld",
             server.repl_backlog_histlen);
    serverLog(LL_DEBUG, "[PSYNC] Current index: %lld",
             server.repl_backlog_idx);

    /* Compute the amount of bytes we need to discard.
     * */
    skip = offset - server.repl_backlog_off;
    serverLog(LL_DEBUG, "[PSYNC] Skipping: %lld", skip);

    /* Point j to the oldest byte, that is actually our
     * server.repl_backlog_off byte.
     * 将j移动到backlog的起始位置
     * */
    j = (server.repl_backlog_idx +
        (server.repl_backlog_size-server.repl_backlog_histlen)) %
        server.repl_backlog_size;
    serverLog(LL_DEBUG, "[PSYNC] Index of first byte: %lld", j);

    /* Discard the amount of data to seek to the specified 'offset'. */
    j = (j + skip) % server.repl_backlog_size;

    /* Feed slave with data. Since it is a circular buffer we have to
     * split the reply in two parts if we are cross-boundary.
     * 将总长度 - 需要跳过的长度 就得到需要读取的长度
     * */
    len = server.repl_backlog_histlen - skip;
    serverLog(LL_DEBUG, "[PSYNC] Reply total length: %lld", len);
    while(len) {
        long long thislen =
            ((server.repl_backlog_size - j) < len) ?
            (server.repl_backlog_size - j) : len;

        serverLog(LL_DEBUG, "[PSYNC] addReply() length: %lld", thislen);
        // 将积压的数据取出并发送给指定的client
        addReplySds(c,sdsnewlen(server.repl_backlog + j, thislen));
        len -= thislen;
        j = 0;
    }
    return server.repl_backlog_histlen - skip;
}

/* Return the offset to provide as reply to the PSYNC command received
 * from the slave. The returned value is only valid immediately after
 * the BGSAVE process started and before executing any other command
 * from clients.
 * 这里会返回部分数据同步的起始偏移量
 * */
long long getPsyncInitialOffset(void) {
    return server.master_repl_offset;
}

/* Send a FULLRESYNC reply in the specific case of a full resynchronization,
 * as a side effect setup the slave for a full sync in different ways:
 *
 * 1) Remember, into the slave client structure, the replication offset
 *    we sent here, so that if new slaves will later attach to the same
 *    background RDB saving process (by duplicating this client output
 *    buffer), we can get the right offset from this slave.
 * 2) Set the replication state of the slave to WAIT_BGSAVE_END so that
 *    we start accumulating differences from this point.
 * 3) Force the replication stream to re-emit a SELECT statement so
 *    the new slave incremental differences will start selecting the
 *    right database number.
 *
 * Normally this function should be called immediately after a successful
 * BGSAVE for replication was started, or when there is one already in
 * progress that we attached our slave to.
 * 此时明确了某个slave将会采用全量数据同步的方式，offset对应部分数据同步的偏移量
 * */
int replicationSetupSlaveForFullResync(client *slave, long long offset) {
    char buf[128];
    int buflen;

    // 记录明确这个slave将采用全量数据同步时 此时master的数据偏移量 赶上同一份rdb数据的其他slave也会以该offset作为同步数据偏移量
    slave->psync_initial_offset = offset;
    // 标记准备完成
    slave->replstate = SLAVE_STATE_WAIT_BGSAVE_END;
    /* We are going to accumulate the incremental changes for this
     * slave as well. Set slaveseldb to -1 in order to force to re-emit
     * a SELECT statement in the replication stream. */
    server.slaveseldb = -1;

    /* Don't send this reply to slaves that approached us with
     * the old SYNC command.
     * 代表原本尝试使用部分数据同步 但是条件不允许被动采用全量数据同步  这里返回的字符串对应发送psync命令的地方
     * */
    if (!(slave->flags & CLIENT_PRE_PSYNC)) {
        buflen = snprintf(buf,sizeof(buf),"+FULLRESYNC %s %lld\r\n",
                          // 要将此时的副本id 和此时master的最新偏移量发送过去
                          server.replid,offset);
        if (connWrite(slave->conn,buf,buflen) != buflen) {
            freeClientAsync(slave);
            return C_ERR;
        }
    }
    return C_OK;
}

/* This function handles the PSYNC command from the point of view of a
 * master receiving a request for partial resynchronization.
 *
 * On success return C_OK, otherwise C_ERR is returned and we proceed
 * with the usual full resync.
 * 作为master接收到某个slave发出的部分数据同步请求
 * */
int masterTryPartialResynchronization(client *c) {
    long long psync_offset, psync_len;
    // 此时master对应的副本id
    char *master_replid = c->argv[1]->ptr;
    char buf[128];
    int buflen;

    /* Parse the replication offset asked by the slave. Go to full sync
     * on parse error: this should never happen but we try to handle
     * it in a robust way compared to aborting.
     * 第二个参数是同步数据的起始偏移量   在slave刚启动时还不清楚偏移量的情况下会传入-1
     * */
    if (getLongLongFromObjectOrReply(c,c->argv[2],&psync_offset,NULL) !=
       C_OK) goto need_full_resync;

    /* Is the replication ID of this master the same advertised by the wannabe
     * slave via PSYNC? If the replication ID changed this master has a
     * different replication history, and there is no way to continue.
     *
     * Note that there are two potentially valid replication IDs: the ID1
     * and the ID2. The ID2 however is only valid up to a specific offset.
     * 作为master 发现传来的副本id 与replid不同
     * 或者 与replid2不同或者该偏移量超过了second_replid_offset
     * */
    if (strcasecmp(master_replid, server.replid) &&
        (strcasecmp(master_replid, server.replid2) ||
         psync_offset > server.second_replid_offset))
    {
        // 下面的情况只是日志不同 都要同步全量数据
        /* Replid "?" is used by slaves that want to force a full resync.
         * */
        if (master_replid[0] != '?') {
            if (strcasecmp(master_replid, server.replid) &&
                strcasecmp(master_replid, server.replid2))
            {
                serverLog(LL_NOTICE,"Partial resynchronization not accepted: "
                    "Replication ID mismatch (Replica asked for '%s', my "
                    "replication IDs are '%s' and '%s')",
                    master_replid, server.replid, server.replid2);
            } else {
                serverLog(LL_NOTICE,"Partial resynchronization not accepted: "
                    "Requested offset for second ID was %lld, but I can reply "
                    "up to %lld", psync_offset, server.second_replid_offset);
            }
        } else {
            serverLog(LL_NOTICE,"Full resync requested by replica %s",
                replicationGetSlaveName(c));
        }
        goto need_full_resync;
    }

    /* We still have the data our slave is asking for?
     * backlog数据是用于部分数据同步的 如果未开启这个设置 就无法进行部分数据同步
     * 如果偏移量太小代表对应的数据太旧 或者 偏移量太大 都无法同步
     * */
    if (!server.repl_backlog ||
        psync_offset < server.repl_backlog_off ||
        psync_offset > (server.repl_backlog_off + server.repl_backlog_histlen))
    {
        serverLog(LL_NOTICE,
            "Unable to partial resync with replica %s for lack of backlog (Replica request was: %lld).", replicationGetSlaveName(c), psync_offset);
        if (psync_offset > server.master_repl_offset) {
            serverLog(LL_WARNING,
                "Warning: replica %s tried to PSYNC with an offset that is greater than the master replication offset.", replicationGetSlaveName(c));
        }
        goto need_full_resync;
    }

    /* If we reached this point, we are able to perform a partial resync:
     * 1) Set client state to make it a slave.
     * 2) Inform the client we can continue with +CONTINUE
     * 3) Send the backlog data (from the offset to the end) to the slave.
     * 因为该client发起了同步请求 可以确定是slave节点
     * */
    c->flags |= CLIENT_SLAVE;
    // 可以采用部分数据同步的节点 会直接认为在线  这之后执行的command都会存入到该client对应的缓冲区中
    c->replstate = SLAVE_STATE_ONLINE;
    // 更新ack时间戳
    c->repl_ack_time = server.unixtime;
    c->repl_put_online_on_ack = 0;
    // 因为收到了同步请求可以确定该client是slave节点 加入到slaves中
    listAddNodeTail(server.slaves,c);
    /* We can't use the connection buffers since they are used to accumulate
     * new commands at this stage. But we are sure the socket send buffer is
     * empty so this write will never fail actually. */
    if (c->slave_capa & SLAVE_CAPA_PSYNC2) {
        buflen = snprintf(buf,sizeof(buf),"+CONTINUE %s\r\n", server.replid);
    } else {
        buflen = snprintf(buf,sizeof(buf),"+CONTINUE\r\n");
    }

    // 这里将一个 continue的格式化字符串返回给client
    if (connWrite(c->conn,buf,buflen) != buflen) {
        freeClientAsync(c);
        return C_OK;
    }

    // 从slave要求的起始偏移量开始将数据发送给slave (从backlog中获取)
    psync_len = addReplyReplicationBacklog(c,psync_offset);
    serverLog(LL_NOTICE,
        "Partial resynchronization request from %s accepted. Sending %lld bytes of backlog starting from offset %lld.",
            replicationGetSlaveName(c),
            psync_len, psync_offset);
    /* Note that we don't need to set the selected DB at server.slaveseldb
     * to -1 to force the master to emit SELECT, since the slave already
     * has this state from the previous connection with the master. */
    // 刷新此时可用的slave节点数量
    refreshGoodSlavesCount();

    /* Fire the replica change modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                          REDISMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE,
                          NULL);

    return C_OK; /* The caller can return, no full resync needed. */

    // 返回异常信息代表需要同步全量数据
need_full_resync:
    /* We need a full resync for some reason... Note that we can't
     * reply to PSYNC right now if a full SYNC is needed. The reply
     * must include the master offset at the time the RDB file we transfer
     * is generated, so we need to delay the reply to that moment. */
    return C_ERR;
}

/* Start a BGSAVE for replication goals, which is, selecting the disk or
 * socket target depending on the configuration, and making sure that
 * the script cache is flushed before to start.
 *
 * The mincapa argument is the bitwise AND among all the slaves capabilities
 * of the slaves waiting for this BGSAVE, so represents the slave capabilities
 * all the slaves support. Can be tested via SLAVE_CAPA_* macros.
 *
 * Side effects, other than starting a BGSAVE:
 *
 * 1) Handle the slaves in WAIT_START state, by preparing them for a full
 *    sync if the BGSAVE was successfully started, or sending them an error
 *    and dropping them from the list of slaves.
 *
 * 2) Flush the Lua scripting script cache if the BGSAVE was actually
 *    started.
 *
 * Returns C_OK on success or C_ERR otherwise.
 * 开启子进程进行数据同步
 * @param mincapa 当前待同步数据的client具备的能力
 * */
int startBgsaveForReplication(int mincapa) {
    int retval;
    // 代表支持通过socket直接发送数据流
    int socket_target = server.repl_diskless_sync && (mincapa & SLAVE_CAPA_EOF);
    listIter li;
    listNode *ln;

    serverLog(LL_NOTICE,"Starting BGSAVE for SYNC with target: %s",
        socket_target ? "replicas sockets" : "disk");

    rdbSaveInfo rsi, *rsiptr;
    // 填充最新一次访问的dbid
    rsiptr = rdbPopulateSaveInfo(&rsi);
    /* Only do rdbSave* when rsiptr is not NULL,
     * otherwise slave will miss repl-stream-db.
     * 必须要存在rdbSaveInfo 才能进行数据恢复
     * */
    if (rsiptr) {
        if (socket_target)
            // 生成rdb数据 通过socket直接发送给所有需要同步全量数据的slave
            retval = rdbSaveToSlavesSockets(rsiptr);
        else
            // 在磁盘生成rdb数据
            retval = rdbSaveBackground(server.rdb_filename,rsiptr);
    } else {
        serverLog(LL_WARNING,"BGSAVE for replication: replication information not available, can't generate the RDB file right now. Try later.");
        retval = C_ERR;
    }

    /* If we succeeded to start a BGSAVE with disk target, let's remember
     * this fact, so that we can later delete the file if needed. Note
     * that we don't set the flag to 1 if the feature is disabled, otherwise
     * it would never be cleared: the file is not deleted. This way if
     * the user enables it later with CONFIG SET, we are fine.
     * 本次会先生成rdb文件 并且设置了在同步数据完成后 需要删除rdb文件 就会将该flag标记成1
     * */
    if (retval == C_OK && !socket_target && server.rdb_del_sync_files)
        RDBGeneratedByReplication = 1;

    /* If we failed to BGSAVE, remove the slaves waiting for a full
     * resynchronization from the list of slaves, inform them with
     * an error about what happened, close the connection ASAP.
     * 此时可能没有dbid信息 或者生成rdb数据失败了 无法正常同步全量数据 所以放弃这些slave节点
     * */
    if (retval == C_ERR) {
        serverLog(LL_WARNING,"BGSAVE for replication failed");
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;

            // 找到所有此时处于待同步全量数据的slave节点 断开连接  之后这些slave会重新发起连接 进行下一次重试
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                slave->replstate = REPL_STATE_NONE;
                slave->flags &= ~CLIENT_SLAVE;
                listDelNode(server.slaves,ln);
                addReplyError(slave,
                    "BGSAVE failed, replication can't continue");
                slave->flags |= CLIENT_CLOSE_AFTER_REPLY;
            }
        }
        return retval;
    }

    /* If the target is socket, rdbSaveToSlavesSockets() already setup
     * the slaves for a full resync. Otherwise for disk target do it now.
     * 如果本次是在磁盘生成rdb文件 先发送可以开始同步全量数据的标记 这样对端就会修改readHandler
     * */
    if (!socket_target) {
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;

            // 先将一个准备完成的信息返回给client
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                    replicationSetupSlaveForFullResync(slave,
                            getPsyncInitialOffset());
            }
        }
    }

    /* Flush the script cache, since we need that slave differences are
     * accumulated without requiring slaves to match our cached scripts.
     * 这里清理了一些cache属性
     * */
    if (retval == C_OK) replicationScriptCacheFlush();
    return retval;
}

/* SYNC and PSYNC command implementation.
 * 当slave尝试同步master数据时 就会发送该请求
 * */
void syncCommand(client *c) {

    /* ignore SYNC if already slave or in monitor mode
     * 代表重复处理请求了
     * */
    if (c->flags & CLIENT_SLAVE) return;

    /* Refuse SYNC requests if we are a slave but the link with our master
     * is not ok...
     * 作为slave节点也能处理这个请求 前提是他已经完成了与master的数据同步
     * */
    if (server.masterhost && server.repl_state != REPL_STATE_CONNECTED) {
        addReplySds(c,sdsnew("-NOMASTERLINK Can't SYNC while not connected with my master\r\n"));
        return;
    }

    /* SYNC can't be issued when the server has pending data to send to
     * the client about already issued commands. We need a fresh reply
     * buffer registering the differences between the BGSAVE and the current
     * dataset, so that we can copy to other slaves if needed.
     * 此时还有往client返回的数据未发送 无法执行同步请求
     * */
    if (clientHasPendingReplies(c)) {
        addReplyError(c,"SYNC and PSYNC are invalid with pending output");
        return;
    }

    serverLog(LL_NOTICE,"Replica %s asks for synchronization",
        replicationGetSlaveName(c));

    /* Try a partial resynchronization if this is a PSYNC command.
     * If it fails, we continue with usual full resynchronization, however
     * when this happens masterTryPartialResynchronization() already
     * replied with:
     *
     * +FULLRESYNC <replid> <offset>
     *
     * So the slave knows the new replid and offset to try a PSYNC later
     * if the connection with the master is lost.
     * 部分数据同步psync/数据同步sync 都是对应这个command
     * */
    if (!strcasecmp(c->argv[0]->ptr,"psync")) {
        // 判断此时backlog中的数据是否足够 并根据offset直接拉取这部分数据 返回给client
        if (masterTryPartialResynchronization(c) == C_OK) {
            server.stat_sync_partial_ok++;
            return; /* No full resync needed, return. */
        } else {
            // 代表需要同步全量数据
            char *master_replid = c->argv[1]->ptr;

            /* Increment stats for failed PSYNCs, but only if the
             * replid is not "?", as this is used by slaves to force a full
             * resync on purpose when they are not albe to partially
             * resync. */
            if (master_replid[0] != '?') server.stat_sync_partial_err++;
        }
    } else {
        /* If a slave uses SYNC, we are dealing with an old implementation
         * of the replication protocol (like redis-cli --slave). Flag the client
         * so that we don't expect to receive REPLCONF ACK feedbacks. */
        // 本次采用的是全量数据同步
        c->flags |= CLIENT_PRE_PSYNC;
    }

    /* Full resynchronization. */
    server.stat_sync_full++;

    /* Setup the slave as one waiting for BGSAVE to start. The following code
     * paths will change the state if we handle the slave differently.
     * 当采用全量数据同步时 使用后台进程处理
     * */
    c->replstate = SLAVE_STATE_WAIT_BGSAVE_START;

    // 更改连接的配置项
    if (server.repl_disable_tcp_nodelay)
        connDisableTcpNoDelay(c->conn); /* Non critical if it fails. */
    c->repldbfd = -1;
    c->flags |= CLIENT_SLAVE;
    // 添加到server的slave列表中
    listAddNodeTail(server.slaves,c);

    /* Create the replication backlog if needed.
     * 当此时接收到第一个slave节点 才有副本的概念 开始初始化副本id 并且之后才会开始留存backlog数据
     * 同时也表示了第二个slave节点起才可能进行部分数据同步 因为收到第一个节点才有backlog数据
     * */
    if (listLength(server.slaves) == 1 && server.repl_backlog == NULL) {
        /* When we create the backlog from scratch, we always use a new
         * replication ID and clear the ID2, since there is no valid
         * past history.
         * 先是创建副本1id，清理副本2id
         * */
        changeReplicationId();
        clearReplicationId2();
        // 初始化backlog
        createReplicationBacklog();
        serverLog(LL_NOTICE,"Replication backlog created, my new "
                            "replication IDs are '%s' and '%s'",
                            server.replid, server.replid2);
    }

    /* CASE 1: BGSAVE is in progress, with disk target.
     * 此时子进程还在进行生成rdb到磁盘的任务
     * */
    if (server.rdb_child_pid != -1 &&
        server.rdb_child_type == RDB_CHILD_TYPE_DISK)
    {
        /* Ok a background save is in progress. Let's check if it is a good
         * one for replication, i.e. if there is another slave that is
         * registering differences since the server forked to save. */
        client *slave;
        listNode *ln;
        listIter li;

        listRewind(server.slaves,&li);

        // 刚好到了修改成end状态 但是还没有关闭子进程 (因为是单线程可以确保串行) 在关闭子进程后就会开始传输rdb数据流
        while((ln = listNext(&li))) {
            slave = ln->value;
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END) break;
        }
        /* To attach this slave, we check that it has at least all the
         * capabilities of the slave that triggered the current BGSAVE.
         * 此时有某些slave 还没有开始传输数据 可以搭顺风车  这里要求该节点也支持这种同步方式
         * */
        if (ln && ((c->slave_capa & slave->slave_capa) == slave->slave_capa)) {
            /* Perfect, the server is already registering differences for
             * another slave. Set the right state, and copy the buffer.
             * 搭上了这次的rdb顺风车 并且此时该client内还存储了一些写入的command  这些数据是在rdb生成过程中新追加的
             * */
            copyClientOutputBuffer(c,slave);
            // 告知该节点将采用全量同步的方式
            replicationSetupSlaveForFullResync(c,slave->psync_initial_offset);
            serverLog(LL_NOTICE,"Waiting for end of BGSAVE for SYNC");
        } else {
            /* No way, we need to wait for the next BGSAVE in order to
             * register differences.
             * 等待rdb生成完毕后 会自动将所有start状态的修改成 end 并开始发送数据
             * */
            serverLog(LL_NOTICE,"Can't attach the replica to the current BGSAVE. Waiting for next BGSAVE for SYNC");
        }

    /* CASE 2: BGSAVE is in progress, with socket target.
     * 如果采用的是socket发送的方式 只能等待本次任务完成 才能关闭子进程 以及开始下一次传输
     * */
    } else if (server.rdb_child_pid != -1 &&
               server.rdb_child_type == RDB_CHILD_TYPE_SOCKET)
    {
        /* There is an RDB child process but it is writing directly to
         * children sockets. We need to wait for the next BGSAVE
         * in order to synchronize. */
        serverLog(LL_NOTICE,"Current BGSAVE has socket target. Waiting for next BGSAVE for SYNC");

    /* CASE 3: There is no BGSAVE is progress.
     * 此时还没有正在运行的rdb进程
     * */
    } else {
        // 将数据通过socket直接发送
        if (server.repl_diskless_sync && (c->slave_capa & SLAVE_CAPA_EOF)) {
            /* Diskless replication RDB child is created inside
             * replicationCron() since we want to delay its start a
             * few seconds to wait for more slaves to arrive.
             * 代表直接通过socket发送数据的方式存在一个延时 需要在replCron中判断是否满足时间条件
             * */
            if (server.repl_diskless_sync_delay)
                serverLog(LL_NOTICE,"Delay next BGSAVE for diskless SYNC");
        } else {
            /* Target is disk (or the slave is not capable of supporting
             * diskless replication) and we don't have a BGSAVE in progress,
             * let's start one.
             * 确保此时没有子进程时 可以开始生成rdb
             * */
            if (!hasActiveChildProcess()) {
                startBgsaveForReplication(c->slave_capa);
            } else {
                // 有其他子进程在工作 等待replCron的检测和回调
                serverLog(LL_NOTICE,
                    "No BGSAVE in progress, but another BG operation is active. "
                    "BGSAVE for replication delayed");
            }
        }
    }
    return;
}

/* REPLCONF <option> <value> <option> <value> ...
 * This command is used by a slave in order to configure the replication
 * process before starting it with the SYNC command.
 *
 * Currently the only use of this command is to communicate to the master
 * what is the listening port of the Slave redis instance, so that the
 * master can accurately list slaves and their listening ports in
 * the INFO output.
 *
 * In the future the same command can be used in order to configure
 * the replication to initiate an incremental replication instead of a
 * full resync.
 * slave会将最新的信息发送到master
 * */
void replconfCommand(client *c) {
    int j;

    // 参数是成对出现的
    if ((c->argc % 2) == 0) {
        /* Number of arguments must be odd to make sure that every
         * option has a corresponding value. */
        addReply(c,shared.syntaxerr);
        return;
    }

    /* Process every option-value pair.
     * */
    for (j = 1; j < c->argc; j+=2) {
        // slave针对副本功能开放出来的端口
        if (!strcasecmp(c->argv[j]->ptr,"listening-port")) {
            long port;

            if ((getLongFromObjectOrReply(c,c->argv[j+1],
                    &port,NULL) != C_OK))
                return;
            c->slave_listening_port = port;
        } else if (!strcasecmp(c->argv[j]->ptr,"ip-address")) {
            sds ip = c->argv[j+1]->ptr;
            if (sdslen(ip) < sizeof(c->slave_ip)) {
                memcpy(c->slave_ip,ip,sdslen(ip)+1);
            } else {
                addReplyErrorFormat(c,"REPLCONF ip-address provided by "
                    "replica instance is too long: %zd bytes", sdslen(ip));
                return;
            }
        } else if (!strcasecmp(c->argv[j]->ptr,"capa")) {
            /* Ignore capabilities not understood by this master. */
            if (!strcasecmp(c->argv[j+1]->ptr,"eof"))
                c->slave_capa |= SLAVE_CAPA_EOF;
            else if (!strcasecmp(c->argv[j+1]->ptr,"psync2"))
                c->slave_capa |= SLAVE_CAPA_PSYNC2;
            // 在slave节点至少同步了master的rdb数据后 (只剩下backlog数据未同步) 会开始定期向master上报自己的同步偏移量
        } else if (!strcasecmp(c->argv[j]->ptr,"ack")) {
            /* REPLCONF ACK is used by slave to inform the master the amount
             * of replication stream that it processed so far. It is an
             * internal only command that normal clients should never use. */
            long long offset;

            if (!(c->flags & CLIENT_SLAVE)) return;
            if ((getLongLongFromObject(c->argv[j+1], &offset) != C_OK))
                return;
            if (offset > c->repl_ack_off)
                c->repl_ack_off = offset;
            c->repl_ack_time = server.unixtime;
            /* If this was a diskless replication, we need to really put
             * the slave online when the first ACK is received (which
             * confirms slave is online and ready to get more data). This
             * allows for simpler and less CPU intensive EOF detection
             * when streaming RDB files.
             * 针对直接通过socket发送rdb数据流的情况 当slave发起ack请求才能确定它已经消化完这部分数据了 之后就可以发送backlog数据
             * */
            if (c->repl_put_online_on_ack && c->replstate == SLAVE_STATE_ONLINE)
                putSlaveOnline(c);
            /* Note: this command does not reply anything! */
            return;
        } else if (!strcasecmp(c->argv[j]->ptr,"getack")) {
            /* REPLCONF GETACK is used in order to request an ACK ASAP
             * to the slave. */
            if (server.masterhost && server.master) replicationSendAck();
            return;
        } else {
            addReplyErrorFormat(c,"Unrecognized REPLCONF option: %s",
                (char*)c->argv[j]->ptr);
            return;
        }
    }
    addReply(c,shared.ok);
}

/* This function puts a replica in the online state, and should be called just
 * after a replica received the RDB file for the initial synchronization, and
 * we are finally ready to send the incremental stream of commands.
 *
 * It does a few things:
 *
 * 1) Put the slave in ONLINE state. Note that the function may also be called
 *    for a replicas that are already in ONLINE state, but having the flag
 *    repl_put_online_on_ack set to true: we still have to install the write
 *    handler in that case. This function will take care of that.
 * 2) Make sure the writable event is re-installed, since calling the SYNC
 *    command disables it, so that we can accumulate output buffer without
 *    sending it to the replica.
 * 3) Update the count of "good replicas".
 * 当master已经将rdb数据完全发送到slave后 就可以认为该节点已经上线了
 * */
void putSlaveOnline(client *slave) {
    slave->replstate = SLAVE_STATE_ONLINE;
    slave->repl_put_online_on_ack = 0;
    slave->repl_ack_time = server.unixtime; /* Prevent false timeout. */
    // 恢复正常的writeHandler
    if (connSetWriteHandler(slave->conn, sendReplyToClient) == C_ERR) {
        serverLog(LL_WARNING,"Unable to register writable event for replica bulk transfer: %s", strerror(errno));
        freeClient(slave);
        return;
    }
    refreshGoodSlavesCount();
    /* Fire the replica change modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICA_CHANGE,
                          REDISMODULE_SUBEVENT_REPLICA_CHANGE_ONLINE,
                          NULL);
    serverLog(LL_NOTICE,"Synchronization with replica %s succeeded",
        replicationGetSlaveName(slave));
}

/* We call this function periodically to remove an RDB file that was
 * generated because of replication, in an instance that is otherwise
 * without any persistence. We don't want instances without persistence
 * to take RDB files around, this violates certain policies in certain
 * environments.
 * 判断在完成数据同步后是否需要删除rdb文件
 * */
void removeRDBUsedToSyncReplicas(void) {
    /* If the feature is disabled, return ASAP but also clear the
     * RDBGeneratedByReplication flag in case it was set. Otherwise if the
     * feature was enabled, but gets disabled later with CONFIG SET, the
     * flag may remain set to one: then next time the feature is re-enabled
     * via CONFIG SET we have have it set even if no RDB was generated
     * because of replication recently.
     * 不需要删除rdb文件 不做处理  但是会重置RDBGeneratedByReplication标记 这样即使下次调用该方法持久化策略发生了变化 也不会删除文件
     * */
    if (!server.rdb_del_sync_files) {
        RDBGeneratedByReplication = 0;
        return;
    }

    // 此时所有持久化策略都不可用 并且刚产生了一个rdb文件
    if (allPersistenceDisabled() && RDBGeneratedByReplication) {
        client *slave;
        listNode *ln;
        listIter li;

        // 只要还有一个slave未完成数据同步 就不会删除文件
        int delrdb = 1;
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            slave = ln->value;
            // 这3种状态分别对应 开始要求创建rdb文件，等待rdb文件的产生，传输文件数据流  代表文件还在使用中 无法立即删除
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START ||
                slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END ||
                slave->replstate == SLAVE_STATE_SEND_BULK)
            {
                delrdb = 0;
                break; /* No need to check the other replicas. */
            }
        }
        if (delrdb) {
            struct stat sb;
            if (lstat(server.rdb_filename,&sb) != -1) {
                RDBGeneratedByReplication = 0;
                serverLog(LL_NOTICE,
                    "Removing the RDB file used to feed replicas "
                    "in a persistence-less instance");
                bg_unlink(server.rdb_filename);
            }
        }
    }
}

/*
 * 将rdb文件流通过该conn传输到slave
 * @param conn
 */
void sendBulkToSlave(connection *conn) {
    client *slave = connGetPrivateData(conn);
    char buf[PROTO_IOBUF_LEN];
    ssize_t nwritten, buflen;

    /* Before sending the RDB file, we send the preamble as configured by the
     * replication process. Currently the preamble is just the bulk count of
     * the file in the form "$<length>\r\n".
     * 写入总长度信息
     * */
    if (slave->replpreamble) {
        nwritten = connWrite(conn,slave->replpreamble,sdslen(slave->replpreamble));
        if (nwritten == -1) {
            serverLog(LL_VERBOSE,
                "Write error sending RDB preamble to replica: %s",
                connGetLastError(conn));
            freeClient(slave);
            return;
        }
        server.stat_net_output_bytes += nwritten;
        // 裁剪数据 避免重复写入
        sdsrange(slave->replpreamble,nwritten,-1);
        // 此时已经写完序言信息 清理replpreamble
        if (sdslen(slave->replpreamble) == 0) {
            sdsfree(slave->replpreamble);
            slave->replpreamble = NULL;
            /* fall through sending data. */
        } else {
            return;
        }
    }

    // 上面就是写入本次要传输数据的总长度

    /* If the preamble was already transferred, send the RDB bulk data.
     * 通过slave来记录每个需要同步数据的slave此时读取到的rdb文件偏移量
     * repldbfd 此时就是rdb文件的句槟
     * */
    lseek(slave->repldbfd,slave->repldboff,SEEK_SET);
    buflen = read(slave->repldbfd,buf,PROTO_IOBUF_LEN);
    if (buflen <= 0) {
        serverLog(LL_WARNING,"Read error sending DB to replica: %s",
            (buflen == 0) ? "premature EOF" : strerror(errno));
        freeClient(slave);
        return;
    }
    if ((nwritten = connWrite(conn,buf,buflen)) == -1) {
        if (connGetState(conn) != CONN_STATE_CONNECTED) {
            serverLog(LL_WARNING,"Write error sending DB to replica: %s",
                connGetLastError(conn));
            freeClient(slave);
        }
        return;
    }
    slave->repldboff += nwritten;
    server.stat_net_output_bytes += nwritten;
    // 当所有数据读取完毕后 清理writeHandler 并将slave标记成上线状态
    if (slave->repldboff == slave->repldbsize) {
        close(slave->repldbfd);
        slave->repldbfd = -1;
        connSetWriteHandler(slave->conn,NULL);
        putSlaveOnline(slave);
    }
}

/* Remove one write handler from the list of connections waiting to be writable
 * during rdb pipe transfer.
 * 此时conn正在进行rdb的数据传输 但是由于写事件未准备好注册了writeHandler 现在需要移除该handler
 * */
void rdbPipeWriteHandlerConnRemoved(struct connection *conn) {
    // 如果之前conn绑定了writeHandler
    if (!connHasWriteHandler(conn))
        return;
    connSetWriteHandler(conn, NULL);

    // 代表同时使用本批数据的slave.conn减少
    server.rdb_pipe_numconns_writing--;
    /* if there are no more writes for now for this conn, or write error: */
    // 此时所有slave都已经使用完数据 可以开始拉取下一批数据了
    if (server.rdb_pipe_numconns_writing == 0) {
        if (aeCreateFileEvent(server.el, server.rdb_pipe_read, AE_READABLE, rdbPipeReadHandler,NULL) == AE_ERR) {
            serverPanic("Unrecoverable error creating server.rdb_pipe_read file event.");
        }
    }
}

/* Called in diskless master during transfer of data from the rdb pipe, when
 * the replica becomes writable again.
 * 在将rdb数据流直接通过socket发送的时候 如果某个conn的写入事件没有准备好 会注册该writeHandler续写数据
 * */
void rdbPipeWriteHandler(struct connection *conn) {
    serverAssert(server.rdb_pipe_bufflen>0);
    client *slave = connGetPrivateData(conn);
    int nwritten;

    // 每个slave会记录自己的读取偏移量，避免重复读取
    if ((nwritten = connWrite(conn, server.rdb_pipe_buff + slave->repldboff,
                              server.rdb_pipe_bufflen - slave->repldboff)) == -1)
    {
        if (connGetState(conn) == CONN_STATE_CONNECTED)
            return; /* equivalent to EAGAIN */
        serverLog(LL_WARNING,"Write error sending DB to replica: %s",
            connGetLastError(conn));
        freeClient(slave);
        return;
    } else {
        slave->repldboff += nwritten;
        server.stat_net_output_bytes += nwritten;
        // 代表还有数据需要写入 等待下一次触发rdbPipeWriteHandler
        if (slave->repldboff < server.rdb_pipe_bufflen)
            return; /* more data to write.. */
    }
    // 此时该slave 本次拉取的数据已经全部写入了
    rdbPipeWriteHandlerConnRemoved(conn);
}

/* When the the pipe serving diskless rdb transfer is drained (write end was
 * closed), we can clean up all the temporary variables, and cleanup after the
 * fork child.
 * 当父进程将子进程产生的所有rdb数据流都发送完毕后触发该方法
 * */
void RdbPipeCleanup() {
    // 关闭读通道 清理计数器和容器
    close(server.rdb_pipe_read);
    zfree(server.rdb_pipe_conns);
    server.rdb_pipe_conns = NULL;
    server.rdb_pipe_numconns = 0;
    server.rdb_pipe_numconns_writing = 0;
    zfree(server.rdb_pipe_buff);
    server.rdb_pipe_buff = NULL;
    server.rdb_pipe_bufflen = 0;

    /* Since we're avoiding to detect the child exited as long as the pipe is
     * not drained, so now is the time to check.
     * 进行一些子进程的清理工作
     * */
    checkChildrenDone();
}

/* Called in diskless master, when there's data to read from the child's rdb pipe
 * 当子进程将rdb数据都写入到pipe后触发该方法 主要是将这份快照数据发往所有需要同步数据的slave上
 * @param fd 实际上是 pipe_read的句柄
 * */
void rdbPipeReadHandler(struct aeEventLoop *eventLoop, int fd, void *clientData, int mask) {
    UNUSED(mask);
    UNUSED(clientData);
    UNUSED(eventLoop);
    int i;

    // 该buf作为缓冲容器
    if (!server.rdb_pipe_buff)
        server.rdb_pipe_buff = zmalloc(PROTO_IOBUF_LEN);
    serverAssert(server.rdb_pipe_numconns_writing==0);

    while (1) {
        // 从pipe_read中读取数据
        server.rdb_pipe_bufflen = read(fd, server.rdb_pipe_buff, PROTO_IOBUF_LEN);
        // 这里是异常情况 先忽略
        if (server.rdb_pipe_bufflen < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
                return;
            // 这里选择关闭等待数据同步的slave连接
            serverLog(LL_WARNING,"Diskless rdb transfer, read error sending DB to replicas: %s", strerror(errno));
            for (i=0; i < server.rdb_pipe_numconns; i++) {
                connection *conn = server.rdb_pipe_conns[i];
                if (!conn)
                    continue;
                client *slave = connGetPrivateData(conn);
                freeClient(slave);
                server.rdb_pipe_conns[i] = NULL;
            }
            killRDBChild();
            return;
        }

        // 代表此时数据已经完全读取好 就可以将pipe_read从el中移除 看来不仅仅是网络编程会使用到选择器模式 文件也可以套用
        if (server.rdb_pipe_bufflen == 0) {
            /* EOF - write end was closed. */
            int stillUp = 0;
            aeDeleteFileEvent(server.el, server.rdb_pipe_read, AE_READABLE);
            for (i=0; i < server.rdb_pipe_numconns; i++)
            {
                connection *conn = server.rdb_pipe_conns[i];
                if (!conn)
                    continue;
                stillUp++;
            }
            serverLog(LL_WARNING,"Diskless rdb transfer, done reading from pipe, %d replicas still up.", stillUp);
            RdbPipeCleanup();
            return;
        }

        // 每当读取到数据 就发送到所有需要同步数据的slave上
        int stillAlive = 0;
        for (i=0; i < server.rdb_pipe_numconns; i++)
        {
            int nwritten;
            connection *conn = server.rdb_pipe_conns[i];
            if (!conn)
                continue;

            client *slave = connGetPrivateData(conn);
            if ((nwritten = connWrite(conn, server.rdb_pipe_buff, server.rdb_pipe_bufflen)) == -1) {
                if (connGetState(conn) != CONN_STATE_CONNECTED) {
                    serverLog(LL_WARNING,"Diskless rdb transfer, write error sending DB to replica: %s",
                        connGetLastError(conn));
                    freeClient(slave);
                    server.rdb_pipe_conns[i] = NULL;
                    continue;
                }
                /* An error and still in connected state, is equivalent to EAGAIN */
                slave->repldboff = 0;
            } else {
                // 每个slave要维护自己的数据同步偏移量
                slave->repldboff = nwritten;
                server.stat_net_output_bytes += nwritten;
            }
            /* If we were unable to write all the data to one of the replicas,
             * setup write handler (and disable pipe read handler, below)
             * 代表此时socket写事件还没有准备好 针对目标连接注册writeHandler
             * */
            if (nwritten != server.rdb_pipe_bufflen) {
                // 只要有一个conn还未写完这部分数据 就无法开始读取下一份数据
                server.rdb_pipe_numconns_writing++;
                connSetWriteHandler(conn, rdbPipeWriteHandler);
            }
            stillAlive++;
        }

        // 此时所有slave的连接已断开 释放连接清理子进程
        if (stillAlive == 0) {
            serverLog(LL_WARNING,"Diskless rdb transfer, last replica dropped, killing fork child.");
            killRDBChild();
            RdbPipeCleanup();
        }
        /*  Remove the pipe read handler if at least one write handler was set.
         * 本轮数据还未写入到全部的conn中 不能读取下一轮数据
         * */
        if (server.rdb_pipe_numconns_writing || stillAlive == 0) {
            aeDeleteFileEvent(server.el, server.rdb_pipe_read, AE_READABLE);
            break;
        }
    }
}

/* This function is called at the end of every background saving,
 * or when the replication RDB transfer strategy is modified from
 * disk to socket or the other way around.
 *
 * The goal of this function is to handle slaves waiting for a successful
 * background saving in order to perform non-blocking synchronization, and
 * to schedule a new BGSAVE if there are slaves that attached while a
 * BGSAVE was in progress, but it was not a good one for replication (no
 * other slave was accumulating differences).
 *
 * The argument bgsaveerr is C_OK if the background saving succeeded
 * otherwise C_ERR is passed to the function.
 * The 'type' argument is the type of the child that terminated
 * (if it had a disk or socket target).
 * 当子进程生成rdb后触发 此时某些slave可能需要同步数据 在这里就会进行发送
 * @param type 本次执行的rdb类型
 * */
void updateSlavesWaitingBgsave(int bgsaveerr, int type) {
    listNode *ln;
    int startbgsave = 0;
    // 这里是求一个子节点功能的交集
    int mincapa = -1;
    listIter li;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {

            // 代表此时又有新的slave需要同步数据了
            startbgsave = 1;
            // 判断他们支持的同步方式
            mincapa = (mincapa == -1) ? slave->slave_capa :
                                        (mincapa & slave->slave_capa);
            // 这些是已经发送了需要同步全量数据信息标记的slave 并且本次进程创建的rdb就是根据这些slave期望的方式
        } else if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END) {
            struct redis_stat buf;

            if (bgsaveerr != C_OK) {
                freeClient(slave);
                serverLog(LL_WARNING,"SYNC failed. BGSAVE child returned an error");
                continue;
            }

            /* If this was an RDB on disk save, we have to prepare to send
             * the RDB from disk to the slave socket. Otherwise if this was
             * already an RDB -> Slaves socket transfer, used in the case of
             * diskless replication, our work is trivial, we can just put
             * the slave online.
             * 本次是直接通过socket发送rdb数据流的任务完成
             * */
            if (type == RDB_CHILD_TYPE_SOCKET) {
                serverLog(LL_NOTICE,
                    "Streamed RDB transfer with replica %s succeeded (socket). Waiting for REPLCONF ACK from slave to enable streaming",
                        replicationGetSlaveName(slave));
                /* Note: we wait for a REPLCONF ACK message from the replica in
                 * order to really put it online (install the write handler
                 * so that the accumulated data can be transferred). However
                 * we change the replication state ASAP, since our slave
                 * is technically online now.
                 *
                 * So things work like that:
                 *
                 * 1. We end trasnferring the RDB file via socket.
                 * 2. The replica is put ONLINE but the write handler
                 *    is not installed.
                 * 3. The replica however goes really online, and pings us
                 *    back via REPLCONF ACK commands.
                 * 4. Now we finally install the write handler, and send
                 *    the buffers accumulated so far to the replica.
                 *
                 * But why we do that? Because the replica, when we stream
                 * the RDB directly via the socket, must detect the RDB
                 * EOF (end of file), that is a special random string at the
                 * end of the RDB (for streamed RDBs we don't know the length
                 * in advance). Detecting such final EOF string is much
                 * simpler and less CPU intensive if no more data is sent
                 * after such final EOF. So we don't want to glue the end of
                 * the RDB trasfer with the start of the other replication
                 * data.
                 * 这时可以认为该slave已经上线了
                 * */
                slave->replstate = SLAVE_STATE_ONLINE;
                slave->repl_put_online_on_ack = 1;
                slave->repl_ack_time = server.unixtime; /* Timeout otherwise. */
            } else {
                // 代表现在本地生成rdb文件 此时还没有发送给slave
                if ((slave->repldbfd = open(server.rdb_filename,O_RDONLY)) == -1 ||
                    // 通过该函数获取文件统计信息 其中包含了文件的大小
                    redis_fstat(slave->repldbfd,&buf) == -1) {
                    freeClient(slave);
                    serverLog(LL_WARNING,"SYNC failed. Can't open/stat DB after BGSAVE: %s", strerror(errno));
                    continue;
                }
                slave->repldboff = 0;
                slave->repldbsize = buf.st_size;
                // 将状态修改成 要发送大块数据
                slave->replstate = SLAVE_STATE_SEND_BULK;
                // 针对这种情况 要写入数据的总长度
                slave->replpreamble = sdscatprintf(sdsempty(),"$%lld\r\n",
                    (unsigned long long) slave->repldbsize);

                connSetWriteHandler(slave->conn,NULL);
                // 为每个需要同步数据的连接设置 writeHandler
                if (connSetWriteHandler(slave->conn,sendBulkToSlave) == C_ERR) {
                    freeClient(slave);
                    continue;
                }
            }
        }
    }

    // 代表还有节点需要同步数据
    if (startbgsave) startBgsaveForReplication(mincapa);
}

/* Change the current instance replication ID with a new, random one.
 * This will prevent successful PSYNCs between this master and other
 * slaves, so the command should be called when something happens that
 * alters the current story of the dataset.
 * 随机生成一个副本id
 * */
void changeReplicationId(void) {
    getRandomHexChars(server.replid,CONFIG_RUN_ID_SIZE);
    server.replid[CONFIG_RUN_ID_SIZE] = '\0';
}

/* Clear (invalidate) the secondary replication ID. This happens, for
 * example, after a full resynchronization, when we start a new replication
 * history.
 * 清空 replid2
 * */
void clearReplicationId2(void) {
    memset(server.replid2,'0',sizeof(server.replid));
    server.replid2[CONFIG_RUN_ID_SIZE] = '\0';
    server.second_replid_offset = -1;
}

/* Use the current replication ID / offset as secondary replication
 * ID, and change the current one in order to start a new history.
 * This should be used when an instance is switched from slave to master
 * so that it can serve PSYNC requests performed using the master
 * replication ID.
 * 切换此时使用的副本id
 * */
void shiftReplicationId(void) {
    memcpy(server.replid2,server.replid,sizeof(server.replid));
    /* We set the second replid offset to the master offset + 1, since
     * the slave will ask for the first byte it has not yet received, so
     * we need to add one to the offset: for example if, as a slave, we are
     * sure we have the same history as the master for 50 bytes, after we
     * are turned into a master, we can accept a PSYNC request with offset
     * 51, since the slave asking has the same history up to the 50th
     * byte, and is asking for the new bytes starting at offset 51. */
    server.second_replid_offset = server.master_repl_offset+1;
    // 此时会重新生成一个replid
    changeReplicationId();
    serverLog(LL_WARNING,"Setting secondary replication ID to %s, valid up to offset: %lld. New replication ID is %s", server.replid2, server.second_replid_offset, server.replid);
}

/* ----------------------------------- SLAVE -------------------------------- */

/* Returns 1 if the given replication state is a handshake state,
 * 0 otherwise.
 * 作为slave节点 正在尝试跟某个master连接
 * */
int slaveIsInHandshakeState(void) {
    return server.repl_state >= REPL_STATE_RECEIVE_PONG &&
           server.repl_state <= REPL_STATE_RECEIVE_PSYNC;
}

/* Avoid the master to detect the slave is timing out while loading the
 * RDB file in initial synchronization. We send a single newline character
 * that is valid protocol but is guaranteed to either be sent entirely or
 * not, since the byte is indivisible.
 *
 * The function is called in two contexts: while we flush the current
 * data with emptyDb(), and while we load the new data received as an
 * RDB file from the master.
 * */
void replicationSendNewlineToMaster(void) {
    static time_t newline_sent;
    if (time(NULL) != newline_sent) {
        newline_sent = time(NULL);
        /* Pinging back in this stage is best-effort. */
        if (server.repl_transfer_s) connWrite(server.repl_transfer_s, "\n", 1);
    }
}

/* Callback used by emptyDb() while flushing away old data to load
 * the new dataset received by the master.
 * 主要是避免连接断开
 * */
void replicationEmptyDbCallback(void *privdata) {
    UNUSED(privdata);
    if (server.repl_state == REPL_STATE_TRANSFER)
        replicationSendNewlineToMaster();
}

/* Once we have a link with the master and the synchronization was
 * performed, this function materializes the master client we store
 * at server.master, starting from the specified file descriptor.
 * 当副本同步完master的数据后 才会设置master 并且设置readHandler
 * */
void replicationCreateMasterClient(connection *conn, int dbid) {
    server.master = createClient(conn);
    if (conn)
        connSetReadHandler(server.master->conn, readQueryFromClient);
    server.master->flags |= CLIENT_MASTER;
    server.master->authenticated = 1;
    // 填充master属性
    // slave节点此时认为master的总偏移量也是本节点刚刚同步完rdb数据对应的偏移量 通过这个可以判断是否需要同步全量数据
    server.master->reploff = server.master_initial_offset;
    server.master->read_reploff = server.master->reploff;
    server.master->user = NULL; /* This client can do everything. */
    memcpy(server.master->replid, server.master_replid,
        sizeof(server.master_replid));
    /* If master offset is set to -1, this master is old and is not
     * PSYNC capable, so we flag it accordingly.
     * 兼容性代码
     * */
    if (server.master->reploff == -1)
        server.master->flags |= CLIENT_PRE_PSYNC;

    // 如果此时指定了db 就设置master的此时数据针对的db
    if (dbid != -1) selectDb(server.master,dbid);
}

/* This function will try to re-enable the AOF file after the
 * master-replica synchronization: if it fails after multiple attempts
 * the replica cannot be considered reliable and exists with an
 * error.
 * 在数据同步完成后重启aof
 * */
void restartAOFAfterSYNC() {
    unsigned int tries, max_tries = 10;
    for (tries = 0; tries < max_tries; ++tries) {
        if (startAppendOnly() == C_OK) break;
        serverLog(LL_WARNING,
            "Failed enabling the AOF after successful master synchronization! "
            "Trying it again in one second.");
        sleep(1);
    }
    if (tries == max_tries) {
        serverLog(LL_WARNING,
            "FATAL: this replica instance finished the synchronization with "
            "its master, but the AOF can't be turned on. Exiting now.");
        exit(1);
    }
}

/*
 * 代表接收到的数据不希望落入本地文件 而是直接写入到db中
 * @return
 */
static int useDisklessLoad() {
    /* compute boolean decision to use diskless load */
    int enabled = server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB ||
           (server.repl_diskless_load == REPL_DISKLESS_LOAD_WHEN_DB_EMPTY && dbTotalServerKeyCount()==0);
    /* Check all modules handle read errors, otherwise it's not safe to use diskless load. */
    if (enabled && !moduleAllDatatypesHandleErrors()) {
        serverLog(LL_WARNING,
            "Skipping diskless-load because there are modules that don't handle read errors.");
        enabled = 0;
    }
    return enabled;
}

/* Helper function for readSyncBulkPayload() to make backups of the current
 * databases before socket-loading the new ones. The backups may be restored
 * by disklessLoadRestoreBackup or freed by disklessLoadDiscardBackup later. */
dbBackup *disklessLoadMakeBackup(void) {
    return backupDb();
}

/* Helper function for readSyncBulkPayload(): when replica-side diskless
 * database loading is used, Redis makes a backup of the existing databases
 * before loading the new ones from the socket.
 *
 * If the socket loading went wrong, we want to restore the old backups
 * into the server databases. */
void disklessLoadRestoreBackup(dbBackup *buckup) {
    restoreDbBackup(buckup);
}

/* Helper function for readSyncBulkPayload() to discard our old backups
 * when the loading succeeded. */
void disklessLoadDiscardBackup(dbBackup *buckup, int flag) {
    discardDbBackup(buckup, flag, replicationEmptyDbCallback);
}

/* Asynchronously read the SYNC payload we receive from a master
 * 接收从master发送的全量数据(rdb数据)
 * */
#define REPL_MAX_WRITTEN_BEFORE_FSYNC (1024*1024*8) /* 8 MB */
void readSyncBulkPayload(connection *conn) {
    char buf[PROTO_IOBUF_LEN];
    ssize_t nread, readlen, nwritten;

    // 是否要在磁盘先生成rdb文件 还是直接存入到db中
    int use_diskless_load = useDisklessLoad();

    // 某个db的备份对象
    dbBackup *diskless_load_backup = NULL;
    int empty_db_flags = server.repl_slave_lazy_flush ? EMPTYDB_ASYNC :
                                                        EMPTYDB_NO_FLAGS;
    off_t left;

    /* Static vars used to hold the EOF mark, and the last bytes received
     * from the server: when they match, we reached the end of the transfer. */
    static char eofmark[CONFIG_RUN_ID_SIZE];
    static char lastbytes[CONFIG_RUN_ID_SIZE];
    static int usemark = 0;

    /* If repl_transfer_size == -1 we still have to read the bulk length
     * from the master reply.
     * 本次收到的是rdb的第一份数据流  主要就是判断本次rdb数据流总长度是多少
     * */
    if (server.repl_transfer_size == -1) {
        // 读取长度信息
        if (connSyncReadLine(conn,buf,1024,server.repl_syncio_timeout*1000) == -1) {
            serverLog(LL_WARNING,
                "I/O error reading bulk count from MASTER: %s",
                strerror(errno));
            goto error;
        }

        if (buf[0] == '-') {
            serverLog(LL_WARNING,
                "MASTER aborted replication with an error: %s",
                buf+1);
            goto error;
        } else if (buf[0] == '\0') {
            /* At this stage just a newline works as a PING in order to take
             * the connection live. So we refresh our last interaction
             * timestamp. */
            server.repl_transfer_lastio = server.unixtime;
            return;
        } else if (buf[0] != '$') {
            serverLog(LL_WARNING,"Bad protocol from MASTER, the first byte is not '$' (we received '%s'), are you sure the host and port are right?", buf);
            goto error;
        }

        /* There are two possible forms for the bulk payload. One is the
         * usual $<count> bulk format. The other is used for diskless transfers
         * when the master does not know beforehand the size of the file to
         * transfer. In the latter case, the following format is used:
         *
         * $EOF:<40 bytes delimiter>
         *
         * At the end of the file the announced delimiter is transmitted. The
         * delimiter is long and random enough that the probability of a
         * collision with the actual file content can be ignored.
         * */
        if (strncmp(buf+1,"EOF:",4) == 0 && strlen(buf+5) >= CONFIG_RUN_ID_SIZE) {
            usemark = 1;
            memcpy(eofmark,buf+5,CONFIG_RUN_ID_SIZE);
            memset(lastbytes,0,CONFIG_RUN_ID_SIZE);
            /* Set any repl_transfer_size to avoid entering this code path
             * at the next call. */
            server.repl_transfer_size = 0;
            serverLog(LL_NOTICE,
                "MASTER <-> REPLICA sync: receiving streamed RDB from master with EOF %s",
                use_diskless_load? "to parser":"to disk");
        } else {
            // 解析本次rdb数据流的总长度
            usemark = 0;
            server.repl_transfer_size = strtol(buf+1,NULL,10);
            serverLog(LL_NOTICE,
                "MASTER <-> REPLICA sync: receiving %lld bytes from master %s",
                (long long) server.repl_transfer_size,
                use_diskless_load? "to parser":"to disk");
        }
        return;
    }

    // 代表从master接收到的数据 要先落入本地rdb文件
    if (!use_diskless_load) {
        /* Read the data from the socket, store it to a file and search
         * for the EOF. */
        if (usemark) {
            readlen = sizeof(buf);
        } else {
            // 代表剩余要读取的长度    repl_transfer_read 代表此时已经读取的长度
            left = server.repl_transfer_size - server.repl_transfer_read;
            readlen = (left < (signed)sizeof(buf)) ? left : (signed)sizeof(buf);
        }

        // 实际上触发readHandler仅仅代表读事件准备完成 还需要通过connRead之类的方法 真正从conn中读取数据
        nread = connRead(conn,buf,readlen);
        if (nread <= 0) {
            if (connGetState(conn) == CONN_STATE_CONNECTED) {
                /* equivalent to EAGAIN */
                return;
            }
            serverLog(LL_WARNING,"I/O error trying to sync with MASTER: %s",
                (nread == -1) ? strerror(errno) : "connection lost");
            cancelReplicationHandshake();
            return;
        }
        server.stat_net_input_bytes += nread;

        /* When a mark is used, we want to detect EOF asap in order to avoid
         * writing the EOF mark into the file... */
        int eof_reached = 0;

        // 代表EOF 先忽略
        if (usemark) {
            /* Update the last bytes array, and check if it matches our
             * delimiter. */
            if (nread >= CONFIG_RUN_ID_SIZE) {
                memcpy(lastbytes,buf+nread-CONFIG_RUN_ID_SIZE,
                       CONFIG_RUN_ID_SIZE);
            } else {
                int rem = CONFIG_RUN_ID_SIZE-nread;
                memmove(lastbytes,lastbytes+nread,rem);
                memcpy(lastbytes+rem,buf,nread);
            }
            if (memcmp(lastbytes,eofmark,CONFIG_RUN_ID_SIZE) == 0)
                eof_reached = 1;
        }

        /* Update the last I/O time for the replication transfer (used in
         * order to detect timeouts during replication), and write what we
         * got from the socket to the dump file on disk.
         * 更新最后一次收到数据的时间戳
         * */
        server.repl_transfer_lastio = server.unixtime;
        // 将rdb数据拷贝到 本地rdb临时文件
        if ((nwritten = write(server.repl_transfer_fd,buf,nread)) != nread) {
            serverLog(LL_WARNING,
                "Write error or short write writing to the DB dump file "
                "needed for MASTER <-> REPLICA synchronization: %s",
                (nwritten == -1) ? strerror(errno) : "short write");
            goto error;
        }
        server.repl_transfer_read += nread;

        /* Delete the last 40 bytes from the file if we reached EOF. */
        if (usemark && eof_reached) {
            if (ftruncate(server.repl_transfer_fd,
                server.repl_transfer_read - CONFIG_RUN_ID_SIZE) == -1)
            {
                serverLog(LL_WARNING,
                    "Error truncating the RDB file received from the master "
                    "for SYNC: %s", strerror(errno));
                goto error;
            }
        }

        /* Sync data on disk from time to time, otherwise at the end of the
         * transfer we may suffer a big delay as the memory buffers are copied
         * into the actual disk.
         * 每接收一部分数据后 会执行一次fsync操作  (当此时内存中的数据距离上次刷盘已经囤积了一定量)
         * */
        if (server.repl_transfer_read >=
            server.repl_transfer_last_fsync_off + REPL_MAX_WRITTEN_BEFORE_FSYNC)
        {
            // 本次需要刷盘的数据大小
            off_t sync_size = server.repl_transfer_read -
                              server.repl_transfer_last_fsync_off;
            rdb_fsync_range(server.repl_transfer_fd,
                server.repl_transfer_last_fsync_off, sync_size);
            // 更新刷盘点
            server.repl_transfer_last_fsync_off += sync_size;
        }

        /* Check if the transfer is now complete */
        if (!usemark) {
            if (server.repl_transfer_read == server.repl_transfer_size)
                eof_reached = 1;
        }

        /* If the transfer is yet not complete, we need to read more, so
         * return ASAP and wait for the handler to be called again.
         * 当数据还没有读取完时  直接返回
         * */
        if (!eof_reached) return;
    }

    // 有2种情况会进入下面的分支
    // 1.本次接收到的数据不会落入到本地rdb文件 会直接存入到db中
    // 2.此时数据已经处理完成 接下来会从本地rdb文件读取数据并恢复到db中

    /* We reach this point in one of the following cases:
     *
     * 1. The replica is using diskless replication, that is, it reads data
     *    directly from the socket to the Redis memory, without using
     *    a temporary RDB file on disk. In that case we just block and
     *    read everything from the socket.
     *
     * 2. Or when we are done reading from the socket to the RDB file, in
     *    such case we want just to read the RDB file in memory. */
    serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Flushing old data");

    /* We need to stop any AOF rewriting child before flusing and parsing
     * the RDB, otherwise we'll create a copy-on-write disaster.
     * 此时要先关闭aof
     * */
    if (server.aof_state != AOF_OFF) stopAppendOnly();

    /* When diskless RDB loading is used by replicas, it may be configured
     * in order to save the current DB instead of throwing it away,
     * so that we can restore it in case of failed transfer.
     * 代表从master处接收的数据会直接写入到db中  并且此时采用的是交换策略  非交换模式会直接清空db
     * */
    if (use_diskless_load &&
        server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB)
    {
        /* Create a backup of server.db[] and initialize to empty
         * dictionaries.
         * diskless_load_backup存储了备份的数据
         * */
        diskless_load_backup = disklessLoadMakeBackup();
    }
    /* We call to emptyDb even in case of REPL_DISKLESS_LOAD_SWAPDB
     * (Where disklessLoadMakeBackup left server.db empty) because we
     * want to execute all the auxiliary logic of emptyDb (Namely,
     * fire module events)
     * 将所有db清空
     * */
    emptyDb(-1,empty_db_flags,replicationEmptyDbCallback);

    /* Before loading the DB into memory we need to delete the readable
     * handler, otherwise it will get called recursively since
     * rdbLoad() will call the event loop to process events from time to
     * time for non blocking loading.
     * */
    connSetReadHandler(conn, NULL);
    serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Loading DB in memory");
    rdbSaveInfo rsi = RDB_SAVE_INFO_INIT;
    // 代表从conn中接收数据 并直接还原db
    if (use_diskless_load) {
        rio rdb;
        // 将conn包装成rio
        rioInitWithConn(&rdb,conn,server.repl_transfer_size);

        /* Put the socket in blocking mode to simplify RDB transfer.
         * We'll restore it when the RDB is received.*/
        connBlock(conn);
        connRecvTimeout(conn, server.repl_timeout*1000);
        // TODO module相关的
        startLoading(server.repl_transfer_size, RDBFLAGS_REPLICATION);

        // 因为数据流符合rdb的格式所以可以套用rdbLoadRio方法   之后redisObject会被还原,并设置到db中
        if (rdbLoadRio(&rdb,RDBFLAGS_REPLICATION,&rsi) != C_OK) {
            /* RDB loading failed. */
            stopLoading(0);
            serverLog(LL_WARNING,
                "Failed trying to load the MASTER synchronization DB "
                "from socket");
            // 关闭与master的连接，并等待下次在cron中的重连
            cancelReplicationHandshake();
            rioFreeConn(&rdb, NULL);

            /* Remove the half-loaded data in case we started with
             * an empty replica. */
            emptyDb(-1,empty_db_flags,replicationEmptyDbCallback);

            if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
                /* Restore the backed up databases. */
                // 数据恢复失败 重新使用之前备份的db
                disklessLoadRestoreBackup(diskless_load_backup);
            }

            /* Note that there's no point in restarting the AOF on SYNC
             * failure, it'll be restarted when sync succeeds or the replica
             * gets promoted. */
            return;
        }

        // 数据同步完成
        stopLoading(1);

        /* RDB loading succeeded if we reach this point. */
        if (server.repl_diskless_load == REPL_DISKLESS_LOAD_SWAPDB) {
            /* Delete the backup databases we created before starting to load
             * the new RDB. Now the RDB was loaded with success so the old
             * data is useless.
             * 因为数据已经恢复成功了 就可以丢弃备份数据
             * */
            disklessLoadDiscardBackup(diskless_load_backup, empty_db_flags);
        }

        /* Verify the end mark is correct. */
        if (usemark) {
            if (!rioRead(&rdb,buf,CONFIG_RUN_ID_SIZE) ||
                memcmp(buf,eofmark,CONFIG_RUN_ID_SIZE) != 0)
            {
                serverLog(LL_WARNING,"Replication stream EOF marker is broken");
                cancelReplicationHandshake();
                rioFreeConn(&rdb, NULL);
                return;
            }
        }

        /* Cleanup and restore the socket to the original state to continue
         * with the normal replication.
         * rdb数据传输完成 将连接恢复成非阻塞
         * */
        rioFreeConn(&rdb, NULL);
        connNonBlock(conn);
        connRecvTimeout(conn,0);
    } else {
        /* Ensure background save doesn't overwrite synced data
         * 当rdb文件先要落到磁盘时 进入这个分支，通过本地磁盘的rdb文件恢复db中的数据
         * */
        if (server.rdb_child_pid != -1) {
            serverLog(LL_NOTICE,
                "Replica is about to load the RDB file received from the "
                "master, but there is a pending RDB child running. "
                "Killing process %ld and removing its temp file to avoid "
                "any race",
                    (long) server.rdb_child_pid);
            // 如果此时有正在运行的rdb子进程 要先关闭 因为当前db中的数据是旧的 即使生成rdb数据也没有意义
            killRDBChild();
        }

        /* Make sure the new file (also used for persistence) is fully synced
         * (not covered by earlier calls to rdb_fsync_range).
         * 将通过socket接收到的rdb文件流刷盘
         * */
        if (fsync(server.repl_transfer_fd) == -1) {
            serverLog(LL_WARNING,
                "Failed trying to sync the temp DB to disk in "
                "MASTER <-> REPLICA synchronization: %s",
                strerror(errno));
            cancelReplicationHandshake();
            return;
        }

        /* Rename rdb like renaming rewrite aof asynchronously. */
        int old_rdb_fd = open(server.rdb_filename,O_RDONLY|O_NONBLOCK);
        // 通过重命名的方式实现覆盖
        if (rename(server.repl_transfer_tmpfile,server.rdb_filename) == -1) {
            serverLog(LL_WARNING,
                "Failed trying to rename the temp DB into %s in "
                "MASTER <-> REPLICA synchronization: %s",
                server.rdb_filename, strerror(errno));
            cancelReplicationHandshake();
            if (old_rdb_fd != -1) close(old_rdb_fd);
            return;
        }
        /* Close old rdb asynchronously. */
        if (old_rdb_fd != -1) bioCreateBackgroundJob(BIO_CLOSE_FILE,(void*)(long)old_rdb_fd,NULL,NULL);

        // 通过加载rdb文件数据 恢复db
        if (rdbLoad(server.rdb_filename,&rsi,RDBFLAGS_REPLICATION) != C_OK) {
            serverLog(LL_WARNING,
                "Failed trying to load the MASTER synchronization "
                "DB from disk");
            cancelReplicationHandshake();
            if (server.rdb_del_sync_files && allPersistenceDisabled()) {
                serverLog(LL_NOTICE,"Removing the RDB file obtained from "
                                    "the master. This replica has persistence "
                                    "disabled");
                bg_unlink(server.rdb_filename);
            }
            /* Note that there's no point in restarting the AOF on sync failure,
               it'll be restarted when sync succeeds or replica promoted. */
            return;
        }

        /* Cleanup.
         * 删除rdb文件
         * */
        if (server.rdb_del_sync_files && allPersistenceDisabled()) {
            serverLog(LL_NOTICE,"Removing the RDB file obtained from "
                                "the master. This replica has persistence "
                                "disabled");
            bg_unlink(server.rdb_filename);
        }

        zfree(server.repl_transfer_tmpfile);
        // 通过临时文件句柄关闭文件
        close(server.repl_transfer_fd);
        server.repl_transfer_fd = -1;
        server.repl_transfer_tmpfile = NULL;
    }

    // 此时已经完成了数据同步
    /* Final setup of the connected slave <- master link */
    // 当数据同步成功时才会设置master 并且设置readHandler
    replicationCreateMasterClient(server.repl_transfer_s,rsi.repl_stream_db);
    // 此时数据同步完成，可以接收外部client的请求了
    server.repl_state = REPL_STATE_CONNECTED;
    server.repl_down_since = 0;

    /* Fire the master link modules event.
     * */
    moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                          REDISMODULE_SUBEVENT_MASTER_LINK_UP,
                          NULL);

    /* After a full resynchronization we use the replication ID and
     * offset of the master. The secondary ID / offset are cleared since
     * we are starting a new history. */
    memcpy(server.replid,server.master->replid,sizeof(server.replid));

    // 注意在这里设置了master_repl_offset  rdb文件中持久化的是这个偏移量
    server.master_repl_offset = server.master->reploff;
    // 在同步了master的偏移量以及副本id后 清理副本2的数据
    clearReplicationId2();

    /* Let's create the replication backlog if needed. Slaves need to
     * accumulate the backlog regardless of the fact they have sub-slaves
     * or not, in order to behave correctly if they are promoted to
     * masters after a failover.
     * 每个slave节点也会创建backlog
     * */
    if (server.repl_backlog == NULL) createReplicationBacklog();
    serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Finished with success");

    if (server.supervised_mode == SUPERVISED_SYSTEMD) {
        redisCommunicateSystemd("STATUS=MASTER <-> REPLICA sync: Finished with success. Ready to accept connections.\n");
        redisCommunicateSystemd("READY=1\n");
    }

    /* Restart the AOF subsystem now that we finished the sync. This
     * will trigger an AOF rewrite, and when done will start appending
     * to the new file.
     * 重启aof  (在数据未同步完成时启动aof没有意义)
     * */
    if (server.aof_enabled) restartAOFAfterSYNC();
    return;

error:
    cancelReplicationHandshake();
    return;
}

/* Send a synchronous command to the master. Used to send AUTH and
 * REPLCONF commands before starting the replication with SYNC.
 *
 * The command returns an sds string representing the result of the
 * operation. On error the first byte is a "-".
 */
#define SYNC_CMD_READ (1<<0)
#define SYNC_CMD_WRITE (1<<1)
#define SYNC_CMD_FULL (SYNC_CMD_READ|SYNC_CMD_WRITE)
/**
 * 作为slave发送一条数据同步命令到master
 * @param flags
 * @param conn
 * @param ...
 * @return
 */
char *sendSynchronousCommand(int flags, connection *conn, ...) {

    /* Create the command to send to the master, we use redis binary
     * protocol to make sure correct arguments are sent. This function
     * is not safe for all binary data.
     * 本次是否需要发送数据
     * */
    if (flags & SYNC_CMD_WRITE) {
        char *arg;
        va_list ap;
        sds cmd = sdsempty();
        sds cmdargs = sdsempty();
        size_t argslen = 0;
        va_start(ap,conn);

        while(1) {
            arg = va_arg(ap, char*);
            if (arg == NULL) break;

            cmdargs = sdscatprintf(cmdargs,"$%zu\r\n%s\r\n",strlen(arg),arg);
            argslen++;
        }

        va_end(ap);

        cmd = sdscatprintf(cmd,"*%zu\r\n",argslen);
        cmd = sdscatsds(cmd,cmdargs);
        sdsfree(cmdargs);

        /* Transfer command to the server.
         * 以阻塞方式将数据写入到master 原本是通过el线程写入数据
         * */
        if (connSyncWrite(conn,cmd,sdslen(cmd),server.repl_syncio_timeout*1000)
            == -1)
        {
            sdsfree(cmd);
            return sdscatprintf(sdsempty(),"-Writing to master: %s",
                    connGetLastError(conn));
        }
        sdsfree(cmd);
    }

    /* Read the reply from the server.
     * 是否要读取从master返回的数据
     * */
    if (flags & SYNC_CMD_READ) {
        char buf[256];

        if (connSyncReadLine(conn,buf,sizeof(buf),server.repl_syncio_timeout*1000)
            == -1)
        {
            return sdscatprintf(sdsempty(),"-Reading from master: %s",
                    strerror(errno));
        }
        server.repl_transfer_lastio = server.unixtime;
        return sdsnew(buf);
    }
    return NULL;
}

/* Try a partial resynchronization with the master if we are about to reconnect.
 * If there is no cached master structure, at least try to issue a
 * "PSYNC ? -1" command in order to trigger a full resync using the PSYNC
 * command in order to obtain the master replid and the master replication
 * global offset.
 *
 * This function is designed to be called from syncWithMaster(), so the
 * following assumptions are made:
 *
 * 1) We pass the function an already connected socket "fd".
 * 2) This function does not close the file descriptor "fd". However in case
 *    of successful partial resynchronization, the function will reuse
 *    'fd' as file descriptor of the server.master client structure.
 *
 * The function is split in two halves: if read_reply is 0, the function
 * writes the PSYNC command on the socket, and a new function call is
 * needed, with read_reply set to 1, in order to read the reply of the
 * command. This is useful in order to support non blocking operations, so
 * that we write, return into the event loop, and read when there are data.
 *
 * When read_reply is 0 the function returns PSYNC_WRITE_ERR if there
 * was a write error, or PSYNC_WAIT_REPLY to signal we need another call
 * with read_reply set to 1. However even when read_reply is set to 1
 * the function may return PSYNC_WAIT_REPLY again to signal there were
 * insufficient data to read to complete its work. We should re-enter
 * into the event loop and wait in such a case.
 *
 * The function returns:
 *
 * PSYNC_CONTINUE: If the PSYNC command succeeded and we can continue.
 * PSYNC_FULLRESYNC: If PSYNC is supported but a full resync is needed.
 *                   In this case the master replid and global replication
 *                   offset is saved.
 * PSYNC_NOT_SUPPORTED: If the server does not understand PSYNC at all and
 *                      the caller should fall back to SYNC.
 * PSYNC_WRITE_ERROR: There was an error writing the command to the socket.
 * PSYNC_WAIT_REPLY: Call again the function with read_reply set to 1.
 * PSYNC_TRY_LATER: Master is currently in a transient error condition.
 *
 * Notable side effects:
 *
 * 1) As a side effect of the function call the function removes the readable
 *    event handler from "fd", unless the return value is PSYNC_WAIT_REPLY.
 * 2) server.master_initial_offset is set to the right value according
 *    to the master reply. This will be used to populate the 'server.master'
 *    structure replication offset.
 */

#define PSYNC_WRITE_ERROR 0
#define PSYNC_WAIT_REPLY 1
#define PSYNC_CONTINUE 2
#define PSYNC_FULLRESYNC 3
#define PSYNC_NOT_SUPPORTED 4
#define PSYNC_TRY_LATER 5
/*
 * 作为slave节点 当slave与master完成连接后 会先尝试同步部分数据
 * @param conn
 * @param read_reply   0代表发起psync请求  1代表处理结果
 * @return
 */
int slaveTryPartialResynchronization(connection *conn, int read_reply) {
    char *psync_replid;
    char psync_offset[32];
    sds reply;

    /* Writing half 先考虑该参数为0的情况 */
    if (!read_reply) {
        /* Initially set master_initial_offset to -1 to mark the current
         * master replid and offset as not valid. Later if we'll be able to do
         * a FULL resync using the PSYNC command we'll set the offset at the
         * right value, so that this information will be propagated to the
         * client structure representing the master into server.master.
         * 在没有发送同步请求探测master的偏移量时 是不知道master此时的偏移量的
         * */
        server.master_initial_offset = -1;

        // 存在一个旧的master信息 可能是在重启redis时从rdb文件中恢复的 会记录副本id 以及偏移量 只有当副本id匹配的时候才可以通过偏移量去判断需要同步多少数据
        if (server.cached_master) {
            psync_replid = server.cached_master->replid;
            // 这个reploff代表作为副本此时已经与master同步到了这个位置
            snprintf(psync_offset,sizeof(psync_offset),"%lld", server.cached_master->reploff+1);
            serverLog(LL_NOTICE,"Trying a partial resynchronization (request %s:%s).", psync_replid, psync_offset);
        } else {
            // 未获取到master信息时 使用的副本id是"?"
            serverLog(LL_NOTICE,"Partial resynchronization not possible (no cached master)");
            psync_replid = "?";
            memcpy(psync_offset,"-1",3);
        }

        /* Issue the PSYNC command 发送psync请求
         * */
        reply = sendSynchronousCommand(SYNC_CMD_WRITE,conn,"PSYNC",psync_replid,psync_offset,NULL);
        if (reply != NULL) {
            serverLog(LL_WARNING,"Unable to send PSYNC to master: %s",reply);
            sdsfree(reply);
            connSetReadHandler(conn, NULL);
            return PSYNC_WRITE_ERROR;
        }
        return PSYNC_WAIT_REPLY;
    }

    /* Reading half
     * 读取master返回的同步结果 注意这里是按行读取 针对部分数据同步的情况只会读取到一个replid 还不会读取backlog内的数据 之后master会异步的将数据发送过来 */
    reply = sendSynchronousCommand(SYNC_CMD_READ,conn,NULL);
    // 代表此时还没有准备好发送全量数据 需要等待
    if (sdslen(reply) == 0) {
        /* The master may send empty newlines after it receives PSYNC
         * and before to reply, just to keep the connection alive. */
        sdsfree(reply);
        return PSYNC_WAIT_REPLY;
    }

    // 切换readHander 这样就不会再转发到 syncMaster方法处理了
    // 此时满足部分数据同步或者全量数据同步的条件
    connSetReadHandler(conn, NULL);

    // 代表本次会同步全量数据
    if (!strncmp(reply,"+FULLRESYNC",11)) {
        char *replid = NULL, *offset = NULL;

        /* FULL RESYNC, parse the reply in order to extract the replid
         * and the replication offset.
         * 找到replid和offset数据的起始偏移量
         * */
        replid = strchr(reply,' ');
        if (replid) {
            replid++;
            offset = strchr(replid,' ');
            if (offset) offset++;
        }
        // 忽略异常格式
        if (!replid || !offset || (offset-replid-1) != CONFIG_RUN_ID_SIZE) {
            serverLog(LL_WARNING,
                "Master replied with wrong +FULLRESYNC syntax.");
            /* This is an unexpected condition, actually the +FULLRESYNC
             * reply means that the master supports PSYNC, but the reply
             * format seems wrong. To stay safe we blank the master
             * replid to make sure next PSYNCs will fail. */
            memset(server.master_replid,0,CONFIG_RUN_ID_SIZE+1);
        } else {
            memcpy(server.master_replid, replid, offset-replid-1);
            server.master_replid[CONFIG_RUN_ID_SIZE] = '\0';
            // 代表本次同步的rdb数据对应master的偏移量 只有当数据恢复完成才会将这个值设置到master_repl_offset上 因为本节点的rdb持久化是保存master_repl_offset
            server.master_initial_offset = strtoll(offset,NULL,10);
            serverLog(LL_NOTICE,"Full resync from master: %s:%lld",
                server.master_replid,
                server.master_initial_offset);
        }
        /* We are going to full resync, discard the cached master structure.
         * 此时如果要同步全量数据 就代表缓存的master数据都不可靠了  需要清理
         * */
        replicationDiscardCachedMaster();
        sdsfree(reply);
        return PSYNC_FULLRESYNC;
    }

    // 代表本次是部分数据同步
    if (!strncmp(reply,"+CONTINUE",9)) {
        /* Partial resync was accepted. */
        serverLog(LL_NOTICE,
            "Successful partial resynchronization with master.");

        /* Check the new replication ID advertised by the master. If it
         * changed, we need to set the new ID as primary ID, and set or
         * secondary ID as the old master ID up to the current offset, so
         * that our sub-slaves will be able to PSYNC with us after a
         * disconnection.
         * 之后的数据就是从master的backlog中获取的数据
         * */
        char *start = reply+10;
        char *end = reply+9;
        // 这里是读取replid
        while(end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;
        if (end-start == CONFIG_RUN_ID_SIZE) {
            char new[CONFIG_RUN_ID_SIZE+1];
            memcpy(new,start,CONFIG_RUN_ID_SIZE);
            new[CONFIG_RUN_ID_SIZE] = '\0';

            // 当本次传过去的副本id 与replid2相同时 才可能会进入这个分支 (因为replid2的原因才能同步数据)
            // 并且本次能够进行部分数据同步 必然是存在cached_master的(不然replid会传"?")
            if (strcmp(new,server.cached_master->replid)) {
                /* Master ID changed. */
                serverLog(LL_WARNING,"Master replication ID changed to %s",new);

                /* Set the old ID as our ID2, up to the current offset+1.
                 * 将之前的副本id/offset都存储到2号副本信息上
                 * */
                memcpy(server.replid2,server.cached_master->replid,
                    sizeof(server.replid2));
                // 将偏移量转移到了2号位
                server.second_replid_offset = server.master_repl_offset+1;

                /* Update the cached master ID and our own primary ID to the
                 * new one.
                 * 将最新的副本id 同时设置到replid和cached_master_replid 上
                 * */
                memcpy(server.replid,new,sizeof(server.replid));
                memcpy(server.cached_master->replid,new,sizeof(server.replid));

                /* Disconnect all the sub-slaves: they need to be notified.
                 * 每个slave节点在数据同步时可以从其他完成同步的slave上获取 这里就是断开这些slave
                 * */
                disconnectSlaves();
            }
        }

        /* Setup the replication to continue. */
        sdsfree(reply);
        // 这里恢复master 重新注册readHandler这之后就会收到master发出的backlog的数据 这样就完成了与master的数据同步
        replicationResurrectCachedMaster(conn);

        /* If this instance was restarted and we read the metadata to
         * PSYNC from the persistence file, our replication backlog could
         * be still not initialized. Create it.
         * 在本slave节点也初始化backlog结构
         * */
        if (server.repl_backlog == NULL) createReplicationBacklog();
        return PSYNC_CONTINUE;
    }

    /* If we reach this point we received either an error (since the master does
     * not understand PSYNC or because it is in a special state and cannot
     * serve our request), or an unexpected reply from the master.
     *
     * Return PSYNC_NOT_SUPPORTED on errors we don't understand, otherwise
     * return PSYNC_TRY_LATER if we believe this is a transient error.
     * 此时master数据加载还没完成 无法处理请求
     * */
    if (!strncmp(reply,"-NOMASTERLINK",13) ||
        !strncmp(reply,"-LOADING",8))
    {
        serverLog(LL_NOTICE,
            "Master is currently unable to PSYNC "
            "but should be in the future: %s", reply);
        sdsfree(reply);
        return PSYNC_TRY_LATER;
    }

    if (strncmp(reply,"-ERR",4)) {
        /* If it's not an error, log the unexpected event. */
        serverLog(LL_WARNING,
            "Unexpected reply to PSYNC from master: %s", reply);
    } else {
        serverLog(LL_NOTICE,
            "Master does not support PSYNC or is in "
            "error state (reply: %s)", reply);
    }
    sdsfree(reply);
    // 清理缓存数据
    replicationDiscardCachedMaster();
    return PSYNC_NOT_SUPPORTED;
}

/* This handler fires when the non blocking connect was able to
 * establish a connection with the master.
 * 在成功连接上master后  开始进行数据同步
 * @param
 * */
void syncWithMaster(connection *conn) {
    char tmpfile[256], *err = NULL;
    int dfd = -1, maxtries = 5;
    int psync_result;

    /* If this event fired after the user turned the instance into a master
     * with SLAVEOF NO ONE we must just return ASAP.
     * 如果在连接成功后 发现本节点已经脱离了主从结构 关闭连接 (比如master已经下线)
     * */
    if (server.repl_state == REPL_STATE_NONE) {
        connClose(conn);
        return;
    }

    /* Check for errors in the socket: after a non blocking connect() we
     * may find that the socket is in error state.
     * 连接创建失败
     * */
    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        serverLog(LL_WARNING,"Error condition on socket for SYNC: %s",
                connGetLastError(conn));
        goto error;
    }

    /* Send a PING to check the master is able to reply without errors.
     * 正常情况下,发起连接到master的请求后就会将state修改成REPL_STATE_CONNECTING
     * */
    if (server.repl_state == REPL_STATE_CONNECTING) {
        serverLog(LL_NOTICE,"Non blocking connect for SYNC fired the event.");
        /* Delete the writable event so that the readable event remains
         * registered and we can wait for the PONG reply.
         * 当连接上master成功后 会发送一个ping请求，之后等待对方返回结果就好
         * 在network模块接收到conn后会包装成client，并存储在一个列表中
         * */
        connSetReadHandler(conn, syncWithMaster);
        connSetWriteHandler(conn, NULL);
        // 当发出ping命令后 就会修改成pong状态
        server.repl_state = REPL_STATE_RECEIVE_PONG;
        /* Send the PING, don't check for errors at all, we have the timeout
         * that will take care about this.
         * 先发起一条心跳请求确保master可以正常处理请求
         * 当接收到pong结果后 还是触发syncWithMaster方法
         * */
        err = sendSynchronousCommand(SYNC_CMD_WRITE,conn,"PING",NULL);
        if (err) goto write_error;
        return;
    }

    /* Receive the PONG command.
     * 此时收到了master返回的pong请求 代表master可以正常工作 即使ip/port可以连通，也不能确定连接上的就是redis应用 所以需要握手请求
     * */
    if (server.repl_state == REPL_STATE_RECEIVE_PONG) {
        // 当触发readHandler只是代表准备好了读事件 通过调用sendSynchronousCommand完成数据读取  返回的结果应该是 "+PONG"
        err = sendSynchronousCommand(SYNC_CMD_READ,conn,NULL);

        /* We accept only two replies as valid, a positive +PONG reply
         * (we just check for "+") or an authentication error.
         * Note that older versions of Redis replied with "operation not
         * permitted" instead of using a proper error code, so we test
         * both. */
        if (err[0] != '+' &&
            strncmp(err,"-NOAUTH",7) != 0 &&
            strncmp(err,"-NOPERM",7) != 0 &&
            strncmp(err,"-ERR operation not permitted",28) != 0)
        {
            serverLog(LL_WARNING,"Error reply to PING from master: '%s'",err);
            sdsfree(err);
            goto error;
        } else {
            serverLog(LL_NOTICE,
                "Master replied to PING, replication can continue...");
        }
        sdsfree(err);
        // 将权限验证信息发送到master
        server.repl_state = REPL_STATE_SEND_AUTH;
    }

    /* AUTH with the master if required. */
    if (server.repl_state == REPL_STATE_SEND_AUTH) {
        if (server.masteruser && server.masterauth) {

            // 将权限校验信息发送到master
            err = sendSynchronousCommand(SYNC_CMD_WRITE,conn,"AUTH",
                                         server.masteruser,server.masterauth,NULL);
            if (err) goto write_error;
            server.repl_state = REPL_STATE_RECEIVE_AUTH;
            return;
        } else if (server.masterauth) {
            err = sendSynchronousCommand(SYNC_CMD_WRITE,conn,"AUTH",server.masterauth,NULL);
            if (err) goto write_error;
            server.repl_state = REPL_STATE_RECEIVE_AUTH;
            return;
        } else {
            // 如果没有设置密码 直接跳跃到REPL_STATE_SEND_PORT
            server.repl_state = REPL_STATE_SEND_PORT;
        }
    }

    /* Receive AUTH reply.
     * 阻塞等待结果
     * */
    if (server.repl_state == REPL_STATE_RECEIVE_AUTH) {
        err = sendSynchronousCommand(SYNC_CMD_READ,conn,NULL);
        if (err[0] == '-') {
            serverLog(LL_WARNING,"Unable to AUTH to MASTER: %s",err);
            sdsfree(err);
            goto error;
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_SEND_PORT;
    }

    /* Set the slave port, so that Master's INFO command can list the
     * slave listening port correctly.
     * 将用于replication的特殊端口告诉master
     * */
    if (server.repl_state == REPL_STATE_SEND_PORT) {
        int port;
        // 如果有特殊的通知端口 将该端口返回
        if (server.slave_announce_port) port = server.slave_announce_port;
        else if (server.tls_replication && server.tls_port) port = server.tls_port;
        else port = server.port;
        sds portstr = sdsfromlonglong(port);
        err = sendSynchronousCommand(SYNC_CMD_WRITE,conn,"REPLCONF",
                "listening-port",portstr, NULL);
        sdsfree(portstr);
        if (err) goto write_error;
        sdsfree(err);
        server.repl_state = REPL_STATE_RECEIVE_PORT;
        return;
    }

    /* Receive REPLCONF listening-port reply.
     * */
    if (server.repl_state == REPL_STATE_RECEIVE_PORT) {
        // 阻塞等待同步port的结果 正常情况下是+OK
        err = sendSynchronousCommand(SYNC_CMD_READ,conn,NULL);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF listening-port. */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Master does not understand "
                                "REPLCONF listening-port: %s", err);
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_SEND_IP;
    }

    /* Skip REPLCONF ip-address if there is no slave-announce-ip option set. */
    if (server.repl_state == REPL_STATE_SEND_IP &&
        server.slave_announce_ip == NULL)
    {
        // 将当前节点支持的功能发送给master
            server.repl_state = REPL_STATE_SEND_CAPA;
    }

    /* Set the slave ip, so that Master's INFO command can list the
     * slave IP address port correctly in case of port forwarding or NAT.
     * 将ip信息发送给master 这样master就只会将通过该ip传输数据
     * */
    if (server.repl_state == REPL_STATE_SEND_IP) {
        err = sendSynchronousCommand(SYNC_CMD_WRITE,conn,"REPLCONF",
                "ip-address",server.slave_announce_ip, NULL);
        if (err) goto write_error;
        sdsfree(err);
        server.repl_state = REPL_STATE_RECEIVE_IP;
        return;
    }

    /* Receive REPLCONF ip-address reply. */
    if (server.repl_state == REPL_STATE_RECEIVE_IP) {
        err = sendSynchronousCommand(SYNC_CMD_READ,conn,NULL);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF listening-port. */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Master does not understand "
                                "REPLCONF ip-address: %s", err);
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_SEND_CAPA;
    }

    /* Inform the master of our (slave) capabilities.
     *
     * EOF: supports EOF-style RDB transfer for diskless replication.
     * PSYNC2: supports PSYNC v2, so understands +CONTINUE <new repl ID>.
     *
     * The master will ignore capabilities it does not understand.
     * 将本节点支持的功能返回给master  目前同时支持eof和psync2
     * */
    if (server.repl_state == REPL_STATE_SEND_CAPA) {
        err = sendSynchronousCommand(SYNC_CMD_WRITE,conn,"REPLCONF",
                "capa","eof","capa","psync2",NULL);
        if (err) goto write_error;
        sdsfree(err);
        server.repl_state = REPL_STATE_RECEIVE_CAPA;
        return;
    }

    /* Receive CAPA reply. */
    if (server.repl_state == REPL_STATE_RECEIVE_CAPA) {
        // 当触发readHandler时 只是准备好读取事件 这里还要读取数据
        err = sendSynchronousCommand(SYNC_CMD_READ,conn,NULL);
        /* Ignore the error if any, not all the Redis versions support
         * REPLCONF capa. */
        if (err[0] == '-') {
            serverLog(LL_NOTICE,"(Non critical) Master does not understand "
                                  "REPLCONF capa: %s", err);
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_SEND_PSYNC;
    }

    /* Try a partial resynchonization. If we don't have a cached master
     * slaveTryPartialResynchronization() will at least try to use PSYNC
     * to start a full resynchronization so that we get the master replid
     * and the global offset, to try a partial resync at the next
     * reconnection attempt.
     * 当基础信息交换完后 开始同步数据 这里先判断能否仅同步部分数据
     * */
    if (server.repl_state == REPL_STATE_SEND_PSYNC) {
        if (slaveTryPartialResynchronization(conn,0) == PSYNC_WRITE_ERROR) {
            err = sdsnew("Write error sending the PSYNC command.");
            goto write_error;
        }
        // 等待psync结果
        server.repl_state = REPL_STATE_RECEIVE_PSYNC;
        return;
    }

    /* If reached this point, we should be in REPL_STATE_RECEIVE_PSYNC. */
    if (server.repl_state != REPL_STATE_RECEIVE_PSYNC) {
        serverLog(LL_WARNING,"syncWithMaster(): state machine error, "
                             "state should be RECEIVE_PSYNC but is %d",
                             server.repl_state);
        goto error;
    }

    // 此时收到了有关psync请求的结果
    psync_result = slaveTryPartialResynchronization(conn,1);
    // 本次收到的数据长度为0  等待下次数据 (在这之前readHandler还没有置空，所以下次还是会触发该方法)
    if (psync_result == PSYNC_WAIT_REPLY) return; /* Try again later... */

    /* If the master is in an transient error, we should try to PSYNC
     * from scratch later, so go to the error path. This happens when
     * the server is loading the dataset or is not connected with its
     * master and so forth.
     * 先断开与master的连接 等待之后的重连
     * */
    if (psync_result == PSYNC_TRY_LATER) goto error;

    /* Note: if PSYNC does not return WAIT_REPLY, it will take care of
     * uninstalling the read handler from the file descriptor.
     * 返回PSYNC_CONTINUE后 master会慢慢的将backlog以及之后的数据发送到slave节点 可以认为此时slave已经可用 此时已经将readHandler恢复成readQueryFromClient
     * */
    if (psync_result == PSYNC_CONTINUE) {
        serverLog(LL_NOTICE, "MASTER <-> REPLICA sync: Master accepted a Partial Resynchronization.");
        if (server.supervised_mode == SUPERVISED_SYSTEMD) {
            redisCommunicateSystemd("STATUS=MASTER <-> REPLICA sync: Partial Resynchronization accepted. Ready to accept connections.\n");
            redisCommunicateSystemd("READY=1\n");
        }
        return;
    }

    // 接下去的逻辑都是针对全量数据同步的 其中有可以正常同步数据的 也有出现异常情况的

    /* PSYNC failed or is not supported: we want our slaves to resync with us
     * as well, if we have any sub-slaves. The master may transfer us an
     * entirely different data set and we have no way to incrementally feed
     * our slaves after that.
     * 当本节点需要同步全量数据时 需要与所有slave断开连接 因为本节点的数据已经不可靠了
     * */
    disconnectSlaves(); /* Force our slaves to resync with us as well. */
    // backlog的数据也可以被清理了
    freeReplicationBacklog(); /* Don't allow our chained slaves to PSYNC. */

    /* Fall back to SYNC if needed. Otherwise psync_result == PSYNC_FULLRESYNC
     * and the server.master_replid and master_initial_offset are
     * already populated.
     * 因为某些原因 此时还无法同步全量数据
     * */
    if (psync_result == PSYNC_NOT_SUPPORTED) {
        serverLog(LL_NOTICE,"Retrying with SYNC...");
        // 直接发送一个要求同步全量数据的请求
        if (connSyncWrite(conn,"SYNC\r\n",6,server.repl_syncio_timeout*1000) == -1) {
            serverLog(LL_WARNING,"I/O error writing to MASTER: %s",
                strerror(errno));
            goto error;
        }
    }

    // 在收到master返回的"+FULLRESYNC" 就可以等待传输过来的rdb数据流了
    /* Prepare a suitable temp file for bulk transfer
     * 代表本次接收的rdb数据要先落入本地文件
     * */
    if (!useDisklessLoad()) {
        while(maxtries--) {
            snprintf(tmpfile,256,
                "temp-%d.%ld.rdb",(int)server.unixtime,(long int)getpid());
            dfd = open(tmpfile,O_CREAT|O_WRONLY|O_EXCL,0644);
            if (dfd != -1) break;
            sleep(1);
        }
        if (dfd == -1) {
            serverLog(LL_WARNING,"Opening the temp file needed for MASTER <-> REPLICA synchronization: %s",strerror(errno));
            goto error;
        }
        // 设置rdb临时文件 以及文件句柄 用于接收master传输过来的文件流
        server.repl_transfer_tmpfile = zstrdup(tmpfile);
        server.repl_transfer_fd = dfd;
    }

    /* Setup the non blocking download of the bulk file.
     * 设置用于接收master.rdb数据流的readHandler
     * */
    if (connSetReadHandler(conn, readSyncBulkPayload)
            == C_ERR)
    {
        char conninfo[CONN_INFO_LEN];
        serverLog(LL_WARNING,
            "Can't create readable event for SYNC: %s (%s)",
            strerror(errno), connGetInfo(conn, conninfo, sizeof(conninfo)));
        goto error;
    }

    server.repl_state = REPL_STATE_TRANSFER;
    server.repl_transfer_size = -1;
    server.repl_transfer_read = 0;
    server.repl_transfer_last_fsync_off = 0;
    server.repl_transfer_lastio = server.unixtime;
    return;

error:
    if (dfd != -1) close(dfd);
    connClose(conn);
    server.repl_transfer_s = NULL;
    if (server.repl_transfer_fd != -1)
        close(server.repl_transfer_fd);
    if (server.repl_transfer_tmpfile)
        zfree(server.repl_transfer_tmpfile);
    server.repl_transfer_tmpfile = NULL;
    server.repl_transfer_fd = -1;
    server.repl_state = REPL_STATE_CONNECT;
    return;

write_error: /* Handle sendSynchronousCommand(SYNC_CMD_WRITE) errors. */
    serverLog(LL_WARNING,"Sending command to master in replication handshake: %s", err);
    sdsfree(err);
    goto error;
}

/**
 * 当发现副本状态为待连接时，发起与master的连接
 * @return
 */
int connectWithMaster(void) {
    // 忽略tls 这里先创建一个默认连接
    server.repl_transfer_s = server.tls_replication ? connCreateTLS() : connCreateSocket();

    // 发起异步连接，成功时会触发syncWithMaster
    if (connConnect(server.repl_transfer_s, server.masterhost, server.masterport,
                NET_FIRST_BIND_ADDR, syncWithMaster) == C_ERR) {
        serverLog(LL_WARNING,"Unable to connect to MASTER: %s",
                connGetLastError(server.repl_transfer_s));
        // 连接失败 关闭句柄
        connClose(server.repl_transfer_s);
        server.repl_transfer_s = NULL;
        return C_ERR;
    }


    // 从创建连接开始就开始计时了 并将状态修改成连接中
    server.repl_transfer_lastio = server.unixtime;
    server.repl_state = REPL_STATE_CONNECTING;
    return C_OK;
}

/* This function can be called when a non blocking connection is currently
 * in progress to undo it.
 * Never call this function directly, use cancelReplicationHandshake() instead.
 */
void undoConnectWithMaster(void) {
    connClose(server.repl_transfer_s);
    server.repl_transfer_s = NULL;
}

/* Abort the async download of the bulk dataset while SYNC-ing with master.
 * Never call this function directly, use cancelReplicationHandshake() instead.
 */
void replicationAbortSyncTransfer(void) {
    serverAssert(server.repl_state == REPL_STATE_TRANSFER);
    undoConnectWithMaster();
    if (server.repl_transfer_fd!=-1) {
        close(server.repl_transfer_fd);
        bg_unlink(server.repl_transfer_tmpfile);
        zfree(server.repl_transfer_tmpfile);
        server.repl_transfer_tmpfile = NULL;
        server.repl_transfer_fd = -1;
    }
}

/* This function aborts a non blocking replication attempt if there is one
 * in progress, by canceling the non-blocking connect attempt or
 * the initial bulk transfer.
 *
 * If there was a replication handshake in progress 1 is returned and
 * the replication state (server.repl_state) set to REPL_STATE_CONNECT.
 *
 * Otherwise zero is returned and no operation is performed at all.
 * 比如与master连接超时 会断开连接
 * */
int cancelReplicationHandshake(void) {
    if (server.repl_state == REPL_STATE_TRANSFER) {
        replicationAbortSyncTransfer();
        // 如果原本正在传输中 修改成待连接状态
        server.repl_state = REPL_STATE_CONNECT;
    } else if (server.repl_state == REPL_STATE_CONNECTING ||
               slaveIsInHandshakeState())
    {
        // 断开与master的连接 并修改成待连接状态
        undoConnectWithMaster();
        server.repl_state = REPL_STATE_CONNECT;
    } else {
        return 0;
    }
    return 1;
}

/* Set replication to the specified master address and port.
 * 强制替换当前节点的master
 * */
void replicationSetMaster(char *ip, int port) {
    int was_master = server.masterhost == NULL;

    sdsfree(server.masterhost);
    server.masterhost = sdsnew(ip);
    server.masterport = port;
    // 如果已经存在master了 对旧的连接进行释放
    if (server.master) {
        freeClient(server.master);
    }
    // 断开所有处于阻塞状态的client  为什么 什么情况下client会阻塞 等待某个key ???
    disconnectAllBlockedClients(); /* Clients blocked in master, now slave. */

    /* Update oom_score_adj */
    setOOMScoreAdj(-1);

    /* Force our slaves to resync with us as well. They may hopefully be able
     * to partially resync with us, but we can notify the replid change.
     * 是因为发现了master节点么 本节点就要放弃所有的slave 也就是cluster中 还是只有一个master节点 ???
     * */
    disconnectSlaves();
    // 当发现此时保持着与master的连接 这条连接主要是用于传输数据的
    cancelReplicationHandshake();
    /* Before destroying our master state, create a cached master using
     * our own parameters, to later PSYNC with the new master.
     * 之前已经有master数据了 这里缓存一份master数据
     * */
    if (was_master) {
        replicationDiscardCachedMaster();
        replicationCacheMasterUsingMyself();
    }

    /* Fire the role change modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICATION_ROLE_CHANGED,
                          REDISMODULE_EVENT_REPLROLECHANGED_NOW_REPLICA,
                          NULL);

    /* Fire the master link modules event. */
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    server.repl_state = REPL_STATE_CONNECT;
}

/* Cancel replication, setting the instance as a master itself.
 * 因为master节点发生了替换 清理原来的数据
 * */
void replicationUnsetMaster(void) {
    // 代表之前没有设置master相关信息
    if (server.masterhost == NULL) return; /* Nothing to do. */

    /* Fire the master link modules event. */
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    sdsfree(server.masterhost);
    server.masterhost = NULL;
    // 释放连接
    if (server.master) freeClient(server.master);
    replicationDiscardCachedMaster();
    cancelReplicationHandshake();
    /* When a slave is turned into a master, the current replication ID
     * (that was inherited from the master at synchronization time) is
     * used as secondary ID up to the current offset, and a new replication
     * ID is created to continue with a new replication history.
     *
     * NOTE: this function MUST be called after we call
     * freeClient(server.master), since there we adjust the replication
     * offset trimming the final PINGs. See Github issue #7320.
     * TODO 切换 replicaid 切换了会怎么样呢 ???
     * */
    shiftReplicationId();
    /* Disconnecting all the slaves is required: we need to inform slaves
     * of the replication ID change (see shiftReplicationId() call). However
     * the slaves will be able to partially resync with us, so it will be
     * a very fast reconnection.
     * 释放所有的slave连接 为什么 应该不会有影响啊 切换master会影响到该节点之前维护的slave么 如果本节点晋升成master呢
     * */
    disconnectSlaves();
    server.repl_state = REPL_STATE_NONE;

    /* We need to make sure the new master will start the replication stream
     * with a SELECT statement. This is forced after a full resync, but
     * with PSYNC version 2, there is no need for full resync after a
     * master switch.
     * 此时选择的db 也重置了
     * */
    server.slaveseldb = -1;

    /* Update oom_score_adj */
    setOOMScoreAdj(-1);

    /* Once we turn from slave to master, we consider the starting time without
     * slaves (that is used to count the replication backlog time to live) as
     * starting from now. Otherwise the backlog will be freed after a
     * failover if slaves do not connect immediately.
     * 每当某个时刻slave节点被清空时 就会更新该时间戳
     * */
    server.repl_no_slaves_since = server.unixtime;

    /* Fire the role change modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_REPLICATION_ROLE_CHANGED,
                          REDISMODULE_EVENT_REPLROLECHANGED_NOW_MASTER,
                          NULL);

    /* Restart the AOF subsystem in case we shut it down during a sync when
     * we were still a slave.
     * 重启aof aof和replica是冲突的么 那么在数据同步的时候 aof还能工作么  还能同步新接入的请求么
     * */
    if (server.aof_enabled && server.aof_state == AOF_OFF) restartAOFAfterSYNC();
}

/* This function is called when the slave lose the connection with the
 * master into an unexpected way.
 * 当感应到master节点下线时 设置repl_down_since
 * */
void replicationHandleMasterDisconnection(void) {
    /* Fire the master link modules event. */
    if (server.repl_state == REPL_STATE_CONNECTED)
        moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                              REDISMODULE_SUBEVENT_MASTER_LINK_DOWN,
                              NULL);

    server.master = NULL;
    // 将状态重置成待连接
    server.repl_state = REPL_STATE_CONNECT;
    server.repl_down_since = server.unixtime;
    /* We lost connection with our master, don't disconnect slaves yet,
     * maybe we'll be able to PSYNC with our master later. We'll disconnect
     * the slaves only if we'll have to do a full resync with our master. */
}

void replicaofCommand(client *c) {
    /* SLAVEOF is not allowed in cluster mode as replication is automatically
     * configured using the current address of the master node. */
    if (server.cluster_enabled) {
        addReplyError(c,"REPLICAOF not allowed in cluster mode.");
        return;
    }

    /* The special host/port combination "NO" "ONE" turns the instance
     * into a master. Otherwise the new master address is set. */
    if (!strcasecmp(c->argv[1]->ptr,"no") &&
        !strcasecmp(c->argv[2]->ptr,"one")) {
        if (server.masterhost) {
            replicationUnsetMaster();
            sds client = catClientInfoString(sdsempty(),c);
            serverLog(LL_NOTICE,"MASTER MODE enabled (user request from '%s')",
                client);
            sdsfree(client);
        }
    } else {
        long port;

        if (c->flags & CLIENT_SLAVE)
        {
            /* If a client is already a replica they cannot run this command,
             * because it involves flushing all replicas (including this
             * client) */
            addReplyError(c, "Command is not valid when client is a replica.");
            return;
        }

        if ((getLongFromObjectOrReply(c, c->argv[2], &port, NULL) != C_OK))
            return;

        /* Check if we are already attached to the specified slave */
        if (server.masterhost && !strcasecmp(server.masterhost,c->argv[1]->ptr)
            && server.masterport == port) {
            serverLog(LL_NOTICE,"REPLICAOF would result into synchronization "
                                "with the master we are already connected "
                                "with. No operation performed.");
            addReplySds(c,sdsnew("+OK Already connected to specified "
                                 "master\r\n"));
            return;
        }
        /* There was no previous master or the user specified a different one,
         * we can continue. */
        replicationSetMaster(c->argv[1]->ptr, port);
        sds client = catClientInfoString(sdsempty(),c);
        serverLog(LL_NOTICE,"REPLICAOF %s:%d enabled (user request from '%s')",
            server.masterhost, server.masterport, client);
        sdsfree(client);
    }
    addReply(c,shared.ok);
}

/* ROLE command: provide information about the role of the instance
 * (master or slave) and additional information related to replication
 * in an easy to process format. */
void roleCommand(client *c) {
    if (server.masterhost == NULL) {
        listIter li;
        listNode *ln;
        void *mbcount;
        int slaves = 0;

        addReplyArrayLen(c,3);
        addReplyBulkCBuffer(c,"master",6);
        addReplyLongLong(c,server.master_repl_offset);
        mbcount = addReplyDeferredLen(c);
        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;
            char ip[NET_IP_STR_LEN], *slaveip = slave->slave_ip;

            if (slaveip[0] == '\0') {
                if (connPeerToString(slave->conn,ip,sizeof(ip),NULL) == -1)
                    continue;
                slaveip = ip;
            }
            if (slave->replstate != SLAVE_STATE_ONLINE) continue;
            addReplyArrayLen(c,3);
            addReplyBulkCString(c,slaveip);
            addReplyBulkLongLong(c,slave->slave_listening_port);
            addReplyBulkLongLong(c,slave->repl_ack_off);
            slaves++;
        }
        setDeferredArrayLen(c,mbcount,slaves);
    } else {
        char *slavestate = NULL;

        addReplyArrayLen(c,5);
        addReplyBulkCBuffer(c,"slave",5);
        addReplyBulkCString(c,server.masterhost);
        addReplyLongLong(c,server.masterport);
        if (slaveIsInHandshakeState()) {
            slavestate = "handshake";
        } else {
            switch(server.repl_state) {
            case REPL_STATE_NONE: slavestate = "none"; break;
            case REPL_STATE_CONNECT: slavestate = "connect"; break;
            case REPL_STATE_CONNECTING: slavestate = "connecting"; break;
            case REPL_STATE_TRANSFER: slavestate = "sync"; break;
            case REPL_STATE_CONNECTED: slavestate = "connected"; break;
            default: slavestate = "unknown"; break;
            }
        }
        addReplyBulkCString(c,slavestate);
        addReplyLongLong(c,server.master ? server.master->reploff : -1);
    }
}

/* Send a REPLCONF ACK command to the master to inform it about the current
 * processed offset. If we are not connected with a master, the command has
 * no effects.
 * 副本会定期上报自己的同步偏移量
 * */
void replicationSendAck(void) {
    client *c = server.master;

    if (c != NULL) {
        // 加上标记才能向master发送消息  正常情况下slave是不能向master发送消息的
        c->flags |= CLIENT_MASTER_FORCE_REPLY;
        addReplyArrayLen(c,3);
        addReplyBulkCString(c,"REPLCONF");
        addReplyBulkCString(c,"ACK");
        addReplyBulkLongLong(c,c->reploff);
        c->flags &= ~CLIENT_MASTER_FORCE_REPLY;
    }
}

/* ---------------------- MASTER CACHING FOR PSYNC -------------------------- */

/* In order to implement partial synchronization we need to be able to cache
 * our master's client structure after a transient disconnection.
 * It is cached into server.cached_master and flushed away using the following
 * functions. */

/* This function is called by freeClient() in order to cache the master
 * client structure instead of destroying it. freeClient() will return
 * ASAP after this function returns, so every action needed to avoid problems
 * with a client that is really "suspended" has to be done by this function.
 *
 * The other functions that will deal with the cached master are:
 *
 * replicationDiscardCachedMaster() that will make sure to kill the client
 * as for some reason we don't want to use it in the future.
 *
 * replicationResurrectCachedMaster() that is used after a successful PSYNC
 * handshake in order to reactivate the cached master.
 */
void replicationCacheMaster(client *c) {
    serverAssert(server.master != NULL && server.cached_master == NULL);
    serverLog(LL_NOTICE,"Caching the disconnected master state.");

    /* Unlink the client from the server structures. */
    unlinkClient(c);

    /* Reset the master client so that's ready to accept new commands:
     * we want to discard te non processed query buffers and non processed
     * offsets, including pending transactions, already populated arguments,
     * pending outputs to the master. */
    sdsclear(server.master->querybuf);
    sdsclear(server.master->pending_querybuf);
    server.master->read_reploff = server.master->reploff;
    if (c->flags & CLIENT_MULTI) discardTransaction(c);
    listEmpty(c->reply);
    c->sentlen = 0;
    c->reply_bytes = 0;
    c->bufpos = 0;
    resetClient(c);

    /* Save the master. Server.master will be set to null later by
     * replicationHandleMasterDisconnection(). */
    server.cached_master = server.master;

    /* Invalidate the Peer ID cache. */
    if (c->peerid) {
        sdsfree(c->peerid);
        c->peerid = NULL;
    }

    /* Caching the master happens instead of the actual freeClient() call,
     * so make sure to adjust the replication state. This function will
     * also set server.master to NULL. */
    replicationHandleMasterDisconnection();
}

/* This function is called when a master is turend into a slave, in order to
 * create from scratch a cached master for the new client, that will allow
 * to PSYNC with the slave that was promoted as the new master after a
 * failover.
 *
 * Assuming this instance was previously the master instance of the new master,
 * the new master will accept its replication ID, and potentiall also the
 * current offset if no data was lost during the failover. So we use our
 * current replication ID and offset in order to synthesize a cached master.
 * 在重启slave节点时 会通过加载rdb的数据发现上一次副本相关的repl_id/repl_offset 通过这些指标可以快速判断怎样与本次连接的master同步数据 全量同步还是部分同步
 * */
void replicationCacheMasterUsingMyself(void) {
    serverLog(LL_NOTICE,
        "Before turning into a replica, using my own master parameters "
        "to synthesize a cached master: I may be able to synchronize with "
        "the new master with just a partial transfer.");

    /* This will be used to populate the field server.master->reploff
     * by replicationCreateMasterClient(). We'll later set the created
     * master as server.cached_master, so the replica will use such
     * offset for PSYNC.
     * 设置初始偏移量
     * */
    server.master_initial_offset = server.master_repl_offset;

    /* The master client we create can be set to any DBID, because
     * the new master will start its replication stream with SELECT.
     * 生成的master最后是要设置到 cached_master上的
     * */
    replicationCreateMasterClient(NULL,-1);

    /* Use our own ID / offset. 拷贝replid */
    memcpy(server.master->replid, server.replid, sizeof(server.replid));

    /* Set as cached master. */
    unlinkClient(server.master);
    server.cached_master = server.master;
    server.master = NULL;
}

/* Free a cached master, called when there are no longer the conditions for
 * a partial resync on reconnection.
 * 清理master的缓存数据
 * */
void replicationDiscardCachedMaster(void) {
    if (server.cached_master == NULL) return;

    serverLog(LL_NOTICE,"Discarding previously cached master state.");
    server.cached_master->flags &= ~CLIENT_MASTER;
    freeClient(server.cached_master);
    server.cached_master = NULL;
}

/* Turn the cached master into the current master, using the file descriptor
 * passed as argument as the socket for the new master.
 *
 * This function is called when successfully setup a partial resynchronization
 * so the stream of data that we'll receive will start from were this
 * master left.
 * 将master设置成可用状态
 * */
void replicationResurrectCachedMaster(connection *conn) {
    server.master = server.cached_master;
    server.cached_master = NULL;
    server.master->conn = conn;
    connSetPrivateData(server.master->conn, server.master);
    server.master->flags &= ~(CLIENT_CLOSE_AFTER_REPLY|CLIENT_CLOSE_ASAP);
    server.master->authenticated = 1;
    server.master->lastinteraction = server.unixtime;
    server.repl_state = REPL_STATE_CONNECTED;
    server.repl_down_since = 0;

    /* Fire the master link modules event. */
    moduleFireServerEvent(REDISMODULE_EVENT_MASTER_LINK_CHANGE,
                          REDISMODULE_SUBEVENT_MASTER_LINK_UP,
                          NULL);

    /* Re-add to the list of clients.
     * 此时本节点已经能正常使用了 恢复readHandler/writeHandler
     * */
    linkClient(server.master);
    if (connSetReadHandler(server.master->conn, readQueryFromClient)) {
        serverLog(LL_WARNING,"Error resurrecting the cached master, impossible to add the readable handler: %s", strerror(errno));
        freeClientAsync(server.master); /* Close ASAP. */
    }

    /* We may also need to install the write handler as well if there is
     * pending data in the write buffers. */
    if (clientHasPendingReplies(server.master)) {
        if (connSetWriteHandler(server.master->conn, sendReplyToClient)) {
            serverLog(LL_WARNING,"Error resurrecting the cached master, impossible to add the writable handler: %s", strerror(errno));
            freeClientAsync(server.master); /* Close ASAP. */
        }
    }
}

/* ------------------------- MIN-SLAVES-TO-WRITE  --------------------------- */

/* This function counts the number of slaves with lag <= min-slaves-max-lag.
 * If the option is active, the server will prevent writes if there are not
 * enough connected slaves with the specified lag (or less).
 * 更新此时可用的slave数量
 * */
void refreshGoodSlavesCount(void) {
    listIter li;
    listNode *ln;
    int good = 0;

    // repl_good_slaves_count这个值本身是用来判断当前可用slave数量能否支撑正常执行command的  如果repl_min_slaves_to_write本身要求为false 就不需要更新这个值了
    if (!server.repl_min_slaves_to_write ||
        !server.repl_min_slaves_max_lag) return;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;
        time_t lag = server.unixtime - slave->repl_ack_time;

        if (slave->replstate == SLAVE_STATE_ONLINE &&
            lag <= server.repl_min_slaves_max_lag) good++;
    }
    server.repl_good_slaves_count = good;
}

/* ----------------------- REPLICATION SCRIPT CACHE --------------------------
 * The goal of this code is to keep track of scripts already sent to every
 * connected slave, in order to be able to replicate EVALSHA as it is without
 * translating it to EVAL every time it is possible.
 *
 * We use a capped collection implemented by a hash table for fast lookup
 * of scripts we can send as EVALSHA, plus a linked list that is used for
 * eviction of the oldest entry when the max number of items is reached.
 *
 * We don't care about taking a different cache for every different slave
 * since to fill the cache again is not very costly, the goal of this code
 * is to avoid that the same big script is transmitted a big number of times
 * per second wasting bandwidth and processor speed, but it is not a problem
 * if we need to rebuild the cache from scratch from time to time, every used
 * script will need to be transmitted a single time to reappear in the cache.
 *
 * This is how the system works:
 *
 * 1) Every time a new slave connects, we flush the whole script cache.
 * 2) We only send as EVALSHA what was sent to the master as EVALSHA, without
 *    trying to convert EVAL into EVALSHA specifically for slaves.
 * 3) Every time we transmit a script as EVAL to the slaves, we also add the
 *    corresponding SHA1 of the script into the cache as we are sure every
 *    slave knows about the script starting from now.
 * 4) On SCRIPT FLUSH command, we replicate the command to all the slaves
 *    and at the same time flush the script cache.
 * 5) When the last slave disconnects, flush the cache.
 * 6) We handle SCRIPT LOAD as well since that's how scripts are loaded
 *    in the master sometimes.
 */

/* Initialize the script cache, only called at startup.
 * 初始化脚本缓存  replication的职责应该就是将请求同步到集群中其他节点
 * */
void replicationScriptCacheInit(void) {
    server.repl_scriptcache_size = 10000;
    server.repl_scriptcache_dict = dictCreate(&replScriptCacheDictType,NULL);
    server.repl_scriptcache_fifo = listCreate();
}

/* Empty the script cache. Should be called every time we are no longer sure
 * that every slave knows about all the scripts in our set, or when the
 * current AOF "context" is no longer aware of the script. In general we
 * should flush the cache:
 *
 * 1) Every time a new slave reconnects to this master and performs a
 *    full SYNC (PSYNC does not require flushing).
 * 2) Every time an AOF rewrite is performed.
 * 3) Every time we are left without slaves at all, and AOF is off, in order
 *    to reclaim otherwise unused memory.
 */
void replicationScriptCacheFlush(void) {
    dictEmpty(server.repl_scriptcache_dict,NULL);
    listRelease(server.repl_scriptcache_fifo);
    server.repl_scriptcache_fifo = listCreate();
}

/* Add an entry into the script cache, if we reach max number of entries the
 * oldest is removed from the list. */
void replicationScriptCacheAdd(sds sha1) {
    int retval;
    sds key = sdsdup(sha1);

    /* Evict oldest. */
    if (listLength(server.repl_scriptcache_fifo) == server.repl_scriptcache_size)
    {
        listNode *ln = listLast(server.repl_scriptcache_fifo);
        sds oldest = listNodeValue(ln);

        retval = dictDelete(server.repl_scriptcache_dict,oldest);
        serverAssert(retval == DICT_OK);
        listDelNode(server.repl_scriptcache_fifo,ln);
    }

    /* Add current. */
    retval = dictAdd(server.repl_scriptcache_dict,key,NULL);
    listAddNodeHead(server.repl_scriptcache_fifo,key);
    serverAssert(retval == DICT_OK);
}

/* Returns non-zero if the specified entry exists inside the cache, that is,
 * if all the slaves are aware of this script SHA1. */
int replicationScriptCacheExists(sds sha1) {
    return dictFind(server.repl_scriptcache_dict,sha1) != NULL;
}

/* ----------------------- SYNCHRONOUS REPLICATION --------------------------
 * Redis synchronous replication design can be summarized in points:
 *
 * - Redis masters have a global replication offset, used by PSYNC.
 * - Master increment the offset every time new commands are sent to slaves.
 * - Slaves ping back masters with the offset processed so far.
 *
 * So synchronous replication adds a new WAIT command in the form:
 *
 *   WAIT <num_replicas> <milliseconds_timeout>
 *
 * That returns the number of replicas that processed the query when
 * we finally have at least num_replicas, or when the timeout was
 * reached.
 *
 * The command is implemented in this way:
 *
 * - Every time a client processes a command, we remember the replication
 *   offset after sending that command to the slaves.
 * - When WAIT is called, we ask slaves to send an acknowledgement ASAP.
 *   The client is blocked at the same time (see blocked.c).
 * - Once we receive enough ACKs for a given offset or when the timeout
 *   is reached, the WAIT command is unblocked and the reply sent to the
 *   client.
 */

/* This just set a flag so that we broadcast a REPLCONF GETACK command
 * to all the slaves in the beforeSleep() function. Note that this way
 * we "group" all the clients that want to wait for synchronous replication
 * in a given event loop iteration, and send a single GETACK for them all. */
void replicationRequestAckFromSlaves(void) {
    server.get_ack_from_slaves = 1;
}

/* Return the number of slaves that already acknowledged the specified
 * replication offset.
 * 判断有多少slave节点的同步offset(以ack的形式进行上报) 达到了要求值
 * */
int replicationCountAcksByOffset(long long offset) {
    listIter li;
    listNode *ln;
    int count = 0;

    listRewind(server.slaves,&li);
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        // 代表当前slave节点与server断开连接
        if (slave->replstate != SLAVE_STATE_ONLINE) continue;
        if (slave->repl_ack_off >= offset) count++;
    }
    return count;
}

/* WAIT for N replicas to acknowledge the processing of our latest
 * write command (and all the previous commands).
 * 作为master节点 接受某个副本的请求 等待集群中指定数量的副本的同步偏移量达到该client的偏移量
 * */
void waitCommand(client *c) {
    mstime_t timeout;
    long numreplicas, ackreplicas;
    long long offset = c->woff;

    // 副本节点不能执行这个command
    if (server.masterhost) {
        addReplyError(c,"WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.");
        return;
    }

    /* Argument parsing.
     * 读取数量和等待时间信息
     * */
    if (getLongFromObjectOrReply(c,c->argv[1],&numreplicas,NULL) != C_OK)
        return;
    if (getTimeoutFromObjectOrReply(c,c->argv[2],&timeout,UNIT_MILLISECONDS)
        != C_OK) return;

    /* First try without blocking at all.
     * 判断此时有多少节点已经同步到这个偏移量了
     * */
    ackreplicas = replicationCountAcksByOffset(c->woff);
    // 已经满足条件了 直接返回结果
    if (ackreplicas >= numreplicas || c->flags & CLIENT_MULTI) {
        addReplyLongLong(c,ackreplicas);
        return;
    }

    /* Otherwise block the client and put it into our list of clients
     * waiting for ack from slaves.
     * 将该client标记成阻塞状态
     * */
    c->bpop.timeout = timeout;
    // 使用这个同步偏移量字段记录信息
    c->bpop.reploffset = offset;
    c->bpop.numreplicas = numreplicas;
    // 将client追加到一个等待队列中
    listAddNodeTail(server.clients_waiting_acks,c);
    // 阻塞client
    blockClient(c,BLOCKED_WAIT);

    /* Make sure that the server will send an ACK request to all the slaves
     * before returning to the event loop.
     * 此时需要与其他所有slave节点通信，以便获取这些slave最新的同步偏移量
     * */
    replicationRequestAckFromSlaves();
}

/* This is called by unblockClient() to perform the blocking op type
 * specific cleanup. We just remove the client from the list of clients
 * waiting for replica acks. Never call it directly, call unblockClient()
 * instead.
 * 从对应的队列中移除元素
 * */
void unblockClientWaitingReplicas(client *c) {
    listNode *ln = listSearchKey(server.clients_waiting_acks,c);
    serverAssert(ln != NULL);
    listDelNode(server.clients_waiting_acks,ln);
}

/* Check if there are clients blocked in WAIT that can be unblocked since
 * we received enough ACKs from slaves.
 * 检测此时某些client要求的同步偏移量 是否有足够的slave满足
 * */
void processClientsWaitingReplicas(void) {
    long long last_offset = 0;
    int last_numreplicas = 0;

    listIter li;
    listNode *ln;

    listRewind(server.clients_waiting_acks,&li);
    while((ln = listNext(&li))) {
        client *c = ln->value;

        /* Every time we find a client that is satisfied for a given
         * offset and number of replicas, we remember it so the next client
         * may be unblocked without calling replicationCountAcksByOffset()
         * if the requested offset / replicas were equal or less. */
        if (last_offset && last_offset > c->bpop.reploffset &&
                           last_numreplicas > c->bpop.numreplicas)
        {
            unblockClient(c);
            addReplyLongLong(c,last_numreplicas);
        } else {
            // 针对这些处于wait状态的client，获取他们要求的同步偏移量进行检测
            int numreplicas = replicationCountAcksByOffset(c->bpop.reploffset);

            // 此时条件已经满足 将client从clients_waiting_acks中移除  如果client被阻塞会影响其他操作吗
            if (numreplicas >= c->bpop.numreplicas) {
                last_offset = c->bpop.reploffset;
                last_numreplicas = numreplicas;
                unblockClient(c);
                addReplyLongLong(c,numreplicas);
            }
        }
    }
}

/* Return the slave replication offset for this instance, that is
 * the offset for which we already processed the master replication stream.
 * 将本节点记录的master的最近一次偏移量返回
 * */
long long replicationGetSlaveOffset(void) {
    long long offset = 0;

    if (server.masterhost != NULL) {
        if (server.master) {
            offset = server.master->reploff;
        } else if (server.cached_master) {
            offset = server.cached_master->reploff;
        }
    }
    /* offset may be -1 when the master does not support it at all, however
     * this function is designed to return an offset that can express the
     * amount of data processed by the master, so we return a positive
     * integer. */
    if (offset < 0) offset = 0;
    return offset;
}

/* --------------------------- REPLICATION CRON  ---------------------------- */

/* Replication cron function, called 1 time per second.
 * 作为整个副本的入口
 * */
void replicationCron(void) {
    static long long replication_cron_loops = 0;

    /* Non blocking connection timeout?
     * 还处于连接阶段 连接超时
     * */
    if (server.masterhost &&
        (server.repl_state == REPL_STATE_CONNECTING ||
         slaveIsInHandshakeState()) &&
         (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        serverLog(LL_WARNING,"Timeout connecting to the MASTER...");
        // 断开连接 并将repl_state修改成待连接
        cancelReplicationHandshake();
    }

    /* Bulk transfer I/O timeout?
     * 在同步数据的过程中超时
     * */
    if (server.masterhost && server.repl_state == REPL_STATE_TRANSFER &&
        (time(NULL)-server.repl_transfer_lastio) > server.repl_timeout)
    {
        serverLog(LL_WARNING,"Timeout receiving bulk data from MASTER... If the problem persists try to set the 'repl-timeout' parameter in redis.conf to a larger value.");
        // 同样用这个方法处理
        cancelReplicationHandshake();
    }

    /* Timed out master when we are an already connected slave?
     * 长时间没有与master交互 断开连接
     * */
    if (server.masterhost && server.repl_state == REPL_STATE_CONNECTED &&
        (time(NULL)-server.master->lastinteraction) > server.repl_timeout)
    {
        serverLog(LL_WARNING,"MASTER timeout: no data nor PING received...");
        // 该方法会间接调用 replicationHandleMasterDisconnection() 就会将连接状态重置成REPL_STATE_CONNECT
        freeClient(server.master);
    }

    /* Check if we should connect to a MASTER
     * 此时处于待连接状态，同时发现还未连接到master
     * */
    if (server.repl_state == REPL_STATE_CONNECT) {
        serverLog(LL_NOTICE,"Connecting to MASTER %s:%d",
            server.masterhost, server.masterport);
        // 开始发起连接 会将状态修改成connecting 并在连接建立成功后回调syncWithMaster
        if (connectWithMaster() == C_OK) {
            serverLog(LL_NOTICE,"MASTER <-> REPLICA sync started");
        }
    }

    /* Send ACK to master from time to time.
     * Note that we do not send periodic acks to masters that don't
     * support PSYNC and replication offsets.
     * 作为slave节点 会时不时的将自身偏移量发送给master  (当server.master被设置时就代表至少完成了rdb的数据同步 或者只需要同步部分数据)
     * 携带CLIENT_PRE_PSYNC标记 代表此时不确定master的偏移量 不发送ack信息
     * */
    if (server.masterhost && server.master &&
        !(server.master->flags & CLIENT_PRE_PSYNC))
        replicationSendAck();

    /* If we have attached slaves, PING them from time to time.
     * So slaves can implement an explicit timeout to masters, and will
     * be able to detect a link disconnection even if the TCP connection
     * will not actually go down. */
    listIter li;
    listNode *ln;
    robj *ping_argv[1];

    /* First, send PING according to ping_slave_period.
     * 作为master节点向所有slave发送消息
     * */
    if ((replication_cron_loops % server.repl_ping_slave_period) == 0 &&
        listLength(server.slaves))
    {
        /* Note that we don't send the PING if the clients are paused during
         * a Redis Cluster manual failover: the PING we send will otherwise
         * alter the replication offsets of master and slave, and will no longer
         * match the one stored into 'mf_master_offset' state.
         * 如果此时开启了手动故障转移，检测是否超时 如果未超时，代表master需要等待某个slave晋升
         * */
        int manual_failover_in_progress =
            server.cluster_enabled &&
            server.cluster->mf_end &&
            clientsArePaused();

        // 这是正常情况 也就是不处于故障转移状态 会定期向所有slave节点发送ping请求 主要是为了更新最后一次与master交互的时间 避免被认为是断开连接了
        if (!manual_failover_in_progress) {
            ping_argv[0] = createStringObject("PING",4);
            replicationFeedSlaves(server.slaves, server.slaveseldb,
                ping_argv, 1);
            decrRefCount(ping_argv[0]);
        }
    }

    /* Second, send a newline to all the slaves in pre-synchronization
     * stage, that is, slaves waiting for the master to create the RDB file.
     *
     * Also send the a newline to all the chained slaves we have, if we lost
     * connection from our master, to keep the slaves aware that their
     * master is online. This is needed since sub-slaves only receive proxied
     * data from top-level masters, so there is no explicit pinging in order
     * to avoid altering the replication offsets. This special out of band
     * pings (newlines) can be sent, they will have no effect in the offset.
     *
     * The newline will be ignored by the slave but will refresh the
     * last interaction timer preventing a timeout. In this case we ignore the
     * ping period and refresh the connection once per second since certain
     * timeouts are set at a few seconds (example: PSYNC response).
     * */
    listRewind(server.slaves,&li);
    // 在执行syncCommand时 会发现此时不满足发送全量数据的条件,无法处理，但是会给slave打上SLAVE_STATE_WAIT_BGSAVE_START标记  在这里就是针对这些slave返回空字符串
    // 在slave接受数据的地方就会进行等待。
    while((ln = listNext(&li))) {
        client *slave = ln->value;

        int is_presync =
            (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START ||
            // 下面代表会发送rdb文件流
            (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
             server.rdb_child_type != RDB_CHILD_TYPE_SOCKET));

        if (is_presync) {
            connWrite(slave->conn, "\n", 1);
        }
    }

    /* Disconnect timedout slaves.
     * 检测与哪些节点长时间未通信 清理下线slave
     * */
    if (listLength(server.slaves)) {
        listIter li;
        listNode *ln;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;

            // 此时还没有上线的slave不需要处理
            if (slave->replstate != SLAVE_STATE_ONLINE) continue;
            // 采用全量数据同步的slave也跳过
            if (slave->flags & CLIENT_PRE_PSYNC) continue;
            // 代表该slave长时间未更新ack时间戳 认为已经断开连接了  对应slave定期发起ack请求
            if ((server.unixtime - slave->repl_ack_time) > server.repl_timeout)
            {
                serverLog(LL_WARNING, "Disconnecting timedout replica: %s",
                    replicationGetSlaveName(slave));
                freeClient(slave);
            }
        }
    }

    /* If this is a master without attached slaves and there is a replication
     * backlog active, in order to reclaim memory we can free it after some
     * (configured) time. Note that this cannot be done for slaves: slaves
     * without sub-slaves attached should still accumulate data into the
     * backlog, in order to reply to PSYNC queries if they are turned into
     * masters after a failover.
     * 作为master节点 此时没有slave节点连接上
     * 并且backlog数据有一个有效时间 这时就可以尝试清理backlog数据了
     * */
    if (listLength(server.slaves) == 0 && server.repl_backlog_time_limit &&
        server.repl_backlog && server.masterhost == NULL)
    {
        // 代表从没有slave到现在过了多久
        time_t idle = server.unixtime - server.repl_no_slaves_since;

        // 当时间间隔超过了要保留积压数据的时间 就可以清理这些数据了
        if (idle > server.repl_backlog_time_limit) {
            /* When we free the backlog, we always use a new
             * replication ID and clear the ID2. This is needed
             * because when there is no backlog, the master_repl_offset
             * is not updated, but we would still retain our replication
             * ID, leading to the following problem:
             *
             * 1. We are a master instance.
             * 2. Our slave is promoted to master. It's repl-id-2 will
             *    be the same as our repl-id.
             * 3. We, yet as master, receive some updates, that will not
             *    increment the master_repl_offset.
             * 4. Later we are turned into a slave, connect to the new
             *    master that will accept our PSYNC request by second
             *    replication ID, but there will be data inconsistency
             *    because we received writes.
             *    因为长时间没有slave 认为replica已经发生了变化所以要重新分配一个id
             *    */
            changeReplicationId();
            clearReplicationId2();
            // 清理积压数据
            freeReplicationBacklog();
            serverLog(LL_NOTICE,
                "Replication backlog freed after %d seconds "
                "without connected replicas.",
                (int) server.repl_backlog_time_limit);
        }
    }

    /* If AOF is disabled and we no longer have attached slaves, we can
     * free our Replication Script Cache as there is no need to propagate
     * EVALSHA at all.
     * TODO 忽略脚本数据
     * */
    if (listLength(server.slaves) == 0 &&
        server.aof_state == AOF_OFF &&
        listLength(server.repl_scriptcache_fifo) != 0)
    {
        replicationScriptCacheFlush();
    }

    /* Start a BGSAVE good for replication if we have slaves in
     * WAIT_BGSAVE_START state.
     *
     * In case of diskless replication, we make sure to wait the specified
     * number of seconds (according to configuration) so that other slaves
     * have the time to arrive before we start streaming.
     * 在master接收到slave的同步数据请求时 可能会发现rdb任务正在进行中 这时就无法发送全量数据。
     * 在主循环中检测子进程是否已经完成任务，完成的话就可以重新创建rdb生成任务
     * */
    if (!hasActiveChildProcess()) {

        // max_idle 记录所有slave中等待时间最长的节点
        time_t idle, max_idle = 0;
        // 此时有多少slave在等待同步数据
        int slaves_waiting = 0;
        // 所有子节点支持的数据同步方式交集
        int mincapa = -1;
        listNode *ln;
        listIter li;

        listRewind(server.slaves,&li);
        while((ln = listNext(&li))) {
            client *slave = ln->value;
            // 该slave处于等待数据同步的阶段
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                idle = server.unixtime - slave->lastinteraction;
                if (idle > max_idle) max_idle = idle;
                slaves_waiting++;
                mincapa = (mincapa == -1) ? slave->slave_capa :
                                            (mincapa & slave->slave_capa);
            }
        }

        // 有节点在等待传输同步数据 并且本次采用的是先生成rdb的方式 或者直接通过socket传输数据且满足了延时条件
        if (slaves_waiting &&
            (!server.repl_diskless_sync ||
             max_idle > server.repl_diskless_sync_delay))
        {
            /* Start the BGSAVE. The called function may start a
             * BGSAVE with socket target or disk target depending on the
             * configuration and slaves capabilities.
             * 通过后台进程同步全量数据
             * */
            startBgsaveForReplication(mincapa);
        }
    }

    /* Remove the RDB file used for replication if Redis is not running
     * with any persistence.
     * 如果rdb不要求对副本使用到的rdb文件进行持久化 要进行删除
     * */
    removeRDBUsedToSyncReplicas();

    /* Refresh the number of slaves with lag <= min-slaves-max-lag.
     * 刷新此时可用的slave数量
     * */
    refreshGoodSlavesCount();
    replication_cron_loops++; /* Incremented with frequency 1 HZ. */
}
