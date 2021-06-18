/* Redis Cluster implementation.
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
#include "endianconv.h"

#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <math.h>

/* A global reference to myself is handy to make code more clear.
 * Myself always points to server.cluster->myself, that is, the clusterNode
 * that represents this node. */
clusterNode *myself = NULL;

clusterNode *createClusterNode(char *nodename, int flags);

int clusterAddNode(clusterNode *node);

void clusterAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask);

void clusterReadHandler(connection *conn);

void clusterSendPing(clusterLink *link, int type);

void clusterSendFail(char *nodename);

void clusterSendFailoverAuthIfNeeded(clusterNode *node, clusterMsg *request);

void clusterUpdateState(void);

int clusterNodeGetSlotBit(clusterNode *n, int slot);

sds clusterGenNodesDescription(int filter);

clusterNode *clusterLookupNode(const char *name);

int clusterNodeAddSlave(clusterNode *master, clusterNode *slave);

int clusterAddSlot(clusterNode *n, int slot);

int clusterDelSlot(int slot);

int clusterDelNodeSlots(clusterNode *node);

int clusterNodeSetSlotBit(clusterNode *n, int slot);

void clusterSetMaster(clusterNode *n);

void clusterHandleSlaveFailover(void);

void clusterHandleSlaveMigration(int max_slaves);

int bitmapTestBit(unsigned char *bitmap, int pos);

void clusterDoBeforeSleep(int flags);

void clusterSendUpdate(clusterLink *link, clusterNode *node);

void resetManualFailover(void);

void clusterCloseAllSlots(void);

void clusterSetNodeAsMaster(clusterNode *n);

void clusterDelNode(clusterNode *delnode);

sds representClusterNodeFlags(sds ci, uint16_t flags);

uint64_t clusterGetMaxEpoch(void);

int clusterBumpConfigEpochWithoutConsensus(void);

void moduleCallClusterReceivers(const char *sender_id, uint64_t module_id, uint8_t type, const unsigned char *payload,
                                uint32_t len);

#define RCVBUF_INIT_LEN 1024
#define RCVBUF_MAX_PREALLOC (1<<20) /* 1MB */

/* -----------------------------------------------------------------------------
 * Initialization
 * -------------------------------------------------------------------------- */

/* Load the cluster config from 'filename'.
 *
 * If the file does not exist or is zero-length (this may happen because
 * when we lock the nodes.conf file, we create a zero-length one for the
 * sake of locking if it does not already exist), C_ERR is returned.
 * If the configuration was loaded from the file, C_OK is returned.
 * 在redis节点启动时 如果发现是以集群模式打开的 就会加载集群配置
 * */
int clusterLoadConfig(char *filename) {
    FILE *fp = fopen(filename, "r");
    struct stat sb;
    char *line;
    int maxline, j;

    if (fp == NULL) {
        if (errno == ENOENT) {
            return C_ERR;
        } else {
            serverLog(LL_WARNING,
                      "Loading the cluster node config from %s: %s",
                      filename, strerror(errno));
            exit(1);
        }
    }

    /* Check if the file is zero-length: if so return C_ERR to signal
     * we have to write the config.
     * 文件长度为0 返回异常信息
     * */
    if (fstat(fileno(fp), &sb) != -1 && sb.st_size == 0) {
        fclose(fp);
        return C_ERR;
    }

    /* Parse the file. Note that single lines of the cluster config file can
     * be really long as they include all the hash slots of the node.
     * This means in the worst possible case, half of the Redis slots will be
     * present in a single line, possibly in importing or migrating state, so
     * together with the node ID of the sender/receiver.
     *
     * To simplify we allocate 1024+CLUSTER_SLOTS*128 bytes per line.
     * 每一行数据的最大长度
     * */
    maxline = 1024 + CLUSTER_SLOTS * 128;
    line = zmalloc(maxline);
    // 从文件中按行读取数据
    while (fgets(line, maxline, fp) != NULL) {
        int argc;
        sds *argv;
        clusterNode *n, *master;
        char *p, *s;

        /* Skip blank lines, they can be created either by users manually
         * editing nodes.conf or by the config writing process if stopped
         * before the truncate() call.
         * 跳过空行
         * */
        if (line[0] == '\n' || line[0] == '\0') continue;

        /* Split the line into arguments for processing. */
        argv = sdssplitargs(line, &argc);
        if (argv == NULL) goto fmterr;

        /* Handle the special "vars" line. Don't pretend it is the last
         * line even if it actually is when generated by Redis.
         * 获取变量信息 记录了当前任期 以及上一次任期选择的节点
         * 这行记录是由redis自己产生的
         * */
        if (strcasecmp(argv[0], "vars") == 0) {
            if (!(argc % 2)) goto fmterr;
            for (j = 1; j < argc; j += 2) {
                if (strcasecmp(argv[j], "currentEpoch") == 0) {
                    server.cluster->currentEpoch =
                            strtoull(argv[j + 1], NULL, 10);
                } else if (strcasecmp(argv[j], "lastVoteEpoch") == 0) {
                    server.cluster->lastVoteEpoch =
                            strtoull(argv[j + 1], NULL, 10);
                } else {
                    serverLog(LL_WARNING,
                              "Skipping unknown cluster config variable '%s'",
                              argv[j]);
                }
            }
            sdsfreesplitres(argv, argc);
            continue;
        }

        /* Regular config lines have at least eight fields
         * 除了"vars"外 其余参数长度不小于8
         * */
        if (argc < 8) {
            sdsfreesplitres(argv, argc);
            goto fmterr;
        }

        // 接下来读取到的就是集群中之前发现的所有节点  动态发现么?还是写死在配置中
        /* Create this node if it does not exist */
        n = clusterLookupNode(argv[0]);
        if (!n) {
            // 此时在内存中还没有该节点的数据 进行数据恢复
            n = createClusterNode(argv[0], 0);
            // 将节点插入到cluster.nodes
            clusterAddNode(n);
        }
        /* Address and port */
        if ((p = strrchr(argv[1], ':')) == NULL) {
            sdsfreesplitres(argv, argc);
            goto fmterr;
        }
        // 这里把":"替换成'\0' 这样字符串就自动被拆分成 ip port 了
        *p = '\0';
        memcpy(n->ip, argv[1], strlen(argv[1]) + 1);
        char *port = p + 1;
        char *busp = strchr(port, '@');
        if (busp) {
            *busp = '\0';
            busp++;
        }
        n->port = atoi(port);
        /* In older versions of nodes.conf the "@busport" part is missing.
         * In this case we set it to the default offset of 10000 from the
         * base port.
         * 仅针对集群内节点访问的端口
         * */
        n->cport = busp ? atoi(busp) : n->port + CLUSTER_PORT_INCR;

        /* Parse flags
         * 解析节点的一些其他信息
         * */
        p = s = argv[2];
        while (p) {
            // 之后的数据是由多个 , 拼接的 这里每当解析到一个, 就转换成参数信息
            p = strchr(s, ',');
            if (p) *p = '\0';
            if (!strcasecmp(s, "myself")) {
                serverAssert(server.cluster->myself == NULL);
                myself = server.cluster->myself = n;
                n->flags |= CLUSTER_NODE_MYSELF;
            } else if (!strcasecmp(s, "master")) {
                n->flags |= CLUSTER_NODE_MASTER;
            } else if (!strcasecmp(s, "slave")) {
                n->flags |= CLUSTER_NODE_SLAVE;
            } else if (!strcasecmp(s, "fail?")) {
                n->flags |= CLUSTER_NODE_PFAIL;
            } else if (!strcasecmp(s, "fail")) {
                n->flags |= CLUSTER_NODE_FAIL;
                n->fail_time = mstime();
            } else if (!strcasecmp(s, "handshake")) {
                n->flags |= CLUSTER_NODE_HANDSHAKE;
            } else if (!strcasecmp(s, "noaddr")) {
                n->flags |= CLUSTER_NODE_NOADDR;
            } else if (!strcasecmp(s, "nofailover")) {
                n->flags |= CLUSTER_NODE_NOFAILOVER;
            } else if (!strcasecmp(s, "noflags")) {
                /* nothing to do */
            } else {
                serverPanic("Unknown flag in redis cluster config file");
            }
            if (p) s = p + 1;
        }

        /* Get master if any. Set the master and populate master's
         * slave list.
         * 每个节点可能会记录此时认可的master节点 在某一时刻 他们认可的节点可能会不一样
         * */
        if (argv[3][0] != '-') {
            master = clusterLookupNode(argv[3]);
            if (!master) {
                master = createClusterNode(argv[3], 0);
                clusterAddNode(master);
            }
            n->slaveof = master;
            clusterNodeAddSlave(master, n);
        }

        /* Set ping sent / pong received timestamps */
        if (atoi(argv[4])) n->ping_sent = mstime();
        if (atoi(argv[5])) n->pong_received = mstime();

        /* Set configEpoch for this node. */
        n->configEpoch = strtoull(argv[6], NULL, 10);

        /* Populate hash slots served by this instance.
         * 还原路由表信息 包括一些数据的迁移方向
         * */
        for (j = 8; j < argc; j++) {
            int start, stop;

            // 解析数据迁移信息
            if (argv[j][0] == '[') {
                /* Here we handle migrating / importing slots */
                int slot;
                char direction;
                clusterNode *cn;

                p = strchr(argv[j], '-');
                serverAssert(p != NULL);
                *p = '\0';
                // 代表数据迁移的方向
                direction = p[1]; /* Either '>' or '<' */
                // 代表本次是第几个slot的数据发生了迁移
                slot = atoi(argv[j] + 1);
                if (slot < 0 || slot >= CLUSTER_SLOTS) {
                    sdsfreesplitres(argv, argc);
                    goto fmterr;
                }

                // 找到数据迁移的目标节点
                p += 3;
                cn = clusterLookupNode(p);
                if (!cn) {
                    cn = createClusterNode(p, 0);
                    clusterAddNode(cn);
                }

                // 根据方向信息来填充 migrating_slots_to/importing_slots_from
                if (direction == '>') {
                    server.cluster->migrating_slots_to[slot] = cn;
                } else {
                    server.cluster->importing_slots_from[slot] = cn;
                }
                continue;
                // 记录了分配到该node的所有slot 这里每次解析到都是一个范围 可能可以写入多个范围
            } else if ((p = strchr(argv[j], '-')) != NULL) {
                *p = '\0';
                start = atoi(argv[j]);
                stop = atoi(p + 1);
            } else {
                start = stop = atoi(argv[j]);
            }
            if (start < 0 || start >= CLUSTER_SLOTS ||
                stop < 0 || stop >= CLUSTER_SLOTS) {
                sdsfreesplitres(argv, argc);
                goto fmterr;
            }
            // 更新slot所属的node信息
            while (start <= stop) clusterAddSlot(n, start++);
        }

        sdsfreesplitres(argv, argc);
    }
    /* Config sanity check
     * 配置文件中至少要确定哪个是本节点
     * */
    if (server.cluster->myself == NULL) goto fmterr;

    zfree(line);
    fclose(fp);

    serverLog(LL_NOTICE, "Node configuration loaded, I'm %.40s", myself->name);

    /* Something that should never happen: currentEpoch smaller than
     * the max epoch found in the nodes configuration. However we handle this
     * as some form of protection against manual editing of critical files.
     * 当发现此时加载的集群中所有节点最大的任期 超过了redis之前记录的任期(如果之前没记录 默认是0) 更新maxEpoch
     * */
    if (clusterGetMaxEpoch() > server.cluster->currentEpoch) {
        server.cluster->currentEpoch = clusterGetMaxEpoch();
    }
    return C_OK;

    fmterr:
    serverLog(LL_WARNING,
              "Unrecoverable error: corrupted cluster config file.");
    zfree(line);
    if (fp) fclose(fp);
    exit(1);
}

/* Cluster node configuration is exactly the same as CLUSTER NODES output.
 *
 * This function writes the node config and returns 0, on error -1
 * is returned.
 *
 * Note: we need to write the file in an atomic way from the point of view
 * of the POSIX filesystem semantics, so that if the server is stopped
 * or crashes during the write, we'll end with either the old file or the
 * new one. Since we have the full payload to write available we can use
 * a single write to write the whole file. If the pre-existing file was
 * bigger we pad our payload with newlines that are anyway ignored and truncate
 * the file afterward.
 * @param do_fsync 是否刷盘
 * 基于当前集群状态 保存集群配置文件
 * */
int clusterSaveConfig(int do_fsync) {
    sds ci;
    size_t content_size;
    struct stat sb;
    int fd;

    // todo_before_sleep 记录了每轮循环要做的事 这里清理掉saveConfig 并开始存储配置项
    server.cluster->todo_before_sleep &= ~CLUSTER_TODO_SAVE_CONFIG;

    /* Get the nodes description and concatenate our "vars" directive to
     * save currentEpoch and lastVoteEpoch.
     * 生成config的内容体 针对此时已经完全连接上的节点
     * */
    ci = clusterGenNodesDescription(CLUSTER_NODE_HANDSHAKE);
    ci = sdscatprintf(ci, "vars currentEpoch %llu lastVoteEpoch %llu\n",
                      (unsigned long long) server.cluster->currentEpoch,
                      (unsigned long long) server.cluster->lastVoteEpoch);
    content_size = sdslen(ci);

    if ((fd = open(server.cluster_configfile, O_WRONLY | O_CREAT, 0644))
        == -1)
        goto err;

    /* Pad the new payload if the existing file length is greater. */
    if (fstat(fd, &sb) != -1) {
        if (sb.st_size > (off_t) content_size) {
            ci = sdsgrowzero(ci, sb.st_size);
            memset(ci + content_size, '\n', sb.st_size - content_size);
        }
    }
    if (write(fd, ci, sdslen(ci)) != (ssize_t) sdslen(ci)) goto err;

    // 如果本次执行了刷盘工作 那么在todo_before_sleep中也可以移除fsync标记 代表本轮已经执行过刷盘逻辑了
    if (do_fsync) {
        server.cluster->todo_before_sleep &= ~CLUSTER_TODO_FSYNC_CONFIG;
        fsync(fd);
    }

    /* Truncate the file if needed to remove the final \n padding that
     * is just garbage. */
    if (content_size != sdslen(ci) && ftruncate(fd, content_size) == -1) {
        /* ftruncate() failing is not a critical error. */
    }
    close(fd);
    sdsfree(ci);
    return 0;

    err:
    if (fd != -1) close(fd);
    sdsfree(ci);
    return -1;
}

/**
 * 当发现集群配置文件未初始化时 创建 或者在关闭前更新
 * @param do_fsync
 */
void clusterSaveConfigOrDie(int do_fsync) {
    if (clusterSaveConfig(do_fsync) == -1) {
        serverLog(LL_WARNING, "Fatal: can't update cluster config file.");
        exit(1);
    }
}

/* Lock the cluster config using flock(), and leaks the file descriptor used to
 * acquire the lock so that the file will be locked forever.
 *
 * This works because we always update nodes.conf with a new version
 * in-place, reopening the file, and writing to it in place (later adjusting
 * the length with ftruncate()).
 *
 * On success C_OK is returned, otherwise an error is logged and
 * the function returns C_ERR to signal a lock was not acquired.
 * 对某个集群配置文件上锁
 * */
int clusterLockConfig(char *filename) {
/* flock() does not exist on Solaris
 * and a fcntl-based solution won't help, as we constantly re-open that file,
 * which will release _all_ locks anyway
 */
#if !defined(__sun)
    /* To lock it, we need to open the file in a way it is created if
     * it does not exist, otherwise there is a race condition with other
     * processes. */
    int fd = open(filename, O_WRONLY | O_CREAT, 0644);
    if (fd == -1) {
        serverLog(LL_WARNING,
                  "Can't open %s in order to acquire a lock: %s",
                  filename, strerror(errno));
        return C_ERR;
    }

    // 主要就是调用内核函数 flock
    if (flock(fd, LOCK_EX | LOCK_NB) == -1) {
        if (errno == EWOULDBLOCK) {
            serverLog(LL_WARNING,
                      "Sorry, the cluster configuration file %s is already used "
                      "by a different Redis Cluster node. Please make sure that "
                      "different nodes use different cluster configuration "
                      "files.", filename);
        } else {
            serverLog(LL_WARNING,
                      "Impossible to lock %s: %s", filename, strerror(errno));
        }
        close(fd);
        return C_ERR;
    }
    /* Lock acquired: leak the 'fd' by not closing it, so that we'll retain the
     * lock to the file as long as the process exists.
     *
     * After fork, the child process will get the fd opened by the parent process,
     * we need save `fd` to `cluster_config_file_lock_fd`, so that in redisFork(),
     * it will be closed in the child process.
     * If it is not closed, when the main process is killed -9, but the child process
     * (redis-aof-rewrite) is still alive, the fd(lock) will still be held by the
     * child process, and the main process will fail to get lock, means fail to start.
     * 返回配置文件句柄
     * */
    server.cluster_config_file_lock_fd = fd;
#endif /* __sun */

    return C_OK;
}

/* Some flags (currently just the NOFAILOVER flag) may need to be updated
 * in the "myself" node based on the current configuration of the node,
 * that may change at runtime via CONFIG SET. This function changes the
 * set of flags in myself->flags accordingly. */
void clusterUpdateMyselfFlags(void) {
    int oldflags = myself->flags;
    int nofailover = server.cluster_slave_no_failover ?
                     CLUSTER_NODE_NOFAILOVER : 0;
    myself->flags &= ~CLUSTER_NODE_NOFAILOVER;
    myself->flags |= nofailover;
    if (myself->flags != oldflags) {
        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                             CLUSTER_TODO_UPDATE_STATE);
    }
}

/**
 * 在redisServer启动时 如果发现本次在集群模式下启动 会初始化cluster
 */
void clusterInit(void) {
    int saveconf = 0;

    server.cluster = zmalloc(sizeof(clusterState));
    // 在初始阶段 还无法设置myself
    server.cluster->myself = NULL;
    // 集群中每次选举出一个master时 epoch就会更新 该值从0开始更新
    server.cluster->currentEpoch = 0;
    // 在初始阶段 集群还处于无法工作的状态
    server.cluster->state = CLUSTER_FAIL;
    // 在未探测到集群中的其他节点时 认为集群中仅存在一个节点
    server.cluster->size = 1;
    // 标记在事件循环中是否要处理集群的某些事件
    server.cluster->todo_before_sleep = 0;
    server.cluster->nodes = dictCreate(&clusterNodesDictType, NULL);

    // 这里可以为集群设置一个黑名单  黑名单内的节点无法连接到集群
    server.cluster->nodes_black_list =
            dictCreate(&clusterNodesBlackListDictType, NULL);
    // 一些默认属性的设置
    server.cluster->failover_auth_time = 0;
    server.cluster->failover_auth_count = 0;
    server.cluster->failover_auth_rank = 0;
    server.cluster->failover_auth_epoch = 0;
    server.cluster->cant_failover_reason = CLUSTER_CANT_FAILOVER_NONE;
    server.cluster->lastVoteEpoch = 0;
    // 统计每种消息出现的次数
    for (int i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
        server.cluster->stats_bus_messages_sent[i] = 0;
        server.cluster->stats_bus_messages_received[i] = 0;
    }
    server.cluster->stats_pfail_nodes = 0;
    // redis集群存在一个路由表，不同的key会被设置到不同的slots中，而每个主从负责存储一定数量slot的数据
    memset(server.cluster->slots, 0, sizeof(server.cluster->slots));
    // 清空有关数据迁移的信息
    clusterCloseAllSlots();

    /* Lock the cluster config file to make sure every node uses
     * its own nodes.conf.
     * 这里要对集群配置文件上锁
     * */
    server.cluster_config_file_lock_fd = -1;
    // 对配置文件上锁失败时 退出redis
    if (clusterLockConfig(server.cluster_configfile) == C_ERR)
        exit(1);

    /* Load or create a new nodes configuration.
     * 读取配置信息  如果之前没有任何的配置文件 那么在启动后会自动加入到集群 并在之后保存配置文件信息
     * */
    if (clusterLoadConfig(server.cluster_configfile) == C_ERR) {
        /* No configuration found. We will just use the random name provided
         * by the createClusterNode() function.
         * 集群配置无效 选择自动生成
         * 此时认为自己是master节点
         * */
        myself = server.cluster->myself =
                createClusterNode(NULL, CLUSTER_NODE_MYSELF | CLUSTER_NODE_MASTER);
        serverLog(LL_NOTICE, "No cluster configuration found, I'm %.40s",
                  myself->name);
        clusterAddNode(myself);
        saveconf = 1;
    }

    // 创建配置文件 默认是直接刷盘
    if (saveconf) clusterSaveConfigOrDie(1);

    /* We need a listening TCP port for our cluster messaging needs.
     * */
    server.cfd_count = 0;

    /* Port sanity check II
     * The other handshake port check is triggered too late to stop
     * us from trying to use a too-high cluster port number. */
    int port = server.tls_cluster ? server.tls_port : server.port;
    if (port > (65535 - CLUSTER_PORT_INCR)) {
        serverLog(LL_WARNING, "Redis port number too high. "
                              "Cluster communication port is 10,000 port "
                              "numbers higher than your Redis port. "
                              "Your Redis port number must be "
                              "lower than 55535.");
        exit(1);
    }
    // 服务器端口在增加一个特殊值后 会变成一个集群的监听端口  也就是cport  生成的socket句柄会设置到cfd上
    if (listenToPort(port + CLUSTER_PORT_INCR,
                     server.cfd, &server.cfd_count) == C_ERR) {
        exit(1);
    } else {
        int j;
        // 每个在集群中的节点都会对外开放集群的特殊端口 在集群内的子节点会连接到master上
        for (j = 0; j < server.cfd_count; j++) {
            if (aeCreateFileEvent(server.el, server.cfd[j], AE_READABLE,
                                  clusterAcceptHandler, NULL) == AE_ERR)
                serverPanic("Unrecoverable error creating Redis Cluster "
                            "file event.");
        }
    }

    /* The slots -> keys map is a radix tree. Initialize it here.
     * 每个slot中会存在大量的key 并且当用户从外部访问redisCluster 查询某个key时 就会通过slots_to_keys 对应到具体的slot 之后再定位到具体的节点 并在master节点处做转发
     * 因为key可能会有大量的前缀重复 并且他还会修改 不像fst一旦固定就无法修改 所以使用rax结构
     * */
    server.cluster->slots_to_keys = raxNew();
    memset(server.cluster->slots_keys_count, 0,
           sizeof(server.cluster->slots_keys_count));

    /* Set myself->port / cport to my listening ports, we'll just need to
     * discover the IP address via MEET messages. */
    myself->port = port;
    myself->cport = port + CLUSTER_PORT_INCR;

    // 如果有为集群指定特殊端口 设置成这些端口
    if (server.cluster_announce_port)
        myself->port = server.cluster_announce_port;
    if (server.cluster_announce_bus_port)
        myself->cport = server.cluster_announce_bus_port;

    // mf的含义是手动故障转移  这里将mf_end重置成0
    server.cluster->mf_end = 0;
    // 重置手动故障转移相关的属性
    resetManualFailover();
    // myself进行自我检测 当发现属性发生了变化 就修改flags 并标记需要进行cluster_config的更新
    clusterUpdateMyselfFlags();
}

/* Reset a node performing a soft or hard reset:
 *
 * 1) All other nodes are forgotten.
 * 2) All the assigned / open slots are released.
 * 3) If the node is a slave, it turns into a master.
 * 4) Only for hard reset: a new Node ID is generated.
 * 5) Only for hard reset: currentEpoch and configEpoch are set to 0.
 * 6) The new configuration is saved and the cluster state updated.
 * 7) If the node was a slave, the whole data set is flushed away.
 * 重置某个节点
 * @param hard 软重置/硬重置
 * */
void clusterReset(int hard) {
    dictIterator *di;
    dictEntry *de;
    int j;

    /* Turn into master.
     * 如果当前节点是slave 重置后它会变成master节点
     * */
    if (nodeIsSlave(myself)) {
        clusterSetNodeAsMaster(myself);
        // 因为master节点发生了更新 清理replica之前的master
        replicationUnsetMaster();
        // 清空该节点下所有db的数据
        emptyDb(-1, EMPTYDB_NO_FLAGS, NULL);
    }

    /* Close slots, reset manual failover state.
     * 清理所有有关slot数据迁移的记录
     * */
    clusterCloseAllSlots();
    // 将故障转移相关的参数也重置
    resetManualFailover();

    /* Unassign all the slots.
     * 释放该节点下所有slot的分配信息
     * */
    for (j = 0; j < CLUSTER_SLOTS; j++) clusterDelSlot(j);

    /* Forget all the nodes, but myself.
     * 清空cluster->nodes 自己除外
     * */
    di = dictGetSafeIterator(server.cluster->nodes);
    while ((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (node == myself) continue;
        clusterDelNode(node);
    }
    dictReleaseIterator(di);

    /* Hard reset only: set epochs to 0, change node ID.
     * 如果是硬重置 连同epoch和nodename 一起重置
     * */
    if (hard) {
        sds oldname;

        server.cluster->currentEpoch = 0;
        server.cluster->lastVoteEpoch = 0;
        myself->configEpoch = 0;
        serverLog(LL_WARNING, "configEpoch set to 0 via CLUSTER RESET HARD");

        /* To change the Node ID we need to remove the old name from the
         * nodes table, change the ID, and re-add back with new name.
         * 为本节点生成新的 node->name
         * */
        oldname = sdsnewlen(myself->name, CLUSTER_NAMELEN);
        dictDelete(server.cluster->nodes, oldname);
        sdsfree(oldname);
        getRandomHexChars(myself->name, CLUSTER_NAMELEN);
        clusterAddNode(myself);
        serverLog(LL_NOTICE, "Node hard reset, now I'm %.40s", myself->name);
    }

    /* Make sure to persist the new config and update the state. */
    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                         CLUSTER_TODO_UPDATE_STATE |
                         CLUSTER_TODO_FSYNC_CONFIG);
}

/* -----------------------------------------------------------------------------
 * CLUSTER communication link
 * -------------------------------------------------------------------------- */

/**
 * 基于当前node构建一个clusterLink结构
 * @param node
 * @return
 */
clusterLink *createClusterLink(clusterNode *node) {
    clusterLink *link = zmalloc(sizeof(*link));
    link->ctime = mstime();
    link->sndbuf = sdsempty();
    link->rcvbuf = zmalloc(link->rcvbuf_alloc = RCVBUF_INIT_LEN);
    link->rcvbuf_len = 0;
    link->node = node;
    link->conn = NULL;
    return link;
}

/* Free a cluster link, but does not free the associated node of course.
 * This function will just make sure that the original node associated
 * with this link will have the 'link' field set to NULL.
 * 释放link对象
 * */
void freeClusterLink(clusterLink *link) {
    if (link->conn) {
        connClose(link->conn);
        link->conn = NULL;
    }
    sdsfree(link->sndbuf);
    zfree(link->rcvbuf);
    if (link->node)
        link->node->link = NULL;
    zfree(link);
}

/**
 * 当本节点接收到其他节点的连接后，会创建conn并执行该方法
 * @param conn
 */
static void clusterConnAcceptHandler(connection *conn) {
    clusterLink *link;

    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        serverLog(LL_VERBOSE,
                  "Error accepting cluster node connection: %s", connGetLastError(conn));
        connClose(conn);
        return;
    }

    /* Create a link object we use to handle the connection.
     * It gets passed to the readable handler when data is available.
     * Initially the link->node pointer is set to NULL as we don't know
     * which node is, but the right node is references once we know the
     * node identity.
     * 被动创建的连接是不会关联node信息的
     * */
    link = createClusterLink(NULL);
    link->conn = conn;
    connSetPrivateData(conn, link);

    /* Register read handler
     * 为该连接设置readHandler
     * */
    connSetReadHandler(conn, clusterReadHandler);
}

#define MAX_CLUSTER_ACCEPTS_PER_CALL 1000

/**
 * 集群中每个节点 在cron中 会根据cluster->nodes 记录的信息与其他节点建立连结  这是目标接收到连接请求时触发的函数 发起连接的节点在连接成功后也会触发相关handler
 * @param el
 * @param fd
 * @param privdata
 * @param mask
 */
void clusterAcceptHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    int cport, cfd;
    int max = MAX_CLUSTER_ACCEPTS_PER_CALL;
    char cip[NET_IP_STR_LEN];
    UNUSED(el);
    UNUSED(mask);
    UNUSED(privdata);

    /* If the server is starting up, don't accept cluster connections:
     * UPDATE messages may interact with the database content.
     * 在数据恢复阶段无法接收其他节点的连接
     * */
    if (server.masterhost == NULL && server.loading) return;

    // 每轮最多只接收MAX_CLUSTER_ACCEPTS_PER_CALL数量的连接 先假设集群本身不大，可以忽略这个条件
    while (max--) {
        // 将读取到的数据转换成对端socket句柄
        cfd = anetTcpAccept(server.neterr, fd, cip, sizeof(cip), &cport);
        if (cfd == ANET_ERR) {
            if (errno != EWOULDBLOCK)
                serverLog(LL_VERBOSE,
                          "Error accepting cluster node: %s", server.neterr);
            return;
        }

        // 将句柄包装成conn对象
        connection *conn = server.tls_cluster ?
                           connCreateAcceptedTLS(cfd, TLS_CLIENT_AUTH_YES) : connCreateAcceptedSocket(cfd);

        /* Make sure connection is not in an error state */
        if (connGetState(conn) != CONN_STATE_ACCEPTING) {
            serverLog(LL_VERBOSE,
                      "Error creating an accepting connection for cluster node: %s",
                      connGetLastError(conn));
            connClose(conn);
            return;
        }
        // 对连接本身进行配置
        connNonBlock(conn);
        connEnableTcpNoDelay(conn);

        /* Use non-blocking I/O for cluster messages. */
        serverLog(LL_VERBOSE, "Accepting cluster node connection from %s:%d", cip, cport);

        /* Accept the connection now.  connAccept() may call our handler directly
         * or schedule it for later depending on connection implementation.
         * 将conn对象标记成已经接收连接 并执行clusterConnAcceptHandler
         */
        if (connAccept(conn, clusterConnAcceptHandler) == C_ERR) {
            if (connGetState(conn) == CONN_STATE_ERROR)
                serverLog(LL_VERBOSE,
                          "Error accepting cluster node connection: %s",
                          connGetLastError(conn));
            connClose(conn);
            return;
        }
    }
}

/* Return the approximated number of sockets we are using in order to
 * take the cluster bus connections.
 * 集群内总计有多少client
 * */
unsigned long getClusterConnectionsCount(void) {
    /* We decrement the number of nodes by one, since there is the
     * "myself" node too in the list. Each node uses two file descriptors,
     * one incoming and one outgoing, thus the multiplication by 2. */
    return server.cluster_enabled ?
           ((dictSize(server.cluster->nodes) - 1) * 2) : 0;
}

/* -----------------------------------------------------------------------------
 * Key space handling
 * -------------------------------------------------------------------------- */

/* We have 16384 hash slots. The hash slot of a given key is obtained
 * as the least significant 14 bits of the crc16 of the key.
 *
 * However if the key contains the {...} pattern, only the part between
 * { and } is hashed. This may be useful in the future to force certain
 * keys to be in the same node (assuming no resharding is in progress).
 * 将key转换成slot的下标
 * */
unsigned int keyHashSlot(char *key, int keylen) {
    int s, e; /* start-end indexes of { and } */

    for (s = 0; s < keylen; s++)
        if (key[s] == '{') break;

    /* No '{' ? Hash the whole key. This is the base case. */
    if (s == keylen) return crc16(key, keylen) & 0x3FFF;

    /* '{' found? Check if we have the corresponding '}'. */
    for (e = s + 1; e < keylen; e++)
        if (key[e] == '}') break;

    /* No '}' or nothing between {} ? Hash the whole key. */
    if (e == keylen || e == s + 1) return crc16(key, keylen) & 0x3FFF;

    /* If we are here there is both a { and a } on its right. Hash
     * what is in the middle between { and }. */
    return crc16(key + s + 1, e - s - 1) & 0x3FFF;
}

/* -----------------------------------------------------------------------------
 * CLUSTER node API
 * -------------------------------------------------------------------------- */

/* Create a new cluster node, with the specified flags.
 * If "nodename" is NULL this is considered a first handshake and a random
 * node name is assigned to this node (it will be fixed later when we'll
 * receive the first pong).
 *
 * The node is created and returned to the user, but it is not automatically
 * added to the nodes hash table.
 * 通过节点名称和属性 初始化某个新节点
 * */
clusterNode *createClusterNode(char *nodename, int flags) {
    clusterNode *node = zmalloc(sizeof(*node));

    if (nodename)
        memcpy(node->name, nodename, CLUSTER_NAMELEN);
    else
        getRandomHexChars(node->name, CLUSTER_NAMELEN);
    node->ctime = mstime();
    node->configEpoch = 0;
    node->flags = flags;
    memset(node->slots, 0, sizeof(node->slots));
    node->numslots = 0;
    node->numslaves = 0;
    node->slaves = NULL;
    node->slaveof = NULL;
    node->ping_sent = node->pong_received = 0;
    node->data_received = 0;
    node->fail_time = 0;
    node->link = NULL;
    memset(node->ip, 0, sizeof(node->ip));
    node->port = 0;
    node->cport = 0;
    node->fail_reports = listCreate();
    node->voted_time = 0;
    node->orphaned_time = 0;
    node->repl_offset_time = 0;
    node->repl_offset = 0;
    listSetFreeMethod(node->fail_reports, zfree);
    return node;
}

/* This function is called every time we get a failure report from a node.
 * The side effect is to populate the fail_reports list (or to update
 * the timestamp of an existing report).
 *
 * 'failing' is the node that is in failure state according to the
 * 'sender' node.
 *
 * The function returns 0 if it just updates a timestamp of an existing
 * failure report from the same sender. 1 is returned if a new failure
 * report is created.
 * 由sender告知本节点  某个节点此时处于无效状态
 * */
int clusterNodeAddFailureReport(clusterNode *failing, clusterNode *sender) {
    list *l = failing->fail_reports;
    listNode *ln;
    listIter li;
    clusterNodeFailReport *fr;

    /* If a failure report from the same sender already exists, just update
     * the timestamp. */
    listRewind(l, &li);
    // 代表该节点的失败情况 已经被sender节点报告过
    while ((ln = listNext(&li)) != NULL) {
        fr = ln->value;
        if (fr->node == sender) {
            fr->time = mstime();
            return 0;
        }
    }

    /* Otherwise create a new report. */
    fr = zmalloc(sizeof(*fr));
    fr->node = sender;
    fr->time = mstime();
    listAddNodeTail(l, fr);
    return 1;
}

/* Remove failure reports that are too old, where too old means reasonably
 * older than the global node timeout. Note that anyway for a node to be
 * flagged as FAIL we need to have a local PFAIL state that is at least
 * older than the global node timeout, so we don't just trust the number
 * of failure reports from other nodes.
 * 可能会收到一些通知该节点已经下线的报告 但是如果报告比较旧 是不作数的  惰性清理
 * */
void clusterNodeCleanupFailureReports(clusterNode *node) {
    list *l = node->fail_reports;
    listNode *ln;
    listIter li;
    clusterNodeFailReport *fr;
    mstime_t maxtime = server.cluster_node_timeout *
                       CLUSTER_FAIL_REPORT_VALIDITY_MULT;
    mstime_t now = mstime();

    listRewind(l, &li);
    while ((ln = listNext(&li)) != NULL) {
        fr = ln->value;
        // 代表距离上一次报告的时间间隔 超过了最大时间 从fail_reports中移除
        if (now - fr->time > maxtime) listDelNode(l, ln);
    }
}

/* Remove the failing report for 'node' if it was previously considered
 * failing by 'sender'. This function is called when a node informs us via
 * gossip that a node is OK from its point of view (no FAIL or PFAIL flags).
 *
 * Note that this function is called relatively often as it gets called even
 * when there are no nodes failing, and is O(N), however when the cluster is
 * fine the failure reports list is empty so the function runs in constant
 * time.
 *
 * The function returns 1 if the failure report was found and removed.
 * Otherwise 0 is returned.
 * 清除node下有关sender的错误报告信息 此时该节点可能重新上线 也可能从集群中移除
 * */
int clusterNodeDelFailureReport(clusterNode *node, clusterNode *sender) {
    // 如果sender 与node->fail_reports中的某个节点匹配  移除，否则不需要处理
    list *l = node->fail_reports;
    listNode *ln;
    listIter li;
    clusterNodeFailReport *fr;

    /* Search for a failure report from this sender. */
    listRewind(l, &li);

    while ((ln = listNext(&li)) != NULL) {
        fr = ln->value;
        if (fr->node == sender) break;
    }
    // 在 fail_reports中没有发现该节点 不需要处理
    if (!ln) return 0; /* No failure report from this sender. */

    /* Remove the failure report.
     * 因为这个节点此时已经被处理了 所以不需要保留失败的报告信息
     * */
    listDelNode(l, ln);
    // 找到一些已经过期的数据 进行清理
    clusterNodeCleanupFailureReports(node);
    return 1;
}

/* Return the number of external nodes that believe 'node' is failing,
 * not including this node, that may have a PFAIL or FAIL state for this
 * node as well.
 * 此时集群中有多少节点报告该节点已经下线
 * */
int clusterNodeFailureReportsCount(clusterNode *node) {
    // 先尝试清理一下太早的报告 那些报告可能会不准确
    clusterNodeCleanupFailureReports(node);
    return listLength(node->fail_reports);
}

/**
 * 将某个slave从master下的slave列表中移除
 * @param master
 * @param slave
 * @return
 */
int clusterNodeRemoveSlave(clusterNode *master, clusterNode *slave) {
    int j;

    for (j = 0; j < master->numslaves; j++) {
        if (master->slaves[j] == slave) {
            if ((j + 1) < master->numslaves) {
                int remaining_slaves = (master->numslaves - j) - 1;
                memmove(master->slaves + j, master->slaves + (j + 1),
                        (sizeof(*master->slaves) * remaining_slaves));
            }
            master->numslaves--;
            // 当master下没有任何slave了 清理该标记
            if (master->numslaves == 0)
                master->flags &= ~CLUSTER_NODE_MIGRATE_TO;
            return C_OK;
        }
    }
    return C_ERR;
}

/**
 * 将某个slave节点插入到master的slaves中
 * @param master
 * @param slave
 * @return
 */
int clusterNodeAddSlave(clusterNode *master, clusterNode *slave) {
    int j;

    /* If it's already a slave, don't add it again. */
    for (j = 0; j < master->numslaves; j++)
        if (master->slaves[j] == slave) return C_ERR;
    master->slaves = zrealloc(master->slaves,
                              sizeof(clusterNode *) * (master->numslaves + 1));
    master->slaves[master->numslaves] = slave;
    master->numslaves++;
    // 只要master下至少有一个slave 就会设置这个标记
    master->flags |= CLUSTER_NODE_MIGRATE_TO;
    return C_OK;
}

/**
 * 检测该节点下是否有有效的slave节点
 * @param n
 * @return
 */
int clusterCountNonFailingSlaves(clusterNode *n) {
    int j, okslaves = 0;

    for (j = 0; j < n->numslaves; j++)
        if (!nodeFailed(n->slaves[j])) okslaves++;
    return okslaves;
}

/* Low level cleanup of the node structure. Only called by clusterDelNode().
 * 清理该节点 主要是做内存释放工作
 * */
void freeClusterNode(clusterNode *n) {
    sds nodename;
    int j;

    /* If the node has associated slaves, we have to set
     * all the slaves->slaveof fields to NULL (unknown).
     * 在集群的master节点上已经记录了树状的节点图了么 本次要删除的slave如果下面还有子级的slave 将他们的slaveof置空
     * */
    for (j = 0; j < n->numslaves; j++)
        n->slaves[j]->slaveof = NULL;

    /* Remove this node from the list of slaves of its master.
     * 将该节点从它跟从的master节点下移除
     * */
    if (nodeIsSlave(n) && n->slaveof) clusterNodeRemoveSlave(n->slaveof, n);

    /* Unlink from the set of nodes. */
    nodename = sdsnewlen(n->name, CLUSTER_NAMELEN);
    // 从cluster->nodes中移除
    serverAssert(dictDelete(server.cluster->nodes, nodename) == DICT_OK);
    sdsfree(nodename);

    /* Release link and associated data structures.
     * 下面就是一些内存释放
     * */
    if (n->link) freeClusterLink(n->link);
    listRelease(n->fail_reports);
    zfree(n->slaves);
    zfree(n);
}

/* Add a node to the nodes hash table
 * 将节点加入到cluster->nodes中
 * */
int clusterAddNode(clusterNode *node) {
    int retval;

    retval = dictAdd(server.cluster->nodes,
                     sdsnewlen(node->name, CLUSTER_NAMELEN), node);
    return (retval == DICT_OK) ? C_OK : C_ERR;
}

/* Remove a node from the cluster. The function performs the high level
 * cleanup, calling freeClusterNode() for the low level cleanup.
 * Here we do the following:
 *
 * 1) Mark all the slots handled by it as unassigned.
 * 2) Remove all the failure reports sent by this node and referenced by
 *    other nodes.
 * 3) Free the node with freeClusterNode() that will in turn remove it
 *    from the hash table and from the list of slaves of its master, if
 *    it is a slave node.
 *    将该节点从集群中移除
 */
void clusterDelNode(clusterNode *delnode) {
    int j;
    dictIterator *di;
    dictEntry *de;

    /* 1) Mark slots as unassigned.
     * 该节点关联的某个slot 作为数据迁移的目的地 那么slot需要被清理
     * */
    for (j = 0; j < CLUSTER_SLOTS; j++) {
        if (server.cluster->importing_slots_from[j] == delnode)
            server.cluster->importing_slots_from[j] = NULL;
        if (server.cluster->migrating_slots_to[j] == delnode)
            server.cluster->migrating_slots_to[j] = NULL;
        if (server.cluster->slots[j] == delnode)
            clusterDelSlot(j);
    }

    /* 2) Remove failure reports.
     * 因为本节点已经确定会从集群中移除了 就不需要其他节点有关该节点的错误信息报告了 */
    di = dictGetSafeIterator(server.cluster->nodes);
    while ((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (node == delnode) continue;
        clusterNodeDelFailureReport(node, delnode);
    }
    dictReleaseIterator(di);

    /* 3) Free the node, unlinking it from the cluster.
     * 将该节点从集群中移除
     * */
    freeClusterNode(delnode);
}

/* Node lookup by name
 * 通过节点名称 查找集群中的节点
 * */
clusterNode *clusterLookupNode(const char *name) {
    sds s = sdsnewlen(name, CLUSTER_NAMELEN);
    dictEntry *de;

    de = dictFind(server.cluster->nodes, s);
    sdsfree(s);
    if (de == NULL) return NULL;
    return dictGetVal(de);
}

/* This is only used after the handshake. When we connect a given IP/PORT
 * as a result of CLUSTER MEET we don't have the node name yet, so we
 * pick a random one, and will fix it when we receive the PONG request using
 * this function.
 * 更新节点名字 并更新在cluster->nodes中的映射关系
 * */
void clusterRenameNode(clusterNode *node, char *newname) {
    int retval;
    sds s = sdsnewlen(node->name, CLUSTER_NAMELEN);

    serverLog(LL_DEBUG, "Renaming node %.40s into %.40s",
              node->name, newname);
    retval = dictDelete(server.cluster->nodes, s);
    sdsfree(s);
    serverAssert(retval == DICT_OK);
    memcpy(node->name, newname, CLUSTER_NAMELEN);
    clusterAddNode(node);
}

/* -----------------------------------------------------------------------------
 * CLUSTER config epoch handling
 * -------------------------------------------------------------------------- */

/* Return the greatest configEpoch found in the cluster, or the current
 * epoch if greater than any node configEpoch.
 * 获取集群中最大节点的任期
 * */
uint64_t clusterGetMaxEpoch(void) {
    uint64_t max = 0;
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(server.cluster->nodes);
    while ((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);
        if (node->configEpoch > max) max = node->configEpoch;
    }
    dictReleaseIterator(di);
    if (max < server.cluster->currentEpoch) max = server.cluster->currentEpoch;
    return max;
}

/* If this node epoch is zero or is not already the greatest across the
 * cluster (from the POV of the local configuration), this function will:
 *
 * 1) Generate a new config epoch, incrementing the current epoch.
 * 2) Assign the new epoch to this node, WITHOUT any consensus.
 * 3) Persist the configuration on disk before sending packets with the
 *    new configuration.
 *
 * If the new config epoch is generated and assigned, C_OK is returned,
 * otherwise C_ERR is returned (since the node has already the greatest
 * configuration around) and no operation is performed.
 *
 * Important note: this function violates the principle that config epochs
 * should be generated with consensus and should be unique across the cluster.
 * However Redis Cluster uses this auto-generated new config epochs in two
 * cases:
 *
 * 1) When slots are closed after importing. Otherwise resharding would be
 *    too expensive.
 * 2) When CLUSTER FAILOVER is called with options that force a slave to
 *    failover its master even if there is not master majority able to
 *    create a new configuration epoch.
 *
 * Redis Cluster will not explode using this function, even in the case of
 * a collision between this node and another node, generating the same
 * configuration epoch unilaterally, because the config epoch conflict
 * resolution algorithm will eventually move colliding nodes to different
 * config epochs. However using this function may violate the "last failover
 * wins" rule, so should only be used with care.
 * 当集群中epoch发生了碰撞 通过该方法进行处理
 * */
int clusterBumpConfigEpochWithoutConsensus(void) {
    // 获取本节点记录的集群中最大的epoch
    uint64_t maxEpoch = clusterGetMaxEpoch();

    // 只要本节点与预期值不一致 就会增加集群epoch
    if (myself->configEpoch == 0 ||
        myself->configEpoch != maxEpoch) {
        server.cluster->currentEpoch++;
        myself->configEpoch = server.cluster->currentEpoch;
        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                             CLUSTER_TODO_FSYNC_CONFIG);
        serverLog(LL_WARNING,
                  "New configEpoch set to %llu",
                  (unsigned long long) myself->configEpoch);
        return C_OK;
    } else {
        return C_ERR;
    }
}

/* This function is called when this node is a master, and we receive from
 * another master a configuration epoch that is equal to our configuration
 * epoch.
 *
 * BACKGROUND
 *
 * It is not possible that different slaves get the same config
 * epoch during a failover election, because the slaves need to get voted
 * by a majority. However when we perform a manual resharding of the cluster
 * the node will assign a configuration epoch to itself without to ask
 * for agreement. Usually resharding happens when the cluster is working well
 * and is supervised by the sysadmin, however it is possible for a failover
 * to happen exactly while the node we are resharding a slot to assigns itself
 * a new configuration epoch, but before it is able to propagate it.
 *
 * So technically it is possible in this condition that two nodes end with
 * the same configuration epoch.
 *
 * Another possibility is that there are bugs in the implementation causing
 * this to happen.
 *
 * Moreover when a new cluster is created, all the nodes start with the same
 * configEpoch. This collision resolution code allows nodes to automatically
 * end with a different configEpoch at startup automatically.
 *
 * In all the cases, we want a mechanism that resolves this issue automatically
 * as a safeguard. The same configuration epoch for masters serving different
 * set of slots is not harmful, but it is if the nodes end serving the same
 * slots for some reason (manual errors or software bugs) without a proper
 * failover procedure.
 *
 * In general we want a system that eventually always ends with different
 * masters having different configuration epochs whatever happened, since
 * nothing is worse than a split-brain condition in a distributed system.
 *
 * BEHAVIOR
 *
 * When this function gets called, what happens is that if this node
 * has the lexicographically smaller Node ID compared to the other node
 * with the conflicting epoch (the 'sender' node), it will assign itself
 * the greatest configuration epoch currently detected among nodes plus 1.
 *
 * This means that even if there are multiple nodes colliding, the node
 * with the greatest Node ID never moves forward, so eventually all the nodes
 * end with a different configuration epoch.
 * 不同的主从结构间epoch不能一致 这里+1
 */
void clusterHandleConfigEpochCollision(clusterNode *sender) {
    /* Prerequisites: nodes have the same configEpoch and are both masters.
     * 判断是否满足碰撞条件
     * */
    if (sender->configEpoch != myself->configEpoch ||
        !nodeIsMaster(sender) || !nodeIsMaster(myself))
        return;
    /* Don't act if the colliding node has a smaller Node ID.
     * 因为这个处理本身是双向的 2个冲突的master都会执行该方法 nodeName大的所在的主从结构epoch都会大
     * */
    if (memcmp(sender->name, myself->name, CLUSTER_NAMELEN) <= 0) return;
    /* Get the next ID available at the best of this node knowledge.
     * 强制升级epoch 并更新配置文件
     * */
    server.cluster->currentEpoch++;
    myself->configEpoch = server.cluster->currentEpoch;
    clusterSaveConfigOrDie(1);
    serverLog(LL_VERBOSE,
              "WARNING: configEpoch collision with node %.40s."
              " configEpoch set to %llu",
              sender->name,
              (unsigned long long) myself->configEpoch);
}

/* -----------------------------------------------------------------------------
 * CLUSTER nodes blacklist
 *
 * The nodes blacklist is just a way to ensure that a given node with a given
 * Node ID is not readded before some time elapsed (this time is specified
 * in seconds in CLUSTER_BLACKLIST_TTL).
 *
 * This is useful when we want to remove a node from the cluster completely:
 * when CLUSTER FORGET is called, it also puts the node into the blacklist so
 * that even if we receive gossip messages from other nodes that still remember
 * about the node we want to remove, we don't re-add it before some time.
 *
 * Currently the CLUSTER_BLACKLIST_TTL is set to 1 minute, this means
 * that redis-trib has 60 seconds to send CLUSTER FORGET messages to nodes
 * in the cluster without dealing with the problem of other nodes re-adding
 * back the node to nodes we already sent the FORGET command to.
 *
 * The data structure used is a hash table with an sds string representing
 * the node ID as key, and the time when it is ok to re-add the node as
 * value.
 * -------------------------------------------------------------------------- */

#define CLUSTER_BLACKLIST_TTL 60      /* 1 minute. */


/* Before of the addNode() or Exists() operations we always remove expired
 * entries from the black list. This is an O(N) operation but it is not a
 * problem since add / exists operations are called very infrequently and
 * the hash table is supposed to contain very little elements at max.
 * However without the cleanup during long uptime and with some automated
 * node add/removal procedures, entries could accumulate.
 * 黑名单内的数据本身有一个时限
 * */
void clusterBlacklistCleanup(void) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(server.cluster->nodes_black_list);
    while ((de = dictNext(di)) != NULL) {
        int64_t expire = dictGetUnsignedIntegerVal(de);

        if (expire < server.unixtime)
            dictDelete(server.cluster->nodes_black_list, dictGetKey(de));
    }
    dictReleaseIterator(di);
}

/* Cleanup the blacklist and add a new node ID to the black list.
 * 将某个节点加入到黑名单中
 * */
void clusterBlacklistAddNode(clusterNode *node) {
    dictEntry *de;
    sds id = sdsnewlen(node->name, CLUSTER_NAMELEN);

    // 黑名单中的节点有一个时限 这里是惰性检测
    clusterBlacklistCleanup();
    if (dictAdd(server.cluster->nodes_black_list, id, NULL) == DICT_OK) {
        /* If the key was added, duplicate the sds string representation of
         * the key for the next lookup. We'll free it at the end. */
        id = sdsdup(id);
    }
    de = dictFind(server.cluster->nodes_black_list, id);
    dictSetUnsignedIntegerVal(de, time(NULL) + CLUSTER_BLACKLIST_TTL);
    sdsfree(id);
}

/* Return non-zero if the specified node ID exists in the blacklist.
 * You don't need to pass an sds string here, any pointer to 40 bytes
 * will work.
 * 判断某个节点是否在黑名单中  主要是发现新的node想要连接到redis集群时 需要做一层拦截
 * */
int clusterBlacklistExists(char *nodeid) {
    sds id = sdsnewlen(nodeid, CLUSTER_NAMELEN);
    int retval;

    clusterBlacklistCleanup();
    retval = dictFind(server.cluster->nodes_black_list, id) != NULL;
    sdsfree(id);
    return retval;
}

/* -----------------------------------------------------------------------------
 * CLUSTER messages exchange - PING/PONG and gossip
 * -------------------------------------------------------------------------- */

/* This function checks if a given node should be marked as FAIL.
 * It happens if the following conditions are met:
 *
 * 1) We received enough failure reports from other master nodes via gossip.
 *    Enough means that the majority of the masters signaled the node is
 *    down recently.
 * 2) We believe this node is in PFAIL state.
 *
 * If a failure is detected we also inform the whole cluster about this
 * event trying to force every other node to set the FAIL flag for the node.
 *
 * Note that the form of agreement used here is weak, as we collect the majority
 * of masters state during some time, and even if we force agreement by
 * propagating the FAIL message, because of partitions we may not reach every
 * node. However:
 *
 * 1) Either we reach the majority and eventually the FAIL state will propagate
 *    to all the cluster.
 * 2) Or there is no majority so no slave promotion will be authorized and the
 *    FAIL flag will be cleared after some time.
 *    判断是否要将该节点修改成失败节点
 */
void markNodeAsFailingIfNeeded(clusterNode *node) {
    int failures;
    // 需要多少节点选择才认为该节点真的下线
    int needed_quorum = (server.cluster->size / 2) + 1;

    // 只要本节点还能访问到 至少本节点不会发出该节点下线的通知
    if (!nodeTimedOut(node)) return; /* We can reach it. */
    // 如果本节点已经被标记成失败了 不需要重复处理
    if (nodeFailed(node)) return; /* Already FAILing. */

    // 这时node的状态必然是pfail

    // 此时有多少节点认为node已经下线
    failures = clusterNodeFailureReportsCount(node);
    /* Also count myself as a voter if I'm a master.
     * 本节点作为master才有决策权
     * */
    if (nodeIsMaster(myself)) failures++;
    // 票数不够,认为node还未下线
    if (failures < needed_quorum) return; /* No weak agreement from masters. */

    serverLog(LL_NOTICE,
              "Marking node %.40s as failing (quorum reached).", node->name);

    /* Mark the node as failing. 此时获得了足够的票数 可以将该节点标记成下线了 清理半下线的标记 */
    node->flags &= ~CLUSTER_NODE_PFAIL;
    node->flags |= CLUSTER_NODE_FAIL;
    node->fail_time = mstime();

    /* Broadcast the failing node name to everybody, forcing all the other
     * reachable nodes to flag the node as FAIL.
     * We do that even if this node is a replica and not a master: anyway
     * the failing state is triggered collecting failure reports from masters,
     * so here the replica is only helping propagating this status.
     * 将该节点失败的信息广播到集群
     * */
    clusterSendFail(node->name);
    clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE | CLUSTER_TODO_SAVE_CONFIG);
}

/* This function is called only if a node is marked as FAIL, but we are able
 * to reach it again. It checks if there are the conditions to undo the FAIL
 * state.
 * 清理节点的fail标记
 * */
void clearNodeFailureIfNeeded(clusterNode *node) {
    mstime_t now = mstime();

    serverAssert(nodeFailed(node));

    /* For slaves we always clear the FAIL flag if we can contact the
     * node again.
     * 当本节点是slave或者下面的slot数量为0，情况比较简单。直接清理fail标记就好
     * */
    if (nodeIsSlave(node) || node->numslots == 0) {
        serverLog(LL_NOTICE,
                  "Clear FAIL state for node %.40s: %s is reachable again.",
                  node->name,
                  nodeIsSlave(node) ? "replica" : "master without slots");
        node->flags &= ~CLUSTER_NODE_FAIL;
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE | CLUSTER_TODO_SAVE_CONFIG);
    }

    /* If it is a master and...
     * 1) The FAIL state is old enough.
     * 2) It is yet serving slots from our point of view (not failed over).
     * Apparently no one is going to fix these slots, clear the FAIL flag.
     * 首先本节点是一个有效的master节点 也就是有分配到slot
     * 其次要求此时距离fail的判定时间超过了一个限制值  同时满足这2个条件才会清理fail标记
     * */
    if (nodeIsMaster(node) && node->numslots > 0 &&
        (now - node->fail_time) >
        (server.cluster_node_timeout * CLUSTER_FAIL_UNDO_TIME_MULT)) {
        serverLog(LL_NOTICE,
                  "Clear FAIL state for node %.40s: is reachable again and nobody is serving its slots after some time.",
                  node->name);
        node->flags &= ~CLUSTER_NODE_FAIL;
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE | CLUSTER_TODO_SAVE_CONFIG);
    }
}

/* Return true if we already have a node in HANDSHAKE state matching the
 * specified ip address and port number. This function is used in order to
 * avoid adding a new handshake node for the same address multiple times.
 * 是否已经存在该节点
 * */
int clusterHandshakeInProgress(char *ip, int port, int cport) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(server.cluster->nodes);
    while ((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (!nodeInHandshake(node)) continue;
        if (!strcasecmp(node->ip, ip) &&
            node->port == port &&
            node->cport == cport)
            break;
    }
    dictReleaseIterator(di);
    return de != NULL;
}

/* Start a handshake with the specified address if there is not one
 * already in progress. Returns non-zero if the handshake was actually
 * started. On error zero is returned and errno is set to one of the
 * following values:
 *
 * EAGAIN - There is already a handshake in progress for this address.
 * EINVAL - IP or port are not valid.
 * 外部client通过调用clusterCommand命令 往集群中插入一个新的节点
 * */
int clusterStartHandshake(char *ip, int port, int cport) {
    clusterNode *n;
    char norm_ip[NET_IP_STR_LEN];
    struct sockaddr_storage sa;

    /* IP sanity check */
    if (inet_pton(AF_INET, ip,
                  &(((struct sockaddr_in *) &sa)->sin_addr))) {
        sa.ss_family = AF_INET;
    } else if (inet_pton(AF_INET6, ip,
                         &(((struct sockaddr_in6 *) &sa)->sin6_addr))) {
        sa.ss_family = AF_INET6;
    } else {
        errno = EINVAL;
        return 0;
    }

    /* Port sanity check */
    if (port <= 0 || port > 65535 || cport <= 0 || cport > 65535) {
        errno = EINVAL;
        return 0;
    }

    /* Set norm_ip as the normalized string representation of the node
     * IP address. */
    memset(norm_ip, 0, NET_IP_STR_LEN);
    if (sa.ss_family == AF_INET)
        inet_ntop(AF_INET,
                  (void *) &(((struct sockaddr_in *) &sa)->sin_addr),
                  norm_ip, NET_IP_STR_LEN);
    else
        inet_ntop(AF_INET6,
                  (void *) &(((struct sockaddr_in6 *) &sa)->sin6_addr),
                  norm_ip, NET_IP_STR_LEN);

    // 检测集群中是否已经存在该节点
    if (clusterHandshakeInProgress(norm_ip, port, cport)) {
        errno = EAGAIN;
        return 0;
    }

    /* Add the node with a random address (NULL as first argument to
     * createClusterNode()). Everything will be fixed during the
     * handshake.
     * 创建的节点包含handshake标记  注意这个时候节点的名称是未知的
     * meet的作用实际上是与ping区分开 对端在接收到meet请求后会针对发送方创建一个没有名称 处于handshake状态的节点。之后探测发送节点并处理pong消息，回填nodename
     * 而ping消息本身不具备这个功能
     * */
    n = createClusterNode(NULL, CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_MEET);
    memcpy(n->ip, norm_ip, sizeof(n->ip));
    n->port = port;
    n->cport = cport;
    clusterAddNode(n);
    return 1;
}

/* Process the gossip section of PING or PONG packets.
 * Note that this function assumes that the packet is already sanity-checked
 * by the caller, not in the content of the gossip section, but in the
 * length.
 * 集群内节点每次交互信息时 都会携带一些其他节点的信息 (采用抽样的方式选择这些节点) 通过解析这些信息判断某个节点是否下线 该节点信息是否需要更新 该节点是否想要加入到集群中
 * */
void clusterProcessGossipSection(clusterMsg *hdr, clusterLink *link) {
    // 本次采集到了多少个节点的数据
    uint16_t count = ntohs(hdr->count);
    clusterMsgDataGossip *g = (clusterMsgDataGossip *) hdr->data.ping.gossip;
    // 如果本节点作为一个ping请求的接收者 是有可能找不到sender的 首先link->node为null 其次sender刚好没有加入到集群中 那么他发现的所有节点也无法加入到集群
    clusterNode *sender = link->node ? link->node : clusterLookupNode(hdr->sender);

    // 挨个处理每个gossip数据
    while (count--) {
        uint16_t flags = ntohs(g->flags);
        clusterNode *node;
        sds ci;

        // 忽略DEBUG信息
        if (server.verbosity == LL_DEBUG) {
            ci = representClusterNodeFlags(sdsempty(), flags);
            serverLog(LL_DEBUG, "GOSSIP %.40s %s:%d@%d %s",
                      g->nodename,
                      g->ip,
                      ntohs(g->port),
                      ntohs(g->cport),
                      ci);
            sdsfree(ci);
        }

        /* Update our state accordingly to the gossip sections
         * 查看本节点是否在集群中
         * */
        node = clusterLookupNode(g->nodename);
        // 当本地存在该节点信息时 就是判断是否需要更新
        if (node) {
            /* We already know this node.
               Handle failure reports, only when the sender is a master.
               只有当sender是master时 可信度才是高的
               */
            if (sender && nodeIsMaster(sender) && node != myself) {
                // 对端认为该节点可能离线
                if (flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) {
                    // 将sender的报告信息存储起来
                    if (clusterNodeAddFailureReport(node, sender)) {
                        serverLog(LL_VERBOSE,
                                  "Node %.40s reported node %.40s as not reachable.",
                                  sender->name, node->name);
                    }
                    // 通过此时采集到的所有报告信息判断本节点是否真的下线
                    markNodeAsFailingIfNeeded(node);
                } else {
                    // sender此时认为该节点在线， 将对应的失败报告清除
                    if (clusterNodeDelFailureReport(node, sender)) {
                        serverLog(LL_VERBOSE,
                                  "Node %.40s reported node %.40s is back online.",
                                  sender->name, node->name);
                    }
                }
            }

            /* If from our POV the node is up (no failure flags are set),
             * we have no pending ping for the node, nor we have failure
             * reports for this node, update the last pong time with the
             * one we see from the other nodes.
             * 当前节点是可靠的 同时还没有发送ping请求 同时还没有节点报告该节点不可用 这样的话可以信任本次gossip数据
             * */
            if (!(flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) &&
                node->ping_sent == 0 &&
                clusterNodeFailureReportsCount(node) == 0) {
                // g->pong_received 就是sender节点上次收到该节点pong数据的时间戳 这里信任这份数据 直接使用 就省去了发送ping请求的功夫
                mstime_t pongtime = ntohl(g->pong_received);
                pongtime *= 1000; /* Convert back to milliseconds. */

                /* Replace the pong time with the received one only if
                 * it's greater than our view but is not in the future
                 * (with 500 milliseconds tolerance) from the POV of our
                 * clock.
                 * */
                if (pongtime <= (server.mstime + 500) &&
                    pongtime > node->pong_received) {
                    node->pong_received = pongtime;
                }
            }

            /* If we already know this node, but it is not reachable, and
             * we see a different address in the gossip section of a node that
             * can talk with this other node, update the address, disconnect
             * the old link if any, so that we'll attempt to connect with the
             * new address.
             * 同一个nodename 对应的节点的地址发生了变化 释放原link 之后会在clusterCron中重新创建连接
             * */
            if (node->flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL) &&
                !(flags & CLUSTER_NODE_NOADDR) &&
                !(flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) &&
                (strcasecmp(node->ip, g->ip) ||
                 node->port != ntohs(g->port) ||
                 node->cport != ntohs(g->cport))) {
                if (node->link) freeClusterLink(node->link);
                memcpy(node->ip, g->ip, NET_IP_STR_LEN);
                node->port = ntohs(g->port);
                node->cport = ntohs(g->cport);
                node->flags &= ~CLUSTER_NODE_NOADDR;
            }
        } else {
            // 通过nodeName无法在本地找到node数据  此时节点会加入到本地缓存的集群中 前提是sender节点存在 也就是发送请求的节点可靠

            /* If it's not in NOADDR state and we don't have it, we
             * add it to our trusted dict with exact nodeid and flag.
             * Note that we cannot simply start a handshake against
             * this IP/PORT pairs, since IP/PORT can be reused already,
             * otherwise we risk joining another cluster.
             *
             * Note that we require that the sender of this gossip message
             * is a well known node in our cluster, otherwise we risk
             * joining another cluster.
             * */
            if (sender &&
                !(flags & CLUSTER_NODE_NOADDR) &&
                !clusterBlacklistExists(g->nodename)) {
                clusterNode *node;
                node = createClusterNode(g->nodename, flags);
                memcpy(node->ip, g->ip, NET_IP_STR_LEN);
                node->port = ntohs(g->port);
                node->cport = ntohs(g->cport);
                clusterAddNode(node);
            }
        }

        /* Next node */
        g++;
    }
}

/* IP -> string conversion. 'buf' is supposed to at least be 46 bytes.
 * If 'announced_ip' length is non-zero, it is used instead of extracting
 * the IP from the socket peer address.
 * 将ip转换成string
 * */
void nodeIp2String(char *buf, clusterLink *link, char *announced_ip) {
    if (announced_ip[0] != '\0') {
        memcpy(buf, announced_ip, NET_IP_STR_LEN);
        buf[NET_IP_STR_LEN - 1] = '\0'; /* We are not sure the input is sane. */
    } else {
        connPeerToString(link->conn, buf, NET_IP_STR_LEN, NULL);
    }
}

/* Update the node address to the IP address that can be extracted
 * from link->fd, or if hdr->myip is non empty, to the address the node
 * is announcing us. The port is taken from the packet header as well.
 *
 * If the address or port changed, disconnect the node link so that we'll
 * connect again to the new address.
 *
 * If the ip/port pair are already correct no operation is performed at
 * all.
 *
 * The function returns 0 if the node address is still the same,
 * otherwise 1 is returned.
 * 判断是否要修改该节点的ip地址
 * @param node 发送数据的节点
 * @param link 存储该node相关conn的对象
 * @param hdr 本次收到的数据
 * */
int nodeUpdateAddressIfNeeded(clusterNode *node, clusterLink *link,
                              clusterMsg *hdr) {
    char ip[NET_IP_STR_LEN] = {0};
    int port = ntohs(hdr->port);
    int cport = ntohs(hdr->cport);

    /* We don't proceed if the link is the same as the sender link, as this
     * function is designed to see if the node link is consistent with the
     * symmetric link that is used to receive PINGs from the node.
     *
     * As a side effect this function never frees the passed 'link', so
     * it is safe to call during packet processing.
     * 如果2个link对象是一样的 不需要处理
     * */
    if (link == node->link) return 0;

    // 只有当目标节点设置了 cluster_announce_ip message中才会携带myip  这里是将myip的数据转移到 ip中
    nodeIp2String(ip, link, hdr->myip);
    // 本次数据没有发生变化 直接返回
    if (node->port == port && node->cport == cport &&
        strcmp(ip, node->ip) == 0)
        return 0;

    /* IP / port is different, update it.
     * 更改原节点的ip信息
     * */
    memcpy(node->ip, ip, sizeof(ip));
    node->port = port;
    node->cport = cport;

    // 之后在cron中 会重新创建link 并发起ping请求
    if (node->link) freeClusterLink(node->link);
    node->flags &= ~CLUSTER_NODE_NOADDR;
    serverLog(LL_WARNING, "Address updated for node %.40s, now %s:%d",
              node->name, node->ip, node->port);

    /* Check if this is our master and we have to change the
     * replication target as well.
     * 如果本节点是slave节点 并且将该节点认定为master 更新本节点的master信息
     * */
    if (nodeIsSlave(myself) && myself->slaveof == node)
        replicationSetMaster(node->ip, node->port);
    return 1;
}

/* Reconfigure the specified node 'n' as a master. This function is called when
 * a node that we believed to be a slave is now acting as master in order to
 * update the state of the node.
 * 将指定的节点变成master
 * */
void clusterSetNodeAsMaster(clusterNode *n) {
    // 如果已经打上了master的标记 就不需要处理
    if (nodeIsMaster(n)) return;

    // 如果该节点之前将其他节点作为master 将本节点从之前master下管理的slave列表中移除
    if (n->slaveof) {
        clusterNodeRemoveSlave(n->slaveof, n);
        if (n != myself) n->flags |= CLUSTER_NODE_MIGRATE_TO;
    }
    // 移除slave标记 并设置master标记
    n->flags &= ~CLUSTER_NODE_SLAVE;
    n->flags |= CLUSTER_NODE_MASTER;
    n->slaveof = NULL;

    /* Update config and state.
     * 因为本节点维护的cluster信息中 某个节点信息发生了变化 所以要做保存配置文件的变化
     * */
    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                         CLUSTER_TODO_UPDATE_STATE);
}

/* This function is called when we receive a master configuration via a
 * PING, PONG or UPDATE packet. What we receive is a node, a configEpoch of the
 * node, and the set of slots claimed under this configEpoch.
 *
 * What we do is to rebind the slots with newer configuration compared to our
 * local configuration, and if needed, we turn ourself into a replica of the
 * node (see the function comments for more info).
 *
 * The 'sender' is the node for which we received a configuration update.
 * Sometimes it is not actually the "Sender" of the information, like in the
 * case we receive the info via an UPDATE packet.
 * 更新sender的slots sender必然是master节点
 * @param senderConfigEpoch 本次收到请求的节点对应的epoch
 * @param slots 本次接收到的有关master的slots信息
 * */
void clusterUpdateSlotsConfigWith(clusterNode *sender, uint64_t senderConfigEpoch, unsigned char *slots) {
    int j;
    clusterNode *curmaster, *newmaster = NULL;
    /* The dirty slots list is a list of slots for which we lose the ownership
     * while having still keys inside. This usually happens after a failover
     * or after a manual cluster reconfiguration operated by the admin.
     *
     * If the update message is not able to demote a master to slave (in this
     * case we'll resync with the master updating the whole key space), we
     * need to delete all the keys in the slots we lost ownership.
     * 本节点作为master节点 与本次收到的master节点的slots发生了冲突 并且sender节点的epoch更大(优先级更高) 那么所有让出的slot下标会记录在这个数组中
     * */
    uint16_t dirty_slots[CLUSTER_SLOTS];
    int dirty_slots_count = 0;

    /* Here we set curmaster to this node or the node this node
     * replicates to if it's a slave. In the for loop we are
     * interested to check if slots are taken away from curmaster.
     * 获取本节点此时的master
     * */
    curmaster = nodeIsMaster(myself) ? myself : myself->slaveof;

    // sender不能是自己
    if (sender == myself) {
        serverLog(LL_WARNING, "Discarding UPDATE message about myself.");
        return;
    }

    // 这里开始遍历每个slot
    for (j = 0; j < CLUSTER_SLOTS; j++) {
        // 查看本次接收到的节点的master 占有哪些slots 并且是否与本节点占有的冲突
        if (bitmapTestBit(slots, j)) {
            /* The slot is already bound to the sender of this message.
             * 本地记录的slot也是属于这个节点 就不需要处理了
             * */
            if (server.cluster->slots[j] == sender) continue;

            /* The slot is in importing state, it should be modified only
             * manually via redis-trib (example: a resharding is in progress
             * and the migrating side slot was already closed and is advertising
             * a new config. We still want the slot to be closed manually).
             * TODO 数据马上就要迁往这个slot，也不需要处理
             * */
            if (server.cluster->importing_slots_from[j]) continue;

            /* We rebind the slot to the new node claiming it if:
             * 1) The slot was unassigned or the new node claims it with a
             *    greater configEpoch.
             * 2) We are not currently importing the slot.
             * 如果当前slot还没有指定所在的节点 或者说这个数据信息已经比较旧了 将slot指向新的node
             * */
            if (server.cluster->slots[j] == NULL ||
                server.cluster->slots[j]->configEpoch < senderConfigEpoch) {
                /* Was this slot mine, and still contains keys? Mark it as
                 * a dirty slot.
                 * 如果这个slot之前被本节点占用(同时暗示了本节点是master)
                 * */
                if (server.cluster->slots[j] == myself &&
                    countKeysInSlot(j) &&
                    sender != myself) {
                    // 使用这个数组记录所有需要本节点出让的slot
                    dirty_slots[dirty_slots_count] = j;
                    dirty_slots_count++;
                }

                // 这里就是故障转移的场景了 自己原来的master所拥有的所有slot都被另一个节点占有 就是slave晋升接替之前masterslots的过程
                // 如果本节点就是master 就对应手动故障转移
                if (server.cluster->slots[j] == curmaster)
                    newmaster = sender;
                // 清理slot的所有者
                clusterDelSlot(j);
                // 更新slot的拥有者
                clusterAddSlot(sender, j);
                // 因为集群信息发生了变化 需要更新集群配置文件
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                                     CLUSTER_TODO_UPDATE_STATE |
                                     CLUSTER_TODO_FSYNC_CONFIG);
            }
        }
    }

    /* After updating the slots configuration, don't do any actual change
     * in the state of the server if a module disabled Redis Cluster
     * keys redirections.
     * 不支持重定向的情况下 没有设置的必要了
     * */
    if (server.cluster_module_flags & CLUSTER_MODULE_FLAG_NO_REDIRECTION)
        return;

    /* If at least one slot was reassigned from a node to another node
     * with a greater configEpoch, it is possible that:
     * 1) We are a master left without slots. This means that we were
     *    failed over and we should turn into a replica of the new
     *    master.
     * 2) We are a slave and our master is left without slots. We need
     *    to replicate to the new slots owner.
     *    代表当前节点的master此时没有任何slot了 它将变成sender的slave 对应故障转移
     *    */
    if (newmaster && curmaster->numslots == 0) {
        serverLog(LL_WARNING,
                  "Configuration change detected. Reconfiguring myself "
                  "as a replica of %.40s", sender->name);
        clusterSetMaster(sender);
        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                             CLUSTER_TODO_UPDATE_STATE |
                             CLUSTER_TODO_FSYNC_CONFIG);
        // 剩下的这些slot内的数据已经不在数据本节点对应的主从群了  将这部分数据从节点中删除
    } else if (dirty_slots_count) {
        /* If we are here, we received an update message which removed
         * ownership for certain slots we still have keys about, but still
         * we are serving some slots, so this master node was not demoted to
         * a slave.
         *
         * In order to maintain a consistent state between keys and slots
         * we need to remove all the keys from the slots we lost.
         * */
        for (j = 0; j < dirty_slots_count; j++)
            delKeysInSlot(dirty_slots[j]);
    }
}

/* When this function is called, there is a packet to process starting
 * at node->rcvbuf. Releasing the buffer is up to the caller, so this
 * function should just handle the higher level stuff of processing the
 * packet, modifying the cluster state if needed.
 *
 * The function returns 1 if the link is still valid after the packet
 * was processed, otherwise 0 if the link was freed since the packet
 * processing lead to some inconsistency error (for instance a PONG
 * received from the wrong sender ID).
 * readHandler 会解决粘包拆包的问题 这里就可以解析数据包了
 * */
int clusterProcessPacket(clusterLink *link) {
    // 此时整个数据包都存储在 rcvbuf 中
    clusterMsg *hdr = (clusterMsg *) link->rcvbuf;
    uint32_t totlen = ntohl(hdr->totlen);
    uint16_t type = ntohs(hdr->type);
    mstime_t now = mstime();

    // 增加统计数据
    if (type < CLUSTERMSG_TYPE_COUNT)
        server.cluster->stats_bus_messages_received[type]++;
    serverLog(LL_DEBUG, "--- Processing packet of type %d, %lu bytes",
              type, (unsigned long) totlen);

    /* Perform sanity checks */
    if (totlen < 16) return 1; /* At least signature, version, totlen, count. */
    if (totlen > link->rcvbuf_len) return 1;

    // 2个server的集群协议版本不一致 抛出异常
    if (ntohs(hdr->ver) != CLUSTER_PROTO_VER) {
        /* Can't handle messages of different versions. */
        return 1;
    }

    uint16_t flags = ntohs(hdr->flags);
    uint64_t senderCurrentEpoch = 0, senderConfigEpoch = 0;
    clusterNode *sender;

    // 下面主要是做一些数据长度校验
    // 这几种类型 会携带一些node的信息(抽样决定的)
    if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_PONG ||
        type == CLUSTERMSG_TYPE_MEET) {
        uint16_t count = ntohs(hdr->count);
        uint32_t explen; /* expected length of this packet */

        // 数据长度不匹配
        explen = sizeof(clusterMsg) - sizeof(union clusterMsgData);
        explen += (sizeof(clusterMsgDataGossip) * count);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_FAIL) {
        uint32_t explen = sizeof(clusterMsg) - sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgDataFail);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_PUBLISH) {
        uint32_t explen = sizeof(clusterMsg) - sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgDataPublish) -
                  8 +
                  ntohl(hdr->data.publish.msg.channel_len) +
                  ntohl(hdr->data.publish.msg.message_len);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST ||
               type == CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK ||
               type == CLUSTERMSG_TYPE_MFSTART) {
        uint32_t explen = sizeof(clusterMsg) - sizeof(union clusterMsgData);

        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_UPDATE) {
        uint32_t explen = sizeof(clusterMsg) - sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgDataUpdate);
        if (totlen != explen) return 1;
    } else if (type == CLUSTERMSG_TYPE_MODULE) {
        uint32_t explen = sizeof(clusterMsg) - sizeof(union clusterMsgData);

        explen += sizeof(clusterMsgModule) -
                  3 + ntohl(hdr->data.module.msg.len);
        if (totlen != explen) return 1;
    }

    /* Check if the sender is a known node. Note that for incoming connections
     * we don't store link->node information, but resolve the node by the
     * ID in the header each time in the current implementation.
     * 从本节点存储的集群信息中 找到发送数据的节点 比较该信息与消息体信息
     * */
    sender = clusterLookupNode(hdr->sender);

    /* Update the last time we saw any data from this node. We
     * use this in order to avoid detecting a timeout from a node that
     * is just sending a lot of data in the cluster bus, for instance
     * because of Pub/Sub.
     * 更新最近一次收到该节点数据的时间戳  如果长时间没有收到该节点的信息 可能就认为本节点已经脱离集群了。
     * */
    if (sender) sender->data_received = now;

    // 比较缓存的该节点的信息 与本次消息体的信息
    if (sender && !nodeInHandshake(sender)) {
        /* Update our currentEpoch if we see a newer epoch in the cluster. */
        senderCurrentEpoch = ntohu64(hdr->currentEpoch);
        senderConfigEpoch = ntohu64(hdr->configEpoch);
        // 通过发送心跳包的方式确保集群内的epoch同步
        if (senderCurrentEpoch > server.cluster->currentEpoch)
            server.cluster->currentEpoch = senderCurrentEpoch;
        /* Update the sender configEpoch if it is publishing a newer one. */
        if (senderConfigEpoch > sender->configEpoch) {
            sender->configEpoch = senderConfigEpoch;
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                                 CLUSTER_TODO_FSYNC_CONFIG);
        }
        /* Update the replication offset info for this node.
         * hdr->offset存储的是该节点观察到的此时master的同步偏移量 (如果该节点是slave节点，偏移量可能就是一个快照值)
         * */
        sender->repl_offset = ntohu64(hdr->offset);
        sender->repl_offset_time = now;
        /* If we are a slave performing a manual failover and our master
         * sent its offset while already paused, populate the MF state.
         * 这几个条件的意思是 此时集群正处于故障转移阶段 并且sender节点是本节点的master节点  并且在消息体上携带了paused标记
         * 并且此时故障转移的mf_master_offset为0 将它与此时sender的偏移量同步
         * */
        if (server.cluster->mf_end &&
            nodeIsSlave(myself) &&
            myself->slaveof == sender &&
            hdr->mflags[0] & CLUSTERMSG_FLAG0_PAUSED &&
            server.cluster->mf_master_offset == 0) {
            server.cluster->mf_master_offset = sender->repl_offset;
            serverLog(LL_WARNING,
                      "Received replication offset for paused "
                      "master manual failover: %lld",
                      server.cluster->mf_master_offset);
        }
    }

    /* Initial processing of PING and MEET requests replying with a PONG.
     * 当本次收到的是一个心跳包时 或者是一个meet包  meet包是用户执行clusterCommand插入的
     */
    if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_MEET) {
        serverLog(LL_DEBUG, "Ping packet received: %p", (void *) link->node);

        /* We use incoming MEET messages in order to set the address
         * for 'myself', since only other cluster nodes will send us
         * MEET messages on handshakes, when the cluster joins, or
         * later if we changed address, and those nodes will use our
         * official address to connect to us. So by obtaining this address
         * from the socket is a simple way to discover / update our own
         * address in the cluster without it being hardcoded in the config.
         *
         * However if we don't have an address at all, we update the address
         * even with a normal PING packet. If it's wrong it will be fixed
         * by MEET later.
         * 如果本节点为设置自己的ip 在完成连接后 读取本节点ip
         * */
        if ((type == CLUSTERMSG_TYPE_MEET || myself->ip[0] == '\0') &&
            server.cluster_announce_ip == NULL) {
            char ip[NET_IP_STR_LEN];

            if (connSockName(link->conn, ip, sizeof(ip), NULL) != -1 &&
                strcmp(ip, myself->ip)) {
                memcpy(myself->ip, ip, NET_IP_STR_LEN);
                serverLog(LL_WARNING, "IP address for this node updated to %s",
                          myself->ip);
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
            }
        }

        /* Add this node if it is new for us and the msg type is MEET.
         * In this stage we don't try to add the node with the right
         * flags, slaveof pointer, and so forth, as this details will be
         * resolved when we'll receive PONGs from the node.
         * 如果发送者没有被记录在本地 但是本次是一个meet命令 就为sender创建node并保存在本地
         * */
        if (!sender && type == CLUSTERMSG_TYPE_MEET) {
            clusterNode *node;

            // 被动创建的节点是CLUSTER_NODE_HANDSHAKE  注意这里没有设置nodename  之后该节点会主动发起心跳请求 因为对端在接收到之前的pong时已经记录了nodename
            // 所以收到下次的ping请求可以匹配到对应节点 并返回pong 那段修改name的逻辑就可以fuyong6
            node = createClusterNode(NULL, CLUSTER_NODE_HANDSHAKE);
            nodeIp2String(node->ip, link, hdr->myip);
            node->port = ntohs(hdr->port);
            node->cport = ntohs(hdr->cport);
            clusterAddNode(node);
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
        }

        /* If this is a MEET packet from an unknown node, we still process
         * the gossip section here since we have to trust the sender because
         * of the message type.
         * 尝试处理携带的其他节点的信息
         * */
        if (!sender && type == CLUSTERMSG_TYPE_MEET)
            clusterProcessGossipSection(hdr, link);

        /* Anyway reply with a PONG
         * 这里会回复一个pong消息  有些场景 比如故障转移选举出新的master时 会直接发出一个pong类型请求 意义就是不会要求接收端返回一个pong消息
         * */
        clusterSendPing(link, CLUSTERMSG_TYPE_PONG);
    }

    /* PING, PONG, MEET: process config information.
     * */
    if (type == CLUSTERMSG_TYPE_PING || type == CLUSTERMSG_TYPE_PONG ||
        type == CLUSTERMSG_TYPE_MEET) {
        serverLog(LL_DEBUG, "%s packet received: %p",
                  type == CLUSTERMSG_TYPE_PING ? "ping" : "pong",
                  (void *) link->node);

        // 只有用于发送ping请求的link会维护node信息 此时就是接收到pong
        if (link->node) {
            // handshake代表还没有确认节点名称的节点
            if (nodeInHandshake(link->node)) {
                /* If we already have this node, try to change the
                 * IP/port of the node with the new one.
                 * TODO
                 * */
                if (sender) {
                    serverLog(LL_VERBOSE,
                              "Handshake: we already know node %.40s, "
                              "updating the address if needed.", sender->name);
                    if (nodeUpdateAddressIfNeeded(sender, link, hdr)) {
                        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                                             CLUSTER_TODO_UPDATE_STATE);
                    }
                    /* Free this node as we already have it. This will
                     * cause the link to be freed as well.
                     * */
                    clusterDelNode(link->node);
                    return 0;
                }

                /* First thing to do is replacing the random name with the
                 * right node name if this was a handshake stage.
                 * 因为发送meet请求前还不知道node的名字 在收到pong结果后就可以知道名字了 这里修改后去除CLUSTER_NODE_HANDSHAKE标记
                 * */
                clusterRenameNode(link->node, hdr->sender);
                serverLog(LL_DEBUG, "Handshake with node %.40s completed.",
                          link->node->name);
                link->node->flags &= ~CLUSTER_NODE_HANDSHAKE;
                link->node->flags |= flags & (CLUSTER_NODE_MASTER | CLUSTER_NODE_SLAVE);
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                // 发现sender与node信息不匹配  也就是心跳检测发现错误的节点 将link释放 下次就不会往这个节点发送数据了
            } else if (memcmp(link->node->name, hdr->sender,
                              CLUSTER_NAMELEN) != 0) {
                /* If the reply has a non matching node ID we
                 * disconnect this node and set it as not having an associated
                 * address. */
                serverLog(LL_DEBUG,
                          "PONG contains mismatching sender ID. About node %.40s added %d ms ago, having flags %d",
                          link->node->name,
                          (int) (now - (link->node->ctime)),
                          link->node->flags);
                link->node->flags |= CLUSTER_NODE_NOADDR;
                link->node->ip[0] = '\0';
                link->node->port = 0;
                link->node->cport = 0;
                freeClusterLink(link);
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                return 0;
            }
        }

        /* Copy the CLUSTER_NODE_NOFAILOVER flag from what the sender
         * announced. This is a dynamic flag that we receive from the
         * sender, and the latest status must be trusted. We need it to
         * be propagated because the slave ranking used to understand the
         * delay of each slave in the voting process, needs to know
         * what are the instances really competing.
         * 更新本节点有关故障转移的信息
         * */
        if (sender) {
            int nofailover = flags & CLUSTER_NODE_NOFAILOVER;
            sender->flags &= ~CLUSTER_NODE_NOFAILOVER;
            sender->flags |= nofailover;
        }

        /* Update the node address if it changed.
         * 本次处理的是一个ping消息
         * */
        if (sender && type == CLUSTERMSG_TYPE_PING &&
            !nodeInHandshake(sender) &&
            // 检测该节点的地址信息是否发生了变化 并更新 这时会释放旧link 并在clusterCron中对新地址发起连接
            nodeUpdateAddressIfNeeded(sender, link, hdr)) {
            clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                                 CLUSTER_TODO_UPDATE_STATE);
        }

        /* Update our info about the node
         * 代表发送ping的节点收到了回应信息
         * */
        if (link->node && type == CLUSTERMSG_TYPE_PONG) {
            // ping_sent/pong_received 一次只需要设置一个值
            link->node->pong_received = now;
            link->node->ping_sent = 0;

            /* The PFAIL condition can be reversed without external
             * help if it is momentary (that is, if it does not
             * turn into a FAIL state).
             *
             * The FAIL condition is also reversible under specific
             * conditions detected by clearNodeFailureIfNeeded().
             * 如果之前长时间没有收到该节点的请求 会打上一个CLUSTER_NODE_PFAIL标记
             * 因为此时已经收到了心跳 所以可以移除这个标记了
             * */
            if (nodeTimedOut(link->node)) {
                link->node->flags &= ~CLUSTER_NODE_PFAIL;
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                                     CLUSTER_TODO_UPDATE_STATE);
                // 如果之前认为node已经是fail了 现在可以清理这个标记
            } else if (nodeFailed(link->node)) {
                clearNodeFailureIfNeeded(link->node);
            }
        }

        /* Check for role switch: slave -> master or master -> slave.
         * 判断该节点是否发生了角色的切换 比如从slave到master 或者从master降级成slave
         * 针对的场景就是故障转移 比如执行手动故障转移master节点在变成slave后 会将信息通过心跳的方式传播出去
         * */
        if (sender) {
            // 当该节点的master被设置成NULL 就代表sender节点已经变成了新的master
            if (!memcmp(hdr->slaveof, CLUSTER_NODE_NULL_NAME,
                        sizeof(hdr->slaveof))) {
                /* Node is a master.
                 * 将sender信息更新成master的
                 * */
                clusterSetNodeAsMaster(sender);
            } else {
                /* Node is a slave.
                 * hdr->slaveof 不为空 就代表sender已经变成了一个slave节点
                 * */
                clusterNode *master = clusterLookupNode(hdr->slaveof);

                // 之前sender节点在本节点是被当作master的  这里要处理降级逻辑
                if (nodeIsMaster(sender)) {
                    /* Master turned into a slave! Reconfigure the node.
                     * 先释放当前节点拥有的所有slot
                     * */
                    clusterDelNodeSlots(sender);
                    // 移除该节点的master标记 以及数据迁移标记 TODO CLUSTER_NODE_MIGRATE_TO 这是什么意思???
                    sender->flags &= ~(CLUSTER_NODE_MASTER |
                                       CLUSTER_NODE_MIGRATE_TO);
                    // 追加一个slave标记
                    sender->flags |= CLUSTER_NODE_SLAVE;

                    /* Update config and state. 因为节点角色变化 所以需要更新状态 */
                    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                                         CLUSTER_TODO_UPDATE_STATE);
                }

                /* Master node changed for this slave?
                 * 这里对应故障转移的2种情况 master降级成slave/slave选择了新的master
                 * 数据迁移也会这样 某个slave主动转移到一个孤儿master下
                 * */
                if (master && sender->slaveof != master) {
                    // 清理之前的关联关系 以及建立新的关联关系
                    if (sender->slaveof)
                        clusterNodeRemoveSlave(sender->slaveof, sender);
                    clusterNodeAddSlave(master, sender);
                    sender->slaveof = master;

                    /* Update config. */
                    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG);
                }
            }
        }

        /* Update our info about served slots.
         *
         * Note: this MUST happen after we update the master/slave state
         * so that CLUSTER_NODE_MASTER flag will be set. */

        // 上面处理完角色的变化后 现在是处理slot的变化  需要知道的一点就是当发生故障转移的时候slave节点会占用之前master的slots，并发出pong请求。
        // 在下面的判断中如果发现原本master占用的slot此时变成了别的节点，就会自动的加入到这个节点，就是完成了参与故障转移的其他slave加入新master的工作。

        /* Many checks are only needed if the set of served slots this
         * instance claims is different compared to the set of slots we have
         * for it. Check this ASAP to avoid other computational expansive
         * checks later.
         * */
        clusterNode *sender_master = NULL; /* Sender or its master if slave. */
        int dirty_slots = 0; /* Sender claimed slots don't match my view? */

        if (sender) {
            // 现在在故障转移的场景下理解一下这段代码。 sender如果刚刚晋升成master sender_master就是自己，而它的slots还是原来的(sender还作为slave时的slots，slave是不维护slots的，也就是没有slots信息)
            // 而sender节点在晋升后，会将之前master的slots转移给自己，他们的slots信息就必然不同
            sender_master = nodeIsMaster(sender) ? sender : sender->slaveof;
            if (sender_master) {
                dirty_slots = memcmp(sender_master->slots,
                                     hdr->myslots, sizeof(hdr->myslots)) != 0;
            }
        }

        /* 1) If the sender of the message is a master, and we detected that
         *    the set of slots it claims changed, scan the slots to see if we
         *    need to update our configuration.
         *    只有当master节点发送请求 并发现slots发生了变化 才会处理
         *    slave更换master不会进入该方法
         *    */
        if (sender && nodeIsMaster(sender) && dirty_slots)
            // 当发现此时本节点缓存的某个master的slots信息 与从该master节点发来的slots信息不一致时 解决冲突
            clusterUpdateSlotsConfigWith(sender, senderConfigEpoch, hdr->myslots);

        /* 2) We also check for the reverse condition, that is, the sender
         *    claims to serve slots we know are served by a master with a
         *    greater configEpoch. If this happens we inform the sender.
         *
         * This is useful because sometimes after a partition heals, a
         * reappearing master may be the last one to claim a given set of
         * hash slots, but with a configuration that other instances know to
         * be deprecated. Example:
         *
         * A and B are master and slave for slots 1,2,3.
         * A is partitioned away, B gets promoted.
         * B is partitioned away, and A returns available.
         *
         * Usually B would PING A publishing its set of served slots and its
         * configEpoch, but because of the partition B can't inform A of the
         * new configuration, so other nodes that have an updated table must
         * do it. In this way A will stop to act as a master (or can try to
         * failover if there are the conditions to win the election).
         * 此时无论sender是slave还是master都会执行下面的逻辑 而clusterUpdateSlotsConfigWith只有当sender是master时才会执行
         * */
        if (sender && dirty_slots) {
            int j;
            for (j = 0; j < CLUSTER_SLOTS; j++) {
                // 遍历本次sender占有的所有slot
                if (bitmapTestBit(hdr->myslots, j)) {
                    // 如果sender是slave节点会出现 server.cluster->slots[j] == NULL
                    // 如果sender已经是master节点了 会在clusterUpdateSlotsConfigWith中设置slots 所以不会出现这种情况
                    if (server.cluster->slots[j] == sender ||
                        server.cluster->slots[j] == NULL)
                        continue;

                    // 代表发送者本身的数据比较旧 这里只要发送差异化的数据就可以
                    if (server.cluster->slots[j]->configEpoch >
                        senderConfigEpoch) {
                        serverLog(LL_VERBOSE,
                                  "Node %.40s has old slots configuration, sending "
                                  "an UPDATE message about %.40s",
                                  sender->name, server.cluster->slots[j]->name);
                        // 这种情况代表了在sender节点上的 slot真正所属的节点的数据是旧的 它才会发出这种错误的信息。需要在sender节点上更新slot所属节点的信息
                        clusterSendUpdate(sender->link,
                                          server.cluster->slots[j]);

                        /* TODO: instead of exiting the loop send every other
                         * UPDATE packet for other nodes that are the new owner
                         * of sender's slots. */
                        break;
                    }
                }
            }
        }

        /* If our config epoch collides with the sender's try to fix
         * the problem.
         * 同一个主从结构下epoch需要保持一致 不同的主从结构间epoch 不能相同
         * */
        if (sender &&
            nodeIsMaster(myself) && nodeIsMaster(sender) &&
            senderConfigEpoch == myself->configEpoch) {
            clusterHandleConfigEpochCollision(sender);
        }

        /* Get info from the gossip section
         * 在发送数据包时 ping/pong/meet会携带一些其他节点的信息 这里就是处于剩余的数据
         * 主要就是判断节点是否下线 如果判断该节点下线了就会往其他节点发送fail数据
         * */
        if (sender) clusterProcessGossipSection(hdr, link);
        // 对应的情况就是某个节点被确定是下线了
    } else if (type == CLUSTERMSG_TYPE_FAIL) {
        clusterNode *failing;

        if (sender) {
            // 本节点是否知道那个下线节点的信息
            failing = clusterLookupNode(hdr->data.fail.about.nodename);
            // 要求那个节点此时还没有打上fail的标记 同时不是本节点才处理
            if (failing &&
                !(failing->flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_MYSELF))) {
                serverLog(LL_NOTICE,
                          "FAIL message received from %.40s about %.40s",
                          hdr->sender, hdr->data.fail.about.nodename);
                // 加上fail标记 以及更新下线时间
                failing->flags |= CLUSTER_NODE_FAIL;
                failing->fail_time = now;
                failing->flags &= ~CLUSTER_NODE_PFAIL;
                clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                                     CLUSTER_TODO_UPDATE_STATE);
            }
        } else {
            // 未知节点发送的数据直接忽略
            serverLog(LL_NOTICE,
                      "Ignoring FAIL message from unknown node %.40s about %.40s",
                      hdr->sender, hdr->data.fail.about.nodename);
        }
        // TODO 先忽略
    } else if (type == CLUSTERMSG_TYPE_PUBLISH) {
        robj *channel, *message;
        uint32_t channel_len, message_len;

        /* Don't bother creating useless objects if there are no
         * Pub/Sub subscribers. */
        if (dictSize(server.pubsub_channels) ||
            listLength(server.pubsub_patterns)) {
            channel_len = ntohl(hdr->data.publish.msg.channel_len);
            message_len = ntohl(hdr->data.publish.msg.message_len);
            channel = createStringObject(
                    (char *) hdr->data.publish.msg.bulk_data, channel_len);
            message = createStringObject(
                    (char *) hdr->data.publish.msg.bulk_data + channel_len,
                    message_len);
            pubsubPublishMessage(channel, message);
            decrRefCount(channel);
            decrRefCount(message);
        }

        // 代表本次收到的是一个投票请求 某个主从结构发生了故障转移 slave节点想晋升 就要征求其他节点的意见
    } else if (type == CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST) {
        if (!sender) return 1;  /* We don't know that node. */
        clusterSendFailoverAuthIfNeeded(sender, hdr);
        // 处理选举结果  只有被选择的节点才能收到ack信息 同一轮中之后的请求会被忽略
    } else if (type == CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK) {
        if (!sender) return 1;  /* We don't know that node. */
        /* We consider this vote only if the sender is a master serving
         * a non zero number of slots, and its currentEpoch is greater or
         * equal to epoch where this node started the election.
         * */
        if (nodeIsMaster(sender) && sender->numslots > 0 &&
            senderCurrentEpoch >= server.cluster->failover_auth_epoch) {
            server.cluster->failover_auth_count++;
            /* Maybe we reached a quorum here, set a flag to make sure
             * we check ASAP. */
            clusterDoBeforeSleep(CLUSTER_TODO_HANDLE_FAILOVER);
        }
        // 某个slave节点申请进行故障转移 master在收到通知后 就会处于暂停状态 拒绝新的写入请求 并将同步偏移量返回 等到slave同步完数据后就会开始选票
    } else if (type == CLUSTERMSG_TYPE_MFSTART) {
        /* This message is acceptable only if I'm a master and the sender
         * is one of my slaves.
         * 如果本地没有该slave的信息 或者显示该节点不是本节点的slave 不处理
         * */
        if (!sender || sender->slaveof != myself) return 1;
        /* Manual failover requested from slaves. Initialize the state
         * accordingly.
         * 重置本节点故障转移相关的信息 如果此时client已经处于暂停状态 先恢复
         * */
        resetManualFailover();
        // 设置选举时间上限
        server.cluster->mf_end = now + CLUSTER_MF_TIMEOUT;
        // 首推发起申请的节点为接任节点
        server.cluster->mf_slave = sender;
        // 暂停处理所有的client 并会在下一次心跳中将同步偏移量告诉slave 当slave准备完成时就会自主的发起投票
        pauseClients(now + (CLUSTER_MF_TIMEOUT * CLUSTER_MF_PAUSE_MULT));
        serverLog(LL_WARNING, "Manual failover requested by replica %.40s.",
                  sender->name);
        // 代表本节点维护的某个节点信息已经过期 需要更新
    } else if (type == CLUSTERMSG_TYPE_UPDATE) {
        clusterNode *n; /* The node the update is about. */
        // 通过比较epoch来确定哪个信息更新
        uint64_t reportedConfigEpoch =
                ntohu64(hdr->data.update.nodecfg.configEpoch);

        // 如果本次从一个未知的节点收到数据 不处理
        if (!sender) return 1;  /* We don't know the sender. */
        n = clusterLookupNode(hdr->data.update.nodecfg.nodename);
        // 本地找不到待更新的节点 忽略
        if (!n) return 1;   /* We don't know the reported node. */

        // 本地此时的版本更新 忽略
        if (n->configEpoch >= reportedConfigEpoch) return 1; /* Nothing new. */

        /* If in our current config the node is a slave, set it as a master.
         * 当发送心跳请求时 发现某个slot已经被分配到一个新的节点了 这时就会反向发送一个update消息 用于通知本节点有关占有slot的那个节点信息已经过期
         * 首先slot只能被master节点占有所以要将该节点升级成master
         * */
        if (nodeIsSlave(n)) clusterSetNodeAsMaster(n);

        /* Update the node's configEpoch. */
        n->configEpoch = reportedConfigEpoch;
        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                             CLUSTER_TODO_FSYNC_CONFIG);

        /* Check the bitmap of served slots and update our
         * config accordingly.
         * 更新该节点占有的slot
         * */
        clusterUpdateSlotsConfigWith(n, reportedConfigEpoch,
                                     hdr->data.update.nodecfg.slots);
    } else if (type == CLUSTERMSG_TYPE_MODULE) {
        if (!sender) return 1;  /* Protect the module from unknown nodes. */
        /* We need to route this message back to the right module subscribed
         * for the right message type. */
        uint64_t module_id = hdr->data.module.msg.module_id; /* Endian-safe ID */
        uint32_t len = ntohl(hdr->data.module.msg.len);
        uint8_t type = hdr->data.module.msg.type;
        unsigned char *payload = hdr->data.module.msg.bulk_data;
        moduleCallClusterReceivers(sender->name, module_id, type, payload, len);
    } else {
        serverLog(LL_WARNING, "Received unknown packet type: %d", type);
    }
    return 1;
}

/* This function is called when we detect the link with this node is lost.
   We set the node as no longer connected. The Cluster Cron will detect
   this connection and will try to get it connected again.

   Instead if the node is a temporary node used to accept a query, we
   completely free the node on error. */
void handleLinkIOError(clusterLink *link) {
    freeClusterLink(link);
}

/* Send data. This is handled using a trivial send buffer that gets
 * consumed by write(). We don't try to optimize this for speed too much
 * as this is a very low traffic channel.
 * 当写事件准备完成时 触发该方法
 * */
void clusterWriteHandler(connection *conn) {
    clusterLink *link = connGetPrivateData(conn);
    ssize_t nwritten;

    nwritten = connWrite(conn, link->sndbuf, sdslen(link->sndbuf));
    if (nwritten <= 0) {
        serverLog(LL_DEBUG, "I/O error writing to node link: %s",
                  (nwritten == -1) ? connGetLastError(conn) : "short write");
        handleLinkIOError(link);
        return;
    }
    sdsrange(link->sndbuf, nwritten, -1);
    // 如果数据写完 就注销writeHandler
    if (sdslen(link->sndbuf) == 0)
        connSetWriteHandler(link->conn, NULL);
}

/* A connect handler that gets called when a connection to another node
 * gets established.
 * 当与某个节点的连接建立完成后触发该方法
 */
void clusterLinkConnectHandler(connection *conn) {
    // 针对建立连接者 会在conn上绑定一个link
    clusterLink *link = connGetPrivateData(conn);
    clusterNode *node = link->node;

    /* Check if connection succeeded
     * 在执行conn_handler前 会根据本次连接的结果修改state  CONN_STATE_ERROR代表失败  CONN_STATE_CONNECTED代表成功
     * */
    if (connGetState(conn) != CONN_STATE_CONNECTED) {
        serverLog(LL_VERBOSE, "Connection with Node %.40s at %s:%d failed: %s",
                  node->name, node->ip, node->cport,
                  connGetLastError(conn));
        // 释放link对象 以及关闭底层连接
        freeClusterLink(link);
        return;
    }

    /* Register a read handler from now on
     * 为该连接设置readHandler  注意集群内对端节点设置的read_handler是一样的
     * */
    connSetReadHandler(conn, clusterReadHandler);

    /* Queue a PING in the new connection ASAP: this is crucial
     * to avoid false positives in failure detection.
     *
     * If the node is flagged as MEET, we send a MEET message instead
     * of a PING one, to force the receiver to add us in its node
     * table.
     * 建立连接后 立马向该节点发送一条数据
     * 一般情况下ping_sent 应该是null
     * */
    mstime_t old_ping_sent = node->ping_sent;

    // 用户通过调用clusterCommand插入的node会设置一个meet标记
    clusterSendPing(link, node->flags & CLUSTER_NODE_MEET ?
                          CLUSTERMSG_TYPE_MEET : CLUSTERMSG_TYPE_PING);

    // 一般情况下ping_sent 应该是null  如果有值就要还原
    if (old_ping_sent) {
        /* If there was an active ping before the link was
         * disconnected, we want to restore the ping time, otherwise
         * replaced by the clusterSendPing() call. */
        node->ping_sent = old_ping_sent;
    }
    /* We can clear the flag after the first packet is sent.
     * If we'll never receive a PONG, we'll never send new packets
     * to this node. Instead after the PONG is received and we
     * are no longer in meet/handshake status, we want to send
     * normal PING packets.
     * */
    node->flags &= ~CLUSTER_NODE_MEET;

    serverLog(LL_DEBUG, "Connecting with Node %.40s at %s:%d",
              node->name, node->ip, node->cport);
}

/* Read data. Try to read the first field of the header first to check the
 * full length of the packet. When a whole packet is in memory this function
 * will call the function to process the packet. And so forth.
 * 集群中的节点在接收到数据时 统一触发该方法
 * */
void clusterReadHandler(connection *conn) {
    clusterMsg buf[1];
    ssize_t nread;
    clusterMsg *hdr;
    clusterLink *link = connGetPrivateData(conn);
    unsigned int readlen, rcvbuflen;

    while (1) { /* Read as long as there is data to read. */
        // 这个是用来解决粘包拆包的
        rcvbuflen = link->rcvbuf_len;
        // 数据头部信息总计8byte 如果没有达到8bytes 代表此时读取的数据还不足 会将之前的数据先暂存到rcvbuf中
        if (rcvbuflen < 8) {
            /* First, obtain the first 8 bytes to get the full message
             * length. */
            readlen = 8 - rcvbuflen;
        } else {
            /* Finally read the full message. */
            hdr = (clusterMsg *) link->rcvbuf;
            // 此时数据已经满足8bytes了 检测头部信息 以及长度信息 并且rcvbuf的数据没有被清除应该是需要等到本次msg的数据全部被处理完后 才会清除
            if (rcvbuflen == 8) {
                /* Perform some sanity check on the message signature
                 * and length.
                 * 检测头部数据 数据有误 或者长度信息小于一个限制值 认为本次连接有问题
                 * */
                if (memcmp(hdr->sig, "RCmb", 4) != 0 ||
                    ntohl(hdr->totlen) < CLUSTERMSG_MIN_LEN) {
                    serverLog(LL_WARNING,
                              "Bad message length or signature received "
                              "from Cluster bus.");
                    handleLinkIOError(link);
                    return;
                }
            }
            // 根据结构体大小 读取对应长度
            readlen = ntohl(hdr->totlen) - rcvbuflen;
            if (readlen > sizeof(buf)) readlen = sizeof(buf);
        }

        // 按需读取数据
        nread = connRead(conn, buf, readlen);
        // 此时没有足够的数据
        if (nread == -1 && (connGetState(conn) == CONN_STATE_CONNECTED)) return; /* No more data ready. */

        // 连接断开导致的数据读取失败
        if (nread <= 0) {
            /* I/O error... */
            serverLog(LL_DEBUG, "I/O error reading from node link: %s",
                      (nread == 0) ? "connection closed" : connGetLastError(conn));
            handleLinkIOError(link);
            return;
        } else {
            /* Read data and recast the pointer to the new buffer.
             * rcvbuf_alloc应该是rcvbuf的总大小
             * rcvbuf_len是此时已经使用的大小
             * */
            size_t unused = link->rcvbuf_alloc - link->rcvbuf_len;

            // 代表此时没有足够的空间
            if ((size_t) nread > unused) {
                // 计算需要的大小
                size_t required = link->rcvbuf_len + nread;
                /* If less than 1mb, grow to twice the needed size, if larger grow by 1mb. */
                link->rcvbuf_alloc = required < RCVBUF_MAX_PREALLOC ? required * 2 : required + RCVBUF_MAX_PREALLOC;
                link->rcvbuf = zrealloc(link->rcvbuf, link->rcvbuf_alloc);
            }
            // 进行数据拷贝
            memcpy(link->rcvbuf + link->rcvbuf_len, buf, nread);
            // 因为读取了新数据 更新len
            link->rcvbuf_len += nread;
            hdr = (clusterMsg *) link->rcvbuf;
            rcvbuflen += nread;
        }

        /* Total length obtained? Process this packet.
         * 代表本次数据已经全部读取完毕
         * */
        if (rcvbuflen >= 8 && rcvbuflen == ntohl(hdr->totlen)) {
            // 处理数据包
            if (clusterProcessPacket(link)) {
                // 当此时缓冲区太大时 重新分配 否则只要重置len就可以
                if (link->rcvbuf_alloc > RCVBUF_INIT_LEN) {
                    zfree(link->rcvbuf);
                    link->rcvbuf = zmalloc(link->rcvbuf_alloc = RCVBUF_INIT_LEN);
                }
                link->rcvbuf_len = 0;
            } else {
                return; /* Link no longer valid. */
            }
        }
    }
}

/* Put stuff into the send buffer.
 *
 * It is guaranteed that this function will never have as a side effect
 * the link to be invalidated, so it is safe to call this function
 * from event handlers that will do stuff with the same link later.
 * 将数据体通过link关联的conn发送到目标node
 * */
void clusterSendMessage(clusterLink *link, unsigned char *msg, size_t msglen) {
    // 这里没有直接写入到socket 而是先注册写入事件,回调writeHandler
    if (sdslen(link->sndbuf) == 0 && msglen != 0)
        connSetWriteHandlerWithBarrier(link->conn, clusterWriteHandler, 1);

    // 拼接数据 sds底层会做自动扩容
    link->sndbuf = sdscatlen(link->sndbuf, msg, msglen);

    /* Populate sent messages stats. */
    clusterMsg *hdr = (clusterMsg *) msg;
    uint16_t type = ntohs(hdr->type);
    // 根据请求类型 更新统计数据
    if (type < CLUSTERMSG_TYPE_COUNT)
        server.cluster->stats_bus_messages_sent[type]++;
}

/* Send a message to all the nodes that are part of the cluster having
 * a connected link.
 *
 * It is guaranteed that this function will never have as a side effect
 * some node->link to be invalidated, so it is safe to call this function
 * from event handlers that will do stuff with node links later.
 * 将buf中的数据 转发给所有节点
 * */
void clusterBroadcastMessage(void *buf, size_t len) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(server.cluster->nodes);
    while ((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (!node->link) continue;
        if (node->flags & (CLUSTER_NODE_MYSELF | CLUSTER_NODE_HANDSHAKE))
            continue;
        clusterSendMessage(node->link, buf, len);
    }
    dictReleaseIterator(di);
}

/* Build the message header. hdr must point to a buffer at least
 * sizeof(clusterMsg) in bytes.
 * @param type 消息类型
 * @param hdr 存储消息的结构体
 * 根据本次要发送的消息体类型 填充结构体
 * */
void clusterBuildMessageHdr(clusterMsg *hdr, int type) {
    int totlen = 0;
    uint64_t offset;
    clusterNode *master;

    /* If this node is a master, we send its slots bitmap and configEpoch.
     * If this node is a slave we send the master's information instead (the
     * node is flagged as slave so the receiver knows that it is NOT really
     * in charge for this slots.
     * 找到本节点此时认可的master (可能是自己)
     * */
    master = (nodeIsSlave(myself) && myself->slaveof) ?
             myself->slaveof : myself;

    // 先用0填充hdr内存块
    memset(hdr, 0, sizeof(*hdr));
    // 写入固定的头部
    hdr->ver = htons(CLUSTER_PROTO_VER);
    hdr->sig[0] = 'R';
    hdr->sig[1] = 'C';
    hdr->sig[2] = 'm';
    hdr->sig[3] = 'b';
    // 设置消息体类型
    hdr->type = htons(type);
    // 设置消息发送者
    memcpy(hdr->sender, myself->name, CLUSTER_NAMELEN);

    /* If cluster-announce-ip option is enabled, force the receivers of our
     * packets to use the specified address for this node. Otherwise if the
     * first byte is zero, they'll do auto discovery.
     * 如果设置了cluster_announce_ip属性 将ip修改成这个值 否则为0(对端会自己去获取ip)
     * */
    memset(hdr->myip, 0, NET_IP_STR_LEN);
    if (server.cluster_announce_ip) {
        strncpy(hdr->myip, server.cluster_announce_ip, NET_IP_STR_LEN);
        hdr->myip[NET_IP_STR_LEN - 1] = '\0';
    }

    /* Handle cluster-announce-port as well.
     * 在hdr结构中填充本节点ip/port以及本节点观测到的集群状态/任期等
     * */
    int port = server.tls_cluster ? server.tls_port : server.port;
    int announced_port = server.cluster_announce_port ?
                         server.cluster_announce_port : port;
    int announced_cport = server.cluster_announce_bus_port ?
                          server.cluster_announce_bus_port :
                          (port + CLUSTER_PORT_INCR);

    // slave节点是不会保存slots信息的  所以要获取该slave认可的master的slots
    memcpy(hdr->myslots, master->slots, sizeof(hdr->myslots));
    memset(hdr->slaveof, 0, CLUSTER_NAMELEN);
    // 将本节点此时认可的master设置到hdr中   如果本节点自身就是master slaveof为null
    if (myself->slaveof != NULL)
        memcpy(hdr->slaveof, myself->slaveof->name, CLUSTER_NAMELEN);
    // 填充端口信息 以及描述集群/当前节点的状态信息
    hdr->port = htons(announced_port);
    hdr->cport = htons(announced_cport);
    hdr->flags = htons(myself->flags);
    hdr->state = server.cluster->state;

    /* Set the currentEpoch and configEpochs. */
    hdr->currentEpoch = htonu64(server.cluster->currentEpoch);
    hdr->configEpoch = htonu64(master->configEpoch);

    /* Set the replication offset.
     * 这里都是返回本节点认为的master节点的数据偏移量
     * */
    if (nodeIsSlave(myself))
        // 因为本节点是slave 所以无法获取最新的偏移量 只能获取一个近似值
        offset = replicationGetSlaveOffset();
    else
        // 该偏移量是用于主副本同步的  本节点作为master时 会直接记录这个值
        offset = server.master_repl_offset;
    hdr->offset = htonu64(offset);

    /* Set the message flags.
     * master节点正在进行手动故障转移  所有收到ping的slave节点会记录此时master的同步偏移量
     * 发起手动故障转移的节点也会设置mf_end 但是它不需要将同步偏移量告诉其他节点 只需要把这个值作为判断手动故障转移是否过期的判断条件就可以了 所以这里要求必须是master节点
     * */
    if (nodeIsMaster(myself) && server.cluster->mf_end)
        hdr->mflags[0] |= CLUSTERMSG_FLAG0_PAUSED;

    /* Compute the message length for certain messages. For other messages
     * this is up to the caller.
     * 本次消息类型是失败 或者更新 增加总长度
     * */
    if (type == CLUSTERMSG_TYPE_FAIL) {
        totlen = sizeof(clusterMsg) - sizeof(union clusterMsgData);
        totlen += sizeof(clusterMsgDataFail);
        // 代表本次发送的是一个更新数据
    } else if (type == CLUSTERMSG_TYPE_UPDATE) {
        totlen = sizeof(clusterMsg) - sizeof(union clusterMsgData);
        totlen += sizeof(clusterMsgDataUpdate);
    }
    hdr->totlen = htonl(totlen);
    /* For PING, PONG, and MEET, fixing the totlen field is up to the caller. */
}

/* Return non zero if the node is already present in the gossip section of the
 * message pointed by 'hdr' and having 'count' gossip entries. Otherwise
 * zero is returned. Helper for clusterSendPing().
 * 判断此时msg中是否已经有该node的数据了
 * */
int clusterNodeIsInGossipSection(clusterMsg *hdr, int count, clusterNode *n) {
    int j;
    for (j = 0; j < count; j++) {
        if (memcmp(hdr->data.ping.gossip[j].nodename, n->name,
                   CLUSTER_NAMELEN) == 0)
            break;
    }
    return j != count;
}

/* Set the i-th entry of the gossip section in the message pointed by 'hdr'
 * to the info of the specified node 'n'.
 * 抽取节点数据 生成gossip数据 并填充到hdr中
 * */
void clusterSetGossipEntry(clusterMsg *hdr, int i, clusterNode *n) {
    clusterMsgDataGossip *gossip;

    // 获取数组元素首地址
    gossip = &(hdr->data.ping.gossip[i]);
    // 填充数据
    memcpy(gossip->nodename, n->name, CLUSTER_NAMELEN);
    gossip->ping_sent = htonl(n->ping_sent / 1000);
    gossip->pong_received = htonl(n->pong_received / 1000);
    memcpy(gossip->ip, n->ip, sizeof(n->ip));
    gossip->port = htons(n->port);
    gossip->cport = htons(n->cport);
    gossip->flags = htons(n->flags);
    gossip->notused1 = 0;
}

/* Send a PING or PONG packet to the specified node, making sure to add enough
 * gossip information.
 * 在集群中每个节点都是相互连接的 并且会定期向其他节点发送一些自己观察到的信息
 * @param link 内部存储了连接目标服务器的conn
 * @param type 类型决定了本次传输数据的内容
 * */
void clusterSendPing(clusterLink *link, int type) {
    unsigned char *buf;
    clusterMsg *hdr;
    int gossipcount = 0; /* Number of gossip sections added so far. */
    int wanted; /* Number of gossip sections we want to append if possible. */
    int totlen; /* Total packet length. */

    /* freshnodes is the max number of nodes we can hope to append at all:
     * nodes available minus two (ourself and the node we are sending the
     * message to). However practically there may be less valid nodes since
     * nodes in handshake state, disconnected, are not considered.
     * 除开本节点和接收的目标节点外 剩余还有多少节点
     * */
    int freshnodes = dictSize(server.cluster->nodes) - 2;

    /* How many gossip sections we want to add? 1/10 of the number of nodes
     * and anyway at least 3. Why 1/10?
     *
     * If we have N masters, with N/10 entries, and we consider that in
     * node_timeout we exchange with each other node at least 4 packets
     * (we ping in the worst case in node_timeout/2 time, and we also
     * receive two pings from the host), we have a total of 8 packets
     * in the node_timeout*2 failure reports validity time. So we have
     * that, for a single PFAIL node, we can expect to receive the following
     * number of failure reports (in the specified window of time):
     *
     * PROB * GOSSIP_ENTRIES_PER_PACKET * TOTAL_PACKETS:
     *
     * PROB = probability of being featured in a single gossip entry,
     *        which is 1 / NUM_OF_NODES.
     * ENTRIES = 10.
     * TOTAL_PACKETS = 2 * 4 * NUM_OF_MASTERS.
     *
     * If we assume we have just masters (so num of nodes and num of masters
     * is the same), with 1/10 we always get over the majority, and specifically
     * 80% of the number of nodes, to account for many masters failing at the
     * same time.
     *
     * Since we have non-voting slaves that lower the probability of an entry
     * to feature our node, we set the number of entries per packet as
     * 10% of the total nodes we have.
     * 这里选取10%的节点
     * */
    wanted = floor(dictSize(server.cluster->nodes) / 10);
    // 至少发送3个节点的数据
    if (wanted < 3) wanted = 3;
    // 需要排除自己和对端节点的信息
    if (wanted > freshnodes) wanted = freshnodes;

    /* Include all the nodes in PFAIL state, so that failure reports are
     * faster to propagate to go from PFAIL to FAIL state.
     * 本节点认为这些节点可能已经下线了 通过与其他节点交互判断是否其他节点也无法连接上这些节点
     * */
    int pfail_wanted = server.cluster->stats_pfail_nodes;

    /* Compute the maximum totlen to allocate our buffer. We'll fix the totlen
     * later according to the number of gossip sections we really were able
     * to put inside the packet.
     * 因为clusterMsgData的长度可变 这里根据需要重新计算totlen
     * */
    totlen = sizeof(clusterMsg) - sizeof(union clusterMsgData);
    // 首先有关pfail的节点信息一定会发送
    totlen += (sizeof(clusterMsgDataGossip) * (wanted + pfail_wanted));
    /* Note: clusterBuildMessageHdr() expects the buffer to be always at least
     * sizeof(clusterMsg) or more. */
    if (totlen < (int) sizeof(clusterMsg)) totlen = sizeof(clusterMsg);

    // 申请内存
    buf = zcalloc(totlen);
    hdr = (clusterMsg *) buf;

    /* Populate the header.
     * 代表本次发起的是一次心跳请求  更新最后一次往被节点发送请求的时间戳
     * */
    if (link->node && type == CLUSTERMSG_TYPE_PING)
        link->node->ping_sent = mstime();

    // 根据本次发送消息的类型 填充消息体
    clusterBuildMessageHdr(hdr, type);

    /* Populate the gossip fields
     * */
    int maxiterations = wanted * 3;

    // 总节点数是freshnodes 但是只要能够采集到wanted数量的数据就可以了 并且是随机挑选  这里还有最大随机次数的限制 避免极端情况总是选择出现过的节点
    while (freshnodes > 0 && gossipcount < wanted && maxiterations--) {
        dictEntry *de = dictGetRandomKey(server.cluster->nodes);
        clusterNode *this = dictGetVal(de);

        /* Don't include this node: the whole packet header is about us
         * already, so we just gossip about other nodes.
         * 本节点信息不需要发送
         * */
        if (this == myself) continue;

        /* PFAIL nodes will be added later.
         * 因为失败的节点必然会加入到msg中 这里只是挑选普通节点信息
         * */
        if (this->flags & CLUSTER_NODE_PFAIL) continue;

        /* In the gossip section don't include:
         * 1) Nodes in HANDSHAKE state.
         * 3) Nodes with the NOADDR flag set.
         * 4) Disconnected nodes if they don't have configured slots.
         * 如果与该节点还处于握手阶段 或者无该节点的地址信息 或者连接未创建 或者没有任何数据分配到该节点上
         */
        if (this->flags & (CLUSTER_NODE_HANDSHAKE | CLUSTER_NODE_NOADDR) ||
            (this->link == NULL && this->numslots == 0)) {
            freshnodes--; /* Technically not correct, but saves CPU. */
            continue;
        }

        /* Do not add a node we already have.
         * 代表当前节点数据已经设置到msg中了
         * */
        if (clusterNodeIsInGossipSection(hdr, gossipcount, this)) continue;

        /* Add it 为当前node生成gossip数据 并填充到指定的位置 */
        clusterSetGossipEntry(hdr, gossipcount, this);
        freshnodes--;
        gossipcount++;
    }

    /* If there are PFAIL nodes, add them at the end.
     * pfail节点数据 必须全部加入到msg中
     * */
    if (pfail_wanted) {
        dictIterator *di;
        dictEntry *de;

        di = dictGetSafeIterator(server.cluster->nodes);
        while ((de = dictNext(di)) != NULL && pfail_wanted > 0) {
            clusterNode *node = dictGetVal(de);
            if (node->flags & CLUSTER_NODE_HANDSHAKE) continue;
            if (node->flags & CLUSTER_NODE_NOADDR) continue;
            if (!(node->flags & CLUSTER_NODE_PFAIL)) continue;
            clusterSetGossipEntry(hdr, gossipcount, node);
            freshnodes--;
            gossipcount++;
            /* We take the count of the slots we allocated, since the
             * PFAIL stats may not match perfectly with the current number
             * of PFAIL nodes. */
            pfail_wanted--;
        }
        dictReleaseIterator(di);
    }

    /* Ready to send... fix the totlen fiend and queue the message in the
     * output buffer.
     * 根据实际情况重新计算msg的大小
     * */
    totlen = sizeof(clusterMsg) - sizeof(union clusterMsgData);
    totlen += (sizeof(clusterMsgDataGossip) * gossipcount);
    // 设置gossip总数 以及总长度
    hdr->count = htons(gossipcount);
    hdr->totlen = htonl(totlen);
    // 将数据设置到缓冲区中 当写入事件准备完成后 触发writeHandler
    clusterSendMessage(link, buf, totlen);
    zfree(buf);
}

/* Send a PONG packet to every connected node that's not in handshake state
 * and for which we have a valid link.
 *
 * In Redis Cluster pongs are not used just for failure detection, but also
 * to carry important configuration information. So broadcasting a pong is
 * useful when something changes in the configuration and we want to make
 * the cluster aware ASAP (for instance after a slave promotion).
 *
 * The 'target' argument specifies the receiving instances using the
 * defines below:
 *
 * CLUSTER_BROADCAST_ALL -> All known instances.
 * CLUSTER_BROADCAST_LOCAL_SLAVES -> All slaves in my master-slaves ring.
 */
#define CLUSTER_BROADCAST_ALL 0
#define CLUSTER_BROADCAST_LOCAL_SLAVES 1

/**
 * 向所有slave发送数据
 * @param target
 */
void clusterBroadcastPong(int target) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(server.cluster->nodes);
    while ((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (!node->link) continue;
        if (node == myself || nodeInHandshake(node)) continue;
        // 通知本节点下所有slave 或者同一master的所有slave
        if (target == CLUSTER_BROADCAST_LOCAL_SLAVES) {
            int local_slave =
                    nodeIsSlave(node) && node->slaveof &&
                    (node->slaveof == myself || node->slaveof == myself->slaveof);
            if (!local_slave) continue;
        }
        // 直接发送pong类型的消息 实际上是代表不需要对端返回信息  如果发送的是ping类型 对端会返回一个pong信息
        clusterSendPing(node->link, CLUSTERMSG_TYPE_PONG);
    }
    dictReleaseIterator(di);
}

/* Send a PUBLISH message.
 *
 * If link is NULL, then the message is broadcasted to the whole cluster.
 * 某个client往该server的某个channel投递一条消息
 * @param link 没有指定link 代表广播到集群的所有节点
 * */
void clusterSendPublish(clusterLink *link, robj *channel, robj *message) {
    unsigned char *payload;
    clusterMsg buf[1];
    clusterMsg *hdr = (clusterMsg *) buf;
    uint32_t totlen;
    uint32_t channel_len, message_len;

    channel = getDecodedObject(channel);
    message = getDecodedObject(message);
    channel_len = sdslen(channel->ptr);
    message_len = sdslen(message->ptr);

    // 构建一条集群内传递的消息
    clusterBuildMessageHdr(hdr, CLUSTERMSG_TYPE_PUBLISH);
    totlen = sizeof(clusterMsg) - sizeof(union clusterMsgData);
    // 这里是计算clusterMsgData的大小
    totlen += sizeof(clusterMsgDataPublish) - 8 + channel_len + message_len;

    // 只有此时是发布一条消息时 才会设置这2个标识
    hdr->data.publish.msg.channel_len = htonl(channel_len);
    hdr->data.publish.msg.message_len = htonl(message_len);
    hdr->totlen = htonl(totlen);

    /* Try to use the local buffer if possible */
    if (totlen < sizeof(buf)) {
        payload = (unsigned char *) buf;
    } else {
        // 空间不足的情况 申请额外空间
        payload = zmalloc(totlen);
        memcpy(payload, hdr, sizeof(*hdr));
        hdr = (clusterMsg *) payload;
    }
    // 存储channel以及message
    memcpy(hdr->data.publish.msg.bulk_data, channel->ptr, sdslen(channel->ptr));
    memcpy(hdr->data.publish.msg.bulk_data + sdslen(channel->ptr),
           message->ptr, sdslen(message->ptr));

    if (link)
        clusterSendMessage(link, payload, totlen);
    else
        // 将数据发往集群中每个节点
        clusterBroadcastMessage(payload, totlen);

    decrRefCount(channel);
    decrRefCount(message);
    if (payload != (unsigned char *) buf) zfree(payload);
}

/* Send a FAIL message to all the nodes we are able to contact.
 * The FAIL message is sent when we detect that a node is failing
 * (CLUSTER_NODE_PFAIL) and we also receive a gossip confirmation of this:
 * we switch the node state to CLUSTER_NODE_FAIL and ask all the other
 * nodes to do the same ASAP.
 * 将某个节点失败的信息通知到其他节点
 * */
void clusterSendFail(char *nodename) {
    clusterMsg buf[1];
    clusterMsg *hdr = (clusterMsg *) buf;

    clusterBuildMessageHdr(hdr, CLUSTERMSG_TYPE_FAIL);
    memcpy(hdr->data.fail.about.nodename, nodename, CLUSTER_NAMELEN);
    clusterBroadcastMessage(buf, ntohl(hdr->totlen));
}

/* Send an UPDATE message to the specified link carrying the specified 'node'
 * slots configuration. The node name, slots bitmap, and configEpoch info
 * are included.
 * 将node的信息发往link关联的节点
 * */
void clusterSendUpdate(clusterLink *link, clusterNode *node) {
    clusterMsg buf[1];
    clusterMsg *hdr = (clusterMsg *) buf;

    if (link == NULL) return;
    // 不同于ping请求不需要发送一些其他node的信息
    clusterBuildMessageHdr(hdr, CLUSTERMSG_TYPE_UPDATE);

    // 将node节点的名字以及此时最新的slots同步到link对应的节点上 name是用来匹配的 通过configEpoch来判断信息的新旧
    memcpy(hdr->data.update.nodecfg.nodename, node->name, CLUSTER_NAMELEN);
    hdr->data.update.nodecfg.configEpoch = htonu64(node->configEpoch);
    memcpy(hdr->data.update.nodecfg.slots, node->slots, sizeof(node->slots));
    clusterSendMessage(link, (unsigned char *) buf, ntohl(hdr->totlen));
}

/* Send a MODULE message.
 *
 * If link is NULL, then the message is broadcasted to the whole cluster. */
void clusterSendModule(clusterLink *link, uint64_t module_id, uint8_t type,
                       unsigned char *payload, uint32_t len) {
    unsigned char *heapbuf;
    clusterMsg buf[1];
    clusterMsg *hdr = (clusterMsg *) buf;
    uint32_t totlen;

    clusterBuildMessageHdr(hdr, CLUSTERMSG_TYPE_MODULE);
    totlen = sizeof(clusterMsg) - sizeof(union clusterMsgData);
    totlen += sizeof(clusterMsgModule) - 3 + len;

    hdr->data.module.msg.module_id = module_id; /* Already endian adjusted. */
    hdr->data.module.msg.type = type;
    hdr->data.module.msg.len = htonl(len);
    hdr->totlen = htonl(totlen);

    /* Try to use the local buffer if possible */
    if (totlen < sizeof(buf)) {
        heapbuf = (unsigned char *) buf;
    } else {
        heapbuf = zmalloc(totlen);
        memcpy(heapbuf, hdr, sizeof(*hdr));
        hdr = (clusterMsg *) heapbuf;
    }
    memcpy(hdr->data.module.msg.bulk_data, payload, len);

    if (link)
        clusterSendMessage(link, heapbuf, totlen);
    else
        clusterBroadcastMessage(heapbuf, totlen);

    if (heapbuf != (unsigned char *) buf) zfree(heapbuf);
}

/* This function gets a cluster node ID string as target, the same way the nodes
 * addresses are represented in the modules side, resolves the node, and sends
 * the message. If the target is NULL the message is broadcasted.
 *
 * The function returns C_OK if the target is valid, otherwise C_ERR is
 * returned. */
int clusterSendModuleMessageToTarget(const char *target, uint64_t module_id, uint8_t type, unsigned char *payload,
                                     uint32_t len) {
    clusterNode *node = NULL;

    if (target != NULL) {
        node = clusterLookupNode(target);
        if (node == NULL || node->link == NULL) return C_ERR;
    }

    clusterSendModule(target ? node->link : NULL,
                      module_id, type, payload, len);
    return C_OK;
}

/* -----------------------------------------------------------------------------
 * CLUSTER Pub/Sub support
 *
 * For now we do very little, just propagating PUBLISH messages across the whole
 * cluster. In the future we'll try to get smarter and avoiding propagating those
 * messages to hosts without receives for a given channel.
 * 某个client往该服务器的某个channel推送某条消息时执行该方法 (当发现服务器是集群模式的情况)
 * @param channel
 * @param message
 * -------------------------------------------------------------------------- */
void clusterPropagatePublish(robj *channel, robj *message) {
    clusterSendPublish(NULL, channel, message);
}

/* -----------------------------------------------------------------------------
 * SLAVE node specific functions
 * -------------------------------------------------------------------------- */

/* This function sends a FAILOVE_AUTH_REQUEST message to every node in order to
 * see if there is the quorum for this slave instance to failover its failing
 * master.
 *
 * Note that we send the failover request to everybody, master and slave nodes,
 * but only the masters are supposed to reply to our query.
 * 当要进行故障转移时 将选举请求发往其他所有节点
 * */
void clusterRequestFailoverAuth(void) {
    clusterMsg buf[1];
    clusterMsg *hdr = (clusterMsg *) buf;
    uint32_t totlen;

    // 发送的消息类型是CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST
    clusterBuildMessageHdr(hdr, CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST);
    /* If this is a manual failover, set the CLUSTERMSG_FLAG0_FORCEACK bit
     * in the header to communicate the nodes receiving the message that
     * they should authorized the failover even if the master is working.
     * 代表本次是手动发起的故障转移 需要强制ack
     * */
    if (server.cluster->mf_end) hdr->mflags[0] |= CLUSTERMSG_FLAG0_FORCEACK;
    totlen = sizeof(clusterMsg) - sizeof(union clusterMsgData);
    hdr->totlen = htonl(totlen);
    clusterBroadcastMessage(buf, totlen);
}

/* Send a FAILOVER_AUTH_ACK message to the specified node.
 * 在某轮故障转移选举中 将node看作新的master
 * */
void clusterSendFailoverAuth(clusterNode *node) {
    clusterMsg buf[1];
    clusterMsg *hdr = (clusterMsg *) buf;
    uint32_t totlen;

    if (!node->link) return;
    // 选举结果信息类型是 CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK
    clusterBuildMessageHdr(hdr, CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK);
    totlen = sizeof(clusterMsg) - sizeof(union clusterMsgData);
    hdr->totlen = htonl(totlen);
    clusterSendMessage(node->link, (unsigned char *) buf, totlen);
}

/* Send a MFSTART message to the specified node.
 * 本节点作为slave 打算发起一次手动故障转移
 * 需要通知master 让他暂停接收新的请求 同时把偏移量返回回来 等本节点同步完这些数据后 就可以开始故障转移了
 * */
void clusterSendMFStart(clusterNode *node) {
    clusterMsg buf[1];
    clusterMsg *hdr = (clusterMsg *) buf;
    uint32_t totlen;

    // 如果还未与该节点建立连接 直接返回 也就是非强制故障转移在这一步就终止了
    if (!node->link) return;
    // 填充数据包
    clusterBuildMessageHdr(hdr, CLUSTERMSG_TYPE_MFSTART);
    totlen = sizeof(clusterMsg) - sizeof(union clusterMsgData);
    hdr->totlen = htonl(totlen);
    clusterSendMessage(node->link, (unsigned char *) buf, totlen);
}

/* Vote for the node asking for our vote if there are the conditions.
 * 处理某个slave的拉票请求
 * */
void clusterSendFailoverAuthIfNeeded(clusterNode *node, clusterMsg *request) {
    clusterNode *master = node->slaveof;
    // 当节点发起投票时 会增加currentEpoch
    uint64_t requestCurrentEpoch = ntohu64(request->currentEpoch);
    uint64_t requestConfigEpoch = ntohu64(request->configEpoch);
    // 这个是slave对应的master节点此时分配的slots
    unsigned char *claimed_slots = request->myslots;
    // 如果是手动故障转移 会加上forceack标记
    int force_ack = request->mflags[0] & CLUSTERMSG_FLAG0_FORCEACK;
    int j;

    /* IF we are not a master serving at least 1 slot, we don't have the
     * right to vote, as the cluster size in Redis Cluster is the number
     * of masters serving at least one slot, and quorum is the cluster
     * size + 1
     * 能够参与选举的就是其他master节点 并且他们的slots必须有效
     * */
    if (nodeIsSlave(myself) || myself->numslots == 0) return;

    /* Request epoch must be >= our currentEpoch.
     * Note that it is impossible for it to actually be greater since
     * our currentEpoch was updated as a side effect of receiving this
     * request, if the request epoch was greater.
     * 如果发起投票的节点的任期已经落后了  不需要处理
     * */
    if (requestCurrentEpoch < server.cluster->currentEpoch) {
        serverLog(LL_WARNING,
                  "Failover auth denied to %.40s: reqEpoch (%llu) < curEpoch(%llu)",
                  node->name,
                  (unsigned long long) requestCurrentEpoch,
                  (unsigned long long) server.cluster->currentEpoch);
        return;
    }

    /* I already voted for this epoch? Return ASAP.
     * 针对这个epoch已经投票过了
     * */
    if (server.cluster->lastVoteEpoch == server.cluster->currentEpoch) {
        serverLog(LL_WARNING,
                  "Failover auth denied to %.40s: already voted for epoch %llu",
                  node->name,
                  (unsigned long long) server.cluster->currentEpoch);
        return;
    }

    /* Node must be a slave and its master down.
     * The master can be non failing if the request is flagged
     * with CLUSTERMSG_FLAG0_FORCEACK (manual failover).
     * 如果目标节点已经是master节点了
     * 或者没有该node的master信息
     * 或者master节点此时是上线状态 并且本次不是手动故障转移    不处理
     * 也就是当某个slave自发的发起故障转移时 必须要通过超半数的其他master的支持才能成功 也就代表超半数的master节点都与这个master失联
     * */
    if (nodeIsMaster(node) || master == NULL ||
        (!nodeFailed(master) && !force_ack)) {
        if (nodeIsMaster(node)) {
            serverLog(LL_WARNING,
                      "Failover auth denied to %.40s: it is a master node",
                      node->name);
        } else if (master == NULL) {
            serverLog(LL_WARNING,
                      "Failover auth denied to %.40s: I don't know its master",
                      node->name);
        } else if (!nodeFailed(master)) {
            serverLog(LL_WARNING,
                      "Failover auth denied to %.40s: its master is up",
                      node->name);
        }
        return;
    }

    /* We did not voted for a slave about this master for two
     * times the node timeout. This is not strictly needed for correctness
     * of the algorithm but makes the base case more linear.
     * 这个相当于是一个锁 该master下的所有slave发送选举请求时 通过判断针对该master的选举时间来判断是否已经处理过投票请求了
     * */
    if (mstime() - node->slaveof->voted_time < server.cluster_node_timeout * 2) {
        serverLog(LL_WARNING,
                  "Failover auth denied to %.40s: "
                  "can't vote about this master before %lld milliseconds",
                  node->name,
                  (long long) ((server.cluster_node_timeout * 2) -
                               (mstime() - node->slaveof->voted_time)));
        return;
    }

    /* The slave requesting the vote must have a configEpoch for the claimed
     * slots that is >= the one of the masters currently serving the same
     * slots in the current configuration. */
    for (j = 0; j < CLUSTER_SLOTS; j++) {
        if (bitmapTestBit(claimed_slots, j) == 0) continue;

        // 遍历此时分配到该master下的所有slot 如果slot发生了冲突 也无法继续处理
        if (server.cluster->slots[j] == NULL ||
            server.cluster->slots[j]->configEpoch <= requestConfigEpoch) {
            continue;
        }
        /* If we reached this point we found a slot that in our current slots
         * is served by a master with a greater configEpoch than the one claimed
         * by the slave requesting our vote. Refuse to vote for this slave. */
        serverLog(LL_WARNING,
                  "Failover auth denied to %.40s: "
                  "slot %d epoch (%llu) > reqEpoch (%llu)",
                  node->name, j,
                  (unsigned long long) server.cluster->slots[j]->configEpoch,
                  (unsigned long long) requestConfigEpoch);
        return;
    }

    /* We can vote for this slave.
     * 记录最近一次投票的任期
     * */
    server.cluster->lastVoteEpoch = server.cluster->currentEpoch;
    node->slaveof->voted_time = mstime();
    clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_FSYNC_CONFIG);

    // 代表本轮针对该主从结构 选择node作为新的master
    clusterSendFailoverAuth(node);
    serverLog(LL_WARNING, "Failover auth granted to %.40s for epoch %llu",
              node->name, (unsigned long long) server.cluster->currentEpoch);
}

/* This function returns the "rank" of this instance, a slave, in the context
 * of its master-slaves ring. The rank of the slave is given by the number of
 * other slaves for the same master that have a better replication offset
 * compared to the local one (better means, greater, so they claim more data).
 *
 * A slave with rank 0 is the one with the greatest (most up to date)
 * replication offset, and so forth. Note that because how the rank is computed
 * multiple slaves may have the same rank, in case they have the same offset.
 *
 * The slave rank is used to add a delay to start an election in order to
 * get voted and replace a failing master. Slaves with better replication
 * offsets are more likely to win.
 * 在故障转移节点 可能会有多个slave尝试替换master 这里为slave判定等级 等级越高优先级越低
 * 大体逻辑是这样的 越多的slave偏移量 > 本节点的偏移量 反过来说本节点的偏移量就越落后
 * */
int clusterGetSlaveRank(void) {
    long long myoffset;
    int j, rank = 0;
    clusterNode *master;

    // 确保本节点是slave节点
    serverAssert(nodeIsSlave(myself));
    master = myself->slaveof;
    if (master == NULL) return 0; /* Never called by slaves without master. */

    myoffset = replicationGetSlaveOffset();
    for (j = 0; j < master->numslaves; j++)
        if (master->slaves[j] != myself &&
            !nodeCantFailover(master->slaves[j]) &&
            master->slaves[j]->repl_offset > myoffset)
            rank++;
    return rank;
}

/* This function is called by clusterHandleSlaveFailover() in order to
 * let the slave log why it is not able to failover. Sometimes there are
 * not the conditions, but since the failover function is called again and
 * again, we can't log the same things continuously.
 *
 * This function works by logging only if a given set of conditions are
 * true:
 *
 * 1) The reason for which the failover can't be initiated changed.
 *    The reasons also include a NONE reason we reset the state to
 *    when the slave finds that its master is fine (no FAIL flag).
 * 2) Also, the log is emitted again if the master is still down and
 *    the reason for not failing over is still the same, but more than
 *    CLUSTER_CANT_FAILOVER_RELOG_PERIOD seconds elapsed.
 * 3) Finally, the function only logs if the slave is down for more than
 *    five seconds + NODE_TIMEOUT. This way nothing is logged when a
 *    failover starts in a reasonable time.
 *
 * The function is called with the reason why the slave can't failover
 * which is one of the integer macros CLUSTER_CANT_FAILOVER_*.
 *
 * The function is guaranteed to be called only if 'myself' is a slave.
 * 当无法进行故障转移时  打印日志
 * @param reason 原因信息
 * */
void clusterLogCantFailover(int reason) {
    char *msg;
    static time_t lastlog_time = 0;
    mstime_t nolog_fail_time = server.cluster_node_timeout + 5000;

    /* Don't log if we have the same reason for some time.
     * 如果原因 已经被记录 并且记录上次打印日志时间没隔多久 避免重复打印
     * */
    if (reason == server.cluster->cant_failover_reason &&
        time(NULL) - lastlog_time < CLUSTER_CANT_FAILOVER_RELOG_PERIOD)
        return;

    // 设置失败原因
    server.cluster->cant_failover_reason = reason;

    /* We also don't emit any log if the master failed no long ago, the
     * goal of this function is to log slaves in a stalled condition for
     * a long time.
     * 失败时间小于某个值 忽略
     * */
    if (myself->slaveof &&
        nodeFailed(myself->slaveof) &&
        (mstime() - myself->slaveof->fail_time) < nolog_fail_time)
        return;

    switch (reason) {
        case CLUSTER_CANT_FAILOVER_DATA_AGE:
            msg = "Disconnected from master for longer than allowed. "
                  "Please check the 'cluster-replica-validity-factor' configuration "
                  "option.";
            break;
        case CLUSTER_CANT_FAILOVER_WAITING_DELAY:
            msg = "Waiting the delay before I can start a new failover.";
            break;
        case CLUSTER_CANT_FAILOVER_EXPIRED:
            msg = "Failover attempt expired.";
            break;
        case CLUSTER_CANT_FAILOVER_WAITING_VOTES:
            msg = "Waiting for votes, but majority still not reached.";
            break;
        default:
            msg = "Unknown reason code.";
            break;
    }
    lastlog_time = time(NULL);
    serverLog(LL_WARNING, "Currently unable to failover: %s", msg);
}

/* This function implements the final part of automatic and manual failovers,
 * where the slave grabs its master's hash slots, and propagates the new
 * configuration.
 *
 * Note that it's up to the caller to be sure that the node got a new
 * configuration epoch already.
 * 本节点替代原来的master 成为新的master
 * */
void clusterFailoverReplaceYourMaster(void) {
    int j;
    clusterNode *oldmaster = myself->slaveof;

    // 因为是替代关系 所以oldmaster不能为空
    if (nodeIsMaster(myself) || oldmaster == NULL) return;

    /* 1) Turn this node into a master. 将本节点设置成master 这里还没有通知其他节点 */
    clusterSetNodeAsMaster(myself);
    // 在副本模块清理掉master信息
    replicationUnsetMaster();

    /* 2) Claim all the slots assigned to our master.
     * 将slots信息转移到本节点上  slave是不需要维护slots信息的
     * */
    for (j = 0; j < CLUSTER_SLOTS; j++) {
        if (clusterNodeGetSlotBit(oldmaster, j)) {
            clusterDelSlot(j);
            clusterAddSlot(myself, j);
        }
    }

    /* 3) Update state and save config.
     * 更新集群状态以及 更新配置文件
     * */
    clusterUpdateState();
    clusterSaveConfigOrDie(1);

    /* 4) Pong all the other nodes so that they can update the state
     *    accordingly and detect that we switched to master role.
     *    通知所有节点本节点已经成功晋升 其他slave会自动加入到本节点
     *    */
    clusterBroadcastPong(CLUSTER_BROADCAST_ALL);

    /* 5) If there was a manual failover in progress, clear the state.
     * 重置手动故障转移相关的参数
     * */
    resetManualFailover();
}

/* This function is called if we are a slave node and our master serving
 * a non-zero amount of hash slots is in FAIL state.
 *
 * The gaol of this function is:
 * 1) To check if we are able to perform a failover, is our data updated?
 * 2) Try to get elected by masters.
 * 3) Perform the failover informing all the other nodes.
 * 进行故障转移
 */
void clusterHandleSlaveFailover(void) {

    mstime_t data_age;
    // 计算当前时间距离上次开始选举过了多久
    mstime_t auth_age = mstime() - server.cluster->failover_auth_time;
    // 代表选举需要的票数
    int needed_quorum = (server.cluster->size / 2) + 1;
    // 判断本次故障转移是否是手动触发的
    int manual_failover = server.cluster->mf_end != 0 &&
                          server.cluster->mf_can_start;

    mstime_t auth_timeout, auth_retry_time;

    // 因为在这里已经处理过故障转移了 所以在 before_sleep 函数中就不需要再处理了
    server.cluster->todo_before_sleep &= ~CLUSTER_TODO_HANDLE_FAILOVER;

    /* Compute the failover timeout (the max time we have to send votes
     * and wait for replies), and the failover retry time (the time to wait
     * before trying to get voted again).
     *
     * Timeout is MAX(NODE_TIMEOUT*2,2000) milliseconds.
     * Retry is two times the Timeout.
     * 计算一个选举时间 最少为2秒 重试时间为4秒
     */
    auth_timeout = server.cluster_node_timeout * 2;
    if (auth_timeout < 2000) auth_timeout = 2000;
    auth_retry_time = auth_timeout * 2;

    /* Pre conditions to run the function, that must be met both in case
     * 下面的条件中 既有处理手动故障转移的情况 也有slave自主发起故障转移的情况
     * of an automatic or manual failover:
     * 1) We are a slave.                                                      本节点必须是slave
     * 2) Our master is flagged as FAIL, or this is a manual failover.         master必须出现故障 且这是一次故障转移操作
     * 3) We don't have the no failover configuration set, and this is         没有拒绝slave的故障转移
     *    not a manual failover.
     * 4) It is serving slots.                                                 master节点必须有slot                */
    if (nodeIsMaster(myself) ||   // 当前节点是master 不应该处理故障转移
        myself->slaveof == NULL ||  // 同上master节点没有slaveof
        (!nodeFailed(myself->slaveof) && !manual_failover) || // 本节点无法连接到master 并且本次不是手动故障转移 这个意思是在master还没下线的时候不允许slave主动发起故障转移
        (server.cluster_slave_no_failover && !manual_failover) || // 如果不允许slave自主发起故障转移 并且本次不是手动故障转移
        myself->slaveof->numslots == 0) {  // 如果master节点下还没有任何slot 不需要故障转移
        /* There are no reasons to failover, so we set the reason why we
         * are returning without failing over to NONE.
         * 设置失败原因为 无法进行故障转移
         * */
        server.cluster->cant_failover_reason = CLUSTER_CANT_FAILOVER_NONE;
        return;
    }

    /* Set data_age to the number of seconds we are disconnected from
     * the master.
     * */
    // 代表此时保持着与master的连接
    if (server.repl_state == REPL_STATE_CONNECTED) {
        // 计算距离上次与master交互过了多久
        data_age = (mstime_t) (server.unixtime - server.master->lastinteraction)
                   * 1000;
    } else {
        // 距离与master断开连接过了多久
        data_age = (mstime_t) (server.unixtime - server.repl_down_since) * 1000;
    }

    /* Remove the node timeout from the data age as it is fine that we are
     * disconnected from our master at least for the time it was down to be
     * flagged as FAIL, that's the baseline.
     * */
    if (data_age > server.cluster_node_timeout)
        data_age -= server.cluster_node_timeout;

    // 即使满足了故障转移的基本约束 下面还有一些客观条件的检测

    /* Check if our data is recent enough according to the slave validity
     * factor configured by the user.
     *
     * Check bypassed for manual failovers.
     * TODO
     * */
    if (server.cluster_slave_validity_factor &&
        data_age >
        (((mstime_t) server.repl_ping_slave_period * 1000) +
         (server.cluster_node_timeout * server.cluster_slave_validity_factor))) {
        if (!manual_failover) {
            clusterLogCantFailover(CLUSTER_CANT_FAILOVER_DATA_AGE);
            return;
        }
    }

    /* If the previous failover attempt timeout and the retry time has
     * elapsed, we can setup a new one.
     * 这里已经超过了重试时间 需要重新发起投票 首次发起故障转移也是进入这个分支
     * */
    if (auth_age > auth_retry_time) {

        // 更新发起故障转移的时间
        server.cluster->failover_auth_time = mstime() +
                                             500 + /* Fixed delay of 500 milliseconds, let FAIL msg propagate. */
                                             random() % 500; /* Random delay between 0 and 500 milliseconds. */
        // 重置相关参数
        server.cluster->failover_auth_count = 0;
        server.cluster->failover_auth_sent = 0;
        // rank越高 优先级越低
        server.cluster->failover_auth_rank = clusterGetSlaveRank();
        /* We add another delay that is proportional to the slave rank.
         * Specifically 1 second * rank. This way slaves that have a probably
         * less updated replication offset, are penalized.
         * 尽可能让优先级低的slave 晚触发选举动作
         * */
        server.cluster->failover_auth_time +=
                server.cluster->failover_auth_rank * 1000;
        /* However if this is a manual failover, no delay is needed.
         * 如果这是一次手动触发的故障转移 不需要延迟 直接触发选举 选举逻辑在 before_sleep中执行
         * */
        if (server.cluster->mf_end) {
            server.cluster->failover_auth_time = mstime();
            server.cluster->failover_auth_rank = 0;
            // 标记需要进行故障转移
            clusterDoBeforeSleep(CLUSTER_TODO_HANDLE_FAILOVER);
        }
        serverLog(LL_WARNING,
                  "Start of election delayed for %lld milliseconds "
                  "(rank #%d, offset %lld).",
                  server.cluster->failover_auth_time - mstime(),
                  server.cluster->failover_auth_rank,
                  replicationGetSlaveOffset());
        /* Now that we have a scheduled election, broadcast our offset
         * to all the other slaves so that they'll updated their offsets
         * if our offset is better.
         * 向其他所有slave节点发送通知信息 将自身的偏移量通知到所有slave 这样确保其他节点可以维护本节点正确的偏移量
         * */
        clusterBroadcastPong(CLUSTER_BROADCAST_LOCAL_SLAVES);
        return;
    }

    // 下面是重试的逻辑

    /* It is possible that we received more updated offsets from other
     * slaves for the same master since we computed our election delay.
     * Update the delay if our rank changed.
     *
     * Not performed if this is a manual failover.
     * slave自主发起故障转移的场景 并且还没有发出请求(在成为master前要征询其他slave的意见)
     * 判断是否需要延长触发时间
     * */
    if (server.cluster->failover_auth_sent == 0 &&
        server.cluster->mf_end == 0) {
        // 重新计算本节点的优先级
        int newrank = clusterGetSlaveRank();
        // 代表优先级降低了
        if (newrank > server.cluster->failover_auth_rank) {
            long long added_delay =
                    (newrank - server.cluster->failover_auth_rank) * 1000;
            // 延迟触发时间
            server.cluster->failover_auth_time += added_delay;
            server.cluster->failover_auth_rank = newrank;
            serverLog(LL_WARNING,
                      "Replica rank updated to #%d, added %lld milliseconds of delay.",
                      newrank, added_delay);
        }
    }

    /* Return ASAP if we can't still start the election.
     * 本slave的时间条件还没有满足 无法进行故障转移
     * */
    if (mstime() < server.cluster->failover_auth_time) {
        clusterLogCantFailover(CLUSTER_CANT_FAILOVER_WAITING_DELAY);
        return;
    }

    /* Return ASAP if the election is too old to be valid.
     * 已经超时了 无法处理 直到时间差达到重试时间后 开始重试
     * */
    if (auth_age > auth_timeout) {
        clusterLogCantFailover(CLUSTER_CANT_FAILOVER_EXPIRED);
        return;
    }

    /* Ask for votes if needed.
     * 达到了failover_auth_time时间 并且还没有发起过请求
     * */
    if (server.cluster->failover_auth_sent == 0) {
        // 更新当前任期
        server.cluster->currentEpoch++;
        // 记录本次发起故障转移的任期
        server.cluster->failover_auth_epoch = server.cluster->currentEpoch;
        serverLog(LL_WARNING, "Starting a failover election for epoch %llu.",
                  (unsigned long long) server.cluster->currentEpoch);
        // 将投票请求发往集群中的其他节点  只有master节点可以处理 当master选择本节点时 就会返回ack信息 同时会增加 failover_auth_count
        clusterRequestFailoverAuth();
        // 代表选举请求已发送 这样短期内就不会重复发送了
        server.cluster->failover_auth_sent = 1;

        // 因为epoch发生了变化 所以要更新配置文件
        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG |
                             CLUSTER_TODO_UPDATE_STATE |
                             CLUSTER_TODO_FSYNC_CONFIG);
        return; /* Wait for replies. */
    }

    /* Check if we reached the quorum.
     * 此时本轮已经发送过请求了  现在就是不断的检测是否有足够的票数
     * */
    if (server.cluster->failover_auth_count >= needed_quorum) {
        /* We have the quorum, we can finally failover the master. */

        serverLog(LL_WARNING,
                  "Failover election won: I'm the new master.");

        /* Update my configEpoch to the epoch of the election.
         * 当某个节点晋升成功后 才会增加configEpoch
         * */
        if (myself->configEpoch < server.cluster->failover_auth_epoch) {
            myself->configEpoch = server.cluster->failover_auth_epoch;
            serverLog(LL_WARNING,
                      "configEpoch set to %llu after successful failover",
                      (unsigned long long) myself->configEpoch);
        }

        /* Take responsibility for the cluster slots.
         * 本节点晋升成master
         * */
        clusterFailoverReplaceYourMaster();
    } else {
        // 此时还没有收到足够的票数 等待下次该方法触发  此时如果新的节点晋升成功就会通知本节点 然后本节点就会重置有关故障转移的所有参数
        clusterLogCantFailover(CLUSTER_CANT_FAILOVER_WAITING_VOTES);
    }
}

/* -----------------------------------------------------------------------------
 * CLUSTER slave migration
 *
 * Slave migration is the process that allows a slave of a master that is
 * already covered by at least another slave, to "migrate" to a master that
 * is orphaned, that is, left with no working slaves.
 * ------------------------------------------------------------------------- */

/* This function is responsible to decide if this replica should be migrated
 * to a different (orphaned) master. It is called by the clusterCron() function
 * only if:
 *
 * 1) We are a slave node.
 * 2) It was detected that there is at least one orphaned master in
 *    the cluster.
 * 3) We are a slave of one of the masters with the greatest number of
 *    slaves.
 *
 * This checks are performed by the caller since it requires to iterate
 * the nodes anyway, so we spend time into clusterHandleSlaveMigration()
 * if definitely needed.
 *
 * The function is called with a pre-computed max_slaves, that is the max
 * number of working (not in FAIL state) slaves for a single master.
 *
 * Additional conditions for migration are examined inside the function.
 * 将某个主从结构下的slave转移到孤儿节点下  实际上调用该方法时 本节点所在的主从结构下的slave最多
 * @param max_slaves 此时集群中 master下最多的slave数量是多少
 */
void clusterHandleSlaveMigration(int max_slaves) {
    int j, okslaves = 0;
    // 找到本节点选择的master节点
    clusterNode *mymaster = myself->slaveof, *target = NULL, *candidate = NULL;
    dictIterator *di;
    dictEntry *de;

    /* Step 1: Don't migrate if the cluster state is not ok.
     * 本节点此时还没有连接上集群的大部分master节点 此时认为本节点不可用
     * */
    if (server.cluster->state != CLUSTER_OK) return;

    /* Step 2: Don't migrate if my master will not be left with at least
     *         'migration-barrier' slaves after my migration.
     *         master节点不应该触发该方法
     *         */
    if (mymaster == NULL) return;

    // 找到此时还有效的所有节点
    for (j = 0; j < mymaster->numslaves; j++)
        if (!nodeFailed(mymaster->slaves[j]) &&
            !nodeTimedOut(mymaster->slaves[j]))
            okslaves++;

    // 当可用的slave数量要至少大于该值 才能进行slave的迁移
    if (okslaves <= server.cluster_migration_barrier) return;

    /* Step 3: Identify a candidate for migration, and check if among the
     * masters with the greatest number of ok slaves, I'm the one with the
     * smallest node ID (the "candidate slave").
     *
     * Note: this means that eventually a replica migration will occur
     * since slaves that are reachable again always have their FAIL flag
     * cleared, so eventually there must be a candidate. At the same time
     * this does not mean that there are no race conditions possible (two
     * slaves migrating at the same time), but this is unlikely to
     * happen, and harmless when happens. */
    candidate = myself;
    di = dictGetSafeIterator(server.cluster->nodes);
    while ((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);
        int okslaves = 0, is_orphaned = 1;

        // 开始寻找孤儿master
        /* We want to migrate only if this master is working, orphaned, and
         * used to have slaves or if failed over a master that had slaves
         * (MIGRATE_TO flag). This way we only migrate to instances that were
         * supposed to have replicas. */
        if (nodeIsSlave(node) || nodeFailed(node)) is_orphaned = 0;

        // 要求孤儿节点必须携带CLUSTER_NODE_MIGRATE_TO标记
        if (!(node->flags & CLUSTER_NODE_MIGRATE_TO)) is_orphaned = 0;

        /* Check number of working slaves.
         * 孤儿节点下可用的slave必须是0
         * */
        if (nodeIsMaster(node)) okslaves = clusterCountNonFailingSlaves(node);
        if (okslaves > 0) is_orphaned = 0;

        // 此时发现了孤儿节点
        if (is_orphaned) {
            // 设置本次要转移的目的地
            if (!target && node->numslots > 0) target = node;

            /* Track the starting time of the orphaned condition for this
             * master. */
            if (!node->orphaned_time) node->orphaned_time = mstime();
        } else {
            // 本节点不再是孤儿节点  重置时间戳
            node->orphaned_time = 0;
        }

        /* Check if I'm the slave candidate for the migration: attached
         * to a master with the maximum number of slaves and with the smallest
         * node ID.
         * 此时可能会有多个master下挂载的slave数量等同于max_slaves  这里就将name最小的节点作为迁移节点
         * */
        if (okslaves == max_slaves) {
            for (j = 0; j < node->numslaves; j++) {
                if (memcmp(node->slaves[j]->name,
                           candidate->name,
                           CLUSTER_NAMELEN) < 0) {
                    candidate = node->slaves[j];
                }
            }
        }
    }
    dictReleaseIterator(di);

    /* Step 4: perform the migration if there is a target, and if I'm the
     * candidate, but only if the master is continuously orphaned for a
     * couple of seconds, so that during failovers, we give some time to
     * the natural slaves of this instance to advertise their switch from
     * the old master to the new one.
     * 只有当迁移的节点就是本节点时 才会进行真正的迁移 就是为了避免多个slave同时出现这样的判断 然后很多节点一齐迁移过去。
     * */
    if (target && candidate == myself &&
        (mstime() - target->orphaned_time) > CLUSTER_SLAVE_MIGRATION_DELAY &&
        !(server.cluster_module_flags & CLUSTER_MODULE_FLAG_NO_FAILOVER)) {
        serverLog(LL_WARNING, "Migrating to orphaned master %.40s",
                  target->name);
        // 将本节点的master修改成 target 也就是发生了node的迁移
        clusterSetMaster(target);
    }
}

/* -----------------------------------------------------------------------------
 * CLUSTER manual failover
 *
 * This are the important steps performed by slaves during a manual failover:
 * 1) User send CLUSTER FAILOVER command. The failover state is initialized
 *    setting mf_end to the millisecond unix time at which we'll abort the
 *    attempt.
 * 2) Slave sends a MFSTART message to the master requesting to pause clients
 *    for two times the manual failover timeout CLUSTER_MF_TIMEOUT.
 *    When master is paused for manual failover, it also starts to flag
 *    packets with CLUSTERMSG_FLAG0_PAUSED.
 * 3) Slave waits for master to send its replication offset flagged as PAUSED.
 * 4) If slave received the offset from the master, and its offset matches,
 *    mf_can_start is set to 1, and clusterHandleSlaveFailover() will perform
 *    the failover as usually, with the difference that the vote request
 *    will be modified to force masters to vote for a slave that has a
 *    working master.
 *
 * From the point of view of the master things are simpler: when a
 * PAUSE_CLIENTS packet is received the master sets mf_end as well and
 * the sender in mf_slave. During the time limit for the manual failover
 * the master will just send PINGs more often to this slave, flagged with
 * the PAUSED flag, so that the slave will set mf_master_offset when receiving
 * a packet from the master with this flag set.
 *
 * The gaol of the manual failover is to perform a fast failover without
 * data loss due to the asynchronous master-slave replication.
 * -------------------------------------------------------------------------- */

/* Reset the manual failover state. This works for both masters and slaves
 * as all the state about manual failover is cleared.
 *
 * The function can be used both to initialize the manual failover state at
 * startup or to abort a manual failover in progress.
 * 重置手动故障转移相关的东西
 * */
void resetManualFailover(void) {
    // 如果此时client已经处于暂停状态了 解除暂停
    if (server.cluster->mf_end && clientsArePaused()) {
        // 这里将暂停结束时间设置成0 再次调用clientsArePaused() 会从暂停状态解除
        server.clients_pause_end_time = 0;
        clientsArePaused(); /* Just use the side effect of the function. */
    }
    server.cluster->mf_end = 0; /* No manual failover in progress. */
    server.cluster->mf_can_start = 0;
    server.cluster->mf_slave = NULL;
    server.cluster->mf_master_offset = 0;
}

/* If a manual failover timed out, abort it.
 * 代表故障转移操作超时
 * */
void manualFailoverCheckTimeout(void) {
    if (server.cluster->mf_end && server.cluster->mf_end < mstime()) {
        serverLog(LL_WARNING, "Manual failover timed out.");
        resetManualFailover();
    }
}

/* This function is called from the cluster cron function in order to go
 * forward with a manual failover state machine.
 * 处理手动故障转移相关的
 * */
void clusterHandleManualFailover(void) {
    /* Return ASAP if no manual failover is in progress.
     * 代表这个是否还没有开始故障转移
     * */
    if (server.cluster->mf_end == 0) return;

    /* If mf_can_start is non-zero, the failover was already triggered so the
     * next steps are performed by clusterHandleSlaveFailover().
     * 已启动 不需要重复处理
     * */
    if (server.cluster->mf_can_start) return;

    // 当master节点发起了故障转移后 会选择某个slave节点发送一个追加了CLUSTERMSG_FLAG0_PAUSED标记的消息包 slave在处理时会读取内部的offset 代表开启故障转移时master的同步偏移量
    // 如果现在偏移量还是0 还需要等待
    if (server.cluster->mf_master_offset == 0) return; /* Wait for offset... */

    // 这里是等待副本模块完成数据同步吧
    if (server.cluster->mf_master_offset == replicationGetSlaveOffset()) {
        /* Our replication offset matches the master replication offset
         * announced after clients were paused. We can start the failover. */
        // 此时slave已经同步完master发起故障转移时偏移量对应的全部数据了 可以开始升级了
        server.cluster->mf_can_start = 1;
        serverLog(LL_WARNING,
                  "All master replication stream processed, "
                  "manual failover can start.");
    }
}

/* -----------------------------------------------------------------------------
 * CLUSTER cron job
 * -------------------------------------------------------------------------- */

/* This is executed 10 times every second
 * 在server的主循环中 每秒执行10次该方法
 * 在这里会与集群中其他节点建立连接
 * */
void clusterCron(void) {
    dictIterator *di;
    dictEntry *de;
    int update_state = 0;
    int orphaned_masters; /* How many masters there are without ok slaves. */
    int max_slaves; /* Max number of ok slaves for a single master. */

    // 本节点的master下 有效的slave
    int this_slaves; /* Number of ok slaves for our master (if we are slave). */
    mstime_t min_pong = 0, now = mstime();
    clusterNode *min_pong_node = NULL;

    // 记录总共执行了多少次循环
    static unsigned long long iteration = 0;
    mstime_t handshake_timeout;

    iteration++; /* Number of times this function was called so far. */

    /* We want to take myself->ip in sync with the cluster-announce-ip option.
     * The option can be set at runtime via CONFIG SET, so we periodically check
     * if the option changed to reflect this into myself->ip. */
    {
        static char *prev_ip = NULL;

        // 检测ip是否发生了变化 在config.c中看到该值默认为NULL 先忽略
        char *curr_ip = server.cluster_announce_ip;
        int changed = 0;

        if (prev_ip == NULL && curr_ip != NULL) changed = 1;
        else if (prev_ip != NULL && curr_ip == NULL) changed = 1;
        else if (prev_ip && curr_ip && strcmp(prev_ip, curr_ip)) changed = 1;

        if (changed) {
            if (prev_ip) zfree(prev_ip);
            prev_ip = curr_ip;

            if (curr_ip) {
                /* We always take a copy of the previous IP address, by
                 * duplicating the string. This way later we can check if
                 * the address really changed. */
                prev_ip = zstrdup(prev_ip);
                strncpy(myself->ip, server.cluster_announce_ip, NET_IP_STR_LEN);
                myself->ip[NET_IP_STR_LEN - 1] = '\0';
            } else {
                myself->ip[0] = '\0'; /* Force autodetection. */
            }
        }
    }

    /* The handshake timeout is the time after which a handshake node that was
     * not turned into a normal node is removed from the nodes. Usually it is
     * just the NODE_TIMEOUT value, but when NODE_TIMEOUT is too small we use
     * the value of 1 second.
     * 集群节点离线时间不能太短
     * */
    handshake_timeout = server.cluster_node_timeout;
    if (handshake_timeout < 1000) handshake_timeout = 1000;

    /* Update myself flags.
     * 检测myself_flags是否发生了变化
     * */
    clusterUpdateMyselfFlags();

    /* Check if we have disconnected nodes and re-establish the connection.
     * Also update a few stats while we are here, that can be used to make
     * better decisions in other part of the code.
     * 在初始化集群的时候 会从配置文件中读取信息 并填充到cluster->nodes中
     * */
    di = dictGetSafeIterator(server.cluster->nodes);
    server.cluster->stats_pfail_nodes = 0;

    // 遍历集群中的每个节点
    while ((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        /* Not interested in reconnecting the link with myself or nodes
         * for which we have no address.
         * 跳过自身 以及没有设置地址的node
         * */
        if (node->flags & (CLUSTER_NODE_MYSELF | CLUSTER_NODE_NOADDR)) continue;

        // 本节点观测到的连接不上的节点被称为 pfail节点  当多数节点都连接不上该节点时此节点被集群认为是fail节点
        if (node->flags & CLUSTER_NODE_PFAIL)
            server.cluster->stats_pfail_nodes++;

        /* A Node in HANDSHAKE state has a limited lifespan equal to the
         * configured node timeout.
         * 集群与该节点连接超时 将该节点从集群中移除
         * */
        if (nodeInHandshake(node) && now - node->ctime > handshake_timeout) {
            clusterDelNode(node);
            continue;
        }

        // link为空 代表还未尝试建立连接
        if (node->link == NULL) {
            clusterLink *link = createClusterLink(node);
            // 初始化一个conn对象 并尝试连接该node  一旦连接成功会立即发送ping请求
            link->conn = server.tls_cluster ? connCreateTLS() : connCreateSocket();
            connSetPrivateData(link->conn, link);
            // 注意连接的端口是 cport    当连接后就会触发对应的handler
            if (connConnect(link->conn, node->ip, node->cport, NET_FIRST_BIND_ADDR,
                            clusterLinkConnectHandler) == -1) {
                /* We got a synchronous error from connect before
                 * clusterSendPing() had a chance to be called.
                 * If node->ping_sent is zero, failure detection can't work,
                 * so we claim we actually sent a ping now (that will
                 * be really sent as soon as the link is obtained). */
                if (node->ping_sent == 0) node->ping_sent = mstime();
                serverLog(LL_DEBUG, "Unable to connect to "
                                    "Cluster Node [%s]:%d -> %s", node->ip,
                          node->cport, server.neterr);

                freeClusterLink(link);
                continue;
            }
            node->link = link;
        }
    }
    dictReleaseIterator(di);

    /* Ping some random node 1 time every 10 iterations, so that we usually ping
     * one random node every second.
     * 每个节点定期会往其它节点发送ping请求 主要用于同步数据
     * */
    if (!(iteration % 10)) {
        int j;

        /* Check a few random nodes and ping the one with the oldest
         * pong_received time.
         * 每次抽取5个 并从中找到最久未通信的
         * */
        for (j = 0; j < 5; j++) {
            de = dictGetRandomKey(server.cluster->nodes);
            clusterNode *this = dictGetVal(de);

            /* Don't ping nodes disconnected or with a ping currently active.
             * 忽略还未创建连接的node 如果已经向该节点发起了心跳请求 也忽略
             * */
            if (this->link == NULL || this->ping_sent != 0) continue;

            // 忽略自身 以及还处于握手阶段的节点
            if (this->flags & (CLUSTER_NODE_MYSELF | CLUSTER_NODE_HANDSHAKE))
                continue;

            // 在随机的5个节点中 找到最久的未通信的节点 并发送ping请求
            if (min_pong_node == NULL || min_pong > this->pong_received) {
                min_pong_node = this;
                min_pong = this->pong_received;
            }
        }

        // 心跳检测  还会抽取几个node的数据 以及所有失败的node数据 一并发送过去
        if (min_pong_node) {
            serverLog(LL_DEBUG, "Pinging node %.40s", min_pong_node->name);
            clusterSendPing(min_pong_node->link, CLUSTERMSG_TYPE_PING);
        }
    }

    /* Iterate nodes to check if we need to flag something as failing.
     * This loop is also responsible to:
     * 1) Check if there are orphaned masters (masters without non failing
     *    slaves).
     * 2) Count the max number of non failing slaves for a single master.
     * 3) Count the number of slaves for our master, if we are a slave.
     * 检测此时是否有某些master下没有任何slave
     * */
    orphaned_masters = 0;
    // 存储所有节点中 最多的有效slave数量
    max_slaves = 0;
    // 本节点认可的master节点此时的有效slave数量
    this_slaves = 0;
    di = dictGetSafeIterator(server.cluster->nodes);
    while ((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);
        now = mstime(); /* Use an updated time at every iteration. */

        // 跳过连接未完成的节点和自己
        if (node->flags &
            (CLUSTER_NODE_MYSELF | CLUSTER_NODE_NOADDR | CLUSTER_NODE_HANDSHAKE))
            continue;

        /* Orphaned master check, useful only if the current instance
         * is a slave that may migrate to another master.
         * 这里是数据迁移相关的 就是本节点可能会转移到某个孤儿节点 只有本节点是slave时 才有判断的必要
         * */
        if (nodeIsSlave(myself) && nodeIsMaster(node) && !nodeFailed(node)) {
            // 检测该节点下是否有非fail的slave节点
            int okslaves = clusterCountNonFailingSlaves(node);

            /* A master is orphaned if it is serving a non-zero number of
             * slots, have no working slaves, but used to have at least one
             * slave, or failed over a master that used to have slaves.
             * 孤儿节点要求此前必须分配了slot 否则就是无用功 (这个节点本身就不工作 没必要维护它下面的有效子节点数)
             * 当首次往master节点添加一个slave时 就会设置CLUSTER_NODE_MIGRATE_TO标记 也就是孤儿节点要求master之前挂载了slave 但是此时slave下线了
             * */
            if (okslaves == 0 && node->numslots > 0 &&
                node->flags & CLUSTER_NODE_MIGRATE_TO) {
                orphaned_masters++;
            }
            // 更新节点下最多的slave数量
            if (okslaves > max_slaves) max_slaves = okslaves;

            if (nodeIsSlave(myself) && myself->slaveof == node)
                this_slaves = okslaves;
        }

        /* If we are not receiving any data for more than half the cluster
         * timeout, reconnect the link: maybe there is a connection
         * issue even if the node is alive.
         * ping_sent代表最近一次发送ping请求的时间
         * data_received代表最近一次收到该节点数据的时间
         * */
        mstime_t ping_delay = now - node->ping_sent;
        mstime_t data_delay = now - node->data_received;

        // 代表长时间未收到该节点的回复信息 断开连接  在下轮循环中有会针对这些节点重新发起连接
        if (node->link && /* is connected */
            now - node->link->ctime >
            server.cluster_node_timeout && /* was not already reconnected */
            node->ping_sent && /* we already sent a ping */
            node->pong_received < node->ping_sent && /* still waiting pong */
            /* and we are waiting for the pong more than timeout/2 */
            ping_delay > server.cluster_node_timeout / 2 &&
            /* and in such interval we are not seeing any traffic at all. */
            data_delay > server.cluster_node_timeout / 2) {
            /* Disconnect the link, it will be reconnected automatically. */
            freeClusterLink(node->link);
        }

        /* If we have currently no active ping in this instance, and the
         * received PONG is older than half the cluster timeout, send
         * a new ping now, to ensure all the nodes are pinged without
         * a too big delay.
         * 上面的ping请求是抽样发送 到了这里是针对快要过期的节点发送ping请求
         * */
        if (node->link &&
            node->ping_sent == 0 &&
            (now - node->pong_received) > server.cluster_node_timeout / 2) {
            clusterSendPing(node->link, CLUSTERMSG_TYPE_PING);
            continue;
        }

        /* If we are a master and one of the slaves requested a manual
         * failover, ping it continuously.
         * 手动发起故障转移的场景 会向继任者发送ping请求 (针对故障转移会额外携带一个CLUSTERMSG_FLAG0_PAUSED标记 代表这个节点即将停止工作)
         * 这样继任者就会同步一个repl_offset
         * 实际上作为该节点的slave都会获取到CLUSTERMSG_FLAG0_PAUSED标记 并记录故障转移的偏移量
         * 这里的意思应该是确保自己期望的继任者能够感知到发起了故障转移吧(因为ping本身是抽样发送的 极端情况下可能不会发送到继任者上)
         * */
        if (server.cluster->mf_end &&
            nodeIsMaster(myself) &&
            server.cluster->mf_slave == node &&
            node->link) {
            clusterSendPing(node->link, CLUSTERMSG_TYPE_PING);
            continue;
        }

        /* Check only if we have an active ping for this instance. */
        if (node->ping_sent == 0) continue;

        /* Check if this node looks unreachable.
         * Note that if we already received the PONG, then node->ping_sent
         * is zero, so can't reach this code at all, so we don't risk of
         * checking for a PONG delay if we didn't sent the PING.
         *
         * We also consider every incoming data as proof of liveness, since
         * our cluster bus link is also used for data: under heavy data
         * load pong delays are possible.
         * 取更大的交互时间差
         * */
        mstime_t node_delay = (ping_delay < data_delay) ? ping_delay :
                              data_delay;

        // 间隔时间太长 将节点标记成失败  因为在cluster_node_timeout/2的时候应该会发起心跳 但是隔了这么久都没有收到结果 就代表该节点很可能已经下线了
        if (node_delay > server.cluster_node_timeout) {
            /* Timeout reached. Set the node as possibly failing if it is
             * not already in this state. */
            if (!(node->flags & (CLUSTER_NODE_PFAIL | CLUSTER_NODE_FAIL))) {
                serverLog(LL_DEBUG, "*** NODE %.40s possibly failing",
                          node->name);
                node->flags |= CLUSTER_NODE_PFAIL;
                update_state = 1;
            }
        }
    }
    dictReleaseIterator(di);

    /* If we are a slave node but the replication is still turned off,
     * enable it if we know the address of our master and it appears to
     * be up.
     * 在副本模块设置master
     * */
    if (nodeIsSlave(myself) &&
        server.masterhost == NULL &&
        myself->slaveof &&
        nodeHasAddr(myself->slaveof)) {
        // 这里会填充server.masterhost
        replicationSetMaster(myself->slaveof->ip, myself->slaveof->port);
    }

    /* Abort a manual failover if the timeout is reached.
     * 手动故障转移超时 清理相关数据
     * */
    manualFailoverCheckTimeout();

    // slave节点总是会判断是否需要发起故障转移  也就是当slave无法连接到master时 会采取一些措施
    if (nodeIsSlave(myself)) {

        // 该方法是处理手动故障转移 也就是master主动发起的 在slave确保同步了master要求的偏移量时 会修改一个标记 代表满足了手动故障转移的条件
        clusterHandleManualFailover();

        // 确保module中不包含 拒绝故障转移的模块
        if (!(server.cluster_module_flags & CLUSTER_MODULE_FLAG_NO_FAILOVER))
            // 处理故障转移
            clusterHandleSlaveFailover();
        /* If there are orphaned slaves, and we are a slave among the masters
         * with the max number of non-failing slaves, consider migrating to
         * the orphaned masters. Note that it does not make sense to try
         * a migration if there is no master with at least *two* working
         * slaves.
         * 只有当本节点是slave时 才会处理孤儿节点 当本节点所在的主从结构是当前集群下slave最多的时候，会尝试将部分slave转移到孤儿节点下
         * 相当于是一个集群的自适应平衡
         * */
        if (orphaned_masters && max_slaves >= 2 && this_slaves == max_slaves)
            clusterHandleSlaveMigration(max_slaves);
    }

    // 可能有master节点下线 或者上线 会影响到当前集群的运行状态
    if (update_state || server.cluster->state == CLUSTER_FAIL)
        clusterUpdateState();
}

/* This function is called before the event handler returns to sleep for
 * events. It is useful to perform operations that must be done ASAP in
 * reaction to events fired but that are not safe to perform inside event
 * handlers, or to perform potentially expansive tasks that we need to do
 * a single time before replying to clients.
 * */
void clusterBeforeSleep(void) {
    /* Handle failover, this is needed when it is likely that there is already
     * the quorum from masters in order to react fast.
     * 代表需要进行故障转移
     * */
    if (server.cluster->todo_before_sleep & CLUSTER_TODO_HANDLE_FAILOVER)
        clusterHandleSlaveFailover();

    /* Update the cluster state. */
    if (server.cluster->todo_before_sleep & CLUSTER_TODO_UPDATE_STATE)
        clusterUpdateState();

    /* Save the config, possibly using fsync. */
    if (server.cluster->todo_before_sleep & CLUSTER_TODO_SAVE_CONFIG) {
        int fsync = server.cluster->todo_before_sleep &
                    CLUSTER_TODO_FSYNC_CONFIG;
        clusterSaveConfigOrDie(fsync);
    }

    /* Reset our flags (not strictly needed since every single function
     * called for flags set should be able to clear its flag). */
    server.cluster->todo_before_sleep = 0;
}

void clusterDoBeforeSleep(int flags) {
    server.cluster->todo_before_sleep |= flags;
}

/* -----------------------------------------------------------------------------
 * Slots management
 * -------------------------------------------------------------------------- */

/* Test bit 'pos' in a generic bitmap. Return 1 if the bit is set,
 * otherwise 0. */
int bitmapTestBit(unsigned char *bitmap, int pos) {
    off_t byte = pos / 8;
    int bit = pos & 7;
    return (bitmap[byte] & (1 << bit)) != 0;
}

/* Set the bit at position 'pos' in a bitmap. */
void bitmapSetBit(unsigned char *bitmap, int pos) {
    off_t byte = pos / 8;
    int bit = pos & 7;
    bitmap[byte] |= 1 << bit;
}

/* Clear the bit at position 'pos' in a bitmap. */
void bitmapClearBit(unsigned char *bitmap, int pos) {
    off_t byte = pos / 8;
    int bit = pos & 7;
    bitmap[byte] &= ~(1 << bit);
}

/* Return non-zero if there is at least one master with slaves in the cluster.
 * Otherwise zero is returned. Used by clusterNodeSetSlotBit() to set the
 * MIGRATE_TO flag the when a master gets the first slot. */
int clusterMastersHaveSlaves(void) {
    dictIterator *di = dictGetSafeIterator(server.cluster->nodes);
    dictEntry *de;
    int slaves = 0;
    while ((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (nodeIsSlave(node)) continue;
        slaves += node->numslaves;
    }
    dictReleaseIterator(di);
    return slaves != 0;
}

/* Set the slot bit and return the old value.
 * 在位图中设置信息
 * */
int clusterNodeSetSlotBit(clusterNode *n, int slot) {
    int old = bitmapTestBit(n->slots, slot);
    bitmapSetBit(n->slots, slot);
    // 同一个slot还可以使用2次???
    if (!old) {
        n->numslots++;
        /* When a master gets its first slot, even if it has no slaves,
         * it gets flagged with MIGRATE_TO, that is, the master is a valid
         * target for replicas migration, if and only if at least one of
         * the other masters has slaves right now.
         *
         * Normally masters are valid targets of replica migration if:
         * 1. The used to have slaves (but no longer have).
         * 2. They are slaves failing over a master that used to have slaves.
         *
         * However new masters with slots assigned are considered valid
         * migration targets if the rest of the cluster is not a slave-less.
         *
         * See https://github.com/antirez/redis/issues/3043 for more info. */
        if (n->numslots == 1 && clusterMastersHaveSlaves())
            n->flags |= CLUSTER_NODE_MIGRATE_TO;
    }
    return old;
}

/* Clear the slot bit and return the old value. */
int clusterNodeClearSlotBit(clusterNode *n, int slot) {
    int old = bitmapTestBit(n->slots, slot);
    bitmapClearBit(n->slots, slot);
    if (old) n->numslots--;
    return old;
}

/* Return the slot bit from the cluster node structure. */
int clusterNodeGetSlotBit(clusterNode *n, int slot) {
    return bitmapTestBit(n->slots, slot);
}

/* Add the specified slot to the list of slots that node 'n' will
 * serve. Return C_OK if the operation ended with success.
 * If the slot is already assigned to another instance this is considered
 * an error and C_ERR is returned.
 * 该slot属于这个node  通过位图算法记录 节省内存并且直观
 * */
int clusterAddSlot(clusterNode *n, int slot) {
    // 这里有一个反向记录 集群中每个slot属于哪个节点
    if (server.cluster->slots[slot]) return C_ERR;
    clusterNodeSetSlotBit(n, slot);
    server.cluster->slots[slot] = n;
    return C_OK;
}

/* Delete the specified slot marking it as unassigned.
 * Returns C_OK if the slot was assigned, otherwise if the slot was
 * already unassigned C_ERR is returned.
 * 代表本节点不再占有这个slot
 * */
int clusterDelSlot(int slot) {
    clusterNode *n = server.cluster->slots[slot];

    if (!n) return C_ERR;
    serverAssert(clusterNodeClearSlotBit(n, slot) == 1);
    server.cluster->slots[slot] = NULL;
    return C_OK;
}

/* Delete all the slots associated with the specified node.
 * The number of deleted slots is returned.
 * 清理该节点分配的所有slot
 * */
int clusterDelNodeSlots(clusterNode *node) {
    int deleted = 0, j;

    for (j = 0; j < CLUSTER_SLOTS; j++) {
        if (clusterNodeGetSlotBit(node, j)) {
            clusterDelSlot(j);
            deleted++;
        }
    }
    return deleted;
}

/* Clear the migrating / importing state for all the slots.
 * This is useful at initialization and when turning a master into slave.
 * 这里记录的数据的迁移 将有关迁移的信息清空
 * */
void clusterCloseAllSlots(void) {
    memset(server.cluster->migrating_slots_to, 0,
           sizeof(server.cluster->migrating_slots_to));
    memset(server.cluster->importing_slots_from, 0,
           sizeof(server.cluster->importing_slots_from));
}

/* -----------------------------------------------------------------------------
 * Cluster state evaluation function
 * -------------------------------------------------------------------------- */

/* The following are defines that are only used in the evaluation function
 * and are based on heuristics. Actually the main point about the rejoin and
 * writable delay is that they should be a few orders of magnitude larger
 * than the network latency. */
#define CLUSTER_MAX_REJOIN_DELAY 5000
#define CLUSTER_MIN_REJOIN_DELAY 500
#define CLUSTER_WRITABLE_DELAY 2000

/*
 * 更新集群状态
 */
void clusterUpdateState(void) {
    int j, new_state;

    // 此时集群中有多少有效的master是可达的 有效就是指master有自己的slots
    int reachable_masters = 0;
    // 最近一次失败的时间
    static mstime_t among_minority_time;
    // 静态属性 记录首次调用该方法的时间
    static mstime_t first_call_time = 0;

    // 因为这里已经执行了更新操作 在before_sleep中就不需要重复执行了
    server.cluster->todo_before_sleep &= ~CLUSTER_TODO_UPDATE_STATE;

    /* If this is a master node, wait some time before turning the state
     * into OK, since it is not a good idea to rejoin the cluster as a writable
     * master, after a reboot, without giving the cluster a chance to
     * reconfigure this node. Note that the delay is calculated starting from
     * the first call to this function and not since the server start, in order
     * to don't count the DB loading time. */
    if (first_call_time == 0) first_call_time = mstime();

    // 刚启动一段时间内 无法修改集群状态 主要是给连接其他master节点一些时间
    if (nodeIsMaster(myself) &&
        server.cluster->state == CLUSTER_FAIL &&
        mstime() - first_call_time < CLUSTER_WRITABLE_DELAY)
        return;

    /* Start assuming the state is OK. We'll turn it into FAIL if there
     * are the right conditions. */
    new_state = CLUSTER_OK;

    /* Check if all the slots are covered.
     * 如果要求所有的slot都处于使用状态  只要有一个slot不可用 就认为整个cluster不可用
     * */
    if (server.cluster_require_full_coverage) {
        for (j = 0; j < CLUSTER_SLOTS; j++) {
            // 代表当前slot 还未分配 或者分配的节点 此时处于不可用状态
            if (server.cluster->slots[j] == NULL ||
                server.cluster->slots[j]->flags & (CLUSTER_NODE_FAIL)) {
                new_state = CLUSTER_FAIL;
                break;
            }
        }
    }

    /* Compute the cluster size, that is the number of master nodes
     * serving at least a single slot.
     *
     * At the same time count the number of reachable masters having
     * at least one slot. */
    {
        dictIterator *di;
        dictEntry *de;

        server.cluster->size = 0;
        di = dictGetSafeIterator(server.cluster->nodes);
        while ((de = dictNext(di)) != NULL) {
            clusterNode *node = dictGetVal(de);

            // 找到master节点 并且该节点下分配了 slot
            if (nodeIsMaster(node) && node->numslots) {
                server.cluster->size++;
                // 当前节点没有被打上 fail标记
                if ((node->flags & (CLUSTER_NODE_FAIL | CLUSTER_NODE_PFAIL)) == 0)
                    reachable_masters++;
            }
        }
        dictReleaseIterator(di);
    }

    /* If we are in a minority partition, change the cluster state
     * to FAIL.
     * */
    {
        int needed_quorum = (server.cluster->size / 2) + 1;
        // 当前节点如果无法访问到集群中一半的master节点 认为集群处于不可用状态
        if (reachable_masters < needed_quorum) {
            new_state = CLUSTER_FAIL;
            among_minority_time = mstime();
        }
    }

    /* Log a state change
     * 代表集群状态 发生了变化 需要打印日志
     * */
    if (new_state != server.cluster->state) {
        mstime_t rejoin_delay = server.cluster_node_timeout;

        /* If the instance is a master and was partitioned away with the
         * minority, don't let it accept queries for some time after the
         * partition heals, to make sure there is enough time to receive
         * a configuration update. */
        if (rejoin_delay > CLUSTER_MAX_REJOIN_DELAY)
            rejoin_delay = CLUSTER_MAX_REJOIN_DELAY;
        if (rejoin_delay < CLUSTER_MIN_REJOIN_DELAY)
            rejoin_delay = CLUSTER_MIN_REJOIN_DELAY;

        if (new_state == CLUSTER_OK &&
            nodeIsMaster(myself) &&
            mstime() - among_minority_time < rejoin_delay) {
            return;
        }

        /* Change the state and log the event. */
        serverLog(LL_WARNING, "Cluster state changed: %s",
                  new_state == CLUSTER_OK ? "ok" : "fail");
        server.cluster->state = new_state;
    }
}

/* This function is called after the node startup in order to verify that data
 * loaded from disk is in agreement with the cluster configuration:
 *
 * 1) If we find keys about hash slots we have no responsibility for, the
 *    following happens:
 *    A) If no other node is in charge according to the current cluster
 *       configuration, we add these slots to our node.
 *    B) If according to our config other nodes are already in charge for
 *       this slots, we set the slots as IMPORTING from our point of view
 *       in order to justify we have those slots, and in order to make
 *       redis-trib aware of the issue, so that it can try to fix it.
 * 2) If we find data in a DB different than DB0 we return C_ERR to
 *    signal the caller it should quit the server with an error message
 *    or take other actions.
 *
 * The function always returns C_OK even if it will try to correct
 * the error described in "1". However if data is found in DB different
 * from DB0, C_ERR is returned.
 *
 * The function also uses the logging facility in order to warn the user
 * about desynchronizations between the data we have in memory and the
 * cluster configuration.
 * 这里主要是自动分配slot
 * 此时已经完成了数据恢复
 * */
int verifyClusterConfigWithData(void) {
    int j;
    int update_config = 0;

    /* Return ASAP if a module disabled cluster redirections. In that case
     * every master can store keys about every possible hash slot.
     * 如果本身不支持重定向 直接可以忽略集群了
     * */
    if (server.cluster_module_flags & CLUSTER_MODULE_FLAG_NO_REDIRECTION)
        return C_OK;

    /* If this node is a slave, don't perform the check at all as we
     * completely depend on the replication stream.
     * slave节点不需要数据校验
     * */
    if (nodeIsSlave(myself)) return C_OK;

    /* Make sure we only have keys in DB0.
     * master节点要求只能在db0存储数据
     * */
    for (j = 1; j < server.dbnum; j++) {
        if (dictSize(server.db[j].dict)) return C_ERR;
    }

    /* Check that all the slots we see populated memory have a corresponding
     * entry in the cluster table. Otherwise fix the table.
     * 根据此时db中的数据 来反推当前master所占有的slots
     * */
    for (j = 0; j < CLUSTER_SLOTS; j++) {
        // 跳过没有数据的slot
        if (!countKeysInSlot(j)) continue; /* No keys in this slot. */
        /* Check if we are assigned to this slot or if we are importing it.
         * In both cases check the next slot as the configuration makes
         * sense. */
        if (server.cluster->slots[j] == myself ||
            server.cluster->importing_slots_from[j] != NULL)
            continue;

        /* If we are here data and cluster config don't agree, and we have
         * slot 'j' populated even if we are not importing it, nor we are
         * assigned to this slot. Fix this condition. */

        update_config++;
        /* Case A: slot is unassigned. Take responsibility for it.
         * 自动将这个slot划到本节点下
         * */
        if (server.cluster->slots[j] == NULL) {
            serverLog(LL_WARNING, "I have keys for unassigned slot %d. "
                                  "Taking responsibility for it.", j);
            clusterAddSlot(myself, j);
        } else {
            serverLog(LL_WARNING, "I have keys for slot %d, but the slot is "
                                  "assigned to another node. "
                                  "Setting it to importing state.", j);
            server.cluster->importing_slots_from[j] = server.cluster->slots[j];
        }
    }
    if (update_config) clusterSaveConfigOrDie(1);
    return C_OK;
}

/* -----------------------------------------------------------------------------
 * SLAVE nodes handling
 * -------------------------------------------------------------------------- */

/* Set the specified node 'n' as master for this node.
 * If this node is currently a master, it is turned into a slave.
 * myself将n作为新的master节点
 * */
void clusterSetMaster(clusterNode *n) {
    serverAssert(n != myself);
    // 只有当本节点此时没有任何slot时 才能更换master 当作为master时 对应手动故障转移就会清空slots 作为slave本身slots就是0
    serverAssert(myself->numslots == 0);

    if (nodeIsMaster(myself)) {
        myself->flags &= ~(CLUSTER_NODE_MASTER | CLUSTER_NODE_MIGRATE_TO);
        myself->flags |= CLUSTER_NODE_SLAVE;
        // 这里将迁移信息也清除
        clusterCloseAllSlots();
    } else {
        // 将本节点从原本连接的master节点处移除
        if (myself->slaveof)
            clusterNodeRemoveSlave(myself->slaveof, myself);
    }
    myself->slaveof = n;
    clusterNodeAddSlave(n, myself);
    // 数据同步/复制应该是监听某种事件 比如master变更事件 之后自动发送数据同步的请求
    replicationSetMaster(n->ip, n->port);
    // 因为当前节点的master已经换成了一个可靠的节点了 所以有关故障转移的信息要重置
    resetManualFailover();
}

/* -----------------------------------------------------------------------------
 * Nodes to string representation functions.
 * -------------------------------------------------------------------------- */

struct redisNodeFlags {
    uint16_t flag;
    char *name;
};

static struct redisNodeFlags redisNodeFlagsTable[] = {
        {CLUSTER_NODE_MYSELF,     "myself,"},
        {CLUSTER_NODE_MASTER,     "master,"},
        {CLUSTER_NODE_SLAVE,      "slave,"},
        {CLUSTER_NODE_PFAIL,      "fail?,"},
        {CLUSTER_NODE_FAIL,       "fail,"},
        {CLUSTER_NODE_HANDSHAKE,  "handshake,"},
        {CLUSTER_NODE_NOADDR,     "noaddr,"},
        {CLUSTER_NODE_NOFAILOVER, "nofailover,"}
};

/* Concatenate the comma separated list of node flags to the given SDS
 * string 'ci'. */
sds representClusterNodeFlags(sds ci, uint16_t flags) {
    size_t orig_len = sdslen(ci);
    int i, size = sizeof(redisNodeFlagsTable) / sizeof(struct redisNodeFlags);
    for (i = 0; i < size; i++) {
        struct redisNodeFlags *nodeflag = redisNodeFlagsTable + i;
        if (flags & nodeflag->flag) ci = sdscat(ci, nodeflag->name);
    }
    /* If no flag was added, add the "noflags" special flag. */
    if (sdslen(ci) == orig_len) ci = sdscat(ci, "noflags,");
    sdsIncrLen(ci, -1); /* Remove trailing comma. */
    return ci;
}

/* Generate a csv-alike representation of the specified cluster node.
 * See clusterGenNodesDescription() top comment for more information.
 *
 * The function returns the string representation as an SDS string.
 * 这里的逻辑与loadConfig相匹配
 * */
sds clusterGenNodeDescription(clusterNode *node) {
    int j, start;
    sds ci;

    /* Node coordinates */
    ci = sdscatprintf(sdsempty(), "%.40s %s:%d@%d ",
                      node->name,
                      node->ip,
                      node->port,
                      node->cport);

    /* Flags */
    ci = representClusterNodeFlags(ci, node->flags);

    /* Slave of... or just "-" */
    if (node->slaveof)
        ci = sdscatprintf(ci, " %.40s ", node->slaveof->name);
    else
        ci = sdscatlen(ci, " - ", 3);

    unsigned long long nodeEpoch = node->configEpoch;
    if (nodeIsSlave(node) && node->slaveof) {
        nodeEpoch = node->slaveof->configEpoch;
    }
    /* Latency from the POV of this node, config epoch, link status */
    ci = sdscatprintf(ci, "%lld %lld %llu %s",
                      (long long) node->ping_sent,
                      (long long) node->pong_received,
                      nodeEpoch,
                      (node->link || node->flags & CLUSTER_NODE_MYSELF) ?
                      "connected" : "disconnected");

    /* Slots served by this instance
     * 将slot转换成多个范围写入
     * */
    start = -1;
    for (j = 0; j < CLUSTER_SLOTS; j++) {
        int bit;

        if ((bit = clusterNodeGetSlotBit(node, j)) != 0) {
            if (start == -1) start = j;
        }
        if (start != -1 && (!bit || j == CLUSTER_SLOTS - 1)) {
            if (bit && j == CLUSTER_SLOTS - 1) j++;

            if (start == j - 1) {
                ci = sdscatprintf(ci, " %d", start);
            } else {
                ci = sdscatprintf(ci, " %d-%d", start, j - 1);
            }
            start = -1;
        }
    }

    /* Just for MYSELF node we also dump info about slots that
     * we are migrating to other instances or importing from other
     * instances.
     * 每个节点产生的 cluster_config 只会记录本节点内数据的迁移记录
     * */
    if (node->flags & CLUSTER_NODE_MYSELF) {
        for (j = 0; j < CLUSTER_SLOTS; j++) {
            if (server.cluster->migrating_slots_to[j]) {
                ci = sdscatprintf(ci, " [%d->-%.40s]", j,
                                  server.cluster->migrating_slots_to[j]->name);
            } else if (server.cluster->importing_slots_from[j]) {
                ci = sdscatprintf(ci, " [%d-<-%.40s]", j,
                                  server.cluster->importing_slots_from[j]->name);
            }
        }
    }
    return ci;
}

/* Generate a csv-alike representation of the nodes we are aware of,
 * including the "myself" node, and return an SDS string containing the
 * representation (it is up to the caller to free it).
 *
 * All the nodes matching at least one of the node flags specified in
 * "filter" are excluded from the output, so using zero as a filter will
 * include all the known nodes in the representation, including nodes in
 * the HANDSHAKE state.
 *
 * The representation obtained using this function is used for the output
 * of the CLUSTER NODES function, and as format for the cluster
 * configuration file (nodes.conf) for a given node.
 * 为集群中每个节点生成描述信息 这些信息之后会被保存到cluster_config中
 * */
sds clusterGenNodesDescription(int filter) {
    sds ci = sdsempty(), ni;
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(server.cluster->nodes);
    while ((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);

        if (node->flags & filter) continue;
        ni = clusterGenNodeDescription(node);
        ci = sdscatsds(ci, ni);
        sdsfree(ni);
        ci = sdscatlen(ci, "\n", 1);
    }
    dictReleaseIterator(di);
    return ci;
}

/* -----------------------------------------------------------------------------
 * CLUSTER command
 * -------------------------------------------------------------------------- */

const char *clusterGetMessageTypeString(int type) {
    switch (type) {
        case CLUSTERMSG_TYPE_PING:
            return "ping";
        case CLUSTERMSG_TYPE_PONG:
            return "pong";
        case CLUSTERMSG_TYPE_MEET:
            return "meet";
        case CLUSTERMSG_TYPE_FAIL:
            return "fail";
        case CLUSTERMSG_TYPE_PUBLISH:
            return "publish";
        case CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST:
            return "auth-req";
        case CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK:
            return "auth-ack";
        case CLUSTERMSG_TYPE_UPDATE:
            return "update";
        case CLUSTERMSG_TYPE_MFSTART:
            return "mfstart";
        case CLUSTERMSG_TYPE_MODULE:
            return "module";
    }
    return "unknown";
}

int getSlotOrReply(client *c, robj *o) {
    long long slot;

    if (getLongLongFromObject(o, &slot) != C_OK ||
        slot < 0 || slot >= CLUSTER_SLOTS) {
        addReplyError(c, "Invalid or out of range slot");
        return -1;
    }
    return (int) slot;
}

/**
 * 将所有slots信息返回
 * @param c
 */
void clusterReplyMultiBulkSlots(client *c) {
    /* Format: 1) 1) start slot
     *            2) end slot
     *            3) 1) master IP
     *               2) master port
     *               3) node ID
     *            4) 1) replica IP
     *               2) replica port
     *               3) node ID
     *           ... continued until done
     */

    int num_masters = 0;
    void *slot_replylen = addReplyDeferredLen(c);

    dictEntry *de;
    dictIterator *di = dictGetSafeIterator(server.cluster->nodes);

    // 遍历所有节点
    while ((de = dictNext(di)) != NULL) {
        clusterNode *node = dictGetVal(de);
        int j = 0, start = -1;
        int i, nested_elements = 0;

        /* Skip slaves (that are iterated when producing the output of their
         * master) and  masters not serving any slot.
         * 集群中同时存在 slave 和master节点 并且master节点应该有多个  或者当前节点没有被分配到slot slot应该是由master分配给多个slave的 master到底是几个呢
         * */
        if (!nodeIsMaster(node) || node->numslots == 0) continue;

        // 跳过此时不可用的节点  长时间脱离集群的节点被标记成fail
        // nested_elements 是记录所有有效的节点么 ???
        for (i = 0; i < node->numslaves; i++) {
            if (nodeFailed(node->slaves[i])) continue;
            nested_elements++;
        }

        // 遍历 获取所有slot的信息
        for (j = 0; j < CLUSTER_SLOTS; j++) {
            int bit, i;

            // 每当确认完一个范围后 就会重置start 然后当首次发现该slot属于本节点时 又会设置start
            if ((bit = clusterNodeGetSlotBit(node, j)) != 0) {
                if (start == -1) start = j;
            }

            // 看来slot是连续分配的 从slot开始匹配上该节点到slot不属于node的这个范围内 所有slot的信息都需要返回 或者此时已经读取到最后一个slot了 也需要生成数据
            if (start != -1 && (!bit || j == CLUSTER_SLOTS - 1)) {
                addReplyArrayLen(c, nested_elements + 3); /* slots (2) + master addr (1). */

                // 由于第二个原因发送数据 需要对j+1
                if (bit && j == CLUSTER_SLOTS - 1) j++;

                /* If slot exists in output map, add to it's list.
                 * else, create a new output map for this slot
                 * 代表分配到该node下的slot只有一个 这里是将slot的起始下标和终止下标发送过去
                 * */
                if (start == j - 1) {
                    addReplyLongLong(c, start); /* only one slot; low==high */
                    addReplyLongLong(c, start);
                } else {
                    addReplyLongLong(c, start); /* low */
                    addReplyLongLong(c, j - 1);   /* high */
                }

                // 本次范围已经确认 重置start
                start = -1;

                /* First node reply position is always the master
                 * 先写入node的相关信息 这是不变的
                 * */
                addReplyArrayLen(c, 3);
                addReplyBulkCString(c, node->ip);
                addReplyLongLong(c, node->port);
                addReplyBulkCBuffer(c, node->name, CLUSTER_NAMELEN);

                // 将该节点下所有的slave信息也写进去
                /* Remaining nodes in reply are replicas for slot range */
                for (i = 0; i < node->numslaves; i++) {
                    /* This loop is copy/pasted from clusterGenNodeDescription()
                     * with modifications for per-slot node aggregation */
                    if (nodeFailed(node->slaves[i])) continue;
                    addReplyArrayLen(c, 3);
                    addReplyBulkCString(c, node->slaves[i]->ip);
                    addReplyLongLong(c, node->slaves[i]->port);
                    addReplyBulkCBuffer(c, node->slaves[i]->name, CLUSTER_NAMELEN);
                }
                num_masters++;
            }
        }
    }
    dictReleaseIterator(di);
    setDeferredArrayLen(c, slot_replylen, num_masters);
}

/**
 * 所有集群相关的command都在里面 通过阅读内部逻辑可以大体知道cluster是做什么的
 */
void clusterCommand(client *c) {

    // 当前节点并没有以集群模式开启 无法执行集群命令
    if (server.cluster_enabled == 0) {
        addReplyError(c, "This instance has cluster support disabled");
        return;
    }

    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr, "help")) {
        const char *help[] = {
                "ADDSLOTS <slot> [slot ...] -- Assign slots to current node.",
                "BUMPEPOCH -- Advance the cluster config epoch.",
                "COUNT-failure-reports <node-id> -- Return number of failure reports for <node-id>.",
                "COUNTKEYSINSLOT <slot> - Return the number of keys in <slot>.",
                "DELSLOTS <slot> [slot ...] -- Delete slots information from current node.",
                "FAILOVER [force|takeover] -- Promote current replica node to being a master.",
                "FORGET <node-id> -- Remove a node from the cluster.",
                "GETKEYSINSLOT <slot> <count> -- Return key names stored by current node in a slot.",
                "FLUSHSLOTS -- Delete current node own slots information.",
                "INFO - Return information about the cluster.",
                "KEYSLOT <key> -- Return the hash slot for <key>.",
                "MEET <ip> <port> [bus-port] -- Connect nodes into a working cluster.",
                "MYID -- Return the node id.",
                "NODES -- Return cluster configuration seen by node. Output format:",
                "    <id> <ip:port> <flags> <master> <pings> <pongs> <epoch> <link> <slot> ... <slot>",
                "REPLICATE <node-id> -- Configure current node as replica to <node-id>.",
                "RESET [hard|soft] -- Reset current node (default: soft).",
                "SET-config-epoch <epoch> - Set config epoch of current node.",
                "SETSLOT <slot> (importing|migrating|stable|node <node-id>) -- Set slot state.",
                "REPLICAS <node-id> -- Return <node-id> replicas.",
                "SAVECONFIG - Force saving cluster configuration on disk.",
                "SLOTS -- Return information about slots range mappings. Each range is made of:",
                "    start, end, master and replicas IP addresses, ports and ids",
                NULL
        };
        addReplyHelp(c, help);
        // meet是用于集群中探测新节点的
    } else if (!strcasecmp(c->argv[1]->ptr, "meet") && (c->argc == 4 || c->argc == 5)) {
        /* CLUSTER MEET <ip> <port> [cport]
         * 4,5号参数 分别代表 port cport  cport是redis集群内节点通信使用的端口
         * */
        long long port, cport;

        if (getLongLongFromObject(c->argv[3], &port) != C_OK) {
            addReplyErrorFormat(c, "Invalid TCP base port specified: %s",
                                (char *) c->argv[3]->ptr);
            return;
        }

        if (c->argc == 5) {
            if (getLongLongFromObject(c->argv[4], &cport) != C_OK) {
                addReplyErrorFormat(c, "Invalid TCP bus port specified: %s",
                                    (char *) c->argv[4]->ptr);
                return;
            }
        } else {
            cport = port + CLUSTER_PORT_INCR;
        }

        // 尝试与指定的地址进行连接   这里只需要将ip port包装成node 之后会在clusterCron中自动进行连接
        if (clusterStartHandshake(c->argv[2]->ptr, port, cport) == 0 &&
            errno == EINVAL) {
            addReplyErrorFormat(c, "Invalid node address specified: %s:%s",
                                (char *) c->argv[2]->ptr, (char *) c->argv[3]->ptr);
        } else {
            addReply(c, shared.ok);
        }
        // 返回当前节点观测到的其他节点信息
    } else if (!strcasecmp(c->argv[1]->ptr, "nodes") && c->argc == 2) {
        /* CLUSTER NODES */
        sds nodes = clusterGenNodesDescription(0);
        addReplyVerbatim(c, nodes, sdslen(nodes), "txt");
        sdsfree(nodes);
        // 将本节点的nodename返回
    } else if (!strcasecmp(c->argv[1]->ptr, "myid") && c->argc == 2) {
        /* CLUSTER MYID */
        addReplyBulkCBuffer(c, myself->name, CLUSTER_NAMELEN);
        // 将所有slots信息返回  slot是连续分配的
    } else if (!strcasecmp(c->argv[1]->ptr, "slots") && c->argc == 2) {
        /* CLUSTER SLOTS */
        clusterReplyMultiBulkSlots(c);
        // 这个flushslots实际上是要删除本节点下这些slot的数据
    } else if (!strcasecmp(c->argv[1]->ptr, "flushslots") && c->argc == 2) {
        /* CLUSTER FLUSHSLOTS
         * 要求db[0]必须为空
         * */
        if (dictSize(server.db[0].dict) != 0) {
            addReplyError(c, "DB must be empty to perform CLUSTER FLUSHSLOTS.");
            return;
        }
        // 只有当本节点是master时 才会真正删除数据
        clusterDelNodeSlots(myself);
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE | CLUSTER_TODO_SAVE_CONFIG);
        addReply(c, shared.ok);
        // 除了在初始化阶段会根据db中的数据 自动设置占有的slot 还可以手动指定某个节点此时拥有的slot
    } else if ((!strcasecmp(c->argv[1]->ptr, "addslots") ||
                !strcasecmp(c->argv[1]->ptr, "delslots")) && c->argc >= 3) {
        /* CLUSTER ADDSLOTS <slot> [slot] ... */
        /* CLUSTER DELSLOTS <slot> [slot] ... */
        int j, slot;
        unsigned char *slots = zmalloc(CLUSTER_SLOTS);
        // 代表是删除操作
        int del = !strcasecmp(c->argv[1]->ptr, "delslots");

        // 先重置结果容器 c语言要求手动执行内存释放工作
        memset(slots, 0, CLUSTER_SLOTS);
        /* Check that all the arguments are parseable and that all the
         * slots are not already busy.
         * 第一个参数是 clusterCommand
         * 第二个参数是 addslots/delslots
         * */
        for (j = 2; j < c->argc; j++) {

            // 没有解析到 slot信息 本次操作终止  解析出来的应该是一个longlong类型的值
            if ((slot = getSlotOrReply(c, c->argv[j])) == -1) {
                zfree(slots);
                return;
            }
            // 当指定的slot已经被删除时 直接返回
            if (del && server.cluster->slots[slot] == NULL) {
                addReplyErrorFormat(c, "Slot %d is already unassigned", slot);
                zfree(slots);
                return;
                // 当要添加的slot已经存在时 直接返回
            } else if (!del && server.cluster->slots[slot]) {
                addReplyErrorFormat(c, "Slot %d is already busy", slot);
                zfree(slots);
                return;
            }
            // 这里主要是判断本次command操作的slot是否有重复
            if (slots[slot]++ == 1) {
                addReplyErrorFormat(c, "Slot %d specified multiple times",
                                    (int) slot);
                zfree(slots);
                return;
            }
        }

        // 现在才是真正处理 cluster.slots 上面的逻辑只是做一些校验
        for (j = 0; j < CLUSTER_SLOTS; j++) {
            if (slots[j]) {
                int retval;

                /* If this slot was set as importing we can clear this
                 * state as now we are the real owner of the slot.
                 * 如果该slot正在导入数据 清除这个标记
                 * */
                if (server.cluster->importing_slots_from[j])
                    server.cluster->importing_slots_from[j] = NULL;

                // 更新node.slots信息  如果删除了slot 不需要在db中移除对应的数据吗
                retval = del ? clusterDelSlot(j) :
                         clusterAddSlot(myself, j);
                serverAssertWithInfo(c, NULL, retval == C_OK);
            }
        }

        // 操作完毕后释放slots
        zfree(slots);
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE | CLUSTER_TODO_SAVE_CONFIG);
        addReply(c, shared.ok);
        // 这个是数据迁移相关的
    } else if (!strcasecmp(c->argv[1]->ptr, "setslot") && c->argc >= 4) {
        /* SETSLOT 10 MIGRATING <node ID> */
        /* SETSLOT 10 IMPORTING <node ID> */
        /* SETSLOT 10 STABLE */
        /* SETSLOT 10 NODE <node ID> */
        int slot;
        clusterNode *n;

        // 只有master节点可以处理这个command
        if (nodeIsSlave(myself)) {
            addReplyError(c, "Please use SETSLOT only with masters.");
            return;
        }

        // 本次操作涉及到的是哪个slot
        if ((slot = getSlotOrReply(c, c->argv[2])) == -1) return;

        // 代表本次是迁出操作
        if (!strcasecmp(c->argv[3]->ptr, "migrating") && c->argc == 5) {

            // 如果当前slot不属于本节点 无法迁出
            if (server.cluster->slots[slot] != myself) {
                addReplyErrorFormat(c, "I'm not the owner of hash slot %u", slot);
                return;
            }
            // 目标节点没有被本节点观测到 无法移动
            if ((n = clusterLookupNode(c->argv[4]->ptr)) == NULL) {
                addReplyErrorFormat(c, "I don't know about node %s",
                                    (char *) c->argv[4]->ptr);
                return;
            }
            // 设置数据迁出的记录  注意这里并没有立即进行迁移相关的操作
            server.cluster->migrating_slots_to[slot] = n;
            // 需要从某个节点迁入某个slot的数据
        } else if (!strcasecmp(c->argv[3]->ptr, "importing") && c->argc == 5) {
            // 此时该slot不能被本节点拥有
            if (server.cluster->slots[slot] == myself) {
                addReplyErrorFormat(c,
                                    "I'm already the owner of hash slot %u", slot);
                return;
            }
            // 目标节点不存在 无法告知迁移请求
            if ((n = clusterLookupNode(c->argv[4]->ptr)) == NULL) {
                addReplyErrorFormat(c, "I don't know about node %s",
                                    (char *) c->argv[4]->ptr);
                return;
            }
            server.cluster->importing_slots_from[slot] = n;
            // 清理之前的迁移标记
        } else if (!strcasecmp(c->argv[3]->ptr, "stable") && c->argc == 4) {
            /* CLUSTER SETSLOT <SLOT> STABLE */
            server.cluster->importing_slots_from[slot] = NULL;
            server.cluster->migrating_slots_to[slot] = NULL;
            // 代表将某个slot分配到该node下  不涉及数据迁移
        } else if (!strcasecmp(c->argv[3]->ptr, "node") && c->argc == 5) {
            /* CLUSTER SETSLOT <SLOT> NODE <NODE ID>
             * */
            clusterNode *n = clusterLookupNode(c->argv[4]->ptr);

            // 本节点此时没有维护该节点信息 无法处理
            if (!n) {
                addReplyErrorFormat(c, "Unknown node %s",
                                    (char *) c->argv[4]->ptr);
                return;
            }
            /* If this hash slot was served by 'myself' before to switch
             * make sure there are no longer local keys for this hash slot.
             * 如果这个slot此时正在本节点下 并且内部还有数据 无法转移
             * */
            if (server.cluster->slots[slot] == myself && n != myself) {
                if (countKeysInSlot(slot) != 0) {
                    addReplyErrorFormat(c,
                                        "Can't assign hashslot %d to a different node "
                                        "while I still hold keys for this hash slot.", slot);
                    return;
                }
            }
            /* If this slot is in migrating status but we have no keys
             * for it assigning the slot to another node will clear
             * the migrating status.
             * node操作会影响到数据迁移 比如发现当前slot内没有数据 就清理掉迁移标记
             * */
            if (countKeysInSlot(slot) == 0 &&
                server.cluster->migrating_slots_to[slot])
                server.cluster->migrating_slots_to[slot] = NULL;

            /* If this node was importing this slot, assigning the slot to
             * itself also clears the importing status.
             * 只有当该slot不被本节点拥有时 才能在importing_slots_from中设置该slot
             * migrating_slots_to则要求此时slot必然被本节点持有
             * 这里就是取消迁入标记 直接把slot分配该myself
             * */
            if (n == myself &&
                server.cluster->importing_slots_from[slot]) {
                /* This slot was manually migrated, set this node configEpoch
                 * to a new epoch so that the new version can be propagated
                 * by the cluster.
                 *
                 * Note that if this ever results in a collision with another
                 * node getting the same configEpoch, for example because a
                 * failover happens at the same time we close the slot, the
                 * configEpoch collision resolution will fix it assigning
                 * a different epoch to each node.
                 * 增加 epoch epoch相当于是整个集群的版本号
                 * */
                if (clusterBumpConfigEpochWithoutConsensus() == C_OK) {
                    serverLog(LL_WARNING,
                              "configEpoch updated after importing slot %d", slot);
                }
                // 这里也是清除迁入标记
                server.cluster->importing_slots_from[slot] = NULL;
            }

            // 将slot分配到该节点
            clusterDelSlot(slot);
            clusterAddSlot(n, slot);
        } else {

            // 剩余的命令无法被识别 代表异常情况
            addReplyError(c,
                          "Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP");
            return;
        }
        // 因为执行命令都会修改slots 所以这里要更新config
        clusterDoBeforeSleep(CLUSTER_TODO_SAVE_CONFIG | CLUSTER_TODO_UPDATE_STATE);
        addReply(c, shared.ok);
        // 检测epoch是否碰撞 并返回结果
    } else if (!strcasecmp(c->argv[1]->ptr, "bumpepoch") && c->argc == 2) {
        /* CLUSTER BUMPEPOCH */
        int retval = clusterBumpConfigEpochWithoutConsensus();
        sds reply = sdscatprintf(sdsempty(), "+%s %llu\r\n",
                                 (retval == C_OK) ? "BUMPED" : "STILL",
                                 (unsigned long long) myself->configEpoch);
        addReplySds(c, reply);
        // 将本集群此时所有slot的状态信息返回 比如有多少个slot可用 有多少个未分配
    } else if (!strcasecmp(c->argv[1]->ptr, "info") && c->argc == 2) {
        /* CLUSTER INFO */
        char *statestr[] = {"ok", "fail", "needhelp"};
        int slots_assigned = 0, slots_ok = 0, slots_pfail = 0, slots_fail = 0;
        uint64_t myepoch;
        int j;

        // 查看所有已经被分配的slot相关的node此时是否处于可用状态
        for (j = 0; j < CLUSTER_SLOTS; j++) {
            clusterNode *n = server.cluster->slots[j];

            if (n == NULL) continue;
            slots_assigned++;
            if (nodeFailed(n)) {
                slots_fail++;
            } else if (nodeTimedOut(n)) {
                slots_pfail++;
            } else {
                slots_ok++;
            }
        }

        // 获取当前节点的epoch
        myepoch = (nodeIsSlave(myself) && myself->slaveof) ?
                  myself->slaveof->configEpoch : myself->configEpoch;

        sds info = sdscatprintf(sdsempty(),
                                "cluster_state:%s\r\n"
                                "cluster_slots_assigned:%d\r\n"
                                "cluster_slots_ok:%d\r\n"
                                "cluster_slots_pfail:%d\r\n"
                                "cluster_slots_fail:%d\r\n"
                                "cluster_known_nodes:%lu\r\n"
                                "cluster_size:%d\r\n"
                                "cluster_current_epoch:%llu\r\n"
                                "cluster_my_epoch:%llu\r\n", statestr[server.cluster->state],
                                slots_assigned,
                                slots_ok,
                                slots_pfail,
                                slots_fail,
                                dictSize(server.cluster->nodes),
                                server.cluster->size,
                                (unsigned long long) server.cluster->currentEpoch,
                                (unsigned long long) myepoch
        );

        /* Show stats about messages sent and received. */
        long long tot_msg_sent = 0;
        long long tot_msg_received = 0;

        for (int i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
            if (server.cluster->stats_bus_messages_sent[i] == 0) continue;
            tot_msg_sent += server.cluster->stats_bus_messages_sent[i];
            info = sdscatprintf(info,
                                "cluster_stats_messages_%s_sent:%lld\r\n",
                                clusterGetMessageTypeString(i),
                                server.cluster->stats_bus_messages_sent[i]);
        }
        info = sdscatprintf(info,
                            "cluster_stats_messages_sent:%lld\r\n", tot_msg_sent);

        for (int i = 0; i < CLUSTERMSG_TYPE_COUNT; i++) {
            if (server.cluster->stats_bus_messages_received[i] == 0) continue;
            tot_msg_received += server.cluster->stats_bus_messages_received[i];
            info = sdscatprintf(info,
                                "cluster_stats_messages_%s_received:%lld\r\n",
                                clusterGetMessageTypeString(i),
                                server.cluster->stats_bus_messages_received[i]);
        }
        info = sdscatprintf(info,
                            "cluster_stats_messages_received:%lld\r\n", tot_msg_received);

        /* Produce the reply protocol. */
        addReplyVerbatim(c, info, sdslen(info), "txt");
        sdsfree(info);
        // 立即对配置文件进行刷盘
    } else if (!strcasecmp(c->argv[1]->ptr, "saveconfig") && c->argc == 2) {
        int retval = clusterSaveConfig(1);

        if (retval == 0)
            addReply(c, shared.ok);
        else
            addReplyErrorFormat(c, "error saving the cluster node config: %s",
                                strerror(errno));
        // 根据key 计算slot
    } else if (!strcasecmp(c->argv[1]->ptr, "keyslot") && c->argc == 3) {
        /* CLUSTER KEYSLOT <key> */
        sds key = c->argv[2]->ptr;

        addReplyLongLong(c, keyHashSlot(key, sdslen(key)));

        // 获取指定slot下key的数量
    } else if (!strcasecmp(c->argv[1]->ptr, "countkeysinslot") && c->argc == 3) {
        /* CLUSTER COUNTKEYSINSLOT <slot> */
        long long slot;

        if (getLongLongFromObjectOrReply(c, c->argv[2], &slot, NULL) != C_OK)
            return;
        if (slot < 0 || slot >= CLUSTER_SLOTS) {
            addReplyError(c, "Invalid slot");
            return;
        }
        addReplyLongLong(c, countKeysInSlot(slot));
        // 返回某个slot下的key 需要指定key的数量 (避免key数量太大)
    } else if (!strcasecmp(c->argv[1]->ptr, "getkeysinslot") && c->argc == 4) {
        /* CLUSTER GETKEYSINSLOT <slot> <count> */
        long long maxkeys, slot;
        unsigned int numkeys, j;
        robj **keys;

        if (getLongLongFromObjectOrReply(c, c->argv[2], &slot, NULL) != C_OK)
            return;
        if (getLongLongFromObjectOrReply(c, c->argv[3], &maxkeys, NULL)
            != C_OK)
            return;
        if (slot < 0 || slot >= CLUSTER_SLOTS || maxkeys < 0) {
            addReplyError(c, "Invalid slot or number of keys");
            return;
        }

        /* Avoid allocating more than needed in case of large COUNT argument
         * and smaller actual number of keys. */
        unsigned int keys_in_slot = countKeysInSlot(slot);
        if (maxkeys > keys_in_slot) maxkeys = keys_in_slot;

        keys = zmalloc(sizeof(robj *) * maxkeys);
        // 获取某个slot下maxKeys数量的key 将指针keys指向这些数据
        numkeys = getKeysInSlot(slot, keys, maxkeys);
        // 将这些数据返回给client
        addReplyArrayLen(c, numkeys);
        for (j = 0; j < numkeys; j++) {
            addReplyBulk(c, keys[j]);
            decrRefCount(keys[j]);
        }
        zfree(keys);
        // 将某个节点从集群中移除 并加入到黑名单
    } else if (!strcasecmp(c->argv[1]->ptr, "forget") && c->argc == 3) {
        /* CLUSTER FORGET <NODE ID> */
        // 找到目标节点
        clusterNode *n = clusterLookupNode(c->argv[2]->ptr);

        if (!n) {
            addReplyErrorFormat(c, "Unknown node %s", (char *) c->argv[2]->ptr);
            return;
        } else if (n == myself) {
            addReplyError(c, "I tried hard but I can't forget myself...");
            return;
        } else if (nodeIsSlave(myself) && myself->slaveof == n) {
            addReplyError(c, "Can't forget my master!");
            return;
        }
        clusterBlacklistAddNode(n);
        clusterDelNode(n);
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE |
                             CLUSTER_TODO_SAVE_CONFIG);
        addReply(c, shared.ok);

        // 这个命令是将当前节点变成某个节点的slave节点 并从它那里拷贝数据
    } else if (!strcasecmp(c->argv[1]->ptr, "replicate") && c->argc == 3) {
        /* CLUSTER REPLICATE <NODE ID> */
        clusterNode *n = clusterLookupNode(c->argv[2]->ptr);

        /* Lookup the specified node in our table. */
        if (!n) {
            addReplyErrorFormat(c, "Unknown node %s", (char *) c->argv[2]->ptr);
            return;
        }

        /* I can't replicate myself.
         * 本节点不能作为拷贝的数据源
         * */
        if (n == myself) {
            addReplyError(c, "Can't replicate myself");
            return;
        }

        /* Can't replicate a slave.
         * 数据源不能是一个slave节点
         * */
        if (nodeIsSlave(n)) {
            addReplyError(c, "I can only replicate a master, not a replica.");
            return;
        }

        /* If the instance is currently a master, it should have no assigned
         * slots nor keys to accept to replicate some other node.
         * Slaves can switch to another master without issues.
         * 本节点是要降级的 如果本节点此时作为一个master并且有数据了 那就不能随意降级了
         * */
        if (nodeIsMaster(myself) &&
            (myself->numslots != 0 || dictSize(server.db[0].dict) != 0)) {
            addReplyError(c,
                          "To set a master the node must be empty and "
                          "without assigned slots.");
            return;
        }

        /* Set the master.
         * 将n作为myself的新的master节点  注意此时没有删除之前master对应的slot下的数据
         * */
        clusterSetMaster(n);
        clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE | CLUSTER_TODO_SAVE_CONFIG);
        addReply(c, shared.ok);
        // 获取某个节点下所有slave节点
    } else if ((!strcasecmp(c->argv[1]->ptr, "slaves") ||
                !strcasecmp(c->argv[1]->ptr, "replicas")) && c->argc == 3) {
        /* CLUSTER SLAVES <NODE ID>
         * 本次要处理的源节点
         * */
        clusterNode *n = clusterLookupNode(c->argv[2]->ptr);
        int j;

        /* Lookup the specified node in our table. */
        if (!n) {
            addReplyErrorFormat(c, "Unknown node %s", (char *) c->argv[2]->ptr);
            return;
        }

        // 该节点必须是master节点
        if (nodeIsSlave(n)) {
            addReplyError(c, "The specified node is not a master");
            return;
        }

        addReplyArrayLen(c, n->numslaves);
        // 将节点的描述信息返回给client
        for (j = 0; j < n->numslaves; j++) {
            sds ni = clusterGenNodeDescription(n->slaves[j]);
            addReplyBulkCString(c, ni);
            sdsfree(ni);
        }
        // 判断此时有多少节点认为某个节点此时已经下线
    } else if (!strcasecmp(c->argv[1]->ptr, "count-failure-reports") &&
               c->argc == 3) {
        /* CLUSTER COUNT-FAILURE-REPORTS <NODE ID> */
        clusterNode *n = clusterLookupNode(c->argv[2]->ptr);

        if (!n) {
            addReplyErrorFormat(c, "Unknown node %s", (char *) c->argv[2]->ptr);
            return;
        } else {
            addReplyLongLong(c, clusterNodeFailureReportsCount(n));
        }
        // 手动触发故障转移
    } else if (!strcasecmp(c->argv[1]->ptr, "failover") &&
               (c->argc == 2 || c->argc == 3)) {
        /* CLUSTER FAILOVER [FORCE|TAKEOVER] */
        int force = 0, takeover = 0;

        // 获取一些故障转移相关的选项
        if (c->argc == 3) {
            if (!strcasecmp(c->argv[2]->ptr, "force")) {
                force = 1;
                // 接管模式就是直接将本节点作为新的master
            } else if (!strcasecmp(c->argv[2]->ptr, "takeover")) {
                takeover = 1;
                force = 1; /* Takeover also implies force. */
            } else {
                addReply(c, shared.syntaxerr);
                return;
            }
        }

        /* Check preconditions.
         * 进行一些先决条件的检查
         * */
        // 故障转移 应该发给一个slave节点  促使其选举新的master节点
        if (nodeIsMaster(myself)) {
            addReplyError(c, "You should send CLUSTER FAILOVER to a replica");
            return;
            // 游离的slave没有处理的必要
        } else if (myself->slaveof == NULL) {
            addReplyError(c, "I'm a replica but my master is unknown to me");
            return;
            // 当无法访问到master时 必须使用force模式
        } else if (!force &&
                   (nodeFailed(myself->slaveof) ||
                    myself->slaveof->link == NULL)) {
            addReplyError(c, "Master is down or failed, "
                             "please use CLUSTER FAILOVER FORCE");
            return;
        }

        // 因为本次是手动发起故障转移 需要先进行重置一些参数
        resetManualFailover();
        // 手动故障转移有一个时间限制 并且当检测到该值被设置 就代表开启了手动故障转移
        server.cluster->mf_end = mstime() + CLUSTER_MF_TIMEOUT;

        // 如果是接管模式 就是跳过选举
        if (takeover) {
            /* A takeover does not perform any initial check. It just
             * generates a new configuration epoch for this node without
             * consensus, claims the master's slots, and broadcast the new
             * configuration. */
            serverLog(LL_WARNING, "Taking over the master (user request).");
            // 增加cluster.currentEpoch 这样其他master节点感知到信息后就会认可该节点的数据 (之前那个master就会被降级)
            clusterBumpConfigEpochWithoutConsensus();
            // 接管模式就是直接将该节点晋升成master
            clusterFailoverReplaceYourMaster();
            // 原本手动故障转移 需要等到slave与master的数据同步 当传入force参数时 会直接发起投票
        } else if (force) {
            /* If this is a forced failover, we don't need to talk with our
             * master to agree about the offset. We just failover taking over
             * it without coordination. */
            serverLog(LL_WARNING, "Forced failover user request accepted.");
            server.cluster->mf_can_start = 1;
        } else {
            // 这里就是正常发起手动故障转移的场景
            serverLog(LL_WARNING, "Manual failover user request accepted.");
            clusterSendMFStart(myself->slaveof);
        }
        addReply(c, shared.ok);
        // 强制设置当前configEpoch
    } else if (!strcasecmp(c->argv[1]->ptr, "set-config-epoch") && c->argc == 3) {
        /* CLUSTER SET-CONFIG-EPOCH <epoch>
         *
         * The user is allowed to set the config epoch only when a node is
         * totally fresh: no config epoch, no other known node, and so forth.
         * This happens at cluster creation time to start with a cluster where
         * every node has a different node ID, without to rely on the conflicts
         * resolution system which is too slow when a big cluster is created. */
        long long epoch;

        if (getLongLongFromObjectOrReply(c, c->argv[2], &epoch, NULL) != C_OK)
            return;

        if (epoch < 0) {
            addReplyErrorFormat(c, "Invalid config epoch specified: %lld", epoch);
            // 只有当本节点还没有加入到集群时 才能手动设置epoch
        } else if (dictSize(server.cluster->nodes) > 1) {
            addReplyError(c, "The user can assign a config epoch only when the "
                             "node does not know any other node.");
            // 如果已经存在 configEpoch 无法手动设置epoch
        } else if (myself->configEpoch != 0) {
            addReplyError(c, "Node config epoch is already non-zero");
        } else {
            myself->configEpoch = epoch;
            serverLog(LL_WARNING,
                      "configEpoch set to %llu via CLUSTER SET-CONFIG-EPOCH",
                      (unsigned long long) myself->configEpoch);

            if (server.cluster->currentEpoch < (uint64_t) epoch)
                server.cluster->currentEpoch = epoch;
            /* No need to fsync the config here since in the unlucky event
             * of a failure to persist the config, the conflict resolution code
             * will assign a unique config to this node. */
            clusterDoBeforeSleep(CLUSTER_TODO_UPDATE_STATE |
                                 CLUSTER_TODO_SAVE_CONFIG);
            addReply(c, shared.ok);
        }
        // 执行的是reset
    } else if (!strcasecmp(c->argv[1]->ptr, "reset") &&
               (c->argc == 2 || c->argc == 3)) {
        /* CLUSTER RESET [SOFT|HARD] */
        int hard = 0;

        /* Parse soft/hard argument. Default is soft.
         * 读取软重置/硬重置
         * */
        if (c->argc == 3) {
            if (!strcasecmp(c->argv[2]->ptr, "hard")) {
                hard = 1;
            } else if (!strcasecmp(c->argv[2]->ptr, "soft")) {
                hard = 0;
            } else {
                addReply(c, shared.syntaxerr);
                return;
            }
        }

        /* Slaves can be reset while containing data, but not master nodes
         * that must be empty.
         * 如果本节点是master节点 并且有数据 是不能重置的
         * */
        if (nodeIsMaster(myself) && dictSize(c->db->dict) != 0) {
            addReplyError(c, "CLUSTER RESET can't be called with "
                             "master nodes containing keys");
            return;
        }
        clusterReset(hard);
        addReply(c, shared.ok);
    } else {
        // 不支持其他命令
        addReplySubcommandSyntaxError(c);
        return;
    }
}

/* -----------------------------------------------------------------------------
 * DUMP, RESTORE and MIGRATE commands
 * -------------------------------------------------------------------------- */

/* Generates a DUMP-format representation of the object 'o', adding it to the
 * io stream pointed by 'rio'. This function can't fail.
 * @param payload 存放存储结果的容器
 * @param o 在某个db中通过key 定位到的redisObject
 * @param key 该对象对应的key
 * */
void createDumpPayload(rio *payload, robj *o, robj *key) {
    unsigned char buf[2];
    uint64_t crc;

    /* Serialize the object in an RDB-like format. It consist of an object type
     * byte followed by the serialized object. This is understood by RESTORE.
     * 该rio流 底层是一个redisString
     * */
    rioInitWithBuffer(payload, sdsempty());
    // 将数据按照rdb协议存储到容器中
    serverAssert(rdbSaveObjectType(payload, o));
    serverAssert(rdbSaveObject(payload, o, key));

    /* Write the footer, this is how it looks like:
     * ----------------+---------------------+---------------+
     * ... RDB payload | 2 bytes RDB version | 8 bytes CRC64 |
     * ----------------+---------------------+---------------+
     * RDB version and CRC are both in little endian.
     */

    /* RDB version 头2个数据存储的是rdb版本 */
    buf[0] = RDB_VERSION & 0xff;
    buf[1] = (RDB_VERSION >> 8) & 0xff;
    payload->io.buffer.ptr = sdscatlen(payload->io.buffer.ptr, buf, 2);

    /* CRC64 */
    crc = crc64(0, (unsigned char *) payload->io.buffer.ptr,
                sdslen(payload->io.buffer.ptr));
    memrev64ifbe(&crc);
    payload->io.buffer.ptr = sdscatlen(payload->io.buffer.ptr, &crc, 8);
}

/* Verify that the RDB version of the dump payload matches the one of this Redis
 * instance and that the checksum is ok.
 * If the DUMP payload looks valid C_OK is returned, otherwise C_ERR
 * is returned.
 * 进行数据校验
 * */
int verifyDumpPayload(unsigned char *p, size_t len) {
    unsigned char *footer;
    uint16_t rdbver;
    uint64_t crc;

    /* At least 2 bytes of RDB version and 8 of CRC64 should be present. */
    if (len < 10) return C_ERR;
    // 只需要校验最后10个字节 2个是rdb版本号 8个是crc
    footer = p + (len - 10);

    /* Verify RDB version */
    rdbver = (footer[1] << 8) | footer[0];
    if (rdbver > RDB_VERSION) return C_ERR;

    /* Verify CRC64 */
    crc = crc64(0, p, len - 8);
    memrev64ifbe(&crc);
    return (memcmp(&crc, footer + 2, 8) == 0) ? C_OK : C_ERR;
}

/* DUMP keyname
 * DUMP is actually not used by Redis Cluster but it is the obvious
 * complement of RESTORE and can be useful for different applications.
 * 通过key查询到某个redisObject 并按照rdb协议存储到容器中
 * */
void dumpCommand(client *c) {
    robj *o;
    rio payload;

    /* Check if the key is here.
     * 先根据相关参数找到 redisObject
     * */
    if ((o = lookupKeyRead(c->db, c->argv[1])) == NULL) {
        addReplyNull(c);
        return;
    }

    /* Create the DUMP encoded representation.
     * 基于该redisObject 生成转储数据
     * */
    createDumpPayload(&payload, o, c->argv[1]);

    /* Transfer to the client 将数据返回 */
    addReplyBulkSds(c, payload.io.buffer.ptr);
    return;
}

/* RESTORE key ttl serialized-value [REPLACE]
 * 将数据流还原成redisObject 并插入到db中
 * */
void restoreCommand(client *c) {
    long long ttl, lfu_freq = -1, lru_idle = -1, lru_clock = -1;
    rio payload;
    int j, type, replace = 0, absttl = 0;
    robj *obj;

    /* Parse additional options
     * 解析参数信息  应该是要存储哪些信息
     * */
    for (j = 4; j < c->argc; j++) {
        // 代表还有多少参数未解析
        int additional = c->argc - j - 1;
        if (!strcasecmp(c->argv[j]->ptr, "replace")) {
            replace = 1;
        } else if (!strcasecmp(c->argv[j]->ptr, "absttl")) {
            absttl = 1;
            // 本次读取到的是idletime 还需要额外读取一个参数作为lru_idle
        } else if (!strcasecmp(c->argv[j]->ptr, "idletime") && additional >= 1 &&
                   lfu_freq == -1) {
            // 读取额外参数
            if (getLongLongFromObjectOrReply(c, c->argv[j + 1], &lru_idle, NULL)
                != C_OK)
                return;
            // 如果idle数据不合法 直接返回
            if (lru_idle < 0) {
                addReplyError(c, "Invalid IDLETIME value, must be >= 0");
                return;
            }
            lru_clock = LRU_CLOCK();
            j++; /* Consume additional arg. */
            // 同上 分别代表通过lru和lfu的淘汰策略
        } else if (!strcasecmp(c->argv[j]->ptr, "freq") && additional >= 1 &&
                   lru_idle == -1) {
            if (getLongLongFromObjectOrReply(c, c->argv[j + 1], &lfu_freq, NULL)
                != C_OK)
                return;
            if (lfu_freq < 0 || lfu_freq > 255) {
                addReplyError(c, "Invalid FREQ value, must be >= 0 and <= 255");
                return;
            }
            j++; /* Consume additional arg. */
        } else {
            addReply(c, shared.syntaxerr);
            return;
        }
    }

    /* Make sure this key does not already exist here...
     * */
    robj *key = c->argv[1];
    // 非替换模式 必须要求原数据不存在
    if (!replace && lookupKeyWrite(c->db, key) != NULL) {
        addReply(c, shared.busykeyerr);
        return;
    }

    /* Check if the TTL value makes sense 第二个参数是ttl */
    if (getLongLongFromObjectOrReply(c, c->argv[2], &ttl, NULL) != C_OK) {
        return;
    } else if (ttl < 0) {
        addReplyError(c, "Invalid TTL value, must be >= 0");
        return;
    }

    /* Verify RDB version and data checksum. 进行crc校验 */
    if (verifyDumpPayload(c->argv[3]->ptr, sdslen(c->argv[3]->ptr)) == C_ERR) {
        addReplyError(c, "DUMP payload version or checksum are wrong");
        return;
    }

    // 将客户端传入的数据还原成redisObject
    rioInitWithBuffer(&payload, c->argv[3]->ptr);
    // 还原redisObject
    if (((type = rdbLoadObjectType(&payload)) == -1) ||
        ((obj = rdbLoadObject(type, &payload, key->ptr)) == NULL)) {
        addReplyError(c, "Bad data format");
        return;
    }

    /* Remove the old key if needed. 替换模式 先删除旧数据 */
    int deleted = 0;
    if (replace)
        deleted = dbDelete(c->db, key);

    // 如果传入的ttl不是绝对时间 需要自己计算
    if (ttl && !absttl) ttl += mstime();
    // 如果该robj 一开始就是过期的 就不需要插入了
    if (ttl && checkAlreadyExpired(ttl)) {
        if (deleted) {
            // 将命令修改成del 可以简化之后其他node的处理逻辑
            rewriteClientCommandVector(c, 2, shared.del, key);
            // 尝试唤醒阻塞在该key上的client   什么情况下client会被阻塞在某个client上呢
            signalModifiedKey(c, c->db, key);
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", key, c->db->id);
            server.dirty++;
        }
        decrRefCount(obj);
        addReply(c, shared.ok);
        return;
    }

    /* Create the key and set the TTL if any */
    dbAdd(c->db, key, obj);
    // 设置该redisObject的超时时间
    if (ttl) {
        setExpire(c, c->db, key, ttl);
    }
    // 设置lru lfu 信息
    objectSetLRUOrLFU(obj, lfu_freq, lru_idle, lru_clock, 1000);
    signalModifiedKey(c, c->db, key);
    notifyKeyspaceEvent(NOTIFY_GENERIC, "restore", key, c->db->id);
    addReply(c, shared.ok);
    server.dirty++;
}

/* MIGRATE socket cache implementation.
 *
 * We take a map between host:ip and a TCP socket that we used to connect
 * to this instance in recent time.
 * This sockets are closed when the max number we cache is reached, and also
 * in serverCron() when they are around for more than a few seconds. */
#define MIGRATE_SOCKET_CACHE_ITEMS 64 /* max num of items in the cache. */
#define MIGRATE_SOCKET_CACHE_TTL 10 /* close cached sockets after 10 sec. */

/**
 * 数据迁移使用的连接
 */
typedef struct migrateCachedSocket {
    connection *conn;
    long last_dbid;
    time_t last_use_time;
} migrateCachedSocket;

/* Return a migrateCachedSocket containing a TCP socket connected with the
 * target instance, possibly returning a cached one.
 *
 * This function is responsible of sending errors to the client if a
 * connection can't be established. In this case -1 is returned.
 * Otherwise on success the socket is returned, and the caller should not
 * attempt to free it after usage.
 *
 * If the caller detects an error while using the socket, migrateCloseSocket()
 * should be called so that the connection will be created from scratch
 * the next time.
 * @param c 发起数据迁移请求的client
 * @param host 本次迁移的目标节点的host
 * @param port 本次迁移的目标节点的端口号
 * */
migrateCachedSocket *migrateGetSocket(client *c, robj *host, robj *port, long timeout) {
    connection *conn;
    sds name = sdsempty();
    migrateCachedSocket *cs;

    /* Check if we have an already cached socket for this ip:port pair. */
    name = sdscatlen(name, host->ptr, sdslen(host->ptr));
    name = sdscatlen(name, ":", 1);
    name = sdscatlen(name, port->ptr, sdslen(port->ptr));
    // 尝试获取缓存的socket
    cs = dictFetchValue(server.migrate_cached_sockets, name);
    if (cs) {
        sdsfree(name);
        // 更新最后的使用时间
        cs->last_use_time = server.unixtime;
        return cs;
    }

    /* No cached socket, create one.
     * 需要新建连接 并且缓存的连接有一个上限
     * */
    if (dictSize(server.migrate_cached_sockets) == MIGRATE_SOCKET_CACHE_ITEMS) {
        /* Too many items, drop one at random. */
        dictEntry *de = dictGetRandomKey(server.migrate_cached_sockets);
        cs = dictGetVal(de);
        connClose(cs->conn);
        zfree(cs);
        dictDelete(server.migrate_cached_sockets, dictGetKey(de));
    }

    /* Create the socket
     * 创建一个句柄对象
     * */
    conn = server.tls_cluster ? connCreateTLS() : connCreateSocket();
    // 等待指定时间 直到连接到目标节点
    if (connBlockingConnect(conn, c->argv[1]->ptr, atoi(c->argv[2]->ptr), timeout)
        != C_OK) {
        addReplySds(c,
                    sdsnew("-IOERR error or timeout connecting to the client\r\n"));
        connClose(conn);
        sdsfree(name);
        return NULL;
    }
    connEnableTcpNoDelay(conn);

    /* Add to the cache and return it to the caller. */
    cs = zmalloc(sizeof(*cs));
    cs->conn = conn;

    // 将socket加入到缓存中
    cs->last_dbid = -1;
    cs->last_use_time = server.unixtime;
    dictAdd(server.migrate_cached_sockets, name, cs);
    return cs;
}

/* Free a migrate cached connection.
 * 与某个节点的连接失败 释放资源
 * */
void migrateCloseSocket(robj *host, robj *port) {
    sds name = sdsempty();
    migrateCachedSocket *cs;

    name = sdscatlen(name, host->ptr, sdslen(host->ptr));
    name = sdscatlen(name, ":", 1);
    name = sdscatlen(name, port->ptr, sdslen(port->ptr));
    cs = dictFetchValue(server.migrate_cached_sockets, name);
    if (!cs) {
        sdsfree(name);
        return;
    }

    connClose(cs->conn);
    zfree(cs);
    dictDelete(server.migrate_cached_sockets, name);
    sdsfree(name);
}

/**
 * 某些缓存的socket 如果长时间未使用
 */
void migrateCloseTimedoutSockets(void) {
    dictIterator *di = dictGetSafeIterator(server.migrate_cached_sockets);
    dictEntry *de;

    while ((de = dictNext(di)) != NULL) {
        // 遍历每个缓存的socket
        migrateCachedSocket *cs = dictGetVal(de);

        // 某些缓存的连接长时间未使用
        if ((server.unixtime - cs->last_use_time) > MIGRATE_SOCKET_CACHE_TTL) {
            connClose(cs->conn);
            zfree(cs);
            dictDelete(server.migrate_cached_sockets, dictGetKey(de));
        }
    }
    dictReleaseIterator(di);
}

/* MIGRATE host port key dbid timeout [COPY | REPLACE | AUTH password |
 *         AUTH2 username password]
 *
 * On in the multiple keys form:
 *
 * MIGRATE host port "" dbid timeout [COPY | REPLACE | AUTH password |
 *         AUTH2 username password] KEYS key1 key2 ... keyN
 *         以key->obj为单位进行数据迁移
 *         */
void migrateCommand(client *c) {
    migrateCachedSocket *cs;
    int copy = 0, replace = 0, j;
    char *username = NULL;
    char *password = NULL;
    long timeout;
    long dbid;
    robj **ov = NULL; /* Objects to migrate. */
    robj **kv = NULL; /* Key names. */
    robj **newargv = NULL; /* Used to rewrite the command as DEL ... keys ... */
    rio cmd, payload;
    int may_retry = 1;
    int write_error = 0;
    int argv_rewritten = 0;

    /* To support the KEYS option we need the following additional state. */
    int first_key = 3; /* Argument index of the first key. */
    int num_keys = 1;  /* By default only migrate the 'key' argument. */

    /* Parse additional options
     * 解析迁移的相关参数
     * */
    for (j = 6; j < c->argc; j++) {
        // 如果是某些指令 还会携带一些参数
        int moreargs = (c->argc - 1) - j;
        if (!strcasecmp(c->argv[j]->ptr, "copy")) {
            copy = 1;
        } else if (!strcasecmp(c->argv[j]->ptr, "replace")) {
            replace = 1;
            // 如果需要认证 读取密码属性
        } else if (!strcasecmp(c->argv[j]->ptr, "auth")) {
            if (!moreargs) {
                addReply(c, shared.syntaxerr);
                return;
            }
            j++;
            password = c->argv[j]->ptr;
        } else if (!strcasecmp(c->argv[j]->ptr, "auth2")) {
            if (moreargs < 2) {
                addReply(c, shared.syntaxerr);
                return;
            }
            username = c->argv[++j]->ptr;
            password = c->argv[++j]->ptr;
        } else if (!strcasecmp(c->argv[j]->ptr, "keys")) {
            if (sdslen(c->argv[3]->ptr) != 0) {
                addReplyError(c,
                              "When using MIGRATE KEYS option, the key argument"
                              " must be set to the empty string");
                return;
            }
            // keys 必然是最后一个选项 并且后面的都是keys
            first_key = j + 1;
            num_keys = c->argc - j - 1;
            break; /* All the remaining args are keys. */
        } else {
            addReply(c, shared.syntaxerr);
            return;
        }
    }

    /* Sanity check
     * 读取超时时间 以及dbid
     * */
    if (getLongFromObjectOrReply(c, c->argv[5], &timeout, NULL) != C_OK ||
        getLongFromObjectOrReply(c, c->argv[4], &dbid, NULL) != C_OK) {
        return;
    }
    if (timeout <= 0) timeout = 1000;

    /* Check if the keys are here. If at least one key is to migrate, do it
     * otherwise if all the keys are missing reply with "NOKEY" to signal
     * the caller there was nothing to migrate. We don't return an error in
     * this case, since often this is due to a normal condition like the key
     * expiring in the meantime.
     * 为需要进行迁移的数据 分配空间
     * ov 存储redisObject
     * kv 存储redisKey
     * */
    ov = zrealloc(ov, sizeof(robj *) * num_keys);
    kv = zrealloc(kv, sizeof(robj *) * num_keys);
    int oi = 0;

    // keys中包含了本次要迁移的所有数据
    for (j = 0; j < num_keys; j++) {
        // 查询redisObject是否存在
        if ((ov[oi] = lookupKeyRead(c->db, c->argv[first_key + j])) != NULL) {
            // 取出key
            kv[oi] = c->argv[first_key + j];
            oi++;
        }
    }

    // num_keys 代表实际找到了多少个
    num_keys = oi;
    // 没有找到对象 直接返回
    if (num_keys == 0) {
        zfree(ov);
        zfree(kv);
        addReplySds(c, sdsnew("+NOKEY\r\n"));
        return;
    }

    try_again:
    write_error = 0;

    /* Connect
     * 开始与目标节点建立连接
     * */
    cs = migrateGetSocket(c, c->argv[1], c->argv[2], timeout);
    if (cs == NULL) {
        zfree(ov);
        zfree(kv);
        return; /* error sent to the client by migrateGetSocket() */
    }

    // 基于一个空的sds生成rio
    rioInitWithBuffer(&cmd, sdsempty());

    /* Authentication
     * 代表需要进行权限校验 追加一个authCommand
     * */
    if (password) {
        int arity = username ? 3 : 2;
        serverAssertWithInfo(c, NULL, rioWriteBulkCount(&cmd, '*', arity));
        serverAssertWithInfo(c, NULL, rioWriteBulkString(&cmd, "AUTH", 4));
        // 把用户名 密码写入到数据体内
        if (username) {
            serverAssertWithInfo(c, NULL, rioWriteBulkString(&cmd, username,
                                                             sdslen(username)));
        }
        serverAssertWithInfo(c, NULL, rioWriteBulkString(&cmd, password,
                                                         sdslen(password)));
    }

    /* Send the SELECT command if the current DB is not already selected. */
    int select = cs->last_dbid != dbid; /* Should we emit SELECT? */
    // db不匹配 要先执行selectCommand
    if (select) {
        serverAssertWithInfo(c, NULL, rioWriteBulkCount(&cmd, '*', 2));
        serverAssertWithInfo(c, NULL, rioWriteBulkString(&cmd, "SELECT", 6));
        serverAssertWithInfo(c, NULL, rioWriteBulkLongLong(&cmd, dbid));
    }

    int non_expired = 0; /* Number of keys that we'll find non expired.
                            Note that serializing large keys may take some time
                            so certain keys that were found non expired by the
                            lookupKey() function, may be expired later. */

    /* Create RESTORE payload and generate the protocol to call the command.
     * 处理每个key
     * */
    for (j = 0; j < num_keys; j++) {
        long long ttl = 0;
        long long expireat = getExpire(c->db, kv[j]);

        if (expireat != -1) {
            ttl = expireat - mstime();
            // 代表该key已经过期 不会立即处理 而是在主流程的cron中 通过抽样的方式进行清理
            if (ttl < 0) {
                continue;
            }
            if (ttl < 1) ttl = 1;
        }

        /* Relocate valid (non expired) keys into the array in successive
         * positions to remove holes created by the keys that were present
         * in the first lookup but are now expired after the second lookup.
         * 通过第一轮的筛选后 进行第二轮筛选 主要是要求该key还没有过期
         * */
        kv[non_expired++] = kv[j];

        // 应该是代表如果目标节点已经存在该key了 应该怎么处理 这里是选择替换
        serverAssertWithInfo(c, NULL,
                             rioWriteBulkCount(&cmd, '*', replace ? 5 : 4));

        if (server.cluster_enabled)
            serverAssertWithInfo(c, NULL,
                                 rioWriteBulkString(&cmd, "RESTORE-ASKING", 14));
        else
            serverAssertWithInfo(c, NULL, rioWriteBulkString(&cmd, "RESTORE", 7));
        serverAssertWithInfo(c, NULL, sdsEncodedObject(kv[j]));
        // 写入key 和 ttl信息
        serverAssertWithInfo(c, NULL, rioWriteBulkString(&cmd, kv[j]->ptr,
                                                         sdslen(kv[j]->ptr)));
        serverAssertWithInfo(c, NULL, rioWriteBulkLongLong(&cmd, ttl));

        /* Emit the payload argument, that is the serialized object using
         * the DUMP format.
         * 将redisObject包装成rdb数据流后追加到cmd上
         * */
        createDumpPayload(&payload, ov[j], kv[j]);
        serverAssertWithInfo(c, NULL,
                             rioWriteBulkString(&cmd, payload.io.buffer.ptr,
                                                sdslen(payload.io.buffer.ptr)));
        sdsfree(payload.io.buffer.ptr);

        /* Add the REPLACE option to the RESTORE command if it was specified
         * as a MIGRATE option. */
        if (replace)
            serverAssertWithInfo(c, NULL, rioWriteBulkString(&cmd, "REPLACE", 7));
    }

    // 此时本次迁移的所有数据都已经写入到cmd中了

    /* Fix the actual number of keys we are migrating.
     * 实际上迁移的数量只有未过期的
     * */
    num_keys = non_expired;

    /* Transfer the query to the other node in 64K chunks. */
    errno = 0;
    {
        sds buf = cmd.io.buffer.ptr;
        size_t pos = 0, towrite;
        int nwritten = 0;

        while ((towrite = sdslen(buf) - pos) > 0) {
            towrite = (towrite > (64 * 1024) ? (64 * 1024) : towrite);
            nwritten = connSyncWrite(cs->conn, buf + pos, towrite, timeout);
            if (nwritten != (signed) towrite) {
                write_error = 1;
                goto socket_err;
            }
            pos += nwritten;
        }
    }

    // 之后阻塞当前线程并等待结果
    char buf0[1024]; /* Auth reply. */
    char buf1[1024]; /* Select reply. */
    char buf2[1024]; /* Restore reply. */

    /* Read the AUTH reply if needed. */
    if (password && connSyncReadLine(cs->conn, buf0, sizeof(buf0), timeout) <= 0)
        goto socket_err;

    /* Read the SELECT reply if needed. */
    if (select && connSyncReadLine(cs->conn, buf1, sizeof(buf1), timeout) <= 0)
        goto socket_err;

    /* Read the RESTORE replies. */
    int error_from_target = 0;
    int socket_error = 0;
    int del_idx = 1; /* Index of the key argument for the replicated DEL op. */

    /* Allocate the new argument vector that will replace the current command,
     * to propagate the MIGRATE as a DEL command (if no COPY option was given).
     * We allocate num_keys+1 because the additional argument is for "DEL"
     * command name itself.
     * 非复制情况 也就是本地的key会删除 这里使用一份额外的内存保存这些被迁移的key
     * */
    if (!copy) newargv = zmalloc(sizeof(robj *) * (num_keys + 1));

    // 对端返回的数据数量与一开始发送的数据数量匹配
    for (j = 0; j < num_keys; j++) {
        if (connSyncReadLine(cs->conn, buf2, sizeof(buf2), timeout) <= 0) {
            socket_error = 1;
            break;
        }
        // 代表auth 或者select执行失败 或者插入k-v失败
        if ((password && buf0[0] == '-') ||
            (select && buf1[0] == '-') ||
            buf2[0] == '-') {
            /* On error assume that last_dbid is no longer valid. */
            if (!error_from_target) {
                cs->last_dbid = -1;
                char *errbuf;
                if (password && buf0[0] == '-') errbuf = buf0;
                else if (select && buf1[0] == '-') errbuf = buf1;
                else errbuf = buf2;

                error_from_target = 1;
                addReplyErrorFormat(c, "Target instance replied with error: %s",
                                    errbuf + 1);
            }
            // 认为本次可以正常处理
        } else {
            // 非复制情况  将本地对应的key清除 同时将key存储到 newargv中 这样就完成了2个节点redisObject的交换
            if (!copy) {
                /* No COPY option: remove the local key, signal the change. */
                dbDelete(c->db, kv[j]);
                signalModifiedKey(c, c->db, kv[j]);
                notifyKeyspaceEvent(NOTIFY_GENERIC, "del", kv[j], c->db->id);
                server.dirty++;

                /* Populate the argument vector to replace the old one. */
                newargv[del_idx++] = kv[j];
                incrRefCount(kv[j]);
            }
        }
    }

    /* On socket error, if we want to retry, do it now before rewriting the
     * command vector. We only retry if we are sure nothing was processed
     * and we failed to read the first reply (j == 0 test). */
    if (!error_from_target && socket_error && j == 0 && may_retry &&
        errno != ETIMEDOUT) {
        goto socket_err; /* A retry is guaranteed because of tested conditions.*/
    }

    /* On socket errors, close the migration socket now that we still have
     * the original host/port in the ARGV. Later the original command may be
     * rewritten to DEL and will be too later.
     * 连接失败 关闭连接释放资源
     * */
    if (socket_error) migrateCloseSocket(c->argv[1], c->argv[2]);

    // 这里主要是更改command 并传播到副本上 (副本相当于直接执行删除)
    if (!copy) {
        /* Translate MIGRATE as DEL for replication/AOF. Note that we do
         * this only for the keys for which we received an acknowledgement
         * from the receiving Redis server, by using the del_idx index.
         * */
        if (del_idx > 1) {
            newargv[0] = createStringObject("DEL", 3);
            /* Note that the following call takes ownership of newargv. */
            replaceClientCommandVector(c, del_idx, newargv);
            argv_rewritten = 1;
        } else {
            /* No key transfer acknowledged, no need to rewrite as DEL. */
            zfree(newargv);
        }
        newargv = NULL; /* Make it safe to call zfree() on it in the future. */
    }

    /* If we are here and a socket error happened, we don't want to retry.
     * Just signal the problem to the client, but only do it if we did not
     * already queue a different error reported by the destination server. */
    if (!error_from_target && socket_error) {
        may_retry = 0;
        goto socket_err;
    }

    if (!error_from_target) {
        /* Success! Update the last_dbid in migrateCachedSocket, so that we can
         * avoid SELECT the next time if the target DB is the same. Reply +OK.
         *
         * Note: If we reached this point, even if socket_error is true
         * still the SELECT command succeeded (otherwise the code jumps to
         * socket_err label.
         * 记录最近一次数据迁移相关的db 同时返回成功结果
         * */
        cs->last_dbid = dbid;
        addReply(c, shared.ok);
    } else {
        /* On error we already sent it in the for loop above, and set
         * the currently selected socket to -1 to force SELECT the next time. */
    }

    sdsfree(cmd.io.buffer.ptr);
    zfree(ov);
    zfree(kv);
    zfree(newargv);
    return;

/* On socket errors we try to close the cached socket and try again.
 * It is very common for the cached socket to get closed, if just reopening
 * it works it's a shame to notify the error to the caller. */
    socket_err:
    /* Cleanup we want to perform in both the retry and no retry case.
     * Note: Closing the migrate socket will also force SELECT next time. */
    sdsfree(cmd.io.buffer.ptr);

    /* If the command was rewritten as DEL and there was a socket error,
     * we already closed the socket earlier. While migrateCloseSocket()
     * is idempotent, the host/port arguments are now gone, so don't do it
     * again. */
    if (!argv_rewritten) migrateCloseSocket(c->argv[1], c->argv[2]);
    zfree(newargv);
    newargv = NULL; /* This will get reallocated on retry. */

    /* Retry only if it's not a timeout and we never attempted a retry
     * (or the code jumping here did not set may_retry to zero). */
    if (errno != ETIMEDOUT && may_retry) {
        may_retry = 0;
        goto try_again;
    }

    /* Cleanup we want to do if no retry is attempted. */
    zfree(ov);
    zfree(kv);
    addReplySds(c,
                sdscatprintf(sdsempty(),
                             "-IOERR error or timeout %s to target instance\r\n",
                             write_error ? "writing" : "reading"));
    return;
}

/* -----------------------------------------------------------------------------
 * Cluster functions related to serving / redirecting clients
 * -------------------------------------------------------------------------- */

/* The ASKING command is required after a -ASK redirection.
 * The client should issue ASKING before to actually send the command to
 * the target instance. See the Redis Cluster specification for more
 * information.
 * 给调用该方法的client追加一个ask标记
 * */
void askingCommand(client *c) {
    if (server.cluster_enabled == 0) {
        addReplyError(c, "This instance has cluster support disabled");
        return;
    }
    c->flags |= CLIENT_ASKING;
    addReply(c, shared.ok);
}

/* The READONLY command is used by clients to enter the read-only mode.
 * In this mode slaves will not redirect clients as long as clients access
 * with read-only commands to keys that are served by the slave's master.
 * 给client追加只读标记
 * */
void readonlyCommand(client *c) {
    if (server.cluster_enabled == 0) {
        addReplyError(c, "This instance has cluster support disabled");
        return;
    }
    c->flags |= CLIENT_READONLY;
    addReply(c, shared.ok);
}

/* The READWRITE command just clears the READONLY command state.
 * 恢复成可读写
 * */
void readwriteCommand(client *c) {
    c->flags &= ~CLIENT_READONLY;
    addReply(c, shared.ok);
}

/* Return the pointer to the cluster node that is able to serve the command.
 * For the function to succeed the command should only target either:
 *
 * 1) A single key (even multiple times like LPOPRPUSH mylist mylist).
 * 2) Multiple keys in the same hash slot, while the slot is stable (no
 *    resharding in progress).
 *
 * On success the function returns the node that is able to serve the request.
 * If the node is not 'myself' a redirection must be performed. The kind of
 * redirection is specified setting the integer passed by reference
 * 'error_code', which will be set to CLUSTER_REDIR_ASK or
 * CLUSTER_REDIR_MOVED.
 *
 * When the node is 'myself' 'error_code' is set to CLUSTER_REDIR_NONE.
 *
 * If the command fails NULL is returned, and the reason of the failure is
 * provided via 'error_code', which will be set to:
 *
 * CLUSTER_REDIR_CROSS_SLOT if the request contains multiple keys that
 * don't belong to the same hash slot.
 *
 * CLUSTER_REDIR_UNSTABLE if the request contains multiple keys
 * belonging to the same slot, but the slot is not stable (in migration or
 * importing state, likely because a resharding is in progress).
 *
 * CLUSTER_REDIR_DOWN_UNBOUND if the request addresses a slot which is
 * not bound to any node. In this case the cluster global state should be
 * already "down" but it is fragile to rely on the update of the global state,
 * so we also handle it here.
 *
 * CLUSTER_REDIR_DOWN_STATE and CLUSTER_REDIR_DOWN_RO_STATE if the cluster is
 * down but the user attempts to execute a command that addresses one or more keys.
 * 判断本次command相关的key是否在本节点上  是否需要转发到其他节点
 * */
clusterNode *getNodeByQuery(client *c, struct redisCommand *cmd, robj **argv, int argc, int *hashslot, int *error_code) {
    clusterNode *n = NULL;
    robj *firstkey = NULL;
    int multiple_keys = 0;
    multiState *ms, _ms;
    multiCmd mc;
    int i, slot = 0, migrating_slot = 0, importing_slot = 0, missing_keys = 0;

    /* Allow any key to be set if a module disabled cluster redirections.
     * 当集群中不允许转发时 直接在本节点处理
     * */
    if (server.cluster_module_flags & CLUSTER_MODULE_FLAG_NO_REDIRECTION)
        return myself;

    /* Set error code optimistically for the base case.
     * 先设置一个无异常信息
     * */
    if (error_code) *error_code = CLUSTER_REDIR_NONE;

    /* Modules can turn off Redis Cluster redirection: this is useful
     * when writing a module that implements a completely different
     * distributed system. */

    /* We handle all the cases as if they were EXEC commands, so we have
     * a common code path for everything
     * 如果本次是一个 exec命令 获取之前的multiState
     * */
    if (cmd->proc == execCommand) {
        /* If CLIENT_MULTI flag is not set EXEC is just going to return an
         * error. 这个属于异常情况 exec必须在multi命令后执行 */
        if (!(c->flags & CLIENT_MULTI)) return myself;
        ms = &c->mstate;
    } else {
        /* In order to have a single codepath create a fake Multi State
         * structure if the client is not in MULTI/EXEC state, this way
         * we have a single codepath below.
         * 生成一个command数量为1的 multiCommand
         * */
        ms = &_ms;
        _ms.commands = &mc;
        _ms.count = 1;
        mc.argv = argv;
        mc.argc = argc;
        mc.cmd = cmd;
    }

    /* Check that all the keys are in the same hash slot, and obtain this
     * slot and the node associated.
     * 执行子任务 每个command应该都会携带一组能够定位key的东西 然后将key转化成slot 获取slot对应的node
     * */
    for (i = 0; i < ms->count; i++) {
        struct redisCommand *mcmd;
        robj **margv;
        int margc, *keyindex, numkeys, j;

        mcmd = ms->commands[i].cmd;
        margc = ms->commands[i].argc;
        margv = ms->commands[i].argv;

        getKeysResult result = GETKEYS_RESULT_INIT;
        // 抽取本次command相关的key
        numkeys = getKeysFromCommand(mcmd, margv, margc, &result);
        keyindex = result.keys;

        // 遍历执行该command用到的所有key
        for (j = 0; j < numkeys; j++) {
            robj *thiskey = margv[keyindex[j]];
            // 将key转换成slot
            int thisslot = keyHashSlot((char *) thiskey->ptr,
                                       sdslen(thiskey->ptr));

            // 首次设置
            if (firstkey == NULL) {
                /* This is the first key we see. Check what is the slot
                 * and node. */
                firstkey = thiskey;
                slot = thisslot;
                // 该slot此时属于哪个节点
                n = server.cluster->slots[slot];

                /* Error: If a slot is not served, we are in "cluster down"
                 * state. However the state is yet to be updated, so this was
                 * not trapped earlier in processCommand(). Report the same
                 * error to the client.
                 * 此时slot还没有被分配到某个节点 无法执行命令
                 * */
                if (n == NULL) {
                    getKeysFreeResult(&result);
                    if (error_code)
                        *error_code = CLUSTER_REDIR_DOWN_UNBOUND;
                    return NULL;
                }

                /* If we are migrating or importing this slot, we need to check
                 * if we have all the keys in the request (the only way we
                 * can safely serve the request, otherwise we return a TRYAGAIN
                 * error). To do so we set the importing/migrating state and
                 * increment a counter for every missing key.
                 * slot分配到了本节点 同时本slot的数据即将迁出
                 * */
                if (n == myself &&
                    server.cluster->migrating_slots_to[slot] != NULL) {
                    migrating_slot = 1;
                    // 首先本节点是master节点 并且此时还没有该slot的数据 准备迁入
                } else if (server.cluster->importing_slots_from[slot] != NULL) {
                    importing_slot = 1;
                } // 其余情况可能该节点是slave
            } else {
                /* If it is not the first key, make sure it is exactly
                 * the same key as the first we saw.
                 * 本次事务的所有cmd中 出现了不同的key
                 * */
                if (!equalStringObjects(firstkey, thiskey)) {
                    // 所有key必须在同一slot下
                    if (slot != thisslot) {
                        /* Error: multiple keys from different slots.
                         * */
                        getKeysFreeResult(&result);
                        if (error_code)
                            *error_code = CLUSTER_REDIR_CROSS_SLOT;
                        return NULL;
                    } else {
                        /* Flag this request as one with multiple different
                         * keys.
                         * 在整个事务中出现了多个key
                         * */
                        multiple_keys = 1;
                    }
                }
            }

            /* Migrating / Importing slot? Count keys we don't have.
             * 代表本次要执行的key 不在本节点上
             * migrating_slot || importing_slot 这2种情况是考虑数据迁移的 首先本节点必须是master 其次数据即将迁出 或者数据还没有迁入
             * lookupKeyRead(&server.db[0], thiskey) 代表无论本节点是master还是slave 此时没有相关数据
             * */
            if ((migrating_slot || importing_slot) &&
                lookupKeyRead(&server.db[0], thiskey) == NULL) {
                // 每检测到一个key不存在就+1
                missing_keys++;
            }
        }
        getKeysFreeResult(&result);
    }

    /* No key at all in command? then we can serve the request
     * without redirections or errors in all the cases.
     * 代表所有命令都没有key 可以在本节点执行
     * */
    if (n == NULL) return myself;

    /* Cluster is globally down but we got keys? We only serve the request
     * if it is a read command and when allow_reads_when_down is enabled.
     * 如果此时集群不可用
     * */
    if (server.cluster->state != CLUSTER_OK){
        // 在集群不可用时 无法访问本节点 返回null 这样client的cmd就无法执行了
        if (!server.cluster_allow_reads_when_down) {
            /* The cluster is configured to block commands when the
             * cluster is down. */
            if (error_code) *error_code = CLUSTER_REDIR_DOWN_STATE;
            return NULL;
            // 集群不可用情况下无法执行写入任务
        } else if (!(cmd->flags & CMD_READONLY) && !(cmd->proc == evalCommand)
                   && !(cmd->proc == evalShaCommand)) {
            /* The cluster is configured to allow read only commands
             * but this command is neither readonly, nor EVAL or
             * EVALSHA. */
            if (error_code) *error_code = CLUSTER_REDIR_DOWN_RO_STATE;
            return NULL;
        } else {
            /* Fall through and allow the command to be executed:
             * this happens when server.cluster_allow_reads_when_down is
             * true and the command is a readonly command or EVAL / EVALSHA. */
        }
    }

    /* Return the hashslot by reference. 设置本次路由到的slot(必须是一个) */
    if (hashslot) *hashslot = slot;

    /* MIGRATE always works in the context of the local node if the slot
     * is open (migrating or importing state). We need to be able to freely
     * move keys among instances in this case.
     * 如果本次要执行的是迁移命令是允许的 TODO
     * */
    if ((migrating_slot || importing_slot) && cmd->proc == migrateCommand)
        return myself;

    /* If we don't have all the keys and we are migrating the slot, send
     * an ASK redirection.
     * 代表数据已经被迁移出去了 返回迁移的目标节点
     * */
    if (migrating_slot && missing_keys) {
        if (error_code) *error_code = CLUSTER_REDIR_ASK;
        return server.cluster->migrating_slots_to[slot];
    }

    /* If we are receiving the slot, and the client correctly flagged the
     * request as "ASKING", we can serve the request. However if the request
     * involves multiple keys and we don't have them all, the only option is
     * to send a TRYAGAIN error.
     * TODO
     * */
    if (importing_slot &&
        (c->flags & CLIENT_ASKING || cmd->flags & CMD_ASKING)) {
        if (multiple_keys && missing_keys) {
            if (error_code) *error_code = CLUSTER_REDIR_UNSTABLE;
            return NULL;
        } else {
            return myself;
        }
    }

    /* Handle the read-only client case reading from a slave: if this
     * node is a slave and the request is about a hash slot our master
     * is serving, we can reply without redirection.
     * 本次命令是只读 或者本次事务全是只读事务
     * */
    int is_readonly_command = (c->cmd->flags & CMD_READONLY) ||
                              (c->cmd->proc == execCommand && !(c->mstate.cmd_inv_flags & CMD_READONLY));
    // 本节点作为slave节点允许处理
    if (c->flags & CLIENT_READONLY &&
        (is_readonly_command || cmd->proc == evalCommand ||
         cmd->proc == evalShaCommand) &&
        nodeIsSlave(myself) &&
        myself->slaveof == n) {
        return myself;
    }

    /* Base case: just return the right node. However if this node is not
     * myself, set error_code to MOVED since we need to issue a redirection.
     * 本节点是master节点 并且本次slot不在本节点 将路由到的节点返回
     * */
    if (n != myself && error_code) *error_code = CLUSTER_REDIR_MOVED;
    return n;
}

/* Send the client the right redirection code, according to error_code
 * that should be set to one of CLUSTER_REDIR_* macros.
 *
 * If CLUSTER_REDIR_ASK or CLUSTER_REDIR_MOVED error codes
 * are used, then the node 'n' should not be NULL, but should be the
 * node we want to mention in the redirection. Moreover hashslot should
 * be set to the hash slot that caused the redirection.
 * 将错误信息返回给client
 * */
void clusterRedirectClient(client *c, clusterNode *n, int hashslot, int error_code) {
    if (error_code == CLUSTER_REDIR_CROSS_SLOT) {
        addReplySds(c, sdsnew("-CROSSSLOT Keys in request don't hash to the same slot\r\n"));
    } else if (error_code == CLUSTER_REDIR_UNSTABLE) {
        /* The request spawns multiple keys in the same slot,
         * but the slot is not "stable" currently as there is
         * a migration or import in progress. */
        addReplySds(c, sdsnew("-TRYAGAIN Multiple keys request during rehashing of slot\r\n"));

        // 将集群处于不可用状态的信息返回给client
    } else if (error_code == CLUSTER_REDIR_DOWN_STATE) {
        addReplySds(c, sdsnew("-CLUSTERDOWN The cluster is down\r\n"));
    } else if (error_code == CLUSTER_REDIR_DOWN_RO_STATE) {
        addReplySds(c, sdsnew("-CLUSTERDOWN The cluster is down and only accepts read commands\r\n"));
    } else if (error_code == CLUSTER_REDIR_DOWN_UNBOUND) {
        addReplySds(c, sdsnew("-CLUSTERDOWN Hash slot not served\r\n"));
    } else if (error_code == CLUSTER_REDIR_MOVED ||
               error_code == CLUSTER_REDIR_ASK) {
        addReplySds(c, sdscatprintf(sdsempty(),
                                    "-%s %d %s:%d\r\n",
                                    (error_code == CLUSTER_REDIR_ASK) ? "ASK" : "MOVED",
                                    hashslot, n->ip, n->port));
    } else {
        serverPanic("getNodeByQuery() unknown error.");
    }
}

/* This function is called by the function processing clients incrementally
 * to detect timeouts, in order to handle the following case:
 *
 * 1) A client blocks with BLPOP or similar blocking operation.
 * 2) The master migrates the hash slot elsewhere or turns into a slave.
 * 3) The client may remain blocked forever (or up to the max timeout time)
 *    waiting for a key change that will never happen.
 *
 * If the client is found to be blocked into a hash slot this node no
 * longer handles, the client is sent a redirection error, and the function
 * returns 1. Otherwise 0 is returned and no operation is performed.
 * 当该client处于阻塞状态时 判断能否解除阻塞
 * */
int clusterRedirectBlockedClientIfNeeded(client *c) {
    // 代表由于这3种类型导致的阻塞
    if (c->flags & CLIENT_BLOCKED &&
        (c->btype == BLOCKED_LIST ||
         c->btype == BLOCKED_ZSET ||
         c->btype == BLOCKED_STREAM)) {
        dictEntry *de;
        dictIterator *di;

        /* If the cluster is down, unblock the client with the right error.
         * If the cluster is configured to allow reads on cluster down, we
         * still want to emit this error since a write will be required
         * to unblock them which may never come.
         * 此时本节点认为集群处于不可用状态 会解除阻塞
         * */
        if (server.cluster->state == CLUSTER_FAIL) {
            clusterRedirectClient(c, NULL, 0, CLUSTER_REDIR_DOWN_STATE);
            return 1;
        }

        /* All keys must belong to the same slot, so check first key only.
         * 被阻塞的所有key必然属于同一个slot 所以这里只要看一个slot就好
         * */
        di = dictGetIterator(c->bpop.keys);
        if ((de = dictNext(di)) != NULL) {
            robj *key = dictGetKey(de);
            int slot = keyHashSlot((char *) key->ptr, sdslen(key->ptr));
            // 查看这些key此时应该存在于哪些节点上
            clusterNode *node = server.cluster->slots[slot];

            /* if the client is read-only and attempting to access key that our
             * replica can handle, allow it.
             * 本节点作为拥有该slot的master的子节点 并且执行的是只读命令 就代表还是由本节点处理 不会发生重定向 原先阻塞的client会继续保持阻塞
             * */
            if ((c->flags & CLIENT_READONLY) &&
                (c->lastcmd->flags & CMD_READONLY) &&
                nodeIsSlave(myself) && myself->slaveof == node) {
                node = myself;
            }

            /* We send an error and unblock the client if:
             * 1) The slot is unassigned, emitting a cluster down error.
             * 2) The slot is not handled by this node, nor being imported.
             * 此时key已经被转移到其他节点了 继续阻塞client也没有意义
             * 并且本节点并没有打算从其他节点拉取该slot的数据
             * */
            if (node != myself &&
                server.cluster->importing_slots_from[slot] == NULL) {
                if (node == NULL) {
                    clusterRedirectClient(c, NULL, 0,
                                          CLUSTER_REDIR_DOWN_UNBOUND);
                } else {
                    clusterRedirectClient(c, node, slot,
                                          CLUSTER_REDIR_MOVED);
                }
                dictReleaseIterator(di);
                return 1;
            }
        }
        dictReleaseIterator(di);
    }
    return 0;
}
