/* rio.c is a simple stream-oriented I/O abstraction that provides an interface
 * to write code that can consume/produce data using different concrete input
 * and output devices. For instance the same rdb.c code using the rio
 * abstraction can be used to read and write the RDB format using in-memory
 * buffers or files.
 *
 * A rio object provides the following methods:
 *  read: read from stream.
 *  write: write to stream.
 *  tell: get the current offset.
 *
 * It is also possible to set a 'checksum' method that is used by rio.c in order
 * to compute a checksum of the data written or read, or to query the rio object
 * for the current checksum.
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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


#include "fmacros.h"
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include "rio.h"
#include "util.h"
#include "crc64.h"
#include "config.h"
#include "server.h"

/* ------------------------- Buffer I/O implementation ----------------------- */

/* Returns 1 or 0 for success/failure.
 * 将buf中的数据追加到 rio中
 * */
static size_t rioBufferWrite(rio *r, const void *buf, size_t len) {
    r->io.buffer.ptr = sdscatlen(r->io.buffer.ptr,(char*)buf,len);
    r->io.buffer.pos += len;
    return 1;
}

/* Returns 1 or 0 for success/failure.
 * 从rio的缓冲区中读取数据 并设置到入参中
 * */
static size_t rioBufferRead(rio *r, void *buf, size_t len) {
    if (sdslen(r->io.buffer.ptr)-r->io.buffer.pos < len)
        return 0; /* not enough buffer to return len bytes. */
    memcpy(buf,r->io.buffer.ptr+r->io.buffer.pos,len);
    r->io.buffer.pos += len;
    return 1;
}

/* Returns read/write position in buffer.
 * 获取此时buffer的指针位置
 * */
static off_t rioBufferTell(rio *r) {
    return r->io.buffer.pos;
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures.
 * rio本身是模拟一个io流 他不支持刷盘
 * */
static int rioBufferFlush(rio *r) {
    UNUSED(r);
    return 1; /* Nothing to do, our write just appends to the buffer. */
}

static const rio rioBufferIO = {
    rioBufferRead,
    rioBufferWrite,
    rioBufferTell,
    rioBufferFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* flags */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

/**
 * 使用一个存在的sds初始化rio结构
 * @param r
 * @param s
 */
void rioInitWithBuffer(rio *r, sds s) {
    *r = rioBufferIO;
    r->io.buffer.ptr = s;
    r->io.buffer.pos = 0;
}

/* --------------------- Stdio file pointer implementation ------------------- */

/* Returns 1 or 0 for success/failure.
 * 将buf数据写入到 rio流中
 * */
static size_t rioFileWrite(rio *r, const void *buf, size_t len) {
    size_t retval;

    // 将buf中的数据直接写入到rio关联的文件句柄
    retval = fwrite(buf,len,1,r->io.file.fp);

    // 往文件缓冲区中写入了多少数据 还需要刷盘才能交换到磁盘中
    r->io.file.buffered += len;

    // 代表有数据还没有刷盘
    if (r->io.file.autosync &&
        r->io.file.buffered >= r->io.file.autosync)
    {
        fflush(r->io.file.fp);
        // fsync
        redis_fsync(fileno(r->io.file.fp));
        r->io.file.buffered = 0;
    }
    return retval;
}

/* Returns 1 or 0 for success/failure.
 * 从一个rio流中读取 len长度的数据 并写入到buf中
 * */
static size_t rioFileRead(rio *r, void *buf, size_t len) {
    return fread(buf,len,1,r->io.file.fp);
}

/* Returns read/write position in file.
 * 获取此时fp的偏移量
 * */
static off_t rioFileTell(rio *r) {
    return ftello(r->io.file.fp);
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures.
 * 将文件数据刷盘
 * */
static int rioFileFlush(rio *r) {
    return (fflush(r->io.file.fp) == 0) ? 1 : 0;
}

static const rio rioFileIO = {
    rioFileRead,
    rioFileWrite,
    rioFileTell,
    rioFileFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* flags */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

/**
 * 通过某个文件句柄 初始化rio
 * @param r
 * @param fp
 */
void rioInitWithFile(rio *r, FILE *fp) {
    *r = rioFileIO;
    r->io.file.fp = fp;
    // 下面分别代表这个流中此时的数据 以及此时已经刷盘的量
    r->io.file.buffered = 0;
    r->io.file.autosync = 0;
}

/* ------------------- Connection implementation -------------------
 * We use this RIO implemetnation when reading an RDB file directly from
 * the connection to the memory via rdbLoadRio(), thus this implementation
 * only implements reading from a connection that is, normally,
 * just a socket.
 */
static size_t rioConnWrite(rio *r, const void *buf, size_t len) {
    UNUSED(r);
    UNUSED(buf);
    UNUSED(len);
    return 0; /* Error, this target does not yet support writing. */
}

/* Returns 1 or 0 for success/failure.
 * 从rio中读取len长度的数据  输入到buf中
 * 如果此时rio中数据不足 则直接从rio关联的网络缓冲区读取数据  以确保读取len长度的数据
 * */
static size_t rioConnRead(rio *r, void *buf, size_t len) {
    // 差值部分代表还未读取的数据
    size_t avail = sdslen(r->io.conn.buf)-r->io.conn.pos;

    /* If the buffer is too small for the entire request: realloc.
     * sdslen(r->io.conn.buf) + sdsavail(r->io.conn.buf) 对应整个sds结构的长度
     * */
    if (sdslen(r->io.conn.buf) + sdsavail(r->io.conn.buf) < len)
        r->io.conn.buf = sdsMakeRoomFor(r->io.conn.buf, len - sdslen(r->io.conn.buf));

    /* If the remaining unused buffer is not large enough: memmove so that we
     * can read the rest.
     * 如果sds中空间不是很多 就释放已读的部分
     * */
    if (len > avail && sdsavail(r->io.conn.buf) < len - avail) {
        // 相当于是移除掉 0～conn.pos
        sdsrange(r->io.conn.buf, r->io.conn.pos, -1);
        r->io.conn.pos = 0;
    }

    /* If we don't already have all the data in the sds, read more
     * 此时要读取的长度超过了 sds的总长度
     * */
    while (len > sdslen(r->io.conn.buf) - r->io.conn.pos) {
        // 此时还未读取的长度
        size_t buffered = sdslen(r->io.conn.buf) - r->io.conn.pos;
        // 代表之后还需要读取这么多数据
        size_t needs = len - buffered;
        /* Read either what's missing, or PROTO_IOBUF_LEN, the bigger of
         * the two.
         * */
        size_t toread = needs < PROTO_IOBUF_LEN ? PROTO_IOBUF_LEN: needs;
        if (toread > sdsavail(r->io.conn.buf)) toread = sdsavail(r->io.conn.buf);

        // 代表本次读取的数据长度超过了一个限制
        if (r->io.conn.read_limit != 0 &&
            r->io.conn.read_so_far + buffered + toread > r->io.conn.read_limit)
        {
            /* Make sure the caller didn't request to read past the limit.
             * If they didn't we'll buffer till the limit, if they did, we'll
             * return an error.
             * 主要就是修正 toread的值
             * */
            if (r->io.conn.read_limit >= r->io.conn.read_so_far + len)
                toread = r->io.conn.read_limit - r->io.conn.read_so_far - buffered;
            else {
                errno = EOVERFLOW;
                return 0;
            }
        }
        // 从conn中读取数据
        int retval = connRead(r->io.conn.conn,
                          (char*)r->io.conn.buf + sdslen(r->io.conn.buf),
                          toread);
        // 代表数据不足
        if (retval <= 0) {
            if (errno == EWOULDBLOCK) errno = ETIMEDOUT;
            return 0;
        }
        // 因为数据追加到了sds中 增加len
        sdsIncrLen(r->io.conn.buf, retval);
    }

    // 将数据转移到目标buf中
    memcpy(buf, (char*)r->io.conn.buf + r->io.conn.pos, len);
    r->io.conn.read_so_far += len;
    r->io.conn.pos += len;
    return len;
}

/* Returns read/write position in file.
 * 上次从rio.conn中读取了多少数据
 * */
static off_t rioConnTell(rio *r) {
    return r->io.conn.read_so_far;
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures.
 * flush 本身不会做任何操作
 * */
static int rioConnFlush(rio *r) {
    /* Our flush is implemented by the write method, that recognizes a
     * buffer set to NULL with a count of zero as a flush request. */
    return rioConnWrite(r,NULL,0);
}

static const rio rioConnIO = {
    rioConnRead,
    rioConnWrite,
    rioConnTell,
    rioConnFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* flags */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

/* Create an RIO that implements a buffered read from an fd
 * read_limit argument stops buffering when the reaching the limit.
 * 使用conn 和一个read限制量初始化一个rio对象
 * */
void rioInitWithConn(rio *r, connection *conn, size_t read_limit) {
    *r = rioConnIO;
    r->io.conn.conn = conn;
    r->io.conn.pos = 0;
    r->io.conn.read_limit = read_limit;
    r->io.conn.read_so_far = 0;
    r->io.conn.buf = sdsnewlen(NULL, PROTO_IOBUF_LEN);
    sdsclear(r->io.conn.buf);
}

/* Release the RIO tream. Optionally returns the unread buffered data
 * when the SDS pointer 'remaining' is passed.
 * 释放conn.buf 并用remaining指向剩余的数据
 * */
void rioFreeConn(rio *r, sds *remaining) {
    // 代表还需要剩余的数据
    if (remaining && (size_t)r->io.conn.pos < sdslen(r->io.conn.buf)) {
        // 裁剪掉读取过的部分
        if (r->io.conn.pos > 0) sdsrange(r->io.conn.buf, r->io.conn.pos, -1);
        *remaining = r->io.conn.buf;
    } else {
        sdsfree(r->io.conn.buf);
        if (remaining) *remaining = NULL;
    }
    r->io.conn.buf = NULL;
}

/* ------------------- File descriptor implementation ------------------
 * This target is used to write the RDB file to pipe, when the master just
 * streams the data to the replicas without creating an RDB on-disk image
 * (diskless replication option).
 * It only implements writes. */

/* Returns 1 or 0 for success/failure.
 *
 * When buf is NULL and len is 0, the function performs a flush operation
 * if there is some pending buffer, so this function is also used in order
 * to implement rioFdFlush().
 * 将buf中的数据写入到rio.io.fd.buf
 * */
static size_t rioFdWrite(rio *r, const void *buf, size_t len) {
    ssize_t retval;
    unsigned char *p = (unsigned char*) buf;
    int doflush = (buf == NULL && len == 0);

    /* For small writes, we rather keep the data in user-space buffer, and flush
     * it only when it grows. however for larger writes, we prefer to flush
     * any pre-existing buffer, and write the new one directly without reallocs
     * and memory copying.
     * 如果本次要写入的长度比较大 先检测之前有多少数据
     * */
    if (len > PROTO_IOBUF_LEN) {
        /* First, flush any pre-existing buffered data. */
        if (sdslen(r->io.fd.buf)) {
            // 先将之前的数据刷盘
            if (rioFdWrite(r, NULL, 0) == 0)
                return 0;
        }
        /* Write the new data, keeping 'p' and 'len' from the input. */
    } else {
        if (len) {
            // 将本次数据追加到sds上
            r->io.fd.buf = sdscatlen(r->io.fd.buf,buf,len);
            // 当数据量超过一定值时 就要进行刷盘
            if (sdslen(r->io.fd.buf) > PROTO_IOBUF_LEN)
                doflush = 1;
            // 不需要刷盘直接返回
            if (!doflush)
                return 1;
        }
        /* Flusing the buffered data. set 'p' and 'len' accordintly.
         * 这里是要刷盘的buf指针 以及刷盘的长度
         * */
        p = (unsigned char*) r->io.fd.buf;
        len = sdslen(r->io.fd.buf);
    }

    // fd.buf的刷盘就是将数据写入到文件句柄中
    size_t nwritten = 0;
    while(nwritten != len) {
        retval = write(r->io.fd.fd,p+nwritten,len-nwritten);
        if (retval <= 0) {
            /* With blocking io, which is the sole user of this
             * rio target, EWOULDBLOCK is returned only because of
             * the SO_SNDTIMEO socket option, so we translate the error
             * into one more recognizable by the user. */
            if (retval == -1 && errno == EWOULDBLOCK) errno = ETIMEDOUT;
            return 0; /* error. */
        }
        nwritten += retval;
    }

    // fd.pos 记录的是往文件内写入的数据量
    // 将buf中的数据清空
    r->io.fd.pos += len;
    sdsclear(r->io.fd.buf);
    return 1;
}

/* Returns 1 or 0 for success/failure. */
static size_t rioFdRead(rio *r, void *buf, size_t len) {
    UNUSED(r);
    UNUSED(buf);
    UNUSED(len);
    return 0; /* Error, this target does not support reading. */
}

/* Returns read/write position in file.
 * 返回此时文件的偏移量
 * */
static off_t rioFdTell(rio *r) {
    return r->io.fd.pos;
}

/* Flushes any buffer to target device if applicable. Returns 1 on success
 * and 0 on failures. */
static int rioFdFlush(rio *r) {
    /* Our flush is implemented by the write method, that recognizes a
     * buffer set to NULL with a count of zero as a flush request.
     * 长度为0 会直接触发一次刷盘
     * */
    return rioFdWrite(r,NULL,0);
}

static const rio rioFdIO = {
    rioFdRead,
    rioFdWrite,
    rioFdTell,
    rioFdFlush,
    NULL,           /* update_checksum */
    0,              /* current checksum */
    0,              /* flags */
    0,              /* bytes read or written */
    0,              /* read/write chunk size */
    { { NULL, 0 } } /* union for io-specific vars */
};

/**
 * 通过文件句柄 初始化rio
 * @param r
 * @param fd
 */
void rioInitWithFd(rio *r, int fd) {
    *r = rioFdIO;
    r->io.fd.fd = fd;
    r->io.fd.pos = 0;
    r->io.fd.buf = sdsempty();
}

/* release the rio stream. */
void rioFreeFd(rio *r) {
    sdsfree(r->io.fd.buf);
}

/* ---------------------------- Generic functions ---------------------------- */

/* This function can be installed both in memory and file streams when checksum
 * computation is needed.
 * 更新校验和信息
 * */
void rioGenericUpdateChecksum(rio *r, const void *buf, size_t len) {
    r->cksum = crc64(r->cksum,buf,len);
}

/* Set the file-based rio object to auto-fsync every 'bytes' file written.
 * By default this is set to zero that means no automatic file sync is
 * performed.
 *
 * This feature is useful in a few contexts since when we rely on OS write
 * buffers sometimes the OS buffers way too much, resulting in too many
 * disk I/O concentrated in very little time. When we fsync in an explicit
 * way instead the I/O pressure is more distributed across time.
 * */
void rioSetAutoSync(rio *r, off_t bytes) {
    // 只有当rio底层为文件时 才可以设置自动刷盘配置
    if(r->write != rioFileIO.write) return;
    r->io.file.autosync = bytes;
}

/* --------------------------- Higher level interface --------------------------
 *
 * The following higher level functions use lower level rio.c functions to help
 * generating the Redis protocol for the Append Only File. */

/* Write multi bulk count in the format: "*<count>\r\n".
 * 将一个计数值写入到rio中
 * */
size_t rioWriteBulkCount(rio *r, char prefix, long count) {
    char cbuf[128];
    int clen;

    cbuf[0] = prefix;
    clen = 1+ll2string(cbuf+1,sizeof(cbuf)-1,count);
    cbuf[clen++] = '\r';
    cbuf[clen++] = '\n';
    // 将buf中的数据写入到rio流中
    if (rioWrite(r,cbuf,clen) == 0) return 0;
    return clen;
}

/* Write binary-safe string in the format: "$<count>\r\n<payload>\r\n". */
size_t rioWriteBulkString(rio *r, const char *buf, size_t len) {
    size_t nwritten;

    if ((nwritten = rioWriteBulkCount(r,'$',len)) == 0) return 0;
    if (len > 0 && rioWrite(r,buf,len) == 0) return 0;
    if (rioWrite(r,"\r\n",2) == 0) return 0;
    return nwritten+len+2;
}

/* Write a long long value in format: "$<count>\r\n<payload>\r\n". */
size_t rioWriteBulkLongLong(rio *r, long long l) {
    char lbuf[32];
    unsigned int llen;

    llen = ll2string(lbuf,sizeof(lbuf),l);
    return rioWriteBulkString(r,lbuf,llen);
}

/* Write a double value in the format: "$<count>\r\n<payload>\r\n" */
size_t rioWriteBulkDouble(rio *r, double d) {
    char dbuf[128];
    unsigned int dlen;

    dlen = snprintf(dbuf,sizeof(dbuf),"%.17g",d);
    return rioWriteBulkString(r,dbuf,dlen);
}
