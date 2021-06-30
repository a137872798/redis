/*
 * Copyright (c) 2009-2011, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2010-2011, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 *
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
#include <stdlib.h>
#ifndef _MSC_VER
#include <unistd.h>
#include <strings.h>
#endif
#include <assert.h>
#include <errno.h>
#include <ctype.h>
#include <limits.h>
#include <math.h>

#include "alloc.h"
#include "read.h"
#include "sds.h"
#include "win32.h"

/* Initial size of our nested reply stack and how much we grow it when needd */
#define REDIS_READER_STACK_SIZE 9

static void __redisReaderSetError(redisReader *r, int type, const char *str) {
    size_t len;

    if (r->reply != NULL && r->fn && r->fn->freeObject) {
        r->fn->freeObject(r->reply);
        r->reply = NULL;
    }

    /* Clear input buffer on errors. */
    hi_sdsfree(r->buf);
    r->buf = NULL;
    r->pos = r->len = 0;

    /* Reset task stack. */
    r->ridx = -1;

    /* Set error. */
    r->err = type;
    len = strlen(str);
    len = len < (sizeof(r->errstr)-1) ? len : (sizeof(r->errstr)-1);
    memcpy(r->errstr,str,len);
    r->errstr[len] = '\0';
}

static size_t chrtos(char *buf, size_t size, char byte) {
    size_t len = 0;

    switch(byte) {
    case '\\':
    case '"':
        len = snprintf(buf,size,"\"\\%c\"",byte);
        break;
    case '\n': len = snprintf(buf,size,"\"\\n\""); break;
    case '\r': len = snprintf(buf,size,"\"\\r\""); break;
    case '\t': len = snprintf(buf,size,"\"\\t\""); break;
    case '\a': len = snprintf(buf,size,"\"\\a\""); break;
    case '\b': len = snprintf(buf,size,"\"\\b\""); break;
    default:
        if (isprint(byte))
            len = snprintf(buf,size,"\"%c\"",byte);
        else
            len = snprintf(buf,size,"\"\\x%02x\"",(unsigned char)byte);
        break;
    }

    return len;
}

static void __redisReaderSetErrorProtocolByte(redisReader *r, char byte) {
    char cbuf[8], sbuf[128];

    chrtos(cbuf,sizeof(cbuf),byte);
    snprintf(sbuf,sizeof(sbuf),
        "Protocol error, got %s as reply type byte", cbuf);
    __redisReaderSetError(r,REDIS_ERR_PROTOCOL,sbuf);
}

static void __redisReaderSetErrorOOM(redisReader *r) {
    __redisReaderSetError(r,REDIS_ERR_OOM,"Out of memory");
}

/**
 * 读取buffer中的数据
 * @param r
 * @param bytes
 * @return
 */
static char *readBytes(redisReader *r, unsigned int bytes) {
    char *p;
    if (r->len-r->pos >= bytes) {
        p = r->buf+r->pos;
        r->pos += bytes;
        return p;
    }
    return NULL;
}

/* Find pointer to \r\n. */
static char *seekNewline(char *s, size_t len) {
    int pos = 0;
    int _len = len-1;

    /* Position should be < len-1 because the character at "pos" should be
     * followed by a \n. Note that strchr cannot be used because it doesn't
     * allow to search a limited length and the buffer that is being searched
     * might not have a trailing NULL character. */
    while (pos < _len) {
        while(pos < _len && s[pos] != '\r') pos++;
        if (pos==_len) {
            /* Not found. */
            return NULL;
        } else {
            if (s[pos+1] == '\n') {
                /* Found. */
                return s+pos;
            } else {
                /* Continue searching. */
                pos++;
            }
        }
    }
    return NULL;
}

/* Convert a string into a long long. Returns REDIS_OK if the string could be
 * parsed into a (non-overflowing) long long, REDIS_ERR otherwise. The value
 * will be set to the parsed value when appropriate.
 *
 * Note that this function demands that the string strictly represents
 * a long long: no spaces or other characters before or after the string
 * representing the number are accepted, nor zeroes at the start if not
 * for the string "0" representing the zero number.
 *
 * Because of its strictness, it is safe to use this function to check if
 * you can convert a string into a long long, and obtain back the string
 * from the number without any loss in the string representation. */
static int string2ll(const char *s, size_t slen, long long *value) {
    const char *p = s;
    size_t plen = 0;
    int negative = 0;
    unsigned long long v;

    if (plen == slen)
        return REDIS_ERR;

    /* Special case: first and only digit is 0. */
    if (slen == 1 && p[0] == '0') {
        if (value != NULL) *value = 0;
        return REDIS_OK;
    }

    if (p[0] == '-') {
        negative = 1;
        p++; plen++;

        /* Abort on only a negative sign. */
        if (plen == slen)
            return REDIS_ERR;
    }

    /* First digit should be 1-9, otherwise the string should just be 0. */
    if (p[0] >= '1' && p[0] <= '9') {
        v = p[0]-'0';
        p++; plen++;
    } else if (p[0] == '0' && slen == 1) {
        *value = 0;
        return REDIS_OK;
    } else {
        return REDIS_ERR;
    }

    while (plen < slen && p[0] >= '0' && p[0] <= '9') {
        if (v > (ULLONG_MAX / 10)) /* Overflow. */
            return REDIS_ERR;
        v *= 10;

        if (v > (ULLONG_MAX - (p[0]-'0'))) /* Overflow. */
            return REDIS_ERR;
        v += p[0]-'0';

        p++; plen++;
    }

    /* Return if not all bytes were used. */
    if (plen < slen)
        return REDIS_ERR;

    if (negative) {
        if (v > ((unsigned long long)(-(LLONG_MIN+1))+1)) /* Overflow. */
            return REDIS_ERR;
        if (value != NULL) *value = -v;
    } else {
        if (v > LLONG_MAX) /* Overflow. */
            return REDIS_ERR;
        if (value != NULL) *value = v;
    }
    return REDIS_OK;
}

/**
 * @param r
 * @param _len
 * @return
 */
static char *readLine(redisReader *r, int *_len) {
    char *p, *s;
    int len;

    p = r->buf+r->pos;
    // 读取数据 直到遇到换行符
    s = seekNewline(p,(r->len-r->pos));
    if (s != NULL) {
        // 这是本次新读取的长度
        len = s-(r->buf+r->pos);
        // 更新pos 因为本次读取直到换行符为止 所以这里要增加一个换行符的偏移量
        r->pos += len+2; /* skip \r\n */
        if (_len) *_len = len;
        return p;
    }
    return NULL;
}

/**
 * 读取同一个task的下一个ele  如果该task下所有ele都访问完毕后 读取前一个task
 * @param r
 */
static void moveToNextTask(redisReader *r) {
    redisReadTask *cur, *prv;
    // 这是从后往前么
    while (r->ridx >= 0) {
        /* Return a.s.a.p. when the stack is now empty.
         * 代表没有数据可以读取了
         * */
        if (r->ridx == 0) {
            r->ridx--;
            return;
        }

        cur = r->task[r->ridx];
        prv = r->task[r->ridx-1];
        assert(prv->type == REDIS_REPLY_ARRAY ||
               prv->type == REDIS_REPLY_MAP ||
               prv->type == REDIS_REPLY_SET ||
               prv->type == REDIS_REPLY_PUSH);
        // 每个task 下面还有一组数据 这里idx就是这组数据的下标  当该task下所有的ele都处理过后 就可以转到前一个task
        if (cur->idx == prv->elements-1) {
            r->ridx--;
        } else {
            /* Reset the type because the next item can be anything
             * 读取下一个ele
             * */
            assert(cur->idx < prv->elements);
            cur->type = -1;
            cur->elements = -1;
            cur->idx++;
            return;
        }
    }
}

/**
 * 本次task对应的数据 只使用了一行数据来表示
 * @param r
 * @return
 */
static int processLineItem(redisReader *r) {
    redisReadTask *cur = r->task[r->ridx];
    void *obj;
    char *p;
    int len;

    // p作为新行数据的起始指针
    if ((p = readLine(r,&len)) != NULL) {
        // 根据类型创建对象并存储数据
        if (cur->type == REDIS_REPLY_INTEGER) {
            if (r->fn && r->fn->createInteger) {
                long long v;
                if (string2ll(p, len, &v) == REDIS_ERR) {
                    __redisReaderSetError(r,REDIS_ERR_PROTOCOL,
                            "Bad integer value");
                    return REDIS_ERR;
                }
                obj = r->fn->createInteger(cur,v);
            } else {
                obj = (void*)REDIS_REPLY_INTEGER;
            }
        } else if (cur->type == REDIS_REPLY_DOUBLE) {
            if (r->fn && r->fn->createDouble) {
                char buf[326], *eptr;
                double d;

                if ((size_t)len >= sizeof(buf)) {
                    __redisReaderSetError(r,REDIS_ERR_PROTOCOL,
                            "Double value is too large");
                    return REDIS_ERR;
                }

                memcpy(buf,p,len);
                buf[len] = '\0';

                if (strcasecmp(buf,",inf") == 0) {
                    d = INFINITY; /* Positive infinite. */
                } else if (strcasecmp(buf,",-inf") == 0) {
                    d = -INFINITY; /* Negative infinite. */
                } else {
                    d = strtod((char*)buf,&eptr);
                    if (buf[0] == '\0' || eptr[0] != '\0' || isnan(d)) {
                        __redisReaderSetError(r,REDIS_ERR_PROTOCOL,
                                "Bad double value");
                        return REDIS_ERR;
                    }
                }
                obj = r->fn->createDouble(cur,d,buf,len);
            } else {
                obj = (void*)REDIS_REPLY_DOUBLE;
            }
        } else if (cur->type == REDIS_REPLY_NIL) {
            if (r->fn && r->fn->createNil)
                obj = r->fn->createNil(cur);
            else
                obj = (void*)REDIS_REPLY_NIL;
        } else if (cur->type == REDIS_REPLY_BOOL) {
            int bval = p[0] == 't' || p[0] == 'T';
            if (r->fn && r->fn->createBool)
                obj = r->fn->createBool(cur,bval);
            else
                obj = (void*)REDIS_REPLY_BOOL;
        } else {
            /* Type will be error or status. */
            if (r->fn && r->fn->createString)
                obj = r->fn->createString(cur,p,len);
            else
                obj = (void*)(size_t)(cur->type);
        }

        if (obj == NULL) {
            __redisReaderSetErrorOOM(r);
            return REDIS_ERR;
        }

        /* Set reply if this is the root object.
         * 如果此时读取的task是第一个 将reader->reply 指向该task封装后的object对象
         * */
        if (r->ridx == 0) r->reply = obj;
        // 读取下一个对象
        moveToNextTask(r);
        return REDIS_OK;
    }

    return REDIS_ERR;
}

/**
 * 读取大块数据
 * @param r
 * @return
 */
static int processBulkItem(redisReader *r) {
    redisReadTask *cur = r->task[r->ridx];
    void *obj = NULL;
    char *p, *s;
    long long len;
    unsigned long bytelen;
    int success = 0;

    p = r->buf+r->pos;
    // 该行数据记录的是 大块数据的长度信息
    s = seekNewline(p,r->len-r->pos);
    if (s != NULL) {
        p = r->buf+r->pos;
        // 该行数据长度
        bytelen = s-(r->buf+r->pos)+2; /* include \r\n */

        // 转换成long类型
        if (string2ll(p, bytelen - 2, &len) == REDIS_ERR) {
            __redisReaderSetError(r,REDIS_ERR_PROTOCOL,
                    "Bad bulk string length");
            return REDIS_ERR;
        }

        // len不合法
        if (len < -1 || (LLONG_MAX > SIZE_MAX && len > (long long)SIZE_MAX)) {
            __redisReaderSetError(r,REDIS_ERR_PROTOCOL,
                    "Bulk string length out of range");
            return REDIS_ERR;
        }

        // TODO 怎么出现-1
        if (len == -1) {
            /* The nil object can always be created. */
            if (r->fn && r->fn->createNil)
                obj = r->fn->createNil(cur);
            else
                obj = (void*)REDIS_REPLY_NIL;
            success = 1;
        } else {
            /* Only continue when the buffer contains the entire bulk item. */
            bytelen += len+2; /* include \r\n */
            // 这应该是必然的 因为上面的seekNewLine 最大范围就是 r->len
            if (r->pos+bytelen <= r->len) {

                // 代表数据格式不合法
                if ((cur->type == REDIS_REPLY_VERB && len < 4) ||
                    (cur->type == REDIS_REPLY_VERB && s[5] != ':'))
                {
                    __redisReaderSetError(r,REDIS_ERR_PROTOCOL,
                            "Verbatim string 4 bytes of content type are "
                            "missing or incorrectly encoded.");
                    return REDIS_ERR;
                }
                if (r->fn && r->fn->createString)
                    obj = r->fn->createString(cur,s+2,len);
                else
                    obj = (void*)(long)cur->type;
                success = 1;
            }
        }

        /* Proceed when obj was created.
         * 代表对象被成功创建 移动偏移量
         * */
        if (success) {
            if (obj == NULL) {
                __redisReaderSetErrorOOM(r);
                return REDIS_ERR;
            }

            r->pos += bytelen;

            /* Set reply if this is the root object.
             * 代表此时已经指向根task了 将reader->reply指向该obj
             * */
            if (r->ridx == 0) r->reply = obj;
            moveToNextTask(r);
            return REDIS_OK;
        }
    }

    return REDIS_ERR;
}

/**
 * 对task数组进行扩容
 * @param r
 * @return
 */
static int redisReaderGrow(redisReader *r) {
    redisReadTask **aux;
    int newlen;

    /* Grow our stack size
     * 每次扩容都是默认增加9个slot
     * */
    newlen = r->tasks + REDIS_READER_STACK_SIZE;
    // 这个realloc 应该是包含旧数据的拷贝
    aux = hi_realloc(r->task, sizeof(*r->task) * newlen);
    if (aux == NULL)
        goto oom;

    r->task = aux;

    /* Allocate new tasks
     * 上面不是已经分配足够的空间了么 这里calloc是什么意思 c相关的函数不影响理解 主要就是对tasks进行扩容
     * */
    for (; r->tasks < newlen; r->tasks++) {
        r->task[r->tasks] = hi_calloc(1, sizeof(**r->task));
        if (r->task[r->tasks] == NULL)
            goto oom;
    }

    return REDIS_OK;
oom:
    __redisReaderSetErrorOOM(r);
    return REDIS_ERR;
}

/* Process the array, map and set types.
 * 代表读取到的数据是一个 数组/map/set
 * */
static int processAggregateItem(redisReader *r) {
    redisReadTask *cur = r->task[r->ridx];
    void *obj;
    char *p;
    long long elements;
    int root = 0, len;

    /* Set error for nested multi bulks with depth > 7
     * 此时正在填充最后一个task 就会自动对task进行扩容
     * */
    if (r->ridx == r->tasks - 1) {
        if (redisReaderGrow(r) == REDIS_ERR)
            return REDIS_ERR;
    }

    // 先读取一行表示ele数量的信息
    if ((p = readLine(r,&len)) != NULL) {
        if (string2ll(p, len, &elements) == REDIS_ERR) {
            __redisReaderSetError(r,REDIS_ERR_PROTOCOL,
                    "Bad multi-bulk length");
            return REDIS_ERR;
        }

        // 判断本次读取的是否是root对象 是会怎么样 ???
        root = (r->ridx == 0);

        // 聚合数据的数量超过限制
        if (elements < -1 || (LLONG_MAX > SIZE_MAX && elements > SIZE_MAX) ||
            (r->maxelements > 0 && elements > r->maxelements))
        {
            __redisReaderSetError(r,REDIS_ERR_PROTOCOL,
                    "Multi-bulk length out of range");
            return REDIS_ERR;
        }

        // 看来这个nil对象就是指空对象
        if (elements == -1) {
            if (r->fn && r->fn->createNil)
                obj = r->fn->createNil(cur);
            else
                obj = (void*)REDIS_REPLY_NIL;

            if (obj == NULL) {
                __redisReaderSetErrorOOM(r);
                return REDIS_ERR;
            }

            // 切换到下一个元素
            moveToNextTask(r);

            // 代表本次需要解析多个ele
        } else {
            // 如果是map类型 那么要读取的ele翻倍
            if (cur->type == REDIS_REPLY_MAP) elements *= 2;

            // 这些process方法都只是创建obj对象 也没有做什么数据填充
            if (r->fn && r->fn->createArray)
                obj = r->fn->createArray(cur,elements);
            else
                obj = (void*)(long)cur->type;

            if (obj == NULL) {
                __redisReaderSetErrorOOM(r);
                return REDIS_ERR;
            }

            /* Modify task stack when there are more than 0 elements.
             * 如果此时该reply下有多个ele对象  修改相关指针
             * */
            if (elements > 0) {
                cur->elements = elements;
                cur->obj = obj;
                r->ridx++;
                r->task[r->ridx]->type = -1;
                r->task[r->ridx]->elements = -1;
                r->task[r->ridx]->idx = 0;
                r->task[r->ridx]->obj = NULL;
                r->task[r->ridx]->parent = cur;
                r->task[r->ridx]->privdata = r->privdata;
            } else {
                moveToNextTask(r);
            }
        }

        /* Set reply if this is the root object. */
        if (root) r->reply = obj;
        return REDIS_OK;
    }

    return REDIS_ERR;
}

/**
 * 处理某个task对象
 * @param r
 * @return
 */
static int processItem(redisReader *r) {
    // 获取此时正在处理的task
    redisReadTask *cur = r->task[r->ridx];
    char *p;

    /* check if we need to read type
     * 如果type为0代表还未解析类型
     * */
    if (cur->type < 0) {
        // 读取类型
        if ((p = readBytes(r,1)) != NULL) {
            switch (p[0]) {
            case '-':
                cur->type = REDIS_REPLY_ERROR;
                break;
            case '+':
                cur->type = REDIS_REPLY_STATUS;
                break;
            case ':':
                cur->type = REDIS_REPLY_INTEGER;
                break;
            case ',':
                cur->type = REDIS_REPLY_DOUBLE;
                break;
            case '_':
                cur->type = REDIS_REPLY_NIL;
                break;
            case '$':
                cur->type = REDIS_REPLY_STRING;
                break;
            case '*':
                cur->type = REDIS_REPLY_ARRAY;
                break;
            case '%':
                cur->type = REDIS_REPLY_MAP;
                break;
            case '~':
                cur->type = REDIS_REPLY_SET;
                break;
            case '#':
                cur->type = REDIS_REPLY_BOOL;
                break;
            case '=':
                cur->type = REDIS_REPLY_VERB;
                break;
            case '>':
                cur->type = REDIS_REPLY_PUSH;
                break;
            default:
                __redisReaderSetErrorProtocolByte(r,*p);
                return REDIS_ERR;
            }
        } else {
            /* could not consume 1 byte */
            return REDIS_ERR;
        }
    }

    /* process typed item
     * 不同的数据类型 走不同的处理逻辑
     * 比如 line 和bulk数据 都是直接读取reader内的数据 并设置到reply对象中 而 aggregate内 并没有读取数据只是创建等量的ele
     * */
    switch(cur->type) {
    case REDIS_REPLY_ERROR:
    case REDIS_REPLY_STATUS:
    case REDIS_REPLY_INTEGER:
    case REDIS_REPLY_DOUBLE:
    case REDIS_REPLY_NIL:
    case REDIS_REPLY_BOOL:
        // 读取单行数据 并解析
        return processLineItem(r);
    case REDIS_REPLY_STRING:
    case REDIS_REPLY_VERB:
        // 读取大块数据
        return processBulkItem(r);
    case REDIS_REPLY_ARRAY:
    case REDIS_REPLY_MAP:
    case REDIS_REPLY_SET:
    case REDIS_REPLY_PUSH:
        // 读取聚合数据
        return processAggregateItem(r);
    default:
        assert(NULL);
        return REDIS_ERR; /* Avoid warning. */
    }
}

/**
 * 生成reader对象 某些函数逻辑由外部传入
 * @param fn
 * @return
 */
redisReader *redisReaderCreateWithFunctions(redisReplyObjectFunctions *fn) {
    redisReader *r;

    r = hi_calloc(1,sizeof(redisReader));
    if (r == NULL)
        return NULL;

    // reader内部也有一个缓冲区
    r->buf = hi_sdsempty();
    if (r->buf == NULL)
        goto oom;

    // 在reader对象被创建时 就会默认分配task/tasks的内存
    r->task = hi_calloc(REDIS_READER_STACK_SIZE, sizeof(*r->task));
    if (r->task == NULL)
        goto oom;

    for (; r->tasks < REDIS_READER_STACK_SIZE; r->tasks++) {
        r->task[r->tasks] = hi_calloc(1, sizeof(**r->task));
        if (r->task[r->tasks] == NULL)
            goto oom;
    }

    r->fn = fn;
    r->maxbuf = REDIS_READER_MAX_BUF;
    r->maxelements = REDIS_READER_MAX_ARRAY_ELEMENTS;
    r->ridx = -1;

    return r;
oom:
    redisReaderFree(r);
    return NULL;
}

void redisReaderFree(redisReader *r) {
    if (r == NULL)
        return;

    if (r->reply != NULL && r->fn && r->fn->freeObject)
        r->fn->freeObject(r->reply);

    if (r->task) {
        /* We know r->task[i] is allocated if i < r->tasks */
        for (int i = 0; i < r->tasks; i++) {
            hi_free(r->task[i]);
        }

        hi_free(r->task);
    }

    hi_sdsfree(r->buf);
    hi_free(r);
}

/**
 * 将从socket中读取到的数据填充到reader内
 * @param r
 * @param buf
 * @param len
 * @return
 */
int redisReaderFeed(redisReader *r, const char *buf, size_t len) {
    hisds newbuf;

    /* Return early when this reader is in an erroneous state.
     * 如果此时reader已经发现了异常 就不需要处理了
     * */
    if (r->err)
        return REDIS_ERR;

    /* Copy the provided buffer. */
    if (buf != NULL && len >= 1) {
        /* Destroy internal buffer when it is empty and is quite large.
         * 当发现此时内部缓冲区太大时 缩容避免浪费
         * */
        if (r->len == 0 && r->maxbuf != 0 && hi_sdsavail(r->buf) > r->maxbuf) {
            hi_sdsfree(r->buf);
            r->buf = hi_sdsempty();
            if (r->buf == 0) goto oom;

            r->pos = 0;
        }

        // 这里只是做了数据拷贝
        newbuf = hi_sdscatlen(r->buf,buf,len);
        if (newbuf == NULL) goto oom;

        r->buf = newbuf;
        r->len = hi_sdslen(r->buf);
    }

    return REDIS_OK;
oom:
    __redisReaderSetErrorOOM(r);
    return REDIS_ERR;
}

/**
 * 解析reader内部存储的数据
 * @param r
 * @param reply
 * @return
 */
int redisReaderGetReply(redisReader *r, void **reply) {
    /* Default target pointer to NULL. */
    if (reply != NULL)
        *reply = NULL;

    /* Return early when this reader is in an erroneous state.
     * 如果已经出现了异常 直接返回
     * */
    if (r->err)
        return REDIS_ERR;

    /* When the buffer is empty, there will never be a reply.
     * 此时内部没有数据 不需要解析 同时也不会为reply赋值
     * */
    if (r->len == 0)
        return REDIS_OK;

    /* Set first item to process when the stack is empty.
     * 代表此时还未读取任何task 或者说task本身还未被初始化
     * */
    if (r->ridx == -1) {
        // 这里创建一个空的task对象 并将ridx变成0
        r->task[0]->type = -1;
        r->task[0]->elements = -1;
        r->task[0]->idx = -1;
        r->task[0]->obj = NULL;
        r->task[0]->parent = NULL;
        r->task[0]->privdata = r->privdata;
        r->ridx = 0;
    }

    /* Process items in reply. 挨个处理每个task 每次执行processItem时 最后会减少ridx 就会挨个处理每个task */
    while (r->ridx >= 0)
        if (processItem(r) != REDIS_OK)
            break;

    /* Return ASAP when an error occurred. 在处理过程中出现了异常 返回error */
    if (r->err)
        return REDIS_ERR;

    /* Discard part of the buffer when we've consumed at least 1k, to avoid
     * doing unnecessary calls to memmove() in sds.c.
     * 每当偏移量超过1024时 对reader内部的buf 进行一次清理工作
     * */
    if (r->pos >= 1024) {
        if (hi_sdsrange(r->buf,r->pos,-1) < 0) return REDIS_ERR;
        r->pos = 0;
        r->len = hi_sdslen(r->buf);
    }

    /* Emit a reply when there is one.
     * 当所有task 都被处理过后 ridx会变成-1
     * */
    if (r->ridx == -1) {
        // 在process过程中 当发现ridx变成0时 会设置r->reply
        if (reply != NULL) {
            *reply = r->reply;
            // 不需要使用reply指针时 释放对象
        } else if (r->reply != NULL && r->fn && r->fn->freeObject) {
            r->fn->freeObject(r->reply);
        }
        r->reply = NULL;
    }
    return REDIS_OK;
}
