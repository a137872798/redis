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
#include "alloc.h"
#include <stdlib.h>
#include <string.h>
#ifndef _MSC_VER
#include <strings.h>
#endif
#include <assert.h>
#include <ctype.h>
#include <errno.h>
#include "async.h"
#include "net.h"
#include "dict.c"
#include "sds.h"
#include "win32.h"

#include "async_private.h"

/* Forward declarations of hiredis.c functions */
int __redisAppendCommand(redisContext *c, const char *cmd, size_t len);
void __redisSetError(redisContext *c, int type, const char *str);

/* Functions managing dictionary of callbacks for pub/sub. */
static unsigned int callbackHash(const void *key) {
    return dictGenHashFunction((const unsigned char *)key,
                               hi_sdslen((const hisds)key));
}

static void *callbackValDup(void *privdata, const void *src) {
    ((void) privdata);
    redisCallback *dup;

    dup = hi_malloc(sizeof(*dup));
    if (dup == NULL)
        return NULL;

    memcpy(dup,src,sizeof(*dup));
    return dup;
}

static int callbackKeyCompare(void *privdata, const void *key1, const void *key2) {
    int l1, l2;
    ((void) privdata);

    l1 = hi_sdslen((const hisds)key1);
    l2 = hi_sdslen((const hisds)key2);
    if (l1 != l2) return 0;
    return memcmp(key1,key2,l1) == 0;
}

static void callbackKeyDestructor(void *privdata, void *key) {
    ((void) privdata);
    hi_sdsfree((hisds)key);
}

static void callbackValDestructor(void *privdata, void *val) {
    ((void) privdata);
    hi_free(val);
}

static dictType callbackDict = {
    callbackHash,
    NULL,
    callbackValDup,
    callbackKeyCompare,
    callbackKeyDestructor,
    callbackValDestructor
};

/**
 * 将一个普通的上下文对象包装成异步上下文
 * @param c
 * @return
 */
static redisAsyncContext *redisAsyncInitialize(redisContext *c) {
    redisAsyncContext *ac;

    // 同时需要一个channelDict 和一个 patternDict
    dict *channels = NULL, *patterns = NULL;

    channels = dictCreate(&callbackDict,NULL);
    if (channels == NULL)
        goto oom;

    patterns = dictCreate(&callbackDict,NULL);
    if (patterns == NULL)
        goto oom;

    ac = hi_realloc(c,sizeof(redisAsyncContext));
    if (ac == NULL)
        goto oom;

    // 实际上ac->c 就是c 因为是在c的基础上申请更多的内存
    c = &(ac->c);

    /* The regular connect functions will always set the flag REDIS_CONNECTED.
     * For the async API, we want to wait until the first write event is
     * received up before setting this flag, so reset it here.
     * 因为本次采用的是异步连接 可以看到在基于异步模式的连接下 会先设置REDIS_CONNECTED标记 此时就要清理掉该标记
     * */
    c->flags &= ~REDIS_CONNECTED;

    // 这里都是一些属性初始化
    ac->err = 0;
    ac->errstr = NULL;
    ac->data = NULL;
    ac->dataCleanup = NULL;

    ac->ev.data = NULL;
    ac->ev.addRead = NULL;
    ac->ev.delRead = NULL;
    ac->ev.addWrite = NULL;
    ac->ev.delWrite = NULL;
    ac->ev.cleanup = NULL;
    ac->ev.scheduleTimer = NULL;

    ac->onConnect = NULL;
    ac->onDisconnect = NULL;

    ac->replies.head = NULL;
    ac->replies.tail = NULL;
    ac->sub.invalid.head = NULL;
    ac->sub.invalid.tail = NULL;
    ac->sub.channels = channels;
    ac->sub.patterns = patterns;

    return ac;
oom:
    if (channels) dictRelease(channels);
    if (patterns) dictRelease(patterns);
    return NULL;
}

/* We want the error field to be accessible directly instead of requiring
 * an indirection to the redisContext struct. */
static void __redisAsyncCopyError(redisAsyncContext *ac) {
    if (!ac)
        return;

    redisContext *c = &(ac->c);
    ac->err = c->err;
    ac->errstr = c->errstr;
}

/**
 * 基于options 建立异步连接  options中包含了连接方式 options.endpoint中包含了以及ip/port
 * @param options
 * @return
 */
redisAsyncContext *redisAsyncConnectWithOptions(const redisOptions *options) {
    redisOptions myOptions = *options;
    redisContext *c;
    redisAsyncContext *ac;

    /* Clear any erroneously set sync callback and flag that we don't want to
     * use freeReplyObject by default.
     * 设置成no_push
     * */
    myOptions.push_cb = NULL;

    // 设置options
    myOptions.options |= REDIS_OPT_NO_PUSH_AUTOFREE;
    // 注意本次发起的是非阻塞连接
    myOptions.options |= REDIS_OPT_NONBLOCK;
    // 生成上下文对象  这里同时还基于该context发起连接
    c = redisConnectWithOptions(&myOptions);
    if (c == NULL) {
        return NULL;
    }

    // 生成一个异步上下文 内部包含context  此时ac还没有设置其他属性
    ac = redisAsyncInitialize(c);
    if (ac == NULL) {
        redisFree(c);
        return NULL;
    }

    /* Set any configured async push handler
     * 设置ac->push_cb 先假设options中没有携带回调信息
     * */
    redisAsyncSetPushCallback(ac, myOptions.async_push_cb);

    __redisAsyncCopyError(ac);
    return ac;
}

redisAsyncContext *redisAsyncConnect(const char *ip, int port) {
    redisOptions options = {0};
    REDIS_OPTIONS_SET_TCP(&options, ip, port);
    return redisAsyncConnectWithOptions(&options);
}

/**
 * 同步连接到该地址 并返回一个asyncContext对象
 * @param ip
 * @param port
 * @param source_addr
 * @return
 */
redisAsyncContext *redisAsyncConnectBind(const char *ip, int port,
                                         const char *source_addr) {
    redisOptions options = {0};
    // 填充options的 ip/port 以及连接方式
    REDIS_OPTIONS_SET_TCP(&options, ip, port);
    // 本机绑定的地址
    options.endpoint.tcp.source_addr = source_addr;
    // 基于options创建异步连接
    return redisAsyncConnectWithOptions(&options);
}

redisAsyncContext *redisAsyncConnectBindWithReuse(const char *ip, int port,
                                                  const char *source_addr) {
    redisOptions options = {0};
    REDIS_OPTIONS_SET_TCP(&options, ip, port);
    options.options |= REDIS_OPT_REUSEADDR;
    options.endpoint.tcp.source_addr = source_addr;
    return redisAsyncConnectWithOptions(&options);
}

redisAsyncContext *redisAsyncConnectUnix(const char *path) {
    redisOptions options = {0};
    REDIS_OPTIONS_SET_UNIX(&options, path);
    return redisAsyncConnectWithOptions(&options);
}

/**
 * 将连接的回调函数设置到asyncContext上
 * @param ac
 * @param fn
 * @return
 */
int redisAsyncSetConnectCallback(redisAsyncContext *ac, redisConnectCallback *fn) {
    if (ac->onConnect == NULL) {
        ac->onConnect = fn;

        /* The common way to detect an established connection is to wait for
         * the first write event to be fired. This assumes the related event
         * library functions are already set.
         * 这里触发了 addWrite函数
         * */
        _EL_ADD_WRITE(ac);
        return REDIS_OK;
    }
    return REDIS_ERR;
}

/**
 * 设置连接断开时的钩子
 * @param ac
 * @param fn
 * @return
 */
int redisAsyncSetDisconnectCallback(redisAsyncContext *ac, redisDisconnectCallback *fn) {
    if (ac->onDisconnect == NULL) {
        ac->onDisconnect = fn;
        return REDIS_OK;
    }
    return REDIS_ERR;
}

/* Helper functions to push/shift callbacks
 * 将某个回调对象追加到回调列表
 * */
static int __redisPushCallback(redisCallbackList *list, redisCallback *source) {
    redisCallback *cb;

    /* Copy callback from stack to heap */
    cb = hi_malloc(sizeof(*cb));
    if (cb == NULL)
        return REDIS_ERR_OOM;

    if (source != NULL) {
        memcpy(cb,source,sizeof(*cb));
        cb->next = NULL;
    }

    /* Store callback in list
     * 链表操作
     * */
    if (list->head == NULL)
        list->head = cb;
    if (list->tail != NULL)
        list->tail->next = cb;
    list->tail = cb;
    return REDIS_OK;
}

/**
 * 取出第一个callback对象 并赋值到target上
 * @param list
 * @param target
 * @return
 */
static int __redisShiftCallback(redisCallbackList *list, redisCallback *target) {
    redisCallback *cb = list->head;
    if (cb != NULL) {
        list->head = cb->next;
        if (cb == list->tail)
            list->tail = NULL;

        /* Copy callback from heap to stack */
        if (target != NULL)
            memcpy(target,cb,sizeof(*cb));
        hi_free(cb);
        return REDIS_OK;
    }
    return REDIS_ERR;
}

/**
 * 使用当前参数执行某个回调函数
 * @param ac
 * @param cb
 * @param reply
 */
static void __redisRunCallback(redisAsyncContext *ac, redisCallback *cb, redisReply *reply) {
    redisContext *c = &(ac->c);
    if (cb->fn != NULL) {
        c->flags |= REDIS_IN_CALLBACK;
        cb->fn(ac,reply,cb->privdata);
        c->flags &= ~REDIS_IN_CALLBACK;
    }
}

static void __redisRunPushCallback(redisAsyncContext *ac, redisReply *reply) {
    if (ac->push_cb != NULL) {
        ac->c.flags |= REDIS_IN_CALLBACK;
        ac->push_cb(ac, reply);
        ac->c.flags &= ~REDIS_IN_CALLBACK;
    }
}

/* Helper function to free the context.
 * 对内部一些成员进行回收 同时会执行一些回调函数
 * */
static void __redisAsyncFree(redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    redisCallback cb;
    dictIterator *it;
    dictEntry *de;

    /* Execute pending callbacks with NULL reply. 挨个获取每个callback 并执行 */
    while (__redisShiftCallback(&ac->replies,&cb) == REDIS_OK)
        __redisRunCallback(ac,&cb,NULL);

    /* Execute callbacks for invalid commands */
    while (__redisShiftCallback(&ac->sub.invalid,&cb) == REDIS_OK)
        __redisRunCallback(ac,&cb,NULL);

    // sub.channels,sub.patterns 内部存储的是什么
    /* Run subscription callbacks with NULL reply */
    if (ac->sub.channels) {
        it = dictGetIterator(ac->sub.channels);
        if (it != NULL) {
            while ((de = dictNext(it)) != NULL)
                __redisRunCallback(ac,dictGetEntryVal(de),NULL);
            dictReleaseIterator(it);
        }

        dictRelease(ac->sub.channels);
    }

    if (ac->sub.patterns) {
        it = dictGetIterator(ac->sub.patterns);
        if (it != NULL) {
            while ((de = dictNext(it)) != NULL)
                __redisRunCallback(ac,dictGetEntryVal(de),NULL);
            dictReleaseIterator(it);
        }

        dictRelease(ac->sub.patterns);
    }

    /* Signal event lib to clean up
     * 执行ac.cleanup
     * */
    _EL_CLEANUP(ac);

    /* Execute disconnect callback. When redisAsyncFree() initiated destroying
     * this context, the status will always be REDIS_OK.
     * 如果设置了断开连接的函数 在此时执行
     * */
    if (ac->onDisconnect && (c->flags & REDIS_CONNECTED)) {
        if (c->flags & REDIS_FREEING) {
            ac->onDisconnect(ac,REDIS_OK);
        } else {
            ac->onDisconnect(ac,(ac->err == 0) ? REDIS_OK : REDIS_ERR);
        }
    }

    // 执行清理数据的函数
    if (ac->dataCleanup) {
        ac->dataCleanup(ac->data);
    }

    /* Cleanup self */
    redisFree(c);
}

/* Free the async context. When this function is called from a callback,
 * control needs to be returned to redisProcessCallbacks() before actual
 * free'ing. To do so, a flag is set on the context which is picked up by
 * redisProcessCallbacks(). Otherwise, the context is immediately free'd.
 * 释放某个异步上下文对象
 * */
void redisAsyncFree(redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    c->flags |= REDIS_FREEING;
    if (!(c->flags & REDIS_IN_CALLBACK))
        __redisAsyncFree(ac);
}

/* Helper function to make the disconnect happen and clean up.
 * 代表某次异步连接失败
 * */
void __redisAsyncDisconnect(redisAsyncContext *ac) {
    redisContext *c = &(ac->c);

    /* Make sure error is accessible if there is any
     * 将context的异常信息转移到ac上
     * */
    __redisAsyncCopyError(ac);

    if (ac->err == 0) {
        /* For clean disconnects, there should be no pending callbacks.
         * 代表是手动断开连接 要清理所有回调函数
         * */
        int ret = __redisShiftCallback(&ac->replies,NULL);
        assert(ret == REDIS_ERR);
    } else {
        /* Disconnection is caused by an error, make sure that pending
         * callbacks cannot call new commands.
         * 异常情况导致的连接断开 设置标记位
         * */
        c->flags |= REDIS_DISCONNECTING;
    }

    /* cleanup event library on disconnect.
     * this is safe to call multiple times
     * 如果有设置清理函数 要进行清理
     * */
    _EL_CLEANUP(ac);

    /* For non-clean disconnects, __redisAsyncFree() will execute pending
     * callbacks with a NULL-reply.
     * 执行回收函数
     * */
    if (!(c->flags & REDIS_NO_AUTO_FREE)) {
      __redisAsyncFree(ac);
    }
}

/* Tries to do a clean disconnect from Redis, meaning it stops new commands
 * from being issued, but tries to flush the output buffer and execute
 * callbacks for all remaining replies. When this function is called from a
 * callback, there might be more replies and we can safely defer disconnecting
 * to redisProcessCallbacks(). Otherwise, we can only disconnect immediately
 * when there are no pending callbacks. */
void redisAsyncDisconnect(redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    c->flags |= REDIS_DISCONNECTING;

    /** unset the auto-free flag here, because disconnect undoes this */
    c->flags &= ~REDIS_NO_AUTO_FREE;
    if (!(c->flags & REDIS_IN_CALLBACK) && ac->replies.head == NULL)
        __redisAsyncDisconnect(ac);
}

static int __redisGetSubscribeCallback(redisAsyncContext *ac, redisReply *reply, redisCallback *dstcb) {
    redisContext *c = &(ac->c);
    dict *callbacks;
    redisCallback *cb;
    dictEntry *de;
    int pvariant;
    char *stype;
    hisds sname;

    /* Custom reply functions are not supported for pub/sub. This will fail
     * very hard when they are used... */
    if (reply->type == REDIS_REPLY_ARRAY || reply->type == REDIS_REPLY_PUSH) {
        assert(reply->elements >= 2);
        assert(reply->element[0]->type == REDIS_REPLY_STRING);
        stype = reply->element[0]->str;
        pvariant = (tolower(stype[0]) == 'p') ? 1 : 0;

        if (pvariant)
            callbacks = ac->sub.patterns;
        else
            callbacks = ac->sub.channels;

        /* Locate the right callback */
        assert(reply->element[1]->type == REDIS_REPLY_STRING);
        sname = hi_sdsnewlen(reply->element[1]->str,reply->element[1]->len);
        if (sname == NULL)
            goto oom;

        de = dictFind(callbacks,sname);
        if (de != NULL) {
            cb = dictGetEntryVal(de);

            /* If this is an subscribe reply decrease pending counter. */
            if (strcasecmp(stype+pvariant,"subscribe") == 0) {
                cb->pending_subs -= 1;
            }

            memcpy(dstcb,cb,sizeof(*dstcb));

            /* If this is an unsubscribe message, remove it. */
            if (strcasecmp(stype+pvariant,"unsubscribe") == 0) {
                if (cb->pending_subs == 0)
                    dictDelete(callbacks,sname);

                /* If this was the last unsubscribe message, revert to
                 * non-subscribe mode. */
                assert(reply->element[2]->type == REDIS_REPLY_INTEGER);

                /* Unset subscribed flag only when no pipelined pending subscribe. */
                if (reply->element[2]->integer == 0
                    && dictSize(ac->sub.channels) == 0
                    && dictSize(ac->sub.patterns) == 0)
                    c->flags &= ~REDIS_SUBSCRIBED;
            }
        }
        hi_sdsfree(sname);
    } else {
        /* Shift callback for invalid commands. */
        __redisShiftCallback(&ac->sub.invalid,dstcb);
    }
    return REDIS_OK;
oom:
    __redisSetError(&(ac->c), REDIS_ERR_OOM, "Out of memory");
    return REDIS_ERR;
}

#define redisIsSpontaneousPushReply(r) \
    (redisIsPushReply(r) && !redisIsSubscribeReply(r))

static int redisIsSubscribeReply(redisReply *reply) {
    char *str;
    size_t len, off;

    /* We will always have at least one string with the subscribe/message type */
    if (reply->elements < 1 || reply->element[0]->type != REDIS_REPLY_STRING ||
        reply->element[0]->len < sizeof("message") - 1)
    {
        return 0;
    }

    /* Get the string/len moving past 'p' if needed */
    off = tolower(reply->element[0]->str[0]) == 'p';
    str = reply->element[0]->str + off;
    len = reply->element[0]->len - off;

    return !strncasecmp(str, "subscribe", len) ||
           !strncasecmp(str, "message", len);

}

/**
 * 使用回调对象处理reader内存储的数据
 * @param ac
 */
void redisProcessCallbacks(redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    // 这里创建了一个空的回调对象
    redisCallback cb = {NULL, NULL, 0, NULL};
    void *reply = NULL;
    int status;

    // 解析reader内部的数据   redisGetReply 方法本身应该会轮询消耗所有的task 直到ridx变成-1 所以这里为什么要用while
    while((status = redisGetReply(c,&reply)) == REDIS_OK) {
        // 只有当本次处理的是最后一个task时 才会设置reply指针 否则该指针为null
        if (reply == NULL) {
            /* When the connection is being disconnected and there are
             * no more replies, this is the cue to really disconnect.
             * TODO 先忽略连接断开的情况
             * */
            if (c->flags & REDIS_DISCONNECTING && hi_sdslen(c->obuf) == 0
                && ac->replies.head == NULL) {
                __redisAsyncDisconnect(ac);
                return;
            }

            /* If monitor mode, repush callback
             * TODO 先忽略monitor
             * */
            if(c->flags & REDIS_MONITORING) {
                __redisPushCallback(&ac->replies,&cb);
            }

            /* When the connection is not being disconnected, simply stop
             * trying to get replies and wait for the next loop tick. */
            break;
        }

        /* Send any non-subscribe related PUSH messages to our PUSH handler
         * while allowing subscribe related PUSH messages to pass through.
         * This allows existing code to be backward compatible and work in
         * either RESP2 or RESP3 mode.
         * 假设此时已经处理到最后一个task了 这样会返回reply对象
         * 检查reply->type 是否是push类型 TODO 先忽略push类型
         * */
        if (redisIsSpontaneousPushReply(reply)) {
            __redisRunPushCallback(ac, reply);
            c->reader->fn->freeObject(reply);
            continue;
        }

        /* Even if the context is subscribed, pending regular
         * callbacks will get a reply before pub/sub messages arrive.
         * 代表没有回调对象
         * */
        if (__redisShiftCallback(&ac->replies,&cb) != REDIS_OK) {
            /*
             * A spontaneous reply in a not-subscribed context can be the error
             * reply that is sent when a new connection exceeds the maximum
             * number of allowed connections on the server side.
             *
             * This is seen as an error instead of a regular reply because the
             * server closes the connection after sending it.
             *
             * To prevent the error from being overwritten by an EOF error the
             * connection is closed here. See issue #43.
             *
             * Another possibility is that the server is loading its dataset.
             * In this case we also want to close the connection, and have the
             * user wait until the server is ready to take our request.
             * 如果本次处理出现了异常 断开连接   reply->type 是task->type 带过去的
             */
            if (((redisReply*)reply)->type == REDIS_REPLY_ERROR) {
                c->err = REDIS_ERR_OTHER;
                snprintf(c->errstr,sizeof(c->errstr),"%s",((redisReply*)reply)->str);
                c->reader->fn->freeObject(reply);
                __redisAsyncDisconnect(ac);
                return;
            }
            /* No more regular callbacks and no errors, the context *must* be subscribed or monitoring.
             * 如果不包含回调对象的情况 必然是订阅或者监控模式
             * */
            assert((c->flags & REDIS_SUBSCRIBED || c->flags & REDIS_MONITORING));
            // TODO
            if(c->flags & REDIS_SUBSCRIBED)
                __redisGetSubscribeCallback(ac,reply,&cb);
        }

        // 这里找到了回调对象 准备执行  这里的回调对象逻辑应该是什么???
        if (cb.fn != NULL) {
            __redisRunCallback(ac,&cb,reply);
            // 执行完毕后释放reply
            c->reader->fn->freeObject(reply);

            /* Proceed with free'ing when redisAsyncFree() was called.
             * 代表需要释放ac
             * */
            if (c->flags & REDIS_FREEING) {
                __redisAsyncFree(ac);
                return;
            }
        } else {
            /* No callback for this reply. This can either be a NULL callback,
             * or there were no callbacks to begin with. Either way, don't
             * abort with an error, but simply ignore it because the client
             * doesn't know what the server will spit out over the wire. */
            c->reader->fn->freeObject(reply);
        }
    }

    /* Disconnect when there was an error reading the reply */
    if (status != REDIS_OK)
        __redisAsyncDisconnect(ac);
}

/**
 * 当处理连接失败时 触发回调函数
 * @param ac
 */
static void __redisAsyncHandleConnectFailure(redisAsyncContext *ac) {
    if (ac->onConnect) ac->onConnect(ac, REDIS_ERR);
    __redisAsyncDisconnect(ac);
}

/* Internal helper function to detect socket status the first time a read or
 * write event fires. When connecting was not successful, the connect callback
 * is called with a REDIS_ERR status and the context is free'd.
 * 轮询el发现连接事件准备完成
 * */
static int __redisAsyncHandleConnect(redisAsyncContext *ac) {
    int completed = 0;
    redisContext *c = &(ac->c);

    // 连接失败
    if (redisCheckConnectDone(c, &completed) == REDIS_ERR) {
        /* Error! */
        redisCheckSocketError(c);
        __redisAsyncHandleConnectFailure(ac);
        return REDIS_ERR;
    } else if (completed == 1) {
        /* connected! */
        if (c->connection_type == REDIS_CONN_TCP &&
            redisSetTcpNoDelay(c) == REDIS_ERR) {
            __redisAsyncHandleConnectFailure(ac);
            return REDIS_ERR;
        }

        // 本次连接成功 修改context的标记位 同时触发onConnect函数
        if (ac->onConnect) ac->onConnect(ac, REDIS_OK);
        c->flags |= REDIS_CONNECTED;
        return REDIS_OK;
    } else {
        return REDIS_OK;
    }
}

/**
 * 对应 c->funcs->async_read(ac)
 * @param ac
 */
void redisAsyncRead(redisAsyncContext *ac) {
    redisContext *c = &(ac->c);

    // 将socket缓冲区的数据转移到reader中
    if (redisBufferRead(c) == REDIS_ERR) {
        __redisAsyncDisconnect(ac);
    } else {
        /* Always re-schedule reads
         * 只要数据读取成功就可以继续监听reader事件
         * */
        _EL_ADD_READ(ac);
        // 使用callback对象处理此时reader内部的数据
        redisProcessCallbacks(ac);
    }
}

/* This function should be called when the socket is readable.
 * It processes all replies that can be read and executes their callbacks.
 * 代表el发现读事件准备完成了
 */
void redisAsyncHandleRead(redisAsyncContext *ac) {
    redisContext *c = &(ac->c);

    // 如果连接还未完成 先处理连接事件
    if (!(c->flags & REDIS_CONNECTED)) {
        /* Abort connect was not successful. */
        if (__redisAsyncHandleConnect(ac) != REDIS_OK)
            return;
        /* Try again later when the context is still not connected. */
        if (!(c->flags & REDIS_CONNECTED))
            return;
    }

    c->funcs->async_read(ac);
}

/**
 * context中默认的 async_write函数  将context内部的文件句柄注册到el后 当写入事件准备完成就会触发该方法
 * @param ac
 */
void redisAsyncWrite(redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    int done = 0;

    // 将context缓冲区内的数据写入到socket缓冲区中
    if (redisBufferWrite(c,&done) == REDIS_ERR) {
        __redisAsyncDisconnect(ac);
    } else {
        /* Continue writing when not done, stop writing otherwise
         * 代表还有剩余的数据
         * */
        if (!done)
            // 触发addWrite  代表还需要继续在el上轮询该句柄 之后又会回到redisAsyncWrite这个方法
            _EL_ADD_WRITE(ac);
        else
            // 触发delWrite  就是从el上注销
            _EL_DEL_WRITE(ac);

        /* Always schedule reads after writes
         * 触发addRead  也就是监听句柄对应的读事件
         * */
        _EL_ADD_READ(ac);
    }
}

/**
 * 当写入事件准备完成时 触发函数
 * @param ac
 */
void redisAsyncHandleWrite(redisAsyncContext *ac) {
    redisContext *c = &(ac->c);

    // 代表此时连接未完成 先执行连接工作
    if (!(c->flags & REDIS_CONNECTED)) {
        /* Abort connect was not successful. */
        if (__redisAsyncHandleConnect(ac) != REDIS_OK)
            return;
        /* Try again later when the context is still not connected. */
        if (!(c->flags & REDIS_CONNECTED))
            return;
    }

    // 执行异步写入工作
    c->funcs->async_write(ac);
}

void redisAsyncHandleTimeout(redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    redisCallback cb;

    if ((c->flags & REDIS_CONNECTED) && ac->replies.head == NULL) {
        /* Nothing to do - just an idle timeout */
        return;
    }

    if (!c->err) {
        __redisSetError(c, REDIS_ERR_TIMEOUT, "Timeout");
    }

    if (!(c->flags & REDIS_CONNECTED) && ac->onConnect) {
        ac->onConnect(ac, REDIS_ERR);
    }

    while (__redisShiftCallback(&ac->replies, &cb) == REDIS_OK) {
        __redisRunCallback(ac, &cb, NULL);
    }

    /**
     * TODO: Don't automatically sever the connection,
     * rather, allow to ignore <x> responses before the queue is clear
     */
    __redisAsyncDisconnect(ac);
}

/* Sets a pointer to the first argument and its length starting at p. Returns
 * the number of bytes to skip to get to the following argument.
 * 找到下一个参数的起始位置
 * */
static const char *nextArgument(const char *start, const char **str, size_t *len) {
    const char *p = start;

    // 如果存在$ 代表有一个描述参数长度的信息
    if (p[0] != '$') {
        p = strchr(p,'$');
        if (p == NULL) return NULL;
    }

    // 获取长度信息
    *len = (int)strtol(p+1,NULL,10);
    p = strchr(p,'\r');
    assert(p);
    // 这里会更新字符串的起始位置
    *str = p+2;
    return p+2+(*len)+2;
}

/* Helper function for the redisAsyncCommand* family of functions. Writes a
 * formatted command to the output buffer and registers the provided callback
 * function with the context.
 * 异步执行某个command
 * @param cmd 本次执行命令的参数流
 * @param len cmd的长度
 * */
static int __redisAsyncCommand(redisAsyncContext *ac, redisCallbackFn *fn, void *privdata, const char *cmd, size_t len) {
    redisContext *c = &(ac->c);
    redisCallback cb;
    struct dict *cbdict;
    dictEntry *de;
    redisCallback *existcb;
    int pvariant, hasnext;
    const char *cstr, *astr;
    size_t clen, alen;
    const char *p;
    hisds sname;
    int ret;

    /* Don't accept new commands when the connection is about to be closed.
     * 此时该连接已经断开 或者已经释放 无法执行任务
     * */
    if (c->flags & (REDIS_DISCONNECTING | REDIS_FREEING)) return REDIS_ERR;

    /* Setup callback
     * 这里生成一个回调对象专门用来处理接收到的reply对象
     * */
    cb.fn = fn;

    // 因为存在连接被共用的情况 通过设置privdata 来判断这个回调是针对哪个实例设置的
    cb.privdata = privdata;
    cb.pending_subs = 1;

    /* Find out which command will be appended.
     * 获取下一个参数的起始位置
     * */
    p = nextArgument(cmd,&cstr,&clen);
    assert(p != NULL);
    // 判断是否存在下一个参数
    hasnext = (p[0] == '$');
    // p开头就代表本次传入的是一个正则表达式
    pvariant = (tolower(cstr[0]) == 'p') ? 1 : 0;
    cstr += pvariant;
    clen -= pvariant;

    // sentinel会与其他master/slave建立一个专门用于订阅发布的连接 会进入这个分支
    if (hasnext && strncasecmp(cstr,"subscribe\r\n",11) == 0) {
        c->flags |= REDIS_SUBSCRIBED;

        /* Add every channel/pattern to the list of subscription callbacks. */
        while ((p = nextArgument(p,&astr,&alen)) != NULL) {
            sname = hi_sdsnewlen(astr,alen);
            if (sname == NULL)
                goto oom;

            // 根据是否是正则表达式 将channel添加到不同的容器
            if (pvariant)
                cbdict = ac->sub.patterns;
            else
                cbdict = ac->sub.channels;

            de = dictFind(cbdict,sname);

            if (de != NULL) {
                existcb = dictGetEntryVal(de);
                cb.pending_subs = existcb->pending_subs + 1;
            }

            ret = dictReplace(cbdict,sname,&cb);

            if (ret == 0) hi_sdsfree(sname);
        }
    } else if (strncasecmp(cstr,"unsubscribe\r\n",13) == 0) {
        /* It is only useful to call (P)UNSUBSCRIBE when the context is
         * subscribed to one or more channels or patterns. */
        if (!(c->flags & REDIS_SUBSCRIBED)) return REDIS_ERR;

        /* (P)UNSUBSCRIBE does not have its own response: every channel or
         * pattern that is unsubscribed will receive a message. This means we
         * should not append a callback function for this command. */
     } else if(strncasecmp(cstr,"monitor\r\n",9) == 0) {
         /* Set monitor flag and push callback */
         c->flags |= REDIS_MONITORING;
         __redisPushCallback(&ac->replies,&cb);

         // 那些特殊的command先忽略 假设本次执行的是一个auth命令 (哨兵连接到其他节点前可能需要验证)
    } else {
        // 当本次连接是用于订阅发布的时候 会将回调对象设置到一个特殊的列表中
        if (c->flags & REDIS_SUBSCRIBED)
            /* This will likely result in an error reply, but it needs to be
             * received and passed to the callback. */
            __redisPushCallback(&ac->sub.invalid,&cb);
        else
            // 处理正常命令时 就将回调对象加入到列表中 等待对端的reply
            __redisPushCallback(&ac->replies,&cb);
    }

    // 将数据写入到buf中
    __redisAppendCommand(c,cmd,len);

    /* Always schedule a write when the write buffer is non-empty
     * 在el上注册写事件
     * */
    _EL_ADD_WRITE(ac);

    return REDIS_OK;
oom:
    __redisSetError(&(ac->c), REDIS_ERR_OOM, "Out of memory");
    return REDIS_ERR;
}

/**
 * 采用异步方式执行某个command
 * @param ac
 * @param fn
 * @param privdata 一般就是redisInstance
 * @param format
 * @param ap 第一个参数是本次要执行的command
 * @return
 */
int redisvAsyncCommand(redisAsyncContext *ac, redisCallbackFn *fn, void *privdata, const char *format, va_list ap) {
    char *cmd;
    int len;
    int status;

    // 原本要传输的数据流可能包含了一些占位符 这里通过填充ap的数据 生成完整字符信息
    len = redisvFormatCommand(&cmd,format,ap);

    /* We don't want to pass -1 or -2 to future functions as a length. */
    if (len < 0)
        return REDIS_ERR;

    // 这里开始执行command
    status = __redisAsyncCommand(ac,fn,privdata,cmd,len);
    hi_free(cmd);
    return status;
}

/**
 * 通过异步的方式执行一个command 因为此时连接可能还未完成  (这里允许提前指定要执行的命令)
 * @param ac
 * @param fn 处理收到的reply的函数
 * @param privdata sentinel中代表每个节点的实例对象
 * @param format
 * @param ... 一般来说第一个参数是真正的command
 * @return
 */
int redisAsyncCommand(redisAsyncContext *ac, redisCallbackFn *fn, void *privdata, const char *format, ...) {
    va_list ap;
    int status;
    va_start(ap,format);
    status = redisvAsyncCommand(ac,fn,privdata,format,ap);
    va_end(ap);
    return status;
}

int redisAsyncCommandArgv(redisAsyncContext *ac, redisCallbackFn *fn, void *privdata, int argc, const char **argv, const size_t *argvlen) {
    hisds cmd;
    int len;
    int status;
    len = redisFormatSdsCommandArgv(&cmd,argc,argv,argvlen);
    if (len < 0)
        return REDIS_ERR;
    status = __redisAsyncCommand(ac,fn,privdata,cmd,len);
    hi_sdsfree(cmd);
    return status;
}

int redisAsyncFormattedCommand(redisAsyncContext *ac, redisCallbackFn *fn, void *privdata, const char *cmd, size_t len) {
    int status = __redisAsyncCommand(ac,fn,privdata,cmd,len);
    return status;
}

/**
 * 为某个asyncContext 设置回调对象
 * @param ac
 * @param fn
 * @return
 */
redisAsyncPushFn *redisAsyncSetPushCallback(redisAsyncContext *ac, redisAsyncPushFn *fn) {
    redisAsyncPushFn *old = ac->push_cb;
    ac->push_cb = fn;
    return old;
}

int redisAsyncSetTimeout(redisAsyncContext *ac, struct timeval tv) {
    if (!ac->c.command_timeout) {
        ac->c.command_timeout = hi_calloc(1, sizeof(tv));
        if (ac->c.command_timeout == NULL) {
            __redisSetError(&ac->c, REDIS_ERR_OOM, "Out of memory");
            __redisAsyncCopyError(ac);
            return REDIS_ERR;
        }
    }

    if (tv.tv_sec != ac->c.command_timeout->tv_sec ||
        tv.tv_usec != ac->c.command_timeout->tv_usec)
    {
        *ac->c.command_timeout = tv;
    }

    return REDIS_OK;
}
