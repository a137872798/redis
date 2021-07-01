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
 * 将首个回调对象转移到 target上 如果target为NULL 就代表直接丢弃第一个回调
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

    /* Execute pending callbacks with NULL reply. 使用NULL触发每个回调 */
    while (__redisShiftCallback(&ac->replies,&cb) == REDIS_OK)
        __redisRunCallback(ac,&cb,NULL);

    /* Execute callbacks for invalid commands */
    while (__redisShiftCallback(&ac->sub.invalid,&cb) == REDIS_OK)
        __redisRunCallback(ac,&cb,NULL);

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
 * 异步关闭连接
 * */
void __redisAsyncDisconnect(redisAsyncContext *ac) {
    redisContext *c = &(ac->c);

    /* Make sure error is accessible if there is any
     * 将context的异常信息转移到ac上
     * */
    __redisAsyncCopyError(ac);

    if (ac->err == 0) {
        /* For clean disconnects, there should be no pending callbacks.
         * 丢弃第一个callback
         * */
        int ret = __redisShiftCallback(&ac->replies,NULL);
        assert(ret == REDIS_ERR);
    } else {
        /* Disconnection is caused by an error, make sure that pending
         * callbacks cannot call new commands.
         * 这里只是打上标记 还没有真正清理
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

/**
 * 从特殊容器中获取回调对象
 */
static int __redisGetSubscribeCallback(redisAsyncContext *ac, redisReply *reply, redisCallback *dstcb) {
    redisContext *c = &(ac->c);
    dict *callbacks;
    redisCallback *cb;
    dictEntry *de;
    int pvariant;
    char *stype;
    hisds sname;

    /* Custom reply functions are not supported for pub/sub. This will fail
     * very hard when they are used...
     * 当注册的是订阅发布回调时 callback需要能重复使用
     * */
    if (reply->type == REDIS_REPLY_ARRAY || reply->type == REDIS_REPLY_PUSH) {
        assert(reply->elements >= 2);
        assert(reply->element[0]->type == REDIS_REPLY_STRING);
        stype = reply->element[0]->str;
        // 第一个元素标记本次是匹配 channel/pattern
        pvariant = (tolower(stype[0]) == 'p') ? 1 : 0;

        // 找到对应的回调列表
        if (pvariant)
            callbacks = ac->sub.patterns;
        else
            callbacks = ac->sub.channels;

        /* Locate the right callback */
        assert(reply->element[1]->type == REDIS_REPLY_STRING);
        // 对应channel名称
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

            // 这里会取出callback对象 也就可以重复使用
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
        /* Shift callback for invalid commands.
         * 如果对该订阅管道发送了普通命令 那么对应的回调会设置在sub.invalid中 注意也是单次使用 一旦处理完某个响应这个callback就可以丢掉了
         * */
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
 * 开始处理该从该context对应的link接收到的数据 注意之前的数据还没有做粘拆包处理
 * @param ac
 */
void redisProcessCallbacks(redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    // 这里创建了一个空的回调对象
    redisCallback cb = {NULL, NULL, 0, NULL};
    void *reply = NULL;
    int status;

    // 每调用一次redisGetReply 会生成一个完整的数据包 这里在挨个处理
    while((status = redisGetReply(c,&reply)) == REDIS_OK) {
        // 本次数据不完整
        if (reply == NULL) {
            /* When the connection is being disconnected and there are
             * no more replies, this is the cue to really disconnect.
             * 此时该连接已经被标记成待关闭 并且此时没有待处理数据了 就可以真正关闭连接
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
         * TODO 先忽略push类型
         * */
        if (redisIsSpontaneousPushReply(reply)) {
            __redisRunPushCallback(ac, reply);
            c->reader->fn->freeObject(reply);
            continue;
        }

        /* Even if the context is subscribed, pending regular
         * callbacks will get a reply before pub/sub messages arrive.
         * 无法从replies中取出回调对象 可能是因为此连接是针对订阅发布的  这样回调会存储到特殊的链表中
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
             * 先忽略异常情况
             */
            if (((redisReply*)reply)->type == REDIS_REPLY_ERROR) {
                c->err = REDIS_ERR_OTHER;
                snprintf(c->errstr,sizeof(c->errstr),"%s",((redisReply*)reply)->str);
                c->reader->fn->freeObject(reply);
                __redisAsyncDisconnect(ac);
                return;
            }
            /* No more regular callbacks and no errors, the context *must* be subscribed or monitoring.
             * 此时context必然已经被标记成订阅模式
             * */
            assert((c->flags & REDIS_SUBSCRIBED || c->flags & REDIS_MONITORING));
            if(c->flags & REDIS_SUBSCRIBED)
                // 根据reply信息从特殊列表中找到匹配的回调对象
                __redisGetSubscribeCallback(ac,reply,&cb);
        }

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
             * doesn't know what the server will spit out over the wire.
             * 针对返回结果没有设置回调对象 忽略本次返回结果
             * */
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
 * 之前在sentinel中 都是使用异步连接，并将write事件注册到ae上 此时收到write事件可能是代表连接已经完成了
 * */
static int __redisAsyncHandleConnect(redisAsyncContext *ac) {
    int completed = 0;
    redisContext *c = &(ac->c);

    // 连接失败在sentinel的定时任务中就是等待一定时间后 进行下一次连接
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

        // 本次连接成功修改标记位 同时执行onConnect函数
        if (ac->onConnect) ac->onConnect(ac, REDIS_OK);
        c->flags |= REDIS_CONNECTED;
        return REDIS_OK;
    } else {
        return REDIS_OK;
    }
}

/**
 * 对应 c->funcs->async_read(ac)
 * 有关网络模块 client总要准备解析响应结果的逻辑 (服务器则负责解析收到的数据) 在redis内部针对特定的模块都会使用自定义的readHandler 比如cluster/replication
 * 所以我们使用到的redisClient实际上是帮我们做好了这一步
 * @param ac
 */
void redisAsyncRead(redisAsyncContext *ac) {
    redisContext *c = &(ac->c);

    // 将socket缓冲区的数据转移到reader中
    if (redisBufferRead(c) == REDIS_ERR) {
        __redisAsyncDisconnect(ac);
    } else {
        /* Always re-schedule reads
         * 读事件需要一直监听
         * */
        _EL_ADD_READ(ac);
        // 现在开始处理收到的数据  在redisBufferRead中没有做粘拆包处理
        redisProcessCallbacks(ac);
    }
}

/* This function should be called when the socket is readable.
 * It processes all replies that can be read and executes their callbacks.
 * 当注册的read事件准备完成后
 */
void redisAsyncHandleRead(redisAsyncContext *ac) {
    redisContext *c = &(ac->c);

    // 如果此时连接还没有完成 先进行连接
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
 * 对应异步写入执行的函数
 * 在sentinel模块中每次执行asyncCommand时 都是将待发送数据存入到一个缓冲区中
 * @param ac
 */
void redisAsyncWrite(redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    // done代表数据是否全部被写完
    int done = 0;

    // 将缓冲区内的数据写入到socket中
    if (redisBufferWrite(c,&done) == REDIS_ERR) {
        __redisAsyncDisconnect(ac);
    } else {
        /* Continue writing when not done, stop writing otherwise
         * */
        if (!done)
            // 因为还有数据 还需要继续注册写处理器
            _EL_ADD_WRITE(ac);
        else
            // 触发delWrite  就是从el上注销
            _EL_DEL_WRITE(ac);

        /* Always schedule reads after writes
         * 在发送数据后 可能会收到对端返回的信息 所以这里要注册readHandler
         * */
        _EL_ADD_READ(ac);
    }
}

/**
 * 当ae发现写事件准备完成时就会调用writeHandler进行处理
 * @param ac
 */
void redisAsyncHandleWrite(redisAsyncContext *ac) {
    redisContext *c = &(ac->c);

    // 此时发现与目标节点的连接还未完全建立
    if (!(c->flags & REDIS_CONNECTED)) {
        /* Abort connect was not successful.
         * 先处理连接事件  如果本次连接还未完成也就无法进行后面的写入工作
         * */
        if (__redisAsyncHandleConnect(ac) != REDIS_OK)
            return;
        /* Try again later when the context is still not connected. */
        if (!(c->flags & REDIS_CONNECTED))
            return;
    }

    // 在连接还没有完成时 待发送的数据会存储在一个缓冲区中 这里就是打算发送这些数据
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
 * 发送的数据格式为
 * *总长度
 * $第一个参数长度
 * 第一个参数
 * $第二个参数长度
 * 第二个参数
 * ...
 * */
static const char *nextArgument(const char *start, const char **str, size_t *len) {
    const char *p = start;

    // 直接定位到$的位置 对应描述某个参数的长度
    if (p[0] != '$') {
        p = strchr(p,'$');
        if (p == NULL) return NULL;
    }

    // 读取参数长度
    *len = (int)strtol(p+1,NULL,10);
    // 定位到长度后面的换行符
    p = strchr(p,'\r');
    assert(p);
    // 切换到了参数的起点
    *str = p+2;
    // 返回的是参数的终点
    return p+2+(*len)+2;
}

/* Helper function for the redisAsyncCommand* family of functions. Writes a
 * formatted command to the output buffer and registers the provided callback
 * function with the context.
 * 异步执行某个command
 * @param cmd 本次执行命令的参数流
 * @param len cmd的长度
 * @param privdate 本次针对的实例对象
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
     * p对应的是某个参数的终点
     * cstr对应参数的起始位置
     * clen对应参数的长度
     * */
    p = nextArgument(cmd,&cstr,&clen);
    assert(p != NULL);
    // 代表还有下一个参数
    hasnext = (p[0] == '$');
    // p开头就代表本次传入的是一个正则表达式
    pvariant = (tolower(cstr[0]) == 'p') ? 1 : 0;
    cstr += pvariant;
    clen -= pvariant;

    // sentinel会与其他master/slave建立一个专门用于订阅发布的连接 会进入这个分支
    // 这里代表发起的是一个订阅命令 同时后面还有其他参数 实际上就是订阅的管道 而且隐含信息就是每次执行subscribe命令 都只能发送一个channel
    if (hasnext && strncasecmp(cstr,"subscribe\r\n",11) == 0) {
        // 一旦发送过订阅请求 就代表这个context对应的连接是专门用于订阅发布的
        c->flags |= REDIS_SUBSCRIBED;

        /* Add every channel/pattern to the list of subscription callbacks.
         * 本哨兵此时对某个client发起了一个订阅命令 有关订阅了什么channel会被保存在这里
         * 这里是遍历所有订阅的管道
         * */
        while ((p = nextArgument(p,&astr,&alen)) != NULL) {
            sname = hi_sdsnewlen(astr,alen);
            if (sname == NULL)
                goto oom;

            // 找到之前存储callback的容器
            if (pvariant)
                cbdict = ac->sub.patterns;
            else
                cbdict = ac->sub.channels;

            // 找到之前针对这个channel/pattern的回调对象
            de = dictFind(cbdict,sname);

            // 原本存在callback的情况下 增加计数值 使用新的callback替换原对象
            if (de != NULL) {
                existcb = dictGetEntryVal(de);
                cb.pending_subs = existcb->pending_subs + 1;
            }

            // 如果是首次插入 等同于add
            ret = dictReplace(cbdict,sname,&cb);

            if (ret == 0) hi_sdsfree(sname);
        }
        // 取消订阅不需要注册回调对象
    } else if (strncasecmp(cstr,"unsubscribe\r\n",13) == 0) {
        /* It is only useful to call (P)UNSUBSCRIBE when the context is
         * subscribed to one or more channels or patterns.
         * 去除标记
         * */
        if (!(c->flags & REDIS_SUBSCRIBED)) return REDIS_ERR;

        /* (P)UNSUBSCRIBE does not have its own response: every channel or
         * pattern that is unsubscribed will receive a message. This means we
         * should not append a callback function for this command.
         * 一旦执行过监控命令 增加监控标记
         * */
     } else if(strncasecmp(cstr,"monitor\r\n",9) == 0) {
         /* Set monitor flag and push callback */
         c->flags |= REDIS_MONITORING;
         __redisPushCallback(&ac->replies,&cb);

         // 本次执行的是一个普通命令 (哨兵连接到其他节点前可能需要验证)
    } else {
        // 针对用于订阅发布的连接 如果执行的是普通的命令 使用特殊的容器存储数据
        if (c->flags & REDIS_SUBSCRIBED)
            /* This will likely result in an error reply, but it needs to be
             * received and passed to the callback. */
            __redisPushCallback(&ac->sub.invalid,&cb);
        else
            // 其余情况都是使用 replies存储回调函数
            __redisPushCallback(&ac->replies,&cb);
    }

    // 将数据写入到buf中
    __redisAppendCommand(c,cmd,len);

    /* Always schedule a write when the write buffer is non-empty
     * 因为此时添加了需要发送的数据 所以这里要注册writeHandler
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

    // 将format拼接上可变参数后 还会加工成满足redis协议的格式
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
