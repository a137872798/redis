/* A simple event-driven programming library. Originally I wrote this code
 * for the Jim's event-loop (Jim is a Tcl interpreter) but later translated
 * it in form of a library for easy reuse.
 *
 * Copyright (c) 2006-2010, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include "ae.h"
#include "anet.h"

#include <stdio.h>
#include <sys/time.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <time.h>
#include <errno.h>

#include "zmalloc.h"
#include "config.h"

/* Include the best multiplexing layer supported by this system.
 * The following should be ordered by performances, descending.
 * 这里是事件循环类型
 * */
#ifdef HAVE_EVPORT
#include "ae_evport.c"
#else
// 默认使用epoll实现事件循环
#ifdef HAVE_EPOLL
#include "ae_epoll.c"
#else
// mac系统会该c文件
#ifdef HAVE_KQUEUE
#include "ae_kqueue.c"
#else

#include "ae_select.c"

#endif
#endif
#endif

/**
 * 初始化一个事件循环对象
 * @param setsize  对应的是客户端句柄的数量 也就是redis最大允许的同时连接的client数量
 * @return
 */
aeEventLoop *aeCreateEventLoop(int setsize) {
    aeEventLoop *eventLoop;
    int i;

    // 初始化单调性相关的东西
    monotonicInit();    /* just in case the calling app didn't initialize */

    // 尝试分配一个事件循环需要的内存时 发现内存不够
    if ((eventLoop = zmalloc(sizeof(*eventLoop))) == NULL) goto err;
    // 为注册事件和触发事件申请存储槽
    eventLoop->events = zmalloc(sizeof(aeFileEvent) * setsize);
    eventLoop->fired = zmalloc(sizeof(aeFiredEvent) * setsize);
    // 如果有某个槽分配失败了
    if (eventLoop->events == NULL || eventLoop->fired == NULL) goto err;

    // 在c语言中 每次为对象分配内存后 都需要手动为各个属性初始化
    eventLoop->setsize = setsize;
    eventLoop->timeEventHead = NULL;
    eventLoop->timeEventNextId = 0;
    eventLoop->stop = 0;
    // 此时未绑定任何事件 该值为-1
    eventLoop->maxfd = -1;
    eventLoop->beforesleep = NULL;
    eventLoop->aftersleep = NULL;
    eventLoop->flags = 0;
    // 目前只关注 epoll实现  应该是事件循环本身只是一个模板 实现还是需要借助底层数据结构 他们被统一包装成 apidata
    if (aeApiCreate(eventLoop) == -1) goto err;
    /* Events with mask == AE_NONE are not set. So let's initialize the
     * vector with it. */
    // 将相关mask先重置
    for (i = 0; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;
    return eventLoop;

    err:
    // 代表事件循环已经被创建 就要将申请的slot全部释放
    if (eventLoop) {
        zfree(eventLoop->events);
        zfree(eventLoop->fired);
        zfree(eventLoop);
    }
    return NULL;
}

/* Return the current set size.
 * 获取事件循环内的 slot数量
 * */
int aeGetSetSize(aeEventLoop *eventLoop) {
    return eventLoop->setsize;
}

/* Tells the next iteration/s of the event processing to set timeout of 0.
 * 设置是否等待的标识位
 * */
void aeSetDontWait(aeEventLoop *eventLoop, int noWait) {
    if (noWait)
        eventLoop->flags |= AE_DONT_WAIT;
    else
        eventLoop->flags &= ~AE_DONT_WAIT;
}

/* Resize the maximum set size of the event loop.
 * If the requested set size is smaller than the current set size, but
 * there is already a file descriptor in use that is >= the requested
 * set size minus one, AE_ERR is returned and the operation is not
 * performed at all.
 *
 * Otherwise AE_OK is returned and the operation is successful.
 * 使用指定的setSize 重置槽
 * */
int aeResizeSetSize(aeEventLoop *eventLoop, int setsize) {
    int i;

    if (setsize == eventLoop->setsize) return AE_OK;
    // 如果此时已经有某些事件注册上去了 不能缩容到比该值小
    if (eventLoop->maxfd >= setsize) return AE_ERR;
    // 通过调用操作系统级别api 实现更新事件槽长度
    if (aeApiResize(eventLoop, setsize) == -1) return AE_ERR;

    eventLoop->events = zrealloc(eventLoop->events, sizeof(aeFileEvent) * setsize);
    eventLoop->fired = zrealloc(eventLoop->fired, sizeof(aeFiredEvent) * setsize);
    eventLoop->setsize = setsize;

    /* Make sure that if we created new slots, they are initialized with
     * an AE_NONE mask.
     * 重置 max之后的位置
     * */
    for (i = eventLoop->maxfd + 1; i < setsize; i++)
        eventLoop->events[i].mask = AE_NONE;
    return AE_OK;
}

/**
 * 清除某个事件循环
 * @param eventLoop
 */
void aeDeleteEventLoop(aeEventLoop *eventLoop) {
    aeApiFree(eventLoop);
    // 先释放掉文件事件和触发事件
    zfree(eventLoop->events);
    zfree(eventLoop->fired);

    /* Free the time events list. */
    aeTimeEvent *next_te, *te = eventLoop->timeEventHead;
    // 这里遍历时间事件 并挨个进行回收
    while (te) {
        next_te = te->next;
        zfree(te);
        te = next_te;
    }
    zfree(eventLoop);
}

void aeStop(aeEventLoop *eventLoop) {
    eventLoop->stop = 1;
}

/**
 * 创建文件事件 实际上就是socket事件
 * @param eventLoop
 * @param fd socket句柄
 * @param mask 描述该事件是可读/可写
 * @param proc 当感知到事件触发时,使用该函数处理
 * @param clientData
 * @return
 */
int aeCreateFileEvent(aeEventLoop *eventLoop, int fd, int mask,
                      aeFileProc *proc, void *clientData) {
    // 在server初始化时 曾经调整过允许接收的文件句柄数据 setsize就是允许接收的最大长度
    // fd是从0开始递增的么 不过这个逻辑不影响事件循环本身 先不管
    if (fd >= eventLoop->setsize) {
        errno = ERANGE;
        return AE_ERR;
    }

    // 当前槽内还未设置事件
    aeFileEvent *fe = &eventLoop->events[fd];

    // 尝试插入文件事件
    if (aeApiAddEvent(eventLoop, fd, mask) == -1)
        return AE_ERR;
    fe->mask |= mask;
    // 本次mask的类型代表了相关的proc是针对什么操作的函数
    if (mask & AE_READABLE) fe->rfileProc = proc;
    if (mask & AE_WRITABLE) fe->wfileProc = proc;
    fe->clientData = clientData;
    // 更新此时的最大句柄
    if (fd > eventLoop->maxfd)
        eventLoop->maxfd = fd;
    return AE_OK;
}

/**
 * 从事件循环上删除某个事件   一般是要断开某个连接
 * @param eventLoop
 * @param fd  对应client socket句柄
 * @param mask
 */
void aeDeleteFileEvent(aeEventLoop *eventLoop, int fd, int mask) {
    if (fd >= eventLoop->setsize) return;
    // 找到对应的事件
    aeFileEvent *fe = &eventLoop->events[fd];
    // mask 为NONE 就代表还未初始化事件
    if (fe->mask == AE_NONE) return;

    /* We want to always remove AE_BARRIER if set when AE_WRITABLE
     * is removed.
     * 当移除写事件时 自动移除barrier
     * */
    if (mask & AE_WRITABLE) mask |= AE_BARRIER;

    // 在底层进行删除
    aeApiDelEvent(eventLoop, fd, mask);
    fe->mask = fe->mask & (~mask);
    // 此时移除的是最后一个事件
    if (fd == eventLoop->maxfd && fe->mask == AE_NONE) {
        /* Update the max fd */
        int j;

        // 从后往前找到第一个mask不为null的fd
        for (j = eventLoop->maxfd - 1; j >= 0; j--)
            if (eventLoop->events[j].mask != AE_NONE) break;
        eventLoop->maxfd = j;
    }
}

/**
 * 获取 fd对应的事件
 * @param eventLoop
 * @param fd
 * @return
 */
int aeGetFileEvents(aeEventLoop *eventLoop, int fd) {
    if (fd >= eventLoop->setsize) return 0;
    aeFileEvent *fe = &eventLoop->events[fd];

    return fe->mask;
}

/**
 * 创建时间事件
 * @param eventLoop
 * @param milliseconds
 * @param proc 这里传入的是一个函数对象  函数签名:typedef int aeTimeProc(struct aeEventLoop *eventLoop, long long id, void *clientData);
 *             当定时任务触发时就会使用该函数进行处理
 * @param clientData 携带的额外数据
 * @param finalizerProc
 * @return
 */
long long aeCreateTimeEvent(aeEventLoop *eventLoop, long long milliseconds,
                            aeTimeProc *proc, void *clientData,
                            aeEventFinalizerProc *finalizerProc) {
    // 为时间事件分配一个新id
    long long id = eventLoop->timeEventNextId++;
    aeTimeEvent *te;

    // 先申请一个时间事件的内存 分配失败返回 ERR
    te = zmalloc(sizeof(*te));
    if (te == NULL) return AE_ERR;
    // 进行一些赋值操作
    te->id = id;
    // 设置触发时间
    te->when = getMonotonicUs() + milliseconds * 1000;
    te->timeProc = proc;
    te->finalizerProc = finalizerProc;
    te->clientData = clientData;
    te->prev = NULL;

    // 更新链表结构
    te->next = eventLoop->timeEventHead;
    te->refcount = 0;
    if (te->next)
        te->next->prev = te;
    eventLoop->timeEventHead = te;
    return id;
}

/**
 * 删除时间事件
 * @param eventLoop
 * @param id
 * @return
 */
int aeDeleteTimeEvent(aeEventLoop *eventLoop, long long id) {
    aeTimeEvent *te = eventLoop->timeEventHead;
    while (te) {
        // 遍历链表 当遇到匹配的 标记成已经删除  并返回OK
        if (te->id == id) {
            te->id = AE_DELETED_EVENT_ID;
            return AE_OK;
        }
        te = te->next;
    }
    return AE_ERR; /* NO event with the specified ID found */
}

/* How many milliseconds until the first timer should fire.
 * If there are no timers, -1 is returned.
 *
 * Note that's O(N) since time events are unsorted.
 * Possible optimizations (not needed by Redis so far, but...):
 * 1) Insert the event in order, so that the nearest is just the head.
 *    Much better but still insertion or deletion of timers is O(N).
 * 2) Use a skiplist to have this operation as O(1) and insertion as O(log(N)).
 * 遍历链表 并返回最早的时间事件还有多久发生
 */
static long msUntilEarliestTimer(aeEventLoop *eventLoop) {
    aeTimeEvent *te = eventLoop->timeEventHead;
    if (te == NULL) return -1;

    aeTimeEvent *earliest = NULL;
    while (te) {
        if (!earliest || te->when < earliest->when)
            earliest = te;
        te = te->next;
    }

    monotime now = getMonotonicUs();
    return (now >= earliest->when)
           ? 0 : (long) ((earliest->when - now) / 1000);
}

/* Process time events
 * 处理所有的时间事件 注意是单线程去处理
 * */
static int processTimeEvents(aeEventLoop *eventLoop) {
    int processed = 0;
    aeTimeEvent *te;
    long long maxId;

    te = eventLoop->timeEventHead;
    maxId = eventLoop->timeEventNextId - 1;
    monotime now = getMonotonicUs();
    while (te) {
        long long id;

        /*
         * Remove events scheduled for deletion.
         * 当某个时间事件要被移除时 没有直接更新链表 而是先将id标记成一个特殊值
         * 看来是惰性处理
         * */
        if (te->id == AE_DELETED_EVENT_ID) {
            aeTimeEvent *next = te->next;
            /* If a reference exists for this timer event,
             * don't free it. This is currently incremented
             * for recursive timerProc calls
             * 虽然该事件被标记成删除 但是还有地方在引用它
             * */
            if (te->refcount) {
                te = next;
                continue;
            }
            // 链表操作
            if (te->prev)
                te->prev->next = te->next;
            else
                // 如果是首个节点被移除 更新head
                eventLoop->timeEventHead = te->next;
            if (te->next)
                te->next->prev = te->prev;
            // 如果有回收函数 进行回收
            if (te->finalizerProc) {
                te->finalizerProc(eventLoop, te->clientData);
                now = getMonotonicUs();
            }
            zfree(te);
            te = next;
            continue;
        }

        /* Make sure we don't process time events created by time events in
         * this iteration. Note that this check is currently useless: we always
         * add new timers on the head, however if we change the implementation
         * detail, this check may be useful again: we keep it here for future
         * defense.
         * 如果此时某些节点超过了 记录的最大id 先跳过 不进行处理
         * */
        if (te->id > maxId) {
            te = te->next;
            continue;
        }

        // 代表这些时间事件满足触发条件
        if (te->when <= now) {
            int retval;

            id = te->id;
            // 任务已经被触发 增加引用计数 这样该node就不会从链表中被移除
            te->refcount++;
            // 这里是触发处理函数
            retval = te->timeProc(eventLoop, id, te->clientData);
            te->refcount--;
            processed++;
            now = getMonotonicUs();
            // 代表这个任务是反复执行的 更新下次执行时间
            if (retval != AE_NOMORE) {
                te->when = now + retval * 1000;
            } else {
                te->id = AE_DELETED_EVENT_ID;
            }
        }
        te = te->next;
    }
    return processed;
}

/* Process every pending time event, then every pending file event
 * (that may be registered by time event callbacks just processed).
 * Without special flags the function sleeps until some file event
 * fires, or when the next time event occurs (if any).
 *
 * If flags is 0, the function does nothing and returns.
 * if flags has AE_ALL_EVENTS set, all the kind of events are processed.
 * if flags has AE_FILE_EVENTS set, file events are processed.
 * if flags has AE_TIME_EVENTS set, time events are processed.
 * if flags has AE_DONT_WAIT set the function returns ASAP until all
 * the events that's possible to process without to wait are processed.
 * if flags has AE_CALL_AFTER_SLEEP set, the aftersleep callback is called.
 * if flags has AE_CALL_BEFORE_SLEEP set, the beforesleep callback is called.
 *
 * The function returns the number of events processed.
 * @param flags 根据flags的信息 判断要处理哪些事件
 * 当main线程在完成了server的初始化后 会开始执行事件循环
 * */
int aeProcessEvents(aeEventLoop *eventLoop, int flags) {
    int processed = 0, numevents;

    /* Nothing to do? return ASAP
     * 如果不需要处理时间事件 和 文件事件 直接返回
     * 对应定时任务 和 连接/读/写事件
     * */
    if (!(flags & AE_TIME_EVENTS) && !(flags & AE_FILE_EVENTS)) return 0;

    /* Note that we want to call select() even if there are no
     * file events to process as long as we want to process time
     * events, in order to sleep until the next time event is ready
     * to fire.
     * maxfd 应该就是允许接收的句柄数量 == -1 代表不处理fileEvent
     * 不包含 AE_DONT_WAIT 就代表允许调用select() 阻塞自身
     * */
    if (eventLoop->maxfd != -1 ||
        // 如果设置了允许阻塞等待 才会进入下面的块 否则就是自旋直到有满足条件的时间事件
        ((flags & AE_TIME_EVENTS) && !(flags & AE_DONT_WAIT))) {
        int j;
        struct timeval tv, *tvp;
        long msUntilTimer = -1;

        // 计算距离最近的时间事件还有多久触发
        if (flags & AE_TIME_EVENTS && !(flags & AE_DONT_WAIT))
            msUntilTimer = msUntilEarliestTimer(eventLoop);

        // 这里是计算等待时长 是这样 select的阻塞时间 就是直到下一个时间事件触发
        // 如果没有设置时间事件 就是传入-1 select阻塞直到接收到fileEvent
        if (msUntilTimer >= 0) {
            tv.tv_sec = msUntilTimer / 1000;
            tv.tv_usec = (msUntilTimer % 1000) * 1000;
            tvp = &tv;
        } else {
            /* If we have to check for events but need to return
             * ASAP because of AE_DONT_WAIT we need to set the timeout
             * to zero
             * 检测是否拒绝等待  是的话将时间重置成0
             * */
            if (flags & AE_DONT_WAIT) {
                tv.tv_sec = tv.tv_usec = 0;
                tvp = &tv;
            } else {
                /* Otherwise we can block */
                tvp = NULL; /* wait forever */
            }
        }

        if (eventLoop->flags & AE_DONT_WAIT) {
            tv.tv_sec = tv.tv_usec = 0;
            tvp = &tv;
        }

        // 执行前置函数
        if (eventLoop->beforesleep != NULL && flags & AE_CALL_BEFORE_SLEEP)
            eventLoop->beforesleep(eventLoop);

        /* Call the multiplexing API, will return only on timeout or when
         * some event fires.
         * 这里就是底层选择器执行逻辑
         * */
        numevents = aeApiPoll(eventLoop, tvp);

        /* After sleep callback.
         * 执行后置函数
         * */
        if (eventLoop->aftersleep != NULL && flags & AE_CALL_AFTER_SLEEP)
            eventLoop->aftersleep(eventLoop);

        // 挨个处理每个事件
        for (j = 0; j < numevents; j++) {
            // 从poll返回后 会将事件填充到数组中   比如使用的是epoll函数 那么fe就是epoll_event->data.fd 这个是底层操作系统封装的结构体 这时没法确定内部存储的是什么
            aeFileEvent *fe = &eventLoop->events[eventLoop->fired[j].fd];
            // 通过mask信息来判断本次事件类型
            int mask = eventLoop->fired[j].mask;
            int fd = eventLoop->fired[j].fd;
            int fired = 0; /* Number of events fired for current fd. */

            /* Normally we execute the readable event first, and the writable
             * event later. This is useful as sometimes we may be able
             * to serve the reply of a query immediately after processing the
             * query.
             *
             * However if AE_BARRIER is set in the mask, our application is
             * asking us to do the reverse: never fire the writable event
             * after the readable. In such a case, we invert the calls.
             * This is useful when, for instance, we want to do things
             * in the beforeSleep() hook, like fsyncing a file to disk,
             * before replying to a client.
             * socket套接字对应的句柄的掩码
             * 读写事件的处理顺序会根据掩码值进行反转
             * */
            int invert = fe->mask & AE_BARRIER;

            // 下面的操作就是根据事件类型触发 read/write函数

            /* Note the "fe->mask & mask & ..." code: maybe an already
             * processed event removed an element that fired and we still
             * didn't processed, so we check if the event is still valid.
             *
             * Fire the readable event if the call sequence is not
             * inverted.
             * */
            if (!invert && fe->mask & mask & AE_READABLE) {
                fe->rfileProc(eventLoop, fd, fe->clientData, mask);
                fired++;
                fe = &eventLoop->events[fd]; /* Refresh in case of resize. */
            }

            /* Fire the writable event. 执行写函数 */
            if (fe->mask & mask & AE_WRITABLE) {
                if (!fired || fe->wfileProc != fe->rfileProc) {
                    fe->wfileProc(eventLoop, fd, fe->clientData, mask);
                    fired++;
                }
            }

            /* If we have to invert the call, fire the readable event now
             * after the writable one.
             * 代表读事件要求在写事件后触发
             * */
            if (invert) {
                fe = &eventLoop->events[fd]; /* Refresh in case of resize. */
                if ((fe->mask & mask & AE_READABLE) &&
                    (!fired || fe->wfileProc != fe->rfileProc)) {
                    fe->rfileProc(eventLoop, fd, fe->clientData, mask);
                    fired++;
                }
            }

            processed++;
        }
    }
    /* Check time events
     * 找到所有满足时间条件的事件 进行处理
     * */
    if (flags & AE_TIME_EVENTS)
        processed += processTimeEvents(eventLoop);

    return processed; /* return the number of processed file/time events */
}

/* Wait for milliseconds until the given file descriptor becomes
 * writable/readable/exception
 * 阻塞当前线程 直到监听的事件准备完成  一般是用于阻塞连接
 * */
int aeWait(int fd, int mask, long long milliseconds) {
    struct pollfd pfd;
    int retmask = 0, retval;

    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = fd;
    // 代表是否要监听  读/写事件
    if (mask & AE_READABLE) pfd.events |= POLLIN;
    if (mask & AE_WRITABLE) pfd.events |= POLLOUT;

    // 返回准备完成的事件
    if ((retval = poll(&pfd, 1, milliseconds)) == 1) {
        if (pfd.revents & POLLIN) retmask |= AE_READABLE;
        if (pfd.revents & POLLOUT) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLERR) retmask |= AE_WRITABLE;
        if (pfd.revents & POLLHUP) retmask |= AE_WRITABLE;
        return retmask;
    } else {
        return retval;
    }
}

/**
 * 启动事件循环
 * @param eventLoop
 */
void aeMain(aeEventLoop *eventLoop) {
    eventLoop->stop = 0;
    // 执行事件循环
    while (!eventLoop->stop) {
        aeProcessEvents(eventLoop, AE_ALL_EVENTS |
                                   AE_CALL_BEFORE_SLEEP |
                                   AE_CALL_AFTER_SLEEP);
    }
}

char *aeGetApiName(void) {
    return aeApiName();
}

void aeSetBeforeSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *beforesleep) {
    eventLoop->beforesleep = beforesleep;
}

void aeSetAfterSleepProc(aeEventLoop *eventLoop, aeBeforeSleepProc *aftersleep) {
    eventLoop->aftersleep = aftersleep;
}
