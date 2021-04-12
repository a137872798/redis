/* Background I/O service for Redis.
 *
 * This file implements operations that we need to perform in the background.
 * Currently there is only a single operation, that is a background close(2)
 * system call. This is needed as when the process is the last owner of a
 * reference to a file closing it means unlinking it, and the deletion of the
 * file is slow, blocking the server.
 *
 * In the future we'll either continue implementing new things we need or
 * we'll switch to libeio. However there are probably long term uses for this
 * file as we may want to put here Redis specific background tasks (for instance
 * it is not impossible that we'll need a non blocking FLUSHDB/FLUSHALL
 * implementation).
 *
 * DESIGN
 * ------
 *
 * The design is trivial, we have a structure representing a job to perform
 * and a different thread and job queue for every job type.
 * Every thread waits for new jobs in its queue, and process every job
 * sequentially.
 *
 * Jobs of the same type are guaranteed to be processed from the least
 * recently inserted to the most recently inserted (older jobs processed
 * first).
 *
 * Currently there is no way for the creator of the job to be notified about
 * the completion of the operation, this will only be added when/if needed.
 *
 * ----------------------------------------------------------------------------
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
#include "bio.h"

static pthread_t bio_threads[BIO_NUM_OPS];
static pthread_mutex_t bio_mutex[BIO_NUM_OPS];
static pthread_cond_t bio_newjob_cond[BIO_NUM_OPS];
static pthread_cond_t bio_step_cond[BIO_NUM_OPS];
static list *bio_jobs[BIO_NUM_OPS];
/* The following array is used to hold the number of pending jobs for every
 * OP type. This allows us to export the bioPendingJobsOfType() API that is
 * useful when the main thread wants to perform some operation that may involve
 * objects shared with the background thread. The main thread will just wait
 * that there are no longer jobs of this type to be executed before performing
 * the sensible operation. This data is also useful for reporting.
 * 每种后台线程待执行的任务数量
 * */
static unsigned long long bio_pending[BIO_NUM_OPS];

/* This structure represents a background Job. It is only used locally to this
 * file as the API does not expose the internals at all. */
struct bio_job {
    time_t time; /* Time at which the job was created. */
    /* Job specific arguments pointers. If we need to pass more than three
     * arguments we can just pass a pointer to a structure or alike. */
    void *arg1, *arg2, *arg3;
};

void *bioProcessBackgroundJobs(void *arg);
void lazyfreeFreeObjectFromBioThread(robj *o);
void lazyfreeFreeDatabaseFromBioThread(dict *ht1, dict *ht2);
void lazyfreeFreeSlotsMapFromBioThread(zskiplist *sl);

/* Make sure we have enough stack to perform all the things we do in the
 * main thread. */
#define REDIS_THREAD_STACK_SIZE (1024*1024*4)

/* Initialize the background system, spawning the thread.
 * 初始化后台系统 并生成大量线程
 * */
void bioInit(void) {
    pthread_attr_t attr;
    pthread_t thread;
    size_t stacksize;
    int j;

    /* Initialization of state vars and objects */
    for (j = 0; j < BIO_NUM_OPS; j++) {
        // 初始化线程锁/条件对象
        pthread_mutex_init(&bio_mutex[j],NULL);
        pthread_cond_init(&bio_newjob_cond[j],NULL);
        pthread_cond_init(&bio_step_cond[j],NULL);
        // 这应该是每个线程要执行的任务
        bio_jobs[j] = listCreate();
        bio_pending[j] = 0;
    }

    /* Set the stack size as by default it may be small in some system
     * 在使用attr之前 需要进行初始化 内部的属性是由操作系统决定的
     * */
    pthread_attr_init(&attr);
    // 之前通过init方法已经设置了线程的基本属性 现在尝试获取线程栈长度
    pthread_attr_getstacksize(&attr,&stacksize);
    // 代表在某些系统下可能没有栈长度 这里要设置一个默认值
    if (!stacksize) stacksize = 1; /* The world is full of Solaris Fixes */
    // 将线程栈扩充到某个值
    while (stacksize < REDIS_THREAD_STACK_SIZE) stacksize *= 2;
    pthread_attr_setstacksize(&attr, stacksize);

    /* Ready to spawn our threads. We use the single argument the thread
     * function accepts in order to pass the job ID the thread is
     * responsible of.
     * 开始创建线程
     * */
    for (j = 0; j < BIO_NUM_OPS; j++) {
        void *arg = (void*)(unsigned long) j;
        // 可以看到创建线程时 要传入一个thread_attr对象 该线程会基于该属性对象初始化  比如线程的栈大小
        // bioProcessBackgroundJobs 对应每条线程执行的任务
        // arg对应运行函数的参数  可以看到这里的参数实际上就是线程对于线程组的下标
        // 在调用create方法后 会立即执行函数
        if (pthread_create(&thread,&attr,bioProcessBackgroundJobs,arg) != 0) {
            serverLog(LL_WARNING,"Fatal: Can't initialize Background Jobs.");
            exit(1);
        }
        bio_threads[j] = thread;
    }
}

void bioCreateBackgroundJob(int type, void *arg1, void *arg2, void *arg3) {
    struct bio_job *job = zmalloc(sizeof(*job));

    job->time = time(NULL);
    job->arg1 = arg1;
    job->arg2 = arg2;
    job->arg3 = arg3;
    pthread_mutex_lock(&bio_mutex[type]);
    listAddNodeTail(bio_jobs[type],job);
    bio_pending[type]++;
    pthread_cond_signal(&bio_newjob_cond[type]);
    pthread_mutex_unlock(&bio_mutex[type]);
}

/**
 * redis在对server进行初始化的时候 会开启一组后台线程 (默认是3条)
 * @param arg
 * @return
 */
void *bioProcessBackgroundJobs(void *arg) {
    // 每个job对象内部有一个time属性 以及3个void指针
    struct bio_job *job;

    // 对应bio线程的下标 默认0～2 每个线程应该专门用于做一件事
    unsigned long type = (unsigned long) arg;
    sigset_t sigset;

    /* Check that the type is within the right interval. */
    if (type >= BIO_NUM_OPS) {
        serverLog(LL_WARNING,
            "Warning: bio thread started with wrong type %lu",type);
        return NULL;
    }


    // 从这里就可以看到redis的3条后台线程 分别用于关闭文件/aof的异步持久化/内存释放
    switch (type) {
    case BIO_CLOSE_FILE:
        redis_set_thread_title("bio_close_file");
        break;
    case BIO_AOF_FSYNC:
        redis_set_thread_title("bio_aof_fsync");
        break;
    case BIO_LAZY_FREE:
        redis_set_thread_title("bio_lazy_free");
        break;
    }

    // 设置cpu亲和性 也就是让一个进程尽可能的一直在某个cpu上运行
    redisSetCpuAffinity(server.bio_cpulist);

    /* Make the thread killable at any time, so that bioKillThreads()
     * can work reliably.
     * 代表本线程支持处理关闭信号
     * 是配合 pthread_cancel使用的 pthread_cancel会向该线程发起一个关闭信号 但是该函数只关心是否发送成功 而不在意线程处理结果。至于线程会如何处理信号是由线程自己定义的
     * 如果此时线程的cancelState为disable 关闭信号并不会丢失 而是存储在一个队列中 当线程修改cancel状态为
     * */
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    // 这里是定义取消的方式 存在2种取消方式
    // 1.PTHREAD_CANCEL_DEFERRED 此时线程在收到取消事件后不会立即取消，而是会等到执行在一个取消点 cancellation point 很多函数就是取消点函数，在执行完这些函数后
    // 认为线程可以被安全的关闭
    // 2.PTHREAD_CANCEL_ASYNCHRONOUS 立即关闭线程 但是此时线程任务可能处于不确定的状态
    pthread_setcanceltype(PTHREAD_CANCEL_ASYNCHRONOUS, NULL);

    // 对本线程上锁
    pthread_mutex_lock(&bio_mutex[type]);
    /* Block SIGALRM so we are sure that only the main thread will
     * receive the watchdog signal.
     * 初始化信号集 之后加入一个指定的信号
     * */
    sigemptyset(&sigset);
    // 看来是通过这个 watchdog 来产生SIGALRM信号 并且不希望bio线程被影响
    sigaddset(&sigset, SIGALRM);
    // 使得当前线程屏蔽该信号集中的信号
    if (pthread_sigmask(SIG_BLOCK, &sigset, NULL))
        serverLog(LL_WARNING,
            "Warning: can't mask SIGALRM in bio.c thread: %s", strerror(errno));

    // 每个线程都是在自旋执行某个任务
    while(1) {
        listNode *ln;

        /* The loop always starts with the lock hold.
         * 这些线程是从队列中读取任务并执行的 有点像线程池中worker拉取阻塞队列的套路
         * */
        if (listLength(bio_jobs[type]) == 0) {
            // 代表阻塞当前线程直到队列中插入了新的任务
            pthread_cond_wait(&bio_newjob_cond[type],&bio_mutex[type]);
            continue;
        }
        /* Pop the job from the queue. */
        ln = listFirst(bio_jobs[type]);
        job = ln->value;
        /* It is now possible to unlock the background system as we know have
         * a stand alone job structure to process.
         * TODO 这里为什么要解锁 以及线程为什么在创建后要上锁
         * */
        pthread_mutex_unlock(&bio_mutex[type]);

        /* Process the job accordingly to its type. */
        // 按照类型做不同的逻辑
        // TODO 为什么这些任务不放在主线程中执行呢 推测是io操作比较耗时 而redis本身是单工作线程模型，不希望这些io操作阻塞主线程 降低响应度吧
        if (type == BIO_CLOSE_FILE) {
            // arg1应该就是文件句柄
            close((long)job->arg1);
        } else if (type == BIO_AOF_FSYNC) {
            // 执行刷盘操作
            redis_fsync((long)job->arg1);
            // 进行一些内存释放操作  TODO 内存释放操作为什么要异步执行 需要完全理解redis的插入删除逻辑后才好理解
        } else if (type == BIO_LAZY_FREE) {
            /* What we free changes depending on what arguments are set:
             * arg1 -> free the object at pointer.
             * arg2 & arg3 -> free two dictionaries (a Redis DB).
             * only arg3 -> free the skiplist. */
            // arg1 对应某个对象指针 下面这3种api应该会转发到 lazyfree.c 虽然不清楚是通过什么方式
            if (job->arg1)
                lazyfreeFreeObjectFromBioThread(job->arg1);
            // 释放dict内存 目前没看出跟db有什么关系   看来db就是存储在dict中
            else if (job->arg2 && job->arg3)
                lazyfreeFreeDatabaseFromBioThread(job->arg2,job->arg3);
            else if (job->arg3)
                // 就是释放 rax对象
                lazyfreeFreeSlotsMapFromBioThread(job->arg3);
        } else {
            serverPanic("Wrong job type in bioProcessBackgroundJobs().");
        }
        // 在完成任务后 释放job占用内存
        zfree(job);

        /* Lock again before reiterating the loop, if there are no longer
         * jobs to process we'll block again in pthread_cond_wait().
         * 在执行完任务后 本线程重新获取该锁
         * */
        pthread_mutex_lock(&bio_mutex[type]);
        // 将任务从任务队列中移除
        listDelNode(bio_jobs[type],ln);
        bio_pending[type]--;

        /* Unblock threads blocked on bioWaitStepOfType() if any.
         * 通知可以往任务队列中继续插入任务了
         * */
        pthread_cond_broadcast(&bio_step_cond[type]);
    }
}

/* Return the number of pending jobs of the specified type.
 * 获取某一线程此时待执行的任务数量
 * */
unsigned long long bioPendingJobsOfType(int type) {
    unsigned long long val;
    pthread_mutex_lock(&bio_mutex[type]);
    val = bio_pending[type];
    pthread_mutex_unlock(&bio_mutex[type]);
    return val;
}

/* If there are pending jobs for the specified type, the function blocks
 * and waits that the next job was processed. Otherwise the function
 * does not block and returns ASAP.
 *
 * The function returns the number of jobs still to process of the
 * requested type.
 *
 * This function is useful when from another thread, we want to wait
 * a bio.c thread to do more work in a blocking way.
 */
unsigned long long bioWaitStepOfType(int type) {
    unsigned long long val;
    pthread_mutex_lock(&bio_mutex[type]);
    val = bio_pending[type];
    if (val != 0) {
        pthread_cond_wait(&bio_step_cond[type],&bio_mutex[type]);
        val = bio_pending[type];
    }
    pthread_mutex_unlock(&bio_mutex[type]);
    return val;
}

/* Kill the running bio threads in an unclean way. This function should be
 * used only when it's critical to stop the threads for some reason.
 * Currently Redis does this only on crash (for instance on SIGSEGV) in order
 * to perform a fast memory check without other threads messing with memory. */
void bioKillThreads(void) {
    int err, j;

    for (j = 0; j < BIO_NUM_OPS; j++) {
        if (bio_threads[j] && pthread_cancel(bio_threads[j]) == 0) {
            if ((err = pthread_join(bio_threads[j],NULL)) != 0) {
                serverLog(LL_WARNING,
                    "Bio thread for job type #%d can be joined: %s",
                        j, strerror(err));
            } else {
                serverLog(LL_WARNING,
                    "Bio thread for job type #%d terminated",j);
            }
        }
    }
}
