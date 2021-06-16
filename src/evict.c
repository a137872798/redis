/* Maxmemory directive handling (LRU eviction and other policies).
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2016, Salvatore Sanfilippo <antirez at gmail dot com>
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
#include "atomicvar.h"

/* ----------------------------------------------------------------------------
 * Data structures
 * --------------------------------------------------------------------------*/

/* To improve the quality of the LRU approximation we take a set of keys
 * that are good candidate for eviction across freeMemoryIfNeeded() calls.
 *
 * Entries inside the eviction pool are taken ordered by idle time, putting
 * greater idle times to the right (ascending order).
 *
 * When an LFU policy is used instead, a reverse frequency indication is used
 * instead of the idle time, so that we still evict by larger value (larger
 * inverse frequency means to evict keys with the least frequent accesses).
 *
 * Empty entries have the key pointer set to NULL. */
#define EVPOOL_SIZE 16
#define EVPOOL_CACHED_SDS_SIZE 255

struct evictionPoolEntry {

    /**
     * 该对象有多久没有被访问到
     */
    unsigned long long idle;    /* Object idle time (inverse frequency for LFU) */
    sds key;                    /* Key name. */
    sds cached;                 /* Cached SDS object for key name. */
    /**
     * 该key所在的db 也就是说所有db下的数据都存储在同一个pool下
     */
    int dbid;                   /* Key DB number. */
};

/**
 * 本文件的私有变量 不会暴露到外面
 */
static struct evictionPoolEntry *EvictionPoolLRU;

/* ----------------------------------------------------------------------------
 * Implementation of eviction, aging and LRU
 * --------------------------------------------------------------------------*/

/* Return the LRU clock, based on the clock resolution. This is a time
 * in a reduced-bits format that can be used to set and check the
 * object->lru field of redisObject structures.
 */
unsigned int getLRUClock(void) {
    return (mstime() / LRU_CLOCK_RESOLUTION) & LRU_CLOCK_MAX;
}

/* This function is used to obtain the current LRU clock.
 * If the current resolution is lower than the frequency we refresh the
 * LRU clock (as it should be in production servers) we return the
 * precomputed value, otherwise we need to resort to a system call. */
unsigned int LRU_CLOCK(void) {
    unsigned int lruclock;
    // 好像是hz低于某个值 就使用一个缓存值
    if (1000 / server.hz <= LRU_CLOCK_RESOLUTION) {
        lruclock = server.lruclock;
    } else {
        lruclock = getLRUClock();
    }
    return lruclock;
}

/* Given an object returns the min number of milliseconds the object was never
 * requested, using an approximated LRU algorithm.
 * 本次基于lru算法进行内存淘汰 这里是计算该对象的idle时间
 * */
unsigned long long estimateObjectIdleTime(robj *o) {
    unsigned long long lruclock = LRU_CLOCK();

    // lru记录的是一个时钟 需要通过*LRU_CLOCK_RESOLUTION 换算成真正的时间
    if (lruclock >= o->lru) {
        return (lruclock - o->lru) * LRU_CLOCK_RESOLUTION;
    } else {
        return (lruclock + (LRU_CLOCK_MAX - o->lru)) *
               LRU_CLOCK_RESOLUTION;
    }
}

/* freeMemoryIfNeeded() gets called when 'maxmemory' is set on the config
 * file to limit the max memory used by the server, before processing a
 * command.
 *
 * The goal of the function is to free enough memory to keep Redis under the
 * configured memory limit.
 *
 * The function starts calculating how many bytes should be freed to keep
 * Redis under the limit, and enters a loop selecting the best keys to
 * evict accordingly to the configured policy.
 *
 * If all the bytes needed to return back under the limit were freed the
 * function returns C_OK, otherwise C_ERR is returned, and the caller
 * should block the execution of commands that will result in more memory
 * used by the server.
 *
 * ------------------------------------------------------------------------
 *
 * LRU approximation algorithm
 *
 * Redis uses an approximation of the LRU algorithm that runs in constant
 * memory. Every time there is a key to expire, we sample N keys (with
 * N very small, usually in around 5) to populate a pool of best keys to
 * evict of M keys (the pool size is defined by EVPOOL_SIZE).
 *
 * The N keys sampled are added in the pool of good keys to expire (the one
 * with an old access time) if they are better than one of the current keys
 * in the pool.
 *
 * After the pool is populated, the best key we have in the pool is expired.
 * However note that we don't remove keys from the pool when they are deleted
 * so the pool may contain keys that no longer exist.
 *
 * When we try to evict a key, and all the entries in the pool don't exist
 * we populate it again. This time we'll be sure that the pool has at least
 * one key that can be evicted, if there is at least one key that can be
 * evicted in the whole database. */

/* Create a new eviction pool.
 * 创建一个用于维护所有key回收时间的pool对象
 * */
void evictionPoolAlloc(void) {
    struct evictionPoolEntry *ep;
    int j;

    // 池内默认存储16个poolEntry
    ep = zmalloc(sizeof(*ep) * EVPOOL_SIZE);
    // 这里为数组中每个元素填充属性
    for (j = 0; j < EVPOOL_SIZE; j++) {
        ep[j].idle = 0;
        ep[j].key = NULL;
        // 初始化sds结构 同时初始字符串为null
        ep[j].cached = sdsnewlen(NULL, EVPOOL_CACHED_SDS_SIZE);
        ep[j].dbid = 0;
    }
    EvictionPoolLRU = ep;
}

/* This is an helper function for freeMemoryIfNeeded(), it is used in order
 * to populate the evictionPool with a few entries every time we want to
 * expire a key. Keys with idle time smaller than one of the current
 * keys are added. Keys are always added if there are free entries.
 *
 * We insert keys on place in ascending order, so keys with the smaller
 * idle time are on the left, and keys with the higher idle time on the
 * right.
 * @param dbid 本次会从该db下淘汰redisObject
 * @param sampledict 本次处理的目标dict 可能是db.dict/db.expires  根据内存淘汰策略决定
 * @param keydict 就是db.dict
 * @param pool 本次可能会被淘汰的对象都会被包装成entry后存储到pool中
 * */
void evictionPoolPopulate(int dbid, dict *sampledict, dict *keydict, struct evictionPoolEntry *pool) {
    int j, k, count;

    // 根据最大的抽样数量 分配对应的内存空间
    dictEntry *samples[server.maxmemory_samples];

    // 这里随机抽取了一些key  并填充到samples中
    count = dictGetSomeKeys(sampledict, samples, server.maxmemory_samples);
    for (j = 0; j < count; j++) {
        unsigned long long idle;
        sds key;
        robj *o;
        dictEntry *de;

        // 获取该slot下随机采用的key
        de = samples[j];
        key = dictGetKey(de);

        /* If the dictionary we are sampling from is not the main
         * dictionary (but the expires one) we need to lookup the key
         * again in the key dictionary to obtain the value object.
         * 如果淘汰策略允许淘汰未过时的key 切换dict容器  是这样，基于超时时间存储的dict与普通dict的value不同 一个是long 一个是redisObject
         * 基于lru、lfu算法进行淘汰时都是使用redisObject的信息
         * */
        if (server.maxmemory_policy != MAXMEMORY_VOLATILE_TTL) {
            // 从db.dict中查找 key,redisObject
            if (sampledict != keydict) de = dictFind(keydict, key);
            o = dictGetVal(de);
        }

        /* Calculate the idle time according to the policy. This is called
         * idle just because the code initially handled LRU, but is in fact
         * just a score where an higher score means better candidate.
         * lru就是清理最久没有使用的key 这里获取该对象的idle时间
         * */
        if (server.maxmemory_policy & MAXMEMORY_FLAG_LRU) {
            idle = estimateObjectIdleTime(o);

            // 如果本次基于lfu算法 就是看某个对象在一段时间内的使用次数 这些因素会计算出一个数值 根据该数值来判断key的优先级
        } else if (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) {
            /* When we use an LRU policy, we sort the keys by idle time
             * so that we expire keys starting from greater idle time.
             * However when the policy is an LFU one, we have a frequency
             * estimation, and we want to evict keys with lower frequency
             * first. So inside the pool we put objects using the inverted
             * frequency subtracting the actual frequency to the maximum
             * frequency of 255.
             * 这时idle是一个类似分数的概念  计算公式比较复杂 这里就不展开了
             * */
            idle = 255 - LFUDecrAndReturn(o);
            // 根据该对象的过期时间计算idle
        } else if (server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL) {
            /* In this case the sooner the expire the better. */
            idle = ULLONG_MAX - (long) dictGetVal(de);
        } else {
            // 忽略其余淘汰策略
            serverPanic("Unknown eviction policy in evictionPoolPopulate()");
        }

        // 上面计算出了当前遍历到的样本的idle时间  idle越大代表淘汰该对象收益越高

        /* Insert the element inside the pool.
         * First, find the first empty bucket or the first populated
         * bucket that has an idle time smaller than our idle time.
         *
         * */
        k = 0;

        // 在server启动后 pool被初始化时内部每个元素的被提前填充 idle为0 且key默认为null
        // pool中的元素 idle是逐渐增大的
        // 这里是在定位到一个合适的下标   那么淘汰时也是选择淘汰最后一个(idle最大 收益最高)
        while (k < EVPOOL_SIZE &&
               pool[k].key &&
               pool[k].idle < idle)
            k++;

        // 代表此时idle小于pool中所有元素的idle 并且此时pool是满的 (移除的时候是从后往前移除 插入是从前往后)
        if (k == 0 && pool[EVPOOL_SIZE - 1].key != NULL) {
            /* Can't insert if the element is < the worst element we have
             * and there are no empty buckets. */
            continue;
            // 代表本次元素会填充到某个slot中
        } else if (k < EVPOOL_SIZE && pool[k].key == NULL) {
            /* Inserting into empty position. No setup needed before insert. */
        } else {
            // 代表需要替换掉某个元素

            /* Inserting in the middle. Now k points to the first element
             * greater than the element to insert.
             * 如果此时最后一个元素为空 必然会发生数据的迁移 也就是从插入的位置开始 将后面的数据全部往后移动一位
             * */
            if (pool[EVPOOL_SIZE - 1].key == NULL) {
                /* Free space on the right? Insert at k shifting
                 * all the elements from k to end to the right. */

                /* Save SDS before overwriting. */
                sds cached = pool[EVPOOL_SIZE - 1].cached;
                memmove(pool + k + 1, pool + k,
                        sizeof(pool[0]) * (EVPOOL_SIZE - k - 1));
                pool[k].cached = cached;
            } else {
                /* No free space on right? Insert at k-1 */
                // 这种情况代表pool已经满了 就会将插入位置之前的数据前移
                k--;
                /* Shift all elements on the left of k (included) to the
                 * left, so we discard the element with smaller idle time.*/
                sds cached = pool[0].cached; /* Save SDS before overwriting. */
                if (pool[0].key != pool[0].cached) sdsfree(pool[0].key);
                memmove(pool, pool + 1, sizeof(pool[0]) * k);
                pool[k].cached = cached;
            }
        }

        /* Try to reuse the cached SDS string allocated in the pool entry,
         * because allocating and deallocating this object is costly
         * (according to the profiler, not my fantasy. Remember:
         * premature optimization bla bla bla.
         * 由于缓存框架本身的特性 得分高的redisObject对应的key可能总是那几个  所以对key做了缓存
         * 这里有关pool[k].cached的作用看的不是很明白，但是对淘汰排名逻辑本身没有影响。
         * */
        int klen = sdslen(key);
        // 当key的长度没有超过最大缓存长度时 才会复用sds 否则会生成一个新的sds
        if (klen > EVPOOL_CACHED_SDS_SIZE) {
            pool[k].key = sdsdup(key);
        } else {
            // 将key拷贝到cache中
            memcpy(pool[k].cached, key, klen + 1);
            sdssetlen(pool[k].cached, klen);
            pool[k].key = pool[k].cached;
        }

        // 将会被淘汰的entry会设置到pool中
        pool[k].idle = idle;
        pool[k].dbid = dbid;
    }
}

/* ----------------------------------------------------------------------------
 * LFU (Least Frequently Used) implementation.

 * We have 24 total bits of space in each object in order to implement
 * an LFU (Least Frequently Used) eviction policy, since we re-use the
 * LRU field for this purpose.
 *
 * We split the 24 bits into two fields:
 *
 *          16 bits      8 bits
 *     +----------------+--------+
 *     + Last decr time | LOG_C  |
 *     +----------------+--------+
 *
 * LOG_C is a logarithmic counter that provides an indication of the access
 * frequency. However this field must also be decremented otherwise what used
 * to be a frequently accessed key in the past, will remain ranked like that
 * forever, while we want the algorithm to adapt to access pattern changes.
 *
 * So the remaining 16 bits are used in order to store the "decrement time",
 * a reduced-precision Unix time (we take 16 bits of the time converted
 * in minutes since we don't care about wrapping around) where the LOG_C
 * counter is halved if it has an high value, or just decremented if it
 * has a low value.
 *
 * New keys don't start at zero, in order to have the ability to collect
 * some accesses before being trashed away, so they start at COUNTER_INIT_VAL.
 * The logarithmic increment performed on LOG_C takes care of COUNTER_INIT_VAL
 * when incrementing the key, so that keys starting at COUNTER_INIT_VAL
 * (or having a smaller value) have a very high chance of being incremented
 * on access.
 *
 * During decrement, the value of the logarithmic counter is halved if
 * its current value is greater than two times the COUNTER_INIT_VAL, otherwise
 * it is just decremented by one.
 * --------------------------------------------------------------------------*/

/* Return the current time in minutes, just taking the least significant
 * 16 bits. The returned time is suitable to be stored as LDT (last decrement
 * time) for the LFU implementation. */
unsigned long LFUGetTimeInMinutes(void) {
    return (server.unixtime / 60) & 65535;
}

/* Given an object last access time, compute the minimum number of minutes
 * that elapsed since the last access. Handle overflow (ldt greater than
 * the current 16 bits minutes time) considering the time as wrapping
 * exactly once. */
unsigned long LFUTimeElapsed(unsigned long ldt) {
    unsigned long now = LFUGetTimeInMinutes();
    if (now >= ldt) return now - ldt;
    return 65535 - ldt + now;
}

/* Logarithmically increment a counter. The greater is the current counter value
 * the less likely is that it gets really implemented. Saturate it at 255.
 * 增加计数值
 * */
uint8_t LFULogIncr(uint8_t counter) {
    if (counter == 255) return 255;
    double r = (double) rand() / RAND_MAX;
    double baseval = counter - LFU_INIT_VAL;
    if (baseval < 0) baseval = 0;
    double p = 1.0 / (baseval * server.lfu_log_factor + 1);
    if (r < p) counter++;
    return counter;
}

/* If the object decrement time is reached decrement the LFU counter but
 * do not update LFU fields of the object, we update the access time
 * and counter in an explicit way when the object is really accessed.
 * And we will times halve the counter according to the times of
 * elapsed time than server.lfu_decay_time.
 * Return the object frequency counter.
 *
 * This function is used in order to scan the dataset for the best object
 * to fit: as we check for the candidate, we incrementally decrement the
 * counter of the scanned objects if needed.
 * 基于 LFU算法 计算某个value最近时间段内使用的次数  每个value对应的counter值会随着时间的流逝而减小
 * 同时每次key的命中会增加计数值 他们共同作用的结果决定了value的优先级 或者说该value是否会被淘汰
 * */
unsigned long LFUDecrAndReturn(robj *o) {
    // 前16位 代表时间范围
    unsigned long ldt = o->lru >> 8;
    // 后8位 代表此时的计数值
    unsigned long counter = o->lru & 255;
    // 计数值也是惰性计算的 现在通过衰减因子计算最新的计数值  具体的计算公式先不看
    unsigned long num_periods = server.lfu_decay_time ? LFUTimeElapsed(ldt) / server.lfu_decay_time : 0;

    // 代表衰减了一些数值 返回计算后最新的counter
    if (num_periods)
        counter = (num_periods > counter) ? 0 : counter - num_periods;
    return counter;
}

/* ----------------------------------------------------------------------------
 * The external API for eviction: freeMemoryIfNeeded() is called by the
 * server when there is data to add in order to make space if needed.
 * --------------------------------------------------------------------------*/

/* We don't want to count AOF buffers and slaves output buffers as
 * used memory: the eviction should use mostly data size. This function
 * returns the sum of AOF and slaves buffer.
 * 这里要排除掉aof以及slave占用的内存
 * */
size_t freeMemoryGetNotCountedMemory(void) {
    size_t overhead = 0;
    int slaves = listLength(server.slaves);

    if (slaves) {
        listIter li;
        listNode *ln;

        listRewind(server.slaves, &li);
        while ((ln = listNext(&li))) {
            client *slave = listNodeValue(ln);
            // 获取该slave占用的内存
            overhead += getClientOutputBufferMemoryUsage(slave);
        }
    }
    if (server.aof_state != AOF_OFF) {
        overhead += sdsalloc(server.aof_buf) + aofRewriteBufferSize();
    }
    return overhead;
}

/* Get the memory status from the point of view of the maxmemory directive:
 * if the memory used is under the maxmemory setting then C_OK is returned.
 * Otherwise, if we are over the memory limit, the function returns
 * C_ERR.
 *
 * The function may return additional info via reference, only if the
 * pointers to the respective arguments is not NULL. Certain fields are
 * populated only when C_ERR is returned:
 *
 *  'total'     total amount of bytes used.
 *              (Populated both for C_ERR and C_OK)
 *
 *  'logical'   the amount of memory used minus the slaves/AOF buffers.
 *              (Populated when C_ERR is returned)
 *
 *  'tofree'    the amount of memory that should be released
 *              in order to return back into the memory limits.
 *              (Populated when C_ERR is returned)
 *
 *  'level'     this usually ranges from 0 to 1, and reports the amount of
 *              memory currently used. May be > 1 if we are over the memory
 *              limit.
 *              (Populated both for C_ERR and C_OK)
 *              获取此时有关最大内存状态的信息
 *              @param total 此时占用的内存总大小
 *              @param logical 逻辑上占用的内存大小 就是排除了slave/aof的内存开销
 *              @param tofree 代表至少要释放多少内存
 */
int getMaxmemoryState(size_t *total, size_t *logical, size_t *tofree, float *level) {
    size_t mem_reported, mem_used, mem_tofree;

    /* Check if we are over the memory usage limit. If we are not, no need
     * to subtract the slaves output buffers. We can just return ASAP.
     * 获取此时使用的总内存
     * */
    mem_reported = zmalloc_used_memory();
    if (total) *total = mem_reported;

    /* We may return ASAP if there is no need to compute the level.
     * 如果没有设置内存上限 或者此时占用内存没有达到上线 就可以将此时获取到的情况返回
     * */
    int return_ok_asap = !server.maxmemory || mem_reported <= server.maxmemory;
    if (return_ok_asap && !level) return C_OK;

    /* Remove the size of slaves output buffers and AOF buffer from the
     * count of used memory. */
    mem_used = mem_reported;
    // 这里可以将aof和slave占用的内存忽略
    size_t overhead = freeMemoryGetNotCountedMemory();
    mem_used = (mem_used > overhead) ? mem_used - overhead : 0;

    /* Compute the ratio of memory usage.
     * TODO 先忽略level
     * */
    if (level) {
        if (!server.maxmemory) {
            *level = 0;
        } else {
            *level = (float) mem_used / (float) server.maxmemory;
        }
    }

    if (return_ok_asap) return C_OK;

    /* Check if we are still over the memory limit.
     * 代表还有空间可用
     * */
    if (mem_used <= server.maxmemory) return C_OK;

    /* Compute how much memory we need to free.
     * 计算需要释放多少内存
     * */
    mem_tofree = mem_used - server.maxmemory;

    if (logical) *logical = mem_used;
    if (tofree) *tofree = mem_tofree;

    return C_ERR;
}

/* This function is periodically called to see if there is memory to free
 * according to the current "maxmemory" settings. In case we are over the
 * memory limit, the function will try to free some memory to return back
 * under the limit.
 *
 * The function returns C_OK if we are under the memory limit or if we
 * were over the limit, but the attempt to free memory was successful.
 * Otherwise if we are over the memory limit, but not enough memory
 * was freed to return back under the limit, the function returns C_ERR.
 * 判断是否要释放内存 以便执行之后的command
 * */
int freeMemoryIfNeeded(void) {
    int keys_freed = 0;
    /* By default replicas should ignore maxmemory
     * and just be masters exact copies.
     * 当前节点是副本 且设置了不需要考虑slave的内存情况 也就是内存控制任务完全放在master节点 其他节点只要确保同步正常就可以确保内存在合理范围之内
     * */
    if (server.masterhost && server.repl_slave_ignore_maxmemory) return C_OK;

    size_t mem_reported, mem_tofree, mem_freed;
    mstime_t latency, eviction_latency, lazyfree_latency;
    long long delta;
    int slaves = listLength(server.slaves);
    int result = C_ERR;

    /* When clients are paused the dataset should be static not just from the
     * POV of clients not being able to write, but also from the POV of
     * expires and evictions of keys not being performed.
     * 如果此时client都被暂停 此时不会发生新的写入操作 就认为内存是够用的
     * TODO 发现在集群模块中会将某个client暂停 先搁置
     * */
    if (clientsArePaused()) return C_OK;

    // 如果此时mem_reported < server.maxMem 就代表有足够的空间
    // mem_reported 是此时检测已经使用的内存
    // mem_tofree 是要释放的内存
    if (getMaxmemoryState(&mem_reported, NULL, &mem_tofree, NULL) == C_OK)
        return C_OK;

    // 记录已经释放的内存空间
    mem_freed = 0;

    // 此时已经确定要释放内存了
    latencyStartMonitor(latency);
    // 这里就涉及到内存淘汰策略了  如果执行了不允许进行内存淘汰 跳转到cant_free
    if (server.maxmemory_policy == MAXMEMORY_NO_EVICTION)
        goto cant_free; /* We need to free memory, but policy forbids. */


    while (mem_freed < mem_tofree) {
        int j, k, i;

        // 这里要挨个遍历所有db 并尝试内存清理
        static unsigned int next_db = 0;
        sds bestkey = NULL;
        int bestdbid;
        redisDb *db;
        dict *dict;
        dictEntry *de;

        // 当采用lru/lfu 或者 volatile_ttl的内存淘汰策略时
        if (server.maxmemory_policy & (MAXMEMORY_FLAG_LRU | MAXMEMORY_FLAG_LFU) ||
            server.maxmemory_policy == MAXMEMORY_VOLATILE_TTL) {
            // 这个链表按照一个得分对所有需要淘汰的对象进行排序 每次只会淘汰那个得分最高的对象
            struct evictionPoolEntry *pool = EvictionPoolLRU;

            // 通过下面的算法决定哪个key被淘汰的优先级最高
            while (bestkey == NULL) {

                // total_keys 总计会扫描到多少个key
                unsigned long total_keys = 0, keys;

                /* We don't want to make local-db choices when expiring keys,
                 * so to start populate the eviction pool sampling keys from
                 * every DB.
                 * 扫描所有db之后(抽样) 得到评分最低的16个key
                 * */
                for (i = 0; i < server.dbnum; i++) {
                    db = server.db + i;
                    // 根据此时的内存淘汰策略 选择仅从设置了超时时间的key中淘汰对象 还是db下全key范围淘汰对象
                    dict = (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) ?
                           db->dict : db->expires;
                    if ((keys = dictSize(dict)) != 0) {
                        // 抽样计算得分并填充到evictionPool中
                        evictionPoolPopulate(i, dict, db->dict, pool);
                        total_keys += keys;
                    }
                }

                // 代表此时db下没有任何key/或者在expires中没有任何key 也就无法淘汰数据
                if (!total_keys) break; /* No keys to evict. */

                /* Go backward from best to worst element to evict.
                 * 从后往前看,因为在pool中idle得分越高的在越后面
                 * */
                for (k = EVPOOL_SIZE - 1; k >= 0; k--) {
                    if (pool[k].key == NULL) continue;
                    bestdbid = pool[k].dbid;

                    // 回查之前存入的数据
                    if (server.maxmemory_policy & MAXMEMORY_FLAG_ALLKEYS) {
                        de = dictFind(server.db[pool[k].dbid].dict,
                                      pool[k].key);
                    } else {
                        de = dictFind(server.db[pool[k].dbid].expires,
                                      pool[k].key);
                    }

                    /* Remove the entry from the pool. */
                    if (pool[k].key != pool[k].cached)
                        sdsfree(pool[k].key);
                    pool[k].key = NULL;
                    pool[k].idle = 0;

                    /* If the key exists, is our pick. Otherwise it is
                     * a ghost and we need to try the next element. */
                    if (de) {
                        bestkey = dictGetKey(de);
                        break;
                    } else {
                        /* Ghost... Iterate again. */
                    }
                }
            }
        }

            /* volatile-random and allkeys-random policy
             * 这里选择的是随机淘汰策略 上面的话扫描n个db后仅选出了一个key效率会不会太低？
             * 这里随机选出一个key后就可以直接返回了
             * */
        else if (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM ||
                 server.maxmemory_policy == MAXMEMORY_VOLATILE_RANDOM) {
            /* When evicting a random key, we try to evict a key for
             * each DB, so we use the static 'next_db' variable to
             * incrementally visit all DBs. */
            for (i = 0; i < server.dbnum; i++) {
                j = (++next_db) % server.dbnum;
                db = server.db + j;
                dict = (server.maxmemory_policy == MAXMEMORY_ALLKEYS_RANDOM) ?
                       db->dict : db->expires;
                if (dictSize(dict) != 0) {
                    de = dictGetRandomKey(dict);
                    bestkey = dictGetKey(de);
                    bestdbid = j;
                    break;
                }
            }
        }

        /* Finally remove the selected key.
         * 代表本轮找到了合适的key
         * */
        if (bestkey) {
            db = server.db + bestdbid;
            robj *keyobj = createStringObject(bestkey, sdslen(bestkey));

            // 因为该key将被强制淘汰 需要将信息同步到aof/副本上
            propagateExpire(db, keyobj, server.lazyfree_lazy_eviction);
            /* We compute the amount of memory freed by db*Delete() alone.
             * It is possible that actually the memory needed to propagate
             * the DEL in AOF and replication link is greater than the one
             * we are freeing removing the key, but we can't account for
             * that otherwise we would never exit the loop.
             *
             * Same for CSC invalidation messages generated by signalModifiedKey.
             *
             * AOF and Output buffer memory will be freed eventually so
             * we only care about memory used by the key space. */
            delta = (long long) zmalloc_used_memory();
            latencyStartMonitor(eviction_latency);

            // 判断是否采用惰性释放的方式删除对象
            // 无论是同步还是异步都会立即从db中删除这个key  但是内存的释放可能会延后(调用dbAsyncDelete不一定就会通过bio进行释放 还要看本次要释放的redisObject大小
            // 只有对象比较大的时候 回收耗时长 才会通过bio回收任务)
            if (server.lazyfree_lazy_eviction)
                dbAsyncDelete(db, keyobj);
            else
                dbSyncDelete(db, keyobj);
            latencyEndMonitor(eviction_latency);
            latencyAddSampleIfNeeded("eviction-del", eviction_latency);
            delta -= (long long) zmalloc_used_memory();
            // 计算此时已经被释放掉的内存 如果对象被惰性删除了 实际上这里内存还不会立即释放 还会继续抽样
            mem_freed += delta;
            server.stat_evictedkeys++;
            signalModifiedKey(NULL, db, keyobj);

            // 发出一个淘汰事件
            notifyKeyspaceEvent(NOTIFY_EVICTED, "evicted",
                                keyobj, db->id);
            decrRefCount(keyobj);
            keys_freed++;

            /* When the memory to free starts to be big enough, we may
             * start spending so much time here that is impossible to
             * deliver data to the slaves fast enough, so we force the
             * transmission here inside the loop.
             * 提前将缓冲区的数据发送出去 主要是同步副本的删除命令 避免副本没有足够的空间执行之后的命令
             * */
            if (slaves) flushSlavesOutputBuffers();

            /* Normally our stop condition is the ability to release
             * a fixed, pre-computed amount of memory. However when we
             * are deleting objects in another thread, it's better to
             * check, from time to time, if we already reached our target
             * memory, since the "mem_freed" amount is computed only
             * across the dbAsyncDelete() call, while the thread can
             * release the memory all the time.
             * 如果采用的是惰性删除，内存的释放是异步的，那么每隔16轮主动去重新检测内存是否足够
             * */
            if (server.lazyfree_lazy_eviction && !(keys_freed % 16)) {
                if (getMaxmemoryState(NULL, NULL, NULL, NULL) == C_OK) {
                    /* Let's satisfy our stop condition. */
                    mem_freed = mem_tofree;
                }
            }
        } else {
            // 代表本轮没有找到任何可以释放的key
            goto cant_free; /* nothing to free... */
        }
    }
    result = C_OK;

    cant_free:
    /* We are here if we are not able to reclaim memory. There is only one
     * last thing we can try: check if the lazyfree thread has jobs in queue
     * and wait...
     * 代表本次没有释放足够的空间
     * */
    if (result != C_OK) {
        latencyStartMonitor(lazyfree_latency);
        // 阻塞等待后台任务处理完所有的内存释放任务 如果中途有足够的内存了 就可以返回ok 如果内存还是不足 且没有释放内存的任务了 本次就没有足够的内存空间
        while (bioPendingJobsOfType(BIO_LAZY_FREE)) {
            if (getMaxmemoryState(NULL, NULL, NULL, NULL) == C_OK) {
                result = C_OK;
                break;
            }
            // 这是沉睡1000毫秒
            usleep(1000);
        }
        latencyEndMonitor(lazyfree_latency);
        latencyAddSampleIfNeeded("eviction-lazyfree", lazyfree_latency);
    }
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("eviction-cycle", latency);
    return result;
}

/* This is a wrapper for freeMemoryIfNeeded() that only really calls the
 * function if right now there are the conditions to do so safely:
 *
 * - There must be no script in timeout condition.
 * - Nor we are loading data right now.
 * 检测此时是否有足够的内存来执行命令
 */
int freeMemoryIfNeededAndSafe(void) {
    // 如果此时服务器处于数据恢复阶段 内存必然是够用的
    if (server.lua_timedout || server.loading) return C_OK;
    // 判断此时是否有足够的内存空间 并按照内存淘汰策略进行内存释放
    return freeMemoryIfNeeded();
}
