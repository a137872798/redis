/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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

/*-----------------------------------------------------------------------------
 * Sorted set API
 *----------------------------------------------------------------------------*/

/* ZSETs are ordered sets using two data structures to hold the same elements
 * in order to get O(log(N)) INSERT and REMOVE operations into a sorted
 * data structure.
 *
 * The elements are added to a hash table mapping Redis objects to scores.
 * At the same time the elements are added to a skip list mapping scores
 * to Redis objects (so objects are sorted by scores in this "view").
 *
 * Note that the SDS string representing the element is the same in both
 * the hash table and skiplist in order to save memory. What we do in order
 * to manage the shared SDS string more easily is to free the SDS string
 * only in zslFreeNode(). The dictionary has no value free method set.
 * So we should always remove an element from the dictionary, and later from
 * the skiplist.
 *
 * This skiplist implementation is almost a C translation of the original
 * algorithm described by William Pugh in "Skip Lists: A Probabilistic
 * Alternative to Balanced Trees", modified in three ways:
 * a) this implementation allows for repeated scores.
 * b) the comparison is not just by key (our 'score') but by satellite data.
 * c) there is a back pointer, so it's a doubly linked list with the back
 * pointers being only at "level 1". This allows to traverse the list
 * from tail to head, useful for ZREVRANGE. */

#include "server.h"
#include <math.h>

/*-----------------------------------------------------------------------------
 * Skiplist implementation of the low level API
 * zset 代表有序set 它的实现是基于跳跃表
 *----------------------------------------------------------------------------*/

int zslLexValueGteMin(sds value, zlexrangespec *spec);

int zslLexValueLteMax(sds value, zlexrangespec *spec);

/* Create a skiplist node with the specified number of levels.
 * The SDS string 'ele' is referenced by the node after the call.
 * @param level 本次要创建的节点的level
 * */
zskiplistNode *zslCreateNode(int level, double score, sds ele) {
    // level*sizeof(struct zskiplistLevel)  对应node中柔性数组的部分
    // zn + level*sizeof(struct zskiplistLevel) 代表一开始申请一个头节点 并且开辟了32个level结构的大小
    zskiplistNode *zn =
            zmalloc(sizeof(*zn) + level * sizeof(struct zskiplistLevel));
    zn->score = score;
    zn->ele = ele;
    return zn;
}

/* Create a new skiplist.
 * 初始化一个新的跳跃表
 * */
zskiplist *zslCreate(void) {
    int j;
    zskiplist *zsl;

    zsl = zmalloc(sizeof(*zsl));
    // 对于zskiplist level代表当下所有节点中最高的level (header节点不算 且header的默认level为32)
    zsl->level = 1;
    // 此时节点总数 header不算
    zsl->length = 0;

    // 初始化时 score为0 关联的数据为null
    zsl->header = zslCreateNode(ZSKIPLIST_MAXLEVEL, 0, NULL);
    // 初始化头节点的level数组
    for (j = 0; j < ZSKIPLIST_MAXLEVEL; j++) {
        // 每一层的第一个元素 就是level.forward
        zsl->header->level[j].forward = NULL;
        zsl->header->level[j].span = 0;
    }
    zsl->header->backward = NULL;
    zsl->tail = NULL;
    return zsl;
}

/* Free the specified skiplist node. The referenced SDS string representation
 * of the element is freed too, unless node->ele is set to NULL before calling
 * this function. */
void zslFreeNode(zskiplistNode *node) {
    sdsfree(node->ele);
    zfree(node);
}

/* Free a whole skiplist. */
void zslFree(zskiplist *zsl) {
    // 通过header节点维系的链表结构,遍历所有节点
    zskiplistNode *node = zsl->header->level[0].forward, *next;

    zfree(zsl->header);
    while (node) {
        next = node->level[0].forward;
        zslFreeNode(node);
        node = next;
    }
    zfree(zsl);
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and ZSKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned.
 * 这里就是skiplist抛硬币的地方  决定插入的新元素会生成几级level作为索引
 * */
int zslRandomLevel(void) {
    int level = 1;
    while ((random() & 0xFFFF) < (ZSKIPLIST_P * 0xFFFF))
        level += 1;
    return (level < ZSKIPLIST_MAXLEVEL) ? level : ZSKIPLIST_MAXLEVEL;
}

/* Insert a new node in the skiplist. Assumes the element does not already
 * exist (up to the caller to enforce that). The skiplist takes ownership
 * of the passed SDS string 'ele'.
 * 插入逻辑
 * */
zskiplistNode *zslInsert(zskiplist *zsl, double score, sds ele) {

    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned int rank[ZSKIPLIST_MAXLEVEL];
    int i, level;

    serverAssert(!isnan(score));
    x = zsl->header;
    // 这里就是扫描跳表索引的过程 从header节点的最高层level开始
    for (i = zsl->level - 1; i >= 0; i--) {
        /* store rank that is crossed to reach the insert position
         * rank中每个元素挨个存储的是每层的跨度 并且下个槽中的值是以上个槽的值作为起点
         * */
        rank[i] = i == (zsl->level - 1) ? 0 : rank[i + 1];
        // 因为排序是通过score  这里通过比较score的大小来进行跳跃 快速定位要插入的位置
        while (x->level[i].forward &&
               // 本次检测的索引对应的score小于本次要插入的位置 继续寻找下个索引
               (x->level[i].forward->score < score ||
                // 当score相同的情况 检查ele
                (x->level[i].forward->score == score &&
                 sdscmp(x->level[i].forward->ele, ele) < 0))) {
            // 此时没有跳转 就将跨度累加   之后会用rank中的值反推新插入的节点的span值
            rank[i] += x->level[i].span;
            // 找到同level的下个索引
            x = x->level[i].forward;
        }
        // 记录此时每层level将会被更新的节点
        update[i] = x;
    }
    /* we assume the element is not already inside, since we allow duplicated
     * scores, reinserting the same element should never happen since the
     * caller of zslInsert() should test in the hash table if the element is
     * already inside or not.
     * 生成一个随机的level 本次新插入的节点将会按照这个level来生成索引
     * */
    level = zslRandomLevel();

    // 代表本次新追加的level 会超过之前所有非header node的level
    if (level > zsl->level) {
        // 针对这些level也会生成一些新的node
        for (i = zsl->level; i < level; i++) {
            rank[i] = 0;
            update[i] = zsl->header;
            // 这些节点初始span 就是节点总数(也就是从当前节点开始到尾节点跨度是多少)
            update[i]->level[i].span = zsl->length;
        }
        zsl->level = level;
    }
    // 基于这个随机的level来创建节点
    x = zslCreateNode(level, score, ele);
    // 将之前update中的node链接到这个新的node上
    for (i = 0; i < level; i++) {
        // x 会插入到update与同层下个节点之间
        // 链表的标准插入
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;

        /* update span covered by update[i] as x is inserted here
         * 假设此时i为0 x的span变成了原节点的span   而原节点span变成了1
         * 从这里可以看出span本身代表从本节点开始到同一level下一节点的跨度(当i=0的时候在最低层 2个相邻节点的跨度必然是1 刚好符合这种情况) 新插入的节点继承了原节点到下个相邻节点的跨度
         *
         * 当i不为0的时候  update距离新插入的节点的跨度就是 (rank[0] - rank[i])+1 前半部分代表上下层的跨度差 后半部分因为增加了一个节点 所以跨度要加1  剩余部分就是update的新跨度
         * 因为每层update[i].span都是不确定的 唯一能确定的就是上下层的水平跨度差(通过累加下层符合条件的node.span) 所以通过rank数组进行记录
         * */
        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }

    /* increment span for untouched levels
     * 从 level往上 到达zsl->level之间可以认为没有插入新的节点 但是底部由于插入了新的node(用于填充新数据) 所以整个跨度必然要增加 这里就是+1
     * 将每一级要更新的node的span+1
     * */
    for (i = level; i < zsl->level; i++) {
        update[i]->level[i].span++;
    }

    // 每个节点只有在最下层会维护 backward链表 forward可以在任何高度
    x->backward = (update[0] == zsl->header) ? NULL : update[0];
    // 更新backward链表
    if (x->level[0].forward)
        x->level[0].forward->backward = x;
    else
        // 代表本次插入的节点成为了一个新的尾节点
        zsl->tail = x;
    zsl->length++;
    return x;
}

/* Internal function used by zslDelete, zslDeleteRangeByScore and
 * zslDeleteRangeByRank.
 * 从跳跃表中删除某个节点
 * @param x 本次需要被删除的节点
 * @param update 代表本次删除操作会影响到的所有节点  对应insert时找到的update[]
 * */
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update) {
    int i;
    // 这里是维护链表指针以及更新跨度信息
    for (i = 0; i < zsl->level; i++) {
        // 因为下个节点会被移除 需要更新span 以及forward指针
        if (update[i]->level[i].forward == x) {
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;
        } else {
            update[i]->level[i].span -= 1;
        }
    }
    // 更新backward指针
    if (x->level[0].forward) {
        x->level[0].forward->backward = x->backward;
    } else {
        zsl->tail = x->backward;
    }
    // 检测总高度发生变化
    while (zsl->level > 1 && zsl->header->level[zsl->level - 1].forward == NULL)
        zsl->level--;
    zsl->length--;
}

/* Delete an element with matching score/element from the skiplist.
 * The function returns 1 if the node was found and deleted, otherwise
 * 0 is returned.
 *
 * If 'node' is NULL the deleted node is freed by zslFreeNode(), otherwise
 * it is not freed (but just unlinked) and *node is set to the node pointer,
 * so that it is possible for the caller to reuse the node (including the
 * referenced SDS string at node->ele).
 * 按照score/ele从跳表中删除某个节点
 * @param node 指向本次要删除的节点
 * */
int zslDelete(zskiplist *zsl, double score, sds ele, zskiplistNode **node) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        // 找到删除会影响到的所有节点
        while (x->level[i].forward &&
               (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                 sdscmp(x->level[i].forward->ele, ele) < 0))) {
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    /* We may have multiple elements with the same score, what we need
     * is to find the element with both the right score and object.
     * 此时x停留在最靠近score的位置 继续获取下一个元素
     * */
    x = x->level[0].forward;
    // 检测当前x对应的score,ele 与目标节点数据是否匹配
    if (x && score == x->score && sdscmp(x->ele, ele) == 0) {
        // 代表节点存在 进行删除操作
        zslDeleteNode(zsl, x, update);
        if (!node)
            zslFreeNode(x);
        else
            *node = x;
        return 1;
    }
    return 0; /* not found */
}

/* Update the score of an element inside the sorted set skiplist.
 * Note that the element must exist and must match 'score'.
 * This function does not update the score in the hash table side, the
 * caller should take care of it.
 *
 * Note that this function attempts to just update the node, in case after
 * the score update, the node would be exactly at the same position.
 * Otherwise the skiplist is modified by removing and re-adding a new
 * element, which is more costly.
 *
 * The function returns the updated element skiplist node pointer.
 * 通过当前score找到某个节点 并更新score值
 * */
zskiplistNode *zslUpdateScore(zskiplist *zsl, double curscore, sds ele, double newscore) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    /* We need to seek to element to update to start: this is useful anyway,
     * we'll have to update or remove it. */
    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward &&
               (x->level[i].forward->score < curscore ||
                (x->level[i].forward->score == curscore &&
                 sdscmp(x->level[i].forward->ele, ele) < 0))) {
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    /* Jump to our element: note that this function assumes that the
     * element with the matching score exists.
     * 先定位x
     * */
    x = x->level[0].forward;
    serverAssert(x && curscore == x->score && sdscmp(x->ele, ele) == 0);

    /* If the node, after the score update, would be still exactly
     * at the same position, we can just update the score without
     * actually removing and re-inserting the element in the skiplist.
     * 如果当前节点是尾节点 并且score是增加的 这时情况最简单 直接更新score就可以
     * */
    if ((x->backward == NULL || x->backward->score < newscore) &&
        (x->level[0].forward == NULL || x->level[0].forward->score > newscore)) {
        x->score = newscore;
        return x;
    }

    /* No way to reuse the old node: we need to remove and insert a new
     * one at a different place.
     * 通过先删除后插入的方式维护跳表之前的结构
     * */
    zslDeleteNode(zsl, x, update);
    zskiplistNode *newnode = zslInsert(zsl, newscore, x->ele);
    /* We reused the old node x->ele SDS string, free the node now
     * since zslInsert created a new one. */
    x->ele = NULL;
    zslFreeNode(x);
    return newnode;
}

/**
 * 检测某个值是否大于范围规定的最小值
 * @param value
 * @param spec 描述一个范围信息  min/max 以及是否包含min/max
 * @return
 */
int zslValueGteMin(double value, zrangespec *spec) {
    return spec->minex ? (value > spec->min) : (value >= spec->min);
}

/**
 * 检测某个值是否小于范围规定的最大值
 * @param value
 * @param spec
 * @return
 */
int zslValueLteMax(double value, zrangespec *spec) {
    return spec->maxex ? (value < spec->max) : (value <= spec->max);
}

/* Returns if there is a part of the zset is in range.
 * 判断zset中是否有元素在range内  就是通过检测首尾元素
 * */
int zslIsInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    if (range->min > range->max ||
        (range->min == range->max && (range->minex || range->maxex)))
        return 0;
    x = zsl->tail;
    if (x == NULL || !zslValueGteMin(x->score, range))
        return 0;
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslValueLteMax(x->score, range))
        return 0;
    return 1;
}

/* Find the first node that is contained in the specified range.
 * Returns NULL when no element is contained in the range.
 * 找到范围内的第一个元素
 * */
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early.
     * zset与range没有交集 返回null
     * */
    if (!zslIsInRange(zsl, range)) return NULL;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        /* Go forward while *OUT* of range.
         * 找到第一个值超过min的节点
         * */
        while (x->level[i].forward &&
               !zslValueGteMin(x->level[i].forward->score, range))
            x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    x = x->level[0].forward;
    serverAssert(x != NULL);

    /* Check if score <= max.
     * 如果该值超过了max 代表没有元素在range中
     * */
    if (!zslValueLteMax(x->score, range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!zslIsInRange(zsl, range)) return NULL;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        while (x->level[i].forward &&
               zslValueLteMax(x->level[i].forward->score, range))
            x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    serverAssert(x != NULL);

    /* Check if score >= min. */
    if (!zslValueGteMin(x->score, range)) return NULL;
    return x;
}

/* Delete all the elements with score between min and max from the skiplist.
 * Min and max are inclusive, so a score >= min || score <= max is deleted.
 * Note that this function takes the reference to the hash table view of the
 * sorted set, in order to remove the elements from the hash table too.
 * 删除score在某个范围内的节点
 * @param dict 在zset中删除的元素 也会从dict中移除
 * */
unsigned long zslDeleteRangeByScore(zskiplist *zsl, zrangespec *range, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && (range->minex ?
                                       x->level[i].forward->score <= range->min :
                                       x->level[i].forward->score < range->min))
            x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min.
     * 首个超过min的节点*/
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    while (x &&
           (range->maxex ? x->score < range->max : x->score <= range->max)) {
        // 遍历level[0]维护的链表 将score在范围内的节点全部移除
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl, x, update);
        // 将节点关联的ele 作为key 去字典中删除匹配的元素
        dictDelete(dict, x->ele);
        zslFreeNode(x); /* Here is where x->ele is actually released. */
        removed++;
        x = next;
    }
    return removed;
}

/**
 * 基于zset的sds进行删除
 */
unsigned long zslDeleteRangeByLex(zskiplist *zsl, zlexrangespec *range, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;


    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward &&
               !zslLexValueGteMin(x->level[i].forward->ele, range))
            x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    while (x && zslLexValueLteMax(x->ele, range)) {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl, x, update);
        dictDelete(dict, x->ele);
        zslFreeNode(x); /* Here is where x->ele is actually released. */
        removed++;
        x = next;
    }
    return removed;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based
 * 基于偏移量删除元素
 * */
unsigned long zslDeleteRangeByRank(zskiplist *zsl, unsigned int start, unsigned int end, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long traversed = 0, removed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        // 通过traversed 来记录偏移量(累加span)
        while (x->level[i].forward && (traversed + x->level[i].span) < start) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    traversed++;
    x = x->level[0].forward;
    while (x && traversed <= end) {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl, x, update);
        dictDelete(dict, x->ele);
        zslFreeNode(x);
        removed++;
        traversed++;
        x = next;
    }
    return removed;
}

/* Find the rank for an element by both score and key.
 * Returns 0 when the element cannot be found, rank otherwise.
 * Note that the rank is 1-based due to the span of zsl->header to the
 * first element.
 * 通过score和ele定位到某个节点后返回rank值(或者说偏移量)
 * */
unsigned long zslGetRank(zskiplist *zsl, double score, sds ele) {
    zskiplistNode *x;
    unsigned long rank = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward &&
               (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                 sdscmp(x->level[i].forward->ele, ele) <= 0))) {
            rank += x->level[i].span;
            x = x->level[i].forward;
        }

        /* x might be equal to zsl->header, so test if obj is non-NULL
         * 只有ele对应的值匹配时 才会返回偏移量
         * */
        if (x->ele && sdscmp(x->ele, ele) == 0) {
            return rank;
        }
    }
    return 0;
}

/* Finds an element by its rank. The rank argument needs to be 1-based.
 * 通过偏移量定位某个node
 * */
zskiplistNode *zslGetElementByRank(zskiplist *zsl, unsigned long rank) {
    zskiplistNode *x;
    unsigned long traversed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) <= rank) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        if (traversed == rank) {
            return x;
        }
    }
    return NULL;
}

/* Populate the rangespec according to the objects min and max.
 * 使用min/max指针指向的数据 来填充spec
 * */
static int zslParseRange(robj *min, robj *max, zrangespec *spec) {
    char *eptr;
    spec->minex = spec->maxex = 0;

    /* Parse the min-max interval. If one of the values is prefixed
     * by the "(" character, it's considered "open". For instance
     * ZRANGEBYSCORE zset (1.5 (2.5 will match min < x < max
     * ZRANGEBYSCORE zset 1.5 2.5 will instead match min <= x <= max */
    if (min->encoding == OBJ_ENCODING_INT) {
        spec->min = (long) min->ptr;
    } else {
        if (((char *) min->ptr)[0] == '(') {
            spec->min = strtod((char *) min->ptr + 1, &eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return C_ERR;
            spec->minex = 1;
        } else {
            spec->min = strtod((char *) min->ptr, &eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return C_ERR;
        }
    }
    if (max->encoding == OBJ_ENCODING_INT) {
        spec->max = (long) max->ptr;
    } else {
        if (((char *) max->ptr)[0] == '(') {
            spec->max = strtod((char *) max->ptr + 1, &eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return C_ERR;
            spec->maxex = 1;
        } else {
            spec->max = strtod((char *) max->ptr, &eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return C_ERR;
        }
    }

    return C_OK;
}

/* ------------------------ Lexicographic ranges ---------------------------- */

/* Parse max or min argument of ZRANGEBYLEX.
  * (foo means foo (open interval)
  * [foo means foo (closed interval)
  * - means the min string possible
  * + means the max string possible
  *
  * If the string is valid the *dest pointer is set to the redis object
  * that will be used for the comparison, and ex will be set to 0 or 1
  * respectively if the item is exclusive or inclusive. C_OK will be
  * returned.
  *
  * If the string is not a valid range C_ERR is returned, and the value
  * of *dest and *ex is undefined.
  * 从item中解析出范围信息 并生成格式化字符串设置到dest
  * */
int zslParseLexRangeItem(robj *item, sds *dest, int *ex) {
    char *c = item->ptr;

    switch (c[0]) {
        // +/- 代表 max/min
        // (/[ 代表包括不包括
        case '+':
            if (c[1] != '\0') return C_ERR;
            *ex = 1;
            *dest = shared.maxstring;
            return C_OK;
        case '-':
            if (c[1] != '\0') return C_ERR;
            *ex = 1;
            *dest = shared.minstring;
            return C_OK;
        case '(':
            *ex = 1;
            *dest = sdsnewlen(c + 1, sdslen(c) - 1);
            return C_OK;
        case '[':
            *ex = 0;
            *dest = sdsnewlen(c + 1, sdslen(c) - 1);
            return C_OK;
        default:
            return C_ERR;
    }
}

/* Free a lex range structure, must be called only after zelParseLexRange()
 * populated the structure with success (C_OK returned). */
void zslFreeLexRange(zlexrangespec *spec) {
    if (spec->min != shared.minstring &&
        spec->min != shared.maxstring)
        sdsfree(spec->min);
    if (spec->max != shared.minstring &&
        spec->max != shared.maxstring)
        sdsfree(spec->max);
}

/* Populate the lex rangespec according to the objects min and max.
 *
 * Return C_OK on success. On error C_ERR is returned.
 * When OK is returned the structure must be freed with zslFreeLexRange(),
 * otherwise no release is needed.
 * 从min/max中解析出范围信息 并设置到spec中
 * */
int zslParseLexRange(robj *min, robj *max, zlexrangespec *spec) {
    /* The range can't be valid if objects are integer encoded.
     * Every item must start with ( or [. */
    if (min->encoding == OBJ_ENCODING_INT ||
        max->encoding == OBJ_ENCODING_INT)
        return C_ERR;

    spec->min = spec->max = NULL;
    if (zslParseLexRangeItem(min, &spec->min, &spec->minex) == C_ERR ||
        zslParseLexRangeItem(max, &spec->max, &spec->maxex) == C_ERR) {
        zslFreeLexRange(spec);
        return C_ERR;
    } else {
        return C_OK;
    }
}

/* This is just a wrapper to sdscmp() that is able to
 * handle shared.minstring and shared.maxstring as the equivalent of
 * -inf and +inf for strings */
int sdscmplex(sds a, sds b) {
    if (a == b) return 0;
    if (a == shared.minstring || b == shared.maxstring) return -1;
    if (a == shared.maxstring || b == shared.minstring) return 1;
    return sdscmp(a, b);
}

int zslLexValueGteMin(sds value, zlexrangespec *spec) {
    return spec->minex ?
           (sdscmplex(value, spec->min) > 0) :
           (sdscmplex(value, spec->min) >= 0);
}

int zslLexValueLteMax(sds value, zlexrangespec *spec) {
    return spec->maxex ?
           (sdscmplex(value, spec->max) < 0) :
           (sdscmplex(value, spec->max) <= 0);
}

/* Returns if there is a part of the zset is in the lex range.
 * 检测跳表中是否有在范围内的数据
 * */
int zslIsInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    int cmp = sdscmplex(range->min, range->max);
    if (cmp > 0 || (cmp == 0 && (range->minex || range->maxex)))
        return 0;
    x = zsl->tail;
    if (x == NULL || !zslLexValueGteMin(x->ele, range))
        return 0;
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslLexValueLteMax(x->ele, range))
        return 0;
    return 1;
}

/* Find the first node that is contained in the specified lex range.
 * Returns NULL when no element is contained in the range.
 * 找到第一个在范围内的节点
 * */
zskiplistNode *zslFirstInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!zslIsInLexRange(zsl, range)) return NULL;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        while (x->level[i].forward &&
               !zslLexValueGteMin(x->level[i].forward->ele, range))
            x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    x = x->level[0].forward;
    serverAssert(x != NULL);

    /* Check if score <= max. */
    if (!zslLexValueLteMax(x->ele, range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
zskiplistNode *zslLastInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    if (!zslIsInLexRange(zsl, range)) return NULL;

    x = zsl->header;
    for (i = zsl->level - 1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        while (x->level[i].forward &&
               zslLexValueLteMax(x->level[i].forward->ele, range))
            x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    serverAssert(x != NULL);

    /* Check if score >= min. */
    if (!zslLexValueGteMin(x->ele, range)) return NULL;
    return x;
}

/*-----------------------------------------------------------------------------
 * Ziplist-backed sorted set API   下面是基于ziplist实现的zset支持的api   因为ziplist本身不支持排序 所以只有在数据比较少的时候 使用ziplist实现zset
 *----------------------------------------------------------------------------*/

double zzlGetScore(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    char buf[128];
    double score;

    serverAssert(sptr != NULL);
    // 从sptr指针的位置开始读取下一个entry的数据 长度
    serverAssert(ziplistGet(sptr, &vstr, &vlen, &vlong));

    // 将获取的值转换成score后返回
    if (vstr) {
        memcpy(buf, vstr, vlen);
        buf[vlen] = '\0';
        score = strtod(buf, NULL);
    } else {
        score = vlong;
    }

    return score;
}

/* Return a ziplist element as an SDS string.
 * 读取ziplist的entry 并返回数值
 * */
sds ziplistGetObject(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;

    serverAssert(sptr != NULL);
    serverAssert(ziplistGet(sptr, &vstr, &vlen, &vlong));

    if (vstr) {
        return sdsnewlen((char *) vstr, vlen);
    } else {
        return sdsfromlonglong(vlong);
    }
}

/* Compare element in sorted set with given element.
 * 从ziplist中读取一个entry 将数据与 cstr指针指向的数据做比较
 * */
int zzlCompareElements(unsigned char *eptr, unsigned char *cstr, unsigned int clen) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    unsigned char vbuf[32];
    int minlen, cmp;

    serverAssert(ziplistGet(eptr, &vstr, &vlen, &vlong));
    if (vstr == NULL) {
        /* Store string representation of long long in buf. */
        vlen = ll2string((char *) vbuf, sizeof(vbuf), vlong);
        vstr = vbuf;
    }

    minlen = (vlen < clen) ? vlen : clen;
    cmp = memcmp(vstr, cstr, minlen);
    if (cmp == 0) return vlen - clen;
    return cmp;
}

/**
 * ziplist存储zset数据的方式应该也是一次存储2个entry 一个表示score 一个表示ele  类似于用ziplist实现hash
 * @param zl
 * @return
 */
unsigned int zzlLength(unsigned char *zl) {
    return ziplistLen(zl) / 2;
}

/* Move to next entry based on the values in eptr and sptr. Both are set to
 * NULL when there is no next entry. */
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    serverAssert(*eptr != NULL && *sptr != NULL);

    _eptr = ziplistNext(zl, *sptr);
    if (_eptr != NULL) {
        _sptr = ziplistNext(zl, _eptr);
        serverAssert(_sptr != NULL);
    } else {
        /* No next entry. */
        _sptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;
}

/* Move to the previous entry based on the values in eptr and sptr. Both are
 * set to NULL when there is no next entry. */
void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    serverAssert(*eptr != NULL && *sptr != NULL);

    _sptr = ziplistPrev(zl, *eptr);
    if (_sptr != NULL) {
        _eptr = ziplistPrev(zl, _sptr);
        serverAssert(_eptr != NULL);
    } else {
        /* No previous entry. */
        _eptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;
}

/* Returns if there is a part of the zset is in range. Should only be used
 * internally by zzlFirstInRange and zzlLastInRange.
 * 判断ziplist中的元素是否在range之内
 * */
int zzlIsInRange(unsigned char *zl, zrangespec *range) {
    unsigned char *p;
    double score;

    /* Test for ranges that will always be empty. */
    if (range->min > range->max ||
        (range->min == range->max && (range->minex || range->maxex)))
        return 0;

    // 从下面的逻辑可以看出 即使基于ziplist实现zset 内部的数据也是有序的  并且entry1是ele entry2是score

    // 定位到ziplist最后一个entry的偏移量
    p = ziplistIndex(zl, -1); /* Last score. */
    if (p == NULL) return 0; /* Empty sorted set */
    score = zzlGetScore(p);
    if (!zslValueGteMin(score, range))
        return 0;

    p = ziplistIndex(zl, 1); /* First score. */
    serverAssert(p != NULL);
    score = zzlGetScore(p);
    if (!zslValueLteMax(score, range))
        return 0;

    return 1;
}

/* Find pointer to the first element contained in the specified range.
 * Returns NULL when no element is contained in the range.
 * 当基于ziplist实现zset时 检查内部是否有在范围内的数据
 * */
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range) {
    unsigned char *eptr = ziplistIndex(zl, 0), *sptr;
    double score;

    /* If everything is out of range, return early. */
    if (!zzlIsInRange(zl, range)) return NULL;

    while (eptr != NULL) {
        // 每次遍历2个entry
        sptr = ziplistNext(zl, eptr);
        serverAssert(sptr != NULL);

        score = zzlGetScore(sptr);
        // 当发现首个满足条件的score时 返回
        if (zslValueGteMin(score, range)) {
            /* Check if score <= max. */
            if (zslValueLteMax(score, range))
                return eptr;
            return NULL;
        }

        /* Move to next element. */
        eptr = ziplistNext(zl, sptr);
    }

    return NULL;
}

/* Find pointer to the last element contained in the specified range.
 * Returns NULL when no element is contained in the range. */
unsigned char *zzlLastInRange(unsigned char *zl, zrangespec *range) {
    unsigned char *eptr = ziplistIndex(zl, -2), *sptr;
    double score;

    /* If everything is out of range, return early. */
    if (!zzlIsInRange(zl, range)) return NULL;

    while (eptr != NULL) {
        sptr = ziplistNext(zl, eptr);
        serverAssert(sptr != NULL);

        score = zzlGetScore(sptr);
        if (zslValueLteMax(score, range)) {
            /* Check if score >= min. */
            if (zslValueGteMin(score, range))
                return eptr;
            return NULL;
        }

        /* Move to previous element by moving to the score of previous element.
         * When this returns NULL, we know there also is no element.
         * 从后往前 每次移动2个entry
         * */
        sptr = ziplistPrev(zl, eptr);
        if (sptr != NULL)
            serverAssert((eptr = ziplistPrev(zl, sptr)) != NULL);
        else
            eptr = NULL;
    }

    return NULL;
}

/**
 * 此时认为p是ziplist中某个entry的指针 判断对应的值是否在spec内
 * @param p
 * @param spec
 * @return
 */
int zzlLexValueGteMin(unsigned char *p, zlexrangespec *spec) {
    sds value = ziplistGetObject(p);
    int res = zslLexValueGteMin(value, spec);
    sdsfree(value);
    return res;
}

int zzlLexValueLteMax(unsigned char *p, zlexrangespec *spec) {
    sds value = ziplistGetObject(p);
    int res = zslLexValueLteMax(value, spec);
    sdsfree(value);
    return res;
}

/* Returns if there is a part of the zset is in range. Should only be used
 * internally by zzlFirstInRange and zzlLastInRange.
 * 判断ziplist中的 ele是否在范围内
 * */
int zzlIsInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *p;

    /* Test for ranges that will always be empty. */
    int cmp = sdscmplex(range->min, range->max);
    if (cmp > 0 || (cmp == 0 && (range->minex || range->maxex)))
        return 0;

    p = ziplistIndex(zl, -2); /* Last element. */
    if (p == NULL) return 0;
    if (!zzlLexValueGteMin(p, range))
        return 0;

    p = ziplistIndex(zl, 0); /* First element. */
    serverAssert(p != NULL);
    if (!zzlLexValueLteMax(p, range))
        return 0;

    return 1;
}

/* Find pointer to the first element contained in the specified lex range.
 * Returns NULL when no element is contained in the range.
 * 找到第一个在范围内的ele   zset是可以选择通过ele或者score排序么
 * */
unsigned char *zzlFirstInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *eptr = ziplistIndex(zl, 0), *sptr;

    /* If everything is out of range, return early. */
    if (!zzlIsInLexRange(zl, range)) return NULL;

    while (eptr != NULL) {
        if (zzlLexValueGteMin(eptr, range)) {
            /* Check if score <= max. */
            if (zzlLexValueLteMax(eptr, range))
                return eptr;
            return NULL;
        }

        /* Move to next element. */
        sptr = ziplistNext(zl, eptr); /* This element score. Skip it. */
        serverAssert(sptr != NULL);
        eptr = ziplistNext(zl, sptr); /* Next element. */
    }

    return NULL;
}

/* Find pointer to the last element contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
unsigned char *zzlLastInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *eptr = ziplistIndex(zl, -2), *sptr;

    /* If everything is out of range, return early. */
    if (!zzlIsInLexRange(zl, range)) return NULL;

    while (eptr != NULL) {
        if (zzlLexValueLteMax(eptr, range)) {
            /* Check if score >= min. */
            if (zzlLexValueGteMin(eptr, range))
                return eptr;
            return NULL;
        }

        /* Move to previous element by moving to the score of previous element.
         * When this returns NULL, we know there also is no element. */
        sptr = ziplistPrev(zl, eptr);
        if (sptr != NULL)
            serverAssert((eptr = ziplistPrev(zl, sptr)) != NULL);
        else
            eptr = NULL;
    }

    return NULL;
}

/**
 * 在ziplist中找到ele匹配的entry 并将score指向对应的分数
 * @param zl
 * @param ele
 * @param score
 * @return
 */
unsigned char *zzlFind(unsigned char *zl, sds ele, double *score) {
    unsigned char *eptr = ziplistIndex(zl, 0), *sptr;

    while (eptr != NULL) {
        // 记录的是score的指针
        sptr = ziplistNext(zl, eptr);
        serverAssert(sptr != NULL);

        // 先比较ele
        if (ziplistCompare(eptr, (unsigned char *) ele, sdslen(ele))) {
            /* Matching element, pull out score. */
            if (score != NULL) *score = zzlGetScore(sptr);
            return eptr;
        }

        /* Move to next element. */
        eptr = ziplistNext(zl, sptr);
    }
    return NULL;
}

/* Delete (element,score) pair from ziplist. Use local copy of eptr because we
 * don't want to modify the one given as argument. */
unsigned char *zzlDelete(unsigned char *zl, unsigned char *eptr) {
    unsigned char *p = eptr;

    /* TODO: add function to ziplist API to delete N elements from offset. */
    zl = ziplistDelete(zl, &p);
    zl = ziplistDelete(zl, &p);
    return zl;
}

/**
 * 在指定的位置插入数据  在调用该方法前已经确定本次的 ele/score要插入到哪个位置了
 * @param zl
 * @param eptr
 * @param ele
 * @param score
 * @return
 */
unsigned char *zzlInsertAt(unsigned char *zl, unsigned char *eptr, sds ele, double score) {
    unsigned char *sptr;
    char scorebuf[128];
    int scorelen;
    size_t offset;

    // 将score转换成字符串后 返回字符串长度
    scorelen = d2string(scorebuf, sizeof(scorebuf), score);
    // 在没有指定位置时 都是往尾部添加
    if (eptr == NULL) {
        zl = ziplistPush(zl, (unsigned char *) ele, sdslen(ele), ZIPLIST_TAIL);
        zl = ziplistPush(zl, (unsigned char *) scorebuf, scorelen, ZIPLIST_TAIL);
    } else {
        /* Keep offset relative to zl, as it might be re-allocated. */
        offset = eptr - zl;
        zl = ziplistInsert(zl, eptr, (unsigned char *) ele, sdslen(ele));
        // 保持eptr不变
        eptr = zl + offset;

        /* Insert score after the element. */
        serverAssert((sptr = ziplistNext(zl, eptr)) != NULL);
        zl = ziplistInsert(zl, sptr, (unsigned char *) scorebuf, scorelen);
    }
    return zl;
}

/* Insert (element,score) pair in ziplist. This function assumes the element is
 * not yet present in the list.
 * 将ele/score插入到ziplist中
 * */
unsigned char *zzlInsert(unsigned char *zl, sds ele, double score) {
    unsigned char *eptr = ziplistIndex(zl, 0), *sptr;
    double s;

    while (eptr != NULL) {
        sptr = ziplistNext(zl, eptr);
        serverAssert(sptr != NULL);
        s = zzlGetScore(sptr);

        // 按照score插入
        if (s > score) {
            /* First element with score larger than score for element to be
             * inserted. This means we should take its spot in the list to
             * maintain ordering. */
            zl = zzlInsertAt(zl, eptr, ele, score);
            break;
        } else if (s == score) {
            /* Ensure lexicographical ordering for elements. */
            if (zzlCompareElements(eptr, (unsigned char *) ele, sdslen(ele)) > 0) {
                zl = zzlInsertAt(zl, eptr, ele, score);
                break;
            }
        }

        /* Move to next element. */
        eptr = ziplistNext(zl, sptr);
    }

    /* Push on tail of list when it was not yet inserted. */
    if (eptr == NULL)
        zl = zzlInsertAt(zl, NULL, ele, score);
    return zl;
}

/**
 * 删除范围内的entry
 * @param zl
 * @param range
 * @param deleted
 * @return
 */
unsigned char *zzlDeleteRangeByScore(unsigned char *zl, zrangespec *range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    double score;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    eptr = zzlFirstInRange(zl, range);
    if (eptr == NULL) return zl;

    /* When the tail of the ziplist is deleted, eptr will point to the sentinel
     * byte and ziplistNext will return NULL. */
    while ((sptr = ziplistNext(zl, eptr)) != NULL) {
        score = zzlGetScore(sptr);
        if (zslValueLteMax(score, range)) {
            /* Delete both the element and the score. */
            zl = ziplistDelete(zl, &eptr);
            zl = ziplistDelete(zl, &eptr);
            num++;
        } else {
            /* No longer in range. */
            break;
        }
    }

    if (deleted != NULL) *deleted = num;
    return zl;
}

/**
 * 将范围内的ele对应的数据删除
 * @param zl
 * @param range
 * @param deleted
 * @return
 */
unsigned char *zzlDeleteRangeByLex(unsigned char *zl, zlexrangespec *range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    eptr = zzlFirstInLexRange(zl, range);
    if (eptr == NULL) return zl;

    /* When the tail of the ziplist is deleted, eptr will point to the sentinel
     * byte and ziplistNext will return NULL. */
    while ((sptr = ziplistNext(zl, eptr)) != NULL) {
        if (zzlLexValueLteMax(eptr, range)) {
            /* Delete both the element and the score. */
            zl = ziplistDelete(zl, &eptr);
            zl = ziplistDelete(zl, &eptr);
            num++;
        } else {
            /* No longer in range. */
            break;
        }
    }

    if (deleted != NULL) *deleted = num;
    return zl;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based
 * 按照偏移量来删除元素
 * */
unsigned char *zzlDeleteRangeByRank(unsigned char *zl, unsigned int start, unsigned int end, unsigned long *deleted) {
    unsigned int num = (end - start) + 1;
    if (deleted) *deleted = num;
    zl = ziplistDeleteRange(zl, 2 * (start - 1), 2 * num);
    return zl;
}

/*-----------------------------------------------------------------------------
 * Common sorted set API
 *----------------------------------------------------------------------------*/

/**
 * 返回redisObject的长度信息
 * @param zobj
 * @return
 */
unsigned long zsetLength(const robj *zobj) {
    unsigned long length = 0;
    // 根据不同的数据结构 调用不同的方法
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        length = zzlLength(zobj->ptr);
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        length = ((const zset *) zobj->ptr)->zsl->length;
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return length;
}

/**
 * zset支持skiplist与ziplist的相互转换
 * @param zobj
 * @param encoding
 */
void zsetConvert(robj *zobj, int encoding) {
    zset *zs;
    zskiplistNode *node, *next;
    sds ele;
    double score;

    if (zobj->encoding == encoding) return;
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        if (encoding != OBJ_ENCODING_SKIPLIST)
            serverPanic("Unknown target encoding");

        zs = zmalloc(sizeof(*zs));
        zs->dict = dictCreate(&zsetDictType, NULL);
        zs->zsl = zslCreate();

        eptr = ziplistIndex(zl, 0);
        serverAssertWithInfo(NULL, zobj, eptr != NULL);
        sptr = ziplistNext(zl, eptr);
        serverAssertWithInfo(NULL, zobj, sptr != NULL);

        while (eptr != NULL) {
            score = zzlGetScore(sptr);
            serverAssertWithInfo(NULL, zobj, ziplistGet(eptr, &vstr, &vlen, &vlong));
            if (vstr == NULL)
                ele = sdsfromlonglong(vlong);
            else
                ele = sdsnewlen((char *) vstr, vlen);

            // zset内部除了存储数据的list外 还有一个关联的字典
            node = zslInsert(zs->zsl, score, ele);
            serverAssert(dictAdd(zs->dict, ele, &node->score) == DICT_OK);
            zzlNext(zl, &eptr, &sptr);
        }

        zfree(zobj->ptr);
        zobj->ptr = zs;
        zobj->encoding = OBJ_ENCODING_SKIPLIST;
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        unsigned char *zl = ziplistNew();

        if (encoding != OBJ_ENCODING_ZIPLIST)
            serverPanic("Unknown target encoding");

        /* Approach similar to zslFree(), since we want to free the skiplist at
         * the same time as creating the ziplist. */
        zs = zobj->ptr;
        dictRelease(zs->dict);
        node = zs->zsl->header->level[0].forward;
        zfree(zs->zsl->header);
        zfree(zs->zsl);

        while (node) {
            zl = zzlInsertAt(zl, NULL, node->ele, node->score);
            next = node->level[0].forward;
            zslFreeNode(node);
            node = next;
        }

        zfree(zs);
        zobj->ptr = zl;
        zobj->encoding = OBJ_ENCODING_ZIPLIST;
    } else {
        serverPanic("Unknown sorted set encoding");
    }
}

/* Convert the sorted set object into a ziplist if it is not already a ziplist
 * and if the number of elements and the maximum element size is within the
 * expected ranges.
 * 此时是否满足降级成ziplist的条件
 * */
void zsetConvertToZiplistIfNeeded(robj *zobj, size_t maxelelen) {
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) return;
    zset *zset = zobj->ptr;

    if (zset->zsl->length <= server.zset_max_ziplist_entries &&
        maxelelen <= server.zset_max_ziplist_value)
        zsetConvert(zobj, OBJ_ENCODING_ZIPLIST);
}

/* Return (by reference) the score of the specified member of the sorted set
 * storing it into *score. If the element does not exist C_ERR is returned
 * otherwise C_OK is returned and *score is correctly populated.
 * If 'zobj' or 'member' is NULL, C_ERR is returned.
 * 在zset中查找某个 member对应的score
 * */
int zsetScore(robj *zobj, sds member, double *score) {
    if (!zobj || !member) return C_ERR;

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        if (zzlFind(zobj->ptr, member, score) == NULL) return C_ERR;
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        dictEntry *de = dictFind(zs->dict, member);
        if (de == NULL) return C_ERR;
        *score = *(double *) dictGetVal(de);
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return C_OK;
}

/* Add a new element or update the score of an existing element in a sorted
 * set, regardless of its encoding.
 *
 * The set of flags change the command behavior. They are passed with an integer
 * pointer since the function will clear the flags and populate them with
 * other flags to indicate different conditions.
 *
 * The input flags are the following:
 *
 * ZADD_INCR: Increment the current element score by 'score' instead of updating
 *            the current element score. If the element does not exist, we
 *            assume 0 as previous score.
 * ZADD_NX:   Perform the operation only if the element does not exist.
 * ZADD_XX:   Perform the operation only if the element already exist.
 *
 * When ZADD_INCR is used, the new score of the element is stored in
 * '*newscore' if 'newscore' is not NULL.
 *
 * The returned flags are the following:
 *
 * ZADD_NAN:     The resulting score is not a number.
 * ZADD_ADDED:   The element was added (not present before the call).
 * ZADD_UPDATED: The element score was updated.
 * ZADD_NOP:     No operation was performed because of NX or XX.
 *
 * Return value:
 *
 * The function returns 1 on success, and sets the appropriate flags
 * ADDED or UPDATED to signal what happened during the operation (note that
 * none could be set if we re-added an element using the same score it used
 * to have, or in the case a zero increment is used).
 *
 * The function returns 0 on error, currently only when the increment
 * produces a NAN condition, or when the 'score' value is NAN since the
 * start.
 *
 * The command as a side effect of adding a new element may convert the sorted
 * set internal encoding from ziplist to hashtable+skiplist.
 *
 * Memory management of 'ele':
 *
 * The function does not take ownership of the 'ele' SDS string, but copies
 * it if needed.
 * 往zset结构中插入一个新元素
 * */
int zsetAdd(robj *zobj, double score, sds ele, int *flags, double *newscore) {
    /* Turn options into simple to check vars. */
    int incr = (*flags & ZADD_INCR) != 0;
    int nx = (*flags & ZADD_NX) != 0;
    int xx = (*flags & ZADD_XX) != 0;
    *flags = 0; /* We'll return our response flags. */
    double curscore;

    /* NaN as input is an error regardless of all the other parameters. */
    if (isnan(score)) {
        *flags = ZADD_NAN;
        return 0;
    }

    /* Update the sorted set according to its encoding. */
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *eptr;

        // 检查ziplist中是否已经存在ele
        if ((eptr = zzlFind(zobj->ptr, ele, &curscore)) != NULL) {
            /* NX? Return, same element already exists. */
            if (nx) {
                *flags |= ZADD_NOP;
                return 1;
            }

            // 代表本次指令允许覆盖
            /* Prepare the score for the increment if needed.
             * 本次是一次增量指令
             * */
            if (incr) {
                score += curscore;
                if (isnan(score)) {
                    *flags |= ZADD_NAN;
                    return 0;
                }
                if (newscore) *newscore = score;
            }

            /* Remove and re-insert when score changed.
             * 覆盖原值
             * */
            if (score != curscore) {
                zobj->ptr = zzlDelete(zobj->ptr, eptr);
                zobj->ptr = zzlInsert(zobj->ptr, ele, score);
                *flags |= ZADD_UPDATED;
            }
            return 1;
            // 代表原本ziplist中不存在 ele 此时只有nx/incr能生效
        } else if (!xx) {
            /* Optimize: check if the element is too large or the list
             * becomes too long *before* executing zzlInsert. */
            zobj->ptr = zzlInsert(zobj->ptr, ele, score);
            if (zzlLength(zobj->ptr) > server.zset_max_ziplist_entries ||
                sdslen(ele) > server.zset_max_ziplist_value)
                zsetConvert(zobj, OBJ_ENCODING_SKIPLIST);
            if (newscore) *newscore = score;
            *flags |= ZADD_ADDED;
            return 1;
        } else {
            *flags |= ZADD_NOP;
            return 1;
        }
        // 当此时的数据结构是skiplist时
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplistNode *znode;
        dictEntry *de;

        de = dictFind(zs->dict, ele);
        if (de != NULL) {
            /* NX? Return, same element already exists. */
            if (nx) {
                *flags |= ZADD_NOP;
                return 1;
            }
            curscore = *(double *) dictGetVal(de);

            /* Prepare the score for the increment if needed. */
            if (incr) {
                score += curscore;
                if (isnan(score)) {
                    *flags |= ZADD_NAN;
                    return 0;
                }
                if (newscore) *newscore = score;
            }

            /* Remove and re-insert when score changes. */
            if (score != curscore) {
                znode = zslUpdateScore(zs->zsl, curscore, ele, score);
                /* Note that we did not removed the original element from
                 * the hash table representing the sorted set, so we just
                 * update the score. */
                dictGetVal(de) = &znode->score; /* Update score ptr. */
                *flags |= ZADD_UPDATED;
            }
            return 1;
        } else if (!xx) {
            ele = sdsdup(ele);
            znode = zslInsert(zs->zsl, score, ele);
            serverAssert(dictAdd(zs->dict, ele, &znode->score) == DICT_OK);
            *flags |= ZADD_ADDED;
            if (newscore) *newscore = score;
            return 1;
        } else {
            *flags |= ZADD_NOP;
            return 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return 0; /* Never reached. */
}

/* Delete the element 'ele' from the sorted set, returning 1 if the element
 * existed and was deleted, 0 otherwise (the element was not there).
 * 从zset中删除某个ele
 * */
int zsetDel(robj *zobj, sds ele) {
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *eptr;

        if ((eptr = zzlFind(zobj->ptr, ele, NULL)) != NULL) {
            zobj->ptr = zzlDelete(zobj->ptr, eptr);
            return 1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        dictEntry *de;
        double score;

        de = dictUnlink(zs->dict, ele);
        if (de != NULL) {
            /* Get the score in order to delete from the skiplist later. */
            score = *(double *) dictGetVal(de);

            /* Delete from the hash table and later from the skiplist.
             * Note that the order is important: deleting from the skiplist
             * actually releases the SDS string representing the element,
             * which is shared between the skiplist and the hash table, so
             * we need to delete from the skiplist as the final step. */
            dictFreeUnlinkedEntry(zs->dict, de);

            /* Delete from skiplist. */
            int retval = zslDelete(zs->zsl, score, ele, NULL);
            serverAssert(retval);

            if (htNeedsResize(zs->dict)) dictResize(zs->dict);
            return 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return 0; /* No such element found. */
}

/* Given a sorted set object returns the 0-based rank of the object or
 * -1 if the object does not exist.
 *
 * For rank we mean the position of the element in the sorted collection
 * of elements. So the first element has rank 0, the second rank 1, and so
 * forth up to length-1 elements.
 *
 * If 'reverse' is false, the rank is returned considering as first element
 * the one with the lowest score. Otherwise if 'reverse' is non-zero
 * the rank is computed considering as element with rank 0 the one with
 * the highest score.
 * @param reverse 代表要返回的rank是正向的还是反向的
 * */
long zsetRank(robj *zobj, sds ele, int reverse) {
    unsigned long llen;
    unsigned long rank;

    llen = zsetLength(zobj);

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        eptr = ziplistIndex(zl, 0);
        serverAssert(eptr != NULL);
        sptr = ziplistNext(zl, eptr);
        serverAssert(sptr != NULL);

        rank = 1;
        while (eptr != NULL) {
            // 每次前进2个entry 尝试好到匹配ele的entry
            if (ziplistCompare(eptr, (unsigned char *) ele, sdslen(ele)))
                break;
            rank++;
            zzlNext(zl, &eptr, &sptr);
        }

        if (eptr != NULL) {
            if (reverse)
                return llen - rank;
            else
                return rank - 1;
        } else {
            return -1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        dictEntry *de;
        double score;

        de = dictFind(zs->dict, ele);
        if (de != NULL) {
            score = *(double *) dictGetVal(de);
            rank = zslGetRank(zsl, score, ele);
            /* Existing elements always have a rank. */
            serverAssert(rank != 0);
            if (reverse)
                return llen - rank;
            else
                return rank - 1;
        } else {
            return -1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
}

/*-----------------------------------------------------------------------------
 * Sorted set commands
 *----------------------------------------------------------------------------*/

/* This generic command implements both ZADD and ZINCRBY.
 * 处理client接受到的 zadd指令
 * */
void zaddGenericCommand(client *c, int flags) {
    static char *nanerr = "resulting score is not a number (NaN)";
    
    // 首先从db中找到redisObject对象 
    robj *key = c->argv[1];
    robj *zobj;
    sds ele;
    double score = 0, *scores = NULL;
    int j, elements;
    int scoreidx = 0;
    /* The following vars are used in order to track what the command actually
     * did during the execution, to reply to the client and to trigger the
     * notification of keyspace change. */
    int added = 0;      /* Number of new elements added. */
    int updated = 0;    /* Number of elements with updated score. */
    int processed = 0;  /* Number of elements processed, may remain zero with
                           options like XX. */

    /* Parse options. At the end 'scoreidx' is set to the argument position
     * of the score of the first score-element pair. */
    scoreidx = 2;
    // 解析本次command 后面的参数 
    while (scoreidx < c->argc) {
        char *opt = c->argv[scoreidx]->ptr;
        if (!strcasecmp(opt, "nx")) flags |= ZADD_NX;
        else if (!strcasecmp(opt, "xx")) flags |= ZADD_XX;
        else if (!strcasecmp(opt, "ch")) flags |= ZADD_CH;
        else if (!strcasecmp(opt, "incr")) flags |= ZADD_INCR;
        else break;
        scoreidx++;
    }

    /* Turn options into simple to check vars. */
    int incr = (flags & ZADD_INCR) != 0;
    int nx = (flags & ZADD_NX) != 0;
    int xx = (flags & ZADD_XX) != 0;
    int ch = (flags & ZADD_CH) != 0;

    /* After the options, we expect to have an even number of args, since
     * we expect any number of score-element pairs. 
     * 剩余的参数就是本次要插入的数据 注意这里可以是批量插入
     * */
    elements = c->argc - scoreidx;
    if (elements % 2 || !elements) {
        addReply(c, shared.syntaxerr);
        return;
    }
    elements /= 2; /* Now this holds the number of score-element pairs. */

    /* Check for incompatible options. */
    if (nx && xx) {
        addReplyError(c,
                      "XX and NX options at the same time are not compatible");
        return;
    }

    // 当本次指定的类型是incr时 本次不能批量插入 
    if (incr && elements > 1) {
        addReplyError(c,
                      "INCR option supports a single increment-element pair");
        return;
    }

    /* Start parsing all the scores, we need to emit any syntax error
     * before executing additions to the sorted set, as the command should
     * either execute fully or nothing at all. 
     * scores 是一个double数组 存储本次要插入的每个ele 对应的分数 
     * */
    scores = zmalloc(sizeof(double) * elements);
    for (j = 0; j < elements; j++) {
        if (getDoubleFromObjectOrReply(c, c->argv[scoreidx + j * 2], &scores[j], NULL)
            != C_OK)
            goto cleanup;
    }

    /* Lookup the key and create the sorted set if does not exist. */
    zobj = lookupKeyWrite(c->db, key);
    if (zobj == NULL) {
        if (xx) goto reply_to_client; /* No key + XX option: nothing to do. */
        if (server.zset_max_ziplist_entries == 0 ||
            server.zset_max_ziplist_value < sdslen(c->argv[scoreidx + 1]->ptr)) {
            zobj = createZsetObject();
        } else {
            zobj = createZsetZiplistObject();
        }
        dbAdd(c->db, key, zobj);
    } else {
        // db中key对应的redisObject已经存在 但是不是zset结构
        if (zobj->type != OBJ_ZSET) {
            addReply(c, shared.wrongtypeerr);
            goto cleanup;
        }
    }

    for (j = 0; j < elements; j++) {
        double newscore;
        score = scores[j];
        int retflags = flags;

        ele = c->argv[scoreidx + 1 + j * 2]->ptr;
        int retval = zsetAdd(zobj, score, ele, &retflags, &newscore);
        if (retval == 0) {
            addReplyError(c, nanerr);
            goto cleanup;
        }
        if (retflags & ZADD_ADDED) added++;
        if (retflags & ZADD_UPDATED) updated++;
        if (!(retflags & ZADD_NOP)) processed++;
        score = newscore;
    }
    
    // 会有后台线程检查数据是否发生变化 并触发刷盘逻辑
    server.dirty += (added + updated);

    reply_to_client:
    if (incr) { /* ZINCRBY or INCR option. */
        if (processed)
            addReplyDouble(c, score);
        else
            addReplyNull(c);
    } else { /* ZADD. */
        addReplyLongLong(c, ch ? added + updated : added);
    }

    cleanup:
    zfree(scores);
    // 触发事件机制 
    if (added || updated) {
        signalModifiedKey(c, c->db, key);
        notifyKeyspaceEvent(NOTIFY_ZSET,
                            incr ? "zincr" : "zadd", key, c->db->id);
    }
}

void zaddCommand(client *c) {
    zaddGenericCommand(c, ZADD_NONE);
}

void zincrbyCommand(client *c) {
    zaddGenericCommand(c, ZADD_INCR);
}

/**
 * 通过client内部的参数信息 移除zset中的数据
 * @param c 
 */
void zremCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    int deleted = 0, keyremoved = 0, j;

    // 检测 key在db中是否存在  并且类型于zset是否匹配 
    if ((zobj = lookupKeyWriteOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
        return;

    for (j = 2; j < c->argc; j++) {
        if (zsetDel(zobj, c->argv[j]->ptr)) deleted++;
        if (zsetLength(zobj) == 0) {
            dbDelete(c->db, key);
            keyremoved = 1;
            break;
        }
    }

    if (deleted) {
        notifyKeyspaceEvent(NOTIFY_ZSET, "zrem", key, c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", key, c->db->id);
        signalModifiedKey(c, c->db, key);
        server.dirty += deleted;
    }
    addReplyLongLong(c, deleted);
}

/* Implements ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX commands. */
#define ZRANGE_RANK 0
#define ZRANGE_SCORE 1
#define ZRANGE_LEX 2

void zremrangeGenericCommand(client *c, int rangetype) {
    robj *key = c->argv[1];
    robj *zobj;
    int keyremoved = 0;
    unsigned long deleted = 0;
    zrangespec range;
    zlexrangespec lexrange;
    long start, end, llen;

    /* Step 1: Parse the range. */
    if (rangetype == ZRANGE_RANK) {
        if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != C_OK) ||
            (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != C_OK))
            return;
    } else if (rangetype == ZRANGE_SCORE) {
        if (zslParseRange(c->argv[2], c->argv[3], &range) != C_OK) {
            addReplyError(c, "min or max is not a float");
            return;
        }
    } else if (rangetype == ZRANGE_LEX) {
        if (zslParseLexRange(c->argv[2], c->argv[3], &lexrange) != C_OK) {
            addReplyError(c, "min or max not valid string range item");
            return;
        }
    }

    /* Step 2: Lookup & range sanity checks if needed. */
    if ((zobj = lookupKeyWriteOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
        goto cleanup;

    if (rangetype == ZRANGE_RANK) {
        /* Sanitize indexes. */
        llen = zsetLength(zobj);
        if (start < 0) start = llen + start;
        if (end < 0) end = llen + end;
        if (start < 0) start = 0;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        if (start > end || start >= llen) {
            addReply(c, shared.czero);
            goto cleanup;
        }
        if (end >= llen) end = llen - 1;
    }

    /* Step 3: Perform the range deletion operation. */
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        switch (rangetype) {
            case ZRANGE_RANK:
                zobj->ptr = zzlDeleteRangeByRank(zobj->ptr, start + 1, end + 1, &deleted);
                break;
            case ZRANGE_SCORE:
                zobj->ptr = zzlDeleteRangeByScore(zobj->ptr, &range, &deleted);
                break;
            case ZRANGE_LEX:
                zobj->ptr = zzlDeleteRangeByLex(zobj->ptr, &lexrange, &deleted);
                break;
        }
        if (zzlLength(zobj->ptr) == 0) {
            dbDelete(c->db, key);
            keyremoved = 1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        switch (rangetype) {
            case ZRANGE_RANK:
                deleted = zslDeleteRangeByRank(zs->zsl, start + 1, end + 1, zs->dict);
                break;
            case ZRANGE_SCORE:
                deleted = zslDeleteRangeByScore(zs->zsl, &range, zs->dict);
                break;
            case ZRANGE_LEX:
                deleted = zslDeleteRangeByLex(zs->zsl, &lexrange, zs->dict);
                break;
        }
        if (htNeedsResize(zs->dict)) dictResize(zs->dict);
        if (dictSize(zs->dict) == 0) {
            dbDelete(c->db, key);
            keyremoved = 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    /* Step 4: Notifications and reply. */
    if (deleted) {
        char *event[3] = {"zremrangebyrank", "zremrangebyscore", "zremrangebylex"};
        signalModifiedKey(c, c->db, key);
        notifyKeyspaceEvent(NOTIFY_ZSET, event[rangetype], key, c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", key, c->db->id);
    }
    server.dirty += deleted;
    addReplyLongLong(c, deleted);

    cleanup:
    if (rangetype == ZRANGE_LEX) zslFreeLexRange(&lexrange);
}

void zremrangebyrankCommand(client *c) {
    zremrangeGenericCommand(c, ZRANGE_RANK);
}

void zremrangebyscoreCommand(client *c) {
    zremrangeGenericCommand(c, ZRANGE_SCORE);
}

void zremrangebylexCommand(client *c) {
    zremrangeGenericCommand(c, ZRANGE_LEX);
}

typedef struct {
    robj *subject;
    int type; /* Set, sorted set */
    int encoding;
    double weight;

    // 根据subject的类型 内部的迭代器也不同 可能是set(intset/dict)/zset(ziplist/skiplist)
    union {
        /* Set iterators. */
        union _iterset {
            struct {
                intset *is;
                int ii;
            } is;
            struct {
                dict *dict;
                dictIterator *di;
                dictEntry *de;
            } ht;
        } set;

        /* Sorted set iterators. */
        union _iterzset {
            struct {
                unsigned char *zl;
                unsigned char *eptr, *sptr;
            } zl;
            struct {
                zset *zs;
                zskiplistNode *node;
            } sl;
        } zset;
    } iter;
} zsetopsrc;


/* Use dirty flags for pointers that need to be cleaned up in the next
 * iteration over the zsetopval. The dirty flag for the long long value is
 * special, since long long values don't need cleanup. Instead, it means that
 * we already checked that "ell" holds a long long, or tried to convert another
 * representation into a long long value. When this was successful,
 * OPVAL_VALID_LL is set as well. */
#define OPVAL_DIRTY_SDS 1
#define OPVAL_DIRTY_LL 2
#define OPVAL_VALID_LL 4

/* Store value retrieved from the iterator. */
typedef struct {
    int flags;
    unsigned char _buf[32]; /* Private buffer. */
    sds ele;
    unsigned char *estr;
    unsigned int elen;
    long long ell;
    double score;
} zsetopval;

typedef union _iterset iterset;
typedef union _iterzset iterzset;


/**
 * 初始化op内部的迭代器对象 
 * @param op 
 */
void zuiInitIterator(zsetopsrc *op) {
    if (op->subject == NULL)
        return;

    if (op->type == OBJ_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            it->is.is = op->subject->ptr;
            it->is.ii = 0;
        } else if (op->encoding == OBJ_ENCODING_HT) {
            it->ht.dict = op->subject->ptr;
            it->ht.di = dictGetIterator(op->subject->ptr);
            it->ht.de = dictNext(it->ht.di);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            it->zl.zl = op->subject->ptr;
            it->zl.eptr = ziplistIndex(it->zl.zl, 0);
            if (it->zl.eptr != NULL) {
                it->zl.sptr = ziplistNext(it->zl.zl, it->zl.eptr);
                serverAssert(it->zl.sptr != NULL);
            }
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            it->sl.zs = op->subject->ptr;
            it->sl.node = it->sl.zs->zsl->header->level[0].forward;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

/**
 * 释放内部的迭代器
 * @param op
 */
void zuiClearIterator(zsetopsrc *op) {
    if (op->subject == NULL)
        return;

    if (op->type == OBJ_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            UNUSED(it); /* skip */
        } else if (op->encoding == OBJ_ENCODING_HT) {
            dictReleaseIterator(it->ht.di);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            UNUSED(it); /* skip */
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            UNUSED(it); /* skip */
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

unsigned long zuiLength(zsetopsrc *op) {
    if (op->subject == NULL)
        return 0;

    if (op->type == OBJ_SET) {
        if (op->encoding == OBJ_ENCODING_INTSET) {
            return intsetLen(op->subject->ptr);
        } else if (op->encoding == OBJ_ENCODING_HT) {
            dict *ht = op->subject->ptr;
            return dictSize(ht);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            return zzlLength(op->subject->ptr);
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = op->subject->ptr;
            return zs->zsl->length;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

/* Check if the current value is valid. If so, store it in the passed structure
 * and move to the next element. If not valid, this means we have reached the
 * end of the structure and can abort.
 * 通过 op对象内的迭代器遍历元素 并将数据设置到opval上
 * */
int zuiNext(zsetopsrc *op, zsetopval *val) {
    if (op->subject == NULL)
        return 0;

    if (val->flags & OPVAL_DIRTY_SDS)
        sdsfree(val->ele);

    memset(val, 0, sizeof(zsetopval));

    if (op->type == OBJ_SET) {
        // 针对set结构 score都是 1.0
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            int64_t ell;

            if (!intsetGet(it->is.is, it->is.ii, &ell))
                return 0;
            val->ell = ell;
            val->score = 1.0;

            /* Move to next element. */
            it->is.ii++;
        } else if (op->encoding == OBJ_ENCODING_HT) {
            if (it->ht.de == NULL)
                return 0;
            val->ele = dictGetKey(it->ht.de);
            val->score = 1.0;

            /* Move to next element. */
            it->ht.de = dictNext(it->ht.di);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            /* No need to check both, but better be explicit. */
            if (it->zl.eptr == NULL || it->zl.sptr == NULL)
                return 0;
            serverAssert(ziplistGet(it->zl.eptr, &val->estr, &val->elen, &val->ell));
            val->score = zzlGetScore(it->zl.sptr);

            /* Move to next element. */
            zzlNext(it->zl.zl, &it->zl.eptr, &it->zl.sptr);
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            if (it->sl.node == NULL)
                return 0;
            val->ele = it->sl.node->ele;
            val->score = it->sl.node->score;

            /* Move to next element. */
            it->sl.node = it->sl.node->level[0].forward;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
    return 1;
}

int zuiLongLongFromValue(zsetopval *val) {
    if (!(val->flags & OPVAL_DIRTY_LL)) {
        val->flags |= OPVAL_DIRTY_LL;

        if (val->ele != NULL) {
            if (string2ll(val->ele, sdslen(val->ele), &val->ell))
                val->flags |= OPVAL_VALID_LL;
        } else if (val->estr != NULL) {
            if (string2ll((char *) val->estr, val->elen, &val->ell))
                val->flags |= OPVAL_VALID_LL;
        } else {
            /* The long long was already set, flag as valid. */
            val->flags |= OPVAL_VALID_LL;
        }
    }
    return val->flags & OPVAL_VALID_LL;
}

sds zuiSdsFromValue(zsetopval *val) {
    if (val->ele == NULL) {
        if (val->estr != NULL) {
            val->ele = sdsnewlen((char *) val->estr, val->elen);
        } else {
            val->ele = sdsfromlonglong(val->ell);
        }
        val->flags |= OPVAL_DIRTY_SDS;
    }
    return val->ele;
}

/* This is different from zuiSdsFromValue since returns a new SDS string
 * which is up to the caller to free. */
sds zuiNewSdsFromValue(zsetopval *val) {
    if (val->flags & OPVAL_DIRTY_SDS) {
        /* We have already one to return! */
        sds ele = val->ele;
        val->flags &= ~OPVAL_DIRTY_SDS;
        val->ele = NULL;
        return ele;
    } else if (val->ele) {
        return sdsdup(val->ele);
    } else if (val->estr) {
        return sdsnewlen((char *) val->estr, val->elen);
    } else {
        return sdsfromlonglong(val->ell);
    }
}

int zuiBufferFromValue(zsetopval *val) {
    if (val->estr == NULL) {
        if (val->ele != NULL) {
            val->elen = sdslen(val->ele);
            val->estr = (unsigned char *) val->ele;
        } else {
            val->elen = ll2string((char *) val->_buf, sizeof(val->_buf), val->ell);
            val->estr = val->_buf;
        }
    }
    return 1;
}

/* Find value pointed to by val in the source pointer to by op. When found,
 * return 1 and store its score in target. Return 0 otherwise.
 * @param op
 * @param val    检测op关联的redisObject中是否有 val.ele匹配的数据
 * */
int zuiFind(zsetopsrc *op, zsetopval *val, double *score) {
    if (op->subject == NULL)
        return 0;

    if (op->type == OBJ_SET) {
        if (op->encoding == OBJ_ENCODING_INTSET) {
            if (zuiLongLongFromValue(val) &&
                intsetFind(op->subject->ptr, val->ell)) {
                *score = 1.0;
                return 1;
            } else {
                return 0;
            }
        } else if (op->encoding == OBJ_ENCODING_HT) {
            dict *ht = op->subject->ptr;
            zuiSdsFromValue(val);
            if (dictFind(ht, val->ele) != NULL) {
                *score = 1.0;
                return 1;
            } else {
                return 0;
            }
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        zuiSdsFromValue(val);

        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            if (zzlFind(op->subject->ptr, val->ele, score) != NULL) {
                /* Score is already set by zzlFind. */
                return 1;
            } else {
                return 0;
            }
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = op->subject->ptr;
            dictEntry *de;
            if ((de = dictFind(zs->dict, val->ele)) != NULL) {
                *score = *(double *) dictGetVal(de);
                return 1;
            } else {
                return 0;
            }
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

/**
 * 比较2个op结构的元素数量
 * @param s1
 * @param s2
 * @return
 */
int zuiCompareByCardinality(const void *s1, const void *s2) {
    unsigned long first = zuiLength((zsetopsrc *) s1);
    unsigned long second = zuiLength((zsetopsrc *) s2);
    if (first > second) return 1;
    if (first < second) return -1;
    return 0;
}

#define REDIS_AGGR_SUM 1
#define REDIS_AGGR_MIN 2
#define REDIS_AGGR_MAX 3
#define zunionInterDictValue(_e) (dictGetVal(_e) == NULL ? 1.0 : *(double*)dictGetVal(_e))

inline static void zunionInterAggregate(double *target, double val, int aggregate) {
    if (aggregate == REDIS_AGGR_SUM) {
        *target = *target + val;
        /* The result of adding two doubles is NaN when one variable
         * is +inf and the other is -inf. When these numbers are added,
         * we maintain the convention of the result being 0.0. */
        if (isnan(*target)) *target = 0.0;
    } else if (aggregate == REDIS_AGGR_MIN) {
        *target = val < *target ? val : *target;
    } else if (aggregate == REDIS_AGGR_MAX) {
        *target = val > *target ? val : *target;
    } else {
        /* safety net */
        serverPanic("Unknown ZUNION/INTER aggregate type");
    }
}

uint64_t dictSdsHash(const void *key);

int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);

dictType setAccumulatorDictType = {
        dictSdsHash,               /* hash function */
        NULL,                      /* key dup */
        NULL,                      /* val dup */
        dictSdsKeyCompare,         /* key compare */
        NULL,                      /* key destructor */
        NULL                       /* val destructor */
};

/**
 * 将2个redisObject的数据合并
 * @param c
 * @param dstkey
 * @param op
 */
void zunionInterGenericCommand(client *c, robj *dstkey, int op) {
    int i, j;
    long setnum;
    int aggregate = REDIS_AGGR_SUM;
    zsetopsrc *src;
    zsetopval zval;
    sds tmp;
    size_t maxelelen = 0;
    robj *dstobj;
    zset *dstzset;
    zskiplistNode *znode;
    int touched = 0;

    /* expect setnum input keys to be given */
    if ((getLongFromObjectOrReply(c, c->argv[2], &setnum, NULL) != C_OK))
        return;

    if (setnum < 1) {
        addReplyError(c,
                      "at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE");
        return;
    }

    /* test if the expected number of keys would overflow */
    if (setnum > c->argc - 3) {
        addReply(c, shared.syntaxerr);
        return;
    }

    /* read keys to be used for input */
    src = zcalloc(sizeof(zsetopsrc) * setnum);
    for (i = 0, j = 3; i < setnum; i++, j++) {
        robj *obj = lookupKeyWrite(c->db, c->argv[j]);
        if (obj != NULL) {
            if (obj->type != OBJ_ZSET && obj->type != OBJ_SET) {
                zfree(src);
                addReply(c, shared.wrongtypeerr);
                return;
            }

            src[i].subject = obj;
            src[i].type = obj->type;
            src[i].encoding = obj->encoding;
        } else {
            src[i].subject = NULL;
        }

        /* Default all weights to 1. */
        src[i].weight = 1.0;
    }

    /* parse optional extra arguments */
    if (j < c->argc) {
        int remaining = c->argc - j;

        while (remaining) {
            if (remaining >= (setnum + 1) &&
                !strcasecmp(c->argv[j]->ptr, "weights")) {
                j++;
                remaining--;
                for (i = 0; i < setnum; i++, j++, remaining--) {
                    if (getDoubleFromObjectOrReply(c, c->argv[j], &src[i].weight,
                                                   "weight value is not a float") != C_OK) {
                        zfree(src);
                        return;
                    }
                }
            } else if (remaining >= 2 &&
                       !strcasecmp(c->argv[j]->ptr, "aggregate")) {
                j++;
                remaining--;
                if (!strcasecmp(c->argv[j]->ptr, "sum")) {
                    aggregate = REDIS_AGGR_SUM;
                } else if (!strcasecmp(c->argv[j]->ptr, "min")) {
                    aggregate = REDIS_AGGR_MIN;
                } else if (!strcasecmp(c->argv[j]->ptr, "max")) {
                    aggregate = REDIS_AGGR_MAX;
                } else {
                    zfree(src);
                    addReply(c, shared.syntaxerr);
                    return;
                }
                j++;
                remaining--;
            } else {
                zfree(src);
                addReply(c, shared.syntaxerr);
                return;
            }
        }
    }

    /* sort sets from the smallest to largest, this will improve our
     * algorithm's performance */
    qsort(src, setnum, sizeof(zsetopsrc), zuiCompareByCardinality);

    dstobj = createZsetObject();
    dstzset = dstobj->ptr;
    memset(&zval, 0, sizeof(zval));

    if (op == SET_OP_INTER) {
        /* Skip everything if the smallest input is empty. */
        if (zuiLength(&src[0]) > 0) {
            /* Precondition: as src[0] is non-empty and the inputs are ordered
             * by size, all src[i > 0] are non-empty too. */
            zuiInitIterator(&src[0]);
            while (zuiNext(&src[0], &zval)) {
                double score, value;

                score = src[0].weight * zval.score;
                if (isnan(score)) score = 0;

                for (j = 1; j < setnum; j++) {
                    /* It is not safe to access the zset we are
                     * iterating, so explicitly check for equal object. */
                    if (src[j].subject == src[0].subject) {
                        value = zval.score * src[j].weight;
                        zunionInterAggregate(&score, value, aggregate);
                    } else if (zuiFind(&src[j], &zval, &value)) {
                        value *= src[j].weight;
                        zunionInterAggregate(&score, value, aggregate);
                    } else {
                        break;
                    }
                }

                /* Only continue when present in every input. */
                if (j == setnum) {
                    tmp = zuiNewSdsFromValue(&zval);
                    znode = zslInsert(dstzset->zsl, score, tmp);
                    dictAdd(dstzset->dict, tmp, &znode->score);
                    if (sdslen(tmp) > maxelelen) maxelelen = sdslen(tmp);
                }
            }
            zuiClearIterator(&src[0]);
        }
    } else if (op == SET_OP_UNION) {
        dict *accumulator = dictCreate(&setAccumulatorDictType, NULL);
        dictIterator *di;
        dictEntry *de, *existing;
        double score;

        if (setnum) {
            /* Our union is at least as large as the largest set.
             * Resize the dictionary ASAP to avoid useless rehashing. */
            dictExpand(accumulator, zuiLength(&src[setnum - 1]));
        }

        /* Step 1: Create a dictionary of elements -> aggregated-scores
         * by iterating one sorted set after the other. */
        for (i = 0; i < setnum; i++) {
            if (zuiLength(&src[i]) == 0) continue;

            zuiInitIterator(&src[i]);
            while (zuiNext(&src[i], &zval)) {
                /* Initialize value */
                score = src[i].weight * zval.score;
                if (isnan(score)) score = 0;

                /* Search for this element in the accumulating dictionary. */
                de = dictAddRaw(accumulator, zuiSdsFromValue(&zval), &existing);
                /* If we don't have it, we need to create a new entry. */
                if (!existing) {
                    tmp = zuiNewSdsFromValue(&zval);
                    /* Remember the longest single element encountered,
                     * to understand if it's possible to convert to ziplist
                     * at the end. */
                    if (sdslen(tmp) > maxelelen) maxelelen = sdslen(tmp);
                    /* Update the element with its initial score. */
                    dictSetKey(accumulator, de, tmp);
                    dictSetDoubleVal(de, score);
                } else {
                    /* Update the score with the score of the new instance
                     * of the element found in the current sorted set.
                     *
                     * Here we access directly the dictEntry double
                     * value inside the union as it is a big speedup
                     * compared to using the getDouble/setDouble API. */
                    zunionInterAggregate(&existing->v.d, score, aggregate);
                }
            }
            zuiClearIterator(&src[i]);
        }

        /* Step 2: convert the dictionary into the final sorted set. */
        di = dictGetIterator(accumulator);

        /* We now are aware of the final size of the resulting sorted set,
         * let's resize the dictionary embedded inside the sorted set to the
         * right size, in order to save rehashing time. */
        dictExpand(dstzset->dict, dictSize(accumulator));

        while ((de = dictNext(di)) != NULL) {
            sds ele = dictGetKey(de);
            score = dictGetDoubleVal(de);
            znode = zslInsert(dstzset->zsl, score, ele);
            dictAdd(dstzset->dict, ele, &znode->score);
        }
        dictReleaseIterator(di);
        dictRelease(accumulator);
    } else {
        serverPanic("Unknown operator");
    }

    if (dbDelete(c->db, dstkey))
        touched = 1;
    if (dstzset->zsl->length) {
        zsetConvertToZiplistIfNeeded(dstobj, maxelelen);
        dbAdd(c->db, dstkey, dstobj);
        addReplyLongLong(c, zsetLength(dstobj));
        signalModifiedKey(c, c->db, dstkey);
        notifyKeyspaceEvent(NOTIFY_ZSET,
                            (op == SET_OP_UNION) ? "zunionstore" : "zinterstore",
                            dstkey, c->db->id);
        server.dirty++;
    } else {
        decrRefCount(dstobj);
        addReply(c, shared.czero);
        if (touched) {
            signalModifiedKey(c, c->db, dstkey);
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", dstkey, c->db->id);
            server.dirty++;
        }
    }
    zfree(src);
}

void zunionstoreCommand(client *c) {
    zunionInterGenericCommand(c, c->argv[1], SET_OP_UNION);
}

void zinterstoreCommand(client *c) {
    zunionInterGenericCommand(c, c->argv[1], SET_OP_INTER);
}

void zrangeGenericCommand(client *c, int reverse) {
    robj *key = c->argv[1];
    robj *zobj;
    int withscores = 0;
    long start;
    long end;
    long llen;
    long rangelen;

    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != C_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != C_OK))
        return;

    if (c->argc == 5 && !strcasecmp(c->argv[4]->ptr, "withscores")) {
        withscores = 1;
    } else if (c->argc >= 5) {
        addReply(c, shared.syntaxerr);
        return;
    }

    if ((zobj = lookupKeyReadOrReply(c, key, shared.emptyarray)) == NULL
        || checkType(c, zobj, OBJ_ZSET))
        return;

    /* Sanitize indexes. */
    llen = zsetLength(zobj);
    if (start < 0) start = llen + start;
    if (end < 0) end = llen + end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    if (start > end || start >= llen) {
        addReply(c, shared.emptyarray);
        return;
    }
    if (end >= llen) end = llen - 1;
    rangelen = (end - start) + 1;

    /* Return the result in form of a multi-bulk reply. RESP3 clients
     * will receive sub arrays with score->element, while RESP2 returned
     * a flat array. */
    if (withscores && c->resp == 2)
        addReplyArrayLen(c, rangelen * 2);
    else
        addReplyArrayLen(c, rangelen);

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        if (reverse)
            eptr = ziplistIndex(zl, -2 - (2 * start));
        else
            eptr = ziplistIndex(zl, 2 * start);

        serverAssertWithInfo(c, zobj, eptr != NULL);
        sptr = ziplistNext(zl, eptr);

        while (rangelen--) {
            serverAssertWithInfo(c, zobj, eptr != NULL && sptr != NULL);
            serverAssertWithInfo(c, zobj, ziplistGet(eptr, &vstr, &vlen, &vlong));

            if (withscores && c->resp > 2) addReplyArrayLen(c, 2);
            if (vstr == NULL)
                addReplyBulkLongLong(c, vlong);
            else
                addReplyBulkCBuffer(c, vstr, vlen);
            if (withscores) addReplyDouble(c, zzlGetScore(sptr));

            if (reverse)
                zzlPrev(zl, &eptr, &sptr);
            else
                zzlNext(zl, &eptr, &sptr);
        }

    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        sds ele;

        /* Check if starting point is trivial, before doing log(N) lookup. */
        if (reverse) {
            ln = zsl->tail;
            if (start > 0)
                ln = zslGetElementByRank(zsl, llen - start);
        } else {
            ln = zsl->header->level[0].forward;
            if (start > 0)
                ln = zslGetElementByRank(zsl, start + 1);
        }

        while (rangelen--) {
            serverAssertWithInfo(c, zobj, ln != NULL);
            ele = ln->ele;
            if (withscores && c->resp > 2) addReplyArrayLen(c, 2);
            addReplyBulkCBuffer(c, ele, sdslen(ele));
            if (withscores) addReplyDouble(c, ln->score);
            ln = reverse ? ln->backward : ln->level[0].forward;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
}

void zrangeCommand(client *c) {
    zrangeGenericCommand(c, 0);
}

void zrevrangeCommand(client *c) {
    zrangeGenericCommand(c, 1);
}

/* This command implements ZRANGEBYSCORE, ZREVRANGEBYSCORE. */
void genericZrangebyscoreCommand(client *c, int reverse) {
    zrangespec range;
    robj *key = c->argv[1];
    robj *zobj;
    long offset = 0, limit = -1;
    int withscores = 0;
    unsigned long rangelen = 0;
    void *replylen = NULL;
    int minidx, maxidx;

    /* Parse the range arguments. */
    if (reverse) {
        /* Range is given as [max,min] */
        maxidx = 2;
        minidx = 3;
    } else {
        /* Range is given as [min,max] */
        minidx = 2;
        maxidx = 3;
    }

    if (zslParseRange(c->argv[minidx], c->argv[maxidx], &range) != C_OK) {
        addReplyError(c, "min or max is not a float");
        return;
    }

    /* Parse optional extra arguments. Note that ZCOUNT will exactly have
     * 4 arguments, so we'll never enter the following code path. */
    if (c->argc > 4) {
        int remaining = c->argc - 4;
        int pos = 4;

        while (remaining) {
            if (remaining >= 1 && !strcasecmp(c->argv[pos]->ptr, "withscores")) {
                pos++;
                remaining--;
                withscores = 1;
            } else if (remaining >= 3 && !strcasecmp(c->argv[pos]->ptr, "limit")) {
                if ((getLongFromObjectOrReply(c, c->argv[pos + 1], &offset, NULL)
                     != C_OK) ||
                    (getLongFromObjectOrReply(c, c->argv[pos + 2], &limit, NULL)
                     != C_OK)) {
                    return;
                }
                pos += 3;
                remaining -= 3;
            } else {
                addReply(c, shared.syntaxerr);
                return;
            }
        }
    }

    /* Ok, lookup the key and get the range */
    if ((zobj = lookupKeyReadOrReply(c, key, shared.emptyarray)) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
        return;

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        double score;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            eptr = zzlLastInRange(zl, &range);
        } else {
            eptr = zzlFirstInRange(zl, &range);
        }

        /* No "first" element in the specified interval. */
        if (eptr == NULL) {
            addReply(c, shared.emptyarray);
            return;
        }

        /* Get score pointer for the first element. */
        serverAssertWithInfo(c, zobj, eptr != NULL);
        sptr = ziplistNext(zl, eptr);

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addReplyDeferredLen(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl, &eptr, &sptr);
            } else {
                zzlNext(zl, &eptr, &sptr);
            }
        }

        while (eptr && limit--) {
            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zslValueGteMin(score, &range)) break;
            } else {
                if (!zslValueLteMax(score, &range)) break;
            }

            /* We know the element exists, so ziplistGet should always
             * succeed */
            serverAssertWithInfo(c, zobj, ziplistGet(eptr, &vstr, &vlen, &vlong));

            rangelen++;
            if (withscores && c->resp > 2) addReplyArrayLen(c, 2);
            if (vstr == NULL) {
                addReplyBulkLongLong(c, vlong);
            } else {
                addReplyBulkCBuffer(c, vstr, vlen);
            }
            if (withscores) addReplyDouble(c, score);

            /* Move to next node */
            if (reverse) {
                zzlPrev(zl, &eptr, &sptr);
            } else {
                zzlNext(zl, &eptr, &sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            ln = zslLastInRange(zsl, &range);
        } else {
            ln = zslFirstInRange(zsl, &range);
        }

        /* No "first" element in the specified interval. */
        if (ln == NULL) {
            addReply(c, shared.emptyarray);
            return;
        }

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addReplyDeferredLen(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zslValueGteMin(ln->score, &range)) break;
            } else {
                if (!zslValueLteMax(ln->score, &range)) break;
            }

            rangelen++;
            if (withscores && c->resp > 2) addReplyArrayLen(c, 2);
            addReplyBulkCBuffer(c, ln->ele, sdslen(ln->ele));
            if (withscores) addReplyDouble(c, ln->score);

            /* Move to next node */
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    if (withscores && c->resp == 2) rangelen *= 2;
    setDeferredArrayLen(c, replylen, rangelen);
}

void zrangebyscoreCommand(client *c) {
    genericZrangebyscoreCommand(c, 0);
}

void zrevrangebyscoreCommand(client *c) {
    genericZrangebyscoreCommand(c, 1);
}

void zcountCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zrangespec range;
    unsigned long count = 0;

    /* Parse the range arguments */
    if (zslParseRange(c->argv[2], c->argv[3], &range) != C_OK) {
        addReplyError(c, "min or max is not a float");
        return;
    }

    /* Lookup the sorted set */
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
        return;

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        double score;

        /* Use the first element in range as the starting point */
        eptr = zzlFirstInRange(zl, &range);

        /* No "first" element */
        if (eptr == NULL) {
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        sptr = ziplistNext(zl, eptr);
        score = zzlGetScore(sptr);
        serverAssertWithInfo(c, zobj, zslValueLteMax(score, &range));

        /* Iterate over elements in range */
        while (eptr) {
            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            if (!zslValueLteMax(score, &range)) {
                break;
            } else {
                count++;
                zzlNext(zl, &eptr, &sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        zn = zslFirstInRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        if (zn != NULL) {
            rank = zslGetRank(zsl, zn->score, zn->ele);
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            zn = zslLastInRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            if (zn != NULL) {
                rank = zslGetRank(zsl, zn->score, zn->ele);
                count -= (zsl->length - rank);
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    addReplyLongLong(c, count);
}

void zlexcountCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zlexrangespec range;
    unsigned long count = 0;

    /* Parse the range arguments */
    if (zslParseLexRange(c->argv[2], c->argv[3], &range) != C_OK) {
        addReplyError(c, "min or max not valid string range item");
        return;
    }

    /* Lookup the sorted set */
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET)) {
        zslFreeLexRange(&range);
        return;
    }

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        /* Use the first element in range as the starting point */
        eptr = zzlFirstInLexRange(zl, &range);

        /* No "first" element */
        if (eptr == NULL) {
            zslFreeLexRange(&range);
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        sptr = ziplistNext(zl, eptr);
        serverAssertWithInfo(c, zobj, zzlLexValueLteMax(eptr, &range));

        /* Iterate over elements in range */
        while (eptr) {
            /* Abort when the node is no longer in range. */
            if (!zzlLexValueLteMax(eptr, &range)) {
                break;
            } else {
                count++;
                zzlNext(zl, &eptr, &sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        zn = zslFirstInLexRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        if (zn != NULL) {
            rank = zslGetRank(zsl, zn->score, zn->ele);
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            zn = zslLastInLexRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            if (zn != NULL) {
                rank = zslGetRank(zsl, zn->score, zn->ele);
                count -= (zsl->length - rank);
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    zslFreeLexRange(&range);
    addReplyLongLong(c, count);
}

/* This command implements ZRANGEBYLEX, ZREVRANGEBYLEX. */
void genericZrangebylexCommand(client *c, int reverse) {
    zlexrangespec range;
    robj *key = c->argv[1];
    robj *zobj;
    long offset = 0, limit = -1;
    unsigned long rangelen = 0;
    void *replylen = NULL;
    int minidx, maxidx;

    /* Parse the range arguments. */
    if (reverse) {
        /* Range is given as [max,min] */
        maxidx = 2;
        minidx = 3;
    } else {
        /* Range is given as [min,max] */
        minidx = 2;
        maxidx = 3;
    }

    if (zslParseLexRange(c->argv[minidx], c->argv[maxidx], &range) != C_OK) {
        addReplyError(c, "min or max not valid string range item");
        return;
    }

    /* Parse optional extra arguments. Note that ZCOUNT will exactly have
     * 4 arguments, so we'll never enter the following code path. */
    if (c->argc > 4) {
        int remaining = c->argc - 4;
        int pos = 4;

        while (remaining) {
            if (remaining >= 3 && !strcasecmp(c->argv[pos]->ptr, "limit")) {
                if ((getLongFromObjectOrReply(c, c->argv[pos + 1], &offset, NULL) != C_OK) ||
                    (getLongFromObjectOrReply(c, c->argv[pos + 2], &limit, NULL) != C_OK)) {
                    zslFreeLexRange(&range);
                    return;
                }
                pos += 3;
                remaining -= 3;
            } else {
                zslFreeLexRange(&range);
                addReply(c, shared.syntaxerr);
                return;
            }
        }
    }

    /* Ok, lookup the key and get the range */
    if ((zobj = lookupKeyReadOrReply(c, key, shared.emptyarray)) == NULL ||
        checkType(c, zobj, OBJ_ZSET)) {
        zslFreeLexRange(&range);
        return;
    }

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            eptr = zzlLastInLexRange(zl, &range);
        } else {
            eptr = zzlFirstInLexRange(zl, &range);
        }

        /* No "first" element in the specified interval. */
        if (eptr == NULL) {
            addReply(c, shared.emptyarray);
            zslFreeLexRange(&range);
            return;
        }

        /* Get score pointer for the first element. */
        serverAssertWithInfo(c, zobj, eptr != NULL);
        sptr = ziplistNext(zl, eptr);

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addReplyDeferredLen(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl, &eptr, &sptr);
            } else {
                zzlNext(zl, &eptr, &sptr);
            }
        }

        while (eptr && limit--) {
            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zzlLexValueGteMin(eptr, &range)) break;
            } else {
                if (!zzlLexValueLteMax(eptr, &range)) break;
            }

            /* We know the element exists, so ziplistGet should always
             * succeed. */
            serverAssertWithInfo(c, zobj, ziplistGet(eptr, &vstr, &vlen, &vlong));

            rangelen++;
            if (vstr == NULL) {
                addReplyBulkLongLong(c, vlong);
            } else {
                addReplyBulkCBuffer(c, vstr, vlen);
            }

            /* Move to next node */
            if (reverse) {
                zzlPrev(zl, &eptr, &sptr);
            } else {
                zzlNext(zl, &eptr, &sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        if (reverse) {
            ln = zslLastInLexRange(zsl, &range);
        } else {
            ln = zslFirstInLexRange(zsl, &range);
        }

        /* No "first" element in the specified interval. */
        if (ln == NULL) {
            addReply(c, shared.emptyarray);
            zslFreeLexRange(&range);
            return;
        }

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        replylen = addReplyDeferredLen(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            if (reverse) {
                if (!zslLexValueGteMin(ln->ele, &range)) break;
            } else {
                if (!zslLexValueLteMax(ln->ele, &range)) break;
            }

            rangelen++;
            addReplyBulkCBuffer(c, ln->ele, sdslen(ln->ele));

            /* Move to next node */
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    zslFreeLexRange(&range);
    setDeferredArrayLen(c, replylen, rangelen);
}

void zrangebylexCommand(client *c) {
    genericZrangebylexCommand(c, 0);
}

void zrevrangebylexCommand(client *c) {
    genericZrangebylexCommand(c, 1);
}

void zcardCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;

    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
        return;

    addReplyLongLong(c, zsetLength(zobj));
}

void zscoreCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    double score;

    if ((zobj = lookupKeyReadOrReply(c, key, shared.null[c->resp])) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
        return;

    if (zsetScore(zobj, c->argv[2]->ptr, &score) == C_ERR) {
        addReplyNull(c);
    } else {
        addReplyDouble(c, score);
    }
}

void zrankGenericCommand(client *c, int reverse) {
    robj *key = c->argv[1];
    robj *ele = c->argv[2];
    robj *zobj;
    long rank;

    if ((zobj = lookupKeyReadOrReply(c, key, shared.null[c->resp])) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
        return;

    serverAssertWithInfo(c, ele, sdsEncodedObject(ele));
    rank = zsetRank(zobj, ele->ptr, reverse);
    if (rank >= 0) {
        addReplyLongLong(c, rank);
    } else {
        addReplyNull(c);
    }
}

void zrankCommand(client *c) {
    zrankGenericCommand(c, 0);
}

void zrevrankCommand(client *c) {
    zrankGenericCommand(c, 1);
}

void zscanCommand(client *c) {
    robj *o;
    unsigned long cursor;

    if (parseScanCursorOrReply(c, c->argv[2], &cursor) == C_ERR) return;
    if ((o = lookupKeyReadOrReply(c, c->argv[1], shared.emptyscan)) == NULL ||
        checkType(c, o, OBJ_ZSET))
        return;
    scanGenericCommand(c, o, cursor);
}

/* This command implements the generic zpop operation, used by:
 * ZPOPMIN, ZPOPMAX, BZPOPMIN and BZPOPMAX. This function is also used
 * inside blocked.c in the unblocking stage of BZPOPMIN and BZPOPMAX.
 *
 * If 'emitkey' is true also the key name is emitted, useful for the blocking
 * behavior of BZPOP[MIN|MAX], since we can block into multiple keys.
 *
 * The synchronous version instead does not need to emit the key, but may
 * use the 'count' argument to return multiple items if available.
 * 根据where从redisZset中取出一个元素 并返回给client
 * */
void genericZpopCommand(client *c, robj **keyv, int keyc, int where, int emitkey, robj *countarg) {
    int idx;
    robj *key = NULL;
    robj *zobj = NULL;
    sds ele;
    double score;
    long count = 1;

    /* If a count argument as passed, parse it or return an error.
     * TODO
     * */
    if (countarg) {
        if (getLongFromObjectOrReply(c, countarg, &count, NULL) != C_OK)
            return;
        if (count <= 0) {
            addReply(c, shared.emptyarray);
            return;
        }
    }

    /* Check type and break on the first error, otherwise identify candidate.
     * 该方法允许传入一组key 这里找到第一个有效的key
     * */
    idx = 0;
    while (idx < keyc) {
        key = keyv[idx++];
        zobj = lookupKeyWrite(c->db, key);
        if (!zobj) continue;
        if (checkType(c, zobj, OBJ_ZSET)) return;
        break;
    }

    /* No candidate for zpopping, return empty.
     * 代表所有的key都没有找到匹配的zset
     * */
    if (!zobj) {
        addReply(c, shared.emptyarray);
        return;
    }

    void *arraylen_ptr = addReplyDeferredLen(c);
    long arraylen = 0;

    /* We emit the key only for the blocking variant.
     * 代表需要将key信息一并返回
     * */
    if (emitkey) addReplyBulk(c, key);

    /* Remove the element. */
    do {
        if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
            unsigned char *zl = zobj->ptr;
            unsigned char *eptr, *sptr;
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;

            /* Get the first or last element in the sorted set. */
            eptr = ziplistIndex(zl, where == ZSET_MAX ? -2 : 0);
            serverAssertWithInfo(c, zobj, eptr != NULL);
            serverAssertWithInfo(c, zobj, ziplistGet(eptr, &vstr, &vlen, &vlong));
            if (vstr == NULL)
                ele = sdsfromlonglong(vlong);
            else
                ele = sdsnewlen(vstr, vlen);

            /* Get the score. */
            sptr = ziplistNext(zl, eptr);
            serverAssertWithInfo(c, zobj, sptr != NULL);
            score = zzlGetScore(sptr);
        } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = zobj->ptr;
            zskiplist *zsl = zs->zsl;
            zskiplistNode *zln;

            /* Get the first or last element in the sorted set. */
            zln = (where == ZSET_MAX ? zsl->tail :
                   zsl->header->level[0].forward);

            /* There must be an element in the sorted set. */
            serverAssertWithInfo(c, zobj, zln != NULL);
            ele = sdsdup(zln->ele);
            score = zln->score;
        } else {
            serverPanic("Unknown sorted set encoding");
        }

        serverAssertWithInfo(c, zobj, zsetDel(zobj, ele));
        // 因为对象发生了变化 增加dirty计数器
        server.dirty++;

        // 如果本次会获取多个元素 只产生一次事件
        if (arraylen == 0) { /* Do this only for the first iteration. */
            char *events[2] = {"zpopmin", "zpopmax"};
            notifyKeyspaceEvent(NOTIFY_ZSET, events[where], key, c->db->id);
            signalModifiedKey(c, c->db, key);
        }

        // 依次将zset的字面量以及score返回
        addReplyBulkCBuffer(c, ele, sdslen(ele));
        addReplyDouble(c, score);
        sdsfree(ele);
        arraylen += 2;

        /* Remove the key, if indeed needed. */
        if (zsetLength(zobj) == 0) {
            dbDelete(c->db, key);
            notifyKeyspaceEvent(NOTIFY_GENERIC, "del", key, c->db->id);
            break;
        }
    } while (--count);

    setDeferredArrayLen(c, arraylen_ptr, arraylen + (emitkey != 0));
}

/* ZPOPMIN key [<count>] */
void zpopminCommand(client *c) {
    if (c->argc > 3) {
        addReply(c, shared.syntaxerr);
        return;
    }
    genericZpopCommand(c, &c->argv[1], 1, ZSET_MIN, 0,
                       c->argc == 3 ? c->argv[2] : NULL);
}

/* ZMAXPOP key [<count>] */
void zpopmaxCommand(client *c) {
    if (c->argc > 3) {
        addReply(c, shared.syntaxerr);
        return;
    }
    genericZpopCommand(c, &c->argv[1], 1, ZSET_MAX, 0,
                       c->argc == 3 ? c->argv[2] : NULL);
}

/* BZPOPMIN / BZPOPMAX actual implementation. */
void blockingGenericZpopCommand(client *c, int where) {
    robj *o;
    mstime_t timeout;
    int j;

    if (getTimeoutFromObjectOrReply(c, c->argv[c->argc - 1], &timeout, UNIT_SECONDS)
        != C_OK)
        return;

    for (j = 1; j < c->argc - 1; j++) {
        o = lookupKeyWrite(c->db, c->argv[j]);
        if (o != NULL) {
            if (o->type != OBJ_ZSET) {
                addReply(c, shared.wrongtypeerr);
                return;
            } else {
                if (zsetLength(o) != 0) {
                    /* Non empty zset, this is like a normal ZPOP[MIN|MAX]. */
                    genericZpopCommand(c, &c->argv[j], 1, where, 1, NULL);
                    /* Replicate it as an ZPOP[MIN|MAX] instead of BZPOP[MIN|MAX]. */
                    rewriteClientCommandVector(c, 2,
                                               where == ZSET_MAX ? shared.zpopmax : shared.zpopmin,
                                               c->argv[j]);
                    return;
                }
            }
        }
    }

    /* If we are inside a MULTI/EXEC and the zset is empty the only thing
     * we can do is treating it as a timeout (even with timeout 0). */
    if (c->flags & CLIENT_MULTI) {
        addReplyNullArray(c);
        return;
    }

    /* If the keys do not exist we must block */
    blockForKeys(c, BLOCKED_ZSET, c->argv + 1, c->argc - 2, timeout, NULL, NULL);
}

// BZPOPMIN key [key ...] timeout
void bzpopminCommand(client *c) {
    blockingGenericZpopCommand(c, ZSET_MIN);
}

// BZPOPMAX key [key ...] timeout
void bzpopmaxCommand(client *c) {
    blockingGenericZpopCommand(c, ZSET_MAX);
}
