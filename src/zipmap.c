/* String -> String Map data structure optimized for size.
 * This file implements a data structure mapping strings to other strings
 * implementing an O(n) lookup data structure designed to be very memory
 * efficient.
 *
 * The Redis Hash type uses this data structure for hashes composed of a small
 * number of elements, to switch to a hash table once a given number of
 * elements is reached.
 *
 * Given that many times Redis Hashes are used to represent objects composed
 * of few fields, this is a very big win in terms of used memory.
 *
 * --------------------------------------------------------------------------
 *
 * Copyright (c) 2009-2010, Salvatore Sanfilippo <antirez at gmail dot com>
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

/* Memory layout of a zipmap, for the map "foo" => "bar", "hello" => "world":
 *
 * <zmlen><len>"foo"<len><free>"bar"<len>"hello"<len><free>"world"
 *
 * <zmlen> is 1 byte length that holds the current size of the zipmap.
 * When the zipmap length is greater than or equal to 254, this value
 * is not used and the zipmap needs to be traversed to find out the length.
 *
 * <len> is the length of the following string (key or value).
 * <len> lengths are encoded in a single value or in a 5 bytes value.
 * If the first byte value (as an unsigned 8 bit value) is between 0 and
 * 253, it's a single-byte length. If it is 254 then a four bytes unsigned
 * integer follows (in the host byte ordering). A value of 255 is used to
 * signal the end of the hash.
 *
 * <free> is the number of free unused bytes after the string, resulting
 * from modification of values associated to a key. For instance if "foo"
 * is set to "bar", and later "foo" will be set to "hi", it will have a
 * free byte to use if the value will enlarge again later, or even in
 * order to add a key/value pair if it fits.
 *
 * <free> is always an unsigned 8 bit number, because if after an
 * update operation there are more than a few free bytes, the zipmap will be
 * reallocated to make sure it is as small as possible.
 *
 * The most compact representation of the above two elements hash is actually:
 *
 * "\x02\x03foo\x03\x00bar\x05hello\x05\x00world\xff"
 *
 * Note that because keys and values are prefixed length "objects",
 * the lookup will take O(N) where N is the number of elements
 * in the zipmap and *not* the number of bytes needed to represent the zipmap.
 * This lowers the constant times considerably.
 */

#include <stdio.h>
#include <string.h>
#include "zmalloc.h"
#include "endianconv.h"

#define ZIPMAP_BIGLEN 254
#define ZIPMAP_END 255

/* The following defines the max value for the <free> field described in the
 * comments above, that is, the max number of trailing bytes in a value. */
#define ZIPMAP_VALUE_MAX_FREE 4

/* The following macro returns the number of bytes needed to encode the length
 * for the integer value _l, that is, 1 byte for lengths < ZIPMAP_BIGLEN and
 * 5 bytes for all the other lengths.
 * 如果长度小于ZIPMAP_BIGLEN 返回1字节 否则返回1+4
 * */
#define ZIPMAP_LEN_BYTES(_l) (((_l) < ZIPMAP_BIGLEN) ? 1 : sizeof(unsigned int)+1)

/* Create a new empty zipmap.
 * 初始化一个zipmap对象
 * */
unsigned char *zipmapNew(void) {
    unsigned char *zm = zmalloc(2);

    zm[0] = 0; /* Length */
    zm[1] = ZIPMAP_END;
    return zm;
}

/* Decode the encoded length pointed by 'p'
 * 存储每对键值对的方式是  keylen+key+vallen+val
 * */
static unsigned int zipmapDecodeLength(unsigned char *p) {
    unsigned int len = *p;

    // 1字节或者4字节
    if (len < ZIPMAP_BIGLEN) return len;
    memcpy(&len,p+1,sizeof(unsigned int));
    memrev32ifbe(&len);
    return len;
}

static unsigned int zipmapGetEncodedLengthSize(unsigned char *p) {
    return (*p < ZIPMAP_BIGLEN) ? 1: 5;
}

/* Encode the length 'l' writing it in 'p'. If p is NULL it just returns
 * the amount of bytes required to encode such a length.
 * 读取长度信息 如果长度需要4字节来表示那么还额外需要1个字节表示本次长度不是1字节
 * */
static unsigned int zipmapEncodeLength(unsigned char *p, unsigned int len) {
    if (p == NULL) {
        return ZIPMAP_LEN_BYTES(len);
    } else {
        if (len < ZIPMAP_BIGLEN) {
            p[0] = len;
            return 1;
        } else {
            p[0] = ZIPMAP_BIGLEN;
            memcpy(p+1,&len,sizeof(len));
            memrev32ifbe(p+1);
            return 1+sizeof(len);
        }
    }
}

/* Search for a matching key, returning a pointer to the entry inside the
 * zipmap. Returns NULL if the key is not found.
 *
 * If NULL is returned, and totlen is not NULL, it is set to the entire
 * size of the zipmap, so that the calling function will be able to
 * reallocate the original zipmap to make room for more entries.
 * 查询某个key是否存在
 * */
static unsigned char *zipmapLookupRaw(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned int *totlen) {
    // zm[0]记录键值对 总数 这里需要跳过
    unsigned char *p = zm+1, *k = NULL;
    unsigned int l,llen;

    // 从起始位置开始不断迭代
    while(*p != ZIPMAP_END) {
        unsigned char free;

        /* Match or skip the key
         * 解析key的长度
         * */
        l = zipmapDecodeLength(p);
        // 解析实际长度 因为为了表示本次长度是4字节 还需要额外1个字节
        llen = zipmapEncodeLength(NULL,l);
        // 这里是比较本次要插入的key 与原来的key是否一致
        if (key != NULL && k == NULL && l == klen && !memcmp(p+llen,key,l)) {
            /* Only return when the user doesn't care
             * for the total length of the zipmap.
             * 如果本次对总长度感兴趣 先记录下匹配的key后 继续往下遍历
             * */
            if (totlen != NULL) {
                k = p;
            } else {
                // 代表找到了目标key 直接返回
                return p;
            }
        }
        // 更新此时的p指针 同时也在更新长度信息
        p += llen+l;
        /* Skip the value as well
         * 读取valuelen
         * */
        l = zipmapDecodeLength(p);
        // 读取len编码后的总长度
        p += zipmapEncodeLength(NULL,l);
        // 这里跳过value  value 总计由3个部分组成  valuelen free value
        free = p[0];
        p += l+1+free; /* +1 to skip the free byte */
    }
    if (totlen != NULL) *totlen = (unsigned int)(p-zm)+1;
    return k;
}

/**
 * 这里是估算map插入键值对后 需要增加多少长度
 * @param klen
 * @param vlen
 * @return
 */
static unsigned long zipmapRequiredLength(unsigned int klen, unsigned int vlen) {
    unsigned int l;

    // 假设 keylen和valen最少需要1字节 外加1字节的free
    l = klen+vlen+3;
    if (klen >= ZIPMAP_BIGLEN) l += 4;
    if (vlen >= ZIPMAP_BIGLEN) l += 4;
    return l;
}

/* Return the total amount used by a key (encoded length + payload) */
static unsigned int zipmapRawKeyLength(unsigned char *p) {
    unsigned int l = zipmapDecodeLength(p);
    return zipmapEncodeLength(NULL,l) + l;
}

/* Return the total amount used by a value
 * (encoded length + single byte free count + payload)
 * 记录valuelen消耗多少字节
 * */
static unsigned int zipmapRawValueLength(unsigned char *p) {
    // 获取valuelen 规则和key一样 1字节或者5字节
    unsigned int l = zipmapDecodeLength(p);
    unsigned int used;

    used = zipmapEncodeLength(NULL,l);
    // p[used] 是 free byte 推测记录的是key与value的长度差
    used += p[used] + 1 + l;
    return used;
}

/* If 'p' points to a key, this function returns the total amount of
 * bytes used to store this entry (entry = key + associated value + trailing
 * free space if any).
 * 从p指针开始 读取key+value的长度
 * */
static unsigned int zipmapRawEntryLength(unsigned char *p) {
    unsigned int l = zipmapRawKeyLength(p);
    return l + zipmapRawValueLength(p+l);
}

static inline unsigned char *zipmapResize(unsigned char *zm, unsigned int len) {
    zm = zrealloc(zm, len);
    zm[len-1] = ZIPMAP_END;
    return zm;
}

/* Set key to value, creating the key if it does not already exist.
 * If 'update' is not NULL, *update is set to 1 if the key was
 * already preset, otherwise to 0.
 * 将一组键值对插入到zmap中
 * @param update 记录本次是否发生了碰撞 (更新 or 插入)
 * */
unsigned char *zipmapSet(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned char *val, unsigned int vlen, int *update) {
    unsigned int zmlen, offset;
    // zipmapRequiredLength 预估表示该键值对 需要多少字节
    unsigned int freelen, reqlen = zipmapRequiredLength(klen,vlen);
    unsigned int empty, vempty;
    unsigned char *p;

    freelen = reqlen;
    if (update) *update = 0;
    // 查询原key是否存在
    p = zipmapLookupRaw(zm,key,klen,&zmlen);
    // 代表本次是插入操作 需要做扩容  并且此时 freelen等于reqlen
    if (p == NULL) {
        /* Key not found: enlarge */
        zm = zipmapResize(zm, zmlen+reqlen);
        p = zm+zmlen-1;
        zmlen = zmlen+reqlen;

        /* Increase zipmap length (this is an insert)
         * zm第一个元素记录的是键值对总数   只能表示255以内的entry
         * */
        if (zm[0] < ZIPMAP_BIGLEN) zm[0]++;
    } else {
        /* Key found. Is there enough space for the new value? */
        /* Compute the total length: */
        if (update) *update = 1;
        // 计算之前entry的大小
        freelen = zipmapRawEntryLength(p);
        // 代表需要进行扩容 注意当新value小于旧value时 不会进行缩容  因为反复的扩容/缩容会导致不必要的开销
        if (freelen < reqlen) {
            /* Store the offset of this key within the current zipmap, so
             * it can be resized. Then, move the tail backwards so this
             * pair fits at the current position. */
            offset = p-zm;
            zm = zipmapResize(zm, zmlen-freelen+reqlen);
            p = zm+offset;

            /* The +1 in the number of bytes to be moved is caused by the
             * end-of-zipmap byte. Note: the *original* zmlen is used.
             * 将数据后移
             * */
            memmove(p+reqlen, p+freelen, zmlen-(offset+freelen+1));
            zmlen = zmlen-freelen+reqlen;
            // 扩容后 就不需要记录value长度的前后差了
            freelen = reqlen;
        }
    }

    /* We now have a suitable block where the key/value entry can
     * be written. If there is too much free space, move the tail
     * of the zipmap a few bytes to the front and shrink the zipmap,
     * as we want zipmaps to be very space efficient.
     * 插入一个新的entry
     * */
    empty = freelen-reqlen;
    // 实际上进入到这里已经说明了是更新操作  否则empty为0   当变化量超过一定值时需要缩容  主要是避免频繁的缩容
    if (empty >= ZIPMAP_VALUE_MAX_FREE) {
        /* First, move the tail <empty> bytes to the front, then resize
         * the zipmap to be <empty> bytes smaller. */
        offset = p-zm;
        memmove(p+reqlen, p+freelen, zmlen-(offset+freelen+1));
        // 发生缩容和扩容后 内存都是紧凑的
        zmlen -= empty;
        zm = zipmapResize(zm, zmlen);
        p = zm+offset;
        vempty = 0;
    } else {
        // 本次内存紧凑 或者浪费的内存字节数小于4字节时  通过free存储信息
        vempty = empty;
    }

    /* Just write the key + value and we are done. */
    /* Key:
     * 填充key长度
     * */
    p += zipmapEncodeLength(p,klen);
    memcpy(p,key,klen);
    p += klen;
    /* Value:
     * 填充value长度
     * */
    p += zipmapEncodeLength(p,vlen);
    // 反过来说 valen存储的是value的真实长度 但是内存不一定紧凑 free就是用来记录内存空隙大小的 所以实际长度就是 vallen+valencodinglen+free+1
    // 此时free一般最多只存储 ((ZIPMAP_VALUE_MAX_FREE-1) 3)
    *p++ = vempty;
    memcpy(p,val,vlen);
    return zm;
}

/* Remove the specified key. If 'deleted' is not NULL the pointed integer is
 * set to 0 if the key was not found, to 1 if it was found and deleted.
 * 从zipmap中删除某组键值对
 * @param deleted 代表是否删除成功
 * */
unsigned char *zipmapDel(unsigned char *zm, unsigned char *key, unsigned int klen, int *deleted) {
    unsigned int zmlen, freelen;
    unsigned char *p = zipmapLookupRaw(zm,key,klen,&zmlen);

    // key存在
    if (p) {
        freelen = zipmapRawEntryLength(p);
        // 数据前移
        memmove(p, p+freelen, zmlen-((p-zm)+freelen+1));

        // 更新map size大小 以及entry数量
        zm = zipmapResize(zm, zmlen-freelen);

        /* Decrease zipmap length
         * 如果此时entry数量达到255以上 此时不能直接-- 因为255不代表真实数量
         * */
        if (zm[0] < ZIPMAP_BIGLEN) zm[0]--;

        if (deleted) *deleted = 1;
    } else {
        if (deleted) *deleted = 0;
    }
    return zm;
}

/* Call before iterating through elements via zipmapNext()
 * 跳过记录entry总数的slot
 * */
unsigned char *zipmapRewind(unsigned char *zm) {
    return zm+1;
}

/* This function is used to iterate through all the zipmap elements.
 * In the first call the first argument is the pointer to the zipmap + 1.
 * In the next calls what zipmapNext returns is used as first argument.
 * Example:
 *
 * unsigned char *i = zipmapRewind(my_zipmap);
 * while((i = zipmapNext(i,&key,&klen,&value,&vlen)) != NULL) {
 *     printf("%d bytes key at $p\n", klen, key);
 *     printf("%d bytes value at $p\n", vlen, value);
 * }
 *
 * 迭代获取下一个entry
 */
unsigned char *zipmapNext(unsigned char *zm, unsigned char **key, unsigned int *klen, unsigned char **value, unsigned int *vlen) {
    if (zm[0] == ZIPMAP_END) return NULL;
    if (key) {
        *key = zm;
        *klen = zipmapDecodeLength(zm);
        *key += ZIPMAP_LEN_BYTES(*klen);
    }
    zm += zipmapRawKeyLength(zm);
    if (value) {
        *value = zm+1;
        *vlen = zipmapDecodeLength(zm);
        *value += ZIPMAP_LEN_BYTES(*vlen);
    }
    zm += zipmapRawValueLength(zm);
    return zm;
}

/* Search a key and retrieve the pointer and len of the associated value.
 * If the key is found the function returns 1, otherwise 0.
 * 读取 value的长度和起始指针
 * */
int zipmapGet(unsigned char *zm, unsigned char *key, unsigned int klen, unsigned char **value, unsigned int *vlen) {
    unsigned char *p;

    if ((p = zipmapLookupRaw(zm,key,klen,NULL)) == NULL) return 0;
    p += zipmapRawKeyLength(p);
    *vlen = zipmapDecodeLength(p);
    *value = p + ZIPMAP_LEN_BYTES(*vlen) + 1;
    return 1;
}

/* Return 1 if the key exists, otherwise 0 is returned.
 * */
int zipmapExists(unsigned char *zm, unsigned char *key, unsigned int klen) {
    return zipmapLookupRaw(zm,key,klen,NULL) != NULL;
}

/* Return the number of entries inside a zipmap */
unsigned int zipmapLen(unsigned char *zm) {
    unsigned int len = 0;
    // 第一个元素直接代表entry总数 254以内直接读取这个就好 如果超过254 就需要手动遍历
    if (zm[0] < ZIPMAP_BIGLEN) {
        len = zm[0];
    } else {
        // 使用5字节
        unsigned char *p = zipmapRewind(zm);
        while((p = zipmapNext(p,NULL,NULL,NULL,NULL)) != NULL) len++;

        /* Re-store length if small enough
         * 当实际的entry数小于255时 才代表是真实数量 更新zm[0]的值
         * */
        if (len < ZIPMAP_BIGLEN) zm[0] = len;
    }
    return len;
}

/* Return the raw size in bytes of a zipmap, so that we can serialize
 * the zipmap on disk (or everywhere is needed) just writing the returned
 * amount of bytes of the C array starting at the zipmap pointer.
 * 计算总内存开销
 * */
size_t zipmapBlobLen(unsigned char *zm) {
    unsigned int totlen;
    zipmapLookupRaw(zm,NULL,0,&totlen);
    return totlen;
}

/* Validate the integrity of the data stracture.
 * when `deep` is 0, only the integrity of the header is validated.
 * when `deep` is 1, we scan all the entries one by one. */
int zipmapValidateIntegrity(unsigned char *zm, size_t size, int deep) {
#define OUT_OF_RANGE(p) ( \
        (p) < zm + 2 || \
        (p) > zm + size - 1)
    unsigned int l, s, e;

    /* check that we can actually read the header (or ZIPMAP_END). */
    if (size < 2)
        return 0;

    /* the last byte must be the terminator. */
    if (zm[size-1] != ZIPMAP_END)
        return 0;

    if (!deep)
        return 1;

    unsigned int count = 0;
    unsigned char *p = zm + 1; /* skip the count */
    while(*p != ZIPMAP_END) {
        /* read the field name length encoding type */
        s = zipmapGetEncodedLengthSize(p);
        /* make sure the entry length doesn't rech outside the edge of the zipmap */
        if (OUT_OF_RANGE(p+s))
            return 0;

        /* read the field name length */
        l = zipmapDecodeLength(p);
        p += s; /* skip the encoded field size */
        p += l; /* skip the field */

        /* make sure the entry doesn't rech outside the edge of the zipmap */
        if (OUT_OF_RANGE(p))
            return 0;

        /* read the value length encoding type */
        s = zipmapGetEncodedLengthSize(p);
        /* make sure the entry length doesn't rech outside the edge of the zipmap */
        if (OUT_OF_RANGE(p+s))
            return 0;

        /* read the value length */
        l = zipmapDecodeLength(p);
        p += s; /* skip the encoded value size*/
        e = *p++; /* skip the encoded free space (always encoded in one byte) */
        p += l+e; /* skip the value and free space */
        count++;

        /* make sure the entry doesn't rech outside the edge of the zipmap */
        if (OUT_OF_RANGE(p))
            return 0;
    }

    /* check that the count in the header is correct */
    if (zm[0] != ZIPMAP_BIGLEN && zm[0] != count)
        return 0;

    return 1;
#undef OUT_OF_RANGE
}

#ifdef REDIS_TEST
static void zipmapRepr(unsigned char *p) {
    unsigned int l;

    printf("{status %u}",*p++);
    while(1) {
        if (p[0] == ZIPMAP_END) {
            printf("{end}");
            break;
        } else {
            unsigned char e;

            l = zipmapDecodeLength(p);
            printf("{key %u}",l);
            p += zipmapEncodeLength(NULL,l);
            if (l != 0 && fwrite(p,l,1,stdout) == 0) perror("fwrite");
            p += l;

            l = zipmapDecodeLength(p);
            printf("{value %u}",l);
            p += zipmapEncodeLength(NULL,l);
            e = *p++;
            if (l != 0 && fwrite(p,l,1,stdout) == 0) perror("fwrite");
            p += l+e;
            if (e) {
                printf("[");
                while(e--) printf(".");
                printf("]");
            }
        }
    }
    printf("\n");
}

#define UNUSED(x) (void)(x)
int zipmapTest(int argc, char *argv[]) {
    unsigned char *zm;

    UNUSED(argc);
    UNUSED(argv);

    zm = zipmapNew();

    zm = zipmapSet(zm,(unsigned char*) "name",4, (unsigned char*) "foo",3,NULL);
    zm = zipmapSet(zm,(unsigned char*) "surname",7, (unsigned char*) "foo",3,NULL);
    zm = zipmapSet(zm,(unsigned char*) "age",3, (unsigned char*) "foo",3,NULL);
    zipmapRepr(zm);

    zm = zipmapSet(zm,(unsigned char*) "hello",5, (unsigned char*) "world!",6,NULL);
    zm = zipmapSet(zm,(unsigned char*) "foo",3, (unsigned char*) "bar",3,NULL);
    zm = zipmapSet(zm,(unsigned char*) "foo",3, (unsigned char*) "!",1,NULL);
    zipmapRepr(zm);
    zm = zipmapSet(zm,(unsigned char*) "foo",3, (unsigned char*) "12345",5,NULL);
    zipmapRepr(zm);
    zm = zipmapSet(zm,(unsigned char*) "new",3, (unsigned char*) "xx",2,NULL);
    zm = zipmapSet(zm,(unsigned char*) "noval",5, (unsigned char*) "",0,NULL);
    zipmapRepr(zm);
    zm = zipmapDel(zm,(unsigned char*) "new",3,NULL);
    zipmapRepr(zm);

    printf("\nLook up large key:\n");
    {
        unsigned char buf[512];
        unsigned char *value;
        unsigned int vlen, i;
        for (i = 0; i < 512; i++) buf[i] = 'a';

        zm = zipmapSet(zm,buf,512,(unsigned char*) "long",4,NULL);
        if (zipmapGet(zm,buf,512,&value,&vlen)) {
            printf("  <long key> is associated to the %d bytes value: %.*s\n",
                vlen, vlen, value);
        }
    }

    printf("\nPerform a direct lookup:\n");
    {
        unsigned char *value;
        unsigned int vlen;

        if (zipmapGet(zm,(unsigned char*) "foo",3,&value,&vlen)) {
            printf("  foo is associated to the %d bytes value: %.*s\n",
                vlen, vlen, value);
        }
    }
    printf("\nIterate through elements:\n");
    {
        unsigned char *i = zipmapRewind(zm);
        unsigned char *key, *value;
        unsigned int klen, vlen;

        while((i = zipmapNext(i,&key,&klen,&value,&vlen)) != NULL) {
            printf("  %d:%.*s => %d:%.*s\n", klen, klen, key, vlen, vlen, value);
        }
    }
    return 0;
}
#endif
