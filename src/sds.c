/* SDSLib 2.0 -- A C dynamic strings library
 *
 * Copyright (c) 2006-2015, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2015, Oran Agra
 * Copyright (c) 2015, Redis Labs, Inc
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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>
#include <limits.h>
#include "sds.h"
#include "sdsalloc.h"

const char *SDS_NOINIT = "SDS_NOINIT";

/**
 * 这里根据sds的不同大小 获取对应的结构体头部大小  头部大小也就是字符数组之前的数据
 * @param type
 * @return
 */
static inline int sdsHdrSize(char type) {
    switch (type & SDS_TYPE_MASK) {
        case SDS_TYPE_5:
            return sizeof(struct sdshdr5);
        case SDS_TYPE_8:
            return sizeof(struct sdshdr8);
        case SDS_TYPE_16:
            return sizeof(struct sdshdr16);
        case SDS_TYPE_32:
            return sizeof(struct sdshdr32);
        case SDS_TYPE_64:
            return sizeof(struct sdshdr64);
    }
    return 0;
}

/**
 * 根据本次请求生成的字符串长度 返回不同的SDS类型
 * @param string_size
 * @return
 */
static inline char sdsReqType(size_t string_size) {
    // 长度可以用5个byte表示
    if (string_size < 1 << 5)
        return SDS_TYPE_5;
    // 下面都是对应的类型
    if (string_size < 1 << 8)
        return SDS_TYPE_8;
    if (string_size < 1 << 16)
        return SDS_TYPE_16;
#if (LONG_MAX == LLONG_MAX)
    if (string_size < 1ll << 32)
        return SDS_TYPE_32;
    return SDS_TYPE_64;
#else
    return SDS_TYPE_32;
#endif
}

/* Create a new sds string with the content specified by the 'init' pointer
 * and 'initlen'.
 * If NULL is used for 'init' the string is initialized with zero bytes.
 * If SDS_NOINIT is used, the buffer is left uninitialized;
 *
 * The string is always null-termined (all the sds strings are, always) so
 * even if you create an sds string with:
 *
 * mystring = sdsnewlen("abc",3);
 *
 * You can print the string with printf() as there is an implicit \0 at the
 * end of the string. However the string is binary safe and can contain
 * \0 characters in the middle, as the length is stored in the sds header.
 * @param *init 对应字符串本身
 * @param initlen 字符串长度
 *
 * */
sds sdsnewlen(const void *init, size_t initlen) {
    void *sh;
    sds s;

    // 根据长度计算出sds的类型
    char type = sdsReqType(initlen);
    /* Empty strings are usually created in order to append. Use type 8
     * since type 5 is not good at this.
     * 如果起始长度为0 默认设置为8 */
    if (type == SDS_TYPE_5 && initlen == 0) type = SDS_TYPE_8;
    // 返回对应类型的长度
    int hdrlen = sdsHdrSize(type);
    unsigned char *fp; /* flags pointer. */

    // 这里就申请一个头部的大小 + 本次初始数据块对应的大小
    sh = s_malloc(hdrlen + initlen + 1);
    // 此时内存不足 无法继续分配
    if (sh == NULL) return NULL;

    // TODO 等用到再考虑这个特殊值
    if (init == SDS_NOINIT)
        init = NULL;
    else if (!init)
        // 将分配到的内存空间清0 因为c本身不具备垃圾回收功能 需要手动清理
        memset(sh, 0, hdrlen + initlen + 1);

    // 此时将s定位到了 header的后面 之后会从s开始拷贝数据
    s = (char *) sh + hdrlen;
    // s-1 对应type指针的位置
    fp = ((unsigned char *) s) - 1;

    // 根据本次不同的长度 走不同的分支
    switch (type) {
        case SDS_TYPE_5: {
            // 这样fp的高几位就记录了字符串的原始长度  低位则记录了sds的类型  先忽略type5
            *fp = type | (initlen << SDS_TYPE_BITS);
            break;
        }
        case SDS_TYPE_8: {
            // 该函数会将 sh指向sds的首位 这样方便通过 -> 直接进行赋值
            SDS_HDR_VAR(8, s);
            // 设置sds结构体中 初始大小和已经分配的大小
            sh->len = initlen;
            sh->alloc = initlen;
            *fp = type;
            break;
        }
            // 下面的操作都是一个套路 就是将参数替换了
        case SDS_TYPE_16: {
            SDS_HDR_VAR(16, s);
            sh->len = initlen;
            sh->alloc = initlen;
            *fp = type;
            break;
        }
        case SDS_TYPE_32: {
            SDS_HDR_VAR(32, s);
            sh->len = initlen;
            sh->alloc = initlen;
            *fp = type;
            break;
        }
        case SDS_TYPE_64: {
            SDS_HDR_VAR(64, s);
            sh->len = initlen;
            sh->alloc = initlen;
            *fp = type;
            break;
        }
    }
    // 将数据拷贝到新的数组 s 中
    if (initlen && init)
        memcpy(s, init, initlen);
    s[initlen] = '\0';
    return s;
}

/* Create an empty (zero length) sds string. Even in this case the string
 * always has an implicit null term.
 * 分配一个 type8的 sds
 * */
sds sdsempty(void) {
    return sdsnewlen("", 0);
}

/* Create a new sds string starting from a null terminated C string.
 * 根据传入的字符串长度 构建sds 当然字符串本身 也会被拷贝进去
 * */
sds sdsnew(const char *init) {
    size_t initlen = (init == NULL) ? 0 : strlen(init);
    return sdsnewlen(init, initlen);
}

/* Duplicate an sds string.
 * 注意这里传入的是sds的副本
 * 这里应该是假定 此时的偏移量已经在type后面了  可以看到在初始化过程中没有移动init指针 而是从init指针的当前位置直接拷贝数据
 * */
sds sdsdup(const sds s) {
    return sdsnewlen(s, sdslen(s));
}

/*
 * Free an sds string. No operation is performed if 's' is NULL.
 * 将某个sds释放 这里会连同header的部分一起释放
 * */
void sdsfree(sds s) {
    // NOOP
    if (s == NULL) return;
    // 因为sds实际上是 char*的别名 所以可以转换
    s_free((char *) s - sdsHdrSize(s[-1]));
}

/* Set the sds string length to the length as obtained with strlen(), so
 * considering as content only up to the first null term character.
 *
 * This function is useful when the sds string is hacked manually in some
 * way, like in the following example:
 *
 * s = sdsnew("foobar");
 * s[2] = '\0';
 * sdsupdatelen(s);
 * printf("%d\n", sdslen(s));
 *
 * The output will be "2", but if we comment out the call to sdsupdatelen()
 * the output will be "6" as the string was modified but the logical length
 * remains 6 bytes.
 * 根据传入的字符数组 更新sds需要的大小
 * */
void sdsupdatelen(sds s) {
    size_t reallen = strlen(s);
    sdssetlen(s, reallen);
}

/* Modify an sds string in-place to make it empty (zero length).
 * However all the existing buffer is not discarded but set as free space
 * so that next append operations will not require allocations up to the
 * number of bytes previously available. */
void sdsclear(sds s) {
    sdssetlen(s, 0);
    s[0] = '\0';
}

/* Enlarge the free space at the end of the sds string so that the caller
 * is sure that after calling this function can overwrite up to addlen
 * bytes after the end of the string, plus one more byte for nul term.
 *
 * Note: this does not change the *length* of the sds string as returned
 * by sdslen(), but only the free buffer space we have.
 * addlen 是本次需要分配的大小
 * */
sds sdsMakeRoomFor(sds s, size_t addlen) {
    void *sh, *newsh;
    // 获取sds结构 此时可用的大小空间
    size_t avail = sdsavail(s);
    size_t len, newlen;
    char type, oldtype = s[-1] & SDS_TYPE_MASK;
    int hdrlen;

    /* Return ASAP if there is enough space left.  此时有足够的空间 不需要做处理*/
    if (avail >= addlen) return s;

    // 计算此时sds 字符数组的使用大小
    len = sdslen(s);
    // 回到结构体的首位
    sh = (char *) s - sdsHdrSize(oldtype);
    newlen = (len + addlen);
    // 当长度在一定范围内 选择扩容成2倍
    if (newlen < SDS_MAX_PREALLOC)
        newlen *= 2;
    else
        // 当长度超过范围后 增加固定的部分
        newlen += SDS_MAX_PREALLOC;

    // 根据新的长度 判断需要什么样的 sds头部
    type = sdsReqType(newlen);

    /* Don't use type 5: the user is appending to the string and type 5 is
     * not able to remember empty space, so sdsMakeRoomFor() must be called
     * at every appending operation. */
    if (type == SDS_TYPE_5) type = SDS_TYPE_8;

    // 计算header的长度
    hdrlen = sdsHdrSize(type);
    // 代表本次扩容后 数据类型还是同一个 尝试重新调整之前分配的内存块 应该是操作系统本身的内存分块算法是XXX的倍数 所以type不变代表之前分配的内存没有利用完
    if (oldtype == type) {
        newsh = s_realloc(sh, hdrlen + newlen + 1);
        if (newsh == NULL) return NULL;
        // 指向字节数组的首部
        s = (char *) newsh + hdrlen;
    } else {
        /* Since the header size changes, need to move the string forward,
         * and can't use realloc */
        // 无法通过realloc 必须重新分配
        newsh = s_malloc(hdrlen + newlen + 1);
        if (newsh == NULL) return NULL;
        // 从新的字节数组首部开始拷贝数据
        memcpy((char *) newsh + hdrlen, s, len + 1);
        // sh指向sds header首位 现在是要将整个sds释放
        s_free(sh);
        // 更新s指针 并且更新sds的长度
        s = (char *) newsh + hdrlen;
        s[-1] = type;
        sdssetlen(s, len);
    }
    // 更新该sds已经使用的长度
    sdssetalloc(s, newlen);
    return s;
}

/* Reallocate the sds string so that it has no free space at the end. The
 * contained string remains not altered, but next concatenation operations
 * will require a reallocation.
 *
 * After the call, the passed sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call. ;
 * 移除掉多余的空间 实际上是一种缩容操作
 * */
sds sdsRemoveFreeSpace(sds s) {
    void *sh, *newsh;
    char type, oldtype = s[-1] & SDS_TYPE_MASK;
    int hdrlen, oldhdrlen = sdsHdrSize(oldtype);

    // 获取sds此时使用的长度
    size_t len = sdslen(s);
    size_t avail = sdsavail(s);

    // 将指针重置到sds.header 首位
    sh = (char *) s - oldhdrlen;

    /* Return ASAP if there is no space left. 此时已经没有空间可以移除 不需要处理 */
    if (avail == 0) return s;

    /* Check what would be the minimum SDS header that is just good enough to
     * fit this string.
     * 根据此时使用的长度 生成一种匹配的类型
     * */
    type = sdsReqType(len);
    hdrlen = sdsHdrSize(type);

    /* If the type is the same, or at least a large enough type is still
     * required, we just realloc(), letting the allocator to do the copy
     * only if really needed. Otherwise if the change is huge, we manually
     * reallocate the string to use the different header type.
     * 代表兑换的类型还是一致的 不需要特殊处理
     * */
    if (oldtype == type || type > SDS_TYPE_8) {
        // 这里其实并没有涉及到内存块的归还
        newsh = s_realloc(sh, oldhdrlen + len + 1);
        if (newsh == NULL) return NULL;
        // 指针指向数据数组首位
        s = (char *) newsh + oldhdrlen;
    } else {
        // 分配一个小内存块 用于设置sds
        newsh = s_malloc(hdrlen + len + 1);
        if (newsh == NULL) return NULL;
        // 进行数据拷贝
        memcpy((char *) newsh + hdrlen, s, len + 1);
        // 归还内存块
        s_free(sh);
        s = (char *) newsh + hdrlen;
        s[-1] = type;
        sdssetlen(s, len);
    }
    sdssetalloc(s, len);
    return s;
}

/* Return the total size of the allocation of the specified sds string,
 * including:
 * 1) The sds header before the pointer.
 * 2) The string.
 * 3) The free buffer at the end if any.
 * 4) The implicit null term.
 * 更新此时已经分配的长度
 */
size_t sdsAllocSize(sds s) {
    size_t alloc = sdsalloc(s);
    return sdsHdrSize(s[-1]) + alloc + 1;
}

/* Return the pointer of the actual SDS allocation (normally SDS strings
 * are referenced by the start of the string buffer).
 * 返回 sds首位的指针
 * */
void *sdsAllocPtr(sds s) {
    return (void *) (s - sdsHdrSize(s[-1]));
}

/* Increment the sds length and decrements the left free space at the
 * end of the string according to 'incr'. Also set the null term
 * in the new end of the string.
 *
 * This function is used in order to fix the string length after the
 * user calls sdsMakeRoomFor(), writes something after the end of
 * the current string, and finally needs to set the new length.
 *
 * Note: it is possible to use a negative increment in order to
 * right-trim the string.
 *
 * Usage example:
 *
 * Using sdsIncrLen() and sdsMakeRoomFor() it is possible to mount the
 * following schema, to cat bytes coming from the kernel to the end of an
 * sds string without copying into an intermediate buffer:
 *
 * oldlen = sdslen(s);
 * s = sdsMakeRoomFor(s, BUFFER_SIZE);
 * nread = read(fd, s+oldlen, BUFFER_SIZE);
 * ... check for nread <= 0 and handle it ...
 * sdsIncrLen(s, nread);
 * 为sds结构 增加指定的长度
 */
void sdsIncrLen(sds s, ssize_t incr) {
    unsigned char flags = s[-1];
    size_t len;
    switch (flags & SDS_TYPE_MASK) {
        case SDS_TYPE_5: {
            unsigned char *fp = ((unsigned char *) s) - 1;
            unsigned char oldlen = SDS_TYPE_5_LEN(flags);
            assert((incr > 0 && oldlen + incr < 32) || (incr < 0 && oldlen >= (unsigned int) (-incr)));
            *fp = SDS_TYPE_5 | ((oldlen + incr) << SDS_TYPE_BITS);
            len = oldlen + incr;
            break;
        }
        case SDS_TYPE_8: {
            SDS_HDR_VAR(8, s);
            assert((incr >= 0 && sh->alloc - sh->len >= incr) || (incr < 0 && sh->len >= (unsigned int) (-incr)));
            len = (sh->len += incr);
            break;
        }
        case SDS_TYPE_16: {
            SDS_HDR_VAR(16, s);
            assert((incr >= 0 && sh->alloc - sh->len >= incr) || (incr < 0 && sh->len >= (unsigned int) (-incr)));
            len = (sh->len += incr);
            break;
        }
        case SDS_TYPE_32: {
            SDS_HDR_VAR(32, s);
            assert((incr >= 0 && sh->alloc - sh->len >= (unsigned int) incr) ||
                   (incr < 0 && sh->len >= (unsigned int) (-incr)));
            len = (sh->len += incr);
            break;
        }
        case SDS_TYPE_64: {
            SDS_HDR_VAR(64, s);
            assert((incr >= 0 && sh->alloc - sh->len >= (uint64_t) incr) || (incr < 0 && sh->len >= (uint64_t)(-incr)));
            len = (sh->len += incr);
            break;
        }
        default:
            len = 0; /* Just to avoid compilation warnings. */
    }
    s[len] = '\0';
}

/* Grow the sds to have the specified length. Bytes that were not part of
 * the original length of the sds will be set to zero.
 *
 * if the specified length is smaller than the current length, no operation
 * is performed.
 * 确保sds此时有超过len的长度
 * */
sds sdsgrowzero(sds s, size_t len) {
    size_t curlen = sdslen(s);

    if (len <= curlen) return s;
    // 扩容
    s = sdsMakeRoomFor(s, len - curlen);
    if (s == NULL) return NULL;

    /* Make sure added region doesn't contain garbage
     * 将新开放的空间都重置成0
     * */
    memset(s + curlen, 0, (len - curlen + 1)); /* also set trailing \0 byte */
    sdssetlen(s, len);
    return s;
}

/* Append the specified binary-safe string pointed by 't' of 'len' bytes to the
 * end of the specified sds string 's'.
 *
 * After the call, the passed sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call.
 * 将 *t往后len的数据追加到sds上
 * */
sds sdscatlen(sds s, const void *t, size_t len) {
    size_t curlen = sdslen(s);

    // 确保有足够的空间
    s = sdsMakeRoomFor(s, len);
    if (s == NULL) return NULL;
    // 拷贝指定的长度
    memcpy(s + curlen, t, len);
    sdssetlen(s, curlen + len);
    s[curlen + len] = '\0';
    return s;
}

/* Append the specified null termianted C string to the sds string 's'.
 *
 * After the call, the passed sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call.
 * 字符串的拼接
 * */
sds sdscat(sds s, const char *t) {
    return sdscatlen(s, t, strlen(t));
}

/* Append the specified sds 't' to the existing sds 's'.
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call.
 * 将2个 sds拼接  一般传入时sds对应的是字节数组首位的位置 而不是header的首位
 * */
sds sdscatsds(sds s, const sds t) {
    return sdscatlen(s, t, sdslen(t));
}

/* Destructively modify the sds string 's' to hold the specified binary
 * safe string pointed by 't' of length 'len' bytes.
 * 将*t的数据拷贝到sds上 是覆盖操作
 * */
sds sdscpylen(sds s, const char *t, size_t len) {
    // 如果此时alloc小于len 那么可能总空间是不足的 需要尝试扩容
    if (sdsalloc(s) < len) {
        s = sdsMakeRoomFor(s, len - sdslen(s));
        if (s == NULL) return NULL;
    }
    // 调用c的原生api  因为sds是char*的别名 所以可以直接使用c的api
    memcpy(s, t, len);
    s[len] = '\0';
    sdssetlen(s, len);
    return s;
}

/* Like sdscpylen() but 't' must be a null-termined string so that the length
 * of the string is obtained with strlen(). */
sds sdscpy(sds s, const char *t) {
    return sdscpylen(s, t, strlen(t));
}

/* Helper for sdscatlonglong() doing the actual number -> string
 * conversion. 's' must point to a string with room for at least
 * SDS_LLSTR_SIZE bytes.
 *
 * The function returns the length of the null-terminated string
 * representation stored at 's'. */
#define SDS_LLSTR_SIZE 21

/**
 * 将sds 转换成string
 * @param s
 * @param value
 * @return
 */
int sdsll2str(char *s, long long value) {
    char *p, aux;
    unsigned long long v;
    size_t l;

    /* Generate the string representation, this method produces
     * an reversed string.
     * 确保value是一个正数 */
    v = (value < 0) ? -value : value;
    p = s;
    do {
        // 将long转换成 char 并追加到*s后面 而且是反向的
        *p++ = '0' + (v % 10);
        v /= 10;
    } while (v);
    if (value < 0) *p++ = '-';

    /* Compute length and add null term.
     * 计算写入的总长度
     * */
    l = p - s;
    // 补上终止符
    *p = '\0';

    /* Reverse the string. */
    p--;
    // 这里要开始推进s指针
    while (s < p) {
        // 这里又反转了字符串 那么相当于该方法本身就是将value写入到s中
        aux = *s;
        *s = *p;
        *p = aux;
        s++;
        p--;
    }
    return l;
}

/* Identical sdsll2str(), but for unsigned long long type. */
int sdsull2str(char *s, unsigned long long v) {
    char *p, aux;
    size_t l;

    /* Generate the string representation, this method produces
     * an reversed string. */
    p = s;
    do {
        *p++ = '0' + (v % 10);
        v /= 10;
    } while (v);

    /* Compute length and add null term. */
    l = p - s;
    *p = '\0';

    /* Reverse the string. */
    p--;
    while (s < p) {
        aux = *s;
        *s = *p;
        *p = aux;
        s++;
        p--;
    }
    return l;
}

/* Create an sds string from a long long value. It is much faster than:
 *
 * sdscatprintf(sdsempty(),"%lld\n", value);
 * 将value转换成 char数组后 又包装成sds结构
 */
sds sdsfromlonglong(long long value) {
    char buf[SDS_LLSTR_SIZE];
    int len = sdsll2str(buf, value);

    return sdsnewlen(buf, len);
}

/* Like sdscatprintf() but gets va_list instead of being variadic.
 * 打印方法先忽略
 * */
sds sdscatvprintf(sds s, const char *fmt, va_list ap) {
    va_list cpy;
    char staticbuf[1024], *buf = staticbuf, *t;
    size_t buflen = strlen(fmt) * 2;

    /* We try to start using a static buffer for speed.
     * If not possible we revert to heap allocation. */
    if (buflen > sizeof(staticbuf)) {
        buf = s_malloc(buflen);
        if (buf == NULL) return NULL;
    } else {
        buflen = sizeof(staticbuf);
    }

    /* Try with buffers two times bigger every time we fail to
     * fit the string in the current buffer size. */
    while (1) {
        buf[buflen - 2] = '\0';
        va_copy(cpy, ap);
        vsnprintf(buf, buflen, fmt, cpy);
        va_end(cpy);
        if (buf[buflen - 2] != '\0') {
            if (buf != staticbuf) s_free(buf);
            buflen *= 2;
            buf = s_malloc(buflen);
            if (buf == NULL) return NULL;
            continue;
        }
        break;
    }

    /* Finally concat the obtained string to the SDS string and return it. */
    t = sdscat(s, buf);
    if (buf != staticbuf) s_free(buf);
    return t;
}

/* Append to the sds string 's' a string obtained using printf-alike format
 * specifier.
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call.
 *
 * Example:
 *
 * s = sdsnew("Sum is: ");
 * s = sdscatprintf(s,"%d+%d = %d",a,b,a+b).
 *
 * Often you need to create a string from scratch with the printf-alike
 * format. When this is the need, just use sdsempty() as the target string:
 *
 * s = sdscatprintf(sdsempty(), "... your format ...", args);
 */
sds sdscatprintf(sds s, const char *fmt, ...) {
    va_list ap;
    char *t;
    va_start(ap, fmt);
    t = sdscatvprintf(s, fmt, ap);
    va_end(ap);
    return t;
}

/* This function is similar to sdscatprintf, but much faster as it does
 * not rely on sprintf() family functions implemented by the libc that
 * are often very slow. Moreover directly handling the sds string as
 * new data is concatenated provides a performance improvement.
 *
 * However this function only handles an incompatible subset of printf-alike
 * format specifiers:
 *
 * %s - C String
 * %S - SDS string
 * %i - signed int
 * %I - 64 bit signed integer (long long, int64_t)
 * %u - unsigned int
 * %U - 64 bit unsigned integer (unsigned long long, uint64_t)
 * %% - Verbatim "%" character.
 * 格式化输出 先忽略
 */
sds sdscatfmt(sds s, char const *fmt, ...) {
    size_t initlen = sdslen(s);
    const char *f = fmt;
    long i;
    va_list ap;

    /* To avoid continuous reallocations, let's start with a buffer that
     * can hold at least two times the format string itself. It's not the
     * best heuristic but seems to work in practice. */
    s = sdsMakeRoomFor(s, initlen + strlen(fmt) * 2);
    va_start(ap, fmt);
    f = fmt;    /* Next format specifier byte to process. */
    i = initlen; /* Position of the next byte to write to dest str. */
    while (*f) {
        char next, *str;
        size_t l;
        long long num;
        unsigned long long unum;

        /* Make sure there is always space for at least 1 char. */
        if (sdsavail(s) == 0) {
            s = sdsMakeRoomFor(s, 1);
        }

        switch (*f) {
            case '%':
                next = *(f + 1);
                f++;
                switch (next) {
                    case 's':
                    case 'S':
                        str = va_arg(ap,
                        char*);
                        l = (next == 's') ? strlen(str) : sdslen(str);
                        if (sdsavail(s) < l) {
                            s = sdsMakeRoomFor(s, l);
                        }
                        memcpy(s + i, str, l);
                        sdsinclen(s, l);
                        i += l;
                        break;
                    case 'i':
                    case 'I':
                        if (next == 'i')
                            num = va_arg(ap,
                        int);
                        else
                        num = va_arg(ap,
                        long long);
                        {
                            char buf[SDS_LLSTR_SIZE];
                            l = sdsll2str(buf, num);
                            if (sdsavail(s) < l) {
                                s = sdsMakeRoomFor(s, l);
                            }
                            memcpy(s + i, buf, l);
                            sdsinclen(s, l);
                            i += l;
                        }
                        break;
                    case 'u':
                    case 'U':
                        if (next == 'u')
                            unum = va_arg(ap,
                        unsigned int);
                        else
                        unum = va_arg(ap,
                        unsigned long long);
                        {
                            char buf[SDS_LLSTR_SIZE];
                            l = sdsull2str(buf, unum);
                            if (sdsavail(s) < l) {
                                s = sdsMakeRoomFor(s, l);
                            }
                            memcpy(s + i, buf, l);
                            sdsinclen(s, l);
                            i += l;
                        }
                        break;
                    default: /* Handle %% and generally %<unknown>. */
                        s[i++] = next;
                        sdsinclen(s, 1);
                        break;
                }
                break;
            default:
                s[i++] = *f;
                sdsinclen(s, 1);
                break;
        }
        f++;
    }
    va_end(ap);

    /* Add null-term */
    s[i] = '\0';
    return s;
}

/* Remove the part of the string from left and from right composed just of
 * contiguous characters found in 'cset', that is a null terminted C string.
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call.
 *
 * Example:
 *
 * s = sdsnew("AA...AA.a.aa.aHelloWorld     :::");
 * s = sdstrim(s,"Aa. :");
 * printf("%s\n", s);
 *
 * Output will be just "HelloWorld".
 * 从sds左右2边裁剪匹配的字节
 */
sds sdstrim(sds s, const char *cset) {
    char *start, *end, *sp, *ep;
    size_t len;

    sp = start = s;
    // 赋值尾指针
    ep = end = s + sdslen(s) - 1;
    // 找到第一个无法被删除的位置  因为*sp被cset包裹的话 都需要被移除
    while (sp <= end && strchr(cset, *sp)) sp++;
    // 代表从左往右有部分数据无法被移除  现在尝试从右往左找到第一个不匹配的数据
    while (ep > sp && strchr(cset, *ep)) ep--;

    // 代表裁剪后剩余的长度
    len = (sp > ep) ? 0 : ((ep - sp) + 1);
    // 从sp开始拷贝len长度的数据 覆盖旧数据
    if (s != sp) memmove(s, sp, len);
    s[len] = '\0';
    sdssetlen(s, len);
    return s;
}

/* Turn the string into a smaller (or equal) string containing only the
 * substring specified by the 'start' and 'end' indexes.
 *
 * start and end can be negative, where -1 means the last character of the
 * string, -2 the penultimate character, and so forth.
 *
 * The interval is inclusive, so the start and end characters will be part
 * of the resulting string.
 *
 * The string is modified in-place.
 *
 * Example:
 *
 * s = sdsnew("Hello World");
 * sdsrange(s,1,-1); => "ello World"
 * 获取范围内的数据  ssize_t 在宏替换后就是一个long
 */
void sdsrange(sds s, ssize_t start, ssize_t end) {
    size_t newlen, len = sdslen(s);

    // 当前sds内部无数据 直接返回
    if (len == 0) return;
    if (start < 0) {
        start = len + start;
        if (start < 0) start = 0;
    }
    if (end < 0) {
        end = len + end;
        if (end < 0) end = 0;
    }

    // 如果start > end 代表不需要保留数据
    newlen = (start > end) ? 0 : (end - start) + 1;
    if (newlen != 0) {
        // 起点超过总长度 无数据可获取
        if (start >= (ssize_t) len) {
            newlen = 0;
        } else if (end >= (ssize_t) len) {
            end = len - 1;
            newlen = (start > end) ? 0 : (end - start) + 1;
        }
    } else {
        start = 0;
    }
    // 也是数据拷贝
    if (start && newlen) memmove(s, s + start, newlen);
    s[newlen] = 0;
    sdssetlen(s, newlen);
}

/* Apply tolower() to every character of the sds string 's'. */
void sdstolower(sds s) {
    size_t len = sdslen(s), j;

    for (j = 0; j < len; j++) s[j] = tolower(s[j]);
}

/* Apply toupper() to every character of the sds string 's'. */
void sdstoupper(sds s) {
    size_t len = sdslen(s), j;

    for (j = 0; j < len; j++) s[j] = toupper(s[j]);
}

/* Compare two sds strings s1 and s2 with memcmp().
 *
 * Return value:
 *
 *     positive if s1 > s2.
 *     negative if s1 < s2.
 *     0 if s1 and s2 are exactly the same binary string.
 *
 * If two strings share exactly the same prefix, but one of the two has
 * additional characters, the longer string is considered to be greater than
 * the smaller one. */
int sdscmp(const sds s1, const sds s2) {
    size_t l1, l2, minlen;
    int cmp;

    l1 = sdslen(s1);
    l2 = sdslen(s2);
    minlen = (l1 < l2) ? l1 : l2;
    cmp = memcmp(s1, s2, minlen);
    if (cmp == 0) return l1 > l2 ? 1 : (l1 < l2 ? -1 : 0);
    return cmp;
}

/**
 * Split 's' with separator in 'sep'. An array
 * of sds strings is returned. *count will be set
 * by reference to the number of tokens returned.
 *
 * On out of memory, zero length string, zero length
 * separator, NULL is returned.
 *
 * Note that 'sep' is able to split a string using
 * a multi-character separator. For example
 * sdssplit("foo_-_bar","_-_"); will return two
 * elements "foo" and "bar".
 *
 * This version of the function is binary-safe but
 * requires length arguments. sdssplit() is just the
 * same function but for zero-terminated strings.
 * @param seq 分隔符
 * @param seplen 分隔符的长度
 * @param count 记录本次拆解出多少个token
 * 将sds的数据按照指定的分隔符进行拆分
 * 返回的是一个二级指针 char **  (sds 就是char*) 这里的意思是sds* 下每个元素都是sds 而每个sds有对应一个字符串 char*
 */
sds *sdssplitlen(const char *s, ssize_t len, const char *sep, int seplen, int *count) {
    // slot 对应存储拆分后的字符
    int elements = 0, slots = 5;
    long start = 0, j;
    sds *tokens;

    // 分隔符为空 或者sds内数据为空
    if (seplen < 1 || len < 0) return NULL;

    // 默认情况下就是返回一个 长度为5的字节数组
    tokens = s_malloc(sizeof(sds) * slots);
    if (tokens == NULL) return NULL;

    if (len == 0) {
        *count = 0;
        return tokens;
    }
    // 从首字节开始 匹配数据
    for (j = 0; j < (len - (seplen - 1)); j++) {
        /* make sure there is room for the next element and the final one
         * slots的空间可能不够了 需要进行扩容
         * */
        if (slots < elements + 2) {
            sds *newtokens;

            slots *= 2;
            newtokens = s_realloc(tokens, sizeof(sds) * slots);
            // 没有足够的空间
            if (newtokens == NULL) goto cleanup;
            tokens = newtokens;
        }
        /*
         * search the separator
         * 代表分隔符匹配成功
         * */
        if ((seplen == 1 && *(s + j) == sep[0]) || (memcmp(s + j, sep, seplen) == 0)) {
            // 当匹配成功后 将左侧的部分填充到数组中
            tokens[elements] = sdsnewlen(s + start, j - start);
            // 代表没有足够的空间分配sds了
            if (tokens[elements] == NULL) goto cleanup;
            elements++;
            start = j + seplen;
            // 跳过一个分隔符的长度
            j = j + seplen - 1; /* skip the separator */
        }
    }
    /*
     * Add the final element. We are sure there is room in the tokens array.
     * 剩余的字节填充到最后一个位置
     * */
    tokens[elements] = sdsnewlen(s + start, len - start);
    if (tokens[elements] == NULL) goto cleanup;
    elements++;
    *count = elements;
    return tokens;

    cleanup:
    {
        int i;
        for (i = 0; i < elements; i++) sdsfree(tokens[i]);
        s_free(tokens);
        *count = 0;
        return NULL;
    }
}

/*
 * Free the result returned by sdssplitlen(), or do nothing if 'tokens' is NULL.
 * 回收由 split拆解出来的token
 * */
void sdsfreesplitres(sds *tokens, int count) {
    if (!tokens) return;
    while (count--)
        sdsfree(tokens[count]);
    s_free(tokens);
}

/* Append to the sds string "s" an escaped string representation where
 * all the non-printable characters (tested with isprint()) are turned into
 * escapes in the form "\n\r\a...." or "\x<hex-number>".
 *
 * After the call, the modified sds string is no longer valid and all the
 * references must be substituted with the new pointer returned by the call.
 * 追加转义符 可忽略
 * */
sds sdscatrepr(sds s, const char *p, size_t len) {
    s = sdscatlen(s, "\"", 1);
    while (len--) {
        switch (*p) {
            case '\\':
            case '"':
                s = sdscatprintf(s, "\\%c", *p);
                break;
            case '\n':
                s = sdscatlen(s, "\\n", 2);
                break;
            case '\r':
                s = sdscatlen(s, "\\r", 2);
                break;
            case '\t':
                s = sdscatlen(s, "\\t", 2);
                break;
            case '\a':
                s = sdscatlen(s, "\\a", 2);
                break;
            case '\b':
                s = sdscatlen(s, "\\b", 2);
                break;
            default:
                if (isprint(*p))
                    s = sdscatprintf(s, "%c", *p);
                else
                    s = sdscatprintf(s, "\\x%02x", (unsigned char) *p);
                break;
        }
        p++;
    }
    return sdscatlen(s, "\"", 1);
}

/* Helper function for sdssplitargs() that returns non zero if 'c'
 * is a valid hex digit. */
int is_hex_digit(char c) {
    return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') ||
           (c >= 'A' && c <= 'F');
}

/* Helper function for sdssplitargs() that converts a hex digit into an
 * integer from 0 to 15 */
int hex_digit_to_int(char c) {
    switch (c) {
        case '0':
            return 0;
        case '1':
            return 1;
        case '2':
            return 2;
        case '3':
            return 3;
        case '4':
            return 4;
        case '5':
            return 5;
        case '6':
            return 6;
        case '7':
            return 7;
        case '8':
            return 8;
        case '9':
            return 9;
        case 'a':
        case 'A':
            return 10;
        case 'b':
        case 'B':
            return 11;
        case 'c':
        case 'C':
            return 12;
        case 'd':
        case 'D':
            return 13;
        case 'e':
        case 'E':
            return 14;
        case 'f':
        case 'F':
            return 15;
        default:
            return 0;
    }
}

/* Split a line into arguments, where every argument can be in the
 * following programming-language REPL-alike form:
 *
 * foo bar "newline are supported\n" and "\xff\x00otherstuff"
 *
 * The number of arguments is stored into *argc, and an array
 * of sds is returned.
 *
 * The caller should free the resulting array of sds strings with
 * sdsfreesplitres().
 *
 * Note that sdscatrepr() is able to convert back a string into
 * a quoted string in the same format sdssplitargs() is able to parse.
 *
 * The function returns the allocated tokens on success, even when the
 * input string is empty, or NULL if the input contains unbalanced
 * quotes or closed quotes followed by non space characters
 * as in: "foo"bar or "foo'
 * 将传入的字符串 通过args进行拆分
 */
sds *sdssplitargs(const char *line, int *argc) {
    const char *p = line;
    char *current = NULL;
    char **vector = NULL;

    *argc = 0;
    while (1) {
        /*
         * skip blanks
         * 跳过空字符
         * */
        while (*p && isspace(*p)) p++;
        // 对指针进行if/while判断就是检测该指针是否为Null
        if (*p) {
            /* get a token
             * 单引号 还是双引号
             * */
            int inq = 0;  /* set to 1 if we are in "quotes" */
            int insq = 0; /* set to 1 if we are in 'single quotes' */
            int done = 0;

            // 初始化sds结构  此时除header外数据体长度为0
            if (current == NULL) current = sdsempty();
            while (!done) {
                if (inq) {
                    if (*p == '\\' && *(p + 1) == 'x' &&
                        is_hex_digit(*(p + 2)) &&
                        is_hex_digit(*(p + 3))) {
                        unsigned char byte;

                        byte = (hex_digit_to_int(*(p + 2)) * 16) +
                               hex_digit_to_int(*(p + 3));
                        current = sdscatlen(current, (char *) &byte, 1);
                        p += 3;
                    } else if (*p == '\\' && *(p + 1)) {
                        char c;

                        p++;
                        switch (*p) {
                            case 'n':
                                c = '\n';
                                break;
                            case 'r':
                                c = '\r';
                                break;
                            case 't':
                                c = '\t';
                                break;
                            case 'b':
                                c = '\b';
                                break;
                            case 'a':
                                c = '\a';
                                break;
                            default:
                                c = *p;
                                break;
                        }
                        current = sdscatlen(current, &c, 1);
                    } else if (*p == '"') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p + 1) && !isspace(*(p + 1))) goto err;
                        done = 1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        current = sdscatlen(current, p, 1);
                    }
                } else if (insq) {
                    if (*p == '\\' && *(p + 1) == '\'') {
                        p++;
                        current = sdscatlen(current, "'", 1);
                    } else if (*p == '\'') {
                        /* closing quote must be followed by a space or
                         * nothing at all. */
                        if (*(p + 1) && !isspace(*(p + 1))) goto err;
                        done = 1;
                    } else if (!*p) {
                        /* unterminated quotes */
                        goto err;
                    } else {
                        current = sdscatlen(current, p, 1);
                    }
                } else {
                    // 解析下一个字符
                    switch (*p) {
                        // 当解析到这些字符时 代表读取到了一个 token
                        case ' ':
                        case '\n':
                        case '\r':
                        case '\t':
                        case '\0':
                            done = 1;
                            break;
                        case '"':
                            inq = 1;
                            break;
                        case '\'':
                            insq = 1;
                            break;
                        default:
                            // 其余字符直接追加到sds尾部就可以
                            current = sdscatlen(current, p, 1);
                            break;
                    }
                }
                if (*p) p++;
            }
            /* add the token to the vector */
            vector = s_realloc(vector, ((*argc) + 1) * sizeof(char *));
            vector[*argc] = current;
            (*argc)++;
            current = NULL;
        } else {
            /* Even on empty input string return something not NULL. */
            if (vector == NULL) vector = s_malloc(sizeof(void *));
            return vector;
        }
    }

    err:
    while ((*argc)--)
        sdsfree(vector[*argc]);
    s_free(vector);
    if (current) sdsfree(current);
    *argc = 0;
    return NULL;
}

/* Modify the string substituting all the occurrences of the set of
 * characters specified in the 'from' string to the corresponding character
 * in the 'to' array.
 *
 * For instance: sdsmapchars(mystring, "ho", "01", 2)
 * will have the effect of turning the string "hello" into "0ell1".
 *
 * The function returns the sds string pointer, that is always the same
 * as the input pointer since no resize is needed.
 * 将符合条件的字节映射成目标字节
 * */
sds sdsmapchars(sds s, const char *from, const char *to, size_t setlen) {
    size_t j, i, l = sdslen(s);

    for (j = 0; j < l; j++) {
        for (i = 0; i < setlen; i++) {
            if (s[j] == from[i]) {
                s[j] = to[i];
                break;
            }
        }
    }
    return s;
}

/* Join an array of C strings using the specified separator (also a C string).
 * Returns the result as an sds string. */
sds sdsjoin(char **argv, int argc, char *sep) {
    sds join = sdsempty();
    int j;

    for (j = 0; j < argc; j++) {
        join = sdscat(join, argv[j]);
        if (j != argc - 1) join = sdscat(join, sep);
    }
    return join;
}

/* Like sdsjoin, but joins an array of SDS strings. */
sds sdsjoinsds(sds *argv, int argc, const char *sep, size_t seplen) {
    sds join = sdsempty();
    int j;

    for (j = 0; j < argc; j++) {
        join = sdscatsds(join, argv[j]);
        if (j != argc - 1) join = sdscatlen(join, sep, seplen);
    }
    return join;
}

/* Wrappers to the allocators used by SDS. Note that SDS will actually
 * just use the macros defined into sdsalloc.h in order to avoid to pay
 * the overhead of function calls. Here we define these wrappers only for
 * the programs SDS is linked to, if they want to touch the SDS internals
 * even if they use a different allocator. */
void *sds_malloc(size_t size) { return s_malloc(size); }

void *sds_realloc(void *ptr, size_t size) { return s_realloc(ptr, size); }

void sds_free(void *ptr) { s_free(ptr); }

#if defined(SDS_TEST_MAIN)
#include <stdio.h>
#include "testhelp.h"
#include "limits.h"

#define UNUSED(x) (void)(x)
int sdsTest(void) {
    {
        sds x = sdsnew("foo"), y;

        test_cond("Create a string and obtain the length",
            sdslen(x) == 3 && memcmp(x,"foo\0",4) == 0)

        sdsfree(x);
        x = sdsnewlen("foo",2);
        test_cond("Create a string with specified length",
            sdslen(x) == 2 && memcmp(x,"fo\0",3) == 0)

        x = sdscat(x,"bar");
        test_cond("Strings concatenation",
            sdslen(x) == 5 && memcmp(x,"fobar\0",6) == 0);

        x = sdscpy(x,"a");
        test_cond("sdscpy() against an originally longer string",
            sdslen(x) == 1 && memcmp(x,"a\0",2) == 0)

        x = sdscpy(x,"xyzxxxxxxxxxxyyyyyyyyyykkkkkkkkkk");
        test_cond("sdscpy() against an originally shorter string",
            sdslen(x) == 33 &&
            memcmp(x,"xyzxxxxxxxxxxyyyyyyyyyykkkkkkkkkk\0",33) == 0)

        sdsfree(x);
        x = sdscatprintf(sdsempty(),"%d",123);
        test_cond("sdscatprintf() seems working in the base case",
            sdslen(x) == 3 && memcmp(x,"123\0",4) == 0)

        sdsfree(x);
        x = sdsnew("--");
        x = sdscatfmt(x, "Hello %s World %I,%I--", "Hi!", LLONG_MIN,LLONG_MAX);
        test_cond("sdscatfmt() seems working in the base case",
            sdslen(x) == 60 &&
            memcmp(x,"--Hello Hi! World -9223372036854775808,"
                     "9223372036854775807--",60) == 0)
        printf("[%s]\n",x);

        sdsfree(x);
        x = sdsnew("--");
        x = sdscatfmt(x, "%u,%U--", UINT_MAX, ULLONG_MAX);
        test_cond("sdscatfmt() seems working with unsigned numbers",
            sdslen(x) == 35 &&
            memcmp(x,"--4294967295,18446744073709551615--",35) == 0)

        sdsfree(x);
        x = sdsnew(" x ");
        sdstrim(x," x");
        test_cond("sdstrim() works when all chars match",
            sdslen(x) == 0)

        sdsfree(x);
        x = sdsnew(" x ");
        sdstrim(x," ");
        test_cond("sdstrim() works when a single char remains",
            sdslen(x) == 1 && x[0] == 'x')

        sdsfree(x);
        x = sdsnew("xxciaoyyy");
        sdstrim(x,"xy");
        test_cond("sdstrim() correctly trims characters",
            sdslen(x) == 4 && memcmp(x,"ciao\0",5) == 0)

        y = sdsdup(x);
        sdsrange(y,1,1);
        test_cond("sdsrange(...,1,1)",
            sdslen(y) == 1 && memcmp(y,"i\0",2) == 0)

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,1,-1);
        test_cond("sdsrange(...,1,-1)",
            sdslen(y) == 3 && memcmp(y,"iao\0",4) == 0)

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,-2,-1);
        test_cond("sdsrange(...,-2,-1)",
            sdslen(y) == 2 && memcmp(y,"ao\0",3) == 0)

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,2,1);
        test_cond("sdsrange(...,2,1)",
            sdslen(y) == 0 && memcmp(y,"\0",1) == 0)

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,1,100);
        test_cond("sdsrange(...,1,100)",
            sdslen(y) == 3 && memcmp(y,"iao\0",4) == 0)

        sdsfree(y);
        y = sdsdup(x);
        sdsrange(y,100,100);
        test_cond("sdsrange(...,100,100)",
            sdslen(y) == 0 && memcmp(y,"\0",1) == 0)

        sdsfree(y);
        sdsfree(x);
        x = sdsnew("foo");
        y = sdsnew("foa");
        test_cond("sdscmp(foo,foa)", sdscmp(x,y) > 0)

        sdsfree(y);
        sdsfree(x);
        x = sdsnew("bar");
        y = sdsnew("bar");
        test_cond("sdscmp(bar,bar)", sdscmp(x,y) == 0)

        sdsfree(y);
        sdsfree(x);
        x = sdsnew("aar");
        y = sdsnew("bar");
        test_cond("sdscmp(bar,bar)", sdscmp(x,y) < 0)

        sdsfree(y);
        sdsfree(x);
        x = sdsnewlen("\a\n\0foo\r",7);
        y = sdscatrepr(sdsempty(),x,sdslen(x));
        test_cond("sdscatrepr(...data...)",
            memcmp(y,"\"\\a\\n\\x00foo\\r\"",15) == 0)

        {
            unsigned int oldfree;
            char *p;
            int step = 10, j, i;

            sdsfree(x);
            sdsfree(y);
            x = sdsnew("0");
            test_cond("sdsnew() free/len buffers", sdslen(x) == 1 && sdsavail(x) == 0);

            /* Run the test a few times in order to hit the first two
             * SDS header types. */
            for (i = 0; i < 10; i++) {
                int oldlen = sdslen(x);
                x = sdsMakeRoomFor(x,step);
                int type = x[-1]&SDS_TYPE_MASK;

                test_cond("sdsMakeRoomFor() len", sdslen(x) == oldlen);
                if (type != SDS_TYPE_5) {
                    test_cond("sdsMakeRoomFor() free", sdsavail(x) >= step);
                    oldfree = sdsavail(x);
                }
                p = x+oldlen;
                for (j = 0; j < step; j++) {
                    p[j] = 'A'+j;
                }
                sdsIncrLen(x,step);
            }
            test_cond("sdsMakeRoomFor() content",
                memcmp("0ABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJABCDEFGHIJ",x,101) == 0);
            test_cond("sdsMakeRoomFor() final length",sdslen(x)==101);

            sdsfree(x);
        }
    }
    test_report()
    return 0;
}
#endif

#ifdef SDS_TEST_MAIN
int main(void) {
    return sdsTest();
}
#endif
