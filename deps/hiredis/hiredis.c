/*
 * Copyright (c) 2009-2011, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2010-2014, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2015, Matt Stancliff <matt at genges dot com>,
 *                     Jan-Erik Rediger <janerik at fnordig dot com>
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
#include <assert.h>
#include <errno.h>
#include <ctype.h>

#include "hiredis.h"
#include "net.h"
#include "sds.h"
#include "async.h"
#include "win32.h"

extern int redisContextUpdateConnectTimeout(redisContext *c, const struct timeval *timeout);
extern int redisContextUpdateCommandTimeout(redisContext *c, const struct timeval *timeout);

/**
 * 存储了hiredis模块网络交互的默认函数
 */
static redisContextFuncs redisContextDefaultFuncs = {
    .free_privctx = NULL,
    .async_read = redisAsyncRead,
    .async_write = redisAsyncWrite,
    .read = redisNetRead,
    .write = redisNetWrite
};

static redisReply *createReplyObject(int type);
static void *createStringObject(const redisReadTask *task, char *str, size_t len);
static void *createArrayObject(const redisReadTask *task, size_t elements);
static void *createIntegerObject(const redisReadTask *task, long long value);
static void *createDoubleObject(const redisReadTask *task, double value, char *str, size_t len);
static void *createNilObject(const redisReadTask *task);
static void *createBoolObject(const redisReadTask *task, int bval);

/* Default set of functions to build the reply. Keep in mind that such a
 * function returning NULL is interpreted as OOM. */
static redisReplyObjectFunctions defaultFunctions = {
    createStringObject,
    createArrayObject,
    createIntegerObject,
    createDoubleObject,
    createNilObject,
    createBoolObject,
    freeReplyObject
};

/* Create a reply object
 * 在解决粘包拆包后 得到的一份完整的数据会被包装成一个reply对象
 * */
static redisReply *createReplyObject(int type) {
    redisReply *r = hi_calloc(1,sizeof(*r));

    if (r == NULL)
        return NULL;

    r->type = type;
    return r;
}

/* Free a reply object */
void freeReplyObject(void *reply) {
    redisReply *r = reply;
    size_t j;

    if (r == NULL)
        return;

    switch(r->type) {
    case REDIS_REPLY_INTEGER:
        break; /* Nothing to free */
    case REDIS_REPLY_ARRAY:
    case REDIS_REPLY_MAP:
    case REDIS_REPLY_SET:
    case REDIS_REPLY_PUSH:
        if (r->element != NULL) {
            for (j = 0; j < r->elements; j++)
                freeReplyObject(r->element[j]);
            hi_free(r->element);
        }
        break;
    case REDIS_REPLY_ERROR:
    case REDIS_REPLY_STATUS:
    case REDIS_REPLY_STRING:
    case REDIS_REPLY_DOUBLE:
    case REDIS_REPLY_VERB:
        hi_free(r->str);
        break;
    }
    hi_free(r);
}

/**
 * 生成string类型的数据
 * @param task
 * @param str
 * @param len
 * @return
 */
static void *createStringObject(const redisReadTask *task, char *str, size_t len) {
    redisReply *r, *parent;
    char *buf;

    r = createReplyObject(task->type);
    if (r == NULL)
        return NULL;

    assert(task->type == REDIS_REPLY_ERROR  ||
           task->type == REDIS_REPLY_STATUS ||
           task->type == REDIS_REPLY_STRING ||
           task->type == REDIS_REPLY_VERB);

    /* Copy string value
     * 先忽略verb类型
     * */
    if (task->type == REDIS_REPLY_VERB) {
        buf = hi_malloc(len-4+1); /* Skip 4 bytes of verbatim type header. */
        if (buf == NULL) goto oom;

        // 头部信息被存放在vtype数组中
        memcpy(r->vtype,str,3);
        r->vtype[3] = '\0';
        // 剩余数据存放在buf中
        memcpy(buf,str+4,len-4);
        buf[len-4] = '\0';
        // 这里只记录有效长度
        r->len = len - 4;
    } else {
        // 如果是string类型 相对简单 直接将数据存储在buf中
        buf = hi_malloc(len+1);
        if (buf == NULL) goto oom;

        memcpy(buf,str,len);
        buf[len] = '\0';
        r->len = len;
    }
    r->str = buf;

    // 代表本次解析出来的元素在一个聚合类型中 比如先检测到了一个array类型 这里是填充内部的元素
    if (task->parent) {
        // 找到array对象
        parent = task->parent->obj;
        assert(parent->type == REDIS_REPLY_ARRAY ||
               parent->type == REDIS_REPLY_MAP ||
               parent->type == REDIS_REPLY_SET ||
               parent->type == REDIS_REPLY_PUSH);
        // 将本次结果回填到对应的数组中
        parent->element[task->idx] = r;
    }
    return r;

oom:
    freeReplyObject(r);
    return NULL;
}

/**
 * 本次收到的是一个数组类型
 * @param task
 * @param elements 内部有多少个元素
 * @return
 */
static void *createArrayObject(const redisReadTask *task, size_t elements) {
    redisReply *r, *parent;

    r = createReplyObject(task->type);
    if (r == NULL)
        return NULL;

    if (elements > 0) {
        r->element = hi_calloc(elements,sizeof(redisReply*));
        if (r->element == NULL) {
            freeReplyObject(r);
            return NULL;
        }
    }

    r->elements = elements;

    // 这个是代表嵌套了
    if (task->parent) {
        parent = task->parent->obj;
        assert(parent->type == REDIS_REPLY_ARRAY ||
               parent->type == REDIS_REPLY_MAP ||
               parent->type == REDIS_REPLY_SET ||
               parent->type == REDIS_REPLY_PUSH);
        parent->element[task->idx] = r;
    }
    return r;
}

/**
 * 创建一个integer类型的obj
 * @param task
 * @param value
 * @return
 */
static void *createIntegerObject(const redisReadTask *task, long long value) {
    redisReply *r, *parent;

    // 声明reply对象的类型
    r = createReplyObject(REDIS_REPLY_INTEGER);
    if (r == NULL)
        return NULL;

    r->integer = value;

    // TODO
    if (task->parent) {
        parent = task->parent->obj;
        assert(parent->type == REDIS_REPLY_ARRAY ||
               parent->type == REDIS_REPLY_MAP ||
               parent->type == REDIS_REPLY_SET ||
               parent->type == REDIS_REPLY_PUSH);
        parent->element[task->idx] = r;
    }
    return r;
}

/**
 * 代表本次创建的是一个double对象
 * @param task
 * @param value
 * @param str
 * @param len
 * @return
 */
static void *createDoubleObject(const redisReadTask *task, double value, char *str, size_t len) {
    redisReply *r, *parent;

    r = createReplyObject(REDIS_REPLY_DOUBLE);
    if (r == NULL)
        return NULL;

    r->dval = value;
    r->str = hi_malloc(len+1);
    if (r->str == NULL) {
        freeReplyObject(r);
        return NULL;
    }

    /* The double reply also has the original protocol string representing a
     * double as a null terminated string. This way the caller does not need
     * to format back for string conversion, especially since Redis does efforts
     * to make the string more human readable avoiding the calssical double
     * decimal string conversion artifacts. */
    memcpy(r->str, str, len);
    r->str[len] = '\0';

    if (task->parent) {
        parent = task->parent->obj;
        assert(parent->type == REDIS_REPLY_ARRAY ||
               parent->type == REDIS_REPLY_MAP ||
               parent->type == REDIS_REPLY_SET);
        parent->element[task->idx] = r;
    }
    return r;
}

/**
 * 将task包装成nil类型的obj对象后返回
 * @param task
 * @return
 */
static void *createNilObject(const redisReadTask *task) {
    redisReply *r, *parent;

    r = createReplyObject(REDIS_REPLY_NIL);
    if (r == NULL)
        return NULL;

    if (task->parent) {
        parent = task->parent->obj;
        assert(parent->type == REDIS_REPLY_ARRAY ||
               parent->type == REDIS_REPLY_MAP ||
               parent->type == REDIS_REPLY_SET ||
               parent->type == REDIS_REPLY_PUSH);
        parent->element[task->idx] = r;
    }
    return r;
}

static void *createBoolObject(const redisReadTask *task, int bval) {
    redisReply *r, *parent;

    r = createReplyObject(REDIS_REPLY_BOOL);
    if (r == NULL)
        return NULL;

    r->integer = bval != 0;

    if (task->parent) {
        parent = task->parent->obj;
        assert(parent->type == REDIS_REPLY_ARRAY ||
               parent->type == REDIS_REPLY_MAP ||
               parent->type == REDIS_REPLY_SET);
        parent->element[task->idx] = r;
    }
    return r;
}

/* Return the number of digits of 'v' when converted to string in radix 10.
 * Implementation borrowed from link in redis/src/util.c:string2ll(). */
static uint32_t countDigits(uint64_t v) {
  uint32_t result = 1;
  for (;;) {
    if (v < 10) return result;
    if (v < 100) return result + 1;
    if (v < 1000) return result + 2;
    if (v < 10000) return result + 3;
    v /= 10000U;
    result += 4;
  }
}

/* Helper that calculates the bulk length given a certain string length. */
static size_t bulklen(size_t len) {
    return 1+countDigits(len)+2+len+2;
}

/**
 * 将本次要发送的信息格式化  比如传入的是 "%s %s %s" 以及 ap内的3个参数
 * @param target
 * @param format
 * @param ap
 * @return
 */
int redisvFormatCommand(char **target, const char *format, va_list ap) {
    const char *c = format;
    char *cmd = NULL; /* final command */
    int pos; /* position in final command */
    hisds curarg, newarg; /* current argument */
    int touched = 0; /* was the current argument touched? */
    char **curargv = NULL, **newargv = NULL;
    int argc = 0;
    int totlen = 0;
    int error_type = 0; /* 0 = no error; -1 = memory error; -2 = format error */
    int j;

    /* Abort if there is not target to set
     * target是用于存储结果字符串的 不能为NULL
     * */
    if (target == NULL)
        return -1;

    /* Build the command string accordingly to protocol */
    curarg = hi_sdsempty();
    if (curarg == NULL)
        return -1;

    // 代表此时还没有读取到末尾
    while(*c != '\0') {
        // 此时扫描到的不是占位符  或者最后是 %'\0'  代表这个百分号不是占位符的意思
        if (*c != '%' || c[1] == '\0') {
            // 当扫描到空格时 可能是处理完了一个占位符
            if (*c == ' ') {
                // 代表之前解析到了一个字符
                if (touched) {
                    // curargv 是一个数组
                    newargv = hi_realloc(curargv,sizeof(char*)*(argc+1));
                    if (newargv == NULL) goto memory_err;
                    curargv = newargv;
                    // 将解析出来的字符串添加到数组中 这些字符串通过' '来分隔
                    curargv[argc++] = curarg;
                    totlen += bulklen(hi_sdslen(curarg));

                    /* curarg is put in argv so it can be overwritten.
                     * 因为当前字符串已经处理完了 清空curarg 之后解析出来的字符串又会临时存放在这里
                     * */
                    curarg = hi_sdsempty();
                    if (curarg == NULL) goto memory_err;
                    touched = 0;
                }
                // 直接拼接这个字符就好 如果是最后一个百分号也是进入这个分支
            } else {
                newarg = hi_sdscatlen(curarg,c,1);
                if (newarg == NULL) goto memory_err;
                curarg = newarg;
                touched = 1;
            }
            // 先假设扫描到的是占位符
        } else {
            char *arg;
            size_t size;

            /* Set newarg so it can be checked even if it is not touched. */
            newarg = curarg;

            // 此时c[0]是'%'
            switch(c[1]) {
                // 代表第一个参数应该是一个字符串
            case 's':
                // 以char解析第一个参数
                arg = va_arg(ap,char*);
                size = strlen(arg);
                if (size > 0)
                    // 追加参数到curarg上
                    newarg = hi_sdscatlen(curarg,arg,size);
                break;
            case 'b':
                arg = va_arg(ap,char*);
                size = va_arg(ap,size_t);
                if (size > 0)
                    newarg = hi_sdscatlen(curarg,arg,size);
                break;
                // 如果连续遇到2个% 直接添加就好
            case '%':
                newarg = hi_sdscat(curarg,"%");
                break;
            default:
                /* Try to detect printf format
                 * 下面的则是将可变参数转换成不同的类型
                 * */
                {
                    static const char intfmts[] = "diouxX";
                    static const char flags[] = "#0-+ ";
                    char _format[16];
                    const char *_p = c+1;
                    size_t _l = 0;
                    va_list _cpy;

                    /* Flags */
                    while (*_p != '\0' && strchr(flags,*_p) != NULL) _p++;

                    /* Field width */
                    while (*_p != '\0' && isdigit(*_p)) _p++;

                    /* Precision */
                    if (*_p == '.') {
                        _p++;
                        while (*_p != '\0' && isdigit(*_p)) _p++;
                    }

                    /* Copy va_list before consuming with va_arg */
                    va_copy(_cpy,ap);

                    /* Integer conversion (without modifiers) */
                    if (strchr(intfmts,*_p) != NULL) {
                        va_arg(ap,int);
                        goto fmt_valid;
                    }

                    /* Double conversion (without modifiers) */
                    if (strchr("eEfFgGaA",*_p) != NULL) {
                        va_arg(ap,double);
                        goto fmt_valid;
                    }

                    /* Size: char */
                    if (_p[0] == 'h' && _p[1] == 'h') {
                        _p += 2;
                        if (*_p != '\0' && strchr(intfmts,*_p) != NULL) {
                            va_arg(ap,int); /* char gets promoted to int */
                            goto fmt_valid;
                        }
                        goto fmt_invalid;
                    }

                    /* Size: short */
                    if (_p[0] == 'h') {
                        _p += 1;
                        if (*_p != '\0' && strchr(intfmts,*_p) != NULL) {
                            va_arg(ap,int); /* short gets promoted to int */
                            goto fmt_valid;
                        }
                        goto fmt_invalid;
                    }

                    /* Size: long long */
                    if (_p[0] == 'l' && _p[1] == 'l') {
                        _p += 2;
                        if (*_p != '\0' && strchr(intfmts,*_p) != NULL) {
                            va_arg(ap,long long);
                            goto fmt_valid;
                        }
                        goto fmt_invalid;
                    }

                    /* Size: long */
                    if (_p[0] == 'l') {
                        _p += 1;
                        if (*_p != '\0' && strchr(intfmts,*_p) != NULL) {
                            va_arg(ap,long);
                            goto fmt_valid;
                        }
                        goto fmt_invalid;
                    }

                fmt_invalid:
                    va_end(_cpy);
                    goto format_err;

                fmt_valid:
                    _l = (_p+1)-c;
                    if (_l < sizeof(_format)-2) {
                        memcpy(_format,c,_l);
                        _format[_l] = '\0';
                        newarg = hi_sdscatvprintf(curarg,_format,_cpy);

                        /* Update current position (note: outer blocks
                         * increment c twice so compensate here) */
                        c = _p-1;
                    }

                    va_end(_cpy);
                    break;
                }
            }

            if (newarg == NULL) goto memory_err;

            // 更新此时的字符串
            curarg = newarg;

            // 代表此时已经解析了某种格式化数据
            touched = 1;
            // 当解析到 类似 %s 的时候 c要移动2步
            c++;
        }
        // 非占位符的情况要移动一步
        c++;
    }

    /* Add the last argument if needed
     * 最后的参数不需要' '来终结
     * */
    if (touched) {
        newargv = hi_realloc(curargv,sizeof(char*)*(argc+1));
        if (newargv == NULL) goto memory_err;
        curargv = newargv;
        curargv[argc++] = curarg;
        totlen += bulklen(hi_sdslen(curarg));
    } else {
        hi_sdsfree(curarg);
    }

    /* Clear curarg because it was put in curargv or was free'd. */
    curarg = NULL;

    /* Add bytes needed to hold multi bulk count
     * */
    totlen += 1+countDigits(argc)+2;

    /* Build the command at protocol level
     * 这里又额外申请了一个byte空间
     * */
    cmd = hi_malloc(totlen+1);
    if (cmd == NULL) goto memory_err;

    // 以*开头后写入参数总数 之后是回车   这个好像是bulk协议
    pos = sprintf(cmd,"*%d\r\n",argc);
    // 之后每行对应一个参数信息
    for (j = 0; j < argc; j++) {
        // 将每个参数填充到格式化字符串中  这里是先看到参数长度信息 %zu 代表unsigned int $长度信息\r\n
        pos += sprintf(cmd+pos,"$%zu\r\n",hi_sdslen(curargv[j]));
        // 之后将参数拷贝到最终的容器 cmd中
        memcpy(cmd+pos,curargv[j],hi_sdslen(curargv[j]));
        pos += hi_sdslen(curargv[j]);
        hi_sdsfree(curargv[j]);
        // 最后还要插入换行符
        cmd[pos++] = '\r';
        cmd[pos++] = '\n';
    }
    assert(pos == totlen);
    // 在最后插入终止符
    cmd[pos] = '\0';

    hi_free(curargv);
    *target = cmd;
    return totlen;

format_err:
    error_type = -2;
    goto cleanup;

memory_err:
    error_type = -1;
    goto cleanup;

cleanup:
    if (curargv) {
        while(argc--)
            hi_sdsfree(curargv[argc]);
        hi_free(curargv);
    }

    hi_sdsfree(curarg);
    hi_free(cmd);

    return error_type;
}

/* Format a command according to the Redis protocol. This function
 * takes a format similar to printf:
 *
 * %s represents a C null terminated string you want to interpolate
 * %b represents a binary safe string
 *
 * When using %b you need to provide both the pointer to the string
 * and the length in bytes as a size_t. Examples:
 *
 * len = redisFormatCommand(target, "GET %s", mykey);
 * len = redisFormatCommand(target, "SET %s %b", mykey, myval, myvallen);
 */
int redisFormatCommand(char **target, const char *format, ...) {
    va_list ap;
    int len;
    va_start(ap,format);
    len = redisvFormatCommand(target,format,ap);
    va_end(ap);

    /* The API says "-1" means bad result, but we now also return "-2" in some
     * cases.  Force the return value to always be -1. */
    if (len < 0)
        len = -1;

    return len;
}

/* Format a command according to the Redis protocol using an hisds string and
 * hi_sdscatfmt for the processing of arguments. This function takes the
 * number of arguments, an array with arguments and an array with their
 * lengths. If the latter is set to NULL, strlen will be used to compute the
 * argument lengths.
 */
int redisFormatSdsCommandArgv(hisds *target, int argc, const char **argv,
                              const size_t *argvlen)
{
    hisds cmd, aux;
    unsigned long long totlen;
    int j;
    size_t len;

    /* Abort on a NULL target */
    if (target == NULL)
        return -1;

    /* Calculate our total size */
    totlen = 1+countDigits(argc)+2;
    for (j = 0; j < argc; j++) {
        len = argvlen ? argvlen[j] : strlen(argv[j]);
        totlen += bulklen(len);
    }

    /* Use an SDS string for command construction */
    cmd = hi_sdsempty();
    if (cmd == NULL)
        return -1;

    /* We already know how much storage we need */
    aux = hi_sdsMakeRoomFor(cmd, totlen);
    if (aux == NULL) {
        hi_sdsfree(cmd);
        return -1;
    }

    cmd = aux;

    /* Construct command */
    cmd = hi_sdscatfmt(cmd, "*%i\r\n", argc);
    for (j=0; j < argc; j++) {
        len = argvlen ? argvlen[j] : strlen(argv[j]);
        cmd = hi_sdscatfmt(cmd, "$%u\r\n", len);
        cmd = hi_sdscatlen(cmd, argv[j], len);
        cmd = hi_sdscatlen(cmd, "\r\n", sizeof("\r\n")-1);
    }

    assert(hi_sdslen(cmd)==totlen);

    *target = cmd;
    return totlen;
}

void redisFreeSdsCommand(hisds cmd) {
    hi_sdsfree(cmd);
}

/* Format a command according to the Redis protocol. This function takes the
 * number of arguments, an array with arguments and an array with their
 * lengths. If the latter is set to NULL, strlen will be used to compute the
 * argument lengths.
 */
int redisFormatCommandArgv(char **target, int argc, const char **argv, const size_t *argvlen) {
    char *cmd = NULL; /* final command */
    int pos; /* position in final command */
    size_t len;
    int totlen, j;

    /* Abort on a NULL target */
    if (target == NULL)
        return -1;

    /* Calculate number of bytes needed for the command */
    totlen = 1+countDigits(argc)+2;
    for (j = 0; j < argc; j++) {
        len = argvlen ? argvlen[j] : strlen(argv[j]);
        totlen += bulklen(len);
    }

    /* Build the command at protocol level */
    cmd = hi_malloc(totlen+1);
    if (cmd == NULL)
        return -1;

    pos = sprintf(cmd,"*%d\r\n",argc);
    for (j = 0; j < argc; j++) {
        len = argvlen ? argvlen[j] : strlen(argv[j]);
        pos += sprintf(cmd+pos,"$%zu\r\n",len);
        memcpy(cmd+pos,argv[j],len);
        pos += len;
        cmd[pos++] = '\r';
        cmd[pos++] = '\n';
    }
    assert(pos == totlen);
    cmd[pos] = '\0';

    *target = cmd;
    return totlen;
}

void redisFreeCommand(char *cmd) {
    hi_free(cmd);
}

void __redisSetError(redisContext *c, int type, const char *str) {
    size_t len;

    c->err = type;
    if (str != NULL) {
        len = strlen(str);
        len = len < (sizeof(c->errstr)-1) ? len : (sizeof(c->errstr)-1);
        memcpy(c->errstr,str,len);
        c->errstr[len] = '\0';
    } else {
        /* Only REDIS_ERR_IO may lack a description! */
        assert(type == REDIS_ERR_IO);
        strerror_r(errno, c->errstr, sizeof(c->errstr));
    }
}

/**
 * 生成reader对象 专门负责读取context->buf的数据
 * @return
 */
redisReader *redisReaderCreate(void) {
    return redisReaderCreateWithFunctions(&defaultFunctions);
}

static void redisPushAutoFree(void *privdata, void *reply) {
    (void)privdata;
    freeReplyObject(reply);
}

/**
 * 初始化context
 * @return
 */
static redisContext *redisContextInit(void) {
    redisContext *c;

    // 为context对象分配内存
    c = hi_calloc(1, sizeof(*c));
    if (c == NULL)
        return NULL;

    // 设置默认函数 用于读取/写出数据
    c->funcs = &redisContextDefaultFuncs;

    // 存储待发送的数据
    c->obuf = hi_sdsempty();
    // 通过reader读取准备好的数据
    c->reader = redisReaderCreate();
    c->fd = REDIS_INVALID_FD;

    if (c->obuf == NULL || c->reader == NULL) {
        redisFree(c);
        return NULL;
    }

    return c;
}

/**
 * 进行内存清理
 * @param c
 */
void redisFree(redisContext *c) {
    if (c == NULL)
        return;
    redisNetClose(c);

    hi_sdsfree(c->obuf);
    redisReaderFree(c->reader);
    hi_free(c->tcp.host);
    hi_free(c->tcp.source_addr);
    hi_free(c->unix_sock.path);
    hi_free(c->connect_timeout);
    hi_free(c->command_timeout);
    hi_free(c->saddr);

    if (c->privdata && c->free_privdata)
        c->free_privdata(c->privdata);

    if (c->funcs->free_privctx)
        c->funcs->free_privctx(c->privctx);

    memset(c, 0xff, sizeof(*c));
    hi_free(c);
}

redisFD redisFreeKeepFd(redisContext *c) {
    redisFD fd = c->fd;
    c->fd = REDIS_INVALID_FD;
    redisFree(c);
    return fd;
}

int redisReconnect(redisContext *c) {
    c->err = 0;
    memset(c->errstr, '\0', strlen(c->errstr));

    if (c->privctx && c->funcs->free_privctx) {
        c->funcs->free_privctx(c->privctx);
        c->privctx = NULL;
    }

    redisNetClose(c);

    hi_sdsfree(c->obuf);
    redisReaderFree(c->reader);

    c->obuf = hi_sdsempty();
    c->reader = redisReaderCreate();

    if (c->obuf == NULL || c->reader == NULL) {
        __redisSetError(c, REDIS_ERR_OOM, "Out of memory");
        return REDIS_ERR;
    }

    int ret = REDIS_ERR;
    if (c->connection_type == REDIS_CONN_TCP) {
        ret = redisContextConnectBindTcp(c, c->tcp.host, c->tcp.port,
               c->connect_timeout, c->tcp.source_addr);
    } else if (c->connection_type == REDIS_CONN_UNIX) {
        ret = redisContextConnectUnix(c, c->unix_sock.path, c->connect_timeout);
    } else {
        /* Something bad happened here and shouldn't have. There isn't
           enough information in the context to reconnect. */
        __redisSetError(c,REDIS_ERR_OTHER,"Not enough information to reconnect");
        ret = REDIS_ERR;
    }

    if (c->command_timeout != NULL && (c->flags & REDIS_BLOCK) && c->fd != REDIS_INVALID_FD) {
        redisContextSetTimeout(c, *c->command_timeout);
    }

    return ret;
}

/**
 * 基于options生成context
 * @param options
 * @return
 */
redisContext *redisConnectWithOptions(const redisOptions *options) {
    // 创建上下文对象 内部定义了与网络交互的函数 以及存储数据的缓冲区和处理数据的reader
    redisContext *c = redisContextInit();
    if (c == NULL) {
        return NULL;
    }
    // 默认情况下是阻塞连接 如果包含REDIS_OPT_NONBLOCK标记 代表是非阻塞连接 会影响到后面的connect()的调用
    if (!(options->options & REDIS_OPT_NONBLOCK)) {
        c->flags |= REDIS_BLOCK;
    }
    if (options->options & REDIS_OPT_REUSEADDR) {
        c->flags |= REDIS_REUSEADDR;
    }
    if (options->options & REDIS_OPT_NOAUTOFREE) {
        c->flags |= REDIS_NO_AUTO_FREE;
    }

    /* Set any user supplied RESP3 PUSH handler or use freeReplyObject
     * as a default unless specifically flagged that we don't want one.
     * TODO 默认情况下 创建异步上下文不会携带下面的标记
     * */
    if (options->push_cb != NULL)
        redisSetPushCallback(c, options->push_cb);
    else if (!(options->options & REDIS_OPT_NO_PUSH_AUTOFREE))
        redisSetPushCallback(c, redisPushAutoFree);

    // 将私有数据转移到context上
    c->privdata = options->privdata;
    c->free_privdata = options->free_privdata;

    // 使用options 配置context
    if (redisContextUpdateConnectTimeout(c, options->connect_timeout) != REDIS_OK ||
        redisContextUpdateCommandTimeout(c, options->command_timeout) != REDIS_OK) {
        __redisSetError(c, REDIS_ERR_OOM, "Out of memory");
        return c;
    }

    // 代表创建的是tcp连接 进行异步连接
    if (options->type == REDIS_CONN_TCP) {
        redisContextConnectBindTcp(c, options->endpoint.tcp.ip,
                                   options->endpoint.tcp.port, options->connect_timeout,
                                   options->endpoint.tcp.source_addr);

        // 忽略其他情况
    } else if (options->type == REDIS_CONN_UNIX) {
        redisContextConnectUnix(c, options->endpoint.unix_socket,
                                options->connect_timeout);
    } else if (options->type == REDIS_CONN_USERFD) {
        c->fd = options->endpoint.fd;
        c->flags |= REDIS_CONNECTED;
    } else {
        // Unknown type - FIXME - FREE
        return NULL;
    }

    if (options->command_timeout != NULL && (c->flags & REDIS_BLOCK) && c->fd != REDIS_INVALID_FD) {
        redisContextSetTimeout(c, *options->command_timeout);
    }

    return c;
}

/* Connect to a Redis instance. On error the field error in the returned
 * context will be set to the return value of the error function.
 * When no set of reply functions is given, the default set will be used. */
redisContext *redisConnect(const char *ip, int port) {
    redisOptions options = {0};
    REDIS_OPTIONS_SET_TCP(&options, ip, port);
    return redisConnectWithOptions(&options);
}

redisContext *redisConnectWithTimeout(const char *ip, int port, const struct timeval tv) {
    redisOptions options = {0};
    REDIS_OPTIONS_SET_TCP(&options, ip, port);
    options.connect_timeout = &tv;
    return redisConnectWithOptions(&options);
}

redisContext *redisConnectNonBlock(const char *ip, int port) {
    redisOptions options = {0};
    REDIS_OPTIONS_SET_TCP(&options, ip, port);
    options.options |= REDIS_OPT_NONBLOCK;
    return redisConnectWithOptions(&options);
}

redisContext *redisConnectBindNonBlock(const char *ip, int port,
                                       const char *source_addr) {
    redisOptions options = {0};
    REDIS_OPTIONS_SET_TCP(&options, ip, port);
    options.endpoint.tcp.source_addr = source_addr;
    options.options |= REDIS_OPT_NONBLOCK;
    return redisConnectWithOptions(&options);
}

redisContext *redisConnectBindNonBlockWithReuse(const char *ip, int port,
                                                const char *source_addr) {
    redisOptions options = {0};
    REDIS_OPTIONS_SET_TCP(&options, ip, port);
    options.endpoint.tcp.source_addr = source_addr;
    options.options |= REDIS_OPT_NONBLOCK|REDIS_OPT_REUSEADDR;
    return redisConnectWithOptions(&options);
}

redisContext *redisConnectUnix(const char *path) {
    redisOptions options = {0};
    REDIS_OPTIONS_SET_UNIX(&options, path);
    return redisConnectWithOptions(&options);
}

redisContext *redisConnectUnixWithTimeout(const char *path, const struct timeval tv) {
    redisOptions options = {0};
    REDIS_OPTIONS_SET_UNIX(&options, path);
    options.connect_timeout = &tv;
    return redisConnectWithOptions(&options);
}

redisContext *redisConnectUnixNonBlock(const char *path) {
    redisOptions options = {0};
    REDIS_OPTIONS_SET_UNIX(&options, path);
    options.options |= REDIS_OPT_NONBLOCK;
    return redisConnectWithOptions(&options);
}

redisContext *redisConnectFd(redisFD fd) {
    redisOptions options = {0};
    options.type = REDIS_CONN_USERFD;
    options.endpoint.fd = fd;
    return redisConnectWithOptions(&options);
}

/* Set read/write timeout on a blocking socket. */
int redisSetTimeout(redisContext *c, const struct timeval tv) {
    if (c->flags & REDIS_BLOCK)
        return redisContextSetTimeout(c,tv);
    return REDIS_ERR;
}

/* Enable connection KeepAlive. */
int redisEnableKeepAlive(redisContext *c) {
    if (redisKeepAlive(c, REDIS_KEEPALIVE_INTERVAL) != REDIS_OK)
        return REDIS_ERR;
    return REDIS_OK;
}

/* Set a user provided RESP3 PUSH handler and return any old one set. */
redisPushFn *redisSetPushCallback(redisContext *c, redisPushFn *fn) {
    redisPushFn *old = c->push_cb;
    c->push_cb = fn;
    return old;
}

/* Use this function to handle a read event on the descriptor. It will try
 * and read some bytes from the socket and feed them to the reply parser.
 *
 * After this function is called, you may use redisGetReplyFromReader to
 * see if there is a reply available.
 * 首先将数据从socket读取到某个缓冲区中
 * */
int redisBufferRead(redisContext *c) {
    char buf[1024*16];
    int nread;

    /* Return early when the context has seen an error. */
    if (c->err)
        return REDIS_ERR;

    // 这里无法确定能读取到多少数据 也就是粘拆包问题
    nread = c->funcs->read(c, buf, sizeof(buf));
    if (nread > 0) {
        // 将数据填充到reader中
        if (redisReaderFeed(c->reader, buf, nread) != REDIS_OK) {
            __redisSetError(c, c->reader->err, c->reader->errstr);
            return REDIS_ERR;
        } else {
        }
    } else if (nread < 0) {
        return REDIS_ERR;
    }
    return REDIS_OK;
}

/* Write the output buffer to the socket.
 *
 * Returns REDIS_OK when the buffer is empty, or (a part of) the buffer was
 * successfully written to the socket. When the buffer is empty after the
 * write operation, "done" is set to 1 (if given).
 *
 * Returns REDIS_ERR if an error occurred trying to write and sets
 * c->errstr to hold the appropriate error string.
 * 异步写入就是先将数据存储到缓冲区中 等write事件准备好后 再将缓存区的数据写入到socket中
 */
int redisBufferWrite(redisContext *c, int *done) {

    /* Return early when the context has seen an error.
     * 此时已经存在异常信息了 无法将数据写入到缓冲区
     * */
    if (c->err)
        return REDIS_ERR;

    if (hi_sdslen(c->obuf) > 0) {
        // 实际上就是将obuf中的数据写入到socket缓冲区
        ssize_t nwritten = c->funcs->write(c);
        if (nwritten < 0) {
            return REDIS_ERR;
        } else if (nwritten > 0) {
            if (nwritten == (ssize_t)hi_sdslen(c->obuf)) {
                hi_sdsfree(c->obuf);
                c->obuf = hi_sdsempty();
                if (c->obuf == NULL)
                    goto oom;
            } else {
                if (hi_sdsrange(c->obuf,nwritten,-1) < 0) goto oom;
            }
        }
    }
    // 根据缓冲区中是否还有残留数据 返回done
    if (done != NULL) *done = (hi_sdslen(c->obuf) == 0);
    return REDIS_OK;

oom:
    __redisSetError(c, REDIS_ERR_OOM, "Out of memory");
    return REDIS_ERR;
}

/* Internal helper function to try and get a reply from the reader,
 * or set an error in the context otherwise.
 * 将c->reader缓冲区内的数据解析成reply (实际上就是进行粘包和拆包处理)
 * */
int redisGetReplyFromReader(redisContext *c, void **reply) {
    if (redisReaderGetReply(c->reader,reply) == REDIS_ERR) {
        __redisSetError(c,c->reader->err,c->reader->errstr);
        return REDIS_ERR;
    }

    return REDIS_OK;
}

/* Internal helper that returns 1 if the reply was a RESP3 PUSH
 * message and we handled it with a user-provided callback. */
static int redisHandledPushReply(redisContext *c, void *reply) {
    if (reply && c->push_cb && redisIsPushReply(reply)) {
        c->push_cb(c->privdata, reply);
        return 1;
    }

    return 0;
}

/**
 * 首先解析目标节点返回的数据
 * @param c
 * @param reply
 * @return
 */
int redisGetReply(redisContext *c, void **reply) {
    int wdone = 0;
    void *aux = NULL;

    /* Try to read pending replies
     * 解析reader缓冲区内的数据
     * */
    if (redisGetReplyFromReader(c,&aux) == REDIS_ERR)
        return REDIS_ERR;

    /* For the blocking context, flush output buffer and read reply
     * TODO 先忽略阻塞式
     * */
    if (aux == NULL && c->flags & REDIS_BLOCK) {
        /* Write until done */
        do {
            if (redisBufferWrite(c,&wdone) == REDIS_ERR)
                return REDIS_ERR;
        } while (!wdone);

        /* Read until there is a reply */
        do {
            if (redisBufferRead(c) == REDIS_ERR)
                return REDIS_ERR;

            /* We loop here in case the user has specified a RESP3
             * PUSH handler (e.g. for client tracking). */
            do {
                if (redisGetReplyFromReader(c,&aux) == REDIS_ERR)
                    return REDIS_ERR;
            } while (redisHandledPushReply(c, aux));
        } while (aux == NULL);
    }

    /* Set reply or free it if we were passed NULL */
    if (reply != NULL) {
        *reply = aux;
    } else {
        freeReplyObject(aux);
    }

    return REDIS_OK;
}


/* Helper function for the redisAppendCommand* family of functions.
 *
 * Write a formatted command to the output buffer. When this family
 * is used, you need to call redisGetReply yourself to retrieve
 * the reply (or replies in pub/sub).
 * 将数据写入到buf中
 */
int __redisAppendCommand(redisContext *c, const char *cmd, size_t len) {
    hisds newbuf;

    newbuf = hi_sdscatlen(c->obuf,cmd,len);
    if (newbuf == NULL) {
        __redisSetError(c,REDIS_ERR_OOM,"Out of memory");
        return REDIS_ERR;
    }

    c->obuf = newbuf;
    return REDIS_OK;
}

int redisAppendFormattedCommand(redisContext *c, const char *cmd, size_t len) {

    if (__redisAppendCommand(c, cmd, len) != REDIS_OK) {
        return REDIS_ERR;
    }

    return REDIS_OK;
}

int redisvAppendCommand(redisContext *c, const char *format, va_list ap) {
    char *cmd;
    int len;

    len = redisvFormatCommand(&cmd,format,ap);
    if (len == -1) {
        __redisSetError(c,REDIS_ERR_OOM,"Out of memory");
        return REDIS_ERR;
    } else if (len == -2) {
        __redisSetError(c,REDIS_ERR_OTHER,"Invalid format string");
        return REDIS_ERR;
    }

    if (__redisAppendCommand(c,cmd,len) != REDIS_OK) {
        hi_free(cmd);
        return REDIS_ERR;
    }

    hi_free(cmd);
    return REDIS_OK;
}

int redisAppendCommand(redisContext *c, const char *format, ...) {
    va_list ap;
    int ret;

    va_start(ap,format);
    ret = redisvAppendCommand(c,format,ap);
    va_end(ap);
    return ret;
}

int redisAppendCommandArgv(redisContext *c, int argc, const char **argv, const size_t *argvlen) {
    hisds cmd;
    int len;

    len = redisFormatSdsCommandArgv(&cmd,argc,argv,argvlen);
    if (len == -1) {
        __redisSetError(c,REDIS_ERR_OOM,"Out of memory");
        return REDIS_ERR;
    }

    if (__redisAppendCommand(c,cmd,len) != REDIS_OK) {
        hi_sdsfree(cmd);
        return REDIS_ERR;
    }

    hi_sdsfree(cmd);
    return REDIS_OK;
}

/* Helper function for the redisCommand* family of functions.
 *
 * Write a formatted command to the output buffer. If the given context is
 * blocking, immediately read the reply into the "reply" pointer. When the
 * context is non-blocking, the "reply" pointer will not be used and the
 * command is simply appended to the write buffer.
 *
 * Returns the reply when a reply was successfully retrieved. Returns NULL
 * otherwise. When NULL is returned in a blocking context, the error field
 * in the context will be set.
 */
static void *__redisBlockForReply(redisContext *c) {
    void *reply;

    if (c->flags & REDIS_BLOCK) {
        if (redisGetReply(c,&reply) != REDIS_OK)
            return NULL;
        return reply;
    }
    return NULL;
}

void *redisvCommand(redisContext *c, const char *format, va_list ap) {
    if (redisvAppendCommand(c,format,ap) != REDIS_OK)
        return NULL;
    return __redisBlockForReply(c);
}

void *redisCommand(redisContext *c, const char *format, ...) {
    va_list ap;
    va_start(ap,format);
    void *reply = redisvCommand(c,format,ap);
    va_end(ap);
    return reply;
}

void *redisCommandArgv(redisContext *c, int argc, const char **argv, const size_t *argvlen) {
    if (redisAppendCommandArgv(c,argc,argv,argvlen) != REDIS_OK)
        return NULL;
    return __redisBlockForReply(c);
}
