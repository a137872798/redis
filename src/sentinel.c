/* Redis Sentinel implementation
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
#include "hiredis.h"

#ifdef USE_OPENSSL
#include "openssl/ssl.h"
#include "hiredis_ssl.h"
#endif

#include "async.h"

#include <ctype.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <fcntl.h>

extern char **environ;

#ifdef USE_OPENSSL
extern SSL_CTX *redis_tls_ctx;
#endif

#define REDIS_SENTINEL_PORT 26379

/* ======================== Sentinel global state =========================== */

/* Address object, used to describe an ip:port pair. */
typedef struct sentinelAddr {
    char *ip;
    int port;
} sentinelAddr;

/* A Sentinel Redis Instance object is monitoring. */
#define SRI_MASTER  (1<<0)
#define SRI_SLAVE   (1<<1)
#define SRI_SENTINEL (1<<2)
// 代表哨兵认为该节点已经下线
#define SRI_S_DOWN (1<<3)   /* Subjectively down (no quorum). */
#define SRI_O_DOWN (1<<4)   /* Objectively down (confirmed by others). */
#define SRI_MASTER_DOWN (1<<5) /* A Sentinel with this flag set thinks that
                                   its master is down. */
// 代表该节点已经在一轮选举中了 无法发起新的选举
#define SRI_FAILOVER_IN_PROGRESS (1<<6) /* Failover is in progress for
                                           this master. */
#define SRI_PROMOTED (1<<7)            /* Slave selected for promotion. */
#define SRI_RECONF_SENT (1<<8)     /* SLAVEOF <newmaster> sent. */
#define SRI_RECONF_INPROG (1<<9)   /* Slave synchronization in progress. */
#define SRI_RECONF_DONE (1<<10)     /* Slave synchronized with new master. */
// 代表手动触发的选举
#define SRI_FORCE_FAILOVER (1<<11)  /* Force failover with master up. */
// 代表对端正在执行一条耗时的脚本  发出一条关闭脚本的指令
#define SRI_SCRIPT_KILL_SENT (1<<12) /* SCRIPT KILL already sent on -BUSY */

/* Note: times are in milliseconds. */
#define SENTINEL_INFO_PERIOD 10000
#define SENTINEL_PING_PERIOD 1000
#define SENTINEL_ASK_PERIOD 1000
#define SENTINEL_PUBLISH_PERIOD 2000
#define SENTINEL_DEFAULT_DOWN_AFTER 30000
#define SENTINEL_HELLO_CHANNEL "__sentinel__:hello"
#define SENTINEL_TILT_TRIGGER 2000
#define SENTINEL_TILT_PERIOD (SENTINEL_PING_PERIOD*30)
#define SENTINEL_DEFAULT_SLAVE_PRIORITY 100
#define SENTINEL_SLAVE_RECONF_TIMEOUT 10000
#define SENTINEL_DEFAULT_PARALLEL_SYNCS 1
#define SENTINEL_MIN_LINK_RECONNECT_PERIOD 15000
#define SENTINEL_DEFAULT_FAILOVER_TIMEOUT (60*3*1000)
#define SENTINEL_MAX_PENDING_COMMANDS 100
#define SENTINEL_ELECTION_TIMEOUT 10000
#define SENTINEL_MAX_DESYNC 1000
#define SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG 1

/* Failover machine different states. */
#define SENTINEL_FAILOVER_STATE_NONE 0  /* No failover in progress. */
// 故障转移的第一个阶段，代表针对某个master节点发起了故障转移
#define SENTINEL_FAILOVER_STATE_WAIT_START 1  /* Wait for failover_start_time*/
#define SENTINEL_FAILOVER_STATE_SELECT_SLAVE 2 /* Select slave to promote */
#define SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE 3 /* Slave -> Master */
#define SENTINEL_FAILOVER_STATE_WAIT_PROMOTION 4 /* Wait slave to change role */
// 此时master已经成为slave 并选择了一个新的master
#define SENTINEL_FAILOVER_STATE_RECONF_SLAVES 5 /* SLAVEOF newmaster */
#define SENTINEL_FAILOVER_STATE_UPDATE_CONFIG 6 /* Monitor promoted slave. */

// 作为slave此时是否已经连接上master了
#define SENTINEL_MASTER_LINK_STATUS_UP 0
#define SENTINEL_MASTER_LINK_STATUS_DOWN 1

/* Generic flags that can be used with different functions.
 * They use higher bits to avoid colliding with the function specific
 * flags. */
#define SENTINEL_NO_FLAGS 0
#define SENTINEL_GENERATE_EVENT (1<<16)
#define SENTINEL_LEADER (1<<17)
#define SENTINEL_OBSERVER (1<<18)

/* Script execution flags and limits. */
#define SENTINEL_SCRIPT_NONE 0
#define SENTINEL_SCRIPT_RUNNING 1
#define SENTINEL_SCRIPT_MAX_QUEUE 256
#define SENTINEL_SCRIPT_MAX_RUNNING 16
#define SENTINEL_SCRIPT_MAX_RUNTIME 60000 /* 60 seconds max exec time. */
#define SENTINEL_SCRIPT_MAX_RETRY 10
#define SENTINEL_SCRIPT_RETRY_DELAY 30000 /* 30 seconds between retries. */

/* SENTINEL SIMULATE-FAILURE command flags. */
#define SENTINEL_SIMFAILURE_NONE 0
#define SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION (1<<0)
#define SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION (1<<1)

/* The link to a sentinelRedisInstance. When we have the same set of Sentinels
 * monitoring many masters, we have different instances representing the
 * same Sentinels, one per master, and we need to share the hiredis connections
 * among them. Otherwise if 5 Sentinels are monitoring 100 masters we create
 * 500 outgoing connections instead of 5.
 *
 * So this structure represents a reference counted link in terms of the two
 * hiredis connections for commands and Pub/Sub, and the fields needed for
 * failure detection, since the ping/pong time are now local to the link: if
 * the link is available, the instance is available. This way we don't just
 * have 5 connections instead of 500, we also send 5 pings instead of 500.
 *
 * Links are shared only for Sentinels: master and slave instances have
 * a link with refcount = 1, always.
 * 一个link对象中可能存在2个上下文对象 一个对应普通连接 一个对应订阅发布管道
 * */
typedef struct instanceLink {
    int refcount;          /* Number of sentinelRedisInstance owners. */
    int disconnected;      /* Non-zero if we need to reconnect cc or pc. */
    // 此时待执行的所有command  因为command是支持异步执行的   应该是在连接还没建立的时候 就可以先指定稍后要执行的command
    int pending_commands;  /* Number of commands sent waiting for a reply. */
    redisAsyncContext *cc; /* Hiredis context for commands. */
    redisAsyncContext *pc; /* Hiredis context for Pub / Sub. */
    mstime_t cc_conn_time; /* cc connection time. */
    mstime_t pc_conn_time; /* pc connection time. */
    // 对于订阅发布专用的连接 最后一次收到消息的时间
    mstime_t pc_last_activity; /* Last time we received any message. */
    mstime_t last_avail_time; /* Last time the instance replied to ping with
                                 a reply we consider valid. */
    mstime_t act_ping_time;   /* Time at which the last pending ping (no pong
                                 received after it) was sent. This field is
                                 set to 0 when a pong is received, and set again
                                 to the current time if the value is 0 and a new
                                 ping is sent. */
    mstime_t last_ping_time;  /* Time at which we sent the last ping. This is
                                 only used to avoid sending too many pings
                                 during failure. Idle time is computed using
                                 the act_ping_time field. */
    // 最后收到对端心跳响应的时间
    mstime_t last_pong_time;  /* Last time the instance replied to ping,
                                 whatever the reply was. That's used to check
                                 if the link is idle and must be reconnected. */
    mstime_t last_reconn_time;  /* Last reconnection attempt performed when
                                   the link was down. */
} instanceLink;

typedef struct sentinelRedisInstance {
    int flags;      /* See SRI_... defines */
    char *name;     /* Master name from the point of view of this sentinel. */
    char *runid;    /* Run ID of this instance, or unique ID if is a Sentinel.*/
    uint64_t config_epoch;  /* Configuration epoch. */
    sentinelAddr *addr; /* Master host. */
    instanceLink *link; /* Link to the instance, may be shared for Sentinels. */
    mstime_t last_pub_time;   /* Last time we sent hello via Pub/Sub. */
    mstime_t last_hello_time; /* Only used if SRI_SENTINEL is set. Last time
                                 we received a hello from this Sentinel
                                 via Pub/Sub. */
    // 该哨兵接收到master下线的并回复的时间戳
    mstime_t last_master_down_reply_time; /* Time of last reply to
                                             SENTINEL is-master-down command. */
    mstime_t s_down_since_time; /* Subjectively down since time. */
    mstime_t o_down_since_time; /* Objectively down since time. */
    // 如果超过这么长时间没有交互 会认为该节点已经下线
    mstime_t down_after_period; /* Consider it down after that period. */
    mstime_t info_refresh;  /* Time at which we received INFO output from it. */
    dict *renamed_commands;     /* Commands renamed in this instance:
                                   Sentinel will use the alternative commands
                                   mapped on this table to send things like
                                   SLAVEOF, CONFING, INFO, ... */

    /* Role and the first time we observed it.
     * This is useful in order to delay replacing what the instance reports
     * with our own configuration. We need to always wait some time in order
     * to give a chance to the leader to report the new configuration before
     * we do silly things. */
    int role_reported;
    mstime_t role_reported_time;
    mstime_t slave_conf_change_time; /* Last time slave master addr changed. */

    /* Master specific. */
    // 除了本哨兵外 还有多少哨兵在监控这个master
    dict *sentinels;    /* Other sentinels monitoring the same master. */
    // 当前这个master实例下所有的slave
    dict *slaves;       /* Slaves for this master instance. */
    // 当认为本实例下线的 sentinel数量达到该值 就是客观上的下线
    unsigned int quorum;/* Number of sentinels that need to agree on failure. */
    // 同一时间最多允许同时对多少slave发起reconf的请求
    int parallel_syncs; /* How many slaves to reconfigure at same time. */
    char *auth_pass;    /* Password to use for AUTH against master & replica. */
    char *auth_user;    /* Username for ACLs AUTH against master & replica. */

    /* Slave specific. */
    mstime_t master_link_down_time; /* Slave replication link down time. */
    // 作为master的slave 在选择接替人时会参考该值 优先级高的会被推举
    int slave_priority; /* Slave priority according to its INFO output. */
    mstime_t slave_reconf_sent_time; /* Time at which we sent SLAVE OF <new> */

    // 作为slave和sentinel 都只能指定一个master
    struct sentinelRedisInstance *master; /* Master instance if it's slave. */
    char *slave_master_host;    /* Master host as reported by INFO */
    int slave_master_port;      /* Master port as reported by INFO */
    int slave_master_link_status; /* Master link status as reported by INFO */
    unsigned long long slave_repl_offset; /* Slave replication offset. */
    /* Failover */
    char *leader;       /* If this is a master instance, this is the runid of
                           the Sentinel that should perform the failover. If
                           this is a Sentinel, this is the runid of the Sentinel
                           that this Sentinel voted as leader. */
    uint64_t leader_epoch; /* Epoch of the 'leader' field. */
    uint64_t failover_epoch; /* Epoch of the currently started failover. */
    int failover_state; /* See SENTINEL_FAILOVER_STATE_* defines. */
    mstime_t failover_state_change_time;

    // 记录选举发起的时间
    mstime_t failover_start_time;   /* Last failover attempt start time. */
    mstime_t failover_timeout;      /* Max time to refresh failover state. */
    mstime_t failover_delay_logged; /* For what failover_start_time value we
                                       logged the failover delay. */
    struct sentinelRedisInstance *promoted_slave; /* Promoted slave instance. */
    /* Scripts executed to notify admin or reconfigure clients: when they
     * are set to NULL no script is executed. */
    char *notification_script;
    char *client_reconfig_script;
    // 描述该实例的信息
    sds info; /* cached INFO output */
} sentinelRedisInstance;

/* Main state. */
struct sentinelState {
    char myid[CONFIG_RUN_ID_SIZE + 1]; /* This sentinel ID. */
    uint64_t current_epoch;         /* Current epoch. */
    dict *masters;      /* Dictionary of master sentinelRedisInstances.
                           Key is the instance name, value is the
                           sentinelRedisInstance structure pointer. */
    int tilt;           /* Are we in TILT mode? */
    int running_scripts;    /* Number of scripts in execution right now. */
    mstime_t tilt_start_time;       /* When TITL started. */
    mstime_t previous_time;         /* Last time we ran the time handler. */
    // 存储了所有待执行的脚本
    list *scripts_queue;            /* Queue of user scripts to execute. */
    char *announce_ip;  /* IP addr that is gossiped to other sentinels if
                           not NULL. */
    int announce_port;  /* Port that is gossiped to other sentinels if
                           non zero. */
    unsigned long simfailure_flags; /* Failures simulation. */
    int deny_scripts_reconfig; /* Allow SENTINEL SET ... to change script
                                  paths at runtime? */
} sentinel;

/* A script execution job.
 * 待执行的脚本会被包装成job对象
 * */
typedef struct sentinelScriptJob {
    int flags;              /* Script job flags: SENTINEL_SCRIPT_* */
    int retry_num;          /* Number of times we tried to execute it. */
    char **argv;            /* Arguments to call the script. */
    mstime_t start_time;    /* Script execution time if the script is running,
                               otherwise 0 if we are allowed to retry the
                               execution at any time. If the script is not
                               running and it's not 0, it means: do not run
                               before the specified time. */
    // 执行脚本的子进程id
    pid_t pid;              /* Script execution pid. */
} sentinelScriptJob;

/* ======================= hiredis ae.c adapters =============================
 * Note: this implementation is taken from hiredis/adapters/ae.h, however
 * we have our modified copy for Sentinel in order to use our allocator
 * and to have full control over how the adapter works. */

typedef struct redisAeEvents {
    redisAsyncContext *context;
    aeEventLoop *loop;
    int fd;
    int reading, writing;
} redisAeEvents;

/**
 * @param el
 * @param fd
 * @param privdata
 * @param mask
 */
static void redisAeReadEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    ((void) el);
    ((void) fd);
    ((void) mask);

    redisAeEvents *e = (redisAeEvents *) privdata;
    redisAsyncHandleRead(e->context);
}

/**
 * 在哨兵模块中 与其他节点建立普通连接/订阅发布连接时 会注册这个写事件处理器
 * @param el
 * @param fd
 * @param privdata
 * @param mask
 */
static void redisAeWriteEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    ((void) el);
    ((void) fd);
    ((void) mask);

    redisAeEvents *e = (redisAeEvents *) privdata;
    redisAsyncHandleWrite(e->context);
}

/**
 * 在el上注册读事件
 * @param privdata
 */
static void redisAeAddRead(void *privdata) {
    redisAeEvents *e = (redisAeEvents *) privdata;
    aeEventLoop *loop = e->loop;
    // reading代表已经注册了读事件 这里是避免重复注册
    if (!e->reading) {
        e->reading = 1;
        aeCreateFileEvent(loop, e->fd, AE_READABLE, redisAeReadEvent, e);
    }
}

static void redisAeDelRead(void *privdata) {
    redisAeEvents *e = (redisAeEvents *) privdata;
    aeEventLoop *loop = e->loop;
    if (e->reading) {
        e->reading = 0;
        aeDeleteFileEvent(loop, e->fd, AE_READABLE);
    }
}

/**
 * 在首次设置onConnect函数时 会触发该方法
 * @param privdata  对应redisAeEvents对象 内部还有el
 */
static void redisAeAddWrite(void *privdata) {
    redisAeEvents *e = (redisAeEvents *) privdata;
    aeEventLoop *loop = e->loop;
    // writing代表此时已经注册了写事件
    if (!e->writing) {
        e->writing = 1;
        aeCreateFileEvent(loop, e->fd, AE_WRITABLE, redisAeWriteEvent, e);
    }
}

/**
 * 当context缓冲区内的数据发送完毕后 不再注册write事件
 * @param privdata
 */
static void redisAeDelWrite(void *privdata) {
    redisAeEvents *e = (redisAeEvents *) privdata;
    aeEventLoop *loop = e->loop;
    if (e->writing) {
        e->writing = 0;
        aeDeleteFileEvent(loop, e->fd, AE_WRITABLE);
    }
}

static void redisAeCleanup(void *privdata) {
    redisAeEvents *e = (redisAeEvents *) privdata;
    redisAeDelRead(privdata);
    redisAeDelWrite(privdata);
    zfree(e);
}

/**
 * 将事件循环 以及一些其他函数设置到ac上
 * @param loop
 * @param ac
 * @return
 */
static int redisAeAttach(aeEventLoop *loop, redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    redisAeEvents *e;

    /* Nothing should be attached when something is already attached
     * 代表此时已经为该上下文设置过函数了 不需要重复处理
     * */
    if (ac->ev.data != NULL)
        return C_ERR;

    /* Create container for context and r/w events
     * 事件循环相关参数绑定在event上 event设置在data上
     * */
    e = (redisAeEvents *) zmalloc(sizeof(*e));
    e->context = ac;
    e->loop = loop;
    e->fd = c->fd;

    // 初始状态 2个标记位都是0
    e->reading = e->writing = 0;

    /* Register functions to start/stop listening for events */
    ac->ev.addRead = redisAeAddRead;
    ac->ev.delRead = redisAeDelRead;
    ac->ev.addWrite = redisAeAddWrite;
    ac->ev.delWrite = redisAeDelWrite;
    ac->ev.cleanup = redisAeCleanup;
    ac->ev.data = e;

    return C_OK;
}

/* ============================= Prototypes ================================= */

void sentinelLinkEstablishedCallback(const redisAsyncContext *c, int status);

void sentinelDisconnectCallback(const redisAsyncContext *c, int status);

void sentinelReceiveHelloMessages(redisAsyncContext *c, void *reply, void *privdata);

sentinelRedisInstance *sentinelGetMasterByName(char *name);

char *sentinelGetSubjectiveLeader(sentinelRedisInstance *master);

char *sentinelGetObjectiveLeader(sentinelRedisInstance *master);

int yesnotoi(char *s);

void instanceLinkConnectionError(const redisAsyncContext *c);

const char *sentinelRedisInstanceTypeStr(sentinelRedisInstance *ri);

void sentinelAbortFailover(sentinelRedisInstance *ri);

void sentinelEvent(int level, char *type, sentinelRedisInstance *ri, const char *fmt, ...);

sentinelRedisInstance *sentinelSelectSlave(sentinelRedisInstance *master);

void sentinelScheduleScriptExecution(char *path, ...);

void sentinelStartFailover(sentinelRedisInstance *master);

void sentinelDiscardReplyCallback(redisAsyncContext *c, void *reply, void *privdata);

int sentinelSendSlaveOf(sentinelRedisInstance *ri, char *host, int port);

char *sentinelVoteLeader(sentinelRedisInstance *master, uint64_t req_epoch, char *req_runid, uint64_t *leader_epoch);

void sentinelFlushConfig(void);

void sentinelGenerateInitialMonitorEvents(void);

int sentinelSendPing(sentinelRedisInstance *ri);

int sentinelForceHelloUpdateForMaster(sentinelRedisInstance *master);

sentinelRedisInstance *getSentinelRedisInstanceByAddrAndRunID(dict *instances, char *ip, int port, char *runid);

void sentinelSimFailureCrash(void);

/* ========================= Dictionary types =============================== */

uint64_t dictSdsHash(const void *key);

uint64_t dictSdsCaseHash(const void *key);

int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);

int dictSdsKeyCaseCompare(void *privdata, const void *key1, const void *key2);

void releaseSentinelRedisInstance(sentinelRedisInstance *ri);

void dictInstancesValDestructor(void *privdata, void *obj) {
    UNUSED(privdata);
    releaseSentinelRedisInstance(obj);
}

/* Instance name (sds) -> instance (sentinelRedisInstance pointer)
 *
 * also used for: sentinelRedisInstance->sentinels dictionary that maps
 * sentinels ip:port to last seen time in Pub/Sub hello message. */
dictType instancesDictType = {
        dictSdsHash,               /* hash function */
        NULL,                      /* key dup */
        NULL,                      /* val dup */
        dictSdsKeyCompare,         /* key compare */
        NULL,                      /* key destructor */
        dictInstancesValDestructor /* val destructor */
};

/* Instance runid (sds) -> votes (long casted to void*)
 *
 * This is useful into sentinelGetObjectiveLeader() function in order to
 * count the votes and understand who is the leader. */
dictType leaderVotesDictType = {
        dictSdsHash,               /* hash function */
        NULL,                      /* key dup */
        NULL,                      /* val dup */
        dictSdsKeyCompare,         /* key compare */
        NULL,                      /* key destructor */
        NULL                       /* val destructor */
};

/* Instance renamed commands table. */
dictType renamedCommandsDictType = {
        dictSdsCaseHash,           /* hash function */
        NULL,                      /* key dup */
        NULL,                      /* val dup */
        dictSdsKeyCaseCompare,     /* key compare */
        dictSdsDestructor,         /* key destructor */
        dictSdsDestructor          /* val destructor */
};

/* =========================== Initialization =============================== */

void sentinelCommand(client *c);

void sentinelInfoCommand(client *c);

void sentinelSetCommand(client *c);

void sentinelPublishCommand(client *c);

void sentinelRoleCommand(client *c);

/**
 * 这是哨兵支持的所有command
 */
struct redisCommand sentinelcmds[] = {
        {"ping",         pingCommand,            1,  "",                                           0, NULL, 0, 0, 0, 0, 0},
        {"sentinel",     sentinelCommand,        -2, "",                                           0, NULL, 0, 0, 0, 0, 0},
        {"subscribe",    subscribeCommand,       -2, "",                                           0, NULL, 0, 0, 0, 0, 0},
        {"unsubscribe",  unsubscribeCommand,     -1, "",                                           0, NULL, 0, 0, 0, 0, 0},
        {"psubscribe",   psubscribeCommand,      -2, "",                                           0, NULL, 0, 0, 0, 0, 0},
        {"punsubscribe", punsubscribeCommand,    -1, "",                                           0, NULL, 0, 0, 0, 0, 0},
        {"publish",      sentinelPublishCommand, 3,  "",                                           0, NULL, 0, 0, 0, 0, 0},
        {"info",         sentinelInfoCommand,    -1, "",                                           0, NULL, 0, 0, 0, 0, 0},
        {"role",         sentinelRoleCommand,    1,  "ok-loading",                                 0, NULL, 0, 0, 0, 0, 0},
        {"client",       clientCommand,          -2, "read-only no-script",                        0, NULL, 0, 0, 0, 0, 0},
        {"shutdown",     shutdownCommand,        -1, "",                                           0, NULL, 0, 0, 0, 0, 0},
        {"auth",         authCommand,            2,  "no-auth no-script ok-loading ok-stale fast", 0, NULL, 0, 0, 0, 0, 0},
        {"hello",        helloCommand,           -2, "no-auth no-script fast",                     0, NULL, 0, 0, 0, 0, 0}
};

/* This function overwrites a few normal Redis config default with Sentinel
 * specific defaults.
 * 当某个节点发现是以哨兵模式启动时 会触发该方法
 * */
void initSentinelConfig(void) {
    // 哨兵节点的端口是固定的
    server.port = REDIS_SENTINEL_PORT;
    server.protected_mode = 0; /* Sentinel must be exposed. */
}

/* Perform the Sentinel mode initialization.
 * 当redis以哨兵模式启动时 会在初始阶段执行该方法
 * */
void initSentinel(void) {
    unsigned int j;

    /* Remove usual Redis commands from the command table, then just add
     * the SENTINEL command.
     * 看来哨兵是一个特殊的节点 它上面能够执行的command 与一般的redis节点不一样
     * */
    dictEmpty(server.commands, NULL);
    for (j = 0; j < sizeof(sentinelcmds) / sizeof(sentinelcmds[0]); j++) {
        int retval;
        struct redisCommand *cmd = sentinelcmds + j;

        retval = dictAdd(server.commands, sdsnew(cmd->name), cmd);
        serverAssert(retval == DICT_OK);

        /* Translate the command string flags description into an actual
         * set of flags.
         * 从cmd关联的 sflags中解析flag信息 并填充到cmd上
         * */
        if (populateCommandTableParseFlags(cmd, cmd->sflags) == C_ERR)
            serverPanic("Unsupported command flag");
    }

    /* Initialize various data structures. 这里只是一些相关属性的初始化 */
    sentinel.current_epoch = 0;
    // 这是本哨兵监控的所有master
    sentinel.masters = dictCreate(&instancesDictType, NULL);
    sentinel.tilt = 0;
    sentinel.tilt_start_time = 0;
    sentinel.previous_time = mstime();
    sentinel.running_scripts = 0;
    sentinel.scripts_queue = listCreate();
    sentinel.announce_ip = NULL;
    sentinel.announce_port = 0;
    sentinel.simfailure_flags = SENTINEL_SIMFAILURE_NONE;
    sentinel.deny_scripts_reconfig = SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG;
    // 置空 myid
    memset(sentinel.myid, 0, sizeof(sentinel.myid));
}

/* This function gets called when the server is in Sentinel mode, started,
 * loaded the configuration, and is ready for normal operations.
 * 在服务器启动阶段，加载完配置文件后会调用该方法 */
void sentinelIsRunning(void) {
    int j;

    if (server.configfile == NULL) {
        serverLog(LL_WARNING,
                  "Sentinel started without a config file. Exiting...");
        exit(1);
    } else if (access(server.configfile, W_OK) == -1) {
        serverLog(LL_WARNING,
                  "Sentinel config file %s is not writable: %s. Exiting...",
                  server.configfile, strerror(errno));
        exit(1);
    }

    /* If this Sentinel has yet no ID set in the configuration file, we
     * pick a random one and persist the config on disk. From now on this
     * will be this Sentinel ID across restarts.
     * 在启动时，配置文件内部可能会包含myid的相关信息
     * */
    for (j = 0; j < CONFIG_RUN_ID_SIZE; j++)
        if (sentinel.myid[j] != 0) break;

    // 因为是从低位开始往高位写入数据 所以当j已经到达最低位时 就代表myid内部没有数据 就需要随机生成一个
    if (j == CONFIG_RUN_ID_SIZE) {
        /* Pick ID and persist the config. */
        getRandomHexChars(sentinel.myid, CONFIG_RUN_ID_SIZE);
        // 将当前节点相关数据写入到配置文件中并刷盘
        sentinelFlushConfig();
    }

    /* Log its ID to make debugging of issues simpler. */
    serverLog(LL_WARNING, "Sentinel ID is %s", sentinel.myid);

    /* We want to generate a +monitor event for every configured master
     * at startup.
     * 为每个master在消息总线发布监控事件
     * */
    sentinelGenerateInitialMonitorEvents();
}

/* ============================== sentinelAddr ============================== */

/* Create a sentinelAddr object and return it on success.
 * On error NULL is returned and errno is set to:
 *  ENOENT: Can't resolve the hostname.
 *  EINVAL: Invalid port number.
 */
sentinelAddr *createSentinelAddr(char *hostname, int port) {
    char ip[NET_IP_STR_LEN];
    sentinelAddr *sa;

    if (port < 0 || port > 65535) {
        errno = EINVAL;
        return NULL;
    }
    if (anetResolve(NULL, hostname, ip, sizeof(ip)) == ANET_ERR) {
        errno = ENOENT;
        return NULL;
    }
    sa = zmalloc(sizeof(*sa));
    sa->ip = sdsnew(ip);
    sa->port = port;
    return sa;
}

/* Return a duplicate of the source address. */
sentinelAddr *dupSentinelAddr(sentinelAddr *src) {
    sentinelAddr *sa;

    sa = zmalloc(sizeof(*sa));
    sa->ip = sdsnew(src->ip);
    sa->port = src->port;
    return sa;
}

/* Free a Sentinel address. Can't fail. */
void releaseSentinelAddr(sentinelAddr *sa) {
    sdsfree(sa->ip);
    zfree(sa);
}

/* Return non-zero if two addresses are equal. */
int sentinelAddrIsEqual(sentinelAddr *a, sentinelAddr *b) {
    return a->port == b->port && !strcasecmp(a->ip, b->ip);
}

/* =========================== Events notification ========================== */

/* Send an event to log, pub/sub, user notification script.
 *
 * 'level' is the log level for logging. Only LL_WARNING events will trigger
 * the execution of the user notification script.
 *
 * 'type' is the message type, also used as a pub/sub channel name.
 *
 * 'ri', is the redis instance target of this event if applicable, and is
 * used to obtain the path of the notification script to execute.
 *
 * The remaining arguments are printf-alike.
 * If the format specifier starts with the two characters "%@" then ri is
 * not NULL, and the message is prefixed with an instance identifier in the
 * following format:
 *
 *  <instance type> <instance name> <ip> <port>
 *
 *  If the instance type is not master, than the additional string is
 *  added to specify the originating master:
 *
 *  @ <master name> <master ip> <master port>
 *
 *  Any other specifier after "%@" is processed by printf itself.
 *  针对某个实例发出一个事件  还会涉及到订阅发布 以及notification_script
 *  @param type 代表本次的事件类型
 *  @param ri
 */
void sentinelEvent(int level, char *type, sentinelRedisInstance *ri,
                   const char *fmt, ...) {
    va_list ap;
    char msg[LOG_MAX_LEN];
    robj *channel, *payload;

    /* Handle %@
     * %@ 后面的是参数信息
     * */
    if (fmt[0] == '%' && fmt[1] == '@') {
        // sri 代表 sentinel_redis_instance
        sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ?
                                        NULL : ri->master;

        // 代表本次是一个slave节点 在msg中会追加master的信息
        if (master) {
            snprintf(msg, sizeof(msg), "%s %s %s %d @ %s %s %d",
                     sentinelRedisInstanceTypeStr(ri),
                     ri->name, ri->addr->ip, ri->addr->port,
                     master->name, master->addr->ip, master->addr->port);
        } else {
            // 本次针对的是一个master节点
            snprintf(msg, sizeof(msg), "%s %s %s %d",
                     sentinelRedisInstanceTypeStr(ri),
                     ri->name, ri->addr->ip, ri->addr->port);
        }
        fmt += 2;
    } else {
        msg[0] = '\0';
    }

    /* Use vsprintf for the rest of the formatting if any.
     * 将本次要发送的信息 追加到msg上
     * */
    if (fmt[0] != '\0') {
        va_start(ap, fmt);
        vsnprintf(msg + strlen(msg), sizeof(msg) - strlen(msg), fmt, ap);
        va_end(ap);
    }

    /* Log the message if the log level allows it to be logged.
     * TODO
     * */
    if (level >= server.verbosity)
        serverLog(level, "%s %s", type, msg);

    /* Publish the message via Pub/Sub if it's not a debugging one.
     * debug以上的级别信息还会通过订阅发布模式推送出去。 但是在启动过程中还没有看到某个client发起订阅请求
     * */
    if (level != LL_DEBUG) {
        channel = createStringObject(type, strlen(type));
        payload = createStringObject(msg, strlen(msg));
        pubsubPublishMessage(channel, payload);
        decrRefCount(channel);
        decrRefCount(payload);
    }

    /* Call the notification script if applicable.
     * TODO
     * */
    if (level == LL_WARNING && ri != NULL) {
        sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ?
                                        ri : ri->master;
        if (master && master->notification_script) {
            sentinelScheduleScriptExecution(master->notification_script,
                                            type, msg, NULL);
        }
    }
}

/* This function is called only at startup and is used to generate a
 * +monitor event for every configured master. The same events are also
 * generated when a master to monitor is added at runtime via the
 * SENTINEL MONITOR command.
 * 针对所有master节点发起监控事件
 * */
void sentinelGenerateInitialMonitorEvents(void) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(sentinel.masters);
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        sentinelEvent(LL_WARNING, "+monitor", ri, "%@ quorum %d", ri->quorum);
    }
    dictReleaseIterator(di);
}

/* ============================ script execution ============================ */

/* Release a script job structure and all the associated data.
 * 释放脚本
 * */
void sentinelReleaseScriptJob(sentinelScriptJob *sj) {
    int j = 0;

    while (sj->argv[j]) sdsfree(sj->argv[j++]);
    zfree(sj->argv);
    zfree(sj);
}

#define SENTINEL_SCRIPT_MAX_ARGS 16

void sentinelScheduleScriptExecution(char *path, ...) {
    va_list ap;
    char *argv[SENTINEL_SCRIPT_MAX_ARGS + 1];
    int argc = 1;
    sentinelScriptJob *sj;

    va_start(ap, path);
    while (argc < SENTINEL_SCRIPT_MAX_ARGS) {
        argv[argc] = va_arg(ap, char*);
        if (!argv[argc]) break;
        argv[argc] = sdsnew(argv[argc]); /* Copy the string. */
        argc++;
    }
    va_end(ap);
    argv[0] = sdsnew(path);

    sj = zmalloc(sizeof(*sj));
    sj->flags = SENTINEL_SCRIPT_NONE;
    sj->retry_num = 0;
    sj->argv = zmalloc(sizeof(char *) * (argc + 1));
    sj->start_time = 0;
    sj->pid = 0;
    memcpy(sj->argv, argv, sizeof(char *) * (argc + 1));

    listAddNodeTail(sentinel.scripts_queue, sj);

    /* Remove the oldest non running script if we already hit the limit. */
    if (listLength(sentinel.scripts_queue) > SENTINEL_SCRIPT_MAX_QUEUE) {
        listNode *ln;
        listIter li;

        listRewind(sentinel.scripts_queue, &li);
        while ((ln = listNext(&li)) != NULL) {
            sj = ln->value;

            if (sj->flags & SENTINEL_SCRIPT_RUNNING) continue;
            /* The first node is the oldest as we add on tail. */
            listDelNode(sentinel.scripts_queue, ln);
            sentinelReleaseScriptJob(sj);
            break;
        }
        serverAssert(listLength(sentinel.scripts_queue) <=
                     SENTINEL_SCRIPT_MAX_QUEUE);
    }
}

/* Lookup a script in the scripts queue via pid, and returns the list node
 * (so that we can easily remove it from the queue if needed).
 * 通过pid匹配脚本
 * */
listNode *sentinelGetScriptListNodeByPid(pid_t pid) {
    listNode *ln;
    listIter li;

    listRewind(sentinel.scripts_queue, &li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;

        if ((sj->flags & SENTINEL_SCRIPT_RUNNING) && sj->pid == pid)
            return ln;
    }
    return NULL;
}

/* Run pending scripts if we are not already at max number of running
 * scripts.
 * 执行所有待处理的脚本 是lua脚本吗
 * */
void sentinelRunPendingScripts(void) {
    listNode *ln;
    listIter li;
    mstime_t now = mstime();

    /* Find jobs that are not running and run them, from the top to the
     * tail of the queue, so we run older jobs first. */
    listRewind(sentinel.scripts_queue, &li);
    // 单次最多只执行16个脚本
    while (sentinel.running_scripts < SENTINEL_SCRIPT_MAX_RUNNING &&
           (ln = listNext(&li)) != NULL) {
        // 获取脚本任务实例
        sentinelScriptJob *sj = ln->value;
        pid_t pid;

        /* Skip if already running.
         * 跳过正在准备执行的脚本
         * */
        if (sj->flags & SENTINEL_SCRIPT_RUNNING) continue;

        /* Skip if it's a retry, but not enough time has elapsed.
         * 开始时间在当前时间之后的 代表的是重试时间
         * */
        if (sj->start_time && sj->start_time > now) continue;

        // 增加running的标记
        sj->flags |= SENTINEL_SCRIPT_RUNNING;
        sj->start_time = mstime();
        sj->retry_num++;
        pid = fork();

        // 代表创建子进程失败了
        if (pid == -1) {
            /* Parent (fork error).
             * We report fork errors as signal 99, in order to unify the
             * reporting with other kind of errors. */
            sentinelEvent(LL_WARNING, "-script-error", NULL,
                          "%s %d %d", sj->argv[0], 99, 0);
            sj->flags &= ~SENTINEL_SCRIPT_RUNNING;
            sj->pid = 0;
        } else if (pid == 0) {
            /* Child
             * 执行脚本文件
             * */
            execve(sj->argv[0], sj->argv, environ);
            /* If we are here an error occurred. */
            _exit(2); /* Don't retry execution. */
        } else {
            // 增加此时执行的脚本数量 并发出事件
            sentinel.running_scripts++;
            sj->pid = pid;
            sentinelEvent(LL_DEBUG, "+script-child", NULL, "%ld", (long) pid);
        }
    }
}

/* How much to delay the execution of a script that we need to retry after
 * an error?
 *
 * We double the retry delay for every further retry we do. So for instance
 * if RETRY_DELAY is set to 30 seconds and the max number of retries is 10
 * starting from the second attempt to execute the script the delays are:
 * 30 sec, 60 sec, 2 min, 4 min, 8 min, 16 min, 32 min, 64 min, 128 min. */
mstime_t sentinelScriptRetryDelay(int retry_num) {
    mstime_t delay = SENTINEL_SCRIPT_RETRY_DELAY;

    while (retry_num-- > 1) delay *= 2;
    return delay;
}

/* Check for scripts that terminated, and remove them from the queue if the
 * script terminated successfully. If instead the script was terminated by
 * a signal, or returned exit code "1", it is scheduled to run again if
 * the max number of retries did not already elapsed.
 * 将已经处理完毕的脚本移除
 * */
void sentinelCollectTerminatedScripts(void) {
    int statloc;
    pid_t pid;

    // 阻塞当前进程 直到运行脚本的所有子进程处理完毕
    while ((pid = wait3(&statloc, WNOHANG, NULL)) > 0) {
        int exitcode = WEXITSTATUS(statloc);
        int bysignal = 0;
        listNode *ln;
        sentinelScriptJob *sj;

        if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);
        sentinelEvent(LL_DEBUG, "-script-child", NULL, "%ld %d %d",
                      (long) pid, exitcode, bysignal);

        // 通过pid找到本次执行的脚本
        ln = sentinelGetScriptListNodeByPid(pid);
        if (ln == NULL) {
            serverLog(LL_WARNING, "wait3() returned a pid (%ld) we can't find in our scripts execution queue!",
                      (long) pid);
            continue;
        }
        sj = ln->value;

        /* If the script was terminated by a signal or returns an
         * exit code of "1" (that means: please retry), we reschedule it
         * if the max number of retries is not already reached.
         * 本次脚本被意外终止
         * */
        if ((bysignal || exitcode == 1) &&
            sj->retry_num != SENTINEL_SCRIPT_MAX_RETRY) {
            sj->flags &= ~SENTINEL_SCRIPT_RUNNING;
            sj->pid = 0;
            // 计算下一次启动时间
            sj->start_time = mstime() +
                             sentinelScriptRetryDelay(sj->retry_num);
        } else {
            /* Otherwise let's remove the script, but log the event if the
             * execution did not terminated in the best of the ways.
             * 此时已经不满足重试条件了 将该脚本删除
             * */
            if (bysignal || exitcode != 0) {
                sentinelEvent(LL_WARNING, "-script-error", NULL,
                              "%s %d %d", sj->argv[0], bysignal, exitcode);
            }
            listDelNode(sentinel.scripts_queue, ln);
            sentinelReleaseScriptJob(sj);
        }
        sentinel.running_scripts--;
    }
}

/* Kill scripts in timeout, they'll be collected by the
 * sentinelCollectTerminatedScripts() function.
 * 清理已经超时的脚本
 * */
void sentinelKillTimedoutScripts(void) {
    listNode *ln;
    listIter li;
    mstime_t now = mstime();

    listRewind(sentinel.scripts_queue, &li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;

        if (sj->flags & SENTINEL_SCRIPT_RUNNING &&
            (now - sj->start_time) > SENTINEL_SCRIPT_MAX_RUNTIME) {
            sentinelEvent(LL_WARNING, "-script-timeout", NULL, "%s %ld",
                          sj->argv[0], (long) sj->pid);
            kill(sj->pid, SIGKILL);
        }
    }
}

/* Implements SENTINEL PENDING-SCRIPTS command.
 * 将所有待执行的脚本信息返回给client
 * */
void sentinelPendingScriptsCommand(client *c) {
    listNode *ln;
    listIter li;

    addReplyArrayLen(c, listLength(sentinel.scripts_queue));
    listRewind(sentinel.scripts_queue, &li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;
        int j = 0;

        addReplyMapLen(c, 5);

        addReplyBulkCString(c, "argv");
        while (sj->argv[j]) j++;
        addReplyArrayLen(c, j);
        j = 0;
        // 将参数信息 挨个写入到client中
        while (sj->argv[j]) addReplyBulkCString(c, sj->argv[j++]);

        // 将job的相关信息返回
        addReplyBulkCString(c, "flags");
        addReplyBulkCString(c,
                            (sj->flags & SENTINEL_SCRIPT_RUNNING) ? "running" : "scheduled");

        addReplyBulkCString(c, "pid");
        addReplyBulkLongLong(c, sj->pid);

        if (sj->flags & SENTINEL_SCRIPT_RUNNING) {
            addReplyBulkCString(c, "run-time");
            addReplyBulkLongLong(c, mstime() - sj->start_time);
        } else {
            mstime_t delay = sj->start_time ? (sj->start_time - mstime()) : 0;
            if (delay < 0) delay = 0;
            addReplyBulkCString(c, "run-delay");
            addReplyBulkLongLong(c, delay);
        }

        addReplyBulkCString(c, "retry-num");
        addReplyBulkLongLong(c, sj->retry_num);
    }
}

/* This function calls, if any, the client reconfiguration script with the
 * following parameters:
 *
 * <master-name> <role> <state> <from-ip> <from-port> <to-ip> <to-port>
 *
 * It is called every time a failover is performed.
 *
 * <state> is currently always "failover".
 * <role> is either "leader" or "observer".
 *
 * from/to fields are respectively master -> promoted slave addresses for
 * "start" and "end".
 * 当角色变更时 执行脚本
 * */
void sentinelCallClientReconfScript(sentinelRedisInstance *master, int role, char *state, sentinelAddr *from,
                                    sentinelAddr *to) {
    char fromport[32], toport[32];

    // 如果没有配置脚本 不需要处理 TODO 先不考虑脚本
    if (master->client_reconfig_script == NULL) return;
    ll2string(fromport, sizeof(fromport), from->port);
    ll2string(toport, sizeof(toport), to->port);
    sentinelScheduleScriptExecution(master->client_reconfig_script,
                                    master->name,
                                    (role == SENTINEL_LEADER) ? "leader" : "observer",
                                    state, from->ip, fromport, to->ip, toport, NULL);
}

/* =============================== instanceLink ============================= */

/* Create a not yet connected link object.
 * 在创建实例对象时 同时会创建一个link对象 此时该对象还未真正进行连接
 * */
instanceLink *createInstanceLink(void) {
    instanceLink *link = zmalloc(sizeof(*link));

    link->refcount = 1;
    link->disconnected = 1;
    link->pending_commands = 0;
    // 默认情况下创建的cc/pc都是NULL
    link->cc = NULL;
    link->pc = NULL;
    link->cc_conn_time = 0;
    link->pc_conn_time = 0;
    link->last_reconn_time = 0;
    link->pc_last_activity = 0;
    /* We set the act_ping_time to "now" even if we actually don't have yet
     * a connection with the node, nor we sent a ping.
     * This is useful to detect a timeout in case we'll not be able to connect
     * with the node at all. */
    link->act_ping_time = mstime();
    link->last_ping_time = 0;
    link->last_avail_time = mstime();
    link->last_pong_time = mstime();
    return link;
}

/* Disconnect a hiredis connection in the context of an instance link.
 * 关闭与某个实例的连接 必须要传入上下文 并且当上下文与link的属性一致时 释放上下文对象
 * */
void instanceLinkCloseConnection(instanceLink *link, redisAsyncContext *c) {
    if (c == NULL) return;

    if (link->cc == c) {
        link->cc = NULL;
        link->pending_commands = 0;
    }
    if (link->pc == c) link->pc = NULL;
    c->data = NULL;
    link->disconnected = 1;
    redisAsyncFree(c);
}

/* Decrement the refcount of a link object, if it drops to zero, actually
 * free it and return NULL. Otherwise don't do anything and return the pointer
 * to the object.
 *
 * If we are not going to free the link and ri is not NULL, we rebind all the
 * pending requests in link->cc (hiredis connection for commands) to a
 * callback that will just ignore them. This is useful to avoid processing
 * replies for an instance that no longer exists.
 * 释放连接对象
 * */
instanceLink *releaseInstanceLink(instanceLink *link, sentinelRedisInstance *ri) {
    // link在刚创建时 引用计数默认为1
    serverAssert(link->refcount > 0);
    link->refcount--;
    // 代表还有其他对象在使用这条连接 此时还不能释放
    if (link->refcount != 0) {
        // 作为该实例的连接在概念上应该已经被关闭了  所以需要处理相关的回调函数
        if (ri && ri->link->cc) {
            /* This instance may have pending callbacks in the hiredis async
             * context, having as 'privdata' the instance that we are going to
             * free. Let's rewrite the callback list, directly exploiting
             * hiredis internal data structures, in order to bind them with
             * a callback that will ignore the reply at all.
             * 上下文对象上绑定了一组回调函数
             * */
            redisCallback *cb;
            redisCallbackList *callbacks = &link->cc->replies;

            cb = callbacks->head;
            while (cb) {
                // 加了这个判断是因为 多个实例关联的link实际上是同一个对象 所有实例创建的回调对象绑定在同一个链表上
                if (cb->privdata == ri) {
                    // 回调对象原本作为处理通过该link接收到的数据的函数 而连接被断开后就修改成一个丢弃所有接收数据的默认函数
                    cb->fn = sentinelDiscardReplyCallback;
                    // 因为该回调已经无效了 也没有必要保留privdata
                    cb->privdata = NULL; /* Not strictly needed. */
                }
                cb = cb->next;
            }
        }
        return link; /* Other active users. */
    }

    // 代表该连接确实被完全关闭了 做一些内存释放操作 以及设置link->disconnect 为true
    instanceLinkCloseConnection(link, link->cc);
    instanceLinkCloseConnection(link, link->pc);
    zfree(link);
    return NULL;
}

/* This function will attempt to share the instance link we already have
 * for the same Sentinel in the context of a different master, with the
 * instance we are passing as argument.
 *
 * This way multiple Sentinel objects that refer all to the same physical
 * Sentinel instance but in the context of different masters will use
 * a single connection, will send a single PING per second for failure
 * detection and so forth.
 *
 * Return C_OK if a matching Sentinel was found in the context of a
 * different master and sharing was performed. Otherwise C_ERR
 * is returned.
 * 即使同一个哨兵监控多个master  也会为该哨兵创建多个实例 本哨兵连接这些实例只需要一个真实连接
 * */
int sentinelTryConnectionSharing(sentinelRedisInstance *ri) {
    // 要求该节点必须是sentinel
    serverAssert(ri->flags & SRI_SENTINEL);
    dictIterator *di;
    dictEntry *de;

    // 作为哨兵节点 必须指定runid
    if (ri->runid == NULL) return C_ERR; /* No way to identify it. */

    // 该实例的连接已经是指向共享连接了
    if (ri->link->refcount > 1) return C_ERR; /* Already shared. */

    // 下面就是遍历所有的master 查看他们下面是否有某个哨兵与本实例的地址一致 就可以共用连接了
    di = dictGetIterator(sentinel.masters);
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *master = dictGetVal(de), *match;
        /* We want to share with the same physical Sentinel referenced
         * in other masters, so skip our master.
         * 跳过同一master
         * */
        if (master == ri->master) continue;

        // 通过runid去匹配 同一哨兵连接到不同master时 对应多个实例 但是他们的runid还是一致的 并且应该共用一个连接
        match = getSentinelRedisInstanceByAddrAndRunID(master->sentinels,
                                                       NULL, 0, ri->runid);

        // 代表该哨兵没有监控该master
        if (match == NULL) continue; /* No match. */
        // 同一实例不需要处理
        if (match == ri) continue; /* Should never happen but... safer. */

        /* We identified a matching Sentinel, great! Let's free our link
         * and use the one of the matching Sentinel.
         * */
        releaseInstanceLink(ri->link, NULL);
        // 共用同一个连接对象
        ri->link = match->link;
        match->link->refcount++;
        dictReleaseIterator(di);
        return C_OK;
    }
    dictReleaseIterator(di);
    return C_ERR;
}

/* When we detect a Sentinel to switch address (reporting a different IP/port
 * pair in Hello messages), let's update all the matching Sentinels in the
 * context of other masters as well and disconnect the links, so that everybody
 * will be updated.
 *
 * Return the number of updated Sentinel addresses.
 * @param ri 当某个哨兵的地址发生了更新时触发
 * */
int sentinelUpdateSentinelAddressInAllMasters(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_SENTINEL);
    dictIterator *di;
    dictEntry *de;
    int reconfigured = 0;

    // 遍历本节点此时监控的所有master
    di = dictGetIterator(sentinel.masters);
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *master = dictGetVal(de), *match;
        // 通过runid 找到master下维护的所有哨兵实例中匹配的那个
        match = getSentinelRedisInstanceByAddrAndRunID(master->sentinels,
                                                       NULL, 0, ri->runid);
        /* If there is no match, this master does not know about this
         * Sentinel, try with the next one.
         * 此时master下并没有维护这个哨兵 不需要处理
         * */
        if (match == NULL) continue;

        /* Disconnect the old links if connected.
         * 因为这个哨兵的地址已经过期 所以要断开与该实例的连接 之后会在主循环中按照新的地址重新建立连接
         * */
        if (match->link->cc != NULL)
            instanceLinkCloseConnection(match->link, match->link->cc);
        if (match->link->pc != NULL)
            instanceLinkCloseConnection(match->link, match->link->pc);

        if (match == ri) continue; /* Address already updated for it. */

        /* Update the address of the matching Sentinel by copying the address
         * of the Sentinel object that received the address update.
         * 更新这个哨兵实例的地址
         * */
        releaseSentinelAddr(match->addr);
        match->addr = dupSentinelAddr(ri->addr);
        reconfigured++;
    }
    dictReleaseIterator(di);
    // 发布一个地址更新的事件
    if (reconfigured)
        sentinelEvent(LL_NOTICE, "+sentinel-address-update", ri,
                      "%@ %d additional matching instances", reconfigured);
    return reconfigured;
}

/* This function is called when a hiredis connection reported an error.
 * We set it to NULL and mark the link as disconnected so that it will be
 * reconnected again.
 *
 * Note: we don't free the hiredis context as hiredis will do it for us
 * for async connections.
 * 修改disconnected 以便之后发起重连
 * */
void instanceLinkConnectionError(const redisAsyncContext *c) {
    instanceLink *link = c->data;
    int pubsub;

    if (!link) return;

    pubsub = (link->pc == c);
    // 重置上下文才能发起重连
    if (pubsub)
        link->pc = NULL;
    else
        link->cc = NULL;
    link->disconnected = 1;
}

/* Hiredis connection established / disconnected callbacks. We need them
 * just to cleanup our link state.
 * 在连接完成时判断状态 连接失败的情况要进行处理
 * */
void sentinelLinkEstablishedCallback(const redisAsyncContext *c, int status) {
    if (status != C_OK) instanceLinkConnectionError(c);
}

void sentinelDisconnectCallback(const redisAsyncContext *c, int status) {
    UNUSED(status);
    instanceLinkConnectionError(c);
}

/* ========================== sentinelRedisInstance ========================= */

/* Create a redis instance, the following fields must be populated by the
 * caller if needed:
 * runid: set to NULL but will be populated once INFO output is received.
 * info_refresh: is set to 0 to mean that we never received INFO so far.
 *
 * If SRI_MASTER is set into initial flags the instance is added to
 * sentinel.masters table.
 *
 * if SRI_SLAVE or SRI_SENTINEL is set then 'master' must be not NULL and the
 * instance is added into master->slaves or master->sentinels table.
 *
 * If the instance is a slave or sentinel, the name parameter is ignored and
 * is created automatically as hostname:port.
 *
 * The function fails if hostname can't be resolved or port is out of range.
 * When this happens NULL is returned and errno is set accordingly to the
 * createSentinelAddr() function.
 *
 * The function may also fail and return NULL with errno set to EBUSY if
 * a master with the same name, a slave with the same address, or a sentinel
 * with the same ID already exists.
 * 创建某个实例对象
 * @param name 节点名称
 * @param flags 实例类型
 * */
sentinelRedisInstance *createSentinelRedisInstance(char *name, int flags, char *hostname, int port, int quorum,
                                                   sentinelRedisInstance *master) {
    sentinelRedisInstance *ri;
    sentinelAddr *addr;
    dict *table = NULL;
    char slavename[NET_PEER_ID_LEN], *sdsname;

    // flags中必须包含角色属性
    serverAssert(flags & (SRI_MASTER | SRI_SLAVE | SRI_SENTINEL));
    // 如果本次创建的实例不是master 必须要传入master参数
    serverAssert((flags & SRI_MASTER) || master != NULL);

    /* Check address validity.
     * 生成地址信息
     * */
    addr = createSentinelAddr(hostname, port);
    if (addr == NULL) return NULL;

    /* For slaves use ip:port as name.
     * 如果本节点是slave节点 会将ip:port作为name
     * */
    if (flags & SRI_SLAVE) {
        anetFormatAddr(slavename, sizeof(slavename), hostname, port);
        name = slavename;
    }

    /* Make sure the entry is not duplicated. This may happen when the same
     * name for a master is used multiple times inside the configuration or
     * if we try to add multiple times a slave or sentinel with same ip/port
     * to a master.
     * 根据此时的类型 将实例加入到对应的容器中
     * */
    if (flags & SRI_MASTER) table = sentinel.masters;
    else if (flags & SRI_SLAVE) table = master->slaves;
    else if (flags & SRI_SENTINEL) table = master->sentinels;
    sdsname = sdsnew(name);
    if (dictFind(table, sdsname)) {
        releaseSentinelAddr(addr);
        sdsfree(sdsname);
        errno = EBUSY;
        return NULL;
    }

    /* Create the instance object. */
    ri = zmalloc(sizeof(*ri));
    /* Note that all the instances are started in the disconnected state,
     * the event loop will take care of connecting them. */
    ri->flags = flags;
    ri->name = sdsname;
    ri->runid = NULL;
    ri->config_epoch = 0;
    ri->addr = addr;
    ri->link = createInstanceLink();
    ri->last_pub_time = mstime();
    ri->last_hello_time = mstime();
    ri->last_master_down_reply_time = mstime();
    ri->s_down_since_time = 0;
    ri->o_down_since_time = 0;
    ri->down_after_period = master ? master->down_after_period :
                            SENTINEL_DEFAULT_DOWN_AFTER;
    ri->master_link_down_time = 0;
    ri->auth_pass = NULL;
    ri->auth_user = NULL;
    ri->slave_priority = SENTINEL_DEFAULT_SLAVE_PRIORITY;
    ri->slave_reconf_sent_time = 0;
    ri->slave_master_host = NULL;
    ri->slave_master_port = 0;
    ri->slave_master_link_status = SENTINEL_MASTER_LINK_STATUS_DOWN;
    ri->slave_repl_offset = 0;
    ri->sentinels = dictCreate(&instancesDictType, NULL);
    // 作为slave节点 该值与master是一致的
    ri->quorum = quorum;
    ri->parallel_syncs = SENTINEL_DEFAULT_PARALLEL_SYNCS;
    ri->master = master;
    ri->slaves = dictCreate(&instancesDictType, NULL);
    ri->info_refresh = 0;
    ri->renamed_commands = dictCreate(&renamedCommandsDictType, NULL);

    /* Failover state. */
    ri->leader = NULL;
    ri->leader_epoch = 0;
    ri->failover_epoch = 0;
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = 0;
    ri->failover_start_time = 0;
    ri->failover_timeout = SENTINEL_DEFAULT_FAILOVER_TIMEOUT;
    ri->failover_delay_logged = 0;
    ri->promoted_slave = NULL;
    ri->notification_script = NULL;
    ri->client_reconfig_script = NULL;
    ri->info = NULL;

    /* Role
     * 标记此时实例的角色
     * */
    ri->role_reported = ri->flags & (SRI_MASTER | SRI_SLAVE);
    ri->role_reported_time = mstime();
    ri->slave_conf_change_time = mstime();

    /* Add into the right table. */
    dictAdd(table, ri->name, ri);
    return ri;
}

/* Release this instance and all its slaves, sentinels, hiredis connections.
 * This function does not take care of unlinking the instance from the main
 * masters table (if it is a master) or from its master sentinels/slaves table
 * if it is a slave or sentinel.
 * 就是对某个实例进行内存释放
 * */
void releaseSentinelRedisInstance(sentinelRedisInstance *ri) {
    /* Release all its slaves or sentinels if any. */
    dictRelease(ri->sentinels);
    dictRelease(ri->slaves);

    /* Disconnect the instance. */
    releaseInstanceLink(ri->link, ri);

    /* Free other resources. */
    sdsfree(ri->name);
    sdsfree(ri->runid);
    sdsfree(ri->notification_script);
    sdsfree(ri->client_reconfig_script);
    sdsfree(ri->slave_master_host);
    sdsfree(ri->leader);
    sdsfree(ri->auth_pass);
    sdsfree(ri->auth_user);
    sdsfree(ri->info);
    releaseSentinelAddr(ri->addr);
    dictRelease(ri->renamed_commands);

    /* Clear state into the master if needed. */
    if ((ri->flags & SRI_SLAVE) && (ri->flags & SRI_PROMOTED) && ri->master)
        ri->master->promoted_slave = NULL;

    zfree(ri);
}

/* Lookup a slave in a master Redis instance, by ip and port.
 * 检查该master下是否有ip/port匹配的slave
 * */
sentinelRedisInstance *sentinelRedisInstanceLookupSlave(
        sentinelRedisInstance *ri, char *ip, int port) {
    sds key;
    sentinelRedisInstance *slave;
    char buf[NET_PEER_ID_LEN];

    serverAssert(ri->flags & SRI_MASTER);
    anetFormatAddr(buf, sizeof(buf), ip, port);
    key = sdsnew(buf);
    // slaves 的key是addr
    slave = dictFetchValue(ri->slaves, key);
    sdsfree(key);
    return slave;
}

/* Return the name of the type of the instance as a string.
 * 返回某个redis实例类型
 * */
const char *sentinelRedisInstanceTypeStr(sentinelRedisInstance *ri) {
    if (ri->flags & SRI_MASTER) return "master";
    else if (ri->flags & SRI_SLAVE) return "slave";
    else if (ri->flags & SRI_SENTINEL) return "sentinel";
    else return "unknown";
}

/* This function remove the Sentinel with the specified ID from the
 * specified master.
 *
 * If "runid" is NULL the function returns ASAP.
 *
 * This function is useful because on Sentinels address switch, we want to
 * remove our old entry and add a new one for the same ID but with the new
 * address.
 *
 * The function returns 1 if the matching Sentinel was removed, otherwise
 * 0 if there was no Sentinel with this ID.
 * 将master下与runid匹配的sentinel移除
 * */
int removeMatchingSentinelFromMaster(sentinelRedisInstance *master, char *runid) {
    dictIterator *di;
    dictEntry *de;
    int removed = 0;

    if (runid == NULL) return 0;

    di = dictGetSafeIterator(master->sentinels);
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (ri->runid && strcmp(ri->runid, runid) == 0) {
            dictDelete(master->sentinels, ri->name);
            removed++;
        }
    }
    dictReleaseIterator(di);
    return removed;
}

/* Search an instance with the same runid, ip and port into a dictionary
 * of instances. Return NULL if not found, otherwise return the instance
 * pointer.
 *
 * runid or ip can be NULL. In such a case the search is performed only
 * by the non-NULL field.
 * 通过地址 或者runid 检查是否存在匹配的实例
 * @param runid
 * */
sentinelRedisInstance *getSentinelRedisInstanceByAddrAndRunID(dict *instances, char *ip, int port, char *runid) {
    dictIterator *di;
    dictEntry *de;
    sentinelRedisInstance *instance = NULL;

    // 地址或者id 至少要设置一个
    serverAssert(ip || runid);   /* User must pass at least one search param. */
    di = dictGetIterator(instances);
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        // 尝试使用runid匹配 但是该实例没有设置id 跳过
        if (runid && !ri->runid) continue;
        // 返回匹配成功的实例
        if ((runid == NULL || strcmp(ri->runid, runid) == 0) &&
            (ip == NULL || (strcmp(ri->addr->ip, ip) == 0 &&
                            ri->addr->port == port))) {
            instance = ri;
            break;
        }
    }
    dictReleaseIterator(di);
    return instance;
}

/* Master lookup by name
 * dict操作 就是通过name去查找某个master
 * */
sentinelRedisInstance *sentinelGetMasterByName(char *name) {
    sentinelRedisInstance *ri;
    sds sdsname = sdsnew(name);

    ri = dictFetchValue(sentinel.masters, sdsname);
    sdsfree(sdsname);
    return ri;
}

/* Add the specified flags to all the instances in the specified dictionary. */
void sentinelAddFlagsToDictOfRedisInstances(dict *instances, int flags) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        ri->flags |= flags;
    }
    dictReleaseIterator(di);
}

/* Remove the specified flags to all the instances in the specified
 * dictionary. */
void sentinelDelFlagsToDictOfRedisInstances(dict *instances, int flags) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        ri->flags &= ~flags;
    }
    dictReleaseIterator(di);
}

/* Reset the state of a monitored master:
 * 1) Remove all slaves.
 * 2) Remove all sentinels.
 * 3) Remove most of the flags resulting from runtime operations.
 * 4) Reset timers to their default value. For example after a reset it will be
 *    possible to failover again the same master ASAP, without waiting the
 *    failover timeout delay.
 * 5) In the process of doing this undo the failover if in progress.
 * 6) Disconnect the connections with the master (will reconnect automatically).
 * 重置master实例
 */
#define SENTINEL_RESET_NO_SENTINELS (1<<0)
void sentinelResetMaster(sentinelRedisInstance *ri, int flags) {
    // 要求该实例必须是一个master节点
    serverAssert(ri->flags & SRI_MASTER);
    // 释放该master下所有的slave节点
    dictRelease(ri->slaves);
    ri->slaves = dictCreate(&instancesDictType, NULL);

    // 只要flag中没有打上 排除sentinel的标记 就连同sentinel一并清理
    if (!(flags & SENTINEL_RESET_NO_SENTINELS)) {
        dictRelease(ri->sentinels);
        ri->sentinels = dictCreate(&instancesDictType, NULL);
    }

    // 关闭与该实例的连接  这里要同时释放2个异步上下文 一个是cc 一个是pc
    instanceLinkCloseConnection(ri->link, ri->link->cc);
    instanceLinkCloseConnection(ri->link, ri->link->pc);

    // 清理master标记
    ri->flags &= SRI_MASTER;

    // 释放leader  这个leader指的是某个哨兵
    if (ri->leader) {
        sdsfree(ri->leader);
        ri->leader = NULL;
    }
    // 重置剩余的参数
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = 0;
    ri->failover_start_time = 0; /* We can failover again ASAP. */
    ri->promoted_slave = NULL;
    sdsfree(ri->runid);
    sdsfree(ri->slave_master_host);
    ri->runid = NULL;
    ri->slave_master_host = NULL;
    ri->link->act_ping_time = mstime();
    ri->link->last_ping_time = 0;
    ri->link->last_avail_time = mstime();
    ri->link->last_pong_time = mstime();
    ri->role_reported_time = mstime();
    ri->role_reported = SRI_MASTER;
    if (flags & SENTINEL_GENERATE_EVENT)
        sentinelEvent(LL_WARNING, "+reset-master", ri, "%@");
}

/* Call sentinelResetMaster() on every master with a name matching the specified
 * pattern.
 * 找到一组匹配正则的实例 进行重置
 * */
int sentinelResetMastersByPattern(char *pattern, int flags) {
    dictIterator *di;
    dictEntry *de;
    int reset = 0;

    di = dictGetIterator(sentinel.masters);
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        if (ri->name) {
            if (stringmatch(pattern, ri->name, 0)) {
                sentinelResetMaster(ri, flags);
                reset++;
            }
        }
    }
    dictReleaseIterator(di);
    return reset;
}

/* Reset the specified master with sentinelResetMaster(), and also change
 * the ip:port address, but take the name of the instance unmodified.
 *
 * This is used to handle the +switch-master event.
 *
 * The function returns C_ERR if the address can't be resolved for some
 * reason. Otherwise C_OK is returned.
 * 将master节点的地址修改成 目标地址 同时将原本匹配的slave地址改成master地址
 * */
int sentinelResetMasterAndChangeAddress(sentinelRedisInstance *master, char *ip, int port) {
    sentinelAddr *oldaddr, *newaddr;
    sentinelAddr **slaves = NULL;
    int numslaves = 0, j;
    dictIterator *di;
    dictEntry *de;

    // 生成一个新的地址对象
    newaddr = createSentinelAddr(ip, port);
    if (newaddr == NULL) return C_ERR;

    /* Make a list of slaves to add back after the reset.
     * Don't include the one having the address we are switching to. */
    di = dictGetIterator(master->slaves);

    // 找到匹配的slave 之后会将该slave的地址修改成master的
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);

        if (sentinelAddrIsEqual(slave->addr, newaddr)) continue;
        // 相同的slave本次就没有保存在数组中
        slaves = zrealloc(slaves, sizeof(sentinelAddr *) * (numslaves + 1));
        slaves[numslaves++] = createSentinelAddr(slave->addr->ip,
                                                 slave->addr->port);
    }
    dictReleaseIterator(di);

    /* If we are switching to a different address, include the old address
     * as a slave as well, so that we'll be able to sense / reconfigure
     * the old master.
     * 新建一个地址对应原master的实例
     * */
    if (!sentinelAddrIsEqual(newaddr, master->addr)) {
        slaves = zrealloc(slaves, sizeof(sentinelAddr *) * (numslaves + 1));
        slaves[numslaves++] = createSentinelAddr(master->addr->ip,
                                                 master->addr->port);
    }

    /* Reset and switch address. 本次重置不需要修改sentinel信息 */
    sentinelResetMaster(master, SENTINEL_RESET_NO_SENTINELS);
    oldaddr = master->addr;
    master->addr = newaddr;
    // 重置下线的时间 因为现在需要重新连接
    master->o_down_since_time = 0;
    master->s_down_since_time = 0;

    /* Add slaves back. 根据之前保存的slave信息 重新创建这些实例 */
    for (j = 0; j < numslaves; j++) {
        sentinelRedisInstance *slave;

        slave = createSentinelRedisInstance(NULL, SRI_SLAVE, slaves[j]->ip,
                                            slaves[j]->port, master->quorum, master);
        releaseSentinelAddr(slaves[j]);
        if (slave) sentinelEvent(LL_NOTICE, "+slave", slave, "%@");
    }
    zfree(slaves);

    /* Release the old address at the end so we are safe even if the function
     * gets the master->addr->ip and master->addr->port as arguments. */
    releaseSentinelAddr(oldaddr);
    sentinelFlushConfig();
    return C_OK;
}

/* Return non-zero if there was no SDOWN or ODOWN error associated to this
 * instance in the latest 'ms' milliseconds. */
int sentinelRedisInstanceNoDownFor(sentinelRedisInstance *ri, mstime_t ms) {
    mstime_t most_recent;

    most_recent = ri->s_down_since_time;
    if (ri->o_down_since_time > most_recent)
        most_recent = ri->o_down_since_time;
    return most_recent == 0 || (mstime() - most_recent) > ms;
}

/* Return the current master address, that is, its address or the address
 * of the promoted slave if already operational.
 * 从某个实例中获取地址信息
 * */
sentinelAddr *sentinelGetCurrentMasterAddress(sentinelRedisInstance *master) {
    /* If we are failing over the master, and the state is already
     * SENTINEL_FAILOVER_STATE_RECONF_SLAVES or greater, it means that we
     * already have the new configuration epoch in the master, and the
     * slave acknowledged the configuration switch. Advertise the new
     * address.
     * TODO 这不同于cluster的故障转移   不过也代表着某个节点的降级  这时就会选出推举的下一个slave的地址
     * */
    if ((master->flags & SRI_FAILOVER_IN_PROGRESS) &&
        master->promoted_slave &&
        master->failover_state >= SENTINEL_FAILOVER_STATE_RECONF_SLAVES) {
        return master->promoted_slave->addr;
    } else {
        // 返回master的地址
        return master->addr;
    }
}

/* This function sets the down_after_period field value in 'master' to all
 * the slaves and sentinel instances connected to this master.
 * 当某个master节点的属性被修改后 他下面的所有哨兵，slave也要修改
 * */
void sentinelPropagateDownAfterPeriod(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;
    int j;
    dict *d[] = {master->slaves, master->sentinels, NULL};

    for (j = 0; d[j]; j++) {
        di = dictGetIterator(d[j]);
        while ((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);
            ri->down_after_period = master->down_after_period;
        }
        dictReleaseIterator(di);
    }
}

char *sentinelGetInstanceTypeString(sentinelRedisInstance *ri) {
    if (ri->flags & SRI_MASTER) return "master";
    else if (ri->flags & SRI_SLAVE) return "slave";
    else if (ri->flags & SRI_SENTINEL) return "sentinel";
    else return "unknown";
}

/* This function is used in order to send commands to Redis instances: the
 * commands we send from Sentinel may be renamed, a common case is a master
 * with CONFIG and SLAVEOF commands renamed for security concerns. In that
 * case we check the ri->renamed_command table (or if the instance is a slave,
 * we check the one of the master), and map the command that we should send
 * to the set of renamed commads. However, if the command was not renamed,
 * we just return "command" itself.
 * 在执行某个command时  可能传入的是一个别名 这里尝试映射成正确的command
 * @param ri 代表本次从哪个实例获取参数
 * @param command 别名
 * */
char *sentinelInstanceMapCommand(sentinelRedisInstance *ri, char *command) {
    sds sc = sdsnew(command);
    // 总是转成master实例
    if (ri->master) ri = ri->master;
    char *retval = dictFetchValue(ri->renamed_commands, sc);
    sdsfree(sc);
    return retval ? retval : command;
}

/* ============================ Config handling ============================= */

/**
 * 从字符串中解析所有sentinel相关的配置项
 * @param argv
 * @param argc
 * @return
 */
char *sentinelHandleConfiguration(char **argv, int argc) {
    sentinelRedisInstance *ri;

    // 代表要监控某个节点 从配置文件中读取到的都是master节点
    if (!strcasecmp(argv[0], "monitor") && argc == 5) {
        /* monitor <name> <host> <port> <quorum> */
        int quorum = atoi(argv[4]);

        // 当有多少个节点没有观测到master时 认为master已经下线
        if (quorum <= 0) return "Quorum must be 1 or greater.";
        if (createSentinelRedisInstance(argv[1], SRI_MASTER, argv[2],
                                        atoi(argv[3]), quorum, NULL) == NULL) {
            switch (errno) {
                case EBUSY:
                    return "Duplicated master name.";
                case ENOENT:
                    return "Can't resolve master instance hostname.";
                case EINVAL:
                    return "Invalid port number";
            }
        }
        // 设置当与某个节点多久时间没有交互就认为是下线了
    } else if (!strcasecmp(argv[0], "down-after-milliseconds") && argc == 3) {
        /* down-after-milliseconds <name> <milliseconds> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->down_after_period = atoi(argv[2]);
        if (ri->down_after_period <= 0)
            return "negative or zero time parameter.";
        sentinelPropagateDownAfterPeriod(ri);
        // 故障转移超时时间
    } else if (!strcasecmp(argv[0], "failover-timeout") && argc == 3) {
        /* failover-timeout <name> <milliseconds> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->failover_timeout = atoi(argv[2]);
        if (ri->failover_timeout <= 0)
            return "negative or zero time parameter.";
    } else if (!strcasecmp(argv[0], "parallel-syncs") && argc == 3) {
        /* parallel-syncs <name> <milliseconds> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->parallel_syncs = atoi(argv[2]);
    } else if (!strcasecmp(argv[0], "notification-script") && argc == 3) {
        /* notification-script <name> <path> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        if (access(argv[2], X_OK) == -1)
            return "Notification script seems non existing or non executable.";
        ri->notification_script = sdsnew(argv[2]);
    } else if (!strcasecmp(argv[0], "client-reconfig-script") && argc == 3) {
        /* client-reconfig-script <name> <path> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        if (access(argv[2], X_OK) == -1)
            return "Client reconfiguration script seems non existing or "
                   "non executable.";
        ri->client_reconfig_script = sdsnew(argv[2]);
    } else if (!strcasecmp(argv[0], "auth-pass") && argc == 3) {
        /* auth-pass <name> <password> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->auth_pass = sdsnew(argv[2]);
    } else if (!strcasecmp(argv[0], "auth-user") && argc == 3) {
        /* auth-user <name> <username> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->auth_user = sdsnew(argv[2]);
    } else if (!strcasecmp(argv[0], "current-epoch") && argc == 2) {
        /* current-epoch <epoch> */
        unsigned long long current_epoch = strtoull(argv[1], NULL, 10);
        if (current_epoch > sentinel.current_epoch)
            sentinel.current_epoch = current_epoch;
        // 通过配置文件信息恢复myid
    } else if (!strcasecmp(argv[0], "myid") && argc == 2) {
        if (strlen(argv[1]) != CONFIG_RUN_ID_SIZE)
            return "Malformed Sentinel id in myid option.";
        memcpy(sentinel.myid, argv[1], CONFIG_RUN_ID_SIZE);
    } else if (!strcasecmp(argv[0], "config-epoch") && argc == 3) {
        /* config-epoch <name> <epoch> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->config_epoch = strtoull(argv[2], NULL, 10);
        /* The following update of current_epoch is not really useful as
         * now the current epoch is persisted on the config file, but
         * we leave this check here for redundancy. */
        if (ri->config_epoch > sentinel.current_epoch)
            sentinel.current_epoch = ri->config_epoch;
    } else if (!strcasecmp(argv[0], "leader-epoch") && argc == 3) {
        /* leader-epoch <name> <epoch> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        ri->leader_epoch = strtoull(argv[2], NULL, 10);

        // 恢复某个master下的所有slave
    } else if ((!strcasecmp(argv[0], "known-slave") ||
                !strcasecmp(argv[0], "known-replica")) && argc == 4) {
        sentinelRedisInstance *slave;

        /* known-replica <name> <ip> <port> 作为slave节点必须要声明master */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        if ((slave = createSentinelRedisInstance(NULL, SRI_SLAVE, argv[2],
                                                 atoi(argv[3]), ri->quorum, ri)) == NULL) {
            return "Wrong hostname or port for replica.";
        }

        // 监控某个master的所有已知的哨兵   一个哨兵可以监控多个master 但是在生成配置文件时 是以master的视角来存储哨兵的
    } else if (!strcasecmp(argv[0], "known-sentinel") &&
               (argc == 4 || argc == 5)) {
        sentinelRedisInstance *si;

        if (argc == 5) { /* Ignore the old form without runid. */
            /* known-sentinel <name> <ip> <port> [runid] */
            ri = sentinelGetMasterByName(argv[1]);
            if (!ri) return "No such master with specified name.";
            if ((si = createSentinelRedisInstance(argv[4], SRI_SENTINEL, argv[2],
                                                  atoi(argv[3]), ri->quorum, ri)) == NULL) {
                return "Wrong hostname or port for sentinel.";
            }

            // 哨兵节点必须要设置runid
            si->runid = sdsnew(argv[4]);
            // 实际上该哨兵可能同时监控了多个master 但是针对每个关联关系会创建一个哨兵实例 而本节点对于这些哨兵实例实际上只需要维护一条真实连接就可以了
            sentinelTryConnectionSharing(si);
        }
    } else if (!strcasecmp(argv[0], "rename-command") && argc == 4) {
        /* rename-command <name> <command> <renamed-command> */
        ri = sentinelGetMasterByName(argv[1]);
        if (!ri) return "No such master with specified name.";
        sds oldcmd = sdsnew(argv[2]);
        sds newcmd = sdsnew(argv[3]);
        if (dictAdd(ri->renamed_commands, oldcmd, newcmd) != DICT_OK) {
            sdsfree(oldcmd);
            sdsfree(newcmd);
            return "Same command renamed multiple times with rename-command.";
        }

        // 代表哨兵使用了特殊的ip和port
    } else if (!strcasecmp(argv[0], "announce-ip") && argc == 2) {
        /* announce-ip <ip-address> */
        if (strlen(argv[1]))
            sentinel.announce_ip = sdsnew(argv[1]);
    } else if (!strcasecmp(argv[0], "announce-port") && argc == 2) {
        /* announce-port <port> */
        sentinel.announce_port = atoi(argv[1]);
    } else if (!strcasecmp(argv[0], "deny-scripts-reconfig") && argc == 2) {
        /* deny-scripts-reconfig <yes|no> */
        if ((sentinel.deny_scripts_reconfig = yesnotoi(argv[1])) == -1) {
            return "Please specify yes or no for the "
                   "deny-scripts-reconfig options.";
        }
    } else {
        return "Unrecognized sentinel configuration statement.";
    }
    return NULL;
}

/* Implements CONFIG REWRITE for "sentinel" option.
 * This is used not just to rewrite the configuration given by the user
 * (the configured masters) but also in order to retain the state of
 * Sentinel across restarts: config epoch of masters, associated slaves
 * and sentinel instances, and so forth.
 * 根据当前哨兵信息 改写配置项
 * */
void rewriteConfigSentinelOption(struct rewriteConfigState *state) {
    dictIterator *di, *di2;
    dictEntry *de;
    sds line;

    /* sentinel unique ID. */
    line = sdscatprintf(sdsempty(), "sentinel myid %s", sentinel.myid);
    // 写入myid
    rewriteConfigRewriteLine(state, "sentinel", line, 1);

    /* sentinel deny-scripts-reconfig. */
    line = sdscatprintf(sdsempty(), "sentinel deny-scripts-reconfig %s",
                        sentinel.deny_scripts_reconfig ? "yes" : "no");
    rewriteConfigRewriteLine(state, "sentinel", line,
                             sentinel.deny_scripts_reconfig != SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG);

    /* For every master emit a "sentinel monitor" config entry.
     * 存储本哨兵监控的所有master
     * */
    di = dictGetIterator(sentinel.masters);
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *master, *ri;
        sentinelAddr *master_addr;

        /* sentinel monitor */
        master = dictGetVal(de);
        // 获取master地址 如果该节点处于选举状态 选择它推荐的slave节点地址
        master_addr = sentinelGetCurrentMasterAddress(master);

        // 生成描述信息
        line = sdscatprintf(sdsempty(), "sentinel monitor %s %s %d %d",
                            master->name, master_addr->ip, master_addr->port,
                            master->quorum);

        // 将master地址写入到state中
        rewriteConfigRewriteLine(state, "sentinel", line, 1);

        /* sentinel down-after-milliseconds
         * 代表手动指定过下线时间 */
        if (master->down_after_period != SENTINEL_DEFAULT_DOWN_AFTER) {
            line = sdscatprintf(sdsempty(),
                                "sentinel down-after-milliseconds %s %ld",
                                master->name, (long) master->down_after_period);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }

        /* sentinel failover-timeout
         * 写入最新的选举超时时间
         * */
        if (master->failover_timeout != SENTINEL_DEFAULT_FAILOVER_TIMEOUT) {
            line = sdscatprintf(sdsempty(),
                                "sentinel failover-timeout %s %ld",
                                master->name, (long) master->failover_timeout);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }

        // 剩下的类似

        /* sentinel parallel-syncs */
        if (master->parallel_syncs != SENTINEL_DEFAULT_PARALLEL_SYNCS) {
            line = sdscatprintf(sdsempty(),
                                "sentinel parallel-syncs %s %d",
                                master->name, master->parallel_syncs);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }

        /* sentinel notification-script */
        if (master->notification_script) {
            line = sdscatprintf(sdsempty(),
                                "sentinel notification-script %s %s",
                                master->name, master->notification_script);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }

        /* sentinel client-reconfig-script */
        if (master->client_reconfig_script) {
            line = sdscatprintf(sdsempty(),
                                "sentinel client-reconfig-script %s %s",
                                master->name, master->client_reconfig_script);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }

        /* sentinel auth-pass & auth-user */
        if (master->auth_pass) {
            line = sdscatprintf(sdsempty(),
                                "sentinel auth-pass %s %s",
                                master->name, master->auth_pass);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }

        if (master->auth_user) {
            line = sdscatprintf(sdsempty(),
                                "sentinel auth-user %s %s",
                                master->name, master->auth_user);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }

        /* sentinel config-epoch */
        line = sdscatprintf(sdsempty(),
                            "sentinel config-epoch %s %llu",
                            master->name, (unsigned long long) master->config_epoch);
        rewriteConfigRewriteLine(state, "sentinel", line, 1);

        /* sentinel leader-epoch */
        line = sdscatprintf(sdsempty(),
                            "sentinel leader-epoch %s %llu",
                            master->name, (unsigned long long) master->leader_epoch);
        rewriteConfigRewriteLine(state, "sentinel", line, 1);

        /* sentinel known-slave
         * 保存该节点下的所有slave节点地址
         * */
        di2 = dictGetIterator(master->slaves);
        while ((de = dictNext(di2)) != NULL) {
            sentinelAddr *slave_addr;

            ri = dictGetVal(de);
            slave_addr = ri->addr;

            /* If master_addr (obtained using sentinelGetCurrentMasterAddress()
             * so it may be the address of the promoted slave) is equal to this
             * slave's address, a failover is in progress and the slave was
             * already successfully promoted. So as the address of this slave
             * we use the old master address instead.
             * 代表slave晋升， master降级 原master的地址就作为slave了
             * */
            if (sentinelAddrIsEqual(slave_addr, master_addr))
                slave_addr = master->addr;
            line = sdscatprintf(sdsempty(),
                                "sentinel known-replica %s %s %d",
                                master->name, slave_addr->ip, slave_addr->port);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }
        dictReleaseIterator(di2);

        /* sentinel known-sentinel
         * 当前master被哪些哨兵监控
         * */
        di2 = dictGetIterator(master->sentinels);
        while ((de = dictNext(di2)) != NULL) {
            ri = dictGetVal(de);
            if (ri->runid == NULL) continue;
            line = sdscatprintf(sdsempty(),
                                "sentinel known-sentinel %s %s %d %s",
                                master->name, ri->addr->ip, ri->addr->port, ri->runid);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }
        dictReleaseIterator(di2);

        /* sentinel rename-command */
        di2 = dictGetIterator(master->renamed_commands);
        while ((de = dictNext(di2)) != NULL) {
            sds oldname = dictGetKey(de);
            sds newname = dictGetVal(de);
            line = sdscatprintf(sdsempty(),
                                "sentinel rename-command %s %s %s",
                                master->name, oldname, newname);
            rewriteConfigRewriteLine(state, "sentinel", line, 1);
        }
        dictReleaseIterator(di2);
    }

    /* sentinel current-epoch is a global state valid for all the masters. */
    line = sdscatprintf(sdsempty(),
                        "sentinel current-epoch %llu", (unsigned long long) sentinel.current_epoch);
    rewriteConfigRewriteLine(state, "sentinel", line, 1);

    // 如果哨兵使用了特殊ip,port 存起来

    /* sentinel announce-ip. */
    if (sentinel.announce_ip) {
        line = sdsnew("sentinel announce-ip ");
        line = sdscatrepr(line, sentinel.announce_ip, sdslen(sentinel.announce_ip));
        rewriteConfigRewriteLine(state, "sentinel", line, 1);
    }

    /* sentinel announce-port. */
    if (sentinel.announce_port) {
        line = sdscatprintf(sdsempty(), "sentinel announce-port %d",
                            sentinel.announce_port);
        rewriteConfigRewriteLine(state, "sentinel", line, 1);
    }

    dictReleaseIterator(di);
}

/* This function uses the config rewriting Redis engine in order to persist
 * the state of the Sentinel in the current configuration file.
 *
 * Before returning the function calls fsync() against the generated
 * configuration file to make sure changes are committed to disk.
 *
 * On failure the function logs a warning on the Redis log.
 * 将最新的sentinel信息写入到配置文件 并刷盘
 * */
void sentinelFlushConfig(void) {
    int fd = -1;
    int saved_hz = server.hz;
    int rewrite_status;

    server.hz = CONFIG_DEFAULT_HZ;
    // 将本哨兵最新信息写入到配置文件中
    rewrite_status = rewriteConfig(server.configfile, 0);
    server.hz = saved_hz;

    if (rewrite_status == -1) goto werr;
    if ((fd = open(server.configfile, O_RDONLY)) == -1) goto werr;
    // 在这里进行刷盘
    if (fsync(fd) == -1) goto werr;
    if (close(fd) == EOF) goto werr;
    return;

    werr:
    if (fd != -1) close(fd);
    serverLog(LL_WARNING, "WARNING: Sentinel was not able to save the new configuration on disk!!!: %s",
              strerror(errno));
}

/* ====================== hiredis connection handling ======================= */

/* Send the AUTH command with the specified master password if needed.
 * Note that for slaves the password set for the master is used.
 *
 * In case this Sentinel requires a password as well, via the "requirepass"
 * configuration directive, we assume we should use the local password in
 * order to authenticate when connecting with the other Sentinels as well.
 * So basically all the Sentinels share the same password and use it to
 * authenticate reciprocally.
 *
 * We don't check at all if the command was successfully transmitted
 * to the instance as if it fails Sentinel will detect the instance down,
 * will disconnect and reconnect the link and so forth.
 * 根据上下文信息判断连接到某个实例时是否需要进行权限认证
 * */
void sentinelSendAuthIfNeeded(sentinelRedisInstance *ri, redisAsyncContext *c) {
    char *auth_pass = NULL;
    char *auth_user = NULL;

    if (ri->flags & SRI_MASTER) {
        auth_pass = ri->auth_pass;
        auth_user = ri->auth_user;
    } else if (ri->flags & SRI_SLAVE) {
        auth_pass = ri->master->auth_pass;
        auth_user = ri->master->auth_user;
    } else if (ri->flags & SRI_SENTINEL) {
        auth_pass = server.requirepass;
        auth_user = NULL;
    }

    // 只需要密码的场景  这些命令会先存储在容器中 等到连接完成后再回调
    if (auth_pass && auth_user == NULL) {
        if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri, "%s %s",
                              sentinelInstanceMapCommand(ri, "AUTH"),
                              auth_pass) == C_OK)
            ri->link->pending_commands++;
    } else if (auth_pass && auth_user) {
        /* If we also have an username, use the ACL-style AUTH command
         * with two arguments, username and password. */
        if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri, "%s %s %s",
                              sentinelInstanceMapCommand(ri, "AUTH"),
                              auth_user, auth_pass) == C_OK)
            ri->link->pending_commands++;
    }
    // 既没有用户名也没有密码的时候 代表不需要进行权限校验
}

/* Use CLIENT SETNAME to name the connection in the Redis instance as
 * sentinel-<first_8_chars_of_runid>-<connection_type>
 * The connection type is "cmd" or "pubsub" as specified by 'type'.
 *
 * This makes it possible to list all the sentinel instances connected
 * to a Redis server with CLIENT LIST, grepping for a specific name format.
 * 执行一个client command 是将本节点信息告知给对端节点
 * */
void sentinelSetClientName(sentinelRedisInstance *ri, redisAsyncContext *c, char *type) {
    char name[64];

    snprintf(name, sizeof(name), "sentinel-%.8s-%s", sentinel.myid, type);
    if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri,
                          "%s SETNAME %s",
                          sentinelInstanceMapCommand(ri, "CLIENT"),
                          name) == C_OK) {
        ri->link->pending_commands++;
    }
}

static int instanceLinkNegotiateTLS(redisAsyncContext *context) {
#ifndef USE_OPENSSL
    (void) context;
#else
    if (!redis_tls_ctx) return C_ERR;
    SSL *ssl = SSL_new(redis_tls_ctx);
    if (!ssl) return C_ERR;

    if (redisInitiateSSL(&context->c, ssl) == REDIS_ERR) return C_ERR;
#endif
    return C_OK;
}

/* Create the async connections for the instance link if the link
 * is disconnected. Note that link->disconnected is true even if just
 * one of the two links (commands and pub/sub) is missing.
 * 根据需要与该实例重新连接
 * */
void sentinelReconnectInstance(sentinelRedisInstance *ri) {
    // 当前连接没有断开就不需要处理
    if (ri->link->disconnected == 0) return;
    // 代表当前地址无效 无法重连
    if (ri->addr->port == 0) return; /* port == 0 means invalid address. */
    instanceLink *link = ri->link;
    mstime_t now = mstime();

    // 必须超过一定的间隔时间 才能发起重连 也是避免重复发起连接
    if (now - ri->link->last_reconn_time < SENTINEL_PING_PERIOD) return;

    // 更新最近一次重连时间
    ri->link->last_reconn_time = now;

    /* Commands connection.
     * 每个实例对象在创建时 会初始化一个link对象  当连接还没有发起时link->cc为null 通过检查该属性判断是否要发起连接
     * */
    if (link->cc == NULL) {
        // 这里发起了一个非阻塞式连接 可能直接成功 也可能还未成功
        link->cc = redisAsyncConnectBind(ri->addr->ip, ri->addr->port, NET_FIRST_BIND_ADDR);

        // 先忽略安全层失败
        if (!link->cc->err && server.tls_replication &&
            (instanceLinkNegotiateTLS(link->cc) == C_ERR)) {
            sentinelEvent(LL_DEBUG, "-cmd-link-reconnection", ri, "%@ #Failed to initialize TLS");
            // 对link做一些清理操作
            instanceLinkCloseConnection(link, link->cc);
            // 在发起连接过程中出现了异常 比如设置属性失败
        } else if (link->cc->err) {
            sentinelEvent(LL_DEBUG, "-cmd-link-reconnection", ri, "%@ #%s",
                          link->cc->errstr);
            instanceLinkCloseConnection(link, link->cc);
        } else {
            // 代表在重连阶段没有直接抛出异常  这里进行一些属性的重置
            link->pending_commands = 0;
            link->cc_conn_time = mstime();
            link->cc->data = link;
            // 将事件循环以及一些处理网络事件的函数设置到link上
            redisAeAttach(server.el, link->cc);

            // 设置连接完成时的回调 首次设置时还会触发addWrite函数  (addWrite函数会绑定一个写入函数redisAeWriteEvent)
            redisAsyncSetConnectCallback(link->cc,
                                         sentinelLinkEstablishedCallback);
            // 设置连接断开时的函数 基本同sentinelLinkEstablishedCallback 设置disconnect标记 并在下一次定时任务中重新发起连接
            redisAsyncSetDisconnectCallback(link->cc,
                                            sentinelDisconnectCallback);

            // 以下几个command都是异步的 数据都是暂时存放在缓冲区中

            // 判断连接到该实例时是否需要认证  会将数据流先存放到缓冲区中 至少要等到连接建立完成 才能发送数据
            // 如果没有设置用户名，密码就不需要认证
            sentinelSendAuthIfNeeded(ri, link->cc);
            // 当发送完认证信息后 立即发送一个描述本节点myid的请求
            sentinelSetClientName(ri, link->cc, "cmd");

            /* Send a PING ASAP when reconnecting.
             * 发送一个心跳请求  注意这里设置了用于处理reply结果的回调函数  主要就是用于更新响应时间的
             * */
            sentinelSendPing(ri);
        }
    }
    /* Pub / Sub
     * 如果该节点是master或者slave节点  会创建一个用于发布订阅的特殊通道
     * 可以看到sentinel与这些节点的交互很多都是通过sentinelEvent的 就是在这里建立的发布订阅关系
     * */
    if ((ri->flags & (SRI_MASTER | SRI_SLAVE)) && link->pc == NULL) {

        // 同样开始建立连接
        link->pc = redisAsyncConnectBind(ri->addr->ip, ri->addr->port, NET_FIRST_BIND_ADDR);
        if (!link->pc->err && server.tls_replication &&
            (instanceLinkNegotiateTLS(link->pc) == C_ERR)) {
            sentinelEvent(LL_DEBUG, "-pubsub-link-reconnection", ri, "%@ #Failed to initialize TLS");
        } else if (link->pc->err) {
            sentinelEvent(LL_DEBUG, "-pubsub-link-reconnection", ri, "%@ #%s",
                          link->pc->errstr);
            instanceLinkCloseConnection(link, link->pc);
        } else {
            int retval;

            link->pc_conn_time = mstime();
            link->pc->data = link;
            redisAeAttach(server.el, link->pc);
            redisAsyncSetConnectCallback(link->pc,
                                         sentinelLinkEstablishedCallback);
            redisAsyncSetDisconnectCallback(link->pc,
                                            sentinelDisconnectCallback);
            sentinelSendAuthIfNeeded(ri, link->pc);

            // 以上都和建立普通的管道一样 这里会将本节点信息发送过去，并表示本条连接是跟发布订阅有关的
            sentinelSetClientName(ri, link->pc, "pubsub");
            /* Now we subscribe to the Sentinels "Hello" channel.
             * 发起一条订阅请求 本次订阅的通道是SENTINEL_HELLO_CHANNEL
             * */
            retval = redisAsyncCommand(link->pc,
                                       sentinelReceiveHelloMessages, ri, "%s %s",
                                       sentinelInstanceMapCommand(ri, "SUBSCRIBE"),
                                       SENTINEL_HELLO_CHANNEL);
            if (retval != C_OK) {
                /* If we can't subscribe, the Pub/Sub connection is useless
                 * and we can simply disconnect it and try again. */
                instanceLinkCloseConnection(link, link->pc);
                return;
            }
        }
    }
    /* Clear the disconnected status only if we have both the connections
     * (or just the commands connection if this is a sentinel instance).
     * 清理标记 避免对该节点重复发起重连
     * 哨兵之间不需要建立订阅发布管道
     * */
    if (link->cc && (ri->flags & SRI_SENTINEL || link->pc))
        link->disconnected = 0;
}

/* ======================== Redis instances pinging  ======================== */

/* Return true if master looks "sane", that is:
 * 1) It is actually a master in the current configuration.
 * 2) It reports itself as a master.
 * 3) It is not SDOWN or ODOWN.
 * 4) We obtained last INFO no more than two times the INFO period time ago.
 * 本节点没有掉线 并且表示自身是master
 * */
int sentinelMasterLooksSane(sentinelRedisInstance *master) {
    return
            master->flags & SRI_MASTER &&
            master->role_reported == SRI_MASTER &&
            (master->flags & (SRI_S_DOWN | SRI_O_DOWN)) == 0 &&
            (mstime() - master->info_refresh) < SENTINEL_INFO_PERIOD * 2;
}

/* Process the INFO output from masters.
 * 哨兵会定期向其他master/slave节点发送info请求 这里是解析数据流并更新实例信息
 * */
void sentinelRefreshInstanceInfo(sentinelRedisInstance *ri, const char *info) {
    sds *lines;
    int numlines, j;
    int role = 0;

    /* cache full INFO output for instance
     * 清理之前的描述信息
     * */
    sdsfree(ri->info);
    ri->info = sdsnew(info);

    /* The following fields must be reset to a given value in the case they
     * are not found at all in the INFO output.
     * 在处理info信息前 需要重置下线时间
     * */
    ri->master_link_down_time = 0;

    /* Process line by line.
     * 解析本次返回的信息总计有多少行
     * */
    lines = sdssplitlen(info, strlen(info), "\r\n", 2, &numlines);
    for (j = 0; j < numlines; j++) {
        sentinelRedisInstance *slave;
        sds l = lines[j];

        /* run_id:<40 hex chars>
         * 如果runid没有设置的情况 先进行填充
         * */
        if (sdslen(l) >= 47 && !memcmp(l, "run_id:", 7)) {
            if (ri->runid == NULL) {
                ri->runid = sdsnewlen(l + 7, 40);
            } else {
                // 如果runid发生了变化 代表节点重启了
                if (strncmp(ri->runid, l + 7, 40) != 0) {
                    sentinelEvent(LL_NOTICE, "+reboot", ri, "%@");
                    sdsfree(ri->runid);
                    ri->runid = sdsnewlen(l + 7, 40);
                }
            }
        }

        /* old versions: slave0:<ip>,<port>,<state>
         * new versions: slave0:ip=127.0.0.1,port=9999,...
         * 作为master节点 在发送info信息时 会将此时拥有的所有slave信息返回
         * */
        if ((ri->flags & SRI_MASTER) &&
            sdslen(l) >= 7 &&
            !memcmp(l, "slave", 5) && isdigit(l[5])) {
            char *ip, *port, *end;

            // 基于新旧2种不同的格式解析ip和port信息
            if (strstr(l, "ip=") == NULL) {
                /* Old format. */
                ip = strchr(l, ':');
                if (!ip) continue;
                ip++; /* Now ip points to start of ip address. */
                port = strchr(ip, ',');
                if (!port) continue;
                *port = '\0'; /* nul term for easy access. */
                port++; /* Now port points to start of port number. */
                end = strchr(port, ',');
                if (!end) continue;
                *end = '\0'; /* nul term for easy access. */
            } else {
                /* New format. */
                ip = strstr(l, "ip=");
                if (!ip) continue;
                ip += 3; /* Now ip points to start of ip address. */
                port = strstr(l, "port=");
                if (!port) continue;
                port += 5; /* Now port points to start of port number. */
                /* Nul term both fields for easy access. */

                // 在ip 和 port之间插入'\0' 这样之后就可以直接调用api了
                end = strchr(ip, ',');
                if (end) *end = '\0';
                end = strchr(port, ',');
                if (end) *end = '\0';
            }

            /* Check if we already have this slave into our table,
             * otherwise add it.
             * 如果此时该slave还没有设置到这个master下 就会新建实例并建立映射关系，而在哨兵的定时任务中会与该slave进行连接(普通连接以及针对订阅发布的特殊连接)
             * */
            if (sentinelRedisInstanceLookupSlave(ri, ip, atoi(port)) == NULL) {
                if ((slave = createSentinelRedisInstance(NULL, SRI_SLAVE, ip,
                                                         atoi(port), ri->quorum, ri)) != NULL) {
                    sentinelEvent(LL_NOTICE, "+slave", slave, "%@");
                    sentinelFlushConfig();
                }
            }
        }

        /* master_link_down_since_seconds:<seconds>
         * 当slave感应到与master断开连接时 会更新这个属性
         * */
        if (sdslen(l) >= 32 &&
            !memcmp(l, "master_link_down_since_seconds", 30)) {
            ri->master_link_down_time = strtoll(l + 31, NULL, 10) * 1000;
        }

        /* role:<role> 根据role信息 判断该节点此时是master还是slave */
        if (sdslen(l) >= 11 && !memcmp(l, "role:master", 11)) role = SRI_MASTER;
        else if (sdslen(l) >= 10 && !memcmp(l, "role:slave", 10)) role = SRI_SLAVE;

        // 如果该节点此时已经变成了slave节点 解析相关的字段
        if (role == SRI_SLAVE) {
            /* master_host:<host> */
            if (sdslen(l) >= 12 && !memcmp(l, "master_host:", 12)) {
                if (ri->slave_master_host == NULL ||
                    // 如果master的端口发生了变化 进行数据同步
                    strcasecmp(l + 12, ri->slave_master_host)) {
                    sdsfree(ri->slave_master_host);
                    ri->slave_master_host = sdsnew(l + 12);
                    ri->slave_conf_change_time = mstime();
                }
            }

            /* master_port:<port>
             * 更新master port 信息
             * */
            if (sdslen(l) >= 12 && !memcmp(l, "master_port:", 12)) {
                int slave_master_port = atoi(l + 12);

                if (ri->slave_master_port != slave_master_port) {
                    ri->slave_master_port = slave_master_port;
                    ri->slave_conf_change_time = mstime();
                }
            }

            /* master_link_status:<status>
             * 作为slave此时是否已经连接上master (包含数据同步，只有当数据同步完成后才认为连接真正完成)
             * */
            if (sdslen(l) >= 19 && !memcmp(l, "master_link_status:", 19)) {
                ri->slave_master_link_status =
                        (strcasecmp(l + 19, "up") == 0) ?
                        SENTINEL_MASTER_LINK_STATUS_UP :
                        SENTINEL_MASTER_LINK_STATUS_DOWN;
            }

            /* slave_priority:<priority>
             * 更新本节点作为master节点的优先级
             * */
            if (sdslen(l) >= 15 && !memcmp(l, "slave_priority:", 15))
                ri->slave_priority = atoi(l + 15);

            /* slave_repl_offset:<offset>
             * 更新本节点与master节点数据同步的偏移量
             * */
            if (sdslen(l) >= 18 && !memcmp(l, "slave_repl_offset:", 18))
                ri->slave_repl_offset = strtoull(l + 18, NULL, 10);
        }
    }

    // 更新获取info的时间戳
    ri->info_refresh = mstime();
    sdsfreesplitres(lines, numlines);

    /* ---------------------------- Acting half -----------------------------
     * Some things will not happen if sentinel.tilt is true, but some will
     * still be processed. */

    /* Remember when the role changed.
     * 如果发现角色发生了变化
     * */
    if (role != ri->role_reported) {
        ri->role_reported_time = mstime();
        ri->role_reported = role;
        if (role == SRI_SLAVE) ri->slave_conf_change_time = mstime();
        /* Log the event with +role-change if the new role is coherent or
         * with -role-change if there is a mismatch with the current config.
         * 节点角色的变化会发出一个事件
         * */
        sentinelEvent(LL_VERBOSE,
                      ((ri->flags & (SRI_MASTER | SRI_SLAVE)) == role) ?
                      "+role-change" : "-role-change",
                      ri, "%@ new reported role is %s",
                      role == SRI_MASTER ? "master" : "slave",
                      ri->flags & SRI_MASTER ? "master" : "slave");
    }

    /* None of the following conditions are processed when in tilt mode, so
     * return asap.
     * 以下处理必须在tilt未开启时执行  在sentinel定时逻辑中 每隔一段时间就会检测是否需要进入tilt阶段
     * */
    if (sentinel.tilt) return;

    /* Handle master -> slave role switch. */
    if ((ri->flags & SRI_MASTER) && role == SRI_SLAVE) {
        /* Nothing to do, but masters claiming to be slaves are
         * considered to be unreachable by Sentinel, so eventually
         * a failover will be triggered. */
    }

    /* Handle slave -> master role switch.
     * 当本节点从slave晋升成master后 执行下面的逻辑
     * */
    if ((ri->flags & SRI_SLAVE) && role == SRI_MASTER) {
        /* If this is a promoted slave we can change state to the
         * failover state machine.
         * 需要满足3个条件才能进入下面的分支
         * 1.哨兵知道这个实例在选举中获胜
         * 2.此时master节点的选举流程还没有结束
         * 3.此时master节点正在等待slave晋升
         * */
        if ((ri->flags & SRI_PROMOTED) &&
            (ri->master->flags & SRI_FAILOVER_IN_PROGRESS) &&
            // 并且选举状态处于等待slave自己晋升
            (ri->master->failover_state ==
             SENTINEL_FAILOVER_STATE_WAIT_PROMOTION)) {
            /* Now that we are sure the slave was reconfigured as a master
             * set the master configuration epoch to the epoch we won the
             * election to perform this failover. This will force the other
             * Sentinels to update their config (assuming there is not
             * a newer one already available).
             * 此时选举已经结束 更新epoch
             * */
            ri->master->config_epoch = ri->master->failover_epoch;
            ri->master->failover_state = SENTINEL_FAILOVER_STATE_RECONF_SLAVES;
            ri->master->failover_state_change_time = mstime();
            sentinelFlushConfig();

            // 代表该slave已经成功晋升
            sentinelEvent(LL_WARNING, "+promoted-slave", ri, "%@");

            // 默认为0 先忽略
            if (sentinel.simfailure_flags &
                SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION)
                sentinelSimFailureCrash();

            // 发出一个事件 代表master已经降级成slave
            sentinelEvent(LL_WARNING, "+failover-state-reconf-slaves",
                          ri->master, "%@");
            // 执行角色变更的脚本  TODO 有关脚本的逻辑先忽略
            sentinelCallClientReconfScript(ri->master, SENTINEL_LEADER,
                                           "start", ri->master->addr, ri->addr);

            // 更新该master相关的其他节点的last_pub_time 这样在定时任务中会强制同步信息
            sentinelForceHelloUpdateForMaster(ri->master);
        } else {
            /* A slave turned into a master. We want to force our view and
             * reconfigure as slave. Wait some time after the change before
             * going forward, to receive new configs if any.
             * */
            mstime_t wait_time = SENTINEL_PUBLISH_PERIOD * 4;

            /**
             * 1.本轮该slave并没有选举成功
             * 2.master信息还有效，并且也没有角色更改
             * 3.该实例并没有掉线
             * 4.很长时间没有发生角色变更
             */
            if (!(ri->flags & SRI_PROMOTED) &&
                sentinelMasterLooksSane(ri->master) &&
                sentinelRedisInstanceNoDownFor(ri, wait_time) &&
                mstime() - ri->role_reported_time > wait_time) {

                // 将slave重新挂在master下 会涉及到replication模块
                int retval = sentinelSendSlaveOf(ri,
                                                 ri->master->addr->ip,
                                                 ri->master->addr->port);
                if (retval == C_OK)
                    // 发送一个某节点变成slave的事件
                    sentinelEvent(LL_NOTICE, "+convert-to-slave", ri, "%@");
            }
        }
    }

    /* Handle slaves replicating to a different master address.
     * 如果发现slave节点的master地址发生了变化
     * */
    if ((ri->flags & SRI_SLAVE) &&
        role == SRI_SLAVE &&
        (ri->slave_master_port != ri->master->addr->port ||
         strcasecmp(ri->slave_master_host, ri->master->addr->ip))) {
        mstime_t wait_time = ri->master->failover_timeout;

        /* Make sure the master is sane before reconfiguring this instance
         * into a slave.
         * 因为原本的master还有效 所以将地址改回去
         * */
        if (sentinelMasterLooksSane(ri->master) &&
            sentinelRedisInstanceNoDownFor(ri, wait_time) &&
            mstime() - ri->slave_conf_change_time > wait_time) {
            int retval = sentinelSendSlaveOf(ri,
                                             ri->master->addr->ip,
                                             ri->master->addr->port);
            if (retval == C_OK)
                sentinelEvent(LL_NOTICE, "+fix-slave-config", ri, "%@");
        }
    }

    /* Detect if the slave that is in the process of being reconfigured
     * changed state.
     * 如果当前slave 处于SRI_RECONF_SENT或者SRI_RECONF_INPROG状态 要进行处理
     * */
    if ((ri->flags & SRI_SLAVE) && role == SRI_SLAVE &&
        (ri->flags & (SRI_RECONF_SENT | SRI_RECONF_INPROG))) {
        /* SRI_RECONF_SENT -> SRI_RECONF_INPROG. */
        if ((ri->flags & SRI_RECONF_SENT) &&
            // 此时该slave已经认可了 本轮选举成功的节点 可以将状态修改成SRI_RECONF_INPROG
            ri->slave_master_host &&
            strcmp(ri->slave_master_host,
                   ri->master->promoted_slave->addr->ip) == 0 &&
            ri->slave_master_port == ri->master->promoted_slave->addr->port) {
            ri->flags &= ~SRI_RECONF_SENT;
            ri->flags |= SRI_RECONF_INPROG;
            sentinelEvent(LL_NOTICE, "+slave-reconf-inprog", ri, "%@");
        }

        /* SRI_RECONF_INPROG -> SRI_RECONF_DONE
         * 在本slave实例已经被标记成inprog后 再收到info信息时 检测slave节点是否已经连接上master了 连接上后修改成reconf_done
         * 从slave修改master地址 到与master建立连接有时间差 只有当连接建立完成后才认为reconf完成
         */
        if ((ri->flags & SRI_RECONF_INPROG) &&
            ri->slave_master_link_status == SENTINEL_MASTER_LINK_STATUS_UP) {
            ri->flags &= ~SRI_RECONF_INPROG;
            ri->flags |= SRI_RECONF_DONE;
            sentinelEvent(LL_NOTICE, "+slave-reconf-done", ri, "%@");
        }
    }
}

/**
 * 哨兵会周期性的向集群中的其他master/slave节点发送info请求 用来获取他们的信息
 * @param c
 * @param reply
 * @param privdata
 */
void sentinelInfoReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    // 根据返回的实例信息 刷新对应实例的属性
    if (r->type == REDIS_REPLY_STRING)
        sentinelRefreshInstanceInfo(ri, r->str);
}

/* Just discard the reply. We use this when we are not monitoring the return
 * value of the command but its effects directly.
 * 通过某个link接收到的信息会使用这些回调对象进行处理 而该对象对象代表忽略本次接收到的数据 同时减少待接收command数量
 * */
void sentinelDiscardReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    instanceLink *link = c->data;
    UNUSED(reply);
    UNUSED(privdata);

    if (link) link->pending_commands--;
}

/**
 * 当收到心跳请求结果后 触发该对象函数
 * @param c
 * @param reply
 * @param privdata
 */
void sentinelPingReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {

    // 每个回调对象绑定的就是设置该回调的实例
    sentinelRedisInstance *ri = privdata;

    // 多个实例可能会共享同一个link
    instanceLink *link = c->data;
    redisReply *r;

    // 在hiredis模块 返回的字符串在触发回调前会被包装成 redisReply对象
    // 如果没有返回数据 或者link本身被释放了 都不需要处理
    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    if (r->type == REDIS_REPLY_STATUS ||
        r->type == REDIS_REPLY_ERROR) {
        /* Update the "instance available" field only if this is an
         * acceptable reply.
         * 此时本节点都处于可用状态
         * */
        if (strncmp(r->str, "PONG", 4) == 0 ||
            strncmp(r->str, "LOADING", 7) == 0 ||
            strncmp(r->str, "MASTERDOWN", 10) == 0) {
            link->last_avail_time = mstime();
            link->act_ping_time = 0; /* Flag the pong as received. */
        } else {
            /* Send a SCRIPT KILL command if the instance appears to be
             * down because of a busy script.
             * 如果此时对端正在执行一条耗时长的lua脚本 这里会发出一个关闭脚本的请求 但是不会断开连接
             * */
            if (strncmp(r->str, "BUSY", 4) == 0 &&
                (ri->flags & SRI_S_DOWN) &&
                !(ri->flags & SRI_SCRIPT_KILL_SENT)) {
                if (redisAsyncCommand(ri->link->cc,
                                      sentinelDiscardReplyCallback, ri,
                                      "%s KILL",
                                      sentinelInstanceMapCommand(ri, "SCRIPT")) == C_OK) {
                    ri->link->pending_commands++;
                }
                ri->flags |= SRI_SCRIPT_KILL_SENT;
            }
        }
    }

    // 更新从该link最后一次收到心跳包的时间
    link->last_pong_time = mstime();
}

/* This is called when we get the reply about the PUBLISH command we send
 * to the master to advertise this sentinel.
 * 当发出publish到其他节点后 使用该函数处理收到的回复信息
 * */
void sentinelPublishReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    /* Only update pub_time if we actually published our message. Otherwise
     * we'll retry again in 100 milliseconds.
     * 主要就是更新 last_pub_time
     * */
    if (r->type != REDIS_REPLY_ERROR)
        ri->last_pub_time = mstime();
}

/* Process a hello message received via Pub/Sub in master or slave instance,
 * or sent directly to this sentinel via the (fake) PUBLISH command of Sentinel.
 *
 * If the master name specified in the message is not known, the message is
 * discarded.
 * 处理某个哨兵发出的hello信息
 * 该方法只能由哨兵实例触发
 * */
void sentinelProcessHelloMessage(char *hello, int hello_len) {
    /* Format is composed of 8 tokens:
     * 0=ip,1=port,2=runid,3=current_epoch,4=master_name,
     * 5=master_ip,6=master_port,7=master_config_epoch.
     * 哨兵发出的hellow消息由8个部分组成
     * */
    int numtokens, port, removed, master_port;
    uint64_t current_epoch, master_config_epoch;
    // 将消息拆分成多个片段
    char **token = sdssplitlen(hello, hello_len, ",", 1, &numtokens);
    sentinelRedisInstance *si, *master;

    // 将各个片段填充到对应的位置
    if (numtokens == 8) {
        /* Obtain a reference to the master this hello message is about
         * 判断在本哨兵上是否监控了这个master 如果本哨兵没有监控这个master就不需要处理这个消息了
         * */
        master = sentinelGetMasterByName(token[4]);
        if (!master) goto cleanup; /* Unknown master, skip the message. */

        /* First, try to see if we already have this sentinel.
         * */
        port = atoi(token[1]);
        master_port = atoi(token[6]);
        // 因为这个哨兵也是监控master的 要将这个关联关系同步到本节点
        si = getSentinelRedisInstanceByAddrAndRunID(
                master->sentinels, token[0], port, token[2]);
        // 获取该哨兵的任期
        current_epoch = strtoull(token[3], NULL, 10);
        master_config_epoch = strtoull(token[7], NULL, 10);

        // 添加映射关系
        if (!si) {
            /* If not, remove all the sentinels that have the same runid
             * because there was an address change, and add the same Sentinel
             * with the new address back.
             * 如果runid能够匹配到就代表是哨兵的地址发生了变化
             * */
            removed = removeMatchingSentinelFromMaster(master, token[2]);
            // 发出相关事件
            if (removed) {
                sentinelEvent(LL_NOTICE, "+sentinel-address-switch", master,
                              "%@ ip %s port %d for %s", token[0], port, token[2]);
            } else {
                /* Check if there is another Sentinel with the same address this
                 * new one is reporting. What we do if this happens is to set its
                 * port to 0, to signal the address is invalid. We'll update it
                 * later if we get an HELLO message.
                 * 如果这个地址指向了另一个实例 这是异常情况
                 * */
                sentinelRedisInstance *other =
                        getSentinelRedisInstanceByAddrAndRunID(
                                master->sentinels, token[0], port, NULL);
                if (other) {
                    sentinelEvent(LL_NOTICE, "+sentinel-invalid-addr", other, "%@");
                    // 将另一个哨兵的地址重置 此时认为这个哨兵是不合法的
                    other->addr->port = 0; /* It means: invalid address. */
                    // 与旧的哨兵断开连接 更新地址并在之后重连
                    sentinelUpdateSentinelAddressInAllMasters(other);
                }
            }

            /* Add the new sentinel.
             * 因为感应到了新的哨兵 需要创建对应的实例 增加映射关系
             * */
            si = createSentinelRedisInstance(token[2], SRI_SENTINEL,
                                             token[0], port, master->quorum, master);

            if (si) {
                // 发出一个添加了sentinel的事件
                if (!removed) sentinelEvent(LL_NOTICE, "+sentinel", si, "%@");
                /* The runid is NULL after a new instance creation and
                 * for Sentinels we don't have a later chance to fill it,
                 * so do it now.
                 * 设置runid
                 * */
                si->runid = sdsnew(token[2]);
                // 该哨兵可能同时监控多个master 每个关联关系都会创建一个对应的实例 这些实例需要共享连接
                sentinelTryConnectionSharing(si);
                // 更新原master下有关该sentinel的地址信息
                if (removed) sentinelUpdateSentinelAddressInAllMasters(si);
                sentinelFlushConfig();
            }
        }

        // 此时该节点下也已经维护了 此master与sentinel的关联关系  如果发现对端的epoch更新 更新配置文件 同时发出事件
        /* Update local current_epoch if received current_epoch is greater.*/
        if (current_epoch > sentinel.current_epoch) {
            sentinel.current_epoch = current_epoch;
            sentinelFlushConfig();
            sentinelEvent(LL_WARNING, "+new-epoch", master, "%llu",
                          (unsigned long long) sentinel.current_epoch);
        }

        /* Update master info if received configuration is newer.
         * si之前存在并且此时config_epoch落后
         * */
        if (si && master->config_epoch < master_config_epoch) {
            master->config_epoch = master_config_epoch;

            // 更新master的地址信息
            if (master_port != master->addr->port ||
                strcmp(master->addr->ip, token[5])) {
                sentinelAddr *old_addr;
                // 发出更新master 以及 更新config的事件
                sentinelEvent(LL_WARNING, "+config-update-from", si, "%@");
                sentinelEvent(LL_WARNING, "+switch-master",
                              master, "%s %s %d %s %d",
                              master->name,
                              master->addr->ip, master->addr->port,
                              token[5], master_port);

                old_addr = dupSentinelAddr(master->addr);
                // 更新master的地址信息  也会间接更新slave的地址信息
                sentinelResetMasterAndChangeAddress(master, token[5], master_port);
                // 执行某些脚本
                sentinelCallClientReconfScript(master,
                                               SENTINEL_OBSERVER, "start",
                                               old_addr, master->addr);
                releaseSentinelAddr(old_addr);
            }
        }

        /* Update the state of the Sentinel. */
        if (si) si->last_hello_time = mstime();
    }

    cleanup:
    sdsfreesplitres(token, numtokens);
}


/* This is our Pub/Sub callback for the Hello channel. It's useful in order
 * to discover other sentinels attached at the same master.
 * */
void sentinelReceiveHelloMessages(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    redisReply *r;
    UNUSED(c);

    if (!reply || !ri) return;
    r = reply;

    /* Update the last activity in the pubsub channel. Note that since we
     * receive our messages as well this timestamp can be used to detect
     * if the link is probably disconnected even if it seems otherwise.
     * 更新订阅发布连接最新的使用时间
     * */
    ri->link->pc_last_activity = mstime();

    /* Sanity check in the reply we expect, so that the code that follows
     * can avoid to check for details.
     * 收到的结果总计是3个 第一个是message 代表收到的是订阅消息 第二个是channel 代表是因为订阅了哪个channel收到的消息 第三个是发送的消息体
     * */
    if (r->type != REDIS_REPLY_ARRAY ||
        r->elements != 3 ||
        r->element[0]->type != REDIS_REPLY_STRING ||
        r->element[1]->type != REDIS_REPLY_STRING ||
        r->element[2]->type != REDIS_REPLY_STRING ||
        strcmp(r->element[0]->str, "message") != 0)
        return;

    /* We are not interested in meeting ourselves
     * 实际上就是每个哨兵会定期将自身的信息通过订阅发布的方式推送到其他节点上   如果本节点收到了自身发出的信息 不需要处理
     * */
    if (strstr(r->element[2]->str, sentinel.myid) != NULL) return;

    sentinelProcessHelloMessage(r->element[2]->str, r->element[2]->len);
}

/* Send a "Hello" message via Pub/Sub to the specified 'ri' Redis
 * instance in order to broadcast the current configuration for this
 * master, and to advertise the existence of this Sentinel at the same time.
 *
 * The message has the following format:
 *
 * sentinel_ip,sentinel_port,sentinel_runid,current_epoch,
 * master_name,master_ip,master_port,master_config_epoch.
 *
 * Returns C_OK if the PUBLISH was queued correctly, otherwise
 * C_ERR is returned.
 * 哨兵会定期将自身信息同步到集群的其他实例上
 * */
int sentinelSendHello(sentinelRedisInstance *ri) {
    char ip[NET_IP_STR_LEN];
    char payload[NET_IP_STR_LEN + 1024];
    int retval;
    char *announce_ip;
    int announce_port;
    // 获取该节点的master
    sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ? ri : ri->master;
    // 返回master地址
    sentinelAddr *master_addr = sentinelGetCurrentMasterAddress(master);

    // 如果与该节点的连接已经断开 就等待之后的重连
    if (ri->link->disconnected) return C_ERR;

    /* Use the specified announce address if specified, otherwise try to
     * obtain our own IP address. */
    if (sentinel.announce_ip) {
        announce_ip = sentinel.announce_ip;
    } else {
        // 读取ri的ip地址
        if (anetSockName(ri->link->cc->c.fd, ip, sizeof(ip), NULL) == -1)
            return C_ERR;
        announce_ip = ip;
    }
    // 如果设置了宣告的端口 就使用这个端口
    if (sentinel.announce_port) announce_port = sentinel.announce_port;
        // 忽略ssl层
    else if (server.tls_replication && server.tls_port) announce_port = server.tls_port;
        // 否则使用普通端口
    else announce_port = server.port;

    /* Format and send the Hello message.
     * 将本哨兵用于通信的端口ip暴露给其他实例
     * */
    snprintf(payload, sizeof(payload),
             "%s,%d,%s,%llu," /* Info about this sentinel. */
             "%s,%s,%d,%llu", /* Info about current master. */
             announce_ip, announce_port, sentinel.myid,
             (unsigned long long) sentinel.current_epoch,
    /* --- */
             master->name, master_addr->ip, master_addr->port,
             (unsigned long long) master->config_epoch);

    // 将本节点暴露出来的端口 通过hello管道发送出去
    retval = redisAsyncCommand(ri->link->cc,
                               // 该函数主要是更新最近一次发布时间 避免短时间内重复发布
                               sentinelPublishReplyCallback, ri, "%s %s %s",
                               sentinelInstanceMapCommand(ri, "PUBLISH"),
                               SENTINEL_HELLO_CHANNEL, payload);
    if (retval != C_OK) return C_ERR;
    ri->link->pending_commands++;
    return C_OK;
}

/* Reset last_pub_time in all the instances in the specified dictionary
 * in order to force the delivery of a Hello update ASAP.
 * 挨个更新每个实例的pub时间
 * */
void sentinelForceHelloUpdateDictOfRedisInstances(dict *instances) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetSafeIterator(instances);
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        if (ri->last_pub_time >= (SENTINEL_PUBLISH_PERIOD + 1))
            ri->last_pub_time -= (SENTINEL_PUBLISH_PERIOD + 1);
    }
    dictReleaseIterator(di);
}

/* This function forces the delivery of a "Hello" message (see
 * sentinelSendHello() top comment for further information) to all the Redis
 * and Sentinel instances related to the specified 'master'.
 *
 * It is technically not needed since we send an update to every instance
 * with a period of SENTINEL_PUBLISH_PERIOD milliseconds, however when a
 * Sentinel upgrades a configuration it is a good idea to deliver an update
 * to the other Sentinels ASAP.
 * 由于master节点发生了变化  所以要将最新的信息通知到集群的其他节点 这里只是更新last_pub_time 这样在定时任务中就会强制发送hello请求
 * */
int sentinelForceHelloUpdateForMaster(sentinelRedisInstance *master) {
    if (!(master->flags & SRI_MASTER)) return C_ERR;
    if (master->last_pub_time >= (SENTINEL_PUBLISH_PERIOD + 1))
        master->last_pub_time -= (SENTINEL_PUBLISH_PERIOD + 1);
    sentinelForceHelloUpdateDictOfRedisInstances(master->sentinels);
    sentinelForceHelloUpdateDictOfRedisInstances(master->slaves);
    return C_OK;
}

/* Send a PING to the specified instance and refresh the act_ping_time
 * if it is zero (that is, if we received a pong for the previous ping).
 *
 * On error zero is returned, and we can't consider the PING command
 * queued in the connection.
 * 心跳请求本身不发送任何数据
 * */
int sentinelSendPing(sentinelRedisInstance *ri) {
    int retval = redisAsyncCommand(ri->link->cc,
                                   // 该回调函数主要是更新最近的通信时间
                                   sentinelPingReplyCallback, ri, "%s",
                                   sentinelInstanceMapCommand(ri, "PING"));
    // 本次数据写入成功 增加 pending_commands 并且更新最近一次ping的时间
    if (retval == C_OK) {
        ri->link->pending_commands++;
        ri->link->last_ping_time = mstime();
        /* We update the active ping time only if we received the pong for
         * the previous ping, otherwise we are technically waiting since the
         * first ping that did not receive a reply. */
        if (ri->link->act_ping_time == 0)
            ri->link->act_ping_time = ri->link->last_ping_time;
        return 1;
    } else {
        return 0;
    }
}

/* Send periodic PING, INFO, and PUBLISH to the Hello channel to
 * the specified master or slave instance.
 * 针对该实例执行一些周期性的任务
 * */
void sentinelSendPeriodicCommands(sentinelRedisInstance *ri) {
    mstime_t now = mstime();
    mstime_t info_period, ping_period;
    int retval;

    /* Return ASAP if we have already a PING or INFO already pending, or
     * in the case the instance is not properly connected.
     * 此时与本节点已经断开连接了 不需要处理
     * */
    if (ri->link->disconnected) return;

    /* For INFO, PING, PUBLISH that are not critical commands to send we
     * also have a limit of SENTINEL_MAX_PENDING_COMMANDS. We don't
     * want to use a lot of memory just because a link is not working
     * properly (note that anyway there is a redundant protection about this,
     * that is, the link will be disconnected and reconnected if a long
     * timeout condition is detected.
     * 此时囤积的任务过多 可能是对端来不及处理 也可能是连接还未完成 总之不应该继续增大对端的负载
     * */
    if (ri->link->pending_commands >=
        SENTINEL_MAX_PENDING_COMMANDS * ri->link->refcount)
        return;

    /* If this is a slave of a master in O_DOWN condition we start sending
     * it INFO every second, instead of the usual SENTINEL_INFO_PERIOD
     * period. In this state we want to closely monitor slaves in case they
     * are turned into masters by another Sentinel, or by the sysadmin.
     *
     * Similarly we monitor the INFO output more often if the slave reports
     * to be disconnected from the master, so that we can have a fresh
     * disconnection time figure.
     * 本节点作为slave节点 正在准备晋升
     * */
    if ((ri->flags & SRI_SLAVE) &&
        ((ri->master->flags & (SRI_O_DOWN | SRI_FAILOVER_IN_PROGRESS)) ||
         (ri->master_link_down_time != 0))) {
        info_period = 1000;
    } else {
        // 这时没有开始选举 有一个默认的时间值
        info_period = SENTINEL_INFO_PERIOD;
    }

    /* We ping instances every time the last received pong is older than
     * the configured 'down-after-milliseconds' time, but every second
     * anyway if 'down-after-milliseconds' is greater than 1 second.
     * 计算调用info获取节点最新信息的时间间隔 同时该值不应该小于down_after_period
     * */
    ping_period = ri->down_after_period;
    if (ping_period > SENTINEL_PING_PERIOD) ping_period = SENTINEL_PING_PERIOD;

    /* Send INFO to masters and slaves, not sentinels.
     * 每隔一定时间定期获取master/slave节点的信息 通过它们的信息判断某个节点是否发生了角色的变化 以及通过某个slave现在的状态判断是否完成了选举
     * */
    if ((ri->flags & SRI_SENTINEL) == 0 &&
        (ri->info_refresh == 0 ||
         (now - ri->info_refresh) > info_period)) {
        retval = redisAsyncCommand(ri->link->cc,
                                   sentinelInfoReplyCallback, ri, "%s",
                                   sentinelInstanceMapCommand(ri, "INFO"));
        if (retval == C_OK) ri->link->pending_commands++;
    }

    /* Send PING to all the three kinds of instances.
     * 心跳请求会发往所有实例 而info请求只会发往master和slave
     * */
    if ((now - ri->link->last_pong_time) > ping_period &&
        (now - ri->link->last_ping_time) > ping_period / 2) {
        sentinelSendPing(ri);
    }

    /* PUBLISH hello messages to all the three kinds of instances.
     * 与info请求相反，是将本哨兵的信息通知到其他slave/master/sentinel上
     * 当某个slave晋升成功后 sentinel会负责将这个信息同步到其他所有实例上
     * */
    if ((now - ri->last_pub_time) > SENTINEL_PUBLISH_PERIOD) {
        sentinelSendHello(ri);
    }
}

/* =========================== SENTINEL command ============================= */

const char *sentinelFailoverStateStr(int state) {
    switch (state) {
        case SENTINEL_FAILOVER_STATE_NONE:
            return "none";
        case SENTINEL_FAILOVER_STATE_WAIT_START:
            return "wait_start";
        case SENTINEL_FAILOVER_STATE_SELECT_SLAVE:
            return "select_slave";
        case SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE:
            return "send_slaveof_noone";
        case SENTINEL_FAILOVER_STATE_WAIT_PROMOTION:
            return "wait_promotion";
        case SENTINEL_FAILOVER_STATE_RECONF_SLAVES:
            return "reconf_slaves";
        case SENTINEL_FAILOVER_STATE_UPDATE_CONFIG:
            return "update_config";
        default:
            return "unknown";
    }
}

/* Redis instance to Redis protocol representation.
 * 将实例信息写入到client的缓冲区中
 * */
void addReplySentinelRedisInstance(client *c, sentinelRedisInstance *ri) {
    char *flags = sdsempty();
    void *mbl;
    int fields = 0;

    mbl = addReplyDeferredLen(c);

    addReplyBulkCString(c, "name");
    addReplyBulkCString(c, ri->name);
    fields++;

    addReplyBulkCString(c, "ip");
    addReplyBulkCString(c, ri->addr->ip);
    fields++;

    addReplyBulkCString(c, "port");
    addReplyBulkLongLong(c, ri->addr->port);
    fields++;

    addReplyBulkCString(c, "runid");
    addReplyBulkCString(c, ri->runid ? ri->runid : "");
    fields++;

    addReplyBulkCString(c, "flags");
    if (ri->flags & SRI_S_DOWN) flags = sdscat(flags, "s_down,");
    if (ri->flags & SRI_O_DOWN) flags = sdscat(flags, "o_down,");
    if (ri->flags & SRI_MASTER) flags = sdscat(flags, "master,");
    if (ri->flags & SRI_SLAVE) flags = sdscat(flags, "slave,");
    if (ri->flags & SRI_SENTINEL) flags = sdscat(flags, "sentinel,");
    if (ri->link->disconnected) flags = sdscat(flags, "disconnected,");
    if (ri->flags & SRI_MASTER_DOWN) flags = sdscat(flags, "master_down,");
    if (ri->flags & SRI_FAILOVER_IN_PROGRESS)
        flags = sdscat(flags, "failover_in_progress,");
    if (ri->flags & SRI_PROMOTED) flags = sdscat(flags, "promoted,");
    if (ri->flags & SRI_RECONF_SENT) flags = sdscat(flags, "reconf_sent,");
    if (ri->flags & SRI_RECONF_INPROG) flags = sdscat(flags, "reconf_inprog,");
    if (ri->flags & SRI_RECONF_DONE) flags = sdscat(flags, "reconf_done,");

    if (sdslen(flags) != 0) sdsrange(flags, 0, -2); /* remove last "," */
    addReplyBulkCString(c, flags);
    sdsfree(flags);
    fields++;

    addReplyBulkCString(c, "link-pending-commands");
    addReplyBulkLongLong(c, ri->link->pending_commands);
    fields++;

    addReplyBulkCString(c, "link-refcount");
    addReplyBulkLongLong(c, ri->link->refcount);
    fields++;

    if (ri->flags & SRI_FAILOVER_IN_PROGRESS) {
        addReplyBulkCString(c, "failover-state");
        addReplyBulkCString(c, (char *) sentinelFailoverStateStr(ri->failover_state));
        fields++;
    }

    addReplyBulkCString(c, "last-ping-sent");
    addReplyBulkLongLong(c,
                         ri->link->act_ping_time ? (mstime() - ri->link->act_ping_time) : 0);
    fields++;

    addReplyBulkCString(c, "last-ok-ping-reply");
    addReplyBulkLongLong(c, mstime() - ri->link->last_avail_time);
    fields++;

    addReplyBulkCString(c, "last-ping-reply");
    addReplyBulkLongLong(c, mstime() - ri->link->last_pong_time);
    fields++;

    if (ri->flags & SRI_S_DOWN) {
        addReplyBulkCString(c, "s-down-time");
        addReplyBulkLongLong(c, mstime() - ri->s_down_since_time);
        fields++;
    }

    if (ri->flags & SRI_O_DOWN) {
        addReplyBulkCString(c, "o-down-time");
        addReplyBulkLongLong(c, mstime() - ri->o_down_since_time);
        fields++;
    }

    addReplyBulkCString(c, "down-after-milliseconds");
    addReplyBulkLongLong(c, ri->down_after_period);
    fields++;

    /* Masters and Slaves */
    if (ri->flags & (SRI_MASTER | SRI_SLAVE)) {
        addReplyBulkCString(c, "info-refresh");
        addReplyBulkLongLong(c, mstime() - ri->info_refresh);
        fields++;

        addReplyBulkCString(c, "role-reported");
        addReplyBulkCString(c, (ri->role_reported == SRI_MASTER) ? "master" :
                               "slave");
        fields++;

        addReplyBulkCString(c, "role-reported-time");
        addReplyBulkLongLong(c, mstime() - ri->role_reported_time);
        fields++;
    }

    /* Only masters */
    if (ri->flags & SRI_MASTER) {
        addReplyBulkCString(c, "config-epoch");
        addReplyBulkLongLong(c, ri->config_epoch);
        fields++;

        addReplyBulkCString(c, "num-slaves");
        addReplyBulkLongLong(c, dictSize(ri->slaves));
        fields++;

        addReplyBulkCString(c, "num-other-sentinels");
        addReplyBulkLongLong(c, dictSize(ri->sentinels));
        fields++;

        addReplyBulkCString(c, "quorum");
        addReplyBulkLongLong(c, ri->quorum);
        fields++;

        addReplyBulkCString(c, "failover-timeout");
        addReplyBulkLongLong(c, ri->failover_timeout);
        fields++;

        addReplyBulkCString(c, "parallel-syncs");
        addReplyBulkLongLong(c, ri->parallel_syncs);
        fields++;

        if (ri->notification_script) {
            addReplyBulkCString(c, "notification-script");
            addReplyBulkCString(c, ri->notification_script);
            fields++;
        }

        if (ri->client_reconfig_script) {
            addReplyBulkCString(c, "client-reconfig-script");
            addReplyBulkCString(c, ri->client_reconfig_script);
            fields++;
        }
    }

    /* Only slaves */
    if (ri->flags & SRI_SLAVE) {
        addReplyBulkCString(c, "master-link-down-time");
        addReplyBulkLongLong(c, ri->master_link_down_time);
        fields++;

        addReplyBulkCString(c, "master-link-status");
        addReplyBulkCString(c,
                            (ri->slave_master_link_status == SENTINEL_MASTER_LINK_STATUS_UP) ?
                            "ok" : "err");
        fields++;

        addReplyBulkCString(c, "master-host");
        addReplyBulkCString(c,
                            ri->slave_master_host ? ri->slave_master_host : "?");
        fields++;

        addReplyBulkCString(c, "master-port");
        addReplyBulkLongLong(c, ri->slave_master_port);
        fields++;

        addReplyBulkCString(c, "slave-priority");
        addReplyBulkLongLong(c, ri->slave_priority);
        fields++;

        addReplyBulkCString(c, "slave-repl-offset");
        addReplyBulkLongLong(c, ri->slave_repl_offset);
        fields++;
    }

    /* Only sentinels */
    if (ri->flags & SRI_SENTINEL) {
        addReplyBulkCString(c, "last-hello-message");
        addReplyBulkLongLong(c, mstime() - ri->last_hello_time);
        fields++;

        addReplyBulkCString(c, "voted-leader");
        addReplyBulkCString(c, ri->leader ? ri->leader : "?");
        fields++;

        addReplyBulkCString(c, "voted-leader-epoch");
        addReplyBulkLongLong(c, ri->leader_epoch);
        fields++;
    }

    setDeferredMapLen(c, mbl, fields);
}

/* Output a number of instances contained inside a dictionary as
 * Redis protocol.
 * 将一组实例信息写入到client的缓冲区中
 * @param instances
 * */
void addReplyDictOfRedisInstances(client *c, dict *instances) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(instances);
    addReplyArrayLen(c, dictSize(instances));
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        // 将单个实例信息写入到client缓冲区
        addReplySentinelRedisInstance(c, ri);
    }
    dictReleaseIterator(di);
}

/* Lookup the named master into sentinel.masters.
 * If the master is not found reply to the client with an error and returns
 * NULL. 通过name查找某个redis实例 */
sentinelRedisInstance *sentinelGetMasterByNameOrReplyError(client *c,
                                                           robj *name) {
    sentinelRedisInstance *ri;

    ri = dictFetchValue(sentinel.masters, name->ptr);
    if (!ri) {
        addReplyError(c, "No such master with that name");
        return NULL;
    }
    return ri;
}

#define SENTINEL_ISQR_OK 0
// 此时sentinel没有达到某个master的 quornm
#define SENTINEL_ISQR_NOQUORUM (1<<0)
// 此时sentinel没有超过sentinel总数的1/2
#define SENTINEL_ISQR_NOAUTH (1<<1)

/**
 * 根据此时可以正常工作的sentinel数量 返回不同的标记
 * @param master
 * @param usableptr
 * @return
 */
int sentinelIsQuorumReachable(sentinelRedisInstance *master, int *usableptr) {
    dictIterator *di;
    dictEntry *de;
    int usable = 1; /* Number of usable Sentinels. Init to 1 to count myself. */
    // 默认情况下认为quornm是足够的
    int result = SENTINEL_ISQR_OK;
    // master->sentinels 这个是从配置文件中加载出来的 并不包含自己  同时存在与masters中的所有节点就是本节点所监控的
    int voters = dictSize(master->sentinels) + 1; /* Known Sentinels + myself. */

    di = dictGetIterator(master->sentinels);
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        // 代表这些哨兵无法被本节点观测到 无法参与投票
        if (ri->flags & (SRI_S_DOWN | SRI_O_DOWN)) continue;
        usable++;
    }
    dictReleaseIterator(di);

    // quorum 是可以手动设置的 代表多少哨兵的认可 就作为master可用的依据
    if (usable < (int) master->quorum) result |= SENTINEL_ISQR_NOQUORUM;
    // 这里则是要求此时可工作的哨兵数必须超过哨兵总数的1/2
    if (usable < voters / 2 + 1) result |= SENTINEL_ISQR_NOAUTH;
    if (usableptr) *usableptr = usable;
    return result;
}

/**
 * 可以执行哨兵相关的任务
 * @param c
 */
void sentinelCommand(client *c) {

    // 返回此时本哨兵监控的所有master节点
    if (!strcasecmp(c->argv[1]->ptr, "masters")) {
        /* SENTINEL MASTERS */
        if (c->argc != 2) goto numargserr;
        addReplyDictOfRedisInstances(c, sentinel.masters);
        // 返回某个master此时的信息
    } else if (!strcasecmp(c->argv[1]->ptr, "master")) {
        /* SENTINEL MASTER <name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c, c->argv[2]))
            == NULL)
            return;
        addReplySentinelRedisInstance(c, ri);
        // 返回某个master下所有的副本信息
    } else if (!strcasecmp(c->argv[1]->ptr, "slaves") ||
               !strcasecmp(c->argv[1]->ptr, "replicas")) {
        /* SENTINEL REPLICAS <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        // 确保master存在
        if ((ri = sentinelGetMasterByNameOrReplyError(c, c->argv[2])) == NULL)
            return;
        addReplyDictOfRedisInstances(c, ri->slaves);
        // 返回某个master下所有哨兵节点
    } else if (!strcasecmp(c->argv[1]->ptr, "sentinels")) {
        /* SENTINEL SENTINELS <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c, c->argv[2])) == NULL)
            return;
        addReplyDictOfRedisInstances(c, ri->sentinels);

        // 通过传入一个地址信息找到匹配的master 并检测对应的节点是否下线
    } else if (!strcasecmp(c->argv[1]->ptr, "is-master-down-by-addr")) {
        /* SENTINEL IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>
         *
         * Arguments:
         *
         * ip and port are the ip and port of the master we want to be
         * checked by Sentinel. Note that the command will not check by
         * name but just by master, in theory different Sentinels may monitor
         * different masters with the same name.
         *
         * current-epoch is needed in order to understand if we are allowed
         * to vote for a failover leader or not. Each Sentinel can vote just
         * one time per epoch.
         *
         * runid is "*" if we are not seeking for a vote from the Sentinel
         * in order to elect the failover leader. Otherwise it is set to the
         * runid we want the Sentinel to vote if it did not already voted.
         */
        sentinelRedisInstance *ri;
        long long req_epoch;
        uint64_t leader_epoch = 0;
        char *leader = NULL;
        long port;
        int isdown = 0;

        if (c->argc != 6) goto numargserr;
        // 确保port和epoch参数存在
        if (getLongFromObjectOrReply(c, c->argv[3], &port, NULL) != C_OK ||
            getLongLongFromObjectOrReply(c, c->argv[4], &req_epoch, NULL)
            != C_OK)
            return;

        // 通过ip,port找到匹配的master
        ri = getSentinelRedisInstanceByAddrAndRunID(sentinel.masters,
                                                    c->argv[2]->ptr, port, NULL);

        /* It exists? Is actually a master? Is subjectively down? It's down.
         * Note: if we are in tilt mode we always reply with "0".
         * 此时没有处于tilt阶段 并且本节点打上了主观下线的标记 isdown设置成1
         * */
        if (!sentinel.tilt && ri && (ri->flags & SRI_S_DOWN) &&
            (ri->flags & SRI_MASTER))
            isdown = 1;

        /* Vote for the master (or fetch the previous vote) if the request
         * includes a runid, otherwise the sender is not seeking for a vote.
         * 如果本次请求携带一个runid (runid只有哨兵节点才会有)
         * 那么本master节点会在指定的epoch中为该sentinel投一票   多节点哨兵架构中，存在一个主哨兵，被称为leader
         * 如果没有传入runid 就直接返回此时认可的哨兵leader
         * */
        if (ri && ri->flags & SRI_MASTER && strcasecmp(c->argv[5]->ptr, "*")) {
            leader = sentinelVoteLeader(ri, (uint64_t) req_epoch,
                                        c->argv[5]->ptr,  // 传入runid 此时runid不是"*"
                                        &leader_epoch);
        }

        /* Reply with a three-elements multi-bulk reply:
         * down state, leader, vote epoch.
         * 此时可以将本节点是否下线的信息返回了
         * */
        addReplyArrayLen(c, 3);
        addReply(c, isdown ? shared.cone : shared.czero);
        addReplyBulkCString(c, leader ? leader : "*");
        addReplyLongLong(c, (long long) leader_epoch);
        if (leader) sdsfree(leader);

        // 将满足表达式的一组实例的信息重置  仅支持重置master节点
    } else if (!strcasecmp(c->argv[1]->ptr, "reset")) {
        /* SENTINEL RESET <pattern> */
        if (c->argc != 3) goto numargserr;
        // 符合正则条件的所有master 会被重置
        addReplyLongLong(c, sentinelResetMastersByPattern(c->argv[2]->ptr, SENTINEL_GENERATE_EVENT));

        // 通过name 定位到某个master后 返回它的addr
    } else if (!strcasecmp(c->argv[1]->ptr, "get-master-addr-by-name")) {
        /* SENTINEL GET-MASTER-ADDR-BY-NAME <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        ri = sentinelGetMasterByName(c->argv[2]->ptr);
        if (ri == NULL) {
            addReplyNullArray(c);
        } else {
            sentinelAddr *addr = sentinelGetCurrentMasterAddress(ri);

            addReplyArrayLen(c, 2);
            addReplyBulkCString(c, addr->ip);
            addReplyBulkLongLong(c, addr->port);
        }

        // 手动触发选举
    } else if (!strcasecmp(c->argv[1]->ptr, "failover")) {
        /* SENTINEL FAILOVER <master-name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        // 找到本次推举的master
        if ((ri = sentinelGetMasterByNameOrReplyError(c, c->argv[2])) == NULL)
            return;
        // 代表针对该master已经发起选举了 不能重复发起
        if (ri->flags & SRI_FAILOVER_IN_PROGRESS) {
            addReplySds(c, sdsnew("-INPROG Failover already in progress\r\n"));
            return;
        }

        // 这里找到一个合适的交接人
        if (sentinelSelectSlave(ri) == NULL) {
            addReplySds(c, sdsnew("-NOGOODSLAVE No suitable replica to promote\r\n"));
            return;
        }
        serverLog(LL_WARNING, "Executing user requested FAILOVER of '%s'",
                  ri->name);
        // 开始执行选举  这里在发布消息后就没有处理了 后续的异步操作入口在哪呢
        sentinelStartFailover(ri);
        ri->flags |= SRI_FORCE_FAILOVER;
        addReply(c, shared.ok);
        // 返回此时所有待执行的脚本
    } else if (!strcasecmp(c->argv[1]->ptr, "pending-scripts")) {
        /* SENTINEL PENDING-SCRIPTS */

        if (c->argc != 2) goto numargserr;
        sentinelPendingScriptsCommand(c);
        // 本哨兵将追加一个需要监控的master节点
    } else if (!strcasecmp(c->argv[1]->ptr, "monitor")) {
        /* SENTINEL MONITOR <name> <ip> <port> <quorum> */
        sentinelRedisInstance *ri;
        long quorum, port;
        char ip[NET_IP_STR_LEN];

        if (c->argc != 6) goto numargserr;

        // 先进行参数校验
        if (getLongFromObjectOrReply(c, c->argv[5], &quorum, "Invalid quorum")
            != C_OK)
            return;
        if (getLongFromObjectOrReply(c, c->argv[4], &port, "Invalid port")
            != C_OK)
            return;

        if (quorum <= 0) {
            addReplyError(c, "Quorum must be 1 or greater.");
            return;
        }

        /* Make sure the IP field is actually a valid IP before passing it
         * to createSentinelRedisInstance(), otherwise we may trigger a
         * DNS lookup at runtime.
         * 确保ip合法
         * */
        if (anetResolveIP(NULL, c->argv[3]->ptr, ip, sizeof(ip)) == ANET_ERR) {
            addReplyError(c, "Invalid IP address specified");
            return;
        }

        /* Parameters are valid. Try to create the master instance.
         * */
        ri = createSentinelRedisInstance(c->argv[2]->ptr, SRI_MASTER,
                                         c->argv[3]->ptr, port, quorum, NULL);
        // 忽略创建失败的情况
        if (ri == NULL) {
            switch (errno) {
                case EBUSY:
                    addReplyError(c, "Duplicated master name");
                    break;
                case EINVAL:
                    addReplyError(c, "Invalid port number");
                    break;
                default:
                    addReplyError(c, "Unspecified error adding the instance");
                    break;
            }
        } else {
            // 因为此时增加了新的监控节点 更新配置信息
            sentinelFlushConfig();
            sentinelEvent(LL_WARNING, "+monitor", ri, "%@ quorum %d", ri->quorum);
            addReply(c, shared.ok);
        }
        // 手动发起一次配置文件刷盘
    } else if (!strcasecmp(c->argv[1]->ptr, "flushconfig")) {
        if (c->argc != 2) goto numargserr;
        sentinelFlushConfig();
        addReply(c, shared.ok);
        return;
        // 解除对某个master的监控
    } else if (!strcasecmp(c->argv[1]->ptr, "remove")) {
        /* SENTINEL REMOVE <name> */
        sentinelRedisInstance *ri;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c, c->argv[2]))
            == NULL)
            return;
        // 也是发出事件 异步处理
        sentinelEvent(LL_WARNING, "-monitor", ri, "%@");
        dictDelete(sentinel.masters, c->argv[2]->ptr);
        sentinelFlushConfig();
        addReply(c, shared.ok);
        // 检查此时是否有足够的哨兵能够正常工作，以确保监控master的状态
    } else if (!strcasecmp(c->argv[1]->ptr, "ckquorum")) {
        /* SENTINEL CKQUORUM <name> */
        sentinelRedisInstance *ri;

        // 记录此时不可用的哨兵数量
        int usable;

        if (c->argc != 3) goto numargserr;
        if ((ri = sentinelGetMasterByNameOrReplyError(c, c->argv[2]))
            == NULL)
            return;
        // 根据此时可用的哨兵数量 返回不同的标记位
        int result = sentinelIsQuorumReachable(ri, &usable);
        // 代表此时哨兵数量满足
        if (result == SENTINEL_ISQR_OK) {
            addReplySds(c, sdscatfmt(sdsempty(),
                                     "+OK %i usable Sentinels. Quorum and failover authorization "
                                     "can be reached\r\n", usable));
        } else {
            sds e = sdscatfmt(sdsempty(),
                              "-NOQUORUM %i usable Sentinels. ", usable);
            if (result & SENTINEL_ISQR_NOQUORUM)
                e = sdscat(e, "Not enough available Sentinels to reach the"
                              " specified quorum for this master");
            if (result & SENTINEL_ISQR_NOAUTH) {
                if (result & SENTINEL_ISQR_NOQUORUM) e = sdscat(e, ". ");
                e = sdscat(e, "Not enough available Sentinels to reach the"
                              " majority and authorize a failover");
            }
            e = sdscat(e, "\r\n");
            addReplySds(c, e);
        }
        // set命令就是传入一些参数 设置到本实例中
    } else if (!strcasecmp(c->argv[1]->ptr, "set")) {
        if (c->argc < 3) goto numargserr;
        sentinelSetCommand(c);
        // 获取缓存信息
    } else if (!strcasecmp(c->argv[1]->ptr, "info-cache")) {
        /* SENTINEL INFO-CACHE <name> */
        if (c->argc < 2) goto numargserr;
        mstime_t now = mstime();

        /* Create an ad-hoc dictionary type so that we can iterate
         * a dictionary composed of just the master groups the user
         * requested. */
        dictType copy_keeper = instancesDictType;
        copy_keeper.valDestructor = NULL;
        // 默认情况下会展示所有的master
        dict *masters_local = sentinel.masters;
        // 代表client指定了仅获取部分master信息
        if (c->argc > 2) {
            masters_local = dictCreate(&copy_keeper, NULL);

            for (int i = 2; i < c->argc; i++) {
                sentinelRedisInstance *ri;
                ri = sentinelGetMasterByName(c->argv[i]->ptr);
                if (!ri) continue; /* ignore non-existing names */
                dictAdd(masters_local, ri->name, ri);
            }
        }

        /* Reply format:
         *   1.) master name
         *   2.) 1.) info from master
         *       2.) info from replica
         *       ...
         *   3.) other master name
         *   ...
         */
        addReplyArrayLen(c, dictSize(masters_local) * 2);

        dictIterator *di;
        dictEntry *de;

        // 遍历本次指定的所有master
        di = dictGetIterator(masters_local);
        while ((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);
            addReplyBulkCBuffer(c, ri->name, strlen(ri->name));
            // 写入slave数量信息
            addReplyArrayLen(c, dictSize(ri->slaves) + 1); /* +1 for self */
            addReplyArrayLen(c, 2);
            // 最近一次刷新信息的时间戳
            addReplyLongLong(c, now - ri->info_refresh);
            if (ri->info)
                addReplyBulkCBuffer(c, ri->info, sdslen(ri->info));
            else
                addReplyNull(c);

            dictIterator *sdi;
            dictEntry *sde;
            // 写入单个slave的信息
            sdi = dictGetIterator(ri->slaves);
            while ((sde = dictNext(sdi)) != NULL) {
                sentinelRedisInstance *sri = dictGetVal(sde);
                addReplyArrayLen(c, 2);
                addReplyLongLong(c, now - sri->info_refresh);
                if (sri->info)
                    addReplyBulkCBuffer(c, sri->info, sdslen(sri->info));
                else
                    addReplyNull(c);
            }
            dictReleaseIterator(sdi);
        }
        dictReleaseIterator(di);
        if (masters_local != sentinel.masters) dictRelease(masters_local);
        // 修改simulate-failure
    } else if (!strcasecmp(c->argv[1]->ptr, "simulate-failure")) {
        /* SENTINEL SIMULATE-FAILURE <flag> <flag> ... <flag> */
        int j;

        sentinel.simfailure_flags = SENTINEL_SIMFAILURE_NONE;
        for (j = 2; j < c->argc; j++) {
            if (!strcasecmp(c->argv[j]->ptr, "crash-after-election")) {
                sentinel.simfailure_flags |=
                        SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION;
                serverLog(LL_WARNING, "Failure simulation: this Sentinel "
                                      "will crash after being successfully elected as failover "
                                      "leader");
            } else if (!strcasecmp(c->argv[j]->ptr, "crash-after-promotion")) {
                sentinel.simfailure_flags |=
                        SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION;
                serverLog(LL_WARNING, "Failure simulation: this Sentinel "
                                      "will crash after promoting the selected replica to master");
            } else if (!strcasecmp(c->argv[j]->ptr, "help")) {
                addReplyArrayLen(c, 2);
                addReplyBulkCString(c, "crash-after-election");
                addReplyBulkCString(c, "crash-after-promotion");
            } else {
                addReplyError(c, "Unknown failure simulation specified");
                return;
            }
        }
        addReply(c, shared.ok);
    } else {
        addReplyErrorFormat(c, "Unknown sentinel subcommand '%s'",
                            (char *) c->argv[1]->ptr);
    }
    return;

    numargserr:
    addReplyErrorFormat(c, "Wrong number of arguments for 'sentinel %s'",
                        (char *) c->argv[1]->ptr);
}

#define info_section_from_redis(section_name) do { \
    if (defsections || allsections || !strcasecmp(section,section_name)) { \
        sds redissection; \
        if (sections++) info = sdscat(info,"\r\n"); \
        redissection = genRedisInfoString(section_name); \
        info = sdscatlen(info,redissection,sdslen(redissection)); \
        sdsfree(redissection); \
    } \
} while(0)

/* SENTINEL INFO [section]
 * 返回本哨兵信息
 * */
void sentinelInfoCommand(client *c) {
    if (c->argc > 2) {
        addReply(c, shared.syntaxerr);
        return;
    }

    int defsections = 0, allsections = 0;
    // 代表手动设置了要获取的信息范围
    char *section = c->argc == 2 ? c->argv[1]->ptr : NULL;
    if (section) {
        allsections = !strcasecmp(section, "all");
        defsections = !strcasecmp(section, "default");
    } else {
        // 未设置 仅获取默认信息
        defsections = 1;
    }

    int sections = 0;
    sds info = sdsempty();

    info_section_from_redis("server");
    info_section_from_redis("clients");
    info_section_from_redis("cpu");
    info_section_from_redis("stats");

    if (defsections || allsections || !strcasecmp(section, "sentinel")) {
        dictIterator *di;
        dictEntry *de;
        int master_id = 0;

        if (sections++) info = sdscat(info, "\r\n");
        info = sdscatprintf(info,
                            "# Sentinel\r\n"
                            "sentinel_masters:%lu\r\n"
                            "sentinel_tilt:%d\r\n"
                            "sentinel_running_scripts:%d\r\n"
                            "sentinel_scripts_queue_length:%ld\r\n"
                            "sentinel_simulate_failure_flags:%lu\r\n",
                            dictSize(sentinel.masters),
                            sentinel.tilt,
                            sentinel.running_scripts,
                            listLength(sentinel.scripts_queue),
                            sentinel.simfailure_flags);

        di = dictGetIterator(sentinel.masters);
        while ((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);
            char *status = "ok";

            if (ri->flags & SRI_O_DOWN) status = "odown";
            else if (ri->flags & SRI_S_DOWN) status = "sdown";
            info = sdscatprintf(info,
                                "master%d:name=%s,status=%s,address=%s:%d,"
                                "slaves=%lu,sentinels=%lu\r\n",
                                master_id++, ri->name, status,
                                ri->addr->ip, ri->addr->port,
                                dictSize(ri->slaves),
                                dictSize(ri->sentinels) + 1);
        }
        dictReleaseIterator(di);
    }

    addReplyBulkSds(c, info);
}

/* Implements Sentinel version of the ROLE command. The output is
 * "sentinel" and the list of currently monitored master names.
 * 将当前所监控的所有master.name返回  role代表的是名称
 */
void sentinelRoleCommand(client *c) {
    dictIterator *di;
    dictEntry *de;

    addReplyArrayLen(c, 2);
    addReplyBulkCBuffer(c, "sentinel", 8);
    addReplyArrayLen(c, dictSize(sentinel.masters));

    di = dictGetIterator(sentinel.masters);
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        addReplyBulkCString(c, ri->name);
    }
    dictReleaseIterator(di);
}

/* SENTINEL SET <mastername> [<option> <value> ...]
 * 解析参数信息 设置到实例中
 * */
void sentinelSetCommand(client *c) {
    sentinelRedisInstance *ri;
    int j, changes = 0;
    int badarg = 0; /* Bad argument position for error reporting. */
    char *option;

    // 先查看本次针对的是哪个master
    if ((ri = sentinelGetMasterByNameOrReplyError(c, c->argv[2]))
        == NULL)
        return;

    /* Process option - value pairs. */
    for (j = 3; j < c->argc; j++) {
        int moreargs = (c->argc - 1) - j;
        option = c->argv[j]->ptr;
        long long ll;
        int old_j = j; /* Used to know what to log as an event. */

        if (!strcasecmp(option, "down-after-milliseconds") && moreargs > 0) {
            /* down-after-millisecodns <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o, &ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->down_after_period = ll;
            // 将本次修改属性传递到master下每个slave/sentinel
            sentinelPropagateDownAfterPeriod(ri);
            changes++;
            // 设置选举的最大耗时
        } else if (!strcasecmp(option, "failover-timeout") && moreargs > 0) {
            /* failover-timeout <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o, &ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->failover_timeout = ll;
            changes++;
            // 设置并行同步时间
        } else if (!strcasecmp(option, "parallel-syncs") && moreargs > 0) {
            /* parallel-syncs <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o, &ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->parallel_syncs = ll;
            changes++;
            // 什么是通知脚本 ???
        } else if (!strcasecmp(option, "notification-script") && moreargs > 0) {
            /* notification-script <path> */
            char *value = c->argv[++j]->ptr;
            // 如果不允许在运行时修改脚本相关配置 返回错误信息
            if (sentinel.deny_scripts_reconfig) {
                addReplyError(c,
                              "Reconfiguration of scripts path is denied for "
                              "security reasons. Check the deny-scripts-reconfig "
                              "configuration directive in your Sentinel configuration");
                goto seterr;
            }

            // 检测是否有脚本的访问权限
            if (strlen(value) && access(value, X_OK) == -1) {
                addReplyError(c,
                              "Notification script seems non existing or non executable");
                goto seterr;
            }
            // 更改通知的脚本
            sdsfree(ri->notification_script);
            ri->notification_script = strlen(value) ? sdsnew(value) : NULL;
            changes++;
            // 同上
        } else if (!strcasecmp(option, "client-reconfig-script") && moreargs > 0) {
            /* client-reconfig-script <path> */
            char *value = c->argv[++j]->ptr;
            if (sentinel.deny_scripts_reconfig) {
                addReplyError(c,
                              "Reconfiguration of scripts path is denied for "
                              "security reasons. Check the deny-scripts-reconfig "
                              "configuration directive in your Sentinel configuration");
                goto seterr;
            }

            if (strlen(value) && access(value, X_OK) == -1) {
                addReplyError(c,
                              "Client reconfiguration script seems non existing or "
                              "non executable");
                goto seterr;
            }
            sdsfree(ri->client_reconfig_script);
            ri->client_reconfig_script = strlen(value) ? sdsnew(value) : NULL;
            changes++;
            // 修改认证密码
        } else if (!strcasecmp(option, "auth-pass") && moreargs > 0) {
            /* auth-pass <password> */
            char *value = c->argv[++j]->ptr;
            sdsfree(ri->auth_pass);
            ri->auth_pass = strlen(value) ? sdsnew(value) : NULL;
            changes++;
            // 修改认证用户名
        } else if (!strcasecmp(option, "auth-user") && moreargs > 0) {
            /* auth-user <username> */
            char *value = c->argv[++j]->ptr;
            sdsfree(ri->auth_user);
            ri->auth_user = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        } else if (!strcasecmp(option, "quorum") && moreargs > 0) {
            /* quorum <count> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o, &ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->quorum = ll;
            changes++;

            // rename_command 维护在ri->renamed_commands
        } else if (!strcasecmp(option, "rename-command") && moreargs > 1) {
            /* rename-command <oldname> <newname> */
            sds oldname = c->argv[++j]->ptr;
            sds newname = c->argv[++j]->ptr;

            if ((sdslen(oldname) == 0) || (sdslen(newname) == 0)) {
                badarg = sdslen(newname) ? j - 1 : j;
                goto badfmt;
            }

            /* Remove any older renaming for this command. */
            dictDelete(ri->renamed_commands, oldname);

            /* If the target name is the same as the source name there
             * is no need to add an entry mapping to itself. */
            if (!dictSdsKeyCaseCompare(NULL, oldname, newname)) {
                oldname = sdsdup(oldname);
                newname = sdsdup(newname);
                dictAdd(ri->renamed_commands, oldname, newname);
            }
            changes++;
        } else {
            addReplyErrorFormat(c, "Unknown option or number of arguments for "
                                   "SENTINEL SET '%s'", option);
            goto seterr;
        }

        /* Log the event.
         * 代表执行本次指令 还使用了几个参数  这个也要发出事件
         * */
        int numargs = j - old_j + 1;
        switch (numargs) {
            case 2:
                sentinelEvent(LL_WARNING, "+set", ri, "%@ %s %s", c->argv[old_j]->ptr,
                              c->argv[old_j + 1]->ptr);
                break;
            case 3:
                sentinelEvent(LL_WARNING, "+set", ri, "%@ %s %s %s", c->argv[old_j]->ptr,
                              c->argv[old_j + 1]->ptr,
                              c->argv[old_j + 2]->ptr);
                break;
            default:
                sentinelEvent(LL_WARNING, "+set", ri, "%@ %s", c->argv[old_j]->ptr);
                break;
        }
    }

    // 一旦配置发生了变化 立即刷盘
    if (changes) sentinelFlushConfig();
    addReply(c, shared.ok);
    return;

    badfmt: /* Bad format errors */
    addReplyErrorFormat(c, "Invalid argument '%s' for SENTINEL SET '%s'",
                        (char *) c->argv[badarg]->ptr, option);
    seterr:
    if (changes) sentinelFlushConfig();
    return;
}

/* Our fake PUBLISH command: it is actually useful only to receive hello messages
 * from the other sentinel instances, and publishing to a channel other than
 * SENTINEL_HELLO_CHANNEL is forbidden.
 *
 * Because we have a Sentinel PUBLISH, the code to send hello messages is the same
 * for all the three kind of instances: masters, slaves, sentinels.
 * 作为哨兵节点 它的publish 与普通节点的publish触发的command不一样  会直接处理收到的消息
 * 实际上针对hellow消息只有sentinel能处理 因为其他的slave/master并没有订阅这个消息
 * */
void sentinelPublishCommand(client *c) {
    if (strcmp(c->argv[1]->ptr, SENTINEL_HELLO_CHANNEL)) {
        addReplyError(c, "Only HELLO messages are accepted by Sentinel instances.");
        return;
    }
    sentinelProcessHelloMessage(c->argv[2]->ptr, sdslen(c->argv[2]->ptr));
    addReplyLongLong(c, 1);
}

/* ===================== SENTINEL availability checks ======================= */

/* Is this instance down from our point of view?
 * */
void sentinelCheckSubjectivelyDown(sentinelRedisInstance *ri) {
    mstime_t elapsed = 0;

    // 距离最后一次确认该节点肯定存活过了多久
    if (ri->link->act_ping_time)
        elapsed = mstime() - ri->link->act_ping_time;
    else if (ri->link->disconnected)
        elapsed = mstime() - ri->link->last_avail_time;

    /* Check if we are in need for a reconnection of one of the
     * links, because we are detecting low activity.
     *
     * 1) Check if the command link seems connected, was connected not less
     *    than SENTINEL_MIN_LINK_RECONNECT_PERIOD, but still we have a
     *    pending ping for more than half the timeout.
     *    */
    if (ri->link->cc &&
        (mstime() - ri->link->cc_conn_time) >
        SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
        ri->link->act_ping_time != 0 && /* There is a pending ping... */
        /* The pending ping is delayed, and we did not receive
         * error replies as well.
         * 只要上次收到心跳的时间超过了 down_after_period的一半 就认为该节点可能已经下线 手动断开连接 并尝试重连
         * */
        (mstime() - ri->link->act_ping_time) > (ri->down_after_period / 2) &&
        (mstime() - ri->link->last_pong_time) > (ri->down_after_period / 2)) {
        instanceLinkCloseConnection(ri->link, ri->link->cc);
    }

    /* 2) Check if the pubsub link seems connected, was connected not less
     *    than SENTINEL_MIN_LINK_RECONNECT_PERIOD, but still we have no
     *    activity in the Pub/Sub channel for more than
     *    SENTINEL_PUBLISH_PERIOD * 3.
     *    TODO pc 是publish相关的 先不看
     */
    if (ri->link->pc &&
        (mstime() - ri->link->pc_conn_time) >
        SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
        (mstime() - ri->link->pc_last_activity) > (SENTINEL_PUBLISH_PERIOD * 3)) {
        instanceLinkCloseConnection(ri->link, ri->link->pc);
    }

    /* Update the SDOWN flag. We believe the instance is SDOWN if:
     *
     * 1) It is not replying.
     * 2) We believe it is a master, it reports to be a slave for enough time
     *    to meet the down_after_period, plus enough time to get two times
     *    INFO report from the instance.
     *    此时未确认连接的时间已经超过了 down_after_period
     *    或者迟迟没有收到info command 导致本节点信息没有更新
     *    */
    if (elapsed > ri->down_after_period ||
        (ri->flags & SRI_MASTER &&
         ri->role_reported == SRI_SLAVE &&
         mstime() - ri->role_reported_time >
         (ri->down_after_period + SENTINEL_INFO_PERIOD * 2))) {
        /* Is subjectively down
         * 标记该实例已经下线 注意是本哨兵主观上认为下线
         * */
        if ((ri->flags & SRI_S_DOWN) == 0) {
            sentinelEvent(LL_WARNING, "+sdown", ri, "%@");
            ri->s_down_since_time = mstime();
            ri->flags |= SRI_S_DOWN;
        }
    } else {
        /* Is subjectively up
         * 否则该实例此时处于上线状态 如果之前之前是下线的 就要发起一个上线的事件
         * */
        if (ri->flags & SRI_S_DOWN) {
            sentinelEvent(LL_WARNING, "-sdown", ri, "%@");
            ri->flags &= ~(SRI_S_DOWN | SRI_SCRIPT_KILL_SENT);
        }
    }
}

/* Is this instance down according to the configured quorum?
 *
 * Note that ODOWN is a weak quorum, it only means that enough Sentinels
 * reported in a given time range that the instance was not reachable.
 * However messages can be delayed so there are no strong guarantees about
 * N instances agreeing at the same time about the down state.
 * 客观判断该实例是否已经下线 仅针对master节点
 * */
void sentinelCheckObjectivelyDown(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;
    unsigned int quorum = 0, odown = 0;

    // 此时本节点处于下线状态
    if (master->flags & SRI_S_DOWN) {
        /* Is down for enough sentinels? */
        quorum = 1; /* the current sentinel. */
        /* Count all the other sentinels. 遍历监控该master的所有哨兵 */
        di = dictGetIterator(master->sentinels);
        while ((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);

            // 该哨兵认为本节点也是下线状态
            if (ri->flags & SRI_MASTER_DOWN) quorum++;
        }
        dictReleaseIterator(di);
        // 代表此时有多个sentinel认为该节点下线  实锤
        if (quorum >= master->quorum) odown = 1;
    }

    /* Set the flag accordingly to the outcome.
     * 此时本实例被客观认为是下线的 发出事件 更新标识和下线时间
     * */
    if (odown) {
        if ((master->flags & SRI_O_DOWN) == 0) {
            sentinelEvent(LL_WARNING, "+odown", master, "%@ #quorum %d/%d",
                          quorum, master->quorum);
            master->flags |= SRI_O_DOWN;
            master->o_down_since_time = mstime();
        }
    } else {
        // 代表多数sentinel认可该实例处于上线状态 发出事件 更新标识
        if (master->flags & SRI_O_DOWN) {
            sentinelEvent(LL_WARNING, "-odown", master, "%@");
            master->flags &= ~SRI_O_DOWN;
        }
    }
}

/* Receive the SENTINEL is-master-down-by-addr reply, see the
 * sentinelAskMasterStateToOtherSentinels() function for more information.
 * 当本哨兵认为某个master下线后 会通知监控它的其他sentinel节点 这里是处理收到的结果
 * */
void sentinelReceiveIsMasterDownReply(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    // 回复的结果为空 或者连接为空 无法处理
    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    /* Ignore every error or unexpected reply.
     * Note that if the command returns an error for any reason we'll
     * end clearing the SRI_MASTER_DOWN flag for timeout anyway.
     * 这里要求回复的数据类型 必须符合期望
     * */
    if (r->type == REDIS_REPLY_ARRAY && r->elements == 3 &&
        r->element[0]->type == REDIS_REPLY_INTEGER &&
        r->element[1]->type == REDIS_REPLY_STRING &&
        r->element[2]->type == REDIS_REPLY_INTEGER) {
        // 更新reply时间
        ri->last_master_down_reply_time = mstime();
        if (r->element[0]->integer == 1) {
            // 该节点认可master下线 同步该sentinel在本节点上的信息
            ri->flags |= SRI_MASTER_DOWN;
        } else {
            ri->flags &= ~SRI_MASTER_DOWN;
        }
        // 第二个参数不是 * 代表该哨兵此时发起了一次选举
        if (strcmp(r->element[1]->str, "*")) {
            /* If the runid in the reply is not "*" the Sentinel actually
             * replied with a vote.
             * 既然发起了选举 就清理掉之前记录的leader
             * */
            sdsfree(ri->leader);
            if ((long long) ri->leader_epoch != r->element[2]->integer)
                serverLog(LL_WARNING,
                          "%s voted for %s %llu", ri->name,
                          r->element[1]->str,
                          (unsigned long long) r->element[2]->integer);
            // 更新最新的leader信息
            ri->leader = sdsnew(r->element[1]->str);
            ri->leader_epoch = r->element[2]->integer;
        }
    }
}

/* If we think the master is down, we start sending
 * SENTINEL IS-MASTER-DOWN-BY-ADDR requests to other sentinels
 * in order to get the replies that allow to reach the quorum
 * needed to mark the master in ODOWN state and trigger a failover.
 * 将某个master节点的状态变更 通知到其他sentinel
 * */
#define SENTINEL_ASK_FORCED (1<<0)

void sentinelAskMasterStateToOtherSentinels(sentinelRedisInstance *master, int flags) {
    dictIterator *di;
    dictEntry *de;

    // 通过master反查所有哨兵
    di = dictGetIterator(master->sentinels);
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        mstime_t elapsed = mstime() - ri->last_master_down_reply_time;
        char port[32];
        int retval;

        /* If the master state from other sentinel is too old, we clear it.
         * 该哨兵距离上次认为该master下线已经过了很久 它的信息不可靠 清理master_down标记
         * */
        if (elapsed > SENTINEL_ASK_PERIOD * 5) {
            ri->flags &= ~SRI_MASTER_DOWN;
            sdsfree(ri->leader);
            ri->leader = NULL;
        }

        /* Only ask if master is down to other sentinels if:
         *
         * 1) We believe it is down, or there is a failover in progress.
         * 2) Sentinel is connected.
         * 3) We did not receive the info within SENTINEL_ASK_PERIOD ms.
         * 如果本节点认为该master还在线 跳过
         * */
        if ((master->flags & SRI_S_DOWN) == 0) continue;
        // 如果与该哨兵的连接已经断开 无法通知 也跳过
        if (ri->link->disconnected) continue;
        // 非强制通知的情况 如果距离上一次通知时间少于一个最小间隔 不需要重复通知
        if (!(flags & SENTINEL_ASK_FORCED) &&
            mstime() - ri->last_master_down_reply_time < SENTINEL_ASK_PERIOD)
            continue;

        /* Ask */
        ll2string(port, sizeof(port), master->addr->port);
        // 将master下线的信息通知到其他sentinel
        retval = redisAsyncCommand(ri->link->cc,
                                   sentinelReceiveIsMasterDownReply, ri,
                                   "%s is-master-down-by-addr %s %s %llu %s",
                                   sentinelInstanceMapCommand(ri, "SENTINEL"),
                                   master->addr->ip, port,
                                   sentinel.current_epoch,
                                   (master->failover_state > SENTINEL_FAILOVER_STATE_NONE) ?
                                   sentinel.myid : "*");
        if (retval == C_OK) ri->link->pending_commands++;
    }
    dictReleaseIterator(di);
}

/* =============================== FAILOVER ================================= */

/* Crash because of user request via SENTINEL simulate-failure command. */
void sentinelSimFailureCrash(void) {
    serverLog(LL_WARNING,
              "Sentinel CRASH because of SENTINEL simulate-failure");
    exit(99);
}

/* Vote for the sentinel with 'req_runid' or return the old vote if already
 * voted for the specified 'req_epoch' or one greater.
 *
 * If a vote is not available returns NULL, otherwise return the Sentinel
 * runid and populate the leader_epoch with the epoch of the vote.
 * 某个master为某个runid对应的哨兵投票   在整个集群中只有master可以为哨兵投票
 * */
char *sentinelVoteLeader(sentinelRedisInstance *master, uint64_t req_epoch, char *req_runid, uint64_t *leader_epoch) {
    // 如果进入新一轮选举 要更新配置信息
    if (req_epoch > sentinel.current_epoch) {
        sentinel.current_epoch = req_epoch;
        sentinelFlushConfig();
        // 发出一个newEpoch事件
        sentinelEvent(LL_WARNING, "+new-epoch", master, "%llu",
                      (unsigned long long) sentinel.current_epoch);
    }

    // 本次任期更大 投票有效
    if (master->leader_epoch < req_epoch && sentinel.current_epoch <= req_epoch) {
        sdsfree(master->leader);
        // 更新master的leader信息以及任期
        master->leader = sdsnew(req_runid);
        master->leader_epoch = sentinel.current_epoch;
        sentinelFlushConfig();
        // 因为发出了一次投票事件 发布消息
        sentinelEvent(LL_WARNING, "+vote-for-leader", master, "%s %llu",
                      master->leader, (unsigned long long) master->leader_epoch);
        /* If we did not voted for ourselves, set the master failover start
         * time to now, in order to force a delay before we can start a
         * failover for the same master.
         * TODO 如果master本次是为本哨兵投票 重置failover_start_time
         * */
        if (strcasecmp(master->leader, sentinel.myid))
            master->failover_start_time = mstime() + rand() % SENTINEL_MAX_DESYNC;
    }

    // 其余情况代表本节点的任期超过了传入的任期 所以不做操作

    // 返回master此时的任期
    *leader_epoch = master->leader_epoch;
    return master->leader ? sdsnew(master->leader) : NULL;
}

struct sentinelLeader {
    char *runid;
    unsigned long votes;
};

/* Helper function for sentinelGetLeader, increment the counter
 * relative to the specified runid.
 * */
int sentinelLeaderIncr(dict *counters, char *runid) {
    dictEntry *existing, *de;
    uint64_t oldval;

    de = dictAddRaw(counters, runid, &existing);
    if (existing) {
        oldval = dictGetUnsignedIntegerVal(existing);
        dictSetUnsignedIntegerVal(existing, oldval + 1);
        return oldval + 1;
    } else {
        serverAssert(de != NULL);
        dictSetUnsignedIntegerVal(de, 1);
        return 1;
    }
}

/* Scan all the Sentinels attached to this master to check if there
 * is a leader for the specified epoch.
 *
 * To be a leader for a given epoch, we should have the majority of
 * the Sentinels we know (ever seen since the last SENTINEL RESET) that
 * reported the same instance as leader for the same epoch.
 * */
char *sentinelGetLeader(sentinelRedisInstance *master, uint64_t epoch) {
    dict *counters;
    dictIterator *di;
    dictEntry *de;
    unsigned int voters = 0, voters_quorum;
    char *myvote;
    char *winner = NULL;
    uint64_t leader_epoch;
    uint64_t max_votes = 0;

    // 应当在本节点下线 或者开始故障转移时调用该方法
    serverAssert(master->flags & (SRI_O_DOWN | SRI_FAILOVER_IN_PROGRESS));
    counters = dictCreate(&leaderVotesDictType, NULL);

    // 所有哨兵作为参选节点 master->sentinels中不会包含本节点
    voters = dictSize(master->sentinels) + 1; /* All the other sentinels and me.*/

    /* Count other sentinels votes */
    di = dictGetIterator(master->sentinels);
    // 遍历这些哨兵 获取此时哨兵内部认可的leader
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        // sentinel 存储的是本哨兵的信息 当其他哨兵的leader_epoch与本节点current_epoch 一致时 他的信息才有参考价值
        // 此时ri->leader 只是本轮它选举选择的节点 而不是集群中认可的leader
        if (ri->leader != NULL && ri->leader_epoch == sentinel.current_epoch)
            // 增加投票箱的计数
            sentinelLeaderIncr(counters, ri->leader);
    }
    dictReleaseIterator(di);

    /* Check what's the winner. For the winner to win, it needs two conditions:
     * 1) Absolute majority between voters (50% + 1).
     * 2) And anyway at least master->quorum votes.
     * 找到本轮票数最多的哨兵节点作为leader
     * */
    di = dictGetIterator(counters);
    while ((de = dictNext(di)) != NULL) {
        uint64_t votes = dictGetUnsignedIntegerVal(de);

        if (votes > max_votes) {
            max_votes = votes;
            winner = dictGetKey(de);
        }
    }
    dictReleaseIterator(di);

    /* Count this Sentinel vote:
     * if this Sentinel did not voted yet, either vote for the most
     * common voted sentinel, or for itself if no vote exists at all.
     * 获取 master->leader  当epoch大于之前的任期 master在本轮选继续选择winner 作为新一轮的leader  为什么不是本sentinel节点去投票 变成了一个master在投票 ???
     * */
    if (winner)
        myvote = sentinelVoteLeader(master, epoch, winner, &leader_epoch);
    else
        // winner为null 可能代表此时进入新的一轮投票 而此时其他哨兵都还未选择某个节点 这时master就会将本节点作为leader
        myvote = sentinelVoteLeader(master, epoch, sentinel.myid, &leader_epoch);

    // 如果此时master->leader_epoch是有效的 它此时的leader才有参考价值 允许master选择的leader 与其他sentinel不一样么 那么就无法体现一致性了 同一任期下有多个sentinel被当作leader???
    if (myvote && leader_epoch == epoch) {
        uint64_t votes = sentinelLeaderIncr(counters, myvote);

        if (votes > max_votes) {
            max_votes = votes;
            winner = myvote;
        }
    }

    // voters 是集群中所有的sentinels 那么要成为leader 就要获取超过1/2节点的认可
    voters_quorum = voters / 2 + 1;
    // TODO master->quorum 是什么意思
    // 如果此时得到最多票数的节点 还没有到1/2 代表本轮没有选出leader
    if (winner && (max_votes < voters_quorum || max_votes < master->quorum))
        winner = NULL;

    winner = winner ? sdsnew(winner) : NULL;
    sdsfree(myvote);
    dictRelease(counters);
    return winner;
}

/* Send SLAVEOF to the specified instance, always followed by a
 * CONFIG REWRITE command in order to store the new configuration on disk
 * when possible (that is, if the Redis instance is recent enough to support
 * config rewriting, and if the server was started with a configuration file).
 *
 * If Host is NULL the function sends "SLAVEOF NO ONE".
 *
 * The command returns C_OK if the SLAVEOF command was accepted for
 * (later) delivery otherwise C_ERR. The command replies are just
 * discarded.
 * 修改本实例的 slaveOf
 * */
int sentinelSendSlaveOf(sentinelRedisInstance *ri, char *host, int port) {
    char portstr[32];
    int retval;

    ll2string(portstr, sizeof(portstr), port);

    /* If host is NULL we send SLAVEOF NO ONE that will turn the instance
     * into a master.
     * 代表本节点变成了master节点
     * */
    if (host == NULL) {
        host = "NO";
        memcpy(portstr, "ONE", 4);
    }

    /* In order to send SLAVEOF in a safe way, we send a transaction performing
     * the following tasks:
     * 1) Reconfigure the instance according to the specified host/port params.
     * 2) Rewrite the configuration.
     * 3) Disconnect all clients (but this one sending the command) in order
     *    to trigger the ask-master-on-reconnection protocol for connected
     *    clients.
     *
     * Note that we don't check the replies returned by commands, since we
     * will observe instead the effects in the next INFO output.
     * 向该实例发起一组command 使其变成master的slave节点
     * */
    retval = redisAsyncCommand(ri->link->cc,
                               sentinelDiscardReplyCallback, ri, "%s",
                               sentinelInstanceMapCommand(ri, "MULTI"));
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    retval = redisAsyncCommand(ri->link->cc,
                               sentinelDiscardReplyCallback, ri, "%s %s %s",
                               sentinelInstanceMapCommand(ri, "SLAVEOF"),
                               host, portstr);
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    // 更新配置文件
    retval = redisAsyncCommand(ri->link->cc,
                               sentinelDiscardReplyCallback, ri, "%s REWRITE",
                               sentinelInstanceMapCommand(ri, "CONFIG"));
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    /* CLIENT KILL TYPE <type> is only supported starting from Redis 2.8.12,
     * however sending it to an instance not understanding this command is not
     * an issue because CLIENT is variadic command, so Redis will not
     * recognized as a syntax error, and the transaction will not fail (but
     * only the unsupported command will fail).
     * 之后针对该节点关闭普通节点连接以及pubsub连接
     * */
    for (int type = 0; type < 2; type++) {
        retval = redisAsyncCommand(ri->link->cc,
                                   sentinelDiscardReplyCallback, ri, "%s KILL TYPE %s",
                                   sentinelInstanceMapCommand(ri, "CLIENT"),
                                   type == 0 ? "normal" : "pubsub");
        if (retval == C_ERR) return retval;
        ri->link->pending_commands++;
    }

    // 最后执行exec命令
    retval = redisAsyncCommand(ri->link->cc,
                               sentinelDiscardReplyCallback, ri, "%s",
                               sentinelInstanceMapCommand(ri, "EXEC"));
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    return C_OK;
}

/* Setup the master state to start a failover.
 * 针对某个master对应的主从结构 强制发起选举
 * */
void sentinelStartFailover(sentinelRedisInstance *master) {
    serverAssert(master->flags & SRI_MASTER);

    master->failover_state = SENTINEL_FAILOVER_STATE_WAIT_START;
    // 代表已经开始故障转移了 避免针对该主从结构重复发起
    master->flags |= SRI_FAILOVER_IN_PROGRESS;

    // 整个选举是由哨兵来主导的 每发起一轮选举 就要增加epoch
    master->failover_epoch = ++sentinel.current_epoch;
    // epoch发生变化 以及发起了选举 发布2个事件
    sentinelEvent(LL_WARNING, "+new-epoch", master, "%llu",
                  (unsigned long long) sentinel.current_epoch);
    sentinelEvent(LL_WARNING, "+try-failover", master, "%@");
    master->failover_start_time = mstime() + rand() % SENTINEL_MAX_DESYNC;
    master->failover_state_change_time = mstime();
}

/* This function checks if there are the conditions to start the failover,
 * that is:
 *
 * 1) Master must be in ODOWN condition.
 * 2) No failover already in progress.
 * 3) No failover already attempted recently.
 *
 * We still don't know if we'll win the election so it is possible that we
 * start the failover but that we'll not be able to act.
 *
 * Return non-zero if a failover was started.
 * 检查该master节点是否需要故障转移 也就是选择下面的某个slave作为新的master
 * */
int sentinelStartFailoverIfNeeded(sentinelRedisInstance *master) {
    /* We can't failover if the master is not in O_DOWN state.
     * 该节点并没有被多个哨兵承认已经下线  所以不需要故障转移
     * */
    if (!(master->flags & SRI_O_DOWN)) return 0;

    /* Failover already in progress?
     * 如果针对该master已经开始进行故障转移了 也不需要处理 每个哨兵都可以对某个master发起故障转移
     * */
    if (master->flags & SRI_FAILOVER_IN_PROGRESS) return 0;

    /* Last failover attempt started too little time ago?
     * 当故障转移耗时过长时 仅打印日志
     * */
    if (mstime() - master->failover_start_time <
        master->failover_timeout * 2) {
        if (master->failover_delay_logged != master->failover_start_time) {
            time_t clock = (master->failover_start_time +
                            master->failover_timeout * 2) / 1000;
            char ctimebuf[26];

            ctime_r(&clock, ctimebuf);
            ctimebuf[24] = '\0'; /* Remove newline. */
            master->failover_delay_logged = master->failover_start_time;
            serverLog(LL_WARNING,
                      "Next failover delay: I will not start a failover before %s",
                      ctimebuf);
        }
        return 0;
    }

    // 这里主要还是发出事件 具体的逻辑在某个事件处理器上
    sentinelStartFailover(master);
    return 1;
}

/* Select a suitable slave to promote. The current algorithm only uses
 * the following parameters:
 *
 * 1) None of the following conditions: S_DOWN, O_DOWN, DISCONNECTED.
 * 2) Last time the slave replied to ping no more than 5 times the PING period.
 * 3) info_refresh not older than 3 times the INFO refresh period.
 * 4) master_link_down_time no more than:
 *     (now - master->s_down_since_time) + (master->down_after_period * 10).
 *    Basically since the master is down from our POV, the slave reports
 *    to be disconnected no more than 10 times the configured down-after-period.
 *    This is pretty much black magic but the idea is, the master was not
 *    available so the slave may be lagging, but not over a certain time.
 *    Anyway we'll select the best slave according to replication offset.
 * 5) Slave priority can't be zero, otherwise the slave is discarded.
 *
 * Among all the slaves matching the above conditions we select the slave
 * with, in order of sorting key:
 *
 * - lower slave_priority.
 * - bigger processed replication offset.
 * - lexicographically smaller runid.
 *
 * Basically if runid is the same, the slave that processed more commands
 * from the master is selected.
 *
 * The function returns the pointer to the selected slave, otherwise
 * NULL if no suitable slave was found.
 */

/* Helper for sentinelSelectSlave(). This is used by qsort() in order to
 * sort suitable slaves in a "better first" order, to take the first of
 * the list.
 * 作为一组可选的slave 会对他们进行排序
 * */
int compareSlavesForPromotion(const void *a, const void *b) {
    sentinelRedisInstance **sa = (sentinelRedisInstance **) a,
            **sb = (sentinelRedisInstance **) b;
    char *sa_runid, *sb_runid;

    // 如果slave设置了优先级 直接比较优先级
    if ((*sa)->slave_priority != (*sb)->slave_priority)
        return (*sa)->slave_priority - (*sb)->slave_priority;

    /* If priority is the same, select the slave with greater replication
     * offset (processed more data from the master).
     * 选择同步偏移量最大的
     * */
    if ((*sa)->slave_repl_offset > (*sb)->slave_repl_offset) {
        return -1; /* a < b */
    } else if ((*sa)->slave_repl_offset < (*sb)->slave_repl_offset) {
        return 1; /* a > b */
    }

    /* If the replication offset is the same select the slave with that has
     * the lexicographically smaller runid. Note that we try to handle runid
     * == NULL as there are old Redis versions that don't publish runid in
     * INFO. A NULL runid is considered bigger than any other runid.
     * 最后按照slave的runid排序
     * */
    sa_runid = (*sa)->runid;
    sb_runid = (*sb)->runid;
    if (sa_runid == NULL && sb_runid == NULL) return 0;
    else if (sa_runid == NULL) return 1;  /* a > b */
    else if (sb_runid == NULL) return -1; /* a < b */
    return strcasecmp(sa_runid, sb_runid);
}

/**
 * 为当前某个master->slave主从结构选一个接替人
 * @param master
 * @return
 */
sentinelRedisInstance *sentinelSelectSlave(sentinelRedisInstance *master) {

    // 预先分配一个实例数组
    sentinelRedisInstance **instance =
            zmalloc(sizeof(instance[0]) * dictSize(master->slaves));
    sentinelRedisInstance *selected = NULL;
    int instances = 0;
    dictIterator *di;
    dictEntry *de;

    // 计算到现在master已经下线了多久  也可能是手动触发的选举
    mstime_t max_master_down_time = 0;

    if (master->flags & SRI_S_DOWN)
        max_master_down_time += mstime() - master->s_down_since_time;
    max_master_down_time += master->down_after_period * 10;

    di = dictGetIterator(master->slaves);

    // 遍历每个slave
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);
        mstime_t info_validity_time;

        // 无论是主观下线还是客观下线 这种节点会被跳过
        if (slave->flags & (SRI_S_DOWN | SRI_O_DOWN)) continue;
        // 如果与该节点的连接已经断开 就跳过
        if (slave->link->disconnected) continue;
        // 不选择长时间未与本哨兵交互的slave
        if (mstime() - slave->link->last_avail_time > SENTINEL_PING_PERIOD * 5) continue;
        // 优先级太低的slave 也会被跳过
        if (slave->slave_priority == 0) continue;

        /* If the master is in SDOWN state we get INFO for slaves every second.
         * Otherwise we get it with the usual period so we need to account for
         * a larger delay.
         * 此时根据master的下线标记类型 生成不同的时间
         * */
        if (master->flags & SRI_S_DOWN)
            info_validity_time = SENTINEL_PING_PERIOD * 5;
        else
            info_validity_time = SENTINEL_INFO_PERIOD * 3;
        // 距离上一次更新slave的信息比较旧了 忽略
        if (mstime() - slave->info_refresh > info_validity_time) continue;
        // 与master断开连接的时间太长也忽略
        if (slave->master_link_down_time > max_master_down_time) continue;
        // 将候选者加入到 instance数组中
        instance[instances++] = slave;
    }
    dictReleaseIterator(di);
    // 将他们排序后 返回最优的节点
    if (instances) {
        qsort(instance, instances, sizeof(sentinelRedisInstance *),
              compareSlavesForPromotion);
        selected = instance[0];
    }
    zfree(instance);
    return selected;
}

/* ---------------- Failover state machine implementation -------------------
 * 开始进行故障转移
 * */
void sentinelFailoverWaitStart(sentinelRedisInstance *ri) {
    char *leader;
    int isleader;

    /* Check if we are the leader for the failover epoch.
     * 获取之前集群中认可的leader
     * */
    leader = sentinelGetLeader(ri, ri->failover_epoch);
    // 本哨兵节点就是leader
    isleader = leader && strcasecmp(leader, sentinel.myid) == 0;
    sdsfree(leader);

    /* If I'm not the leader, and it is not a forced failover via
     * SENTINEL FAILOVER, then I can't continue with the failover.
     * 故障转移应该由leader节点发起 这样可以确保集群不会混乱  如果是强制发起故障转移的话 即使非leader节点也可以处理
     * */
    if (!isleader && !(ri->flags & SRI_FORCE_FAILOVER)) {
        int election_timeout = SENTINEL_ELECTION_TIMEOUT;

        /* The election timeout is the MIN between SENTINEL_ELECTION_TIMEOUT
         * and the configured failover timeout. */
        if (election_timeout > ri->failover_timeout)
            election_timeout = ri->failover_timeout;
        /* Abort the failover if I'm not the leader after some time. */
        if (mstime() - ri->failover_start_time > election_timeout) {
            sentinelEvent(LL_WARNING, "-failover-abort-not-elected", ri, "%@");
            // 主要是做一些清理工作
            sentinelAbortFailover(ri);
        }
        return;
    }
    // 代表此时已经确定leader节点
    sentinelEvent(LL_WARNING, "+elected-leader", ri, "%@");
    // 什么时候会出现这种情况 现在就是直接退出进程
    if (sentinel.simfailure_flags & SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION)
        sentinelSimFailureCrash();
    // 选择哪个slave作为交接人
    ri->failover_state = SENTINEL_FAILOVER_STATE_SELECT_SLAVE;
    ri->failover_state_change_time = mstime();
    sentinelEvent(LL_WARNING, "+failover-state-select-slave", ri, "%@");
}

/**
 * 开始选择合适的交接人
 * @param ri
 */
void sentinelFailoverSelectSlave(sentinelRedisInstance *ri) {

    // 这里已经找到了合适的slave
    sentinelRedisInstance *slave = sentinelSelectSlave(ri);

    /* We don't handle the timeout in this state as the function aborts
     * the failover or go forward in the next state.
     * 没有找到合适的节点 终止本次故障转移
     * */
    if (slave == NULL) {
        sentinelEvent(LL_WARNING, "-failover-abort-no-good-slave", ri, "%@");
        sentinelAbortFailover(ri);
    } else {
        // 设置 promoted_slave
        sentinelEvent(LL_WARNING, "+selected-slave", slave, "%@");
        slave->flags |= SRI_PROMOTED;
        ri->promoted_slave = slave;
        ri->failover_state = SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE;
        ri->failover_state_change_time = mstime();
        sentinelEvent(LL_NOTICE, "+failover-state-send-slaveof-noone",
                      slave, "%@");
    }
}

/**
 * 通知slave节点 进行故障转移 　
 * @param ri
 */
void sentinelFailoverSendSlaveOfNoOne(sentinelRedisInstance *ri) {
    int retval;

    /* We can't send the command to the promoted slave if it is now
     * disconnected. Retry again and again with this state until the timeout
     * is reached, then abort the failover.
     * 如果此时发现最合适的slave节点已经断开连接了 终止本次故障转移 这样在下次主循环中又会重新发起故障转移
     * */
    if (ri->promoted_slave->link->disconnected) {
        if (mstime() - ri->failover_state_change_time > ri->failover_timeout) {
            sentinelEvent(LL_WARNING, "-failover-abort-slave-timeout", ri, "%@");
            sentinelAbortFailover(ri);
        }
        return;
    }

    /* Send SLAVEOF NO ONE command to turn the slave into a master.
     * We actually register a generic callback for this command as we don't
     * really care about the reply. We check if it worked indirectly observing
     * if INFO returns a different role (master instead of slave).
     * 通知slave节点 此时它的slaveof为空 也就是它自己就是master
     * */
    retval = sentinelSendSlaveOf(ri->promoted_slave, NULL, 0);
    if (retval != C_OK) return;
    sentinelEvent(LL_NOTICE, "+failover-state-wait-promotion",
                  ri->promoted_slave, "%@");
    ri->failover_state = SENTINEL_FAILOVER_STATE_WAIT_PROMOTION;
    ri->failover_state_change_time = mstime();
}

/* We actually wait for promotion indirectly checking with INFO when the
 * slave turns into a master. */
void sentinelFailoverWaitPromotion(sentinelRedisInstance *ri) {
    /* Just handle the timeout. Switching to the next state is handled
     * by the function parsing the INFO command of the promoted slave. */
    if (mstime() - ri->failover_state_change_time > ri->failover_timeout) {
        sentinelEvent(LL_WARNING, "-failover-abort-slave-timeout", ri, "%@");
        sentinelAbortFailover(ri);
    }
}

/**
 * 检查该master下的所有slave是否都已经完成通知
 * @param master
 */
void sentinelFailoverDetectEnd(sentinelRedisInstance *master) {
    int not_reconfigured = 0, timeout = 0;
    dictIterator *di;
    dictEntry *de;

    // 距离开始通知slave到现在过了多久
    mstime_t elapsed = mstime() - master->failover_state_change_time;

    /* We can't consider failover finished if the promoted slave is
     * not reachable.
     * 如果最合适的slave 已经被清理 应该就是完成故障转移流程结束 或者断开连接 就不需要再处理了
     * */
    if (master->promoted_slave == NULL ||
        master->promoted_slave->flags & SRI_S_DOWN)
        return;

    /* The failover terminates once all the reachable slaves are properly
     * configured. */
    di = dictGetIterator(master->slaves);
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);

        // 下线 或者已经完成通知 的跳过
        if (slave->flags & (SRI_PROMOTED | SRI_RECONF_DONE)) continue;
        if (slave->flags & SRI_S_DOWN) continue;
        not_reconfigured++;
    }
    dictReleaseIterator(di);

    /* Force end of failover on timeout.
     * 此时故障转移处理已经超时
     * */
    if (elapsed > master->failover_timeout) {
        not_reconfigured = 0;
        timeout = 1;
        sentinelEvent(LL_WARNING, "+failover-end-for-timeout", master, "%@");
    }

    // 代表所有slave都已经通知完毕 进入最后的阶段 也就是将本地的master降级成slave
    if (not_reconfigured == 0) {
        sentinelEvent(LL_WARNING, "+failover-end", master, "%@");
        master->failover_state = SENTINEL_FAILOVER_STATE_UPDATE_CONFIG;
        master->failover_state_change_time = mstime();
    }

    /* If I'm the leader it is a good idea to send a best effort SLAVEOF
     * command to all the slaves still not reconfigured to replicate with
     * the new master.
     * 处理超时的情况
     * */
    if (timeout) {
        dictIterator *di;
        dictEntry *de;

        di = dictGetIterator(master->slaves);
        // 这里还是继续发起通知
        while ((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *slave = dictGetVal(de);
            int retval;

            if (slave->flags & (SRI_PROMOTED | SRI_RECONF_DONE | SRI_RECONF_SENT)) continue;
            if (slave->link->disconnected) continue;

            retval = sentinelSendSlaveOf(slave,
                                         master->promoted_slave->addr->ip,
                                         master->promoted_slave->addr->port);
            if (retval == C_OK) {
                sentinelEvent(LL_NOTICE, "+slave-reconf-sent-be", slave, "%@");
                slave->flags |= SRI_RECONF_SENT;
            }
        }
        dictReleaseIterator(di);
    }
}

/* Send SLAVE OF <new master address> to all the remaining slaves that
 * still don't appear to have the configuration updated.
 * 将此时最新的信息通知到所有slave
 * */
void sentinelFailoverReconfNextSlave(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;
    int in_progress = 0;

    di = dictGetIterator(master->slaves);
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);

        // 已经向该slave发送了reconf的请求了
        if (slave->flags & (SRI_RECONF_SENT | SRI_RECONF_INPROG))
            in_progress++;
    }
    dictReleaseIterator(di);

    di = dictGetIterator(master->slaves);
    // 同一时间只允许向一定数量的slave发送通知请求 之后必须等待某些节点响应 才可以继续发送请求
    while (in_progress < master->parallel_syncs &&
           (de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);
        int retval;

        /* Skip the promoted slave, and already configured slaves.
         * 跳过已经处理完成的 以及被推选的slave
         * */
        if (slave->flags & (SRI_PROMOTED | SRI_RECONF_DONE)) continue;

        /* If too much time elapsed without the slave moving forward to
         * the next state, consider it reconfigured even if it is not.
         * Sentinels will detect the slave as misconfigured and fix its
         * configuration later.
         * 针对该节点的reconf已经超时
         * */
        if ((slave->flags & SRI_RECONF_SENT) &&
            (mstime() - slave->slave_reconf_sent_time) >
            SENTINEL_SLAVE_RECONF_TIMEOUT) {
            sentinelEvent(LL_NOTICE, "-slave-reconf-sent-timeout", slave, "%@");
            slave->flags &= ~SRI_RECONF_SENT;
            slave->flags |= SRI_RECONF_DONE;
        }

        /* Nothing to do for instances that are disconnected or already
         * in RECONF_SENT state.
         * 跳过已经发送到slave
         * */
        if (slave->flags & (SRI_RECONF_SENT | SRI_RECONF_INPROG)) continue;
        // 跳过断开连接的节点
        if (slave->link->disconnected) continue;

        /* Send SLAVEOF <new master>.
         * 通知这些slave 此时最新的master
         * */
        retval = sentinelSendSlaveOf(slave,
                                     master->promoted_slave->addr->ip,
                                     master->promoted_slave->addr->port);
        if (retval == C_OK) {
            slave->flags |= SRI_RECONF_SENT;
            slave->slave_reconf_sent_time = mstime();
            sentinelEvent(LL_NOTICE, "+slave-reconf-sent", slave, "%@");
            in_progress++;
        }
    }
    dictReleaseIterator(di);

    /* Check if all the slaves are reconfigured and handle timeout. */
    sentinelFailoverDetectEnd(master);
}

/* This function is called when the slave is in
 * SENTINEL_FAILOVER_STATE_UPDATE_CONFIG state. In this state we need
 * to remove it from the master table and add the promoted slave instead.
 * 将该节点降级成slave 同时将推荐的slave升级成master
 * */
void sentinelFailoverSwitchToPromotedSlave(sentinelRedisInstance *master) {
    sentinelRedisInstance *ref = master->promoted_slave ?
                                 master->promoted_slave : master;

    // 发出一个交换master的事件
    sentinelEvent(LL_WARNING, "+switch-master", master, "%s %s %d %s %d",
                  master->name, master->addr->ip, master->addr->port,
                  ref->addr->ip, ref->addr->port);

    // 更改master实例的地址信息
    sentinelResetMasterAndChangeAddress(master, ref->addr->ip, ref->addr->port);
}

/**
 * 根据不同的状态 执行故障转移逻辑  在主循环中会不断推进故障转移流程
 * @param ri
 */
void sentinelFailoverStateMachine(sentinelRedisInstance *ri) {
    // 要求当前节点 必须是master节点 否则无法进行故障转移
    serverAssert(ri->flags & SRI_MASTER);

    // 此时必须打上 SRI_FAILOVER_IN_PROGRESS 标记
    if (!(ri->flags & SRI_FAILOVER_IN_PROGRESS)) return;

    switch (ri->failover_state) {
        // 等待执行
        case SENTINEL_FAILOVER_STATE_WAIT_START:
            sentinelFailoverWaitStart(ri);
            break;
            // 这里找到最合适的slave节点
        case SENTINEL_FAILOVER_STATE_SELECT_SLAVE:
            sentinelFailoverSelectSlave(ri);
            break;
            // 通知该节点
        case SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE:
            sentinelFailoverSendSlaveOfNoOne(ri);
            break;
            // 等待对端返回结果 这里主要看是否超时 如果超时就要取消本次故障转移
        case SENTINEL_FAILOVER_STATE_WAIT_PROMOTION:
            sentinelFailoverWaitPromotion(ri);
            break;
            // 此时已经收到slave的回复信息了 将最新的信息通知到所有slave
        case SENTINEL_FAILOVER_STATE_RECONF_SLAVES:
            sentinelFailoverReconfNextSlave(ri);
            break;
    }
}

/* Abort a failover in progress:
 *
 * This function can only be called before the promoted slave acknowledged
 * the slave -> master switch. Otherwise the failover can't be aborted and
 * will reach its end (possibly by timeout). *
 * 因为某些原因 导致本次故障转移被终止
 */
void sentinelAbortFailover(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_FAILOVER_IN_PROGRESS);
    serverAssert(ri->failover_state <= SENTINEL_FAILOVER_STATE_WAIT_PROMOTION);

    // 去除故障转移的相关标记
    ri->flags &= ~(SRI_FAILOVER_IN_PROGRESS | SRI_FORCE_FAILOVER);
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = mstime();

    // 因为本次选举的终止，清理交接人
    if (ri->promoted_slave) {
        ri->promoted_slave->flags &= ~SRI_PROMOTED;
        ri->promoted_slave = NULL;
    }
}

/* ======================== SENTINEL timer handler ==========================
 * This is the "main" our Sentinel, being sentinel completely non blocking
 * in design. The function is called every second.
 * -------------------------------------------------------------------------- */

/* Perform scheduled operations for the specified Redis instance.
 * 在serverCron中会执行哨兵的定时任务 处理本节点能观测到的所有实例信息
 * */
void sentinelHandleRedisInstance(sentinelRedisInstance *ri) {
    /* ========== MONITORING HALF ============ */
    /* Every kind of instance */
    // 首先检测是否与本节点断开连接， 并根据需要进行重连  除了普通的连接外 sentinel会与master/slave节点建立订阅发布连接
    sentinelReconnectInstance(ri);
    // 发送一些周期性的命令
    sentinelSendPeriodicCommands(ri);

    /* ============== ACTING HALF ============= */
    /* We don't proceed with the acting half if we are in TILT mode.
     * TILT happens when we find something odd with the time, like a
     * sudden change in the clock.
     * 检测是否在tilt模式下经过了很长时间 是的话退出该模式
     * */
    if (sentinel.tilt) {
        if (mstime() - sentinel.tilt_start_time < SENTINEL_TILT_PERIOD) return;
        sentinel.tilt = 0;
        // 退出该模式也会发送一个事件
        sentinelEvent(LL_WARNING, "-tilt", NULL, "#tilt mode exited");
    }

    /* Every kind of instance
     * 本哨兵主观判断该实例是否已经下线
     * */
    sentinelCheckSubjectivelyDown(ri);

    /* Masters and slaves */
    if (ri->flags & (SRI_MASTER | SRI_SLAVE)) {
        /* Nothing so far. */
    }

    /* Only masters
     * 如果本实例是master
     * */
    if (ri->flags & SRI_MASTER) {
        // 客观检查该节点是否已经下线
        sentinelCheckObjectivelyDown(ri);
        // 判断当前master是否需要故障转移
        if (sentinelStartFailoverIfNeeded(ri))
            // 本节点放弃master节点后 会通知其他sentinel
            sentinelAskMasterStateToOtherSentinels(ri, SENTINEL_ASK_FORCED);
        // 根据此时故障实例的状态 进入故障转移的不同阶段  这里就是开始进行故障转移 会先选择支持故障转移的leader  并将故障转移阶段推进到选择合适的slave
        sentinelFailoverStateMachine(ri);
        // 将该master的状态变更通知到其他sentinel
        sentinelAskMasterStateToOtherSentinels(ri, SENTINEL_NO_FLAGS);
    }
}

/* Perform scheduled operations for all the instances in the dictionary.
 * Recursively call the function against dictionaries of slaves.
 * 通过哨兵处理每个实例对象
 * */
void sentinelHandleDictOfRedisInstances(dict *instances) {
    dictIterator *di;
    dictEntry *de;
    sentinelRedisInstance *switch_to_promoted = NULL;

    /* There are a number of things we need to perform against every master. */
    di = dictGetIterator(instances);
    while ((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        // 处理每个实例
        sentinelHandleRedisInstance(ri);
        // 如果本次处理的是master节点 就会变成递归处理 最终会影响到除了本节点外的其他所有实例
        if (ri->flags & SRI_MASTER) {
            sentinelHandleDictOfRedisInstances(ri->slaves);
            sentinelHandleDictOfRedisInstances(ri->sentinels);
            // 代表整个选举完成 本master会降级成slave
            if (ri->failover_state == SENTINEL_FAILOVER_STATE_UPDATE_CONFIG) {
                switch_to_promoted = ri;
            }
        }
    }
    // 如果本次处理的某个master节点的故障转移完成  将它转换成slave节点
    if (switch_to_promoted)
        sentinelFailoverSwitchToPromotedSlave(switch_to_promoted);
    dictReleaseIterator(di);
}

/* This function checks if we need to enter the TITL mode.
 *
 * The TILT mode is entered if we detect that between two invocations of the
 * timer interrupt, a negative amount of time, or too much time has passed.
 * Note that we expect that more or less just 100 milliseconds will pass
 * if everything is fine. However we'll see a negative number or a
 * difference bigger than SENTINEL_TILT_TRIGGER milliseconds if one of the
 * following conditions happen:
 *
 * 1) The Sentinel process for some time is blocked, for every kind of
 * random reason: the load is huge, the computer was frozen for some time
 * in I/O or alike, the process was stopped by a signal. Everything.
 * 2) The system clock was altered significantly.
 *
 * Under both this conditions we'll see everything as timed out and failing
 * without good reasons. Instead we enter the TILT mode and wait
 * for SENTINEL_TILT_PERIOD to elapse before starting to act again.
 *
 * During TILT time we still collect information, we just do not act.
 * 判断此时条件是否满足tilt(发起尝试请求)
 * */
void sentinelCheckTiltCondition(void) {
    mstime_t now = mstime();
    // 距离上次触发该函数过了多久
    mstime_t delta = now - sentinel.previous_time;

    if (delta < 0 || delta > SENTINEL_TILT_TRIGGER) {
        sentinel.tilt = 1;
        sentinel.tilt_start_time = mstime();
        sentinelEvent(LL_WARNING, "+tilt", NULL, "#tilt mode entered");
    }
    sentinel.previous_time = mstime();
}

/**
 * 在serverCron主循环中 会触发该方法 也是整个哨兵的入口
 */
void sentinelTimer(void) {
    // 检查是否满足tilt条件
    sentinelCheckTiltCondition();
    // 处理所有master节点
    sentinelHandleDictOfRedisInstances(sentinel.masters);
    // 运行待执行的脚本文件
    sentinelRunPendingScripts();
    // 等待脚本执行完毕
    sentinelCollectTerminatedScripts();
    // 清理掉已经超时的脚本
    sentinelKillTimedoutScripts();

    /* We continuously change the frequency of the Redis "timer interrupt"
     * in order to desynchronize every Sentinel from every other.
     * This non-determinism avoids that Sentinels started at the same time
     * exactly continue to stay synchronized asking to be voted at the
     * same time again and again (resulting in nobody likely winning the
     * election because of split brain voting). */
    server.hz = CONFIG_DEFAULT_HZ + rand() % CONFIG_DEFAULT_HZ;
}

