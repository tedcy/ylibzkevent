#ifndef ZOOKEEPER_HELPER_H
#define ZOOKEEPER_HELPER_H

#include <zookeeper/zookeeper.h>
#include "queue.h"

#define ZOOKEEPER_HELPER_HOST_MAX_LEN 1024

typedef void (*ZkEventFunc)(zhandle_t *zh,const char *path);

struct ZkEvent
{
    int eventmask;
    ZkEventFunc ConnectedEvent;
    ZkEventFunc ChangedEvent;
    ZkEventFunc ChildEvent;
    ZkEventFunc CreatedEvent;
    ZkEventFunc DeletedEvent;
    ZkEventFunc NotWatchingEvent;
};

extern const int CREATED_EVENT;
extern const int DELETED_EVENT;
extern const int CHANGED_EVENT;
extern const int CHILD_EVENT;

struct ZkString;

struct ZkString
{
    char *key;
    char *value;
    int value_len;
    SLIST_ENTRY(ZkString) next;
};

enum E_MODE
{
    E_CONNECTION_M,
    E_REGISTER_M
};

struct ZookeeperHelper
{
    char host[ZOOKEEPER_HELPER_HOST_MAX_LEN];
    int recv_timeout;
    zhandle_t *zhandle;
    int zk_errno;
    char local_addr[32];
    short local_port;
    SLIST_HEAD(ZkHelperList,ZkString) path_value;
    int mode;
    int reconnection_flag;
};

struct ZookeeperHelper * CreateZookeeperHelper();
int DestoryZookeeperHelper(struct ZookeeperHelper *zk_helper);

int RegisterToZookeeper(struct ZookeeperHelper *zk_helper, \
        const char* host, int recv_timeout);
int AddTmpNode(struct ZookeeperHelper *zk_helper, const char *path, const char *value);
int AddPersistentNode(struct ZookeeperHelper *zk_helper, const char *path, const char *value);

#endif
