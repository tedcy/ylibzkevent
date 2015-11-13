#ifndef ZOOKEEPER_HELPER_H
#define ZOOKEEPER_HELPER_H

#include <zookeeper/zookeeper.h>
#include "queue.h"

#define ZOOKEEPER_HELPER_HOST_MAX_LEN 1024

struct ZkEvent;

typedef void (*ZkEventFunc)(struct ZkEvent *zk_event, zhandle_t *zh, const char *path);

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

struct ZkHelperPair;

struct ZkHelperPair
{
    char *key;
    void *value;
    int flag;
    int value_len;
    SLIST_ENTRY(ZkHelperPair) next;
};

enum E_MODE
{
    E_CONNECTION_M,
    E_REGISTER_M
};
    
SLIST_HEAD(ZkHelperPairList,ZkHelperPair);

struct ZookeeperHelper
{
    char host[ZOOKEEPER_HELPER_HOST_MAX_LEN];
    int recv_timeout;
    zhandle_t *zhandle;
    int zk_errno;
    char local_addr[32];
    short local_port;
    struct ZkHelperPairList zoo_path_list;
    struct ZkHelperPairList zoo_event_list;
    int mode;
    int reconnection_flag;
};

struct ZookeeperHelper * CreateZookeeperHelper();
int DestoryZookeeperHelper(struct ZookeeperHelper *zk_helper);

int RegisterToZookeeper(struct ZookeeperHelper *zk_helper, \
        const char* host, int recv_timeout);
int AddTmpNode(struct ZookeeperHelper *zk_helper, const char *path, const char *value);
int AddPersistentNode(struct ZookeeperHelper *zk_helper, const char *path, const char *value);
int AddZookeeperEvent(struct ZookeeperHelper *zk_helper, \
        int event, const char *path, struct ZkEvent *handle);
int GetChildren(zhandle_t *zh, \
        const char* path, struct String_vector *node_vector);

#endif
