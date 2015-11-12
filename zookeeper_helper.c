#include <sys/select.h>
#include <errno.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "zookeeper_helper.h"
#include "logger.h"

static int CreateNode(struct ZookeeperHelper *zk_helper, \
        char *path, const char *value, const int flag);
static int RecursiveCreateNode(struct ZookeeperHelper *zk_helper, \
        const char *path,const char *value, const int flag);
static void watcher(zhandle_t *zh, int type, int state, \
        const char *path, void *watcherCtx);
static void zoo_sleep(unsigned int nmsecs);
static int get_local_addr();
static void ReConnect(struct ZookeeperHelper *zk_helper);

const int CREATED_EVENT = 1 << 1;
const int DELETED_EVENT = 1 << 2;
const int CHANGED_EVENT = 1 << 3;
const int CHILD_EVENT   = 1 << 4;

static const char* state2Str(int state)
{
    if (state == 0) 
        return "CLOSED_STATE";
    if (state == ZOO_CONNECTING_STATE)
        return "CONNECTING_STATE";
    if (state == ZOO_ASSOCIATING_STATE)
        return "ASSOCIATING_STATE";
    if (state == ZOO_CONNECTED_STATE)
        return "CONNECTED_STATE";
    if (state == ZOO_EXPIRED_SESSION_STATE)
        return "EXPIRED_SESSION_STATE";
    if (state == ZOO_AUTH_FAILED_STATE)
        return "AUTH_FAILED_STATE";

    return "INVALID_STATE";
}

static const char* type2Str(int type)
{
    if(type == ZOO_CREATED_EVENT)
        return "CREATED_EVENT";
    if(type == ZOO_DELETED_EVENT)
        return "DELETED_EVENT";
    if(type == ZOO_CHANGED_EVENT)
        return "CHANGED_EVENT";
    if(type == ZOO_CHILD_EVENT)
        return "CHILD_EVENT";
    if(type == ZOO_SESSION_EVENT)
        return "SESSION_EVENT";
    if(type == ZOO_NOTWATCHING_EVENT)
        return "NOTWATCHING_EVENT";
    return "INVALID_EVENT";
}

struct ZookeeperHelper * CreateZookeeperHelper()
{
    struct ZookeeperHelper *zk_helper = malloc(sizeof(struct ZookeeperHelper));
    memset(zk_helper,0,sizeof(struct ZookeeperHelper));
    return zk_helper; 
}

int DestoryZookeeperHelper(struct ZookeeperHelper *zk_helper)
{
    zk_helper->mode = E_CONNECTION_M;
    free(zk_helper);
    return 0;
}

int RegisterToZookeeper(struct ZookeeperHelper *zk_helper, \
        const char* host, int recv_timeout)
{
    strncpy(zk_helper->host,host,ZOOKEEPER_HELPER_HOST_MAX_LEN);
    zk_helper->recv_timeout = recv_timeout;
    zk_helper->mode = E_CONNECTION_M;
    
    zk_helper->zhandle = zookeeper_init(zk_helper->host, watcher, recv_timeout, \
            NULL, zk_helper, 0);
    if(zk_helper->zhandle == NULL){
        log_error("zookeeper_init error: %s", strerror(errno));
        zk_helper->zk_errno = errno;
        return -1;
    }

    int timeout = 0;
    while(1)
    {
        if (zoo_state(zk_helper->zhandle) == ZOO_CONNECTED_STATE) {
            break;
        }
        if(timeout >= zk_helper->recv_timeout){
            zookeeper_close(zk_helper->zhandle);
            log_error("connect zookeeper Timeout");
            zk_helper->zk_errno = ZOPERATIONTIMEOUT;
            return -1;
        }
        zoo_sleep(1);
        timeout++;
    }

    if(-1 == get_local_addr(zk_helper)){
        zookeeper_close(zk_helper->zhandle);
        return -1;
    }

    return 0;
}

int AddTmpNode(struct ZookeeperHelper *zk_helper, const char *path, const char *value)
{
    zk_helper->mode = E_REGISTER_M;
    return RecursiveCreateNode(zk_helper, path, value, ZOO_EPHEMERAL);
}

int AddPersistentNode(struct ZookeeperHelper *zk_helper, const char *path, const char *value)
{
    zk_helper->mode = E_REGISTER_M;
    return RecursiveCreateNode(zk_helper, path, value, 0);
}

static int RecursiveCreateNode(struct ZookeeperHelper *zk_helper, \
        const char *path,const char *value, const int flag)
{
    if(!path || *path=='\0' || path[0] != '/')
    {
        log_error("Invalid Argument");
        return -1;
    }
    char strPath[256];
    snprintf(strPath, sizeof(strPath), "%s", path);

    char *substr_pos = 0;
    substr_pos = strPath;
    while(1)
    {
        // "/abc/def/gl/" if *substr_pos = '/' and *(substr_pos + 1) = 0
        // need to break because substr_pos touch the end
        if(*(substr_pos + 1) == '\0')
            break;
        substr_pos = strchr(substr_pos + 1, '/');
        // not find, break
        if(substr_pos == NULL)
            break;
        *substr_pos = '\0';
        int res = zoo_exists(zk_helper->zhandle, strPath, 0, NULL);
        if(res != ZOK) {
            if(res == ZNONODE){
                res = zoo_create(zk_helper->zhandle, strPath, " ", 1, \
                        &ZOO_OPEN_ACL_UNSAFE, 0, NULL, 0);
                if(res != ZOK) {
                    zk_helper->zk_errno = res;
                    log_error("create node %s error: %s(%d)", strPath, zerror(res), res);
                }
            }
            else {
                zk_helper->zk_errno = res;
                log_error("check node %s error: %s(%d)", strPath, zerror(res), res);
            }
        }
        *substr_pos = '/';
    }
    return CreateNode(zk_helper, strPath, value, flag);
}

static int CreateNode(struct ZookeeperHelper *zk_helper, \
        char *path, const char *value, const int flag)
{
    int res = zoo_exists(zk_helper->zhandle, path, 0, NULL);
    if(res == ZOK)  //节点存在
    {
        res = zoo_delete(zk_helper->zhandle, path, -1);
        if(res != ZOK)
        {
            zk_helper->zk_errno = res;
            log_error("Delete path %s error: %s", path, zerror(res));
            return -1;
        }
        log_info("Delete path %s success, Create it...", path);
        res = zoo_create(zk_helper->zhandle, path, value, strlen(value), \
                &ZOO_OPEN_ACL_UNSAFE, flag, NULL, 0);

    }
    else if(res == ZNONODE)  //节点不存在
    {
        res = zoo_create(zk_helper->zhandle, path, value, strlen(value), \
                &ZOO_OPEN_ACL_UNSAFE, flag, NULL, 0);
    }
    else
    {
        zk_helper->zk_errno = res;
        log_error("Check node exists error: %s(%d)", zerror(res), res);
        return -1;
    }

    if(res != ZOK)
    {
        zk_helper->zk_errno = res;
        log_error("create node %s flag %d error: %s", path, flag, zerror(res));
        return -1;
    }
    
    struct ZkString *p;
    int find = 0;
    SLIST_FOREACH(p, &zk_helper->path_value, next)
    {
        //should be strncmp in the future
        if(strcmp(path, p->key) == 0) {
            find = 1;
            break;
        }
    }
    if(find == 1){
        //update_value(p, value);
        if(p->value_len < strlen(value) + 1){
            p->value_len = strlen(value) + 1;
            p->value = realloc(p->value, p->value_len);
        }
        strcpy(p->value, value);
    }
    else {
        //create_value(&p, value);
        p = malloc(sizeof(struct ZkString));
        p->key = malloc(strlen(path) + 1);
        strcpy(p->key, path);
        p->value = malloc(strlen(value) + 1);
        strcpy(p->value, value);
        //printf("1%s,%s\n",p->key, p->value);

        SLIST_INSERT_HEAD(&zk_helper->path_value, p, next);
    }

    return 0;
}

static void ReConnect(struct ZookeeperHelper *zk_helper)
{
    if(zk_helper->zhandle) {
        zookeeper_close(zk_helper->zhandle);
    }
    zk_helper->zhandle = zookeeper_init(zk_helper->host, watcher, 
            zk_helper->recv_timeout, NULL, zk_helper, 0);
    if(zk_helper->zhandle)
    {
        log_error("retry connect zookeeper error: %s", strerror(errno));
    }
    zk_helper->reconnection_flag = 1; 
}

static void watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
{
    struct ZookeeperHelper *zk_helper = (struct ZookeeperHelper *)watcherCtx;
    log_info("Watcher %s(%d) state = %s(%d) for path = %s", type2Str(type), type, \
            state2Str(state), state, ((path && strlen(path)>0) ? path : ""));
    if(type == ZOO_SESSION_EVENT)
    {
        if (state == ZOO_CONNECTED_STATE)
        {
            log_info("connected zookeeper");
            if(zk_helper->mode == E_REGISTER_M && zk_helper->reconnection_flag)
            {
                //只有在SESSION EXPIRED事件导致的应用重连才创建临时节点
                struct ZkString *p;
                SLIST_FOREACH(p, &zk_helper->path_value, next)
                {
                    //printf("2%s,%s\n",p->key, p->value);
                    CreateNode(zk_helper, p->key, p->value, ZOO_EPHEMERAL); 
                }
                zk_helper->reconnection_flag = 0;
            }
            //ReSetEvent();     //重新设置观察点
        }
        else if(state == ZOO_AUTH_FAILED_STATE)
        {
            log_error("Authentication failure. Shutting down...");
            zookeeper_close(zk_helper->zhandle);
        }
        else if(state == ZOO_EXPIRED_SESSION_STATE)
        {
            log_error("Session expired. Shutting down...");
            //zookeeper_close(zk_helper->zhandle);
            //自动重连
            ReConnect(zk_helper);
        }
    }
    else 
    {
    }
}

static void zoo_sleep(unsigned int nmsecs)
{
    struct timeval tval;
    unsigned long nusecs = nmsecs*1000;
    tval.tv_sec=nusecs/1000000;
    tval.tv_usec=nusecs%1000000;
    select(0, NULL, NULL, NULL, &tval );
}

static int get_local_addr(struct ZookeeperHelper *zk_helper)
{
    int fd = 0;
    int interest;
    struct timeval tv;

    int res = zookeeper_interest(zk_helper->zhandle, &fd, &interest, &tv);
    if(res != ZOK){
        log_error("get myself ip and port error %s(%d)", zerror(res), res);
        return -1;
    }

    struct sockaddr_in addr_;
    socklen_t addr_len = sizeof(addr_);

    if(-1 == getsockname(fd, (struct sockaddr*)&addr_, &addr_len)){
        log_error("getsockname error %s(fd=%d)", strerror(errno), fd);
        return -1;
    }
    char ip_addr[32];
    if(!inet_ntop(AF_INET, &addr_.sin_addr, ip_addr, sizeof(ip_addr))){
        log_error("inet_ntop error %s", strerror(errno));
        return -1;
    }
    strncpy(zk_helper->local_addr,ip_addr,32);
    zk_helper->local_port = ntohs(addr_.sin_port);
    return 0;
}
