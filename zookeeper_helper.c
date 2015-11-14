#include <sys/select.h>
#include <errno.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include "zookeeper_helper.h"
#include "logger.h"

static int create_node(struct ZookeeperHelper *zk_helper, \
        char *path, const char *value, const int flag);
static int recursive_create_node(struct ZookeeperHelper *zk_helper, \
        const char *path,const char *value, const int flag);
static void watcher(zhandle_t *zh, int type, int state, \
        const char *path, void *watcherCtx);
static void zoo_sleep(unsigned int nmsecs);
static int get_local_addr();
static void re_set_event(struct ZookeeperHelper *zk_helper);
static void re_connect(struct ZookeeperHelper *zk_helper);
static void handle_event(struct ZkEvent *zk_event, zhandle_t* zh, const char* path);

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

struct ZookeeperHelper * create_zookeeper_helper()
{
    struct ZookeeperHelper *zk_helper = malloc(sizeof(struct ZookeeperHelper));
    memset(zk_helper,0,sizeof(struct ZookeeperHelper));
    return zk_helper; 
}

int destory_zookeeper_helper(struct ZookeeperHelper *zk_helper)
{
    zk_helper->mode = E_CONNECTION_M;
    free(zk_helper);
    return 0;
}

int register_to_zookeeper(struct ZookeeperHelper *zk_helper, \
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

int add_tmp_node(struct ZookeeperHelper *zk_helper, const char *path, const char *value)
{
    zk_helper->mode = E_REGISTER_M;
    return recursive_create_node(zk_helper, path, value, ZOO_EPHEMERAL);
}

int add_persistent_node(struct ZookeeperHelper *zk_helper, const char *path, const char *value)
{
    zk_helper->mode = E_REGISTER_M;
    return recursive_create_node(zk_helper, path, value, 0);
}

static int recursive_create_node(struct ZookeeperHelper *zk_helper, \
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
    return create_node(zk_helper, strPath, value, flag);
}

static int create_node(struct ZookeeperHelper *zk_helper, \
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
    if(flag == 0){
        return 0;
    }
    
    struct ZkHelperPair *p;
    int find = 0;
    SLIST_FOREACH(p, &zk_helper->zoo_path_list, next)
    {
        //should be strncmp in the future
        if(strcmp(path, p->key) == 0) {
            find = 1;
            break;
        }
    }
    if(find == 1){
        //update_value(p, value, valuelen);
        if(p->value_len < strlen(value) + 1){
            p->value_len = strlen(value) + 1;
            p->value = realloc(p->value, p->value_len);
        }
        strcpy(p->value, value);
        p->flag = flag;
    }
    else {
        //create_value(&p, value, valuelen);
        p = malloc(sizeof(struct ZkHelperPair));
        p->key = malloc(strlen(path) + 1);
        strcpy(p->key, path);
        p->value = malloc(strlen(value) + 1);
        strcpy(p->value, value);
        p->value_len = strlen(value) + 1;
        //printf("1%s,%s\n",p->key, p->value);

        p->flag = flag;
        SLIST_INSERT_HEAD(&zk_helper->zoo_path_list, p, next);
    }

    return 0;
}

int add_zookeeper_event(struct ZookeeperHelper *zk_helper, \
        int event, const char *path, struct ZkEvent *handle)
{
    int ret = 0;
    handle->eventmask |= event;

    struct ZkHelperPair *p;
    int find = 0;
    SLIST_FOREACH(p, &zk_helper->zoo_path_list, next)
    {
        //should be strncmp in the future
        if(strcmp(path, p->key) == 0) {
            find = 1;
            ((struct ZkEvent *)p->value)->eventmask = handle->eventmask;
            break;
        }
    }
    if(find == 0){
        p = malloc(sizeof(struct ZkHelperPair));
        p->key = malloc(strlen(path) + 1);
        strcpy(p->key, path);
        p->value = malloc(sizeof(struct ZkEvent));
        memcpy(p->value, handle, sizeof(struct ZkEvent));
        SLIST_INSERT_HEAD(&zk_helper->zoo_event_list, p, next);
    }
    if((event & CREATED_EVENT) || (event & DELETED_EVENT) || (event & CHANGED_EVENT)){
        ret = zoo_exists(zk_helper->zhandle, path, 1, NULL);
        if(ret != ZOK){
            if (ret == ZNONODE) {
                ret = add_tmp_node(zk_helper, path, "1");
            }
            if (ret != ZOK) {
                log_error("set watcher for path %s error %s", path, zerror(ret));
                return -1;
            }
        }
    }
    if(event & CHILD_EVENT){
        ret = zoo_get_children(zk_helper->zhandle, path, 1, NULL);
        if(ret != ZOK){
            if (ret == ZNONODE) {
                ret = add_tmp_node(zk_helper, path, "1");
            }

            if (ret != ZOK) {
                log_error("set watcher for path %s error %s", path, zerror(ret));
                return -1;
            }
        }
    }
    
    return 0;
}

static void re_set_event(struct ZookeeperHelper *zk_helper)
{
    int ret = 0;
    struct ZkHelperPair *p;
    int event;
    char *path;
    struct ZkEvent * zk_event;
    SLIST_FOREACH(p, &zk_helper->zoo_event_list, next)
    {
        zk_event = ((struct ZkEvent *)p->value);
        event = zk_event->eventmask;
        path = p->key;
        if((event & CREATED_EVENT) || (event & DELETED_EVENT) || (event & CHANGED_EVENT)){
            ret = zoo_exists(zk_helper->zhandle, path, 1, NULL);
            if(ret != ZOK){
                if (ret == ZNONODE) {
                    ret = add_tmp_node(zk_helper, path, "1");
                }
                if (ret != ZOK) {
                    log_error("set watcher for path %s error %s", path, zerror(ret));
                    continue;
                }
            }
        }
        if(event & CHILD_EVENT){
            ret = zoo_get_children(zk_helper->zhandle, path, 1, NULL);
            if(ret != ZOK){
                if (ret == ZNONODE) {
                    ret = add_tmp_node(zk_helper, path, "1");
                }
                if (ret != ZOK) {
                    log_error("set watcher for path %s error %s", path, zerror(ret));
                    continue;
                }
            }
        }
        zk_event->connected_event(zk_event, zk_helper->zhandle, path);
    }
}

static void re_connect(struct ZookeeperHelper *zk_helper)
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

static void handle_event(struct ZkEvent *zk_event, zhandle_t* zh, const char* path)
{
    int ret;
    int eventmask = zk_event->eventmask;
    log_debug("path %s eventmask: %d", path, eventmask);
    if(eventmask & CREATED_EVENT)
    {
        //重新设置观察点
        ret = zoo_exists(zh, path, 1, NULL);
        if (ZOK != ret){
            log_error("set watcher [ZOO_CREATED_EVENT] for path %s error %s", path, zerror(ret));
        }
        zk_event->created_event(zk_event, zh, path);
    }
    else if(eventmask & CHANGED_EVENT)
    {
        ret = zoo_exists(zh, path, 1, NULL);
        if(ZOK != ret){
            log_error("set watcher [ZOO_CHANGED_EVENT] for path %s error %s", path, zerror(ret));
        }
        zk_event->changed_event(zk_event, zh, path);
    }
    else if(eventmask & CHILD_EVENT)
    {
        ret = zoo_get_children(zh, path, 1, NULL);
        if(ZOK != ret){
            log_error("set watcher [ZOO_CHILD_EVENT] for path %s error %s", path, zerror(ret));
        }
        zk_event->child_event(zk_event, zh, path);
    }
    else if(eventmask & DELETED_EVENT)
    {
        ret = zoo_exists(zh, path, 1, NULL);
        if( ZOK != ret ){
            log_error("set watcher [ZOO_DELETED_EVENT] for path %s error %s", path, zerror(ret));
        }
        zk_event->deleted_event(zk_event, zh, path);
    }

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
                struct ZkHelperPair *p;
                SLIST_FOREACH(p, &zk_helper->zoo_path_list, next)
                {
                    //printf("2%s,%s\n",p->key, p->value);
                    create_node(zk_helper, p->key, p->value, p->flag); 
                }
                zk_helper->reconnection_flag = 0;
            }
            re_set_event(zk_helper);     //重新设置观察点
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
            re_connect(zk_helper);
        }
    }
    else 
    {
        struct ZkHelperPair *p;
        SLIST_FOREACH(p, &zk_helper->zoo_event_list, next)
        {
            //should be strncmp in the future
            log_debug("get key %s",p->key);
            if(strcmp(path, p->key) == 0) {
                log_debug("catch key %s",p->key);
                handle_event(p->value, zk_helper->zhandle, path);
                break;
            }
        }
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

int get_children(zhandle_t *zh, \
        const char* path, struct String_vector *node_vector)
{
    node_vector->count = 0;
    int res = zoo_get_children(zh, path, 0, node_vector);
    if(res != ZOK)
    {
        log_error("Get %s error: %s(%d)", path, zerror(res), res);
        return -1;
    }
    return 0;
}
