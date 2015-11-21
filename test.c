#include <stdio.h>
#include <unistd.h>
#include <memory.h>
#include <signal.h>
#include <sys/epoll.h>
#include "zookeeper_helper.h"
#include "logger.h"

struct ZookeeperHelper *zk_helper;
int if_exit;
int if_exit_for_test;
pthread_t test_id;

void child_event(struct ZkEvent *zk_event, struct ZookeeperHelper *zk_helper, const char *path)
{
    log_info("catch childevent");
    struct String_vector node_vector;
    zoo_helper_get_children(zk_helper, "/fuck/yuanyuanming", &node_vector);
    int i;
    for(i = 0; i < node_vector.count; i++)
    {
        log_info("path %s child: %s", path, node_vector.data[i]);
    }
    deallocate_String_vector(&node_vector);
}

void connected_event(struct ZkEvent *zk_event, struct ZookeeperHelper *zk_helper, const char *path)
{
    log_info("catch connectedevent");
    zk_event->child_event(zk_event, zk_helper, path);
}

void child_event1(struct ZkEvent *zk_event, struct ZookeeperHelper *zk_helper, const char *path)
{
    log_info("catch childevent1");
    struct String_vector node_vector;
    zoo_helper_get_children(zk_helper, "/fuck/yuanyuanming", &node_vector);
    int i;
    for(i = 0; i < node_vector.count; i++)
    {
        log_info("path %s child: %s", path, node_vector.data[i]);
    }
    deallocate_String_vector(&node_vector);
}

void connected_event1(struct ZkEvent *zk_event, struct ZookeeperHelper *zk_helper, const char *path)
{
    log_info("catch connectedevent1");
    zk_event->child_event(zk_event, zk_helper, path);
}

void exiter(int sig)
{
    if(pthread_self() != test_id) {
        log_info("exiter");
        if_exit = 1;
        return;
    }
    log_info("exiter_for_test");
    if_exit_for_test = 1;
}

void test_event(struct ZkEvent *zk_event, struct ZookeeperHelper *zk_helper, const char *path)
{
    struct String_vector node_vector;
    zoo_helper_get_children(zk_helper, "/fuck/yuanyuanming", &node_vector);
    int i;
    for(i = 0; i < node_vector.count; i++) ;
    deallocate_String_vector(&node_vector);
}

void * tester(void *arg)
{
    int fd = epoll_create(1);
    while(!if_exit_for_test)
    {
        test_event(NULL, zk_helper, NULL);
        epoll_wait(fd, NULL, 1,1000);
    }
    return NULL;
}

int main()
{
    signal(SIGINT, exiter);
    log_init(&logger, NULL, LOG_LEVEL_INFO);
    struct ZkEvent zk_event,zk_event1,zk_event2,zk_event3;
    memset(&zk_event, 0, sizeof(struct ZkEvent));
    memset(&zk_event1, 0, sizeof(struct ZkEvent));
    memset(&zk_event2, 0, sizeof(struct ZkEvent));
    memset(&zk_event3, 0, sizeof(struct ZkEvent));
    zk_event.child_event = child_event;
    zk_event.connected_event = connected_event;

    zk_event1.child_event = child_event1;
    zk_event1.connected_event = connected_event1;

    zk_event2.child_event = child_event1;
    zk_event2.connected_event = connected_event1;

    zk_event3.child_event = child_event1;
    zk_event3.connected_event = connected_event1;
    zk_helper = create_zookeeper_helper();
    if(register_to_zookeeper(zk_helper, "172.16.200.239:2181", 6000) < 0) {
        destory_zookeeper_helper(zk_helper);
        return 0;
    }
    add_tmp_node(zk_helper, "/fuck/yuanyuanming/abc", "fuckfuckfuck");
    add_zookeeper_event(zk_helper, CHILD_EVENT ,"/fuck/yuanyuanming", &zk_event);
    add_zookeeper_event(zk_helper, CHANGED_EVENT ,"/fuck/yuanyuanming/abc", &zk_event1);
    add_zookeeper_event(zk_helper, CHANGED_EVENT|CREATED_EVENT ,"/fuck/yuanyuanming/abd", &zk_event2);
    add_zookeeper_event(zk_helper, CHANGED_EVENT ,"/fuck/yuanyuanming/abe", &zk_event3);
    remove_zookeeper_event(zk_helper, "/fuck/yuanyuanming/abe");
    pthread_create(&test_id, NULL, tester, NULL);
    //child_event(NULL, zk_helper->zhandle, NULL);
    while(!if_exit) {
        sleep(1);
    }
    destory_zookeeper_helper(zk_helper);
    zk_helper = NULL;
    pthread_kill(test_id,SIGINT);
    pthread_join(test_id,NULL);

    return 0;
}
