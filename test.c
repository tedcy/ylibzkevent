#include <stdio.h>
#include <unistd.h>
#include <memory.h>
#include "zookeeper_helper.h"
#include "logger.h"

void ChildEvent(struct ZkEvent *zk_event, zhandle_t *zh, const char *path)
{
    log_info("catch childevent");
    struct String_vector node_vector;
    GetChildren(zh, "/fuck/yuanyuanming", &node_vector);
    int i;
    for(i = 0; i < node_vector.count; i++)
    {
        log_info("path %s child: %s", path, node_vector.data[i]);
    }
    deallocate_String_vector(&node_vector);
}

void ConnectedEvent(struct ZkEvent *zk_event, zhandle_t *zh, const char *path)
{
    log_info("catch connectedevent");
    zk_event->ChildEvent(zk_event, zh, path);
}

int main()
{
    log_init(&logger, NULL, LOG_LEVEL_DEBUG);
    struct ZookeeperHelper *zk_helper;
    struct ZkEvent zk_event;
    memset(&zk_event, 0, sizeof(struct ZkEvent));
    zk_event.ChildEvent = ChildEvent;
    zk_event.ConnectedEvent = ConnectedEvent;

    zk_helper = CreateZookeeperHelper();
    RegisterToZookeeper(zk_helper, "172.16.200.239:2181", 6000);
    AddTmpNode(zk_helper, "/fuck/yuanyuanming/abc", "fuckfuckfuck");
    AddZookeeperEvent(zk_helper, CHILD_EVENT ,"/fuck/yuanyuanming", &zk_event);
    sleep(10000);
    DestoryZookeeperHelper(zk_helper);
    return 0;
}
