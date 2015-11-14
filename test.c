#include <stdio.h>
#include <unistd.h>
#include <memory.h>
#include <signal.h>
#include "zookeeper_helper.h"
#include "logger.h"

struct ZookeeperHelper *zk_helper;

void child_event(struct ZkEvent *zk_event, zhandle_t *zh, const char *path)
{
    log_info("catch childevent");
    struct String_vector node_vector;
    get_children(zh, "/fuck/yuanyuanming", &node_vector);
    int i;
    for(i = 0; i < node_vector.count; i++)
    {
        log_info("path %s child: %s", path, node_vector.data[i]);
    }
    deallocate_String_vector(&node_vector);
}

void connected_event(struct ZkEvent *zk_event, zhandle_t *zh, const char *path)
{
    log_info("catch connectedevent");
    zk_event->child_event(zk_event, zh, path);
}

void exiter(int sig)
{
    destory_zookeeper_helper(zk_helper);
    exit(0);
}

int main()
{
    signal(SIGINT, exiter);
    log_init(&logger, NULL, LOG_LEVEL_DEBUG);
    struct ZkEvent zk_event;
    memset(&zk_event, 0, sizeof(struct ZkEvent));
    zk_event.child_event = child_event;
    zk_event.connected_event = connected_event;

    zk_helper = create_zookeeper_helper();
    register_to_zookeeper(zk_helper, "172.16.200.239:2181", 6000);
    add_tmp_node(zk_helper, "/fuck/yuanyuanming/abc", "fuckfuckfuck");
    add_zookeeper_event(zk_helper, CHILD_EVENT ,"/fuck/yuanyuanming", &zk_event);
    sleep(10000);
    return 0;
}
