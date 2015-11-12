#include <stdio.h>
#include <unistd.h>
#include "zookeeper_helper.h"

int main()
{
    struct ZookeeperHelper *zk_helper;

    zk_helper = CreateZookeeperHelper();
    RegisterToZookeeper(zk_helper, "172.16.200.239:2181", 6000);
    AddTmpNode(zk_helper, "/fuck/yuanyuanming/abc", "fuckfuckfuck");
    sleep(10000);
    DestoryZookeeperHelper(zk_helper);
    return 0;
}
