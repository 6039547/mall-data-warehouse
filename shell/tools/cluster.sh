#群起用户行为数据采集架构脚本

#! /bin/bash

case $1 in
"start"){
        echo " -------- 启动 集群 -------"

        echo " -------- 启动 hadoop集群 -------"
        /opt/apps/hadoop/sbin/start-dfs.sh
        ssh worker1 "/opt/apps/hadoop/sbin/start-yarn.sh"

        #启动 Zookeeper集群
        zk.sh start

sleep 10s;

        #启动 Flume采集集群
        f1.sh start

        #启动 Kafka采集集群
        kf.sh start

sleep 15s;

        #启动 Flume消费集群
        f2.sh start

        #启动 KafkaManager(可选)
        #km.sh start
};;
"stop"){
    echo " -------- 停止 集群 -------"

        #停止 KafkaManager
        km.sh stop

    #停止 Flume消费集群
        f2.sh stop

        #停止 Kafka采集集群
        kf.sh stop

    sleep 15s;

        #停止 Flume采集集群
        f1.sh stop

        #停止 Zookeeper集群
        zk.sh stop

        echo " -------- 停止 hadoop集群 -------"
        ssh worker1 "/opt/apps/hadoop/sbin/stop-yarn.sh"
        /opt/apps/hadoop/sbin/stop-dfs.sh
};;
esac
