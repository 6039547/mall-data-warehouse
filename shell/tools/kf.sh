#群起kafka脚本
#! /bin/bash

case $1 in
"start"){
        for i in RuHuTian worker1 worker2
        do
                echo " --------启动 $i Kafka-------"
                # 用于KafkaManager监控
                ssh $i "export JMX_PORT=9988 && /opt/apps/kafka/bin/kafka-server-start.sh -daemon /opt/apps/kafka/config/server.properties "
        done
};;
"stop"){
        for i in RuHuTian worker1 worker2
        do
                echo " --------停止 $i Kafka-------"
                ssh $i "/opt/apps/kafka/bin/kafka-server-stop.sh stop"
        done
};;
esac
