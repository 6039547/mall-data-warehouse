#群启zookeeper脚本

#! /bin/bash

case $1 in
"start"){
        for i in RuHuTian worker1 worker2
        do
                ssh $i "/opt/apps/zookeeper/bin/zkServer.sh start"
        done
};;
"stop"){
        for i in RuHuTian worker1 worker2
        do
                ssh $i "/opt/apps/zookeeper/bin/zkServer.sh stop"
        done
};;
"status"){
        for i in RuHuTian worker1 worker2
        do
                ssh $i "/opt/apps/zookeeper/bin/zkServer.sh status"
        done
};;
esac
