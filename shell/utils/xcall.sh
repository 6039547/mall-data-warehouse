#集群执行统一命令脚本

#! /bin/bash
for i in RuHuTian worker1 worker2
do
        echo --------- $i ----------
        ssh $i "$*"
done
