#启动hive metastore服务脚本
#! /bin/bash

case $1 in
"start"){
        for i in RuHuTian
        do
                echo " --------启动 $i hive-------"
                #启动hive metastore服务
                ssh $i "hive --service metastore & 2>&1 >/dev/null"
        done
};;
"stop"){
        for i in RuHuTian
        do
                echo " --------停止 $i hive-------"
                ssh $i "ps -ef | grep metastore | grep HiveMetaStore |awk '{print \$2}' | xargs kill"
        done
};;
esac
