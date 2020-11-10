#群起采集flume脚本

#! /bin/bash

case $1 in
"start"){
        for i in RuHuTian worker1
        do
                echo " --------启动 $i 采集flume-------"
                ssh $i "nohup /opt/apps/flume/bin/flume-ng agent --conf-file /opt/apps/flume/conf/file-flume-kafka.conf --name a1 -Dflume.root.logger=INFO,LOGFILE >/opt/apps/flume/producers_log.txt 2>&1 &"
        done
};;
"stop"){
        for i in RuHuTian worker1
        do
                echo " --------停止 $i 采集flume-------"
                ssh $i "ps -ef | grep java | grep flume |awk '{print \$2}' | xargs kill"
        done

};;
esac
