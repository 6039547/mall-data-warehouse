#启动消费flume脚本

#! /bin/bash

case $1 in
"start"){
        for i in worker2
        do
                echo " --------启动 $i 消费flume-------"
                ssh $i "nohup /opt/apps/flume/bin/flume-ng agent --conf-file /opt/apps/flume/conf/kafka-flume-hdfs.conf --name a1 -Dflume.root.logger=INFO,LOGFILE >/opt/apps/flume/Consumers_log.txt   2>&1 &"
        done
};;
"stop"){
        for i in worker2
        do
                echo " --------停止 $i 消费flume-------"
                ssh $i "ps -ef | grep java | grep flume |awk '{print \$2}' | xargs kill"
        done

};;
esac
