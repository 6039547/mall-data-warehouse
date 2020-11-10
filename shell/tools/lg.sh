#生成日志文件
#! /bin/bash
        for i in RuHuTian worker1
        do
                ssh $i "java -classpath /opt/software/log-collector-1.0-SNAPSHOT-jar-with-dependencies.jar com.atguigu.appclient.AppMain $1 $2 >/opt/apps/test.log &"
        done
