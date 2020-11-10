--创建ods层启动日志表
drop table if exists ods_start_log;
Create external table ods_start_log (`line` String)
partitioned by (`dt` String)
stored as
inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_start_log';

--向ods层启动日志表导入数据
load data inpath '/origin_data/gmall/log/topic_start/2020-09-25'
into table gmall.ods_start_log partition(dt='2020-09-25');

--查询数据是否正常导入
select * from ods_start_log limit 2;

--创建ods层时间日志表
drop table if exists ods_event_log;
Create external table ods_event_log (`line` String)
partitioned by (`dt` String)
stored as
inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_event_log';

--向ods层事件日志表导入数据
load data inpath '/origin_data/gmall/log/topic_event/2020-09-25'
into table gmall.ods_event_log partition(dt='2020-09-25');

--查询数据是否正常导入
select * from ods_event_log limit 2;

