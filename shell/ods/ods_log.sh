
#行为数仓ods层自动导入hdfs数据

#!/bin/bash
database=gmall
hive=/opt/apps/hive/bin/hive

if [ -n "$1" ] ;then
    do_date=$1
else
    do_date=`date -d "-1 day" +%f`
fi

sql="
use "$database";

load data inpath '/origin_data/gmall/log/topic_start/$do_date' 
into table "$database".ods_start_log partition(dt='$do_date');

load data inpath '/origin_data/gmall/log/topic_event/$do_date' 
into table "$database".ods_event_log partition(dt='$do_date');
"

$hive -e "$sql"