# ADS层加载流失用户数据

#!/bin/bash

# 定义变量方便修改
APP=gmall
hive=/opt/apps/hive/bin/hive

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
	do_date=$1
else 
	do_date=`date -d "-1 day" +%F`  
fi 


sql="
insert overwrite table "$APP".ads_wastage_count
select 
    '$do_date',
    count(*)
from
    (
        select
            mid_id
        from
            "$APP".dws_uv_detail_day
        group by
            mid_id
        having
            max(dt)<=date_add('$do_date',-7)
    ) t1;
"

$hive -e "$sql"