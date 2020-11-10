# 活跃用户数

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
  insert into table "$APP".ads_uv_count
    select
    '$do_date' dt,
    countDay.ct day_count,
    countWk.ct wk_count,
    countMn.ct mn_count,
    if(date_add(next_day('$do_date','MO'),-1)='$do_date','Y','N') is_weekend,
    if(last_day('$do_date')='$do_date','Y','N')
from
        (select count(*) ct from "$APP".dws_uv_detail_day where dt='$do_date') 
        countDay join
        (select count(*) ct from "$APP".dws_uv_detail_wk where dt=concat(date_add(next_day('$do_date','MO'),-7),'_',date_add(next_day('$do_date','MO'),-1))) 
        countWk join
        (select count(*) ct from "$APP".dws_uv_detail_mn where dt=date_format('$do_date','yyyy-MM')) countMn;
"

$hive -e "$sql"