# ADS层加载最近七天连续活跃三天用户数据

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
insert overwrite table "$APP".ads_continuity_uv_count
select 
    '$do_date',
    concat(date_add(next_day('$do_date','mo'),-7-7*2),'_',date_add(next_day('$do_date','mo'),-1)),
    count(*)
from
    (
        select
            mid_id
        from
            (
                select
                    mid_id
                from
                    (
                        select
                            mid_id,
                            date_sub(dt,rank_) date_dif
                        from
                            (
                                select
                                    mid_id,
                                    dt,
                                    rank() over(partition by mid_id order by dt) rank_
                                from
                                    "$APP".dws_uv_detail_day
                                where
                                    dt>=date_add('$do_date',-7) and dt<='$do_date'
                            ) t1
                    ) t2
                group by mid_id,date_dif having count(*)>=3
            ) t3
        group by
            mid_id
    ) t4;
"

$hive -e "$sql"