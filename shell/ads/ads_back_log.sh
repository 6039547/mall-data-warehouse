#回流用户

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
insert overwrite table "$APP".ads_back_count
select 
    '$do_date',
    concat(date_add(next_day('$do_date','mo'),-7),'_',date_add(next_day('$do_date','mo'),-1)),
    count(*)
from
    (
        select 
            t1.mid_id
        from
            (
                select
                    *
                from 
                    "$APP".dws_uv_detail_wk
                where
                    dt=concat(date_add(next_day('$do_date','mo'),-7),'_',date_add(next_day('$do_date','mo'),-1))
            ) t1 left join
            (
                select
                    *
                from 
                    "$APP".dws_uv_detail_wk
                where
                    dt=concat(date_add(next_day('$do_date','mo'),-14),'_',date_add(next_day('$do_date','mo'),-8))
            ) t2 on t1.mid_id = t2.mid_id left join
            (
                select
                    *
                from 
                    "$APP".dws_new_mid_day
                where
                    create_date>=date_add(next_day('$do_date','mo'),-7) and create_date<=date_add(next_day('$do_date','mo'),-1)
            ) t3 on t1.mid_id = t3.mid_id
        where
            t2.mid_id is null and t3.mid_id is null
    ) t1;
"

$hive -e "$sql"