# 最近三周活跃用户数据

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
insert overwrite table "$APP".ads_continuity_wk_count
select
    '$do_date',
    concat(date_add(next_day('$do_date','mo'),-7-7*2),'_',date_add(next_day('$do_date','mo'),-1)),
    count(*)
from
    (
        select
            mid_id
        from
            "$APP".dws_uv_detail_wk
        where
            dt>=concat(date_add(next_day('$do_date','mo'),-7-7*2),'_',date_add(next_day('$do_date','mo'),-1-7*2))
            and
            dt<=concat(date_add(next_day('$do_date','mo'),-7),'_',date_add(next_day('$do_date','mo'),-1))
        group by
            mid_id
        having
            count(*)=3
    ) t1;
"

$hive -e "$sql"
