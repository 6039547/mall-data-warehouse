#GMV分析

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
insert overwrite table "$APP".ads_gmv_sum_day
select
    '$do_date' dt,
    sum(order_count) gmv_count,
    sum(order_amount) gmv_amount,
    sum(payment_amount) gmv_payment
from
    "$APP".dws_user_action
where
    dt='$do_date'
group by dt;
"

$hive -e "$sql"
