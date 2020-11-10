#业务数仓dws层用户行为宽表自动导入数据

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

with  
tmp_order as
(
    select 
        user_id, 
        sum(oi.total_amount) order_amount, 
        count(*)  order_count
    from "$APP".dwd_order_info  oi
    where date_format(oi.create_time,'yyyy-MM-dd')='$do_date'
    group by user_id
)  ,
tmp_payment as
(
    select 
        user_id, 
        sum(pi.total_amount) payment_amount, 
        count(*) payment_count 
    from "$APP".dwd_payment_info pi 
    where date_format(pi.payment_time,'yyyy-MM-dd')='$do_date'
    group by user_id
),
tmp_comment as
(  
    select  
        user_id, 
        count(*) comment_count
    from "$APP".dwd_comment_log c
    where date_format(c.dt,'yyyy-MM-dd')='$do_date'
    group by user_id 
)

Insert overwrite table "$APP".dws_user_action partition(dt='$do_date')
select 
    user_actions.user_id, 
    sum(user_actions.order_count), 
    sum(user_actions.order_amount),
    sum(user_actions.payment_count), 
    sum(user_actions.payment_amount),
    sum(user_actions.comment_count) 
from
(
    select
        user_id,
        order_count,
        order_amount,
        0 payment_count,
        0 payment_amount,
        0 comment_count
    from tmp_order

    union all
    select
        user_id,
        0,
        0,
        payment_count,
        payment_amount,
        0
    from tmp_payment

    union all
    select
        user_id,
        0,
        0,
        0,
        0,
        comment_count 
    from tmp_comment
 ) user_actions
group by user_id;
"

$hive -e "$sql"
