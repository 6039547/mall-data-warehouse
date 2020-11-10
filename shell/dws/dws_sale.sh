#业务数仓dws层用户购买商品明细宽表自动导入数据

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

set hive.exec.dynamic.partition.mode=nonstrict;

with
tmp_detail as
(
    select 
        user_id,
        sku_id, 
        sum(sku_num) sku_num,   
        count(*) order_count, 
        sum(od.order_price*sku_num)  order_amount
    from "$APP".dwd_order_detail od
    where od.dt='$do_date'
    group by user_id, sku_id
)  
insert overwrite table "$APP".dws_sale_detail_daycount partition(dt='$do_date')
select 
    tmp_detail.user_id,
    tmp_detail.sku_id,
    u.gender,
    months_between('$do_date', u.birthday)/12  age, 
    u.user_level,
    price,
    sku_name,
    tm_id,
    category3_id,
    category2_id,
    category1_id,
    category3_name,
    category2_name,
    category1_name,
    spu_id,
    tmp_detail.sku_num,
    tmp_detail.order_count,
    tmp_detail.order_amount 
from tmp_detail 
left join "$APP".dwd_user_info u 
on tmp_detail.user_id=u.id and u.dt='$do_date'
left join "$APP".dwd_sku_info s on tmp_detail.sku_id =s.id  and s.dt='$do_date';

"
$hive -e "$sql"