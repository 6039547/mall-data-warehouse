#业务数仓dwd层自动导入数据

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

insert overwrite table "$APP".dwd_order_info partition(dt)
select * from "$APP".ods_order_info 
where dt='$do_date' and id is not null;
 
insert overwrite table "$APP".dwd_order_detail partition(dt)
select * from "$APP".ods_order_detail 
where dt='$do_date' and id is not null;

insert overwrite table "$APP".dwd_user_info partition(dt)
select * from "$APP".ods_user_info
where dt='$do_date' and id is not null;
 
insert overwrite table "$APP".dwd_payment_info partition(dt)
select * from "$APP".ods_payment_info
where dt='$do_date' and id is not null;

insert overwrite table "$APP".dwd_sku_info partition(dt)
select  
    sku.id,
    sku.spu_id,
    sku.price,
    sku.sku_name,
    sku.sku_desc,
    sku.weight,
    sku.tm_id,
    sku.category3_id,
    c2.id category2_id,
    c1.id category1_id,
    c3.name category3_name,
    c2.name category2_name,
    c1.name category1_name,
    sku.create_time,
    sku.dt
from
    "$APP".ods_sku_info sku
join 
    "$APP".ods_base_category3 c3 on sku.category3_id=c3.id 
join 
    "$APP".ods_base_category2 c2 on c3.category2_id=c2.id 
join 
    "$APP".ods_base_category1 c1 on c2.category1_id=c1.id 
where sku.dt='$do_date'  and c2.dt='$do_date'
and c3.dt='$do_date' and c1.dt='$do_date'
and sku.id is not null;
"

$hive -e "$sql"