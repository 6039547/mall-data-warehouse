#业务数仓ods层自动导入hdfs数据

#!/bin/bash
database=gmall
hive=/opt/apps/hive/bin/hive

if [ -n "$1" ] ;then
    do_date=$1
else
    do_date=`date -d "-1 day" +%f`
fi

sql="
load data inpath '/origin_data/$database/db/order_info/$do_date' 
overwrite into table "$database".ods_order_info partition(dt='$do_date');

load data inpath '/origin_data/$database/db/order_detail/$do_date' 
overwrite into table "$database".ods_order_detail partition(dt='$do_date');

load data inpath '/origin_data/$database/db/sku_info/$do_date' 
overwrite into table "$database".ods_sku_info partition(dt='$do_date');

load data inpath '/origin_data/$database/db/user_info/$do_date' 
overwrite into table "$database".ods_user_info partition(dt='$do_date');

load data inpath '/origin_data/$database/db/payment_info/$do_date' 
overwrite into table "$database".ods_payment_info partition(dt='$do_date');

load data inpath '/origin_data/$database/db/base_category1/$do_date' 
overwrite into table "$database".ods_base_category1 partition(dt='$do_date');

load data inpath '/origin_data/$database/db/base_category2/$do_date' 
overwrite into table "$database".ods_base_category2 partition(dt='$do_date');

load data inpath '/origin_data/$database/db/base_category3/$do_date' 
overwrite into table "$database".ods_base_category3 partition(dt='$do_date'); 
"

$hive -e "$sql"