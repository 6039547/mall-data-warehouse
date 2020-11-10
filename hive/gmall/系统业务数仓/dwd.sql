--创建dwd层订单表
drop table if exists dwd_order_info;
create external table dwd_order_info (
    `id` string COMMENT '',
    `total_amount` decimal(10,2) COMMENT '',
    `order_status` string COMMENT ' 1 2 3 4 5',
    `user_id` string COMMENT 'id',
    `payment_way` string COMMENT '',
    `out_trade_no` string COMMENT '',
    `create_time` string COMMENT '',
    `operate_time` string COMMENT ''
) 
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_order_info/'
tblproperties ("parquet.compression"="snappy");

--创建dwd层订单详情表
drop table if exists dwd_order_detail;
create external table dwd_order_detail( 
    `id` string COMMENT '',
    `order_id` decimal(10,2) COMMENT '', 
    `user_id` string COMMENT 'id',
    `sku_id` string COMMENT 'id',
    `sku_name` string COMMENT '',
    `order_price` string COMMENT '',
    `sku_num` string COMMENT '',
    `create_time` string COMMENT ''
)
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_order_detail/'
tblproperties ("parquet.compression"="snappy");

--创建dwd层用户表
drop table if exists dwd_user_info;
create external table dwd_user_info( 
    `id` string COMMENT 'id',
    `name` string COMMENT '', 
    `birthday` string COMMENT '',
    `gender` string COMMENT '',
    `email` string COMMENT '',
    `user_level` string COMMENT '',
    `create_time` string COMMENT ''
) 
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_user_info/'
tblproperties ("parquet.compression"="snappy");

--创建dwd层支付流水表
drop table if exists dwd_payment_info;
create external table dwd_payment_info(
    `id` bigint COMMENT '',
    `out_trade_no` string COMMENT '',
    `order_id`string COMMENT '',
    `user_id` string COMMENT '',
    `alipay_trade_no` string COMMENT '',
    `total_amount` decimal(16,2) COMMENT '',
    `subject` string COMMENT '',
    `payment_type` string COMMENT '',
    `payment_time` string COMMENT ''
)  
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_payment_info/'
tblproperties ("parquet.compression"="snappy");

--创建dwd层商品表
drop table if exists dwd_sku_info;
create external table dwd_sku_info(
    `id` string COMMENT 'skuId',
    `spu_id` string COMMENT 'spuid',
    `price` decimal(10,2) COMMENT '',
    `sku_name` string COMMENT '',
    `sku_desc` string COMMENT '',
    `weight` string COMMENT '',
    `tm_id` string COMMENT 'id',
    `category3_id` string COMMENT '1id',
    `category2_id` string COMMENT '2id',
    `category1_id` string COMMENT '3id',
    `category3_name` string COMMENT '3',
    `category2_name` string COMMENT '2',
    `category1_name` string COMMENT '1',
    `create_time` string COMMENT ''
) 
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_sku_info/'
tblproperties ("parquet.compression"="snappy");

--创建订单拉链表dwd_order_info_his
drop table if exists dwd_order_info_his;
create external table dwd_order_info_his(
    `id` string COMMENT '订单编号',
    `total_amount` decimal(10,2) COMMENT '订单金额',
    `order_status` string COMMENT '订单状态',
    `user_id` string COMMENT '用户id' ,
    `payment_way` string COMMENT '支付方式',
    `out_trade_no` string COMMENT '支付流水号',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `start_date`  string COMMENT '有效开始日期',
    `end_date`  string COMMENT '有效结束日期'
) COMMENT '订单拉链表'
stored as parquet
location '/warehouse/gmall/dwd/dwd_order_info_his/'
tblproperties ("parquet.compression"="snappy");

--向订单拉链表dwd_order_info_his插入数据
insert overwrite table dwd_order_info_his
select
    id,
    total_amount,
    order_status,
    user_id,
    payment_way,
    out_trade_no,
    create_time,
    operate_time,
    '2020-09-28',
    '9999-99-99'
from ods_order_info oi
where oi.dt='2020-09-28';

--查询数据是否正常插入
select * from dwd_order_info_his limit 2;

--创建订单临时表dwd_order_info_his_tmp
--临时表主要为拉链表服务，用于新增数据、变动数据与连链表源数据的合并
drop table if exists dwd_order_info_his_tmp;
create table dwd_order_info_his_tmp( 
    `id` string COMMENT '订单编号',
    `total_amount` decimal(10,2) COMMENT '订单金额', 
    `order_status` string COMMENT '订单状态',
    `user_id` string COMMENT '用户id',
    `payment_way` string COMMENT '支付方式',
    `out_trade_no` string COMMENT '支付流水号',
    `create_time` string COMMENT '创建时间',
    `operate_time` string COMMENT '操作时间',
    `start_date`  string COMMENT '有效开始日期',
    `end_date`  string COMMENT '有效结束日期'
) COMMENT '订单拉链临时表'
stored as parquet
location '/warehouse/gmall/dwd/dwd_order_info_his_tmp/'
tblproperties ("parquet.compression"="snappy");

--向临时表dwd_order_info_his_tmp插入数据
insert overwrite table dwd_order_info_his_tmp
select
    *
from
    (
        select 
            t1.id,
            t1.total_amount,
            t1.order_status,
            t1.user_id,
            t1.payment_way,
            t1.out_trade_no,
            t1.create_time,
            t1.operate_time,
            t1.start_date,
            if(t2.id is null,t1.end_date,date_add('2020-09-29',-1)) end_date
        from
            dwd_order_info_his t1 left join ods_order_info t2 on t1.id = t2.id and t2.dt = '2020-09-29' and t1.end_date = '9999-99-99'
        union all
        select 
            id,
            total_amount,
            order_status,
            user_id,
            payment_way,
            out_trade_no,
            create_time,
            operate_time,
            '2020-09-29' start_date,
            '9999-99-99' end_date
        from
            ods_order_info
        where
            dt="2020-09-29"
    ) his
order by
    his.id,his.start_date;

--覆盖拉链表dwd_order_info_his数据
insert overwrite table dwd_order_info_his 
select * from dwd_order_info_his_tmp;