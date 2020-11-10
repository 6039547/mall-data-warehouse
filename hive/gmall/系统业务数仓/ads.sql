--创建ads层GMV分析表
drop table if exists ads_gmv_sum_day;
create external table ads_gmv_sum_day(
    `dt` string COMMENT '统计日期',
    `gmv_count`  bigint COMMENT '当日gmv订单个数',
    `gmv_amount`  decimal(16,2) COMMENT '当日gmv订单总金额',
    `gmv_payment`  decimal(16,2) COMMENT '当日支付金额'
) COMMENT 'GMV'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_gmv_sum_day/';

--向ads层GMV分析表导入数据
insert overwrite table ads_gmv_sum_day
select
    '2020-09-25' dt,
    sum(order_count) gmv_count,
    sum(order_amount) gmv_amount,
    sum(payment_amount) gmv_payment
from
    dws_user_action
where
    dt='2020-09-25'
group by dt;

--查询数据是否导入成功
select * from ads_gmv_sum_day;

--创建ads层新增用户转化率表
--转化率=指定指标数/日活数
--例子:新增用户转化率=新增用户输/日活数
drop table if exists ads_user_convert_day;
create external table ads_user_convert_day( 
    `dt` string COMMENT '统计日期',
    `uv_m_count` bigint COMMENT '当日活跃设备',
    `new_m_count`bigint COMMENT '当日新增设备',
    `new_m_ratio` decimal(10,2) COMMENT '当日新增占日活的比率'
) COMMENT '转化率'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_convert_day/';

--向ads层GMV分析表导入数据
insert overwrite table ads_user_convert_day
select
    '2020-09-25' dt,
    uv.day_count uv_m_count,
    new.new_mid_count new_m_count,
    cast(new.new_mid_count/uv.day_count new_m_ratio as decimal(10,2))
from
    ads_uv_count uv join ads_new_mid_count new on uv.dt=new.create_date
where
    uv.dt='2020-09-25';

--查询数据是否导入成功
select * from ads_user_convert_day;

--创建ads层漏斗分析表
drop table if exists ads_user_action_convert_day;
create external  table ads_user_action_convert_day(
    `dt` string COMMENT '统计日期',
    `total_visitor_m_count` bigint COMMENT '总访问人数',
    `order_u_count` bigint COMMENT '下单人数',
    `visitor2order_convert_ratio` decimal(10,2) COMMENT '访问到下单转化率',
    `payment_u_count` bigint COMMENT '支付人数',
    `order2payment_convert_ratio` decimal(10,2) COMMENT '下单到支付的转化率'
 ) COMMENT '用户行为漏斗分析'
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_action_convert_day/';

--向ads层漏斗分析表导入数据
insert overwrite table ads_user_action_convert_day
select
    '2020-09-25' dt,
    uv.day_count total_visitor_m_count,
    ua.order_u order_u_count,
    cast(ua.order_u/uv.day_count as decimal(10,2)) visitor2order_convert_ratio,
    ua.payment_u payment_u_count,
    cast(ua.payment_u/uv.day_count as decimal(10,2)) order2payment_convert_ratio
from
    (
        select
            dt,
            sum(if(order_count>0,1,0)) order_u,
            sum(if(payment_count>0,1,0)) payment_u
        from
            dws_user_action
        where
            dt='2020-09-25'
        group by dt
    ) ua join ads_uv_count uv on ua.dt=uv.dt;

--查询数据是否导入成功
select * from ads_user_action_convert_day;

--创建ads层品牌复购率表
drop table ads_sale_tm_category1_stat_mn;
create external table ads_sale_tm_category1_stat_mn
(   
    tm_id string comment '品牌id',
    category1_id string comment '1级品类id ',
    category1_name string comment '1级品类名称 ',
    buycount   bigint comment  '购买人数',
    buy_twice_last bigint  comment '两次以上购买人数',
    buy_twice_last_ratio decimal(10,2)  comment  '单次复购率',
    buy_3times_last   bigint comment   '三次以上购买人数',
    buy_3times_last_ratio decimal(10,2)  comment  '多次复购率',
    stat_mn string comment '统计月份',
    stat_date string comment '统计日期' 
)   COMMENT '复购率统计'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_sale_tm_category1_stat_mn/';

--向ads层品牌复购率表导入数据
insert into table ads_sale_tm_category1_stat_mn
select   
    mn.sku_tm_id,
    mn.sku_category1_id,
    mn.sku_category1_name,
    sum(if(mn.order_count>=1,1,0)) buycount,
    sum(if(mn.order_count>=2,1,0)) buyTwiceLast,
    sum(if(mn.order_count>=2,1,0))/sum(if(mn.order_count>=1,1,0)) buyTwiceLastRatio,
    sum(if(mn.order_count>=3,1,0))  buy3timeLast  ,
    sum(if(mn.order_count>=3,1,0))/sum(if(mn.order_count>=1,1,0)) buy3timeLastRatio ,
    date_format('2020-09-25' ,'yyyy-MM') stat_mn,
    '2020-09-25' stat_date
from 
(
    select 
        user_id, 
        sd.sku_tm_id,
        sd.sku_category1_id,
        sd.sku_category1_name,
        sum(order_count) order_count
    from dws_sale_detail_daycount sd 
    where date_format(dt,'yyyy-MM')=date_format('2020-09-25' ,'yyyy-MM')
    group by user_id, sd.sku_tm_id, sd.sku_category1_id, sd.sku_category1_name
) mn
group by mn.sku_tm_id, mn.sku_category1_id, mn.sku_category1_name;

--查询数据是否导入成功
select * from ads_sale_tm_category1_stat_mn;

