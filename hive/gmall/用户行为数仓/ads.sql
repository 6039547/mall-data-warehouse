--创建ads层设备活跃表ads_uv_count
drop table if exists ads_uv_count;
create external table ads_uv_count( 
    `dt` string COMMENT '统计日期',
    `day_count` bigint COMMENT '当日用户数量',
    `wk_count`  bigint COMMENT '当周用户数量',
    `mn_count`  bigint COMMENT '当月用户数量',
    `is_weekend` string COMMENT 'Y,N是否是周末,用于得到本周最终结果',
    `is_monthend` string COMMENT 'Y,N是否是月末,用于得到本月最终结果' 
) COMMENT '活跃设备数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_uv_count/';


--向ads层设备活跃表ads_uv_count导入dws层数据
insert into table ads_uv_count
select
    '2020-09-25' dt,
    countDay.ct day_count,
    countWk.ct wk_count,
    countMn.ct mn_count,
    if(date_add(next_day('2020-09-25','MO'),-1)='2020-09-25','Y','N') is_weekend,
    if(last_day('2020-09-25')='2020-09-25','Y','N')
from
        (select count(*) ct from dws_uv_detail_day where dt='2020-09-25') 
        countDay join
        (select count(*) ct from dws_uv_detail_wk where dt=concat(date_add(next_day('2020-09-25','MO'),-7),'_',date_add(next_day('2020-09-25','MO'),-1))) 
        countWk join
        (select count(*) ct from dws_uv_detail_mn where dt=date_format('2020-09-25','yyyy-MM')) countMn 
;


--查询数据是否插入成功
select * from ads_uv_count limit 2;
select count(*) from ads_uv_count;


--创建ads层新增设备数量表ads_new_mid_count
drop table if exists ads_new_mid_count;
create external table ads_new_mid_count
(
    `create_date` string comment '创建时间' ,
    `new_mid_count` BIGINT comment '新增设备数量' 
)  COMMENT '每日新增设备信息数量'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_new_mid_count/';


--向ads层新增设备数量表ads_new_mid_count导入dws层数据
insert into table ads_new_mid_count
select
    create_date,
    count(*) new_mid_count
from
    dws_new_mid_day 
where
    create_date='2020-09-25'
group by 
    create_date;

--查询数据是否插入成功
select * from ads_new_mid_count;


--创建ads层用户留存数量表ads_user_retention_day_count
drop table if exists ads_user_retention_day_count;
create external table ads_user_retention_day_count 
(
    `create_date` string  comment '设备新增日期',
    `retention_day` int comment '截止当前日期留存天数',
    `retention_count` bigint comment  '留存数量'
)  COMMENT '每日用户留存情况'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_retention_day_count/';

--向ads层用户留存数量表ads_user_retention_day_count导入dws层数据
--可以用union all来统计多日留存
insert overwrite table ads_user_retention_day_count
select
    create_date,
    retention_day,
    count(*) retention_count
from
    dws_user_retention_day 
where
    dt='2020-09-27'
group by 
    create_date,retention_day;

--查询数据是否插入成功
select * from ads_user_retention_day_count;


--创建ads层留存用户比率表ads_user_retention_day_rate
drop table if exists ads_user_retention_day_rate;
create external table ads_user_retention_day_rate 
(
    `stat_date` string comment '统计日期',
    `create_date` string  comment '设备新增日期',
    `retention_day` int comment '截止当前日期留存天数',
    `retention_count` bigint comment  '留存数量',
    `new_mid_count` bigint comment '当日设备新增数量',
    `retention_ratio` decimal(10,2) comment '留存率'
)  COMMENT '留存用户比率'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_retention_day_rate/';

--向ads层留存用户比率表ads_user_retention_day_rate
--可以用union all来统计多日留存率
insert overwrite table ads_user_retention_day_rate
select 
    '2020-09-27',
    rc.create_date,
    rc.retention_day,
    rc.retention_count,
    nc.new_mid_count,
    rc.retention_count/nc.new_mid_count
from
     ads_user_retention_day_count rc join ads_new_mid_count nc on rc.create_date = nc.create_date  
where
    rc.create_date='2020-09-25'

--查询数据是否插入成功
select * from ads_user_retention_day_rate;


--创建ads层沉默用户数表ads_slient_count
drop table if exists ads_slient_count;
create external table ads_slient_count( 
    `dt` string COMMENT '统计日期',
    `slient_count` bigint COMMENT '沉默设备数'
) 
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_slient_count';


--向ads层沉默用户数表ads_slient_count导入数据
insert overwrite table ads_slient_count
select 
    '2020-10-06' dt,
    count(*) slient_count
from
    select
        mid_id
    from 
        dws_uv_detail_day
    where
        dt<'2020-10-06'
    group by
        mid_id
    having
        min(dt)<date_add('2020-10-06',-7) and count(*)=1;

--查询数据是否插入成功
select * from ads_slient_count;


--创建ads层回流用户数表ads_back_count
drop table if exists ads_back_count;
create external table ads_back_count( 
    `dt` string COMMENT '统计日期',
    `wk_dt` string COMMENT '统计日期所在周',
    `wastage_count` bigint COMMENT '回流设备数'
) 
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_back_count';


--向ads层回流用户数表ads_back_count导入数据
insert overwrite table ads_back_count
select 
    '2020-10-07',
    concat(date_add(next_day('2020-10-07','mo'),-7),'_',date_add(next_day('2020-10-07','mo'),-1)),
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
                    dws_uv_detail_wk
                where
                    dt=concat(date_add(next_day('2020-10-07','mo'),-7),'_',date_add(next_day('2020-10-07','mo'),-1))
            ) t1 left join
            (
                select
                    *
                from 
                    dws_uv_detail_wk
                where
                    dt=concat(date_add(next_day('2020-10-07','mo'),-14),'_',date_add(next_day('2020-10-07','mo'),-8))
            ) t2 on t1.mid_id = t2.mid_id left join
            (
                select
                    *
                from 
                    dws_new_mid_day
                where
                    create_date>=date_add(next_day('2020-10-07','mo'),-7) and create_date<=date_add(next_day('2020-10-07','mo'),-1)
            ) t3 on t1.mid_id = t3.mid_id
        where
            t2.mid_id is null and t3.mid_id is null
    ) t1;
--查询数据是否插入成功
select * from ads_back_count;

--创建ads层流失用户数表ads_wastage_count
drop table if exists ads_wastage_count;
create external table ads_wastage_count( 
    `dt` string COMMENT '统计日期',
    `wastage_count` bigint COMMENT '流失设备数'
) 
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_wastage_count';


--向ads层流失用户数表ads_wastage_count导入数据
insert overwrite table ads_wastage_count
select 
    '2020-10-06',
    count(*)
from
    (
        select
            mid_id
        from
            dws_uv_detail_day
        group by
            mid_id
        having
            max(dt)<=date_add('2020-10-06',-7)
    ) t1;
    
--查询数据是否插入成功
select * from ads_wastage_count;

--创建ads层连续三周活跃用户数表ads_continuity_wk_count
drop table if exists ads_continuity_wk_count;
create external table ads_continuity_wk_count( 
    `dt` string COMMENT '统计日期,一般用结束周周日日期,如果每天计算一次,可用当天日期',
    `wk_dt` string COMMENT '持续时间',
    `continuity_count` bigint
) 
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_wk_count';

--向ads层连续三周活跃用户数表ads_continuity_wk_countt导入数据
insert overwrite table ads_continuity_wk_count
select 
    '2020-10-06',
    concat(date_add(next_day('2020-10-06','mo'),-7-7*2),'_',date_add(next_day('2020-10-06','mo'),-1)),
    count(*)
from
    (
        select
            mid_id
        from
            dws_uv_detail_wk
        where
            dt>=concat(date_add(next_day('2020-10-06','mo'),-7-7*2),'_',date_add(next_day('2020-10-06','mo'),-1-7*2))
            and
            dt<=concat(date_add(next_day('2020-10-06','mo'),-7),'_',date_add(next_day('2020-10-06','mo'),-1))
        group by 
            mid_id
        having
            count(*)=3
    ) t1;

--查询数据是否插入成功
select * from ads_continuity_wk_count;

--创建ads层最近7天内连续3天活跃用户数表ads_continuity_uv_count
drop table if exists ads_continuity_uv_count;
create external table ads_continuity_uv_count( 
    `dt` string COMMENT '统计日期',
    `wk_dt` string COMMENT '最近7天日期',
    `continuity_count` bigint
) COMMENT '连续活跃设备数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_uv_count';

--向ads层最近7天内连续3天活跃用户数表ads_continuity_uv_count导入数据
insert overwrite table ads_continuity_uv_count
select 
    '2020-10-06',
    concat(date_add(next_day('2020-10-06','mo'),-7-7*2),'_',date_add(next_day('2020-10-06','mo'),-1)),
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
                                    dws_uv_detail_day
                                where
                                    dt>=date_add('2020-10-06',-7) and dt<='2020-10-06'
                            ) t1
                    ) t2
                group by mid_id,date_dif having count(*)>=3
            ) t3
        group by
            mid_id
    ) t4;

--查询数据是否插入成功
select * from ads_continuity_uv_count;

