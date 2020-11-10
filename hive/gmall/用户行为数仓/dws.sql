--创建dws层每日活跃设备明细表dws_uv_detail_day
drop table if exists dws_uv_detail_day;
create external table dws_uv_detail_day(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识', 
    `version_code` string COMMENT '程序版本号', 
    `version_name` string COMMENT '程序版本名', 
    `lang` string COMMENT '系统语言', 
    `source` string COMMENT '渠道号', 
    `os` string COMMENT '安卓系统版本', 
    `area` string COMMENT '区域', 
    `model` string COMMENT '手机型号', 
    `brand` string COMMENT '手机品牌', 
    `sdk_version` string COMMENT 'sdkVersion', 
    `gmail` string COMMENT 'gmail', 
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度'
)
partitioned by (dt string)
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_day';


--向dws层每日活跃设备明细表dws_uv_detail_day导入dwd层启动日志表dwd_start_log数据
insert overwrite table dws_uv_detail_day
partition(dt='2020-09-25')
select
    mid_id,
    concat_ws('|',collect_set(user_id)) user_id,
    concat_ws('|', collect_set(version_code)) version_code,
    concat_ws('|', collect_set(version_name)) version_name,
    concat_ws('|', collect_set(lang))lang,
    concat_ws('|', collect_set(source)) source,
    concat_ws('|', collect_set(os)) os,
    concat_ws('|', collect_set(area)) area, 
    concat_ws('|', collect_set(model)) model,
    concat_ws('|', collect_set(brand)) brand,
    concat_ws('|', collect_set(sdk_version)) sdk_version,
    concat_ws('|', collect_set(gmail)) gmail,
    concat_ws('|', collect_set(height_width)) height_width,
    concat_ws('|', collect_set(app_time)) app_time,
    concat_ws('|', collect_set(network)) network,
    concat_ws('|', collect_set(lng)) lng,
    concat_ws('|', collect_set(lat)) lat
from
    dwd_start_log
where
    dt='2020-09-25'
group by
    mid_id;

--查询数据是否插入成功
select * from dws_uv_detail_day limit 2;
select count(*) from dws_uv_detail_day;


--创建dws层每周活跃设备明细表dws_uv_detail_wk
drop table if exists dws_uv_detail_wk;
create external table dws_uv_detail_wk(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识', 
    `version_code` string COMMENT '程序版本号', 
    `version_name` string COMMENT '程序版本名', 
    `lang` string COMMENT '系统语言', 
    `source` string COMMENT '渠道号', 
    `os` string COMMENT '安卓系统版本', 
    `area` string COMMENT '区域', 
    `model` string COMMENT '手机型号', 
    `brand` string COMMENT '手机品牌', 
    `sdk_version` string COMMENT 'sdkVersion', 
    `gmail` string COMMENT 'gmail', 
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `monday_date` string COMMENT '周一日期',
    `sunday_date` string COMMENT  '周日日期' 
) COMMENT '活跃设备按周明细'
partitioned by (dt string)
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_wk';


--向dws层每周活跃设备明细表dws_uv_detail_wk导入dws层日活表dws_uv_detail_day数据
--需要先开启非严格模式，否则不允许使用动态分区
set hive.exec.dynamic.partition.mode=nonstrict

insert overwrite table dws_uv_detail_wk
partition(dt)
select
    mid_id,
    concat_ws('|',collect_set(user_id)) user_id,
    concat_ws('|', collect_set(version_code)) version_code,
    concat_ws('|', collect_set(version_name)) version_name,
    concat_ws('|', collect_set(lang))lang,
    concat_ws('|', collect_set(source)) source,
    concat_ws('|', collect_set(os)) os,
    concat_ws('|', collect_set(area)) area, 
    concat_ws('|', collect_set(model)) model,
    concat_ws('|', collect_set(brand)) brand,
    concat_ws('|', collect_set(sdk_version)) sdk_version,
    concat_ws('|', collect_set(gmail)) gmail,
    concat_ws('|', collect_set(height_width)) height_width,
    concat_ws('|', collect_set(app_time)) app_time,
    concat_ws('|', collect_set(network)) network,
    concat_ws('|', collect_set(lng)) lng,
    concat_ws('|', collect_set(lat)) lat,
    date_add(next_day('2020-09-25','MO'),-7) monday_date,
    date_add(next_day('2020-09-25','MO'),-1) sunday_date,
    concat(date_add(next_day('2020-09-25','MO'),-7),'_',date_add(next_day('2020-09-25','MO'),-1))
from
    dws_uv_detail_day
where
    dt>=date_add(next_day('2020-09-25','MO'),-7) and dt<=date_add(next_day('2020-09-25','MO'),-1)
group by
    mid_id;

--查询数据是否插入成功
select * from dws_uv_detail_wk limit 2;
select count(*) from dws_uv_detail_wk;

--创建dws层每月活跃设备明细表dws_uv_detail_mn
drop table if exists dws_uv_detail_mn;
create external table dws_uv_detail_mn(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识', 
    `version_code` string COMMENT '程序版本号', 
    `version_name` string COMMENT '程序版本名', 
    `lang` string COMMENT '系统语言', 
    `source` string COMMENT '渠道号', 
    `os` string COMMENT '安卓系统版本', 
    `area` string COMMENT '区域', 
    `model` string COMMENT '手机型号', 
    `brand` string COMMENT '手机品牌', 
    `sdk_version` string COMMENT 'sdkVersion', 
    `gmail` string COMMENT 'gmail', 
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度'
) COMMENT '活跃设备按月明细'
partitioned by (dt string)
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_mn';


--向dws层每月活跃设备明细表dws_uv_detail_mn导入dws日活表dws_uv_detail_day数据
--需要先开启非严格模式，否则不允许使用动态分区
set hive.exec.dynamic.partition.mode=nonstrict

insert overwrite table dws_uv_detail_mn
partition(dt)
select
    mid_id,
    concat_ws('|',collect_set(user_id)) user_id,
    concat_ws('|', collect_set(version_code)) version_code,
    concat_ws('|', collect_set(version_name)) version_name,
    concat_ws('|', collect_set(lang))lang,
    concat_ws('|', collect_set(source)) source,
    concat_ws('|', collect_set(os)) os,
    concat_ws('|', collect_set(area)) area, 
    concat_ws('|', collect_set(model)) model,
    concat_ws('|', collect_set(brand)) brand,
    concat_ws('|', collect_set(sdk_version)) sdk_version,
    concat_ws('|', collect_set(gmail)) gmail,
    concat_ws('|', collect_set(height_width)) height_width,
    concat_ws('|', collect_set(app_time)) app_time,
    concat_ws('|', collect_set(network)) network,
    concat_ws('|', collect_set(lng)) lng,
    concat_ws('|', collect_set(lat)) lat,
    date_format('2020-09-25','yyyy-MM')
from
    dws_uv_detail_day
where
    date_format(dt,'yyyy-MM')=date_format('2020-09-25','yyyy-MM')
group by
    mid_id;

--查询数据是否插入成功
select * from dws_uv_detail_mn limit 2;
select count(*) from dws_uv_detail_mn;


--创建dws层每日新增设备明细表dws_new_mid_day
drop table if exists dws_new_mid_day;
create external table dws_new_mid_day
(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识', 
    `version_code` string COMMENT '程序版本号', 
    `version_name` string COMMENT '程序版本名', 
    `lang` string COMMENT '系统语言', 
    `source` string COMMENT '渠道号', 
    `os` string COMMENT '安卓系统版本', 
    `area` string COMMENT '区域', 
    `model` string COMMENT '手机型号', 
    `brand` string COMMENT '手机品牌', 
    `sdk_version` string COMMENT 'sdkVersion', 
    `gmail` string COMMENT 'gmail', 
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `create_date`  string  comment '创建时间' 
)  COMMENT '每日新增设备信息'
stored as parquet
location '/warehouse/gmall/dws/dws_new_mid_day/';


--向dws层每日新增设备明细表dws_new_mid_day导入数据
insert overwrite table dws_new_mid_day
select
    ud.mid_id,
    ud.user_id, 
    ud.version_code, 
    ud.version_name, 
    ud.lang, 
    ud.source, 
    ud.os, 
    ud.area, 
    ud.model, 
    ud.brand, 
    ud.sdk_version, 
    ud.gmail, 
    ud.height_width,
    ud.app_time,
    ud.network,
    ud.lng,
    ud.lat,
    '2020-09-25'
from
    dws_uv_detail_day ud left join dws_new_mid_day nd on ud.mid_id = nd.mid_id
where
    ud.dt='2020-09-25' and nd.mid_id is null;

--查询数据是否插入成功
select * from dws_new_mid_day limit 2;
select count(*) from dws_new_mid_day;


--创建dws层每日留存用户明细表dws_user_retention_day
drop table if exists dws_user_retention_day;
create external table dws_user_retention_day 
(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识', 
    `version_code` string COMMENT '程序版本号', 
    `version_name` string COMMENT '程序版本名', 
    `lang` string COMMENT '系统语言', 
    `source` string COMMENT '渠道号', 
    `os` string COMMENT '安卓系统版本', 
    `area` string COMMENT '区域', 
    `model` string COMMENT '手机型号', 
    `brand` string COMMENT '手机品牌', 
    `sdk_version` string COMMENT 'sdkVersion', 
    `gmail` string COMMENT 'gmail', 
    `height_width` string COMMENT '屏幕宽高',
    `app_time` string COMMENT '客户端日志产生时的时间',
    `network` string COMMENT '网络模式',
    `lng` string COMMENT '经度',
    `lat` string COMMENT '纬度',
    `create_date` string  comment '设备新增时间',
    `retention_day` int comment '截止当前日期留存天数'
)  COMMENT '每日用户留存情况'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_user_retention_day/';


--向dws层每日留存用户明细表dws_user_retention_day
--计算多日留存时，使用union all将多个查询并起来
insert overwrite table dws_user_retention_day
partition(dt='2020-09-27')
select 
    nd.mid_id,
    nd.user_id , 
    nd.version_code , 
    nd.version_name , 
    nd.lang , 
    nd.source, 
    nd.os, 
    nd.area, 
    nd.model, 
    nd.brand, 
    nd.sdk_version, 
    nd.gmail, 
    nd.height_width,
    nd.app_time,
    nd.network,
    nd.lng,
    nd.lat,
    nd.create_date,
    1 retention_day 
from
    dws_new_mid_day nd join dws_uv_detail_day ud on nd.mid_id=ud.mid_id 
where
    nd.create_date=date_add('2020-09-27',-1) and ud.dt='2020-09-27'
union all
select 
    nd.mid_id,
    nd.user_id , 
    nd.version_code , 
    nd.version_name , 
    nd.lang , 
    nd.source, 
    nd.os, 
    nd.area, 
    nd.model, 
    nd.brand, 
    nd.sdk_version, 
    nd.gmail, 
    nd.height_width,
    nd.app_time,
    nd.network,
    nd.lng,
    nd.lat,
    nd.create_date,
    2 retention_day 
from
    dws_new_mid_day nd join dws_uv_detail_day ud on nd.mid_id=ud.mid_id 
where
    nd.create_date=date_add('2020-09-27',-2) and ud.dt='2020-09-27'


--查询数据是否插入成功
select count(*) from dws_user_retention_day;
select * from dws_user_retention_day;