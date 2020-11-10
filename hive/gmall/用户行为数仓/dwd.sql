--创建dwd层启动日志表
drop table if exists dwd_start_log;
Create external table dwd_start_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `entry` string, 
    `open_ad_type` string, 
    `action` string, 
    `loading_time` string, 
    `detail` string, 
    `extend1` string
)
partitioned by (`dt` String)
location '/warehouse/gmall/dwd/dwd_start_log';
 
--向dwd层启动日志表插入数据
insert overwrite table dwd_start_log 
partition(dt='2020-09-25')
select 
    get_json_object(line,'$.mid') mid_id,
    get_json_object(line,'$.uid') user_id,
    get_json_object(line,'$.vc') version_code,
    get_json_object(line,'$.vn') version_name,
    get_json_object(line,'$.l') lang,
    get_json_object(line,'$.sr') source,
    get_json_object(line,'$.os') os,
    get_json_object(line,'$.ar') area,
    get_json_object(line,'$.md') model,
    get_json_object(line,'$.ba') brand,
    get_json_object(line,'$.sv') sdk_version,
    get_json_object(line,'$.g') gmail,
    get_json_object(line,'$.hw') height_width,
    get_json_object(line,'$.t') app_time,
    get_json_object(line,'$.nw') network,
    get_json_object(line,'$.ln') lng,
    get_json_object(line,'$.la') lat,
    get_json_object(line,'$.entry') entry,
    get_json_object(line,'$.open_ad_type') open_ad_type,
    get_json_object(line,'$.action') action,
    get_json_object(line,'$.loading_time') loading_time,
    get_json_object(line,'$.detail') detail,
    get_json_object(line,'$.extend1') extend1
from
    ods_start_log
where 
    dt='2020-09-25';


--查询数据是否正常插入
select * from dwd_start_log limit 1;

--创建事件日志表dwd_base_event_log;
drop table if exists dwd_base_event_log;
Create external table dwd_base_event_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string, 
    `app_time` string, 
    `network` string, 
    `lng` string, 
    `lat` string, 
    `event_name` string, 
    `event_json` string, 
    `server_time` string
)
partitioned by (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_base_event_log/';

--将自定义hive函数上传后，添加到hive的classpath中
add jar /opt/apps/hive/hivefunction-1.0-SNAPSHOT.jar;

--创建临时函数并关联java类
Create temporary function base_analizer as 'com.RuHuTian.udf.BaseFieldUDF';
Create temporary function flat_analizer as 'com.RuHuTian.udtf.EventJsonUDTF';

--向事件日志表插入数据
insert overwrite table dwd_base_event_log
partition(dt='2020-09-25')
select
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    event_name,
    event_json,
    server_time
from 
    (
        select
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[0]   as mid_id,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[1]   as user_id,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[2]   as version_code,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[3]   as version_name,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[4]   as lang,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[5]   as source,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[6]   as os,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[7]   as area,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[8]   as model,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[9]   as brand,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[10]  as sdk_version,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[11]  as gmail,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[12]  as height_width,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[13]  as app_time,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[14]  as network,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[15]  as lng,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[16]  as lat,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[17]  as ops,
            split(base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la'),'\t')[18]  as server_time
        from
            ods_event_log
        where dt='2020-09-25' and base_analizer(line,'mid,uid,vc,vn,l,sr,os,ar,md,ba,sv,g,hw,t,nw,ln,la')<>''
    ) sdk_log lateral view flat_analizer(ops) tmp_k as event_name,event_json;    

--创建dwd层商品点击事件表dwd_display_log
drop table if exists dwd_display_log;
create external table dwd_display_log(
    `mid_id` string,
    `user_id` string,
    `version_code` string,
    `version_name` string,
    `lang` string,
    `source` string,
    `os` string,
    `area` string,
    `model` string,
    `brand` string,
    `sdk_version` string,
    `gmail` string,
    `height_width` string,
    `app_time` string,
    `network` string,
    `lng` string,
    `lat` string,
    `action` string,
    `goodsid` string,
    `place` string,
    `extend1` string,
    `category` string,
    `server_time` string
)
partitioned by (`dt` string)
location '/warehouse/gmall/dwd/dwd_display_log/';

--dwd层商品点击事件表dwd_dispaly_log从dwd层事件日志表dwd_base_event_log导入数据
insert overwrite table dwd_display_log
partition(dt='2020-09-25')
select
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.goodsid') goodsid,
    get_json_object(event_json,'$.kv.place') place,
    get_json_object(event_json,'$.kv.extend1') extend1,
    get_json_object(event_json,'$.kv.category') category,
    server_time
from
    dwd_base_event_log
where
    dt = '2020-09-25' and event_name = 'display'

--插入数据是否正常插入
select * from dwd_display_log limit 2;

--创建dwd层商品详情事件表dwd_newsdetail_log
drop table if exists dwd_newsdetail_log;
create external table dwd_newsdetail_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string, 
    `app_time` string,  
    `network` string, 
    `lng` string, 
    `lat` string, 
    `entry` string,
    `action` string,
    `goodsid` string,
    `showtype` string,
    `news_staytime` string,
    `loading_time` string,
    `type1` string,
    `category` string,
    `server_time` string
)
partitioned by (`dt` string)
location '/warehouse/gmall/dwd/dwd_newsdetail_log/';

--dwd层商品详情事件表dwd_newsdetail_log从dwd层事件日志表dwd_base_event_log导入数据
insert overwrite table dwd_newsdetail_log
PARTITION (dt='2020-09-25')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.entry') entry,
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.goodsid') goodsid,
    get_json_object(event_json,'$.kv.showtype') showtype,
    get_json_object(event_json,'$.kv.news_staytime') news_staytime,
    get_json_object(event_json,'$.kv.loading_time') loading_time,
    get_json_object(event_json,'$.kv.type1') type1,
    get_json_object(event_json,'$.kv.category') category,
    server_time
from dwd_base_event_log
where dt='2020-09-25' and event_name='newsdetail';

--插入数据是否正常插入
select * from dwd_newsdetail_log limit 2;

--创建dwd层商品列表页事件表dwd_loading_log
drop table if exists dwd_loading_log;
create external table dwd_loading_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string,
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `action` string,
    `loading_time` string,
    `loading_way` string,
    `extend1` string,
    `extend2` string,
    `type` string,
    `type1` string,
    `server_time` string
)
partitioned by (`dt` string)
location '/warehouse/gmall/dwd/dwd_loading_log/';

--dwd层商品列表页事件表dwd_loading_log从dwd层事件日志表dwd_base_event_log导入数据
insert overwrite table dwd_loading_log
PARTITION (dt='2020-09-25')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.loading_time') loading_time,
    get_json_object(event_json,'$.kv.loading_way') loading_way,
    get_json_object(event_json,'$.kv.extend1') extend1,
    get_json_object(event_json,'$.kv.extend2') extend2,
    get_json_object(event_json,'$.kv.type') type,
    get_json_object(event_json,'$.kv.type1') type1,
    server_time
from dwd_base_event_log
where dt='2020-09-25' and event_name='loading';

--插入数据是否正常插入
select * from dwd_loading_log limit 2;

--创建dwd层广告事件表dwd_ad_log
drop table if exists dwd_ad_log;
create external table dwd_ad_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string,
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `entry` string,
    `action` string,
    `content` string,
    `detail` string,
    `ad_source` string,
    `behavior` string,
    `newstype` string,
    `show_style` string,
    `server_time` string
)
partitioned by (`dt` string)
location '/warehouse/gmall/dwd/dwd_ad_log/';

--dwd层广告事件表dwd_ad_log从dwd层事件日志表dwd_base_event_log导入数据
insert overwrite table dwd_ad_log
PARTITION (dt='2020-09-25')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.entry') entry,
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.content') content,
    get_json_object(event_json,'$.kv.detail') detail,
    get_json_object(event_json,'$.kv.source') ad_source,
    get_json_object(event_json,'$.kv.behavior') behavior,
    get_json_object(event_json,'$.kv.newstype') newstype,
    get_json_object(event_json,'$.kv.show_style') show_style,
    server_time
from dwd_base_event_log
where dt='2020-09-25' and event_name='ad';

--插入数据是否正常插入
select * from dwd_ad_log limit 2;

--创建dwd层消息通知表dwd_notification_log
drop table if exists dwd_notification_log;
create external table dwd_notification_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string,
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `action` string,
    `noti_type` string,
    `ap_time` string,
    `content` string,
    `server_time` string
)
partitioned by (`dt` string)
location '/warehouse/gmall/dwd/dwd_notification_log/';

--dwd层消息通知表dwd_notification_log从dwd层事件日志表dwd_base_event_log导入数据
insert overwrite table dwd_notification_log
PARTITION (dt='2020-09-25')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.noti_type') noti_type,
    get_json_object(event_json,'$.kv.ap_time') ap_time,
    get_json_object(event_json,'$.kv.content') content,
    server_time
from dwd_base_event_log
where dt='2020-09-25' and event_name='notification';

--插入数据是否正常插入
select * from dwd_notification_log limit 2;

--创建dwd层用户前台活跃事件表dwd_active_foreground_log
drop table if exists dwd_active_foreground_log;
create external table dwd_active_foreground_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string,
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `push_id` string,
    `access` string,
    `server_time` string
)
partitioned by (`dt` string)
location '/warehouse/gmall/dwd/dwd_active_foreground_log/';

--dwd层前台活跃事件表dwd_active_foreground_log从dwd层事件日志表dwd_base_event_log导入数据
insert overwrite table dwd_active_foreground_log
PARTITION (dt='2020-09-25')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.push_id') push_id,
    get_json_object(event_json,'$.kv.access') access,
    server_time
from dwd_base_event_log
where dt='2020-09-25' and event_name='active_foreground';

--插入数据是否正常插入
select * from dwd_active_foreground_log limit 2;

--创建dwd层用户后台活跃事件表dwd_active_background_log
drop table if exists dwd_active_background_log;
create external table dwd_active_background_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string,
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `active_source` string,
    `server_time` string
)
partitioned by (`dt` string)
location '/warehouse/gmall/dwd/dwd_active_background_log/';

--dwd层用户后台活跃事件表dwd_active_background_log从dwd层事件日志表dwd_base_event_log导入数据
insert overwrite table dwd_active_background_log
PARTITION (dt='2020-09-25')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.active_source') active_source,
    server_time
from dwd_base_event_log
where dt='2020-09-25' and event_name='active_background';

--插入数据是否正常插入
select * from dwd_active_background_log limit 2;

--创建dwd层评论事件表dwd_comment_log
drop table if exists dwd_comment_log;
create external table dwd_comment_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string,
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `comment_id` int,
    `userid` int,
    `p_comment_id` int, 
    `content` string,
    `addtime` string,
    `other_id` int,
    `praise_count` int,
    `reply_count` int,
    `server_time` string
)
partitioned by (`dt` string)
location '/warehouse/gmall/dwd/dwd_comment_log/';

--dwd层评论事件表dwd_comment_log从dwd层事件日志表dwd_base_event_log导入数据
insert overwrite table dwd_comment_log
PARTITION (dt='2020-09-25')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.comment_id') comment_id,
    get_json_object(event_json,'$.kv.userid') userid,
    get_json_object(event_json,'$.kv.p_comment_id') p_comment_id,
    get_json_object(event_json,'$.kv.content') content,
    get_json_object(event_json,'$.kv.addtime') addtime,
    get_json_object(event_json,'$.kv.other_id') other_id,
    get_json_object(event_json,'$.kv.praise_count') praise_count,
    get_json_object(event_json,'$.kv.reply_count') reply_count,
    server_time
from dwd_base_event_log
where dt='2020-09-25' and event_name='comment';

--插入数据是否正常插入
select * from dwd_comment_log limit 2;

--创建dwd层收藏事件表dwd_favorites_log
drop table if exists dwd_favorites_log;
create external table dwd_favorites_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string,
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `id` int, 
    `course_id` int, 
    `userid` int,
    `add_time` string,
    `server_time` string
)
partitioned by (`dt` string)
location '/warehouse/gmall/dwd/dwd_favorites_log/';

--dwd层收藏事件表dwd_favorites_log从dwd层事件日志表dwd_base_event_log导入数据
insert overwrite table dwd_favorites_log
PARTITION (dt='2020-09-25')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.id') id,
    get_json_object(event_json,'$.kv.course_id') course_id,
    get_json_object(event_json,'$.kv.userid') userid,
    get_json_object(event_json,'$.kv.add_time') add_time,
    server_time
from dwd_base_event_log
where dt='2020-09-25' and event_name='favorites';

--插入数据是否正常插入
select * from dwd_favorites_log limit 2;

--创建dwd层点赞事件表dwd_praise_log
drop table if exists dwd_praise_log;
create external table dwd_praise_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string,
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `id` string, 
    `userid` string, 
    `target_id` string,
    `type` string,
    `add_time` string,
    `server_time` string
)
partitioned by (`dt` string)
location '/warehouse/gmall/dwd/dwd_praise_log/';

--dwd层点赞事件表dwd_praise_log从dwd层事件日志表dwd_base_event_log导入数据
insert overwrite table dwd_praise_log
PARTITION (dt='2020-09-25')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.id') id,
    get_json_object(event_json,'$.kv.userid') userid,
    get_json_object(event_json,'$.kv.target_id') target_id,
    get_json_object(event_json,'$.kv.type') type,
    get_json_object(event_json,'$.kv.add_time') add_time,
    server_time
from dwd_base_event_log
where dt='2020-09-25' and event_name='praise';

--插入数据是否正常插入
select * from dwd_praise_log limit 2;

--创建dwd层错误日志事件表dwd_error_log
drop table if exists dwd_error_log;
create external table dwd_error_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string,
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `errorBrief` string, 
    `errorDetail` string, 
    `server_time` string
)
partitioned by (`dt` string)
location '/warehouse/gmall/dwd/dwd_error_log/';

--dwd层错误日志事件表dwd_error_log从dwd层事件日志表dwd_base_event_log导入数据
insert overwrite table dwd_error_log
PARTITION (dt='2020-09-25')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.errorBrief') errorBrief,
    get_json_object(event_json,'$.kv.errorDetail') errorDetail,
    server_time
from dwd_base_event_log
where dt='2020-09-25' and event_name='error';

--插入数据是否正常插入
select * from dwd_error_log limit 2;