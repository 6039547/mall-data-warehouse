# -mall-data-warehouse-

## 项目简介
基于hadoop生态搭建的电商数据仓库，整体功能架构包含数据采集、数仓搭建、数据导出、数据可视化等。

### 系统架构

**系统数据流程如下图：**
![系统数据流程图.jpeg](https://upload-images.jianshu.io/upload_images/19968652-9322de317b9e13c6.jpeg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**数仓分层如下图：**
![数仓分层图.png](https://upload-images.jianshu.io/upload_images/19968652-d134b6a945bee9c0.png?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)

**hive表关系图如下图：**
![hive表关系图.jpeg](https://upload-images.jianshu.io/upload_images/19968652-72e54b3f7ca3076d.jpeg?imageMogr2/auto-orient/strip%7CimageView2/2/w/1240)


### 技术架构

| 名称 | 版本号 | 类型 | 说明 |
| :--- | :--- | :--- | :--- |
| hadoop | 2.7.6 | 数据存储 |  |
| jdk | 1.8.0 | 依赖 |  |
| zookeeper | 3.4.6 | 集群管理 |  |
| flume | 1.7.0 | 数据采集传输 |  |
| kafka | 2.11-0.11 | 数据采集传输 |  |
| kafka  manager| 1.3.3 | 可视化管理 |  |
| hive| 1.2.1 | 数据计算 | 使用tez 0.9.1作为计算引擎 |
| mysql| 5.6.24 | 数据存储 |  |
| sqoop| 1.4.6 | 数据采集传输 |  |
| azkaban| 2.5.0 | 任务调度 |  |
| presto| 0.196 | 数据查询 |使用yanagishima 18.0作为web页面|
| druid| 2.7.10 | 数据查询 | imply方式安装 |
| hbase| 1.2.1 | 数据存储 |  |

### 项目结构说明
```
├─azkaban azkaban job文件 
│
├─flume-interceptor  采集flume拦截器，用来区分日志类型与简单数据清洗
│
├─hive
│  └─gmall
│      ├─用户行为数仓 用户行为数仓hive sql
│      │     
│      └─系统业务数仓 系统业务数仓hive sql
│              
├─hive-function hive自定义函数
│              
├─log-collector 生成日志文件项目，打包成jar包后通过命令运行，将标准输出重定向至log文件即可
│             
├─mysql mysql结构、函数
│      
├─shell 数仓中常用脚本
│  ├─ads ads层加载数据脚本
│  │      
│  ├─dwd dwd层加载数据脚本
│  │      
│  ├─dws dws层加载数据脚本
│  │      
│  ├─ods ods层加载数据脚本
│  │      
│  ├─sqoop mysql导入导出数据脚本
│  │     
│  ├─tools 集群公共脚本
│  │      
│  └─utils 通用工具脚本
│          
└─spring-boot-echarts-master 可视化web项目
```
                        


### 集群规划

![集群规划](https://upload-images.jianshu.io/upload_images/19968652-3b94c7f0270aeac0.png?imageMogr2/auto-orient/strip|imageView2/2/w/681/format/webp)

### 脚本说明

<table>
  <tr>
    <th>名称</th>
    <th>参数</th>
    <th>参数说明</th>
    <th>脚本说明</th>
  </tr>
  <tr>
    <td>ods_log.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>行为数仓ods层导入hdfs数据</td>
  </tr>
   <tr>
    <td>ods_log.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>行为数仓ods层导入hdfs数据</td>
  </tr>
   <tr>
    <td>ods_db.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>业务数仓ods层导入hdfs数据</td>
  </tr>
   <tr>
    <td>dwd_start_log.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>行为数仓dwd层启动日志表dwd_start_log自动导入ods层数据</td>
  </tr>
         <tr>
    <td>dwd_base_log.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>行为数仓dwd层事件日志表dwd_base_event_log自动导入ods层数据</td>
  </tr>
     <tr>
    <td>dwd_event_log.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>行为数仓dwd层各个事件表自动导入数据</td>
  </tr>
     <tr>
    <td>dwd_db.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>业务数仓dwd层自动导入数据</td>
  </tr>
       <tr>
    <td>dws_uv_log.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>行为数仓DWS层加载活跃用户明细数据脚本</td>
  </tr>
        <tr>
    <td>dws_db_wide.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>业务数仓dws层用户行为宽表自动导入数据</td>
  </tr>
   <tr>
    <td>dws_sale.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>业务数仓dws层用户购买商品明细宽表自动导入数据</td>
  </tr>
     <tr>
    <td>ads_back_log.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>ads层回流用户指标分析数据导入脚本</td>
  </tr>
  <tr>
    <td>ads_continuity_log.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>ads层最近七天连续活跃三天用户指标分析数据导入脚本</td>
  </tr>
    <tr>
    <td>ads_continuity_wk_log.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>ads层最近三周活跃用户指标分析数据导入脚本</td>
  </tr>
      <tr>
    <td>ads_db_gmv.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>ads层GMV指标分析数据导入脚本</td>
  </tr>
      <tr>
    <td>ads_slient_log.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>ads层沉默用户指标分析数据导入脚本</td>
  </tr>
        <tr>
    <td>ads_wastage_log.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>ads层流失用户指标分析数据导入脚本</td>
  </tr>
          <tr>
    <td>ads_uv_log.sh</td>
    <td>$1</td>
    <td>分区名（时间）</td>
    <td>ads层活跃用户指标分析数据导入脚本</td>
  </tr>
  <tr>
    <td rowspan="2">sqoop_import.sh</td>
    <td>$1</td>
    <td>hdfs导入指定mysql表数据，可选：表名、all（全部导入）</td>
    <td rowspan="2">hdfs导入mysql数据脚本</td>
  </tr>
  <tr>
    <td>$2</td>
    <td>时间</td>
  </tr>
   <tr>
    <td>sqoop_export.sh</td>
    <td>$1</td>
    <td>hive导出指定hive表数据，可选：ads_uv_count(日活指标表)、ads_user_action_convert_day（漏斗分析指标表）、ads_gmv_sum_day（GVM指标表）、all（上述三个全部导出）</td>
    <td>hive导出ads层数据至mysql脚本</td>
  </tr>
     <tr>
    <td rowspan="2">lg.sh</td>
    <td>$1</td>
    <td>每条日志数据产生的延迟时间(毫秒)</td>
    <td rowspan="2">生成日志文件脚本，生成数据的时间与当前服务器时间一致</td>
  </tr>
   <tr>
    <td>$2</td>
    <td>生成日志数据条数</td>
  </tr>
       <tr>
    <td>zk.sh</td>
    <td>$1</td>
    <td>可选：start（启动）、stop（停止）、status（查看状态）</td>
    <td>群起zookeeper脚本</td>
  </tr>
         <tr>
    <td>f1.sh</td>
    <td>$1</td>
    <td>可选：start（启动）、stop（停止）</td>
    <td>群起采集flume脚本</td>
  </tr>
           <tr>
    <td>f2.sh</td>
    <td>$1</td>
    <td>可选：start（启动）、stop（停止）</td>
    <td>启动消费flume脚本</td>
  </tr>
  <tr>
    <td>kf.sh</td>
    <td>$1</td>
    <td>可选：start（启动）、stop（停止）</td>
    <td>群起kafka脚本</td>
  </tr>
  <tr>
    <td>km.sh</td>
    <td>$1</td>
    <td>可选：start（启动）、stop（停止）</td>
    <td>启动Kafka Manager脚本</td>
  </tr>
  <tr>
    <td>hv.sh</td>
    <td>$1</td>
    <td>可选：start（启动）、stop（停止）</td>
    <td>启动hive metastore服务脚本，注：当启动卡住可以直接Ctrl+C退出启动界面，再使用jps命令查看进程是否启动，进程启动即可。</td>
  </tr>
    <tr>
    <td>cluster.sh</td>
    <td>$1</td>
    <td>可选：start（启动）、stop（停止）</td>
    <td>群起用户行为数据采集架构脚本</td>
  </tr>
   <tr>
    <td>xcall.sh</td>
    <td>$1</td>
    <td>要执行的命令</td>
    <td>集群执行统一命令脚本</td>
  </tr>
   <tr>
    <td>xsync</td>
    <td>$1</td>
    <td>文件绝对路径</td>
    <td>集群分发文件脚本</td>
  </tr>
  
</table>


