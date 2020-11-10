# -mall-data-warehouse-

## 项目简介
基于hadoop生态搭建的电商数据仓库，整体功能架构包含数据采集、数仓搭建、数据导出、数据可视化等。


详情学习攻略请查看

项目踩坑请查看

#### 系统架构

系统数据流程如下图：

#### 技术架构

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

#### 项目结构说明

├─azkaban azkaban job文件 <br>
│<br>
├─flume-interceptor  采集flume拦截器，用来区分日志类型与简单数据清洗<br>
│<br>
├─hive<br>
│  └─gmall<br>
│      ├─用户行为数仓 用户行为数仓hive sql<br>
│      │      <br>
│      └─系统业务数仓 系统业务数仓hive sql<br>
│              <br>
├─hive-function hive自定义函数<br>
│              <br>
├─log-collector 生成日志文件项目，打包成jar包后通过命令运行，将标准输出重定向至log文件即可<br>
│              <br>
├─mysql mysql结构、函数<br>
│      <br>
├─shell 数仓中常用脚本<br>
│  ├─ads ads层加载数据脚本<br>
│  │      <br>
│  ├─dwd dwd层加载数据脚本<br>
│  │      <br>
│  ├─dws dws层加载数据脚本<br>
│  │      <br>
│  ├─ods ods层加载数据脚本<br>
│  │      <br>
│  ├─sqoop mysql导入导出数据脚本<br>
│  │      <br>
│  ├─tools 集群公共脚本<br>
│  │      <br>
│  └─utils 通用工具脚本<br>
│          <br>
└─spring-boot-echarts-master 可视化web项目
                        


#### 集群规划

![集群规划](https://upload-images.jianshu.io/upload_images/19968652-3b94c7f0270aeac0.png?imageMogr2/auto-orient/strip|imageView2/2/w/681/format/webp)

#### 脚本说明

| 名称 | 版本号 | 备注 |
| :--- | :--- | :--- |
| hadoop | 2.7.6 |  |
| jdk | 1.8.0 |  |
| zookeeper | 3.4.6 |  |
| flume | 1.7.0 |  |
| kafka | 2.11-0.11 |  |
| kafka  manager| 1.3.3 |  |
| hive| 1.2.1 | 使用tez 0.9.1作为计算引擎 |
| mysql| 5.6.24 |  |
| sqoop| 1.4.6 |  |
| azkaban| 2.5.0 |  |
| presto| 0.196 |使用yanagishima 18.0作为web页面|
| druid| 2.7.10 | imply方式安装 |
| hbase| 1.2.1 |  |

#### hive 表说明

| code | msg | desc |
| :--- | :--- | :--- |
| 200 | success |  |

#### mysql 表说明 

``` 
如有特别说明请描述

```

---

#### 备注
``` 
关于其它错误返回值与错误代码，参见 [Code码说明](#Link)

```
---
#### Author

| Coder   | 创建时间 | 更新时间 |联系方式 |
| :---     | :---| :--- | :--- |
| carozhu  | 2018.4.28 |2018.5.29  |1025807062@qq.com  |


