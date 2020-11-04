# -mall-data-warehouse-

## 项目简介
基于hadoop生态搭建的电商数据仓库，整体功能架构包含数据采集、数仓搭建、数据导出、数据可视化等
#### 技术架构

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

#### 请求参数说明

| 请求参数 | 类型 | 必填 | 参数说明 | 示例 |
| :--- | :--- | :--- | :--- | :--- |
| username | String | true | 登录用户名 |carozhu |
| password | String | true | 登录密码   |123456   |

#### 返回参数说明

| 返回参数 | 参数类型 | 参数说明 |
| :--- | :--- | :--- |
| responseCode | Integer | 200：成功|
| accessToken | String | 用户token|
| ... | ... | ... |

#### 返回示例JSON

```json
{
    "responseCode": 200,
    "data": {
        "name": "carozhu",
        "type": 4,
        "version": "1.2.4",
        "file": "http://versions.update.com/xxx.apk",
        "md5": "6ed86ad3f14db4db716c808cfc1ca392",
        "description": "update for simple to you！"
    }
}
```

#### code码说明

| code | msg | desc |
| :--- | :--- | :--- |
| 200 | success |  |

#### 接口详细说明 

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


