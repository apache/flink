---
title: 快速入门
weight: 3
type: docs
aliases:
- /dev/table/materialized-table/quickstart.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->


# 快速入门

本文将帮助你快速了解并上手使用物化表，主要内容包括环境配置、创建 CONTINUOUS 模式的物化表以及创建 FULL 模式的物化表。

## 环境搭建

### 目录准备

**请将下面示例路径替换个人机器上的真实路径。**

- 创建 Catalog Store 和 Catalog 依赖的目录

```
# 用于 File Catalog Store 保存 Catalog 信息
mkdir -p /path/to/catalog/store

# 用于 test-filesystem Catalog 保存表的元数据和表数据
mkdir -p /path/to/catalog/test-filesystem

# 用于 test-filesystem Catalog 的默认数据库
mkdir -p /path/to/catalog/test-filesystem/mydb
```

- 创建 Checkpoint 和 Savepoint 目录，分别用于保存 Checkpoints 和 Savepoints：

```
mkdir -p /path/to/checkpoint

mkdir -p /path/to/savepoint
```

### 资源准备

这里的方法和[本地安装]({{< ref "docs/try-flink/local_installation" >}})中记录的步骤类似。Flink 可以运行在任何类 UNIX 的操作系统下面，例如：Linux, Mac OS X 和 Cygwin (for Windows)。你需要在本地安装好 __Java 11__，可以通过下述命令行的方式检查安装好的 Java 版本：

```
java -version
```

下一步, [下载](https://flink.apache.org/downloads/) Flink 最新的二进制包并进行解压：

```
tar -xzf flink-*.tgz
```

下载 [test-filesystem](https://https://repo.maven.apache.org/maven2/org/apache/flink/flink-table-filesystem-test-utils/) 连接器， 并将它放到 lib 目录下。

```
cp flink-table-filesystem-test-utils-{VERSION}.jar flink-*/lib/
```

### 配置准备

编辑 config.yaml 文件，添加如下配置：

```yaml
execution:
  checkpoints:
    dir: file:///path/to/savepoint

# 配置 file catalog
table:
  catalog-store:
    kind: file
    file:
      path: /path/to/catalog/store

# 配置 embedded 调度器 
workflow-scheduler:
  type: embedded

# 配置 SQL gateway 的地址和端口
sql-gateway:
  endpoint:
    rest:
      address: 127.0.0.1
      port: 8083
```

### 启动 Flink Cluster

运行下述脚本，即可在本地启动集群：

```
./bin/start-cluster.sh
```

### 启动 SQL Gateway

运行下述脚本，即可在本地启动 SQL Gateway：

```
./sql-gateway.sh start
```

### 启动 Sql Client

运行下述脚本，即可在本地启动 SQL Client 客户端：

```
./sql-client.sh gateway --endpoint http://127.0.0.1:8083
```

## 创建 Catalog 和 Source 表

1. 创建 test-filesystem catalog

```sql
CREATE CATALOG mt_cat WITH (
  'type' = 'test-filesystem',
  'path' = '/path/to/catalog/test-filesystem',
  'default-database' = 'mydb'
);

USE CATALOG mt_cat;
```

2. 创建 Source 表

```sql

-- 1. 创建 Source 表，并指定 Source 表的数据格式为 json
CREATE TABLE json_source (
  order_id BIGINT,
  user_id BIGINT,
  user_name STRING,
  order_created_at STRING,
  payment_amount_cents BIGINT
) WITH (
  'format' = 'json',
  'source.monitor-interval' = '10s'
);

-- 2. 插入一些测试数据
INSERT INTO json_source VALUES 
  (1001, 1, 'user1', '2024-06-19', 10),
  (1002, 2, 'user2', '2024-06-19', 20),
  (1003, 3, 'user3', '2024-06-19', 30),
  (1004, 4, 'user4', '2024-06-19', 40),
  (1005, 1, 'user1', '2024-06-20', 10),
  (1006, 2, 'user2', '2024-06-20', 20),
  (1007, 3, 'user3', '2024-06-20', 30),
  (1008, 4, 'user4', '2024-06-20', 40);
  
INSERT INTO json_source VALUES 
  (1001, 1, 'user1', '2024-06-24', 10),
  (1002, 2, 'user2', '2024-06-24', 20),
  (1003, 3, 'user3', '2024-06-24', 30),
  (1004, 4, 'user4', '2024-06-24', 40),
  (1005, 1, 'user1', '2024-06-25', 10),
  (1006, 2, 'user2', '2024-06-25', 20),
  (1007, 3, 'user3', '2024-06-25', 30),
  (1008, 4, 'user4', '2024-06-25', 40);
  
INSERT INTO json_source VALUES 
  (1001, 1, 'user1', '2024-06-26 ', 10),
  (1002, 2, 'user2', '2024-06-26 ', 20),
  (1003, 3, 'user3', '2024-06-26 ', 30),
  (1004, 4, 'user4', '2024-06-26 ', 40),
  (1005, 1, 'user1', '2024-06-26 ', 10),
  (1006, 2, 'user2', '2024-06-26 ', 20),
  (1007, 3, 'user3', '2024-06-26 ', 30),
  (1008, 4, 'user4', '2024-06-26 ', 40);
```

## CONTINUOUS 模式

### 创建物化表

创建一个 CONTINUOUS 模式的物化表，对应的数据新鲜度为 10 秒。通过页面 http://localhost:8081 可以查看对应实时刷新作业，处于 RUNNING 状态，对应的 checkpoint 间隔为 10s。

```sql
CREATE MATERIALIZED TABLE continuous_users_shops
PARTITIONED BY (ds)
WITH (
  'format' = 'debezium-json',
  'sink.rolling-policy.rollover-interval' = '10s',
  'sink.rolling-policy.check-interval' = '10s'
)
FRESHNESS = INTERVAL '30' SECOND
AS SELECT
  user_id,
  ds,
  SUM (payment_amount_cents) AS payed_buy_fee_sum,
  SUM (1) AS PV
FROM (
  SELECT user_id, order_created_at AS ds, payment_amount_cents
    FROM json_source
  ) AS tmp
GROUP BY user_id, ds;
```


### 暂停物化表

暂停物化表的刷新管道。您会发现用于持续刷新物化表的 Flink 流处理作业在 http://localhost:8081 上转为 FINISHED 状态。在执行暂停操作之前，您需要设置保存点路径。

```sql
-- 暂停前设置 savepoint 路径
SET 'execution.checkpointing.savepoint-dir' = 'file:///path/to/savepoint';

ALTER MATERIALIZED TABLE continuous_users_shops SUSPEND;
```

### 查询物化表

查询物化表数据并确认数据已写入。

```sql
SELECT * FROM continuous_users_shops;
```

### 恢复物化表

恢复物化表的刷新管道。您会发现一个新的 Flink 流处理作业从指定的保存点路径启动，用于持续刷新物化表，可以在 http://localhost:8081 上找到。
```sql
ALTER MATERIALIZED TABLE continuous_users_shops RESUME;
```

### 删除物化表

删除物化表后，您会发现用于持续刷新物化表的 Flink 流处理作业在 http://localhost:8081 上转为 CANCELED 状态。

```sql
DROP MATERIALIZED TABLE continuous_users_shops;
```

## FULL 模式

### 创建物化表

创建一个 FULL 模式的物化表，对应的数据新鲜度为 1 分钟。（此处设置为 1 分钟只是为了方便测试）您会发现用于定期刷新物化表的 Flink 批处理作业每 1 分钟调度一次，可以在 http://localhost:8081 上找到。

```sql
CREATE MATERIALIZED TABLE full_users_shops
PARTITIONED BY (ds)
WITH (
  'format' = 'json',
  'partition.fields.ds.date-formatter' = 'yyyy-MM-dd'
)
FRESHNESS = INTERVAL '1' MINUTE
REFRESH_MODE = FULL
AS SELECT
  user_id,
  ds,
  SUM (payment_amount_cents) AS payed_buy_fee_sum,
  SUM (1) AS PV
FROM (
  SELECT user_id, order_created_at AS ds, payment_amount_cents
  FROM json_source
) AS tmp
GROUP BY user_id, ds;
```

### 查询物化表

向当天的分区插入一些数据。等待至少 1 分钟，然后查询物化表结果，发现只有当天的分区数据被刷新。

```sql
INSERT INTO json_source VALUES 
  (1001, 1, 'user1', CAST(CURRENT_DATE AS STRING), 10),
  (1002, 2, 'user2', CAST(CURRENT_DATE AS STRING), 20),
  (1003, 3, 'user3', CAST(CURRENT_DATE AS STRING), 30),
  (1004, 4, 'user4', CAST(CURRENT_DATE AS STRING), 40);
```

```sql
SELECT * FROM full_users_shops;
```

### 手动刷新历史分区

手动刷新 2024-06-25 分区，并验证物化表中 2024-06-25 的分区数据。您可以在 http://localhost:8081 页面上找到当前刷新操作的 Flink 批处理作业。

```sql
-- 手动刷新历史分区
ALTER MATERIALIZED TABLE full_users_shops REFRESH PARTITIONS(ds='2024-06-25')

-- 查询物化表数据
SELECT * FROM full_users_shops;
```

### 暂停和恢复物化表

通过暂停和恢复操作，您可以控制物化表对应的刷新作业。暂停后，用于定期刷新物化表的 Flink 批处理作业将不会被调度。恢复后，用于定期刷新物化表的 Flink 批处理作业将再次被调度。您可以在 http://localhost:8081 页面上找到 Flink 作业的调度状态。

```sql
-- 暂停后台刷新任务
ALTER MATERIALIZED TABLE full_users_shops SUSPEND;

-- 恢复后台的刷新任务
ALTER MATERIALIZED TABLE full_users_shops RESUME;
```

#### 删除物化表

删除物化表后，用于定期刷新物化表的 Flink 批处理作业将不再被调度。您可以在 http://localhost:8081 页面上确认。

```sql
DROP MATERIALIZED TABLE full_users_shops;
```
