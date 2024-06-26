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

本入门指南将帮助你快速了解并开始使用物化表。内容包括环境设置，以及创建、修改和删除连续模式和全量模式的物化表。

# 架构介绍

- **Client**: 可以是任何能够与 [Flink SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}}) 交互的客户端，如 [SQL 客户端]({{< ref "docs/dev/table/sqlClient" >}})、[Flink JDBC 驱动]({{< ref "docs/dev/table/jdbcDriver" >}}) 等。
- **Flink SQL Gateway**: 支持创建、修改和删除物化表。并包含了一个内置的工作流调度器，用于定期刷新全量模式的物化表。
- **Flink Cluster**: 用于运行物化表刷新作业的 Flink 集群。
- **Catalog**: 负责管理物化表元数据的创建、查询、修改和删除。
- **Catalog Store**: 提供 Catalog 属性持久化功能，以便在操作物化表时自动初始化 Catalog 并获取相关的元数据。

{{< img src="/fig/materialized-table-architecture.svg" alt="Illustration of Flink Materialized Table Architecture" width="85%" >}}

# 环境搭建

## 目录准备

**请将下面的示例路径替换为你机器上的实际路径。**

- 创建 Catalog Store 和 Catalog 所需的目录

```
# 用于 File Catalog Store 保存 Catalog 属性
mkdir -p {catalog_store_path}

# 用于 test-filesystem Catalog 保存元数据和表数据
mkdir -p {catalog_path}

# 用于 test-filesystem Catalog 的默认数据库
mkdir -p {catalog_path}/mydb
```

- 创建目录分别用于保存 Checkpoints 和 Savepoints：

```
mkdir -p {checkpoints_path}

mkdir -p {savepoints_path}
```

## 资源准备

这里的方法和[本地安装]({{< ref "docs/try-flink/local_installation" >}})中记录的步骤类似。Flink 可以运行在任何类 UNIX 的操作系统下面，例如：Linux, Mac OS X 和 Cygwin (for Windows)。

[下载](https://flink.apache.org/downloads/) Flink 最新的二进制包并进行解压：

```
tar -xzf flink-*.tgz
```

[下载](https://https://repo.maven.apache.org/maven2/org/apache/flink/flink-table-filesystem-test-utils/) test-filesystem 连接器， 并将其放入 lib 目录：

```
cp flink-table-filesystem-test-utils-{VERSION}.jar flink-*/lib/
```

## 配置准备

在 `config.yaml` 文件中添加以下配置：

```yaml
execution:
  checkpoints:
    dir: file://{checkpoints_path}

# 配置 file catalog store
table:
  catalog-store:
    kind: file
    file:
      path: {catalog_store_path}

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

## 启动 Flink Cluster

运行以下脚本，即可在本地启动集群：

```
./bin/start-cluster.sh
```

## 启动 SQL Gateway

运行以下脚本，即可在本地启动 SQL Gateway：

```
./sql-gateway.sh start
```

## 启动 SQL Client

运行以下脚本，即可在本地启动 SQL Client 客户端并连接到指定的 SQL Gateway：

```
./sql-client.sh gateway --endpoint http://127.0.0.1:8083
```

## 创建 Catalog 和 Source 表

- 创建 test-filesystem catalog 用于后续创建物化表。

```sql
CREATE CATALOG mt_cat WITH (
  'type' = 'test-filesystem',
  'path' = '{catalog_path}',
  'default-database' = 'mydb'
);

USE CATALOG mt_cat;
```

- 创建 Source 表作为物化表的数据源。

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
```

# 创建连续模式物化表

## 创建物化表

创建一个连续模式的物化表，对应的数据新鲜度为 `30` 秒。通过 `http://localhost:8081` 页面可以查看对应的 Flink 流作业，该作业处于 `RUNNING` 状态，对应的 `checkpoint` 间隔为 `30` 秒。

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

## 暂停物化表

暂停物化表的刷新管道。在 `http://localhost:8081` 页面上，你会看到负责持续刷新物化表的 Flink 流作业已处于 `FINISHED` 状态。在执行暂停操作前，请确保设置 `savepoint` 路径。

```sql
-- 暂停前设置 savepoint 路径
SET 'execution.checkpointing.savepoint-dir' = 'file://{savepoint_path}';

ALTER MATERIALIZED TABLE continuous_users_shops SUSPEND;
```

## 查询物化表

查询物化表数据并确认数据已成功写入。

```sql
SELECT * FROM continuous_users_shops;
```

## 恢复物化表

恢复物化表的刷新管道，你会发现一个新的 Flink 流作业从指定的 `savepoint` 路径启动，用于持续刷新物化表，可以在 `http://localhost:8081` 页面上查看。

```sql
ALTER MATERIALIZED TABLE continuous_users_shops RESUME;
```

## 删除物化表

删除物化表后，你会发现用于持续刷新物化表的 Flink 流作业在 `http://localhost:8081` 页面上转变为 `CANCELED` 状态。

```sql
DROP MATERIALIZED TABLE continuous_users_shops;
```

# 创建全量模式物化表

## 创建物化表

创建一个全量模式的物化表，对应的数据新鲜度为 1 分钟。（此处设置为 1 分钟只是为了方便测试）你会发现用于定期刷新物化表的 Flink 批作业每 1 分钟调度一次，可以在 `http://localhost:8081` 页面上查看。

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

## 查询物化表

向当天的分区插入一些数据，等待至少 1 分钟，然后查询物化表结果，只能查询到当天分区的数据。

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

## 手动刷新历史分区

手动刷新分区 `ds='2024-06-20'`，并验证物化表中该日期的数据。你可以在 `http://localhost:8081` 页面上找到当前正在运行的 Flink 批作业。

```sql
-- 手动刷新历史分区
ALTER MATERIALIZED TABLE full_users_shops REFRESH PARTITION(ds='2024-06-20');

-- 查询物化表数据
SELECT * FROM full_users_shops;
```

## 暂停和恢复物化表

通过暂停和恢复操作，你可以控制物化表的刷新作业。一旦暂停，负责定期刷新物化表的 Flink 批作业将不再被调度执行。当恢复时，Flink 批作业将重新开始调度以刷新物化表。你可在 `http://localhost:8081` 页面上查看 Flink 作业的调度状态。

```sql
-- 暂停后台刷新任务
ALTER MATERIALIZED TABLE full_users_shops SUSPEND;

-- 恢复后台刷新任务
ALTER MATERIALIZED TABLE full_users_shops RESUME;
```

## 删除物化表

删除物化表后，负责定期刷新该物化表的 Flink 批作业将不再被调度执行。你可以在 `http://localhost:8081` 页面上进行确认。

```sql
DROP MATERIALIZED TABLE full_users_shops;
```
