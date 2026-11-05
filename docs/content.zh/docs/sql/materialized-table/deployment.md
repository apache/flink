---
title: 部署
weight: 3
type: docs
aliases:
- /dev/table/materialized-table/deployment.html
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

# Introduction

物化表的创建及运维涉及多个组件的协同工作。本文将从架构解析、环境准备、部署流程到操作实践，系统地说明物化表的完整部署方案。

# 架构介绍

- **Client**: 可以是任何能够与 [Flink SQL Gateway]({{< ref "docs/sql/interfaces/sql-gateway/overview" >}}) 交互的客户端，如 [SQL 客户端]({{< ref "docs/sql/interfaces/sql-client" >}})、[Flink JDBC 驱动]({{< ref "docs/sql/interfaces/jdbc-driver" >}}) 等。
- **Flink SQL Gateway**: 支持创建、修改和删除物化表。并包含了一个内置的工作流调度器，用于定期刷新全量模式的物化表。
- **Flink Cluster**: 用于运行物化表刷新作业的 Flink 集群。
- **Catalog**: 负责管理物化表元数据的创建、查询、修改和删除。
- **Catalog Store**: 提供 Catalog 属性持久化功能，以便在操作物化表时自动初始化 Catalog 并获取相关的元数据。

{{< img src="/fig/materialized-table-architecture.svg" alt="Illustration of Flink Materialized Table Architecture" width="85%" >}}


# 部署准备

## Flink 集群环境准备

物化表刷新作业目前支持在以下集群环境中运行：
* [Standalone clusters]({{<ref "docs/deployment/resource-providers/standalone/overview">}})
* [YARN clusters]({{<ref "docs/deployment/resource-providers/yarn" >}})
* [Kubernetes clusters]({{<ref "docs/deployment/resource-providers/native_kubernetes" >}})

## 部署 SQL Gateway

Materialized Tables in full refresh mode must be created and operated through SQL Gateway, which schedules their refresh workflows. SQL Gateway requires specific configurations for metadata persistence and job scheduling. Materialized Tables in continuous refresh mode can also be created from any `TableEnvironment` (see [Using Table API](#using-table-api)).

### 配置 Catalog Store

在 `config.yaml` 中增加 `catalog store` 相关配置：
```yaml
table:
  catalog-store:
    kind: file
    file:
      path: {path_to_catalog_store} # 替换成实际的路径
```
更多详情配置可参考 [Catalog Store]({{<ref "docs/sql/catalogs">}}#catalog-store)。

### 配置工作流调度器插件

在 `config.yaml` 增加工作流调度器配置，用于定时调度刷新作业。 当前我们仅支持 `embedded` 调度器：

```yaml
workflow-scheduler:
  type: embedded
```

### 启动 SQL Gateway

使用以下命令启动 SQL Gateway：
```
./sql-gateway.sh start
```

<span class="label label-danger">注意</span>
The Catalog must support creating materialized tables.
[Apache Paimon]({{< ref "docs/connectors/other-projects/paimon" >}}) supports them, and so does
[Apache Fluss](https://fluss.apache.org/docs/engine-flink/ddl/#materialized-table) for the continuous
refresh mode only. The test-filesystem catalog used in the
[Quickstart]({{< ref "docs/sql/materialized-table/quickstart" >}}) is meant for testing only.

# 操作指南

## 连接到 SQL Gateway

使用 SQL Client 的示例：

```shell
./sql-client.sh gateway --endpoint {gateway_endpoint}:{gateway_port}
```

## 创建物化表

### 在 Standalone 集群运行刷新作业

```sql
Flink SQL> SET 'execution.target' = 'remote';
[INFO] Execute statement succeeded.

FLINK SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ...
[INFO] Execute statement succeeded.
```

### 在 session 模式下运行刷新作业

在 session 模式下执行时，需要提前创建 session 集群，具体可以参考文档 [yarn-session]({{< ref "docs/deployment/resource-providers/yarn" >}}#starting-a-flink-session-on-yarn) 和 [kubernetes-session]({{<ref "docs/deployment/resource-providers/native_kubernetes" >}}#starting-a-flink-session-on-kubernetes)

**Kubernetes session 模式：**

```sql
Flink SQL> SET 'execution.target' = 'kubernetes-session';
[INFO] Execute statement succeeded.

Flink SQL> SET 'kubernetes.cluster-id' = 'flink-cluster-mt-session-1';
[INFO] Execute statement succeeded.

FLINK SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ...
[INFO] Execute statement succeeded.
```

设置 `execution.target` 为 `kubernetes-session` 并设置参数 `kubernetes.cluster-id` 指向一个已经存在的 Kubernetes session 集群.

**YARN session 模式：**

```sql
Flink SQL> SET 'execution.target' = 'yarn-session';
[INFO] Execute statement succeeded.

Flink SQL> SET 'yarn.application.id' = 'application-xxxx';
[INFO] Execute statement succeeded.

FLINK SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ...
[INFO] Execute statement succeeded.
```
设置 `execution.target` 为 `yarn-session` 并设置参数 `yarn.application.id` 指向一个已经存在的 YARN session 集群。

### 在 application 模式下运行刷新作业

**Kubernetes application 模式：**

```sql
Flink SQL> SET 'execution.target' = 'kubernetes-application';
[INFO] Execute statement succeeded.

Flink SQL> SET 'kubernetes.cluster-id' = 'flink-cluster-mt-application-1';
[INFO] Execute statement succeeded.

FLINK SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ...
[INFO] Execute statement succeeded.
```
设置 `execution.target` 为 `kubernetes-application` ，`kubernetes.cluster-id` 是一个可选配置，如果未配置，在提交作业时会自动生成。

**YARN application 模式：**

```sql
Flink SQL> SET 'execution.target' = 'yarn-application';
[INFO] Execute statement succeeded.

FLINK SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ...
[INFO] Execute statement succeeded.
```
设置 `execution.target` 为 `yarn-application` ，`yarn.application.id` 无需配置。

## 运维操作

Cluster information (e.g., `execution.target` or `kubernetes.cluster-id`) is already persisted in the catalog and does not need to be set when suspending or dropping a Materialized Table. Resuming a Materialized Table, or altering an activated one, submits a new refresh job to the session's `execution.target` and cluster id, so set them as when creating the Materialized Table.

### 暂停刷新作业
```sql
-- 暂停物化表刷新作业
Flink SQL> ALTER MATERIALIZED TABLE my_materialized_table SUSPEND
[INFO] Execute statement succeeded.
```

### 恢复刷新作业
```sql
-- 恢复物化表刷新作业
Flink SQL> ALTER MATERIALIZED TABLE my_materialized_table RESUME
[INFO] Execute statement succeeded.
```

### 修改查询定义
```sql
-- 修改物化表查询定义
Flink SQL> ALTER MATERIALIZED TABLE my_materialized_table
> AS 
> ...
[INFO] Execute statement succeeded.
```

# Using Table API

Materialized tables in continuous refresh mode can also be managed from any `TableEnvironment`. Full refresh mode still requires SQL Gateway, which schedules the refresh workflows.

Configure the session cluster on the program's `TableConfig`, or in the configuration the program runs with:

| Session cluster | Options |
|---|---|
| Standalone | `execution.target: remote`, `rest.address`, `rest.port` |
| YARN session | `execution.target: yarn-session`, `yarn.application.id` |
| Kubernetes session | `execution.target: kubernetes-session`, `kubernetes.cluster-id` |

```java
TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
tEnv.getConfig().set("execution.target", "remote");
tEnv.getConfig().set("rest.address", "flink-jobmanager");
tEnv.getConfig().set("execution.checkpointing.savepoint-dir", "s3://my-bucket/savepoints");

tEnv.executeSql("CREATE MATERIALIZED TABLE my_materialized_table ...");
```

* A statement that submits a refresh job fails before anything changes if the target can't host one: an unset, `local` or `embedded` target, an application target, or a session target without its cluster id. Application mode requires SQL Gateway.
* Suspending a materialized table, and altering an activated one, stop the refresh job with a savepoint and require `execution.checkpointing.savepoint-dir`.
* Any program that uses the same catalog, and SQL Gateway, can operate the materialized table. The refresh job is reached through the execution target and cluster id recorded in the catalog. For `remote`, configure the same `rest.address` as the program that submitted it.
* Resuming a materialized table, and altering an activated one, restart the refresh job on the program's own target, which must match the recorded one. A refresh job that SQL Gateway deployed in its own application cluster can be suspended, refreshed and dropped from a program, but must be resumed or altered through SQL Gateway.
