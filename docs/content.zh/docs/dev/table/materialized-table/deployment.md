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

- **Client**: 可以是任何能够与 [Flink SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}}) 交互的客户端，如 [SQL 客户端]({{< ref "docs/dev/table/sqlClient" >}})、[Flink JDBC 驱动]({{< ref "docs/dev/table/jdbcDriver" >}}) 等。
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

物化表必须通过 SQL Gateway 创建，SQL Gateway 需要针对元数据持久化和作业调度进行特定的配置。

### 配置 Catalog Store

在 `config.yaml` 中增加 `catalog store` 相关配置：
```yaml
table:
  catalog-store:
    kind: file
    file:
      path: {path_to_catalog_store} # 替换成实际的路径
```
更多详情配置可参考 [Catalog Store]({{<ref "docs/dev/table/catalogs">}}#catalog-store)。

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
Catalog 必须支持创建物化表，目前只有 [Paimon Catalog](https://paimon.apache.org/docs/master/concepts/table-types/#materialized-table) 支持。

# 操作指南

## 连接到 SQL Gateway

使用 SQL Client 的示例：

```shell
./sql-client.sh gateway --endpoint {gateway_endpoint}:{gateway_port}
```

## 创建物化表

### 在 Standalone 集群运行刷新作业

```sql
Flink SQL> SET 'execution.mode' = 'remote';
[INFO] Execute statement succeeded.

FLINK SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ...
[INFO] Execute statement succeeded.
```

### 在 session 模式下运行刷新作业

在 session 模式下执行时，需要提前创建 session 集群，具体可以参考文档 [yarn-session]({{< ref "docs/deployment/resource-providers/yarn" >}}#starting-a-flink-session-on-yarn) 和 [kubernetes-session]({{<ref "docs/deployment/resource-providers/native_kubernetes" >}}#starting-a-flink-session-on-kubernetes)

**Kubernetes session 模式：**

```sql
Flink SQL> SET 'execution.mode' = 'kubernetes-session';
[INFO] Execute statement succeeded.

Flink SQL> SET 'kubernetes.cluster-id' = 'flink-cluster-mt-session-1';
[INFO] Execute statement succeeded.

FLINK SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ...
[INFO] Execute statement succeeded.
```

设置 `execution.mode` 为 `kubernetes-session` 并设置参数 `kubernetes.cluster-id` 指向一个已经存在的 Kubernetes session 集群.

**YARN session 模式：**

```sql
Flink SQL> SET 'execution.mode' = 'yarn-session';
[INFO] Execute statement succeeded.

Flink SQL> SET 'yarn.application.id' = 'application-xxxx';
[INFO] Execute statement succeeded.

FLINK SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ...
[INFO] Execute statement succeeded.
```
设置 `execution.mode` 为 `yarn-session` 并设置参数 `yarn.application.id` 指向一个已经存在的 YARN session 集群。

### 在 application 模式下运行刷新作业

**Kubernetes application 模式：**

```sql
Flink SQL> SET 'execution.mode' = 'kubernetes-application';
[INFO] Execute statement succeeded.

Flink SQL> SET 'kubernetes.cluster-id' = 'flink-cluster-mt-application-1';
[INFO] Execute statement succeeded.

FLINK SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ...
[INFO] Execute statement succeeded.
```
设置 `execution.mode` 为 `kubernetes-application` ，`kubernetes.cluster-id` 是一个可选配置，如果未配置，在提交作业时会自动生成。

**YARN application 模式：**

```sql
Flink SQL> SET 'execution.mode' = 'yarn-application';
[INFO] Execute statement succeeded.

FLINK SQL> CREATE MATERIALIZED TABLE my_materialized_table
> ...
[INFO] Execute statement succeeded.
```
设置 `execution.mode` 为 `yarn-application` ，`yarn.application.id` 无需配置。

## 运维操作

集群信息（如 `execution.mode` 或 `kubernetes.cluster-id`）已持久化在 Catalog 中，暂停或恢复物化表刷新作业时无需重复设置。

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
