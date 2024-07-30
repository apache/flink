---
title: 概览
weight: 1
type: docs
aliases:
- /dev/table/materialized-table/
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

# 介绍

物化表是 Flink SQL 引入的一种新的表类型，旨在简化批处理和流处理数据管道，提供一致的开发体验。在创建物化表时，通过指定数据新鲜度和查询，Flink 引擎会自动推导出物化表的 Schema ，并创建相应的数据刷新管道，以达到指定的新鲜度。

{{< hint warning >}}
**注意**：该功能目前是一个 MVP（最小可行产品）功能，仅在 [SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}})中可用，并且只支持部署作业到 Flink [Standalone]({{< ref "docs/deployment/resource-providers/standalone/overview" >}})集群。
{{< /hint >}}

# 核心概念

物化表包含以下核心概念：数据新鲜度、刷新模式、查询定义和 `Schema` 。

## 数据新鲜度

数据新鲜度定义了物化表数据相对于基础表更新的最大滞后时间。它并非绝对保证，而是 Flink 尝试达到的目标。框架会尽力确保物化表中的数据在指定的新鲜度内刷新。

数据新鲜度是物化表的一个关键属性，具有两个主要作用：
- **确定刷新模式**：目前有连续模式和全量模式。关于如何确定刷新模式的详细信息，请参阅 [materialized-table.refresh-mode.freshness-threshold]({{< ref "docs/dev/table/config" >}}#materialized-table-refresh-mode-freshness-threshold) 配置项。
    - 连续模式：启动 Flink 流作业，持续刷新物化表数据。
    - 全量模式：工作流调度器定期触发 Flink 批处理作业，全量刷新物化表数据。
- **确定刷新频率**：
    - 连续模式下，数据新鲜度转换为 Flink 流作业的 `checkpoint` 间隔。
    - 全量模式下，数据新鲜度转换为工作流的调度周期，例如 `cron` 表达式。

## 刷新模式

刷新模式有连续模式和全量模式两种。默认情况下，根据数据新鲜度推断刷新模式。用户可以为特定业务场景显式指定刷新模式，它的优先级高于根据数据新鲜度推导的刷新模式。

- **连续模式**：Flink 流作业会增量更新物化表数据，下游数据会立即可见，或者等 checkpoint 完成时才可见，由对应的 Connector 行为决定。
- **全量模式**：调度器会定期触发对物化表数据的全量覆盖，其数据刷新周期与工作流的调度周期相匹配。
    - 默认的覆盖行为是表级别的。如果分区字段存在，并且通过 [partition.fields.#.date-formatter]({{< ref "docs/dev/table/config" >}}#partition-fields-date-formatter) 指定了时间分区字段格式，则按照分区粒度覆盖，即每次只刷新最新的分区。

## 查询定义

物化表的查询定义支持所有 Flink SQL [查询]({{< ref "docs/dev/table/sql/queries/overview" >}})。查询结果用于填充物化表。在连续模式下，查询结果会持续更新到物化表中，而在全量模式下，每次查询结果都会覆盖更新到物化表。

## Schema

物化表的 `Schema` 定义与普通表相同，可以声明主键和分区字段。其列名和类型会从相应的查询中推导，用户无法手动指定。

