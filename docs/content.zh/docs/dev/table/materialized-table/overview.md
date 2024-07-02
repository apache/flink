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
物化表是 Flink 1.20 引入的新功能，旨在简化批处理和流处理开发流程，提供一致的开发体验。 用户只需在创建物化表时指定查询和数据新鲜度，框架将自动选择合适执行模式。

{{< hint warning >}}
该特性处于 MVP 阶段，只支持用于 `sql gateway` 并在 `Flink standalone` 模式下使用，目前不建议在生产环境中使用。
{{< /hint >}}

# 核心概念
物化表包含以下核心概念：

## 数据新鲜度

数据新鲜度用来衡量物化表相比上游数据的滞后程度。在创建物化表后，框架会尽可能满足数据新鲜度的需求，但无法保证完全符合。

数据新鲜度是物化表的重要属性，主要有以下两个作用：
1. **决定刷新模式**：目前有 CONTINUOUS 和 FULL 模式。关于如何确定刷新模式，可以参考 [materialized-table.refresh-mode.freshness-threshold]({{< ref "docs/dev/table/config" >}}#materialized-table-refresh-mode-freshness-threshold) 配置项。
    - CONTINUOUS 模式：实时作业持续刷新物化表。
    - FULL 模式：由调度器定时触发批处理作业刷新物化表。
2. **决定刷新频率**：
    - CONTINUOUS 模式下，数据新鲜度会被转换为后台实时作业的 `checkpoint` 频率。
    - FULL 模式下，数据新鲜度会被转换为工作流的调度周期。

## 刷新模式

刷新模式有 FULL 和 CONTINUOUS 两种。默认情况下，刷新模式根据数据新鲜度推导得出。对于特定业务场景，用户可以显式指定刷新模式，此时优先级高于数据新鲜度推导。

- **CONTINUOUS 模式**：实时作业增量更新物化表数据，数据刷新频率与作业的 `checkpoint` 间隔一致。
- **FULL 模式**：调度器定时触发覆盖更新物化表数据，数据刷新周期与作业的调度周期一致。
    - 默认的覆盖行为是表粒度进行覆盖更新。如果有分区字段，并通过 [partition.fields.#.date-formatter]({{< ref "docs/dev/table/config" >}}#partition-fields-date-formatter) 指定了时间分区字段格式，则按分区覆盖，每次刷新只覆盖最新的分区。


## 查询定义

物化表的查询定义与普通查询一样，查询结果用于填充物化表，在 CONTINUOUS 模式下，查询结果会实时写入物化表，在 FULL 模式下，查询结果会覆盖更新至物化表。

## 表结构

物化表的数据结构和普通的表一样，可以声明主键、水印、分区字段。表结构的字段名和类型都由对应的查询自动推导得出，用户无法指定。


