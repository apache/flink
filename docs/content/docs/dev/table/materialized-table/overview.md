---
title: Overview
weight: 1
type: docs
aliases:
- /dev/table/materialized-table.html
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

Materialized Table is a new table type introduced in Flink SQL, aimed at simplifying both batch and stream data pipelines, providing a consistent development experience. By specifying data freshness and query when creating Materialized Table, the engine automatically derives the schema for the materialized table and creates corresponding data refresh pipeline to achieve the specified freshness.

{{< hint warning >}}
**Note**: This feature is currently an MVP (“minimum viable product”) feature and only available within [SQL Gateway]({{< ref "docs/dev/table/sql-gateway/overview" >}}) which connected to a [Standalone]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}) deployed Flink cluster.
{{< /hint >}}

# Core Concepts

Materialized Table encompass the following core concepts: Data Freshness, Refresh Mode, Query Definition and Schema.

## Data Freshness

Data freshness defines the maximum amount of time that the materialized table’s content should lag behind updates to the base tables. Freshness is not a guarantee. Instead, it is a target that Flink attempts to meet. Data in materialized table is refreshed as closely as possible within the freshness.

Data freshness is a crucial attribute of a materialized table, serving two main purposes:
- **Determining the Refresh Mode**. Currently, there are CONTINUOUS and FULL modes. For details on how to determine the refresh mode, refer to the [materialized-table.refresh-mode.freshness-threshold]({{< ref "docs/dev/table/config" >}}#materialized-table-refresh-mode-freshness-threshold) configuration item.
    - CONTINUOUS mode: Launches a Flink streaming job that continuously refreshes the materialized table data.
    - FULL mode: The workflow scheduler periodically triggers a Flink batch job to refresh the materialized table data.
- **Determining the Refresh Frequency**.
    - In CONTINUOUS mode, data freshness is converted into the `checkpoint` interval of the Flink streaming job.
    - In FULL mode, data freshness is converted into the scheduling cycle of the workflow, e.g., a cron expression.

## Refresh Mode

There are two refresh modes: FULL and CONTINUOUS. By default, the refresh mode is inferred based on data freshness. Users can explicitly specify the refresh mode for specific business scenarios, which will take precedence over the data freshness inference.

- **CONTINUOUS Mode**: The Flink streaming job incrementally updates the materialized table data. The visibility of this data depends on the behavior of the corresponding Connector, either being immediate or after checkpoint completion.
- **FULL Mode**: The scheduler periodically triggers a full overwrite of the materialized table data, with the data refresh cycle matching the workflow's scheduling cycle.
  - The default overwrite behavior is table-level. If there are partition fields and the time partition field format is specified via [partition.fields.#.date-formatter]({{< ref "docs/dev/table/config" >}}#partition-fields-date-formatter), the overwrite is by partition. Only the latest partition is refreshed each time.

## Query Definition

The query definition of a materialized table supports all Flink SQL [Queries]({{< ref "docs/dev/table/sql/queries/overview" >}}). The query results are used to populate the materialized table. In CONTINUOUS mode, the query results are updated to the materialized table continuously, while in FULL mode, the query results overwrite the materialized table each time.

## Schema

The schema definition of a materialized table is the same as the regular table. It can declare primary keys and partition keys. The column names and types of materialized table are automatically inferred from the corresponding query, and users cannot specify them.
