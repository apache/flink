---
title: "Streaming Concepts"
nav-id: streaming_tableapi
nav-parent_id: tableapi
nav-pos: 10
is_beta: false
nav-show_overview: true
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

Flink's [Table API]({% link dev/table/tableApi.md %}) and [SQL support]({% link dev/table/sql/index.md %}) are unified APIs for batch and stream processing.
This means that Table API and SQL queries have the same semantics regardless whether their input is bounded batch input or unbounded stream input.

The following pages explain concepts, practical limitations, and stream-specific configuration parameters of Flink's relational APIs on streaming data.

Where to go next?
-----------------

* [Dynamic Tables]({% link dev/table/streaming/dynamic_tables.md %}): Describes the concept of dynamic tables.
* [Time attributes]({% link dev/table/streaming/time_attributes.md %}): Explains time attributes and how time attributes are handled in Table API & SQL.
* [Versioned Tables]({% link dev/table/streaming/versioned_tables.md %}): Describes the Temporal Table concept.
* [Joins in Continuous Queries]({% link dev/table/streaming/joins.md %}): Different supported types of Joins in Continuous Queries.
* [Query configuration]({% link dev/table/streaming/query_configuration.md %}): Lists Table API & SQL specific configuration options.

{% top %}
