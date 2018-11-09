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

Flink's [Table API](../tableApi.html) and [SQL support](../sql.html) are unified APIs for batch and stream processing.
This means that Table API and SQL queries have the same semantics regardless whether their input is bounded batch input or unbounded stream input.
Because the relational algebra and SQL were originally designed for batch processing,
relational queries on unbounded streaming input are not as well understood as relational queries on bounded batch input.

The following pages explain concepts, practical limitations, and stream-specific configuration parameters of Flink's relational APIs on streaming data.

Where to go next?
-----------------

* [Dynamic Tables]({{ site.baseurl }}/dev/table/streaming/dynamic_tables.html): Describes the concept of dynamic tables.
* [Time attributes]({{ site.baseurl }}/dev/table/streaming/time_attributes.html): Explains time attributes and how time attributes are handled in Table API & SQL.
* [Joins in Continuous Queries]({{ site.baseurl }}/dev/table/streaming/joins.html): Different supported types of Joins in Continuous Queries.
* [Temporal Tables]({{ site.baseurl }}/dev/table/streaming/temporal_tables.html): Describes the Temporal Table concept.
* [Query configuration]({{ site.baseurl }}/dev/table/streaming/query_configuration.html): Lists Table API & SQL specific configuration options.
