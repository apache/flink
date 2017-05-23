---
title: "Streaming Concepts"
nav-parent_id: tableapi
nav-pos: 10
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

**TODO: has to be completely written**

* This will be replaced by the TOC
{:toc}

Dynamic table
-------------

* Stream -> Table
* Table -> Stream
* update changes / retraction

{% top %}

Time Attributes
---------------

### Event-time

* DataStream: Timestamps & WMs required, `.rowtime` (replace attribute or extend schema)
* TableSource: Timestamps & WMs & DefinedRowtimeAttribute

{% top %}

### Processing time

* DataStream: `.proctime` (only extend schema)
* TableSource: DefinedProctimeAttribute

{% top %}

Query Configuration
-------------------

In stream processing, compuations are constantly happening and there are many use cases that require to update previously emitted results. There are many ways in which a query can compute and emit updates. These do not affect the semantics of the query but might lead to approximated results. 

Flink's Table API and SQL interface use a `QueryConfig` to control the computation and emission of results and updates.

### State Retention

{% top %}


