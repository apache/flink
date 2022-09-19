---
title: HiveServer2 Endpoint
weight: 3
type: docs
aliases:
- /dev/table/sql-gateway/hiveserver2.html
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

# HiveServer2 Endpoint

HiveServer2 Endpoint is compatible with [HiveServer2](https://cwiki.apache.org/confluence/display/hive/hiveserver2+overview)
wire protocol and allows users to interact (e.g. submit Hive SQL) with Flink SQL Gateway with existing Hive clients, such as Hive JDBC, Beeline, DBeaver, Apache Superset and so on.

It suggests to use HiveServer2 Endpoint with Hive Catalog and Hive dialect to get the same experience
as HiveServer2. Please refer to the [Hive Compatibility]({{< ref "docs/dev/table/hive-compatibility/hiveserver2" >}})
for more details. 
