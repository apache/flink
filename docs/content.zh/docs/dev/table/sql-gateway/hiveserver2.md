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

HiveServer2 Endpoint 兼容 [HiveServer2](https://cwiki.apache.org/confluence/display/hive/hiveserver2+overview)
协议，允许用户使用 Hive JDBC、Beeline、DBeaver、Apache Superset 等 Hive 客户端和 Flink SQL Gateway 交互（例如提交 Hive SQL）。

建议将 HiveServer2 Endpoint 和 Hive Catalog、Hive 方言一起使用，以获得与 HiveServer2 一样的使用体验。
请参阅 [Hive Compatibility]({{< ref "docs/dev/table/hive-compatibility/hiveserver2" >}}) 了解更多详情。 
