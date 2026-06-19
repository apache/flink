---
title: SQL Interfaces
weight: 2
type: docs
bookCollapseSection: true
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

# SQL Interfaces

Flink provides multiple interfaces for executing SQL queries:

- **[SQL Client]({{< ref "docs/sql/interfaces/sql-client" >}})**: An interactive command-line interface for submitting SQL statements to a Flink cluster.
- **[SQL Gateway]({{< ref "docs/sql/interfaces/sql-gateway/overview" >}})**: A service that enables multiple clients to execute SQL queries concurrently, with support for REST and HiveServer2 protocols.
- **[JDBC Driver]({{< ref "docs/sql/interfaces/jdbc-driver" >}})**: A standard JDBC driver for connecting to Flink SQL Gateway from Java applications and SQL tools.

{{< top >}}
