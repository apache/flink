---
title: Overview
weight: 1
type: docs
bookToc: false
aliases:
  - /dev/table/streaming/
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

# Streaming Concepts

Flink's [Table API]({{< ref "docs/dev/table/tableApi" >}}) and [SQL support]({{< ref "docs/dev/table/sql/overview" >}}) are unified APIs for batch and stream processing.
This means that Table API and SQL queries have the same semantics regardless whether their input is bounded batch input or unbounded stream input.

The following pages explain concepts, practical limitations, and stream-specific configuration parameters of Flink's relational APIs on streaming data.

Where to go next?
-----------------

* [Dynamic Tables]({{< ref "docs/dev/table/concepts/dynamic_tables" >}}): Describes the concept of dynamic tables.
* [Time attributes]({{< ref "docs/dev/table/concepts/time_attributes" >}}): Explains time attributes and how time attributes are handled in Table API & SQL.
* [Versioned Tables]({{< ref "docs/dev/table/concepts/versioned_tables" >}}): Describes the Temporal Table concept.
* [Joins in Continuous Queries]({{< ref "docs/dev/table/concepts/joins" >}}): Different supported types of Joins in Continuous Queries.
* [Query configuration]({{< ref "docs/dev/table/config" >}}): Lists Table API & SQL specific configuration options.

{{< top >}}
