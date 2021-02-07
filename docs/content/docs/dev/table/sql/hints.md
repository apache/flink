---
title: "SQL Hints"
weight: 7
type: docs
aliases:
  - /dev/table/sql/hints.html
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

# SQL Hints



SQL hints can be used with SQL statements to alter execution plans. This chapter explains how to use hints to force various approaches.

Generally a hint can be used to:

- Enforce planner: there's no perfect planner, so it makes sense to implement hints to
allow user better control the execution;
- Append meta data(or statistics): some statistics like “table index for scan” and
“skew info of some shuffle keys” are somewhat dynamic for the query, it would be very
convenient to config them with hints because our planning metadata from the planner is very
often not that accurate;
- Operator resource constraints: for many cases, we would give a default resource
configuration for the execution operators, i.e. min parallelism or
managed memory (resource consuming UDF) or special resource requirement (GPU or SSD disk)
and so on, it would be very flexible to profile the resource with hints per query(instead of the Job).

## Dynamic Table Options
Dynamic table options allows to specify or override table options dynamically, different with static table options defined with SQL DDL or connect API, 
these options can be specified flexibly in per-table scope within each query.

Thus it is very suitable to use for the ad-hoc queries in interactive terminal, for example, in the SQL-CLI,
you can specify to ignore the parse error for a CSV source just by adding a dynamic option `/*+ OPTIONS('csv.ignore-parse-errors'='true') */`.

<b>Note:</b> Dynamic table options default is forbidden to use because it may change the semantics of the query.
You need to set the config option `table.dynamic-table-options.enabled` to be `true` explicitly (default is false),
See the <a href="{{< ref "docs/dev/table/config" >}}">Configuration</a> for details on how to set up the config options.

### Syntax
In order to not break the SQL compatibility, we use the Oracle style SQL hint syntax:
```sql
table_path /*+ OPTIONS(key=val [, key=val]*) */

key:
    stringLiteral
val:
    stringLiteral

```

### Examples

```sql

CREATE TABLE kafka_table1 (id BIGINT, name STRING, age INT) WITH (...);
CREATE TABLE kafka_table2 (id BIGINT, name STRING, age INT) WITH (...);

-- override table options in query source
select id, name from kafka_table1 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */;

-- override table options in join
select * from
    kafka_table1 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ t1
    join
    kafka_table2 /*+ OPTIONS('scan.startup.mode'='earliest-offset') */ t2
    on t1.id = t2.id;

-- override table options for INSERT target table
insert into kafka_table1 /*+ OPTIONS('sink.partitioner'='round-robin') */ select * from kafka_table2;

```

{{< top >}}
