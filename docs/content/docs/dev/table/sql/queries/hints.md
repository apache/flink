---
title: "Hints"
weight: 2
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

{{< label Batch >}} {{< label Streaming >}}

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

## Query Hints

### Join Hints

#### LOOKUP

{{< label Streaming >}}

The LOOKUP hint allows users to suggest the Flink optimizer to: 
1. use synchronous(sync) or asynchronous(async) lookup function
2. configure the async parameters
3. enable delayed retry strategy for lookup

#### Syntax
```sql
SELECT /*+ LOOKUP(key=value[, key=value]*) */

key:
    stringLiteral

value:
    stringLiteral
```

#### LOOKUP Hint Options:

<table class="table table-bordered">
<thead>
<tr>
	<th>option type</th>
	<th>option name</th>
	<th>required</th>
	<th>value type</th>
	<th>default value</th>
	<th class="text-left">description</th>
</tr>
</thead>
<tbody>
<tr>
	<td rowspan="1">table</td>
	<td>table</td>
	<td>Y</td>
	<td>string</td>
	<td>N/A</td>
	<td>the table name of the lookup source</td>
</tr>
<tr>
	<td rowspan="4">async</td>
	<td>async</td>
	<td>N</td>
	<td>boolean</td>
	<td>N/A</td>
	<td>value can be 'true' or 'false' to suggest the planner choose the corresponding lookup function.
        If the backend lookup source does not support the suggested lookup mode, it will take no effect.</td>
</tr>
<tr>
	<td>output-mode</td>
	<td>N</td>
	<td>string</td>
	<td>ordered</td>
	<td>value can be 'ordered' or 'allow_unordered'.<br />'allow_unordered' means if users allow unordered result, it will attempt to use AsyncDataStream.OutputMode.UNORDERED when it does not affect the correctness of the result, otherwise ORDERED will be still used. It is consistent with <br />`ExecutionConfigOptions#TABLE_EXEC_ASYNC_LOOKUP_OUTPUT_MODE`.</td>
</tr>
<tr>
	<td>capacity</td>
	<td>N</td>
	<td>integer</td>
	<td>100</td>
	<td>the buffer capacity for the backend asyncWaitOperator of the lookup join operator.</td>
</tr>
<tr>
	<td>timeout</td>
	<td>N</td>
	<td>duration</td>
	<td>300s</td>
	<td>timeout from first invoke to final completion of asynchronous operation, may include multiple retries, and will be reset in case of failover</td>
</tr>
<tr>
	<td rowspan="4">retry</td>
	<td>retry-predicate</td>
	<td>N</td>
	<td>string</td>
	<td>N/A</td>
	<td>can be 'lookup_miss' which will enable retry if lookup result is empty.</td>
</tr>
<tr>
	<td>retry-strategy</td>
	<td>N</td>
	<td>string</td>
	<td>N/A</td>
	<td>can be 'fixed_delay'</td>
</tr>
<tr>
	<td>fixed-delay</td>
	<td>N</td>
	<td>duration</td>
	<td>N/A</td>
	<td>delay time for the 'fixed_delay' strategy</td>
</tr>
<tr>
	<td>max-attempts</td>
	<td>N</td>
	<td>integer</td>
	<td>N/A</td>
	<td>max attempt number of the 'fixed_delay' strategy</td>
</tr>
</tbody>
</table>

Note: 
- 'table' option is required, only table name is supported(keep consistent with which in the FROM clause), alias name is not supported currently(will be supported in later versions).
- async options are all optional, will use default value if not configured.
- there is no default value for retry options, all retry options should be set to valid values when need to enable retry.

#### 1. Use Sync And Async Lookup Function
If the connector has both capabilities of async and sync lookup, users can give the option value 'async'='false'
to suggest the planner to use the sync lookup or 'async'='true' to use the async lookup:

Example:
```sql
-- suggest the optimizer to use sync lookup
LOOKUP('table'='Customers', 'async'='false')

-- suggest the optimizer to use async lookup
LOOKUP('table'='Customers', 'async'='true')
```
Note: the optimizer prefers async lookup if no 'async' option is specified, it will always use sync lookup when:
1. the connector only implements the sync lookup
2. user enables 'TRY_RESOLVE' mode of ['table.optimizer.non-deterministic-update.strategy']({{< ref "docs/dev/table/config" >}}#table-optimizer-non-deterministic-update-strategy) and the
optimizer has checked there's correctness issue caused by non-deterministic update.

#### 2. Configure The Async Parameters
Users can configure the async parameters via async options on async lookup mode.

Example:
```sql
-- configure the async parameters: 'output-mode', 'capacity', 'timeout', can set single one or multi params
LOOKUP('table'='Customers', 'async'='true', 'output-mode'='allow_unordered', 'capacity'='100', 'timeout'='180s')
```
Note: the async options are consistent with the async options in [job level Execution Options]({{< ref "docs/dev/table/config" >}}#execution-options),
will use job level configuration if not set. Another difference is that the scope of the LOOKUP hint
is smaller, limited to the table name corresponding to the hint option set in the current lookup
operation (other lookup operations will not be affected by the LOOKUP hint).

e.g., if the job level configuration is:
```gitexclude
table.exec.async-lookup.output-mode: ORDERED
table.exec.async-lookup.buffer-capacity: 100
table.exec.async-lookup.timeout: 180s
```

then the following hints:
```sql
1. LOOKUP('table'='Customers', 'async'='true', 'output-mode'='allow_unordered')
2. LOOKUP('table'='Customers', 'async'='true', 'timeout'='300s')
```

are equivalent to:
```sql
1. LOOKUP('table'='Customers', 'async'='true', 'output-mode'='allow_unordered', 'capacity'='100', 'timeout'='180s')
2. LOOKUP('table'='Customers', 'async'='true', 'output-mode'='ordered', 'capacity'='100', 'timeout'='300s')
```

#### 3. Enable Delayed Retry Strategy For Lookup
Delayed retry for lookup join is intended to solve the problem of delayed updates in external system
which cause unexpected enrichment with stream data. The hint option 'retry-predicate'='lookup_miss'
can enable retry on both sync and async lookup, only fixed delay retry strategy is supported currently.

Options of fixed delay retry strategy:
```gitexclude
'retry-strategy'='fixed_delay'
-- fixed delay duration
'fixed-delay'='10s'
-- max number of retry(counting from the retry operation, if set to '1', then a single lookup process
-- for a specific lookup key will actually execute up to 2 lookup requests)
'max-attempts'='3'
```

Example:
1. enable retry on async lookup
```sql
LOOKUP('table'='Customers', 'async'='true', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s','max-attempts'='3')
```

2. enable retry on sync lookup
```sql
LOOKUP('table'='Customers', 'async'='false', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s','max-attempts'='3')
```

If the lookup source only has one capability, then the 'async' mode option can be omitted:

```sql
LOOKUP('table'='Customers', 'retry-predicate'='lookup_miss', 'retry-strategy'='fixed_delay', 'fixed-delay'='10s','max-attempts'='3')
```

#### Further Notes

#### Effect Of Enabling Caching On Retries
[FLIP-221](https://cwiki.apache.org/confluence/display/FLINK/FLIP-221%3A+Abstraction+for+lookup+source+cache+and+metric) adds caching support for lookup source,
which has PARTIAL and FULL caching mode(the mode NONE means disable caching). When FULL caching is enabled, there'll
be no retry at all(because it's meaningless to retry lookup via a full cached mirror of lookup source).
When PARTIAL caching is enabled, it will lookup from local cache first for a coming record and will
do an external lookup via backend connector if cache miss(if cache hit, then return the record immediately),
and this will trigger a retry when lookup result is empty(same with caching disabled), the final lookup
result is determined when retry completed(in PARTIAL caching mode, it will also update local cache).


#### Note On Lookup Keys And 'retry-predicate'='lookup_miss' Retry Conditions
For different connectors, the index-lookup capability maybe different, e.g., builtin HBase connector
can lookup on rowkey only (without secondary index), while builtin JDBC connector can provide more
powerful index-lookup capabilities on arbitrary columns, this is determined by the different physical
storages.
The lookup key mentioned here is the field or combination of fields for the index-lookup,
as the example of [`lookup join`]({{< ref "docs/dev/table/sql/queries/joins" >}}#lookup-join), where
`c.id` is the lookup key of the join condition "ON o.customer_id = c.id":

```sql
SELECT o.order_id, o.total, c.country, c.zip
FROM Orders AS o
  JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c
    ON o.customer_id = c.id
```

if we change the join condition to "ON o.customer_id = c.id and c.country = 'US'"：
```sql
SELECT o.order_id, o.total, c.country, c.zip
FROM Orders AS o
  JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c
    ON o.customer_id = c.id and c.country = 'US'
```

both `c.id` and `c.country` will be used as lookup key when `Customers` table was stored in MySql:
```sql
CREATE TEMPORARY TABLE Customers (
  id INT,
  name STRING,
  country STRING,
  zip STRING
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysqlhost:3306/customerdb',
  'table-name' = 'customers'
)
```

only `c.id` can be the lookup key when `Customers` table was stored in HBase, and the remaining join
condition `c.country = 'US'` will be evaluated after lookup result returned
```sql
CREATE TEMPORARY TABLE Customers (
  id INT,
  name STRING,
  country STRING,
  zip STRING,
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'hbase-2.2',
  ...
)
```

Accordingly, the above query will have different retry effects on different storages when enable
'lookup_miss' retry predicate and the fixed-delay retry strategy.

e.g., if there is a row in the `Customers` table:
```gitexclude
id=100, country='CN'
```
When processing an record with 'id=100' in the order stream, in 'jdbc' connector, the corresponding
lookup result is null (`country='CN'` does not satisfy the condition `c.country = 'US'`) because both
`c.id` and `c.country` are used as lookup keys, so this will trigger a retry.

When in 'hbase' connector, only `c.id` will be used as the lookup key, the corresponding lookup result
will not be empty(it will return the record `id=100, country='CN'`), so it will not trigger a retry
(the remaining join condition `c.country = 'US'` will be evaluated as not true for returned record).

Currently, based on SQL semantic considerations, only the 'lookup_miss' retry predicate is provided,
and when it is necessary to wait for delayed updates of the dimension table (where a historical
version record already exists in the table, rather than not), users can try two solutions:
1. implements a custom retry predicate with the new retry support in DataStream Async I/O (allows for more complex judgments on returned records).
2. enable delayed retry by adding another join condition including comparison on some kind of data version generated by timestamp
for the above example, assume the `Customers` table is updated every hour, we can add a new time-dependent
version field `update_version`, which is reserved to hourly precision, e.g., update time '2022-08-15 12:01:02'
of record will store the `update_version` as ' 2022-08-15 12:00'
```sql
CREATE TEMPORARY TABLE Customers (
  id INT,
  name STRING,
  country STRING,
  zip STRING,
  -- the newly added time-dependent version field
  update_version STRING
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysqlhost:3306/customerdb',
  'table-name' = 'customers'
)
```

append an equal condition on both time field of Order stream and `Customers`.`update_version` to the join condition:
```sql
ON o.customer_id = c.id AND DATE_FORMAT(o.order_timestamp, 'yyyy-MM-dd HH:mm') = c.update_version
```
then we can enable delayed retry when Order's record can not lookup the new record with '12:00' version in `Customers` table. 


#### Trouble Shooting
When turning on the delayed retry lookup, it is more likely to encounter a backpressure problem in 
the lookup node, this can be quickly confirmed via the 'Thread Dump' on the 'Task Manager' page of web ui.
From async and sync lookups respectively, call stack of thread sleep will appear in:
1. async lookup：`RetryableAsyncLookupFunctionDelegator`
2. sync lookup：`RetryableLookupFunctionDelegator`

Note:
- async lookup with retry is not capable for fixed delayed processing for all input data (should use other 
lighter ways to solve, e.g., pending source consumption or use sync lookup with retry)
- delayed waiting for retry execution in sync lookup is fully synchronous, i.e., processing of the 
next record does not begin until the current record has completed.
- in async lookup, if 'output-mode' is 'ORDERED' mode, the probability of backpressure caused by delayed
retry maybe higher than 'UNORDERED' mode, in which case increasing async 'capacity' may not be effective
in reducing backpressure, and it may be necessary to consider reducing the delay duration.

{{< top >}}
