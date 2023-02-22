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

`Query hints` can be used to suggest the optimizer to affect query execution plan within a specified query scope.
Their effective scope is current `Query block`([What are query blocks ?](#what-are-query-blocks-)) which `Query Hints` are specified.
Now, Flink `Query Hints` only support `Join Hints`.

### Syntax
The `Query Hints` syntax in Flink follows the syntax of `Query Hints` in Apache Calcite:
```sql
# Query Hints:
SELECT /*+ hint [, hint ] */ ...

hint:
        hintName
    |   hintName '(' optionKey '=' optionVal [, optionKey '=' optionVal ]* ')'
    |   hintName '(' hintOption [, hintOption ]* ')'

optionKey:
        simpleIdentifier
    |   stringLiteral

optionVal:
        stringLiteral

hintOption:
        simpleIdentifier
    |   numericLiteral
    |   stringLiteral
```

### Join Hints

`Join Hints` allow users to suggest the join strategy to optimizer in order to get a more high-performance execution plan.
Now Flink `Join Hints` support `BROADCAST`, `SHUFFLE_HASH`, `SHUFFLE_MERGE` and `NEST_LOOP`.

{{< hint info >}}
Note:
- The table specified in Join Hints must exist. Otherwise, a table not exists error will be thrown.
- Flink Join Hints only support one hint block in a query block, if multiple hint blocks are specified like `/*+ BROADCAST(t1) */ /*+ SHUFFLE_HASH(t1) */`, an exception will be thrown when parse this query statement.
- In one hint block, specifying multiple tables in a single Join Hint like `/*+ BROADCAST(t1, t2, ..., tn) */` or specifying multiple Join Hints like `/*+ BROADCAST(t1), BROADCAST(t2), ..., BROADCAST(tn) */` are both supported.
- For multiple tables in a single Join Hints or multiple Join Hints in a hint block, Flink Join Hints may conflict. If the conflicts occur, Flink will choose the most matching table or join strategy. (See: [Conflict Cases In Join Hints](#conflict-cases-in-join-hints))
  {{< /hint >}}

#### BROADCAST

{{< label Batch >}}

`BROADCAST` suggests that Flink uses `BroadCast join`. The join side with the hint will be broadcast
regardless of `table.optimizer.join.broadcast-threshold`, so it performs well when the data volume of the hint side of table
is very small.

{{< hint info >}}
Note: BROADCAST only supports join with equivalence join condition, and it doesn't support Full Outer Join.
{{< /hint >}}

##### Examples

```sql
CREATE TABLE t1 (id BIGINT, name STRING, age INT) WITH (...);
CREATE TABLE t2 (id BIGINT, name STRING, age INT) WITH (...);
CREATE TABLE t3 (id BIGINT, name STRING, age INT) WITH (...);

-- Flink will use broadcast join and t1 will be the broadcast table.
SELECT /*+ BROADCAST(t1) */ * FROM t1 JOIN t2 ON t1.id = t2.id;

-- Flink will use broadcast join for both joins and t1, t3 will be the broadcast table.
SELECT /*+ BROADCAST(t1, t3) */ * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t1.id = t3.id;

-- BROADCAST don't support non-equivalent join conditions.
-- Join Hint will not work, and only nested loop join can be applied.
SELECT /*+ BROADCAST(t1) */ * FROM t1 join t2 ON t1.id > t2.id;

-- BROADCAST don't support full outer join.
-- Join Hint will not work in this case, and the planner will choose the appropriate join strategy based on cost.
SELECT /*+ BROADCAST(t1) */ * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;
```

#### SHUFFLE_HASH

{{< label Batch >}}

`SHUFFLE_HASH` suggests that Flink uses `Shuffle Hash join`. The join side with the hint will be the join build side, it performs well when
the data volume of the hint side of table is not too large.

{{< hint info >}}
Note: SHUFFLE_HASH only supports join with equivalence join condition.
{{< /hint >}}

##### Examples

```sql
CREATE TABLE t1 (id BIGINT, name STRING, age INT) WITH (...);
CREATE TABLE t2 (id BIGINT, name STRING, age INT) WITH (...);
CREATE TABLE t3 (id BIGINT, name STRING, age INT) WITH (...);

-- Flink will use hash join and t1 will be the build side.
SELECT /*+ SHUFFLE_HASH(t1) */ * FROM t1 JOIN t2 ON t1.id = t2.id;

-- Flink will use hash join for both joins and t1, t3 will be the join build side.
SELECT /*+ SHUFFLE_HASH(t1, t3) */ * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t1.id = t3.id;

-- SHUFFLE_HASH don't support non-equivalent join conditions.
-- For this case, Join Hint will not work, and only nested loop join can be applied.
SELECT /*+ SHUFFLE_HASH(t1) */ * FROM t1 join t2 ON t1.id > t2.id;
```

#### SHUFFLE_MERGE

{{< label Batch >}}

`SHUFFLE_MERGE` suggests that Flink uses `Sort Merge join`. This type of `Join Hint` is recommended for using in the scenario of joining
between two large tables or the scenario that the data at both sides of the join is already in order.

{{< hint info >}}
Note: SHUFFLE_MERGE only supports join with equivalence join condition.
{{< /hint >}}

##### Examples

```sql
CREATE TABLE t1 (id BIGINT, name STRING, age INT) WITH (...);
CREATE TABLE t2 (id BIGINT, name STRING, age INT) WITH (...);
CREATE TABLE t3 (id BIGINT, name STRING, age INT) WITH (...);

-- Sort merge join strategy is adopted.
SELECT /*+ SHUFFLE_MERGE(t1) */ * FROM t1 JOIN t2 ON t1.id = t2.id;

-- Sort merge join strategy is both adopted in these two joins.
SELECT /*+ SHUFFLE_MERGE(t1, t3) */ * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t1.id = t3.id;

-- SHUFFLE_MERGE don't support non-equivalent join conditions.
-- Join Hint will not work, and only nested loop join can be applied.
SELECT /*+ SHUFFLE_MERGE(t1) */ * FROM t1 join t2 ON t1.id > t2.id;
```

#### NEST_LOOP

{{< label Batch >}}

`NEST_LOOP` suggests that Flink uses `Nested Loop join`. This type of join hint is not recommended without special scenario requirements.

{{< hint info >}}
Note: NEST_LOOP supports both equivalent and non-equivalent join condition.
{{< /hint >}}

##### Examples

```sql
CREATE TABLE t1 (id BIGINT, name STRING, age INT) WITH (...);
CREATE TABLE t2 (id BIGINT, name STRING, age INT) WITH (...);
CREATE TABLE t3 (id BIGINT, name STRING, age INT) WITH (...);

-- Flink will use nested loop join and t1 will be the build side.
SELECT /*+ NEST_LOOP(t1) */ * FROM t1 JOIN t2 ON t1.id = t2.id;

-- Flink will use nested loop join for both joins and t1, t3 will be the join build side.
SELECT /*+ NEST_LOOP(t1, t3) */ * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t1.id = t3.id;
```

#### LOOKUP

{{< label Streaming >}}

The LOOKUP hint allows users to suggest the Flink optimizer to: 
1. use synchronous(sync) or asynchronous(async) lookup function
2. configure the async parameters
3. enable delayed retry strategy for lookup

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

{{< hint info >}}
Note: 
- 'table' option is required, only table name is supported(keep consistent with which in the FROM clause), note that only alias name can be used if table has an alias name.
- async options are all optional, will use default value if not configured.
- there is no default value for retry options, all retry options should be set to valid values when need to enable retry.
{{< /hint >}}

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
{{< hint info >}}
Note: the optimizer prefers async lookup if no 'async' option is specified, it will always use sync lookup when:
1. the connector only implements the sync lookup
2. user enables 'TRY_RESOLVE' mode of ['table.optimizer.non-deterministic-update.strategy']({{< ref "docs/dev/table/config" >}}#table-optimizer-non-deterministic-update-strategy) and the
optimizer has checked there's correctness issue caused by non-deterministic update.
{{< /hint >}}

#### 2. Configure The Async Parameters
Users can configure the async parameters via async options on async lookup mode.

Example:
```sql
-- configure the async parameters: 'output-mode', 'capacity', 'timeout', can set single one or multi params
LOOKUP('table'='Customers', 'async'='true', 'output-mode'='allow_unordered', 'capacity'='100', 'timeout'='180s')
```
{{< hint info >}}
Note: the async options are consistent with the async options in [job level Execution Options]({{< ref "docs/dev/table/config" >}}#execution-options),
will use job level configuration if not set. Another difference is that the scope of the LOOKUP hint
is smaller, limited to the table name corresponding to the hint option set in the current lookup
operation (other lookup operations will not be affected by the LOOKUP hint).
{{< /hint >}}

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

{{< hint info >}}
Note:
- async lookup with retry is not capable for fixed delayed processing for all input data (should use other 
lighter ways to solve, e.g., pending source consumption or use sync lookup with retry)
- delayed waiting for retry execution in sync lookup is fully synchronous, i.e., processing of the 
next record does not begin until the current record has completed.
- in async lookup, if 'output-mode' is 'ORDERED' mode, the probability of backpressure caused by delayed
retry maybe higher than 'UNORDERED' mode, in which case increasing async 'capacity' may not be effective
in reducing backpressure, and it may be necessary to consider reducing the delay duration.
{{< /hint >}}

### Conflict Cases In Join Hints

If the `Join Hints` conflicts occur, Flink will choose the most matching one.
- Conflict in one same Join Hint strategy, Flink will choose the first matching table for a join.
- Conflict in different Join Hints strategies, Flink will choose the first matching hint for a join.

#### Examples

```sql
CREATE TABLE t1 (id BIGINT, name STRING, age INT) WITH (...);
CREATE TABLE t2 (id BIGINT, name STRING, age INT) WITH (...);
CREATE TABLE t3 (id BIGINT, name STRING, age INT) WITH (...);

-- Conflict in One Same Join Hints Strategy Case

-- The first hint will be chosen, t2 will be the broadcast table.
SELECT /*+ BROADCAST(t2), BROADCAST(t1) */ * FROM t1 JOIN t2 ON t1.id = t2.id;

-- BROADCAST(t2, t1) will be chosen, and t2 will be the broadcast table.
SELECT /*+ BROADCAST(t2, t1), BROADCAST(t1, t2) */ * FROM t1 JOIN t2 ON t1.id = t2.id;

-- This case equals to BROADCAST(t1, t2) + BROADCAST(t3),
-- when join between t1 and t2, t1 will be the broadcast table,
-- when join between the result after t1 join t2 and t3, t3 will be the broadcast table.
SELECT /*+ BROADCAST(t1, t2, t3) */ * FROM t1 JOIN t2 ON t1.id = t2.id JOIN t3 ON t1.id = t3.id;


-- Conflict in Different Join Hints Strategies Case

-- The first Join Hint (BROADCAST(t1)) will be chosen, and t1 will be the broadcast table.
SELECT /*+ BROADCAST(t1) SHUFFLE_HASH(t1) */ * FROM t1 JOIN t2 ON t1.id = t2.id;

-- Although BROADCAST is first one hint, but it doesn't support full outer join,
-- so the following SHUFFLE_HASH(t1) will be chosen, and t1 will be the join build side.
SELECT /*+ BROADCAST(t1) SHUFFLE_HASH(t1) */ * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id;

-- Although there are two Join Hints were defined, but all of them are neither support non-equivalent join,
-- so only nested loop join can be applied.
SELECT /*+ BROADCAST(t1) SHUFFLE_HASH(t1) */ * FROM t1 FULL OUTER JOIN t2 ON t1.id > t2.id;
```

### What are query blocks ?

A `query block` is a basic unit of SQL. For example, any inline view or sub-query of a SQL statement are considered separate
`query block` to the outer query.

#### Examples

An SQL statement can consist of several sub-queries. The sub-query can be a `SELECT`, `INSERT` or `DELETE`. A sub-query can contain
other sub-queries in the `FROM` clause, the `WHERE` clause, or a sub-select of a `UNION` or `UNION ALL`.

For these different sub-queries or view types, they can be composed of several `query blocks`, For example:

The simple query below has just one sub-query, but it has two `query blocks` - one for the outer `SELECT` and another for the
sub-query `SELECT`.

{{< img src="/fig/hint/hint_query_block_where.png" alt="hint where query block" >}}

The query below is a union query, which contains two `query blocks` - one for the first `SELECT` and another for the second `SELECT`.

{{< img src="/fig/hint/hint_query_block_union.png" alt="hint union query block" >}}

The query below contains a view, and it has two `query blocks` - one for the outer `SELECT` and another for the view.

{{< img src="/fig/hint/hint_query_block_view.png" alt="hint view query block" >}}


{{< top >}}
