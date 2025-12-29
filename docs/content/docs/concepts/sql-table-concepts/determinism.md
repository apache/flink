---
title: "Determinism in Continuous Queries"
weight: 2
type: docs
aliases:
  - /zh/dev/table/streaming/determinism.html
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

# Determinism In Continuous Queries
This article is about:

1. What is determinism?
2. Is all batch processing deterministic?
   - Two examples of batch queries with non-deterministic results
   - Non-determinism in batch processing
3. Determinism in streaming processing
   - Non-determinism in streaming
   - Non-deterministic update in streaming
   - How to eliminate the impact of non-deterministic update in streaming queries
 
## 1. What Is Determinism?
Quoting the SQL standard's description of determinism: 'an operation is deterministic if that operation assuredly computes identical results when repeated with identical input values'.

## 2. Is All Batch Processing Deterministic?
In a classic batch scenario, repeated execution of the same query for a given bounded data set will yield consistent results, which is the most intuitive understanding of determinism.

In practice, however, the same query does not always return consistent results on a batch process either, looking at two example queries.

### 2.1 Two Examples Of Batch Queries With Non-Deterministic Results
For example, there is a newly created website click log table:
```sql
CREATE TABLE clicks (
    uid VARCHAR(128),
    cTime TIMESTAMP(3),
    url VARCHAR(256)
)
```

Some new data were written:
```
+------+---------------------+------------+
|  uid |               cTime |        url |
+------+---------------------+------------+
| Mary | 2022-08-22 12:00:01 |      /home |
|  Bob | 2022-08-22 12:00:01 |      /home |
| Mary | 2022-08-22 12:00:05 | /prod?id=1 |
|  Liz | 2022-08-22 12:01:00 |      /home |
| Mary | 2022-08-22 12:01:30 |      /cart |
|  Bob | 2022-08-22 12:01:35 | /prod?id=3 |
+------+---------------------+------------+
```

1. Query 1 applies a time filter to the log table and wants to filter out the last 2 minutes logs:
```sql
SELECT * FROM clicks
WHERE cTime BETWEEN TIMESTAMPADD(MINUTE, -2, CURRENT_TIMESTAMP) AND CURRENT_TIMESTAMP;
```

When the query was submitted at '2022-08-22 12:02:00', it returned all 6 rows in the table, and when it was executed again a minute later, at '2022-08-22 12:03:00', it returned only 3 items:
```
+------+---------------------+------------+
|  uid |               cTime |        url |
+------+---------------------+------------+
|  Liz | 2022-08-22 12:01:00 |      /home |
| Mary | 2022-08-22 12:01:30 |      /cart |
|  Bob | 2022-08-22 12:01:35 | /prod?id=3 |
+------+---------------------+------------+
```

2. Query 2 wants to add a unique identifier to each returned record (since the `clicks` table does not have a primary key)
```sql
SELECT UUID() AS uuid, * FROM clicks LIMIT 3;
```

Executing this query twice in a row generates a different `uuid` identifier for each row
```
-- first execution
+--------------------------------+------+---------------------+------------+
|                           uuid |  uid |               cTime |        url |
+--------------------------------+------+---------------------+------------+
| aaaa4894-16d4-44d0-a763-03f... | Mary | 2022-08-22 12:00:01 |      /home |
| ed26fd46-960e-4228-aaf2-0aa... |  Bob | 2022-08-22 12:00:01 |      /home |
| 1886afc7-dfc6-4b20-a881-b0e... | Mary | 2022-08-22 12:00:05 | /prod?id=1 |
+--------------------------------+------+---------------------+------------+

-- second execution
+--------------------------------+------+---------------------+------------+
|                           uuid |  uid |               cTime |        url |
+--------------------------------+------+---------------------+------------+
| 95f7301f-bcf2-4b6f-9cf3-1ea... | Mary | 2022-08-22 12:00:01 |      /home |
| 63301e2d-d180-4089-876f-683... |  Bob | 2022-08-22 12:00:01 |      /home |
| f24456d3-e942-43d1-a00f-fdb... | Mary | 2022-08-22 12:00:05 | /prod?id=1 |
+--------------------------------+------+---------------------+------------+
```

### 2.2 Non-Determinism In Batch Processing
The non-determinism in batch processing mainly caused by the non-deterministic functions, as the two query examples above, where the built-in functions `CURRENT_TIMESTAMP` and `UUID()`
actually behave differently in batch processing, continue with a query example:
```sql
SELECT CURRENT_TIMESTAMP, * FROM clicks;
```

`CURRENT_TIMESTAMP` is the same value on all records returned
```
+-------------------------+------+---------------------+------------+
|       CURRENT_TIMESTAMP |  uid |               cTime |        url |
+-------------------------+------+---------------------+------------+
| 2022-08-23 17:25:46.831 | Mary | 2022-08-22 12:00:01 |      /home |
| 2022-08-23 17:25:46.831 |  Bob | 2022-08-22 12:00:01 |      /home |
| 2022-08-23 17:25:46.831 | Mary | 2022-08-22 12:00:05 | /prod?id=1 |
| 2022-08-23 17:25:46.831 |  Liz | 2022-08-22 12:01:00 |      /home |
| 2022-08-23 17:25:46.831 | Mary | 2022-08-22 12:01:30 |      /cart |
| 2022-08-23 17:25:46.831 |  Bob | 2022-08-22 12:01:35 | /prod?id=3 |
+-------------------------+------+---------------------+------------+
```
This difference is due to the fact that Flink inherits the definition of functions from Apache Calcite, where there are two types of functions other than deterministic function (non-deterministic functions) and dynamic function (dynamic functions, built-in dynamic functions are mainly temporal functions).
The non-deterministic functions are executed at runtime (in clusters, evaluated per record), while the dynamic functions determine the corresponding values only when the query plan is generated(not executed at runtime, different values are obtained at different times, but the same values are obtained at the same execution).
For more information see [System (Built-in) Function Determinism]( {{< ref "docs/dev/table/functions/udfs" >}}#system-built-in-function-determinism).

## 3. Determinism In Streaming Processing
A core difference between streaming and batch is the unboundedness of the data. Flink SQL abstracts streaming processing as the [continuous query on dynamic tables]({{< ref "docs/dev/table/concepts/dynamic_tables" >}}#dynamic-tables-amp-continuous-queries).
So the dynamic function in the batch query example is equivalent to a non-deterministic function in a streaming processing(where logically every change in the base table triggers the query to be executed).
If the `clicks` log table in the example is from a Kafka topic that is continuously written, the same query in stream mode will return `CURRENT_TIMESTAMP` that will change over time
```sql
SELECT CURRENT_TIMESTAMP, * FROM clicks;
```
e.g,
```
+-------------------------+------+---------------------+------------+
|       CURRENT_TIMESTAMP |  uid |               cTime |        url |
+-------------------------+------+---------------------+------------+
| 2022-08-23 17:25:46.831 | Mary | 2022-08-22 12:00:01 |      /home |
| 2022-08-23 17:25:47.001 |  Bob | 2022-08-22 12:00:01 |      /home |
| 2022-08-23 17:25:47.310 | Mary | 2022-08-22 12:00:05 | /prod?id=1 |
+-------------------------+------+---------------------+------------+
```

### 3.1 Non-Determinism In Streaming
In addition to the non-deterministic function, other factors that may generate non-determinism are mainly:
1. non-deterministic back read of source connector
2. query based on [Processing Time]({{< ref "docs/dev/table/concepts/time_attributes" >}}#processing-time) 
3. clear internal state data based on [TTL]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl)

#### Non-Deterministic Back Read Of Source Connector
For Flink SQL, the determinism provided is limited to the computation only, because it does not store user data itself (here it is necessary to distinguish between the managed internal state in streaming mode and the user data itself),
so the Source connector's implementation that cannot provide deterministic back read will bring non-determinism of the input data, which may produce non-deterministic results.
Common examples are inconsistent data for multiple reads from a same offset, or requests for data that no longer exists because of the retention time(e.g., the requested data beyond the configured ttl of Kafka topic).

#### Query Based On Processing Time
Unlike event time, processing time is based on the machine's local time, and this processing does not provide determinism. Related operations that rely on the time attribute include [Window Aggregation]({{< ref "docs/dev/table/sql/queries/window-agg" >}}), [Interval Join]({{< ref "docs/dev/table/sql/queries/joins" >}}#interval-joins), [Temporal Join]({{< ref "docs/dev/table/sql/queries/joins" >}}temporal-joins), etc.
Another typical operation is [Lookup Join]({{< ref "docs/dev/table/sql/queries/joins" >}}#lookup-joins), which is semantically similar to Temporal Join based on processing time, where non-determinism arises when the accessed external table is changing over time.

#### Clear Internal State Data Based On TTL
Because of the unbounded nature of streaming processing, the internal state data maintained by long-running streaming queries in operations such as [Regular Join]({{< ref "docs/dev/table/sql/queries/joins" >}}#regular-joins) and [Group Aggregation]({{< ref "docs/dev/table/sql/queries/group-agg" >}}) (non-windowed aggregation) may continuously get larger.
Setting a state TTL to clean up internal state data is often a necessary compromise, but this may also make the computation results non-deterministic.

The impact of the non-determinism on different queries is different, for some queries it just produces non-deterministic results (the query works fine, but multiple runs fail to produce consistent results), while some queries can have more serious effects, such as incorrect results or runtime errors.
The main reason for the latter one is 'non-deterministic update'.

### 3.2 Non-Deterministic Update In Streaming
Flink SQL implements a complete incremental update mechanism based on the ['continuous query on dynamic tables']({{< ref "docs/dev/table/concepts/dynamic_tables" >}}#dynamic-tables-amp-continuous-queries) abstraction. All operations that need to generate incremental messages maintain complete internal state data, and the operation of the entire query pipeline(include the complete dag from source to sink operators) relies on the guarantee of correct delivery of update messages between operators, which can be broken by non-determinism leading to errors.

What is the 'Non-deterministic Update'(NDU)?
Update messages(changelog) may contain kinds of message types: Insert (I), Delete (D), Update_Before (UB) and Update_After (UA). There's no NDU problem in an insert-only changelog pipeline.
When there is an update message (containing at least one message D, UB, UA in addition to I), the update key of the message (which can be regarded as the primary key of the changelog) is deduced from the query:
- when the update key can be deduced, the operators in the pipeline maintains the internal state by the update key
- when the update key cannot be deduced (it is possible that the primary key is not defined in the [CDC source table]({{< ref "docs/connectors/table/kafka" >}}#cdc-changelog-source) or Sink table, or some operations cannot be deduced from the semantics of the query).
All operators maintaining internal state can only process update (D/UB/UA) messages through complete rows, sink nodes work in retract mode when no primary key is defined, and delete operations are performed by complete rows.

Therefore, in the update-by-row mode, all the update messages received by the operators that need to maintain the state cannot be interfered by the non-deterministic column values, otherwise it will cause NDU problems resulting in computation errors.
On query pipeline with update messages and cannot derive the update key, the following three points are the most important sources of NDU problems:
1. Non-deterministic functions(include scalar, table, aggregate functions, builtin or custom ones)
2. LookupJoin on an evolving source
3. [CDC source]({{< ref "docs/connectors/table/kafka" >}}#cdc-changelog-source) carries metadata fields(system columns, not belongs to the entity row itself)

Note: Exceptions caused by cleaning internal state data based on TTL will be discussed separately as a runtime fault-tolerant handling strategy ([FLINK-24666](https://issues.apache.org/jira/browse/FLINK-24666)).

### 3.3 How To Eliminate The Impact Of Non-Deterministic Update In Streaming
The NDU problem in streaming queries is usually not intuitive, and the risk of the problem may arise from a small change in a complex query.

Since 1.16, Flink SQL ([FLINK-27849](https://issues.apache.org/jira/browse/FLINK-27849)) introduces an experimental NDU handling mechanism ['table.optimizer.non-deterministic-update.strategy']({{< ref "docs/dev/table/config" >}}#table-optimizer-non-deterministic-update-strategy)，
When `TRY_RESOLVE` mode is enabled, it will check whether there is NDU problem in the streaming query and try to eliminate the NDU problem generated by Lookup Join (internal materialization will be added), if there are still factors in point 1 or 3 above that cannot be eliminated automatically,
Flink SQL will give as detailed error messages to prompt the user to adjust the SQL to avoid introducing non-determinism(considering the high cost and complexity of operators brought by materialization, there is no corresponding automatic resolution mechanism supported yet).

#### Best Practices
1. Enable `TRY_RESOLVE` mode before running the streaming query, when you check that there is an unsolvable NDU problem in the query, try to modify the SQL according to the error prompt to avoid the problem proactively.

A real case from [FLINK-27639](https://issues.apache.org/jira/browse/FLINK-27639):
```sql
INSERT INTO t_join_sink
SELECT o.order_id, o.order_name, l.logistics_id, l.logistics_target, l.logistics_source, now()
FROM t_order AS o
LEFT JOIN t_logistics AS l ON ord.order_id=logistics.order_id
```
The execution plan generated by default will run with a runtime error, when `TRY_RESOLVE` mode is enabled, the following error will be given:
```
org.apache.flink.table.api.TableException: The column(s): logistics_time(generated by non-deterministic function: NOW ) can not satisfy the determinism requirement for correctly processing update message(changelogMode contains not only insert ‘I’).... Please consider removing these non-deterministic columns or making them deterministic.

related rel plan:
Calc(select=[CAST(order_id AS INTEGER) AS order_id, order_name, logistics_id, logistics_target, logistics_source, CAST(NOW() AS TIMESTAMP(6)) AS logistics_time], changelogMode=[I,UB,UA,D], upsertKeys=[])
+- Join(joinType=[LeftOuterJoin], where=[=(order_id, order_id0)], select=[order_id, order_name, logistics_id, logistics_target, logistics_source, order_id0], leftInputSpec=[JoinKeyContainsUniqueKey], rightInputSpec=[HasUniqueKey], changelogMode=[I,UB,UA,D], upsertKeys=[])
   :- Exchange(distribution=[hash[order_id]], changelogMode=[I,UB,UA,D], upsertKeys=[{0}])
   :  +- TableSourceScan(table=[[default_catalog, default_database, t_order, project=[order_id, order_name], metadata=[]]], fields=[order_id, order_name], changelogMode=[I,UB,UA,D], upsertKeys=[{0}])
   +- Exchange(distribution=[hash[order_id]], changelogMode=[I,UB,UA,D], upsertKeys=[])
      +- TableSourceScan(table=[[default_catalog, default_database, t_logistics, project=[logistics_id, logistics_target, logistics_source, order_id], metadata=[]]], fields=[logistics_id, logistics_target, logistics_source, order_id], changelogMode=[I,UB,UA,D], upsertKeys=[{0}])
```
Follow the error prompt, remove the `now()` function or use other deterministic functions instead (or use the time field in the `order` table), you can eliminate the above NDU problem and avoid runtime errors.

2. When using Lookup Join, try to declare the primary key(if exists)
Lookup source table with primary key definitions can in many cases prevent Flink SQL from deriving update keys, thus saving the high materialization cost

The following two examples shows why users are encouraged to declare primary keys for lookup source tables：
```sql
insert into sink_with_pk
select t1.a, t1.b, t2.c
from (
  select *, proctime() proctime from cdc
) t1 
join dim_with_pk for system_time as of t1.proctime as t2
   on t1.a = t2.a
   
-- plan: the upsertKey of left stream is reserved when lookup table with a pk definition and use it as lookup key, so that the high cost materialization can be omitted.
Sink(table=[default_catalog.default_database.sink_with_pk], fields=[a, b, c])
+- Calc(select=[a, b, c])
   +- LookupJoin(table=[default_catalog.default_database.dim_with_pk], joinType=[InnerJoin], lookup=[a=a], select=[a, b, a, c])
      +- DropUpdateBefore
         +- TableSourceScan(table=[[default_catalog, default_database, cdc, project=[a, b], metadata=[]]], fields=[a, b])   
```

```sql
insert into sink_with_pk
select t1.a, t1.b, t2.c
from (
  select *, proctime() proctime from cdc
) t1 
join dim_without_pk for system_time as of t1.proctime as t2
   on t1.a = t2.a

-- execution plan when `TRY_RESOLVE` is enabled(may encounter errors at runtime when `TRY_RESOLVE` mode is not enabled):
Sink(table=[default_catalog.default_database.sink_with_pk], fields=[a, b, c], upsertMaterialize=[true])
+- Calc(select=[a, b, c])
   +- LookupJoin(table=[default_catalog.default_database.dim_without_pk], joinType=[InnerJoin], lookup=[a=a], select=[a, b, a, c], upsertMaterialize=[true])
      +- TableSourceScan(table=[[default_catalog, default_database, cdc, project=[a, b], metadata=[]]], fields=[a, b])
```
Though the second case can be solved by adding materialization if `TRY_RESOLVE` is enabled, but the cost is very high, there will be two more costly materialization compared to the one with primary key.

3. When the lookup source table accessed by Lookup Join is static, the `TRY_RESOLVE` mode may not be enabled
When Lookup Join accesses a static lookup source table, you can first turn on `TRY_RESOLVE` mode to check that there are no other NDU problems, and then restore `IGNORE` mode to avoid unnecessary materialization overhead.
Note: Here you need to ensure that the lookup source table is purely static and not updated, otherwise `IGNORE` mode is not safe.

{{< top >}}
