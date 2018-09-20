---
title: "Joins in Continuous Queries"
nav-parent_id: streaming_tableapi
nav-pos: 3
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

When we have two tables that we want to connect such operation can usually be expressed via some kind of join.
In batch processing joins can be efficiently executed, since we are working on a bounded completed data sets.
In stream processing things are a little bit more complicated,
especially when it comes to the issue how to handle that data can change over time.
Because of that, there are a couple of ways to actually perform the join using either Table API or SQL.

For more information regarding the syntax please check the Joins sections in [Table API](../tableApi.html#joins) and [SQL](../sql.html#joins).

* This will be replaced by the TOC
{:toc}

Regular Joins
-------------

This is the most basic case in which any new records or changes to either side of the join input are visible
and are affecting the whole join result.
For example, if there is a new record on the left side,
it will be joined with all of the previous and future records on the other side.

These semantics have an important implication:
it requires to keep both sides of the join input in the state indefinitely
and resource usage will grow indefinitely as well,
if one or both input tables are continuously growing.

Example:
{% highlight sql %}
SELECT * FROM Orders
INNER JOIN Product
ON Orders.productId = Product.id
{% endhighlight %}

Time-windowed Joins
-------------------

A time-windowed join is defined by a join predicate,
that checks if [the time attributes](time_attributes.html) of the input records are within a time-window.
Since time attributes are quasi-monontic increasing,
Flink can remove old values from the state without affecting the correctness of the result.

Example:
{% highlight sql %}
SELECT *
FROM
  Orders o,
  Shipments s
WHERE o.id = s.orderId AND
      o.ordertime BETWEEN s.shiptime - INTERVAL '4' HOUR AND s.shiptime
{% endhighlight %}

Join with a Temporal Table
--------------------------

A Temporal Table Join joins an append-only table (left input/probe side) with a [Temporal Tables](temporal_tables.html) (right input/build side),
i.e. a table that changes over time and tracks its changes.
For each record from the probe side, it will be joined only with the latest version of the build side.
That means (in contrast to [Regular Joins](#regular-joins)) if there is a new record on the build side,
it will not affect the previous results of the join.
This again allow Flink to limit the number of elements that must be kept on the state.
In order to support updates (overwrites) of previous values on the build side table, this table must define a primary key.

Compared to [Time-windowed Joins](#time-windowed-joins),
Temporal Table Joins are not defining a time window within which bounds the records will be joined.
Records from the probe side are joined with the most recent versions of the build side and records on the build side might be arbitrary old.
As time passes the previous, no longer needed, versions of the record (for the given primary key) will be removed from the state.

Such behaviour makes temporal table join a good candidate to express stream enrichment.

Example:
{% highlight sql %}
SELECT
  o.amount * r.rate AS amount
FROM
  Orders AS o,
  LATERAL TABLE (Rates(o.proctime)) AS r
WHERE r.currency = o.currency
{% endhighlight %}

For more information about this concept please check [Temporal Tables](temporal_tables.html) page.


