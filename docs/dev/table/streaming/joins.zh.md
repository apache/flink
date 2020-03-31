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

Joins are a common and well-understood operation in batch data processing to connect the rows of two relations. However, the semantics of joins on [dynamic tables](dynamic_tables.html) are much less obvious or even confusing.

Because of that, there are a couple of ways to actually perform a join using either Table API or SQL.

For more information regarding the syntax, please check the join sections in [Table API](../tableApi.html#joins) and [SQL](../sql.html#joins).

* This will be replaced by the TOC
{:toc}

Regular Joins
-------------

Regular joins are the most generic type of join in which any new records or changes to either side of the join input are visible and are affecting the whole join result.
For example, if there is a new record on the left side, it will be joined with all of the previous and future records on the right side.

{% highlight sql %}
SELECT * FROM Orders
INNER JOIN Product
ON Orders.productId = Product.id
{% endhighlight %}

These semantics allow for any kind of updating (insert, update, delete) input tables.

However, this operation has an important implication: it requires to keep both sides of the join input in Flink's state forever.
Thus, the resource usage will grow indefinitely as well, if one or both input tables are continuously growing.

Time-windowed Joins
-------------------

A time-windowed join is defined by a join predicate, that checks if the [time attributes](time_attributes.html) of the input
records are within certain time constraints, i.e., a time window.

{% highlight sql %}
SELECT *
FROM
  Orders o,
  Shipments s
WHERE o.id = s.orderId AND
      o.ordertime BETWEEN s.shiptime - INTERVAL '4' HOUR AND s.shiptime
{% endhighlight %}

Compared to a regular join operation, this kind of join only supports append-only tables with time attributes. Since time attributes are quasi-monontic increasing, Flink can remove old values from its state without affecting the correctness of the result.

Join with a Temporal Table Function
--------------------------

A join with a temporal table function joins an append-only table (left input/probe side) with a temporal table (right input/build side),
i.e., a table that changes over time and tracks its changes. Please check the corresponding page for more information about [temporal tables](temporal_tables.html).

The following example shows an append-only table `Orders` that should be joined with the continuously changing currency rates table `RatesHistory`.

`Orders` is an append-only table that represents payments for the given `amount` and the given `currency`.
For example at `10:15` there was an order for an amount of `2 Euro`.

{% highlight sql %}
SELECT * FROM Orders;

rowtime amount currency
======= ====== =========
10:15        2 Euro
10:30        1 US Dollar
10:32       50 Yen
10:52        3 Euro
11:04        5 US Dollar
{% endhighlight %}

`RatesHistory` represents an ever changing append-only table of currency exchange rates with respect to `Yen` (which has a rate of `1`).
For example, the exchange rate for the period from `09:00` to `10:45` of `Euro` to `Yen` was `114`. From `10:45` to `11:15` it was `116`.

{% highlight sql %}
SELECT * FROM RatesHistory;

rowtime currency   rate
======= ======== ======
09:00   US Dollar   102
09:00   Euro        114
09:00   Yen           1
10:45   Euro        116
11:15   Euro        119
11:49   Pounds      108
{% endhighlight %}

Given that we would like to calculate the amount of all `Orders` converted to a common currency (`Yen`).

For example, we would like to convert the following order using the appropriate conversion rate for the given `rowtime` (`114`).

{% highlight text %}
rowtime amount currency
======= ====== =========
10:15        2 Euro
{% endhighlight %}

Without using the concept of [temporal tables](temporal_tables.html), one would need to write a query like:

{% highlight sql %}
SELECT
  SUM(o.amount * r.rate) AS amount
FROM Orders AS o,
  RatesHistory AS r
WHERE r.currency = o.currency
AND r.rowtime = (
  SELECT MAX(rowtime)
  FROM RatesHistory AS r2
  WHERE r2.currency = o.currency
  AND r2.rowtime <= o.rowtime);
{% endhighlight %}

With the help of a temporal table function `Rates` over `RatesHistory`, we can express such a query in SQL as:

{% highlight sql %}
SELECT
  o.amount * r.rate AS amount
FROM
  Orders AS o,
  LATERAL TABLE (Rates(o.rowtime)) AS r
WHERE r.currency = o.currency
{% endhighlight %}

Each record from the probe side will be joined with the version of the build side table at the time of the correlated time attribute of the probe side record.
In order to support updates (overwrites) of previous values on the build side table, the table must define a primary key.

In our example, each record from `Orders` will be joined with the version of `Rates` at time `o.rowtime`. The `currency` field has been defined as the primary key of `Rates` before and is used to connect both tables in our example. If the query were using a processing-time notion, a newly appended order would always be joined with the most recent version of `Rates` when executing the operation.

In contrast to [regular joins](#regular-joins), this means that if there is a new record on the build side, it will not affect the previous results of the join.
This again allows Flink to limit the number of elements that must be kept in the state.

Compared to [time-windowed joins](#time-windowed-joins), temporal table joins do not define a time window within which bounds the records will be joined.
Records from the probe side are always joined with the build side's version at the time specified by the time attribute. Thus, records on the build side might be arbitrarily old.
As time passes, the previous and no longer needed versions of the record (for the given primary key) will be removed from the state.

Such behaviour makes a temporal table join a good candidate to express stream enrichment in relational terms.

### Usage

After [defining temporal table function](temporal_tables.html#defining-temporal-table-function), we can start using it.
Temporal table functions can be used in the same way as normal table functions would be used.

The following code snippet solves our motivating problem of converting currencies from the `Orders` table:

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
SELECT
  SUM(o_amount * r_rate) AS amount
FROM
  Orders,
  LATERAL TABLE (Rates(o_proctime))
WHERE
  r_currency = o_currency
{% endhighlight %}
</div>
<div data-lang="java" markdown="1">
{% highlight java %}
Table result = orders
    .joinLateral("rates(o_proctime)", "o_currency = r_currency")
    .select("(o_amount * r_rate).sum as amount");
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val result = orders
    .joinLateral(rates('o_proctime), 'r_currency === 'o_currency)
    .select(('o_amount * 'r_rate).sum as 'amount)
{% endhighlight %}
</div>
</div>

**Note**: State retention defined in a [query configuration](query_configuration.html) is not yet implemented for temporal joins.
This means that the required state to compute the query result might grow infinitely depending on the number of distinct primary keys for the history table.

### Processing-time Temporal Joins

With a processing-time time attribute, it is impossible to pass _past_ time attributes as an argument to the temporal table function.
By definition, it is always the current timestamp. Thus, invocations of a processing-time temporal table function will always return the latest known versions of the underlying table
and any updates in the underlying history table will also immediately overwrite the current values.

Only the latest versions (with respect to the defined primary key) of the build side records are kept in the state.
Updates of the build side will have no effect on previously emitted join results.

One can think about a processing-time temporal join as a simple `HashMap<K, V>` that stores all of the records from the build side.
When a new record from the build side has the same key as some previous record, the old value is just simply overwritten.
Every record from the probe side is always evaluated against the most recent/current state of the `HashMap`.

### Event-time Temporal Joins

With an event-time time attribute (i.e., a rowtime attribute), it is possible to pass _past_ time attributes to the temporal table function.
This allows for joining the two tables at a common point in time.

Compared to processing-time temporal joins, the temporal table does not only keep the latest version (with respect to the defined primary key) of the build side records in the state
but stores all versions (identified by time) since the last watermark.

For example, an incoming row with an event-time timestamp of `12:30:00` that is appended to the probe side table
is joined with the version of the build side table at time `12:30:00` according to the [concept of temporal tables](temporal_tables.html).
Thus, the incoming row is only joined with rows that have a timestamp lower or equal to `12:30:00` with
applied updates according to the primary key until this point in time.

By definition of event time, [watermarks]({{ site.baseurl }}/dev/event_time.html) allow the join operation to move
forward in time and discard versions of the build table that are no longer necessary because no incoming row with
lower or equal timestamp is expected.

Join with a Temporal Table
--------------------------

A join with a temporal table joins an arbitrary table (left input/probe side) with a temporal table (right input/build side),
i.e., an external dimension table that changes over time. Please check the corresponding page for more information about [temporal tables](temporal_tables.html#temporal-table).

<span class="label label-danger">Attention</span> Users can not use arbitrary tables as a temporal table, but need to use a table backed by a `LookupableTableSource`. A `LookupableTableSource` can only be used for temporal join as a temporal table. See the page for more details about [how to define LookupableTableSource](../sourceSinks.html#defining-a-tablesource-with-lookupable).

The following example shows an `Orders` stream that should be joined with the continuously changing currency rates table `LatestRates`.

`LatestRates` is a dimension table that is materialized with the latest rate. At time `10:15`, `10:30`, `10:52`, the content of `LatestRates` looks as follows:

{% highlight sql %}
10:15> SELECT * FROM LatestRates;

currency   rate
======== ======
US Dollar   102
Euro        114
Yen           1

10:30> SELECT * FROM LatestRates;

currency   rate
======== ======
US Dollar   102
Euro        114
Yen           1


10:52> SELECT * FROM LatestRates;

currency   rate
======== ======
US Dollar   102
Euro        116     <==== changed from 114 to 116
Yen           1
{% endhighlight %}

The content of `LastestRates` at time `10:15` and `10:30` are equal. The Euro rate has changed from 114 to 116 at `10:52`.

`Orders` is an append-only table that represents payments for the given `amount` and the given `currency`.
For example at `10:15` there was an order for an amount of `2 Euro`.

{% highlight sql %}
SELECT * FROM Orders;

amount currency
====== =========
     2 Euro             <== arrived at time 10:15
     1 US Dollar        <== arrived at time 10:30
     2 Euro             <== arrived at time 10:52
{% endhighlight %}

Given that we would like to calculate the amount of all `Orders` converted to a common currency (`Yen`).

For example, we would like to convert the following orders using the latest rate in `LatestRates`. The result would be:

{% highlight text %}
amount currency     rate   amout*rate
====== ========= ======= ============
     2 Euro          114          228    <== arrived at time 10:15
     1 US Dollar     102          102    <== arrived at time 10:30
     2 Euro          116          232    <== arrived at time 10:52
{% endhighlight %}


With the help of temporal table join, we can express such a query in SQL as:

{% highlight sql %}
SELECT
  o.amout, o.currency, r.rate, o.amount * r.rate
FROM
  Orders AS o
  JOIN LatestRates FOR SYSTEM_TIME AS OF o.proctime AS r
  ON r.currency = o.currency
{% endhighlight %}

Each record from the probe side will be joined with the current version of the build side table. In our example, the query is using the processing-time notion, so a newly appended order would always be joined with the most recent version of `LatestRates` when executing the operation. Note that the result is not deterministic for processing-time.

In contrast to [regular joins](#regular-joins), the previous results of the temporal table join will not be affected despite the changes on the build side. Also, the temporal table join operator is very lightweight and does not keep any state.

Compared to [time-windowed joins](#time-windowed-joins), temporal table joins do not define a time window within which the records will be joined.
Records from the probe side are always joined with the build side's latest version at processing time. Thus, records on the build side might be arbitrarily old.

Both [temporal table function join](#join-with-a-temporal-table-function) and temporal table join come from the same motivation but have different SQL syntax and runtime implementations:
* The SQL syntax of the temporal table function join is a join UDTF, while the temporal table join uses the standard temporal table syntax introduced in SQL:2011.
* The implementation of temporal table function joins actually joins two streams and keeps them in state, while temporal table joins just receive the only input stream and look up the external database according to the key in the record.
* The temporal table function join is usually used to join a changelog stream, while the temporal table join is usually used to join an external table (i.e. dimension table).

Such behaviour makes a temporal table join a good candidate to express stream enrichment in relational terms.

In the future, the temporal table join will support the features of temporal table function joins, i.e. support to temporal join a changelog stream.

### Usage

The syntax of temporal table join is as follows:

{% highlight sql %}
SELECT [column_list]
FROM table1 [AS <alias1>]
[LEFT] JOIN table2 FOR SYSTEM_TIME AS OF table1.proctime [AS <alias2>]
ON table1.column-name1 = table2.column-name1
{% endhighlight %}

Currently, only support INNER JOIN and LEFT JOIN. The `FOR SYSTEM_TIME AS OF table1.proctime` should be followed after temporal table. `proctime` is a [processing time attribute](time_attributes.html#processing-time) of `table1`.
This means that it takes a snapshot of the temporal table at processing time when joining every record from left table.

For example, after [defining temporal table](temporal_tables.html#defining-temporal-table), we can use it as following.

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
SELECT
  SUM(o_amount * r_rate) AS amount
FROM
  Orders
  JOIN LatestRates FOR SYSTEM_TIME AS OF o_proctime
  ON r_currency = o_currency
{% endhighlight %}
</div>
</div>

<span class="label label-danger">Attention</span> It is only supported in Blink planner.

<span class="label label-danger">Attention</span> It is only supported in SQL, and not supported in Table API yet.

<span class="label label-danger">Attention</span> Flink does not support event time temporal table joins currently.

{% top %}
