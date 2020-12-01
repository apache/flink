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

For more information regarding the syntax, please check the join sections in [Table API](../tableApi.html#joins) and [SQL]({% link dev/table/sql/queries.md %}#joins).

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

Interval Joins
--------------

A interval join is defined by a join predicate, that checks if the [time attributes](time_attributes.html) of the input
records are within certain time constraints, i.e., a time window.

{% highlight sql %}
SELECT *
FROM
  Orders o,
  Shipments s
WHERE o.id = s.orderId AND
      o.ordertime BETWEEN s.shiptime - INTERVAL '4' HOUR AND s.shiptime
{% endhighlight %}

Compared to a regular join operation, this kind of join only supports append-only tables with time attributes. Since time attributes are quasi-monotonic increasing, Flink can remove old values from its state without affecting the correctness of the result.

Temporal Joins
--------------
<span class="label label-danger">Attention</span> This feature is only supported in Blink planner.
<span class="label label-danger">Attention</span> Temporal table has two ways to define in Flink, i.e. [temporal table function]({% link dev/table/streaming/temporal_tables.md %}#temporal-table-function) and [temporal table DDL]({% link dev/table/streaming/temporal_tables.md %}#temporal-table-ddl)).
Temporal join that using temporal table function is only supported in Table API, temporal join that using temporal table DDL is only supported in SQL, please see the page [temporal table]({% link dev/table/streaming/temporal_tables.md %}) for more information about the differences between temporal table function and temporal table DDL.

Temporal join is an arbitrary table (left input/probe side) correlate with the versions of temporal table (right input/build side), The temporal table can be an external dimension table that changes over time 
or a changelog that tracks all history changes. 

Flink uses the SQL syntax of `FOR SYSTEM_TIME AS OF` to query temporal table, which is proposed in SQL:2011 standard. The syntax of a temporal table join is as follows:

{% highlight sql %}
SELECT [column_list]
FROM table1 [AS <alias1>]
[LEFT] JOIN table2 FOR SYSTEM_TIME AS OF table1.{ proctime | rowtime } [AS <alias2>]
ON table1.column-name1 = table2.column-name1
{% endhighlight %}

### Event-time Temporal Joins

Event-time temporal join uses the left input tables event time attribute to correlate the corresponding version of [versioned table](temporal_tables.html#defining-versioned-table).
Event-time temporal join only supports versioned tables and the versioned tables need to be a changelog stream. However, an append-only stream can be converted to a changelog stream in Flink, thus the versioned table can come from an append-only stream,
please see [versioned view](temporal_tables.html#defining-versioned-view) for more information to know how to define a versioned table from an append-only stream.

With an event-time time attribute (i.e., a rowtime attribute), it is possible to use a _past_ time attribute with the temporal table.
This allows for joining the two tables at a common point in time. Compared to processing-time temporal joins, the temporal table does not only keep the latest version of
the build side records in the state but stores all versions (identified by time) since the last watermark.

For example, an incoming row with an event-time timestamp of `12:30:00` that is appended to the probe side table
is joined with the version of the build side table at time `12:30:00` according to the [concept of temporal tables](temporal_tables.html).
Thus, the incoming row is only joined with rows that have a timestamp lower or equal to `12:30:00` with
applied updates according to the primary key until this point in time.

By definition of event time, [watermarks]({% link dev/event_time.md %}) allow the join operation to move
forward in time and discard versions of the build table that are no longer necessary because no incoming row with
lower or equal timestamp is expected.

The following event-time temporal table join example shows an append-only table `orders` that should be joined with the  `product_changelog` which comes from the changelog of the database table `products`, the product price in table `products` is changing over time. 

{% highlight sql %}
SELECT * FROM product_changelog;

(changelog kind)  update_time product_name price
================= =========== ============ ===== 
+(INSERT)         00:01:00    scooter      11.11
+(INSERT)         00:02:00    basketball   23.11
-(UPDATE_BEFORE)  12:00:00    scooter      11.11
+(UPDATE_AFTER)   12:00:00    scooter      12.99  <= the price of `scooter` increased to `12.99` at `12:00:00`
-(UPDATE_BEFORE)  12:00:00    basketball   23.11 
+(UPDATE_AFTER)   12:00:00    basketball   19.99  <= the price of `basketball` decreased to `19.99` at `12:00:00`
-(DELETE)         18:00:00    scooter      12.99  <= the product `basketball` is deleted at `18:00:00`
{% endhighlight %}

Given that we would like to output the version of `product_changelog` table of the time `10:00:00`, the following table shows the result. 
{% highlight sql %}
update_time  product_id product_name price
===========  ========== ============ ===== 
00:01:00     p_001      scooter      11.11
00:02:00     p_002      basketball   23.11
{% endhighlight %}

Given that we would like to output the version of `product_changelog` table of the time `13:00:00`, the following table shows the result. 
{% highlight sql %}
update_time  product_id product_name price
===========  ========== ============ ===== 
12:00:00     p_001      scooter      12.99
12:00:00     p_002      basketball   19.99
{% endhighlight %}

With the help of event-time temporal table join, we can join different version of versioned table.

{% highlight sql %}
CREATE TABLE orders (
  order_id STRING,
  product_id STRING,
  order_time TIMESTAMP(3),
  WATERMARK FOR order_time AS order_time  -- defines the necessary event time
) WITH (
...
);

-- Set the session time zone to UTC, the database operation time of changelog stored in epoch milliseconds
-- Flink SQL will use the session time zone when convert the changelog time from milliseconds to timestamp
-- Thus, please set proper timezone according to your database operation time in changelog.
SET table.local-time-zone=UTC;

-- Define a versioned table
CREATE TABLE product_changelog (
  product_id STRING,
  product_name STRING,
  product_price DECIMAL(10, 4),
  update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,  -- Note: Automatically convert the changelog time milliseconds to timestamp
  PRIMARY KEY(product_id) NOT ENFORCED,      -- (1) defines the primary key constraint
  WATERMARK FOR update_time AS update_time   -- (2) defines the event time by watermark                               
) WITH (
  'connector' = 'kafka', 
  'topic' = 'products',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = 'localhost:9092',
  'value.format' = 'debezium-json'
);

-- Do event-time temporal join
SELECT
  order_id,
  order_time,
  product_name,
  product_time,
  price
FROM orders AS O
LEFT JOIN product_changelog FOR SYSTEM_TIME AS OF O.order_time AS P
ON O.product_id = P.product_id;

order_id order_time product_name product_time price
======== ========== ============ ============ =====
o_001    00:01:00   scooter      00:01:00     11.11
o_002    00:03:00   basketball   00:02:00     23.11
o_003    12:00:00   scooter      12:00:00     12.99
o_004    12:00:00   basketball   12:00:00     19.99
o_005    18:00:00   NULL         NULL         NULL
{% endhighlight %}

The event-time temporal join is usually used to enrich the stream with changelog stream.

**Note**: The event-time temporal join is triggered by watermark from left side and right side, please ensure the both sides of the join have set watermark properly. 

**Note**: The event-time temporal join requires the primary key must be contained in the equivalence condition of temporal join condition, e.g. The primary key `P.product_id` of table `product_changelog` must be contained in condition `O.product_id = P.product_id`.

### Processing-time Temporal Joins

Processing-time temporal join uses the left input tables processing time attribute to correlate the latest version of a [regular table](temporal_tables.html#defining-regular-table).
Processing-time temporal join only supports regular tables currently, and the supported regular table can only contain append-only stream. 

With a processing-time time attribute, it is impossible to use a _past_ time attribute as an argument to the temporal table.
By definition, it is always the current timestamp. Thus, invocations of correlating temporal table will always return the latest known versions of the underlying table and any updates in the underlying history table will also immediately overwrite the current values.

One can think about a processing-time temporal join as a simple `HashMap<K, V>` that stores all of the records from the build side.
When a new record from the build side has the same key as some previous record, the old value is just simply overwritten.
Every record from the probe side is always evaluated against the most recent/current state of the `HashMap`.

The following processing-time temporal table join example shows an append-only table `orders` that should be joined with the table `LatestRates`,
`LatestRates` is a dimension table (e.g. HBase table) that is materialized with the latest rate. At time `10:15`, `10:30`, `10:52`, the content of `LatestRates` looks as follows:

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

`Orders` is an append-only table that represents payments for the given `amount` and the given `currency`. For example at `10:15` there was an order for an amount of `2 Euro`.

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

Each record from the probe side will be joined with the current version of the build side table. In our example, the query is using the processing-time notion, so a newly appended order would always be joined with the most recent version of `LatestRates` when executing the operation.
Note that the result is not deterministic for processing-time.
The processing-time temporal join is usually used to enrich the stream with external table (i.e. dimension table).

In contrast to [regular joins](#regular-joins), the previous results of the temporal table join will not be affected despite the changes on the build side.
* For event-time temporal join, the temporal join operator keeps both left table state and right table state and clean up the state by watermark.
* For processing-time temporal join, the temporal join operator keeps only right table state and the data in right state only contains the latest version, the state is lightweight; for temporal table that
 has the ability to lookup external system at runtime, the temporal join operator does not need to keep any state, the temporal table join operator is very lightweight.

Compared to [interval joins](#interval-joins), temporal table joins do not define a time window within which the records will be joined.
Records from the probe side are always joined with the build side's version at the time specified by the time attribute. Thus, records on the build side might be arbitrarily old.
As time passes, the previous and no longer needed versions of the record (for the given primary key) will be removed from the state.

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

Both [temporal table function join](#join-with-a-temporal-table-function) and [temporal join](#temporal-joins) come from the same motivation but have different SQL syntax and runtime implementations:
* The SQL syntax of the temporal table function join is a join UDTF, while the temporal table join uses the standard temporal table syntax introduced in SQL:2011.
* The feature of temporal table function is the subset of temporal table join, and they share some operator implementations, the temporal table function is legacy way, and event-time temporal table join
is supported since Flink 1.12. 
* The implementation of temporal table function joins actually joins two streams and keeps them in state, while temporal table join supports another runtime implementation besides this way, 
i.e.: processing-time temporal table join can keep nothing in state and only receive left input stream and then look up the external database according to the key in the record.
* The temporal table function supports both in legacy planner and Blink planner, but the temporal table join only supports in Blink planner, the legacy planner will be deprecated in the future. 

**Note:** The semantics is problematic both for both processing-time temporal table function and processing-time temporal table join that implements by keeping two stream in state, temporal table function enable this function, 
but temporal table join disable this function. The reason is the join processing for left stream doesn't wait for the complete snapshot of temporal table, the left stream may not found the expected dimension data, this may mislead users in production environment, 

To get the complete snapshot of temporal table may need introduce new mechanism in Flink SQL in the future.

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

{% top %}
