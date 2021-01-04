---
title: "Joins in Continuous Queries"
nav-parent_id: streaming_tableapi
nav-pos: 4
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

Flink SQL supports complex and flexible join operations over dynamic tables. 
There are several different types of joins to account for the wide variety of semantics queries may require. 

For full syntax of each join, please check the join sections in [Table API](../tableApi.html#joins) and [SQL]({% link dev/table/sql/queries.md %}#joins).

* This will be replaced by the TOC
{:toc}

Regular Joins
-------------

Regular joins are the most generic type of join in which any new record, or changes to either side of the join, are visible and affect the entirety of the join result. 
For example, if there is a new record on the left side, it will be joined with all the previous and future records on the right side. 

{% highlight sql %}
SELECT * FROM Orders
INNER JOIN Product
ON Orders.productId = Product.id
{% endhighlight %}


These semantics are the most flexible and allow for any kind of updating (insert, update, delete) input table.
However, this operation has important operational implications: it requires to keep both sides of the join input in Flinks state forever.
Thus, the resource usage may grow indefinitely if both input tables are append-only. 

Interval Joins
--------------

An interval join is defined by a join predicate that checks if the [time attributes](time_attributes.html) of the input
records are within certain time constraints, i.e., a time window.

{% highlight sql %}
SELECT *
FROM
  Orders o,
  Shipments s
WHERE o.id = s.orderId AND
      o.ordertime BETWEEN s.shiptime - INTERVAL '4' HOUR AND s.shiptime
{% endhighlight %}

Compared to a regular join, this kind of join only supports append-only tables with time attributes.
Since time attributes are quasi-monotonic increasing, Flink can remove old values from its state without affecting the correctness of the result.

Temporal Joins
--------------
<span class="label label-danger">Attention</span> This feature is not supported by the Legacy planner

### Event Time Temporal Join

Temporal joins allow joining against a [versioned table]({% link dev/table/streaming/versioned_tables.md %}).
This means a table can be enriched with changing metadata and retrieve its value at a certain point in time. 

Temporal joins take an arbitrary table (left input/probe site) and correlate each row to the corresponding row's relevant version in the versioned table (right input/build side). 
Flink uses the SQL syntax of `FOR SYSTEM_TIME AS OF` to perform this operation from the SQL:2011 standard. 
The syntax of a temporal join is as follows;

{% highlight sql %}
SELECT [column_list]
FROM table1 [AS <alias1>]
[LEFT] JOIN table2 FOR SYSTEM_TIME AS OF table1.{ proctime | rowtime } [AS <alias2>]
ON table1.column-name1 = table2.column-name1
{% endhighlight %}

With an event-time attribute (i.e., a rowtime attribute), it is possible to retrieve the value of a key as it was at some point in the past. 
This allows for joining the two tables at a common point in time. 
The versioned table will store all versions - identified by time - since the last watermark. 

For example, suppose we have a table of orders, each with prices in different currencies.
To properly normalize this table to a single currency, such as USD, each order needs to be joined with the proper currency conversion rate from the point-in-time when the order was placed. 

{% highlight sql %}
-- Create a table of orders. This is a standard
-- append-only dynamic table.
CREATE TABLE orders (
    order_id    STRING,
    price       DECIMAL(32,2),
    currency    STRING,
    order_time  TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time
) WITH (...);

-- Define a versioned table of currency rates. 
-- This could be from a change-data-capture
-- such as Debezium, a compacted Kafka topic, or any other
-- way of defining a versioned table. 
CREATE TABLE currency_rates (
    currency STRING,
    conversion_rate DECIMAL(32, 2),
    update_time TIMESTAMP(3) METADATA FROM `values.source.timestamp` VIRTURAL
    WATERMARK FOR update_time AS update_time
) WITH (...);

SELECT 
     order_id,
     price,
     currency,
     conversion_rate,
     order_time,
FROM orders
LEFT JOIN currency_rates FOR SYSTEM TIME AS OF orders.order_time
ON orders.currency = currency_rates.currency

order_id price currency conversion_rate  order_time
====== ==== ======  ============  ========
o_001    11.11  EUR        1.14                    12:00:00
o_002    12.51  EUR        1.10                    12:0600

{% endhighlight %}

**Note:** The event-time temporal join is triggered by a watermark from the left and right sides; please ensure both sides of the join have set watermark correctly.

**Note:** The event-time temporal join requires the primary key contained in the equivalence condition of the temporal join condition, e.g., The primary key `P.product_id` of table `product_changelog` to be constrained in the condition `O.product_id = P.product_id`.

In contrast to [regular joins](#regular-joins), the previous temporal table results will not be affected despite the changes on the build side.
Compared to [interval joins](#interval-joins), temporal table joins do not define a time window within which the records will be joined.
Records from the probe side are always joined with the build side's version at the time specified by the time attribute. Thus, rows on the build side might be arbitrarily old.
As time passes,  no longer needed versions of the record (for the given primary key) will be removed from the state.

### Processing Time Temporal Join

A processing time temporal table join uses a processing-time attribute to correlate rows to the latest version of a key in an external versioned table. 

By definition, with a processing-time attribute, the join will always return the most up-to-date value for a given key. One can think of a lookup table as a simple HashMap<K, V> that stores all the records from the build side.
The power of this join is it allows Flink to work directly against external systems when it is not feasible to materialize the table as a dynamic table within Flink. 

The following processing-time temporal table join example shows an append-only table `orders` that should be joined with the table `LatestRates`.
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

The content of `LastestRates` at times `10:15` and `10:30` are equal.
The Euro rate has changed from 114 to 116 at `10:52`.

`Orders` is an append-only table representing payments for the given `amount` and the given `currency`.
For example, at `10:15` there was an order for an amount of `2 Euro`.

{% highlight sql %}
SELECT * FROM Orders;

amount currency
====== =========
     2 Euro             <== arrived at time 10:15
     1 US Dollar        <== arrived at time 10:30
     2 Euro             <== arrived at time 10:52
{% endhighlight %}

Given these tables, we would like to calculate all `Orders` converted to a common currency.

{% highlight text %}
amount currency     rate   amount*rate
====== ========= ======= ============
     2 Euro          114          228    <== arrived at time 10:15
     1 US Dollar     102          102    <== arrived at time 10:30
     2 Euro          116          232    <== arrived at time 10:52
{% endhighlight %}


With the help of temporal table join, we can express such a query in SQL as:

{% highlight sql %}
SELECT
  o.amount, o.currency, r.rate, o.amount * r.rate
FROM
  Orders AS o
  JOIN LatestRates FOR SYSTEM_TIME AS OF o.proctime AS r
  ON r.currency = o.currency
{% endhighlight %}

Each record from the probe side will be joined with the current version of the build side table.
In our example, the query uses the processing-time notion, so a newly appended order would always be joined with the most recent version of `LatestRates` when executing the operation.

The result is not deterministic for processing-time.
The processing-time temporal join is most often used to enrich the stream with an external table (i.e., dimension table).

In contrast to [regular joins](#regular-joins), the previous temporal table results will not be affected despite the changes on the build side.
Compared to [interval joins](#interval-joins), temporal table joins do not define a time window within which the records join, i.e., old rows are not stored in state.

{% top %}
