---
title: "Join"
weight: 10
type: docs
aliases:
  - /dev/table/streaming/joins.html
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

# Joins

{{< label Batch >}} {{< label Streaming >}}

Flink SQL supports complex and flexible join operations over dynamic tables. 
There are several different types of joins to account for the wide variety of semantics queries may require. 

By default, the order of joins is not optimized. Tables are joined in the order in which they are specified in the `FROM` clause. You can tweak the performance of your join queries, by listing the tables with the lowest update frequency first and the tables with the highest update frequency last. Make sure to specify tables in an order that does not yield a cross join (Cartesian product), which are not supported and would cause a query to fail.

Regular Joins
-------------

Regular joins are the most generic type of join in which any new record, or changes to either side of the join, are visible and affect the entirety of the join result. 
For example, if there is a new record on the left side, it will be joined with all the previous and future records on the right side when the product id equals. 

```sql
SELECT * FROM Orders
INNER JOIN Product
ON Orders.productId = Product.id
```

For streaming queries, the grammar of regular joins is the most flexible and allow for any kind of updating (insert, update, delete) input table.
However, this operation has important operational implications: it requires to keep both sides of the join input in Flink state forever.
Thus, the required state for computing the query result might grow infinitely depending on the number of distinct input rows of all input tables and intermediate join results. You can provide a query configuration with an appropriate state time-to-live (TTL) to prevent excessive state size. Note that this might affect the correctness of the query result. See [query configuration]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl) for details.

{{< query_state_warning >}}

### INNER Equi-JOIN

Returns a simple Cartesian product restricted by the join condition. Currently, only equi-joins are supported, i.e., joins that have at least one conjunctive condition with an equality predicate. Arbitrary cross or theta joins are not supported.

```sql
SELECT *
FROM Orders
INNER JOIN Product
ON Orders.product_id = Product.id
```

### OUTER Equi-JOIN

Returns all rows in the qualified Cartesian product (i.e., all combined rows that pass its join condition), plus one copy of each row in an outer table for which the join condition did not match with any row of the other table. Flink supports LEFT, RIGHT, and FULL outer joins. Currently, only equi-joins are supported, i.e., joins with at least one conjunctive condition with an equality predicate. Arbitrary cross or theta joins are not supported.

```sql
SELECT *
FROM Orders
LEFT JOIN Product
ON Orders.product_id = Product.id

SELECT *
FROM Orders
RIGHT JOIN Product
ON Orders.product_id = Product.id

SELECT *
FROM Orders
FULL OUTER JOIN Product
ON Orders.product_id = Product.id
```

Interval Joins
--------------

Returns a simple Cartesian product restricted by the join condition and a time constraint. An interval join requires at least one equi-join predicate and a join condition that bounds the time on both sides. Two appropriate range predicates can define such a condition (<, <=, >=, >), a BETWEEN predicate, or a single equality predicate that compares [time attributes]({{< ref "docs/dev/table/concepts/time_attributes" >}}) of the same type (i.e., processing time or event time) of both input tables.

For example, this query will join all orders with their corresponding shipments if the order was shipped four hours after the order was received.

```sql
SELECT *
FROM Orders o, Shipments s
WHERE o.id = s.order_id
AND o.order_time BETWEEN s.ship_time - INTERVAL '4' HOUR AND s.ship_time
```

The following predicates are examples of valid interval join conditions:

- `ltime = rtime`
- `ltime >= rtime AND ltime < rtime + INTERVAL '10' MINUTE`
- `ltime BETWEEN rtime - INTERVAL '10' SECOND AND rtime + INTERVAL '5' SECOND`

For streaming queries, compared to the regular join, interval join only supports append-only tables with time attributes.
Since time attributes are quasi-monotonic increasing, Flink can remove old values from its state without affecting the correctness of the result.

Temporal Joins
--------------

### Event Time Temporal Join

Temporal joins allow joining against a [versioned table]({{< ref "docs/dev/table/concepts/versioned_tables" >}}).
This means a table can be enriched with changing metadata and retrieve its value at a certain point in time. 

Temporal joins take an arbitrary table (left input/probe site) and correlate each row to the corresponding row's relevant version in the versioned table (right input/build side). 
Flink uses the SQL syntax of `FOR SYSTEM_TIME AS OF` to perform this operation from the SQL:2011 standard. 
The syntax of a temporal join is as follows;

```sql
SELECT [column_list]
FROM table1 [AS <alias1>]
[LEFT] JOIN table2 FOR SYSTEM_TIME AS OF table1.{ proctime | rowtime } [AS <alias2>]
ON table1.column-name1 = table2.column-name1
```

With an event-time attribute (i.e., a rowtime attribute), it is possible to retrieve the value of a key as it was at some point in the past. 
This allows for joining the two tables at a common point in time. 
The versioned table will store all versions - identified by time - since the last watermark. 

For example, suppose we have a table of orders, each with prices in different currencies.
To properly normalize this table to a single currency, such as USD, each order needs to be joined with the proper currency conversion rate from the point-in-time when the order was placed. 

```sql
-- Create a table of orders. This is a standard
-- append-only dynamic table.
CREATE TABLE orders (
    order_id    STRING,
    price       DECIMAL(32,2),
    currency    STRING,
    order_time  TIMESTAMP(3),
    WATERMARK FOR order_time AS order_time
) WITH (/* ... */);

-- Define a versioned table of currency rates. 
-- This could be from a change-data-capture
-- such as Debezium, a compacted Kafka topic, or any other
-- way of defining a versioned table. 
CREATE TABLE currency_rates (
    currency STRING,
    conversion_rate DECIMAL(32, 2),
    update_time TIMESTAMP(3) METADATA FROM `values.source.timestamp` VIRTUAL,
    WATERMARK FOR update_time AS update_time,
    PRIMARY KEY(currency) NOT ENFORCED
) WITH (
   'connector' = 'kafka',
   'value.format' = 'debezium-json',
   /* ... */
);

SELECT 
     order_id,
     price,
     currency,
     conversion_rate,
     order_time,
FROM orders
LEFT JOIN currency_rates FOR SYSTEM_TIME AS OF orders.order_time
ON orders.currency = currency_rates.currency;

order_id  price  currency  conversion_rate  order_time
========  =====  ========  ===============  =========
o_001     11.11  EUR       1.14             12:00:00
o_002     12.51  EUR       1.10             12:06:00

```

**Note:** The event-time temporal join is triggered by a watermark from the left and right sides; please ensure both sides of the join have set watermark correctly.

**Note:** The event-time temporal join requires the primary key contained in the equivalence condition of the temporal join condition, e.g., The primary key `currency_rates.currency` of table `currency_rates` to be constrained in the condition `orders.currency = currency_rates.currency`.

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

```sql
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
```

The content of `LastestRates` at times `10:15` and `10:30` are equal.
The Euro rate has changed from 114 to 116 at `10:52`.

`Orders` is an append-only table representing payments for the given `amount` and the given `currency`.
For example, at `10:15` there was an order for an amount of `2 Euro`.

```sql
SELECT * FROM Orders;

amount currency
====== =========
     2 Euro             <== arrived at time 10:15
     1 US Dollar        <== arrived at time 10:30
     2 Euro             <== arrived at time 10:52
```

Given these tables, we would like to calculate all `Orders` converted to a common currency.

```text
amount currency     rate   amount*rate
====== ========= ======= ============
     2 Euro          114          228    <== arrived at time 10:15
     1 US Dollar     102          102    <== arrived at time 10:30
     2 Euro          116          232    <== arrived at time 10:52
```


With the help of temporal table join, we can express such a query in SQL as:

```sql
SELECT
  o.amount, o.currency, r.rate, o.amount * r.rate
FROM
  Orders AS o
  JOIN LatestRates FOR SYSTEM_TIME AS OF o.proctime AS r
  ON r.currency = o.currency
```

Each record from the probe side will be joined with the current version of the build side table.
In our example, the query uses the processing-time notion, so a newly appended order would always be joined with the most recent version of `LatestRates` when executing the operation.

The result is not deterministic for processing-time.
The processing-time temporal join is most often used to enrich the stream with an external table (i.e., dimension table).

In contrast to [regular joins](#regular-joins), the previous temporal table results will not be affected despite the changes on the build side.
Compared to [interval joins](#interval-joins), temporal table joins do not define a time window within which the records join, i.e., old rows are not stored in state.

Lookup Join
--------------

A lookup join is typically used to enrich a table with data that is queried from an external system. The join requires one table to have a processing time attribute and the other table to be backed by a lookup source connector.

The lookup join uses the above [Processing Time Temporal Join](#processing-time-temporal-join) syntax with the right table to be backed by a lookup source connector.

The following example shows the syntax to specify a lookup join.

```sql
-- Customers is backed by the JDBC connector and can be used for lookup joins
CREATE TEMPORARY TABLE Customers (
  id INT,
  name STRING,
  country STRING,
  zip STRING
) WITH (
  'connector' = 'jdbc',
  'url' = 'jdbc:mysql://mysqlhost:3306/customerdb',
  'table-name' = 'customers'
);

-- enrich each order with customer information
SELECT o.order_id, o.total, c.country, c.zip
FROM Orders AS o
  JOIN Customers FOR SYSTEM_TIME AS OF o.proc_time AS c
    ON o.customer_id = c.id;
```

In the example above, the Orders table is enriched with data from the Customers table which resides in a MySQL database. The `FOR SYSTEM_TIME AS OF` clause with the subsequent processing time attribute ensures that each row of the `Orders` table is joined with those Customers rows that match the join predicate at the point in time when the `Orders` row is processed by the join operator. It also prevents that the join result is updated when a joined `Customer` row is updated in the future. The lookup join also requires a mandatory equality join predicate, in the example above `o.customer_id = c.id`.

Array Expansion
--------------

Returns a new row for each element in the given array. Unnesting `WITH ORDINALITY` is not yet supported.

```sql
SELECT order_id, tag
FROM Orders CROSS JOIN UNNEST(tags) AS t (tag)
```

Table Function
--------------

Joins a table with the results of a table function. Each row of the left (outer) table is joined with all rows produced by the corresponding call of the table function. [User-defined table functions]({{< ref "docs/dev/table/functions/udfs" >}}#table-functions) must be registered before use.

### INNER JOIN

The row of the left (outer) table is dropped, if its table function call returns an empty result.

```sql
SELECT order_id, res
FROM Orders,
LATERAL TABLE(table_func(order_id)) t(res)
```

### LEFT OUTER JOIN

If a table function call returns an empty result, the corresponding outer row is preserved, and the result padded with null values. Currently, a left outer join against a lateral table requires a TRUE literal in the ON clause.

```sql
SELECT order_id, res
FROM Orders
LEFT OUTER JOIN LATERAL TABLE(table_func(order_id)) t(res)
  ON TRUE
```

{{< top >}}
