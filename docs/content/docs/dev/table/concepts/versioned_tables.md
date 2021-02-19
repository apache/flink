---
title: "Versioned Tables"
weight: 4
type: docs
aliases:
  - /dev/table/streaming/versioned_tables.html
  - /dev/table/streaming/temporal_tables.html
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

Flink SQL operates over dynamic tables that evolve, which may either be append-only or updating. 
Versioned tables represent a special type of updating table that remembers the past values for each key.

## Concept

Dynamic tables define relations over time. 
Often, particularly when working with metadata, a key's old value does not become irrelevant when it changes. 

Flink SQL can define versioned tables over any dynamic table with a `PRIMARY KEY` constraint and time attribute. 

A primary key constraint in Flink means that a column or set of columns of a table or view are unique and non-null.
The primary key semantic on a upserting table means the materialized changes for a particular key (`INSERT`/`UPDATE`/`DELETE`) represent the changes to a single row over time. The time attribute on a upserting table defines when each change occurred.

Taken together, Flink can track the changes to a row over time and maintain the period for which each value was valid for that key.

Suppose a table tracks the prices of different products in a store. 

```sql
(changelog kind)  update_time  product_id product_name price
================= ===========  ========== ============ ===== 
+(INSERT)         00:01:00     p_001      scooter      11.11
+(INSERT)         00:02:00     p_002      basketball   23.11
-(UPDATE_BEFORE)  12:00:00     p_001      scooter      11.11
+(UPDATE_AFTER)   12:00:00     p_001      scooter      12.99
-(UPDATE_BEFORE)  12:00:00     p_002      basketball   23.11 
+(UPDATE_AFTER)   12:00:00     p_002      basketball   19.99
-(DELETE)         18:00:00     p_001      scooter      12.99 
```

Given this set of changes, we track how the price of a scooter changes over time.
It is initially $11.11 at `00:01:00` when added to the catalog.
The price then rises to $12.99 at `12:00:00` before being deleted from the catalog at `18:00:00`.

If we queried the table for various products' prices at different times, we would retrieve different results. At `10:00:00` the table would show one set of prices.

```sql
update_time  product_id product_name price
===========  ========== ============ ===== 
00:01:00     p_001      scooter      11.11
00:02:00     p_002      basketball   23.11
```


While at `13:00:00,` we would find another set of prices.

```sql
update_time  product_id product_name price
===========  ========== ============ ===== 
12:00:00     p_001      scooter      12.99
12:00:00     p_002      basketball   19.99
```

## Versioned Table Sources

Versioned tables are defined implicitly for any tables whose underlying sources or formats directly define changelogs.
Examples include the [upsert Kafka]({{< ref "docs/connectors/table/upsert-kafka" >}}) source as well as database changelog
formats such as [debezium]({{< ref "docs/connectors/table/formats/debezium" >}}) and [canal]({{< ref "docs/connectors/table/formats/canal" >}}).
As discussed above, the only additional requirement is the `CREATE` table statement must contain a `PRIMARY KEY` and an event-time attribute. 

```sql
CREATE TABLE products (
	product_id    STRING,
	product_name  STRING,
	price         DECIMAL(32, 2),
	update_time   TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,
	PRIMARY KEY (product_id) NOT ENFORCED
	WATERMARK FOR update_time AS update_time
) WITH (...);
```

## Versioned Table Views

Flink also supports defining versioned views if the underlying query contains a unique key constraint
and event-time attribute. Imagine an append-only table of currency rates. 

```sql
CREATE TABLE currency_rates (
	currency      STRING,
	rate          DECIMAL(32, 10)
	update_time   TIMESTAMP(3),
	WATERMARK FOR update_time AS update_time
) WITH (
	'connector' = 'kafka',
	'topic'	    = 'rates',
	'properties.bootstrap.servers' = 'localhost:9092',
	'format'    = 'json'
);
```

The table `currency_rates` contains a row for each currency &mdash; with respect to USD &mdash; 
and receives a new row each time the rate changes.
The `JSON` format does not support native changelog semantics, so Flink can only read this table as append-only.

```sql
(changelog kind) update_time   currency   rate
================ ============= =========  ====
+(INSERT)        09:00:00      Yen        102
+(INSERT)        09:00:00      Euro       114
+(INSERT)        09:00:00      USD        1
+(INSERT)        11:15:00      Euro       119
+(INSERT)        11:49:00      Pounds     108
```

Flink interprets each row as an `INSERT` to the table, meaning we cannot define a `PRIMARY KEY` over currency.
However, it is clear to us (the query developer) that this table has all the necessary information to define a versioned table. 
Flink can reinterpret this table as a versioned table by defining a [deduplication query]({{< ref "docs/dev/table/sql/queries" >}}#deduplication)
which produces an ordered changelog stream with an inferred primary key (currency) and event time (update_time). 

```sql
-- Define a versioned view
CREATE VIEW versioned_rates AS              
SELECT currency, rate, update_time              -- (1) `update_time` keeps the event time
  FROM (
      SELECT *,
      ROW_NUMBER() OVER (PARTITION BY currency  -- (2) the inferred unique key `currency` can be a primary key
         ORDER BY update_time DESC) AS rownum 
      FROM currency_rates)
WHERE rownum = 1; 

-- the view `versioned_rates` will produce a changelog as the following.
(changelog kind) update_time currency   rate
================ ============= =========  ====
+(INSERT)        09:00:00      Yen        102
+(INSERT)        09:00:00      Euro       114
+(INSERT)        09:00:00      USD        1
+(UPDATE_AFTER)  10:45:00      Euro       116
+(UPDATE_AFTER)  11:15:00      Euro       119
+(INSERT)        11:49:00      Pounds     108
```

Flink has a special optimization step that will efficiently transform this query into a versioned
table usable in subsequent queries.
In general, the results of a query with the following format produces a versioned table:

```sql
SELECT [column_list]
FROM (
   SELECT [column_list],
     ROW_NUMBER() OVER ([PARTITION BY col1[, col2...]]
       ORDER BY time_attr DESC) AS rownum
   FROM table_name)
WHERE rownum = 1
```

**Parameter Specification:**

- `ROW_NUMBER()`: Assigns an unique, sequential number to each row, starting with one.
- `PARTITION BY col1[, col2...]`: Specifies the partition columns, i.e. the deduplicate key. These columns form the primary key of the subsequent versioned table.
- `ORDER BY time_attr DESC`: Specifies the ordering column, it must be a [time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}).
- `WHERE rownum = 1`: The `rownum = 1` is required for Flink to recognize this query is to generate a versioned table.

{{< top >}}
