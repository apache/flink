---
title: "Temporal Tables"
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

A Temporal table is a table that evolves over time -  otherwise known in Flink as a [dynamic table]({% link dev/table/streaming/dynamic_tables.md %}). Rows in a temporal table are associated with one or more temporal periods and all Flink tables are temporal(dynamic).

A temporal table contains one or more versioned table snapshots, it can be a changing history table which tracks the changes(e.g. database changelog, contains all snapshots) or a changing dimensioned table which materializes the changes(e.g. database table, contains the latest snapshot). 

**Version**: A temporal table can split into a set of versioned table snapshots, the version in table snapshots represents the valid life circle of rows, the start time and the end time of the valid period can be assigned by users. 
Temporal table can split to `versioned table` and `regular table` according to the table can tracks its history version or not.

**Versioned table**: If the rows a in temporal table can track its history changes and visit its history versions, we call this a versioned table. Tables that comes from a database changelog can be defined as a versioned table.

**Regular table**: If the row in temporal table can only track and visit its latest version，we call this kind of temporal table as regular table. Tables that comes from a database or HBase can be defined as a regular table.

* This will be replaced by the TOC
{:toc}

Motivation
----------

### Correlate with a versioned table
Given a scenario the order stream correlates the dimension table product, the table `orders` comes from kafka which contains the real time orders, the table `product_changelog` comes from the changelog of the database table `products`,
 the product price in table `products` is changing over time. 

{% highlight sql %}
SELECT * FROM product_changelog;

(changelog kind)  update_time  product_id product_name price
================= ===========  ========== ============ ===== 
+(INSERT)         00:01:00     p_001      scooter      11.11
+(INSERT)         00:02:00     p_002      basketball   23.11
-(UPDATE_BEFORE)  12:00:00     p_001      scooter      11.11
+(UPDATE_AFTER)   12:00:00     p_001      scooter      12.99
-(UPDATE_BEFORE)  12:00:00     p_002      basketball   23.11 
+(UPDATE_AFTER)   12:00:00     p_002      basketball   19.99
-(DELETE)         18:00:00     p_001      scooter      12.99 
{% endhighlight %}

The table `product_changelog` represents an ever growing changelog of database table `products`,  for example, the initial price of product `scooter` is `11.11` at `00:01:00`, and the price increases to `12.99` at `12:00:00`,
 the product item is deleted from the table `products` at `18:00:00`.

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

In above example, the specific version of the table is tracked by `update_time` and `product_id`,  the `product_id` would be a primary key for `product_changelog` table and `update_time` would be the event time.

In Flink, this is represented by a [*versioned table*](#defining-versioned-table).

### Correlate with a regular table

On the other hand, some use cases require to join a regular table which is an external database table.

Let's assume that `LatestRates` is a table (e.g. stored in HBase) which is materialized with the latest rates. The `LatestRates` always represents the latest content of hbase table `rates`.
 
Then the content of `LatestRates` table when we query at time `10:15:00` is:
{% highlight sql %}
10:15:00 > SELECT * FROM LatestRates;

currency  rate
========= ====
US Dollar 102
Euro      114
Yen       1
{% endhighlight %}

Then the content of `LatestRates` table when we query at time `11:00:00` is:
{% highlight sql %}
11:00:00 > SELECT * FROM LatestRates;

currency  rate
========= ====
US Dollar 102
Euro      116
Yen       1
{% endhighlight %}

In Flink, this is represented by a [*regular Table*](#defining-regular-table).

Temporal Table
--------------
<span class="label label-danger">Attention</span> This is only supported in Blink planner.

Flink uses primary key constraint and event time to define both versioned table and versioned view.

### Defining Versioned Table
The table is a versioned table in Flink only is the table contains primary key constraint and event time.
{% highlight sql %}
-- Define a versioned table
CREATE TABLE product_changelog (
  product_id STRING,
  product_name STRING,
  product_price DECIMAL(10, 4),
  update_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,
  PRIMARY KEY(product_id) NOT ENFORCED,      -- (1) defines the primary key constraint
  WATERMARK FOR update_time AS update_time   -- (2) defines the event time by watermark                               
) WITH (
  'connector' = 'kafka',
  'topic' = 'products',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = 'localhost:9092',
  'value.format' = 'debezium-json'
);
{% endhighlight %}

Line `(1)` defines the primary key constraint for table `product_changelog`, Line `(2)` defines the `update_time` as event time for table `product_changelog`,
thus table `product_changelog` is a versioned table.

**Note**: The grammar `METADATA FROM 'value.source.timestamp' VIRTUAL` means extract the database
operation execution time for every changelog, it's strongly recommended defines the database operation execution time 
as event time rather than ingestion-time or time in the record, otherwise the version extract from the changelog may
mismatch with the version in database.
 
### Defining Versioned View

Flink also supports defining versioned view only if the view contains unique key constraint and event time.
 
Let’s assume that we have the following table `RatesHistory`:
{% highlight sql %}
-- Define an append-only table
CREATE TABLE RatesHistory (
    currency_time TIMESTAMP(3),
    currency STRING,
    rate DECIMAL(38, 10),
    WATERMARK FOR currency_time AS currency_time   -- defines the event time
) WITH (
  'connector' = 'kafka',
  'topic' = 'rates',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'                                -- this is an append only source
)
{% endhighlight %}

Table `RatesHistory` represents an ever growing append-only table of currency exchange rates with respect to 
Yen (which has a rate of 1). For example, the exchange rate for the period from 09:00 to 10:45 of Euro to Yen was 114.
From 10:45 to 11:15 it was 116.

{% highlight sql %}
SELECT * FROM RatesHistory;

currency_time currency  rate
============= ========= ====
09:00:00      US Dollar 102
09:00:00      Euro      114
09:00:00      Yen       1
10:45:00      Euro      116
11:15:00      Euro      119
11:49:00      Pounds    108
{% endhighlight %}

To define a versioned table on `RatesHistory`, Flink supports defining a versioned view 
by [deduplication query]({% link dev/table/sql/queries.md %}#deduplication) which produces an ordered changelog
stream with an inferred primary key(`currency`) and event time(`currency_time`).

{% highlight sql %}
-- Define a versioned view
CREATE VIEW versioned_rates AS              
SELECT currency, rate, currency_time            -- (1) `currency_time` keeps the event time
  FROM (
      SELECT *,
      ROW_NUMBER() OVER (PARTITION BY currency  -- (2) the inferred unique key `currency` can be a primary key
         ORDER BY currency_time DESC) AS rowNum 
      FROM RatesHistory)
WHERE rowNum = 1; 

-- the view `versioned_rates` will produce changelog as the following.
(changelog kind) currency_time currency   rate
================ ============= =========  ====
+(INSERT)        09:00:00      US Dollar  102
+(INSERT)        09:00:00      Euro       114
+(INSERT)        09:00:00      Yen        1
+(UPDATE_AFTER)  10:45:00      Euro       116
+(UPDATE_AFTER)  11:15:00      Euro       119
+(INSERT)        11:49:00      Pounds     108
{% endhighlight sql %}

Line `(1)` defines the `currency_time` as event time for view `versioned_rates`， Line `(2)` defines the primary key constraint
for view `versioned_rates`, thus view `versioned_rates` is a versioned view.

The deduplication query in view will be optimized in Flink and produce changelog effectively, the produced changelog keep primary key and event times.

Given that we would like to output the version of `versioned_rates` view of the time `11:00:00`, the following table shows the result: 
{% highlight sql %}
currency_time currency   rate  
============= ========== ====
09:00:00      US Dollar  102
09:00:00      Yen        1
10:45:00      Euro       116
{% endhighlight %}

Given that we would like to output the version of `versioned_rates` view of the time `12:00:00`, the following table shows the result: 
{% highlight sql %}
currency_time currency   rate  
============= ========== ====
09:00:00      US Dollar  102
09:00:00      Yen        1
10:45:00      Euro       119
11:49:00      Pounds     108
{% endhighlight %}

### Defining Regular Table
 
Regular table definition is same with Flink table DDL, see also the page about [create table]({% link dev/table/sql/create.md %}#create-table) for more information about how to create a regular table.
 
{% highlight sql %}
-- Define an HBase table with DDL, then we can use it as a temporal table in sql
-- Column 'currency' is the rowKey in HBase table
 CREATE TABLE LatestRates (   
     currency STRING,   
     fam1 ROW<rate DOUBLE>   
 ) WITH (   
    'connector' = 'hbase-1.4',   
    'table-name' = 'rates',   
    'zookeeper.quorum' = 'localhost:2181'   
 );
{% endhighlight %}

<span class="label label-danger">Attention</span>
Arbitrary table can use as temporal table in processing-time temporal join theoretically, but currently the supported tale is the 
table backed by a `LookupableTableSource`. A `LookupableTableSource` can only be used for processing-time temporal join as a temporal table. 

The table defines with `LookupableTableSource` means the table must has lookup ability to look up an external storage system 
by one or more keys during runtime. The current supported regular table in processing-time temporal join includes 
[JDBC]({% link dev/table/connectors/jdbc.md %}), [HBase]({% link dev/table/connectors/hbase.md %}) 
and [Hive]({% link dev/table/connectors/hive/hive_read_write.md %}#temporal-table-join).

See also the page [LookupableTableSource]({% link dev/table/sourceSinks.md%}#lookup-table-source) for more information.

Using arbitrary table as temporal table in processing time temporal table join will be supported in the near future. 

Temporal Table Function
------------------------
The temporal table function is a legacy way to define a temporal table and correlate the temporal table content. In order to access the data in a temporal table,  now we can use
temporal table DDL to define a temporal table and use [temporal table join]({% link dev/table/streaming/joins.md%}#temporal-joins) to correlate the temporal table.

The main difference between Temporal Table DDL and Temporal Table Function are the temporal table DDL can directly use in pure SQL but temporal table function can not; the temporal table DDL support defines versioned table from changelog stream and append-only stream but temporal table function only supports append-only stream.


In order to access the data in a temporal table, one must pass a [time attribute]({% link dev/table/streaming/time_attributes.md %}) that determines the version of the table that will be returned.
Flink uses the SQL syntax of [table functions]({% link dev/table/functions/udfs.md %}#table-functions) to provide a way to express it.

Once defined, a *Temporal Table Function* takes a single time argument `timeAttribute` and returns a set of rows.
This set contains the latest versions of the rows for all of the existing primary keys with respect to the given time attribute.

Assuming that we defined a temporal table function `Rates(timeAttribute)` based on `RatesHistory` table, we could query such a function in the following way:

{% highlight sql %}
SELECT * FROM Rates('10:15:00');

rowtime  currency  rate
=======  ========= ====
09:00:00 US Dollar 102
09:00:00 Euro      114
09:00:00 Yen       1

SELECT * FROM Rates('11:00:00');

rowtime  currency  rate
======== ========= ====
09:00:00 US Dollar 102
10:45:00 Euro      116
09:00:00 Yen       1
{% endhighlight %}

Each query to `Rates(timeAttribute)` would return the state of the `Rates` for the given `timeAttribute`.

**Note**: Currently, Flink doesn't support directly querying the temporal table functions with a constant time attribute parameter. The above example was used to provide an intuition about what the function `Rates(timeAttribute)` returns.

See also the page about [joins for continuous queries]({% link dev/table/streaming/joins.md %}) for more information about how to join with a temporal table.

### Defining Temporal Table Function

The following code snippet illustrates how to create a temporal table by temporal table function from an append-only table.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.table.functions.TemporalTableFunction;
(...)

// Get the stream and table environments.
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

// Provide a static data set of the rates history table.
List<Tuple2<String, Long>> ratesHistoryData = new ArrayList<>();
ratesHistoryData.add(Tuple2.of("US Dollar", 102L));
ratesHistoryData.add(Tuple2.of("Euro", 114L));
ratesHistoryData.add(Tuple2.of("Yen", 1L));
ratesHistoryData.add(Tuple2.of("Euro", 116L));
ratesHistoryData.add(Tuple2.of("Euro", 119L));

// Create and register an example table using above data set.
// In the real setup, you should replace this with your own table.
DataStream<Tuple2<String, Long>> ratesHistoryStream = env.fromCollection(ratesHistoryData);
Table ratesHistory = tEnv.fromDataStream(ratesHistoryStream, $("r_currency"), $("r_rate"), $("r_proctime").proctime());

tEnv.createTemporaryView("RatesHistory", ratesHistory);

// Create and register a temporal table function.
// Define "r_proctime" as the time attribute and "r_currency" as the primary key.
TemporalTableFunction rates = ratesHistory.createTemporalTableFunction("r_proctime", "r_currency"); // <==== (1)
tEnv.registerFunction("Rates", rates);                                                              // <==== (2)
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
// Get the stream and table environments.
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tEnv = StreamTableEnvironment.create(env)

// Provide a static data set of the rates history table.
val ratesHistoryData = new mutable.MutableList[(String, Long)]
ratesHistoryData.+=(("US Dollar", 102L))
ratesHistoryData.+=(("Euro", 114L))
ratesHistoryData.+=(("Yen", 1L))
ratesHistoryData.+=(("Euro", 116L))
ratesHistoryData.+=(("Euro", 119L))

// Create and register an example table using above data set.
// In the real setup, you should replace this with your own table.
val ratesHistory = env
  .fromCollection(ratesHistoryData)
  .toTable(tEnv, 'r_currency, 'r_rate, 'r_proctime.proctime)

tEnv.createTemporaryView("RatesHistory", ratesHistory)

// Create and register TemporalTableFunction.
// Define "r_proctime" as the time attribute and "r_currency" as the primary key.
val rates = ratesHistory.createTemporalTableFunction($"r_proctime", $"r_currency") // <==== (1)
tEnv.registerFunction("Rates", rates)                                          // <==== (2)
{% endhighlight %}
</div>
</div>

Line `(1)` creates a `rates` [temporal table function](#temporal-table-function),
which allows us to use the function `rates` in the [Table API]({% link dev/table/tableApi.md %}#joins).

Line `(2)` registers this function under the name `Rates` in our table environment,
which allows us to use the `Rates` function in [SQL]({% link dev/table/sql/queries.md %}#joins).

{% top %}
