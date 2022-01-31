---
title: 'Write your first Flink program with SQL'
weight: 2 
type: docs
aliases:
  - /try-flink/write_flink_program_with_sql.html
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

# Write your first Flink program with SQL

Flink features [multiple APIs]({{< ref "docs/concepts/overview" >}}) with different levels of abstraction 
that can be used to develop your streaming application. SQL is the highest level of abstraction and 
is supported by Flink as a relational API for batch and stream processing. This means that you can 
write the same queries on both unbounded real-time streams and bounded recorded streams and produce 
the same results. 

SQL on Flink is based on [Apache Calcite](https://calcite.apache.org/) (which is based on standard SQL) 
and is commonly used to ease the process of implementing data analytics, data pipelining, and ETL 
applications. It is a great entryway to writing your first Flink application and requires no Java or Python. 

This tutorial will guide you through writing your first Flink program leveraging SQL alone. Through 
this exercise, you will learn and understand the ease and speed with which you can analyze streaming 
data in Flink! 

## Goals

This tutorial will teach you how to:

- use the Flink SQL client to submit queries 
- consume a data source with Flink SQL
- run a continuous query on a stream of data
- use Flink SQL to write out results to persistent storage 

## Prerequisites 

You only need to have basic knowledge of SQL to follow along.

## Step 1: Start the Flink SQL client 

The [SQL Client]({{< ref "docs/dev/table/sqlClient" >}}) is bundled in the regular Flink distribution 
and can be run out-of-the-box with the provided script. It requires only a running Flink cluster where 
table programs can be executed (since Flink SQL is a thin abstraction over the Table API). 

There are many ways to set up Flink but you will run it locally for the purpose of this tutorial. 
[Download Flink]({{< ref "docs/try-flink/local_installation#downloading-flink" >}}) and [start a local 
cluster]({{< ref "docs/try-flink/local_installation#starting-and-stopping-a-local-cluster" >}}) with 
one worker (the TaskManager).  

The scripts for the SQL client are located in the `/bin` directory of Flink. You can start the client 
by executing:

```sh
./bin/sql-client.sh
```

You should see something like this:

{{< img src="/fig/try-flink/flink-sql.png" alt="Flink SQL client" width="55%" >}}

## Step 2: Set up a data source with flink-faker

Like with any Flink program, you will need a data source to connect to. There are many popular data 
sources but for the interest of this tutorial, you will be using [flink-faker](https://github.com/knaufk/flink-faker). 
This custom [table source]({{< ref "docs/connectors/table/overview" >}}) is based on [Java Faker](https://github.com/DiUS/java-faker) 
and can generate fake data continuously in memory and in a realistic format. 

The next step is to make `flink-faker` discoverable by the Flink SQL client by following these 
[instructions](https://github.com/knaufk/flink-faker#adding-flink-faker-to-flink-sql-client).

Once you have done that, create a table using this table source to confirm that the factory is loaded 
by executing the following query in the SQL client: 

```sql
CREATE TABLE test (`test_field` STRING) WITH ('connector' = 'faker');
```

If you see `[INFO] Execute statement succeed.`, then a table definition has been stored in the in-memory 
catalog successfully.

You are ready to start writing your first Flink program with SQL. 

## Step 3: Consume the data via SQL

For this tutorial, you are going to create a table that models a [Twitch](https://www.twitch.tv) gaming 
stream. This table will contain the following fields: 
- user_name
- game_name
- viewer_count
- started_at
- location
- timestamp

Use the [DDL syntax](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/overview/) 
`CREATE TABLE` to create this table containing these fields. You will also use the `WITH` clause to 
configure the connector (i.e. flink-faker). 

Execute the following query in the SQL client:

```sql
CREATE TEMPORARY TABLE twitch_stream (
  `user_name` STRING,
  `game_name` STRING,
  `viewer_count` INT,
  `started_at` TIMESTAMP_LTZ(3),
  `location` STRING,
  proctime AS PROCTIME()
) WITH (
  'connector' = 'faker',
  'fields.user_name.expression' = '#{name.fullName}',
  'fields.game_name.expression' = '#{book.title}',
  'fields.viewer_count.expression' = '#{number.numberBetween ''0'',''100''}',
  'fields.started_at.expression' = '#{date.past ''15'',''SECONDS''}',
  'fields.location.expression' = '#{country.name}'
);
```

You should hopefully see `[INFO] Execute statement succeed.`

Note that a temporary table does not have a catalog configured and will just be available in this session. 

Let's delve a bit into the notion of time. Within data processing systems, there are typically two 
types of time to reason about: event time (the time at which events actually occurred) and processing 
time (the time at which events are observed in the system). The latter is the [simplest notion of 
time]({{< ref "docs/dev/table/concepts/time_attributes#processing-time" >}}) and is what you will be 
using via `PROCTIME()`.

## Step 4: Run your first continuous SQL query and learn about dynamic tables

Now use the DQL syntax `SELECT` to view the streaming data in this table:

```sql
SELECT * FROM twitch_stream;
```

You should see, on the console of the SQL client, a stream of data populating each of the defined 
fields of the `twitch_stream` table. Notice that this table contains a stream of data and is what is 
known as a [dynamic table]({{< ref "docs/dev/table/concepts/dynamic_tables" >}}).  

Dynamic tables are the fundamental concept behind Flink's SQL support for streaming data. While SQL 
makes it seem like you are querying a database (or static tables that represent batch data, the tables 
in Flink are actually dynamic tables that are defined by queries. So instead of running several different 
queries on the same set of data, you are continuously running one query on a dataset that keeps changing. 

Under the hood, the SQL client submits queries to Flink's JobManager, which works with the TaskManager(s) 
to assign and monitor query tasks. Have a look at [Flink's architecture]({{< ref "docs/concepts/flink-architecture" >}}) 
for more detail. 

## Step 5: Filter the data

Now try to perform a filter operation on this datastream to specify a subset of the data by using 
the `WHERE` keyword.  

To find gaming streams for games whose names end with an exclamation mark, try this query:

```sql
SELECT *
FROM twitch_stream
WHERE game_name LIKE '%!';
```

You should now see a new table with new datastream results.  

## Step 6: Use a Top-N query

Now try a [Top-N]({{< ref "docs/dev/table/sql/queries/topn" >}}) query to find the 10 most popular games.

Top-N queries identify the N smallest or largest values (as ordered by some attribute of the table), 
and are useful when you need to identify the top (or bottom) N items in a stream. 

```sql
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (ORDER BY cnt DESC) AS ranking
    FROM (
        SELECT location, count(*) AS cnt
        FROM twitch_stream
        GROUP BY location
    )
)
WHERE ranking <= 10;
```

Note that Flink uses the combination of an OVER window clause and a filter condition to express a Top-N 
query in order to filter through unbounded datasets. 

You should now see a new dynamic table with the results from this query.  Notice how the top 10 games 
are constantly being revised as new data is processed.

## Step 7: Aggregate the data and learn about windowing 

Now try an aggregate function (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`) to find out how many times each 
game has been played in each location:

```sql
SELECT location,
       game_name,
       COUNT(game_name) 
FROM twitch_stream
WHERE location IS NOT NULL
GROUP BY location, game_name;
```

You should see a table result of continuously changing data that tells you how many times a game has 
been played in each location.

For the last example, let's try a very commonly used function in streaming SQL: computing windowed 
aggregations.

Windows are the fundamental building blocks when it comes to processing infinite streams since they 
provide a way to split streams into finite chunks. Flink SQL offers several [window functions]({{< ref "docs/dev/table/sql/queries/window-tvf" >}}) 
that you can use.  

Let's explore how to apply one of these functions! To calculate an aggregation per game of the total 
viewer count for every 1 minute window, you can use the [tumble]({{< ref "docs/dev/table/sql/queries/window-tvf#tumble" >}}) 
function.

Tumbling windows can be thought of as mini-batches of aggregations over a non-overlapping window of 
time.

```sql
SELECT window_start,
       window_end,
       game_name,
       SUM(viewer_count) AS TotalViewerCount
  FROM TABLE(
    TUMBLE(TABLE twitch_stream, DESCRIPTOR(proctime), INTERVAL '10' SECONDS))
  GROUP BY window_start, window_end, game_name;
```

Fresh results will appear every 10 seconds, showing the sum of the viewer counts for each game.

## Step 8: Write the updated stream to persistent storage

Now that you have written a program with Flink SQL to process all that streaming data, you probably 
want to store it somewhere. Since you are dealing with unbounded data sets, you can think of it as 
maintaining materialized views (or snapshots of the data) in external storage systems.

Flink does not provide its own storage system but instead offers many sink connectors you can use to 
write table updates to.

To write a table to a CSV file, you can use the [FileSystem connector]({{< ref "docs/connectors/table/filesystem" >}}) 
that is built into Flink. This connector supports row-encoded and bulk-encoded formats. Row-encoded 
formats such as CSV and JSON write one row at a time to a file. Bulk-encoded formats collect a batch 
of rows in memory and organize them in a storage-and-scan-efficient format before writing out the data. 

To store the results of the last example query (tumble window), you need to create a table that will 
store the results:

```sql
CREATE TABLE twitch_stream_viewer_count (
  `window_start` TIMESTAMP_LTZ(3),
  `window_end` TIMESTAMP_LTZ(3),
  `game_name` STRING,
  `TotalViewerCount` INT
) WITH (
  'connector' = 'filesystem',
  'path' = 'file:///path/to/directory/results.csv',
  'format' = 'csv'
)
```

Now insert the streaming SQL results into the file system table:

```sql
INSERT INTO twitch_stream_viewer_count 
SELECT 
  `window_start` TIMESTAMP_LTZ(3),
  `window_end` TIMESTAMP_LTZ(3),
  `game_name` STRING,
  `TotalViewerCount` INT
FROM TABLE(
    TUMBLE(TABLE twitch_stream, DESCRIPTOR(proctime), INTERVAL '10' SECONDS))
GROUP BY window_start, window_end, game_name;
```

## Summary

In this tutorial, you learned how to use Flink SQL to define a whole continuous data pipeline. While 
not designed initially with streaming semantics in mind, SQL is a timeless and powerful query language 
that you can write complete programs. Where Flink SQL differs from traditional database queries is that 
it works with dynamic tables and is continuously consuming rows as they arrive and is continuously 
producing updates to the result tables.

All Flink SQL programs follow a similar pattern: define a table source, perform manipulations on the 
data, persist the data to storage.

Moreover, you were introduced to some fundamental concepts when processing unbounded streams of data. 
You learned a bit about [time attributes]({{< ref "docs/dev/table/concepts/time_attributes" >}}) and 
also about [windows]({{< ref "docs/dev/datastream/operators/windows" >}}) and how to apply that with 
Flink SQL on your data.  Windows are the key to turning neverending streams of data into manageable 
chunks for computation and are integral to any stream processor. 

## Next steps

You can do much more with Flink SQL. Check out the [Apache Flink SQL Cookbook](https://github.com/ververica/flink-sql-cookbook) 
for more recipes and patterns and uses cases. :)

We'd love to hear about what you are building with Flink SQL. Feel free to share it via our [mailing 
list](https://flink.apache.org/community.html#mailing-lists) or find us on [Twitter](https://twitter.com/ApacheFlink).   
