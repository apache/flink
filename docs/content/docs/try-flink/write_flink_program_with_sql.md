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

## Introduction

Flink features [multiple APIs](https://nightlies.apache.org/flink/flink-docs-master/docs/concepts/overview/) with different levels of abstraction that can be used to develop your streaming application. SQL is the highest level of abstraction and is supported by Flink as a relational unified API for batch and stream processing. This means that you can write the same queries on both unbounded real-time streams and bounded recorded streams and produce the same results. 

SQL on Flink is based on [Apache Calcite](https://calcite.apache.org/) (which is based on standard SQL) and is commonly used to ease the process of data analytics, data pipelining, and ETL applications.  It is a great entry way to writing your first Flink application and requires no need for Java or Python. 

This tutorial will guide you through writing your first Flink program leveraging SQL alone. Through this exercise you will learn and understand the ease and speed with which you can analyze streaming data in Flink! 


## Goals

This tutorial will teach you how to:

- use the Flink SQL client to submit queries 
- consume a data source with Flink SQL
- run a continuous query on a stream of data
- use Flink SQL to write out results to persistent storage 


## Prerequisites 

You only need to have basic knowledge of SQL to follow along.

## Step 1: Start the Flink SQL client 

The [SQL Client](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sqlclient/) is bundled in the regular Flink distribution and runnable out-of-the-box. It requires only a running Flink cluster where table programs can be executed (since Flink SQL is a thin abstraction over the Table API). 

There are many ways to set up Flink but you will run it locally for the purpose of this tutorial. [Download Flink](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/try-flink/local_installation/#downloading-flink) and [start a local cluster](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/try-flink/local_installation/#starting-and-stopping-a-local-cluster) with one worker (or TaskManager).  

The scripts for the SQL client are located in the `/bin` directory of Flink. You can start the client by executing:

```sh
./bin/sql-client.sh
```

You should see something like this:

{{< img src="/fig/try-flink/flink-sql.png" alt="Flink SQL client" >}}

## Step 2: Set up a data source with flink-faker

Like with any Flink program, you will need a data source to connect to so that Flink can process it. There are many popular data sources but for the interest of this tutorial, you will be using [flink-faker](https://github.com/knaufk/flink-faker).  This custom [table source](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/table/overview/) is based on [Java Faker](https://github.com/DiUS/java-faker) and can generate fake data continuously and in a realistic format. 

Java Faker is a tool for generating this data and flink-faker exposes that as a source in Flink by implementing the [DynamicTableSource interface](https://nightlies.apache.org/flink/flink-docs-release-1.11/api/java/org/apache/flink/table/connector/source/DynamicTableSource.html). The dynamic table source has the logic of how to create a table source (in this case, from flink-faker), and then by adding a factory (https://nightlies.apache.org/flink/flink-docs-master/api/java/org/apache/flink/table/factories/DynamicTableSourceFactory.html) for it you can expose it in the SQL API by referencing it with `"connector" = "faker"`.

The next step is to make `flink-faker` discoverable by the Flink SQL client by following these [instructions](https://github.com/knaufk/flink-faker#adding-flink-faker-to-flink-sql-client).

Once you have done that, create a table using this table source to confirm that the factory is loaded by executing the following query in the SQL client: 

```sql
CREATE TABLE test WITH ('connector' = 'faker');
```

If you see `[INFO] Execute statement succeed.`, then the table source has been loaded correctly. :) 

You are ready to start writing your first Flink program with SQL. 

## Step 3: Consume the data via SQL

Now you are going to create a table that models a [Twitch](https://www.twitch.tv) gaming stream. This table will contain the following fields: id, user_name, game_id, game_name, type, viewer_count, started_at, and timestamp (TS).

Use the [DDL syntax](https://www.geeksforgeeks.org/sql-ddl-dql-dml-dcl-tcl-commands/) `CREATE TABLE` to create this table containing these fields. You will also use the `WITH` clause to display the attributes to connect to external systems (i.e. flink-faker). 

Execute the following query in the SQL client:

```sql
CREATE TABLE twitch_stream (
  `user_name` STRING,
  `game_id` STRING,
  `game_name` STRING,
  `viewer_count` INT,
  `started_at` TIMESTAMP,
  `location` STRING
) WITH (
  'connector' = 'faker',
  'fields.user_name.expression' = '#{name.fullName}',
  'fields.game_id.expression' = '#{idnumber.invalid}',
  'fields.game_name.expression' = '#{book.title}',
  'fields.viewer_count.expression' = '#{number.numberBetween ''0'',''100''}',
  'fields.started_at.expression' = '#{date.past ''15'',''SECONDS''}',
  'fields.location.expression' = '#{country.name}'
);
```

You should hopefully see `[INFO] Execute statement succeed.`

Note: Since you don't have a catalog configured, this table will just be available in this session. 


## Step 4: Run your first continuous SQL query and learn about dynamic tables

Now use the DQL syntax `SELECT` to view the streaming data in this table:

```sql
SELECT * FROM twitch_stream;
```

You should see, on the console of the SQL client, a stream of data populating each of the defined fields of the twitch_stream table.  Notice that this table contains a stream of data and is what is known as a [dynamic table](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/concepts/dynamic_tables/).  

Dynamic tables are the fundamental concept behind Flink's SQL support for streaming data. While SQL makes it seem like you are querying a database (or static tables that represent batch data, the tables in Flink are actually dynamic tables that are defined by queries. So instead of running several different queries on the same set of data, you are continuously running one query on a dataset that keeps changing. 

Under the hood, the SQL client submits queries to Flink's JobManager, which works with the TaskManager to assign and monitor query tasks. Have a look at [Flink's architecture](https://nightlies.apache.org/flink/flink-docs-master/docs/concepts/flink-architecture/) for more detail. 


## Step 5: Filter the data

Now try to perform a filter operation on this data stream by using the `WHERE` keyword.  

To find out what gaming streams started in the last 15 minutes:

```sql
SELECT user_name,
       game_name,
       started_at
FROM twitch_stream
WHERE game_id IS NOT NULL
  AND (mz_logical_timestamp() >= (extract('epoch' from started_at)*1000)::bigint 
  AND mz_logical_timestamp() < (extract('epoch' from started_at)*1000)::bigint + 900000);
```
 

## Step 6: Aggregate the data

For example, to find the most popular games, execute the following query: 

```sql
SELECT game_name,
       SUM(viewer_count) AS agg_viewer_cnt
FROM twitch_stream
WHERE game_id IS NOT NULL
GROUP BY game_name
```

```sql
SELECT game_name, viewer_count
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY game_name ORDER BY viewer_count DESC) AS row_num
  FROM twitch_stream)
WHERE row_num <= 10
```

You should now see a new table with these new datastream results.  



## Step 7: Write the updated stream to persistant storage



## Summary
- you can define whole data pipelines in pure SQL using its SQL client
- SQL provides abstraction to data access and manipulation - timeless

You have written a full program with a query language


## Next steps

 reference SQL cookbook