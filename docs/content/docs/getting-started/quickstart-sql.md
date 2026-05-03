---
title: "Flink SQL Tutorial"
weight: 2
type: docs
aliases:
  - /dev/table/sql/gettingStarted.html
  - /docs/getting-started/quickstart-sql/
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

# Flink SQL Tutorial

Flink SQL makes it simple to develop streaming applications using standard SQL. It is easy to learn Flink if you have ever worked with a database or SQL-like system by remaining ANSI-SQL 2011 compliant.

## What You'll Learn

In this tutorial, you will:

- Start the Flink SQL Client
- Run interactive SQL queries
- Create source tables from external data
- Write continuous queries that process streaming data
- Output results to sink tables

{{< hint info >}}
**No coding required!** The SQL Client provides an interactive environment where you type SQL queries and see results immediately.
{{< /hint >}}

## Prerequisites

- Basic knowledge of SQL
- A running Flink cluster (follow [First Steps]({{< ref "docs/getting-started/local_installation" >}}))

## Starting the SQL Client

The [SQL Client]({{< ref "docs/sql/interfaces/sql-client" >}}) is an interactive client to submit SQL queries to Flink and visualize the results.

If you used **Docker** in First Steps:

```bash
$ docker compose run sql-client
```

If you used **local installation** in First Steps:

```bash
$ ./bin/sql-client.sh
```

To exit the SQL Client, type `exit;` and press Enter. 

### Hello World

Once the SQL client, our query editor, is up and running, it's time to start writing queries.
Let's start with printing 'Hello World', using the following simple query:
 
```sql
SELECT 'Hello World';
```

Running the `HELP` command lists the full set of supported SQL statements. Let's run one such command, `SHOW`, to see a full list of Flink [built-in functions]({{< ref "docs/sql/functions/built-in-functions" >}}).

```sql
SHOW FUNCTIONS;
```

These functions provide users with a powerful toolbox of functionality when developing SQL queries. 
For example, `CURRENT_TIMESTAMP` will print the machine's current system time where it is executed. 

```sql
SELECT CURRENT_TIMESTAMP;
```

---------------

{{< top >}}

## Source Tables

As with all SQL engines, Flink queries operate on top of tables. 
It differs from a traditional database because Flink does not manage data at rest locally; instead, its queries operate continuously over external tables. 

Flink data processing pipelines begin with source tables. Source tables produce rows operated over during the query's execution; they are the tables referenced in the `FROM` clause of a query.  These could be Kafka topics, databases, filesystems, or any other system that Flink knows how to consume. 

Tables can be defined through the SQL client or using environment config file. The SQL client support [SQL DDL commands]({{< ref "docs/sql/reference/overview" >}}) similar to traditional SQL. Standard SQL DDL is used to [create]({{< ref "docs/sql/reference/ddl/create" >}}), [alter]({{< ref "docs/sql/reference/ddl/alter" >}}), [drop]({{< ref "docs/sql/reference/ddl/drop" >}}) tables. 

Flink supports different [connectors]({{< ref "docs/connectors/table/overview" >}}) and [formats]({{< ref "docs/connectors/table/formats/overview" >}}) that can be used with tables. Following is an example to define a source table using the [DataGen connector]({{< ref "docs/connectors/table/datagen" >}}), which generates sample data automatically.

```sql
CREATE TABLE employee_information (
    emp_id INT,
    name STRING,
    dept_id INT
) WITH (
    'connector' = 'datagen',
    'rows-per-second' = '10',
    'fields.emp_id.kind' = 'sequence',
    'fields.emp_id.start' = '1',
    'fields.emp_id.end' = '1000000',
    'fields.name.length' = '8',
    'fields.dept_id.min' = '1',
    'fields.dept_id.max' = '5'
);
```

This creates a table that generates 10 rows per second with sequential employee IDs, random 8-character names, and department IDs between 1 and 5. 

A continuous query can be defined from this table that reads new rows as they are made available and immediately outputs their results. 
For example, you can filter for just those employees who work in department `1`. 

```sql
SELECT * from employee_information WHERE dept_id = 1;
``` 

---------------

{{< top >}}

## Continuous Queries

While not designed initially with streaming semantics in mind, SQL is a powerful tool for building continuous data pipelines. Where Flink SQL differs from traditional database queries is that is continuously consuming rows as the arrives and produces updates to its results. 

A [continuous query]({{< ref "docs/concepts/sql-table-concepts/dynamic_tables" >}}#continuous-queries) never terminates and produces a dynamic table as a result. [Dynamic tables]({{< ref "docs/concepts/sql-table-concepts/dynamic_tables" >}}#continuous-queries) are the core concept of Flink's Table API and SQL support for streaming data. 

Aggregations on continuous streams need to store aggregated results continuously during the execution of the query. For example, suppose you need to count the number of employees for each department from an incoming data stream. The query needs to maintain the most up to date count for each department to output timely results as new rows are processed.

 ```sql
SELECT 
	dept_id,
	COUNT(*) as emp_count 
FROM employee_information 
GROUP BY dept_id;
 ``` 

Such queries are considered _stateful_. Flink's advanced fault-tolerance mechanism will maintain internal state and consistency, so queries always return the correct result, even in the face of hardware failure. 

## Sink Tables

When running this query, the SQL client provides output in real-time but in a read-only fashion. Storing results (for example, to power a report or dashboard) requires writing out to another table. This can be achieved using an `INSERT INTO` statement. The table referenced in this clause is known as a sink table. An `INSERT INTO` statement will be submitted as a detached query to the Flink cluster.

First, create the sink table. This example uses the `print` connector, which writes results to the TaskManager's log:

```sql
CREATE TABLE department_counts (
    dept_id INT,
    emp_count BIGINT
) WITH (
    'connector' = 'print'
);
```

Now insert the aggregated results into this table:

```sql
INSERT INTO department_counts
SELECT
    dept_id,
    COUNT(*) as emp_count
FROM employee_information
GROUP BY dept_id;
```

Once submitted, this runs as a background job and continuously writes results to the sink table. You can view the output in the TaskManager logs:

```bash
$ docker compose logs -f taskmanager
```

In production, you would typically use connectors like [JDBC]({{< ref "docs/connectors/table/jdbc" >}}), [Kafka]({{< ref "docs/connectors/table/kafka" >}}), or [Filesystem]({{< ref "docs/connectors/table/filesystem" >}}) to write to external systems. 

---------------

{{< top >}}

## Next Steps

Now that you've experienced Flink SQL, here are some paths to continue learning:

### Dive Deeper into Flink SQL

- [SQL Reference]({{< ref "docs/sql/reference/overview" >}}): Complete SQL syntax and supported operations
- [SQL Client]({{< ref "docs/sql/interfaces/sql-client" >}}): Advanced SQL Client features and configuration
- [Built-in Functions]({{< ref "docs/sql/functions/built-in-functions" >}}): All available functions for SQL queries
- [Connectors]({{< ref "docs/connectors/table/overview" >}}): Connect to Kafka, databases, filesystems, and more

### Understand Streaming Concepts

- [Dynamic Tables]({{< ref "docs/concepts/sql-table-concepts/dynamic_tables" >}}): How Flink SQL handles streaming data
- [Time Attributes]({{< ref "docs/concepts/sql-table-concepts/time_attributes" >}}): Working with event time and processing time

### Try Other Tutorials

- [Table API Tutorial]({{< ref "docs/getting-started/table_api" >}}): Build a complete streaming pipeline with Java or Scala (requires Docker)
- [Flink Operations Playground]({{< ref "docs/getting-started/flink-operations-playground" >}}): Learn to operate Flink clusters (requires Docker)

## Getting Help

If you get stuck, check out the [community support resources](https://flink.apache.org/community.html).
Apache Flink's [user mailing list](https://flink.apache.org/community.html#mailing-lists) is one of the most active of any Apache project and a great way to get help quickly.

---------------

{{< top >}}
