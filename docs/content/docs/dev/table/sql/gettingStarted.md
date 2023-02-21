---
title: "Getting Started"
weight: 2
type: docs
aliases:
  - /dev/table/sql/gettingStarted.html
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

# Getting Started

Flink SQL makes it simple to develop streaming applications using standard SQL. It is easy to learn Flink if you have ever worked with a database or SQL like system by remaining ANSI-SQL 2011 compliant. This tutorial will help you get started quickly with a Flink SQL development environment. 

### Prerequisites 

You only need to have basic knowledge of SQL to follow along. No other programming experience is assumed. 

### Installation

There are multiple ways to install Flink. For experimentation, the most common option is to download the binaries and run them locally. You can follow the steps in [local installation]({{< ref "docs/try-flink/local_installation" >}}) to set up an environment for the rest of the tutorial. 

Once you're all set, use the following command to start a local cluster from the installation folder:

```bash
./bin/start-cluster.sh
```
 
Once started, the Flink WebUI on [localhost:8081](localhost:8081) is available locally, from which you can monitor the different jobs.

### SQL Client

The [SQL Client]({{< ref "docs/dev/table/sqlClient" >}}) is an interactive client to submit SQL queries to Flink and visualize the results. 
To start the SQL client, run the `sql-client` script from the installation folder.

 ```bash
./bin/sql-client.sh
 ``` 

### Hello World

Once the SQL client, our query editor, is up and running, it's time to start writing queries.
Let's start with printing 'Hello World', using the following simple query:
 
```sql
SELECT 'Hello World';
```

Running the `HELP` command lists the full set of supported SQL statements. Let's run one such command, `SHOW`, to see a full list of Flink [built-in functions]({{< ref "docs/dev/table/functions/systemFunctions" >}}).

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

Tables can be defined through the SQL client or using environment config file. The SQL client support [SQL DDL commands]({{< ref "docs/dev/table/sql/overview" >}}) similar to traditional SQL. Standard SQL DDL is used to [create]({{< ref "docs/dev/table/sql/create" >}}), [alter]({{< ref "docs/dev/table/sql/alter" >}}), [drop]({{< ref "docs/dev/table/sql/drop" >}}) tables. 

Flink has a support for different [connectors]({{< ref "docs/connectors/table/overview" >}}) and [formats]({{< ref "docs/connectors/table/formats/overview" >}}) that can be used with tables. Following is an example to define a source table backed by a [CSV file]({{< ref "docs/connectors/table/formats/csv" >}}) with `emp_id`, `name`, `dept_id` as columns in a `CREATE` table statement.

```sql
CREATE TABLE employee_information (
    emp_id INT,
    name VARCHAR,
    dept_id INT
) WITH ( 
    'connector' = 'filesystem',
    'path' = '/path/to/something.csv',
    'format' = 'csv'
);
``` 

A continuous query can be defined from this table that reads new rows as they are made available and immediately outputs their results. 
For example, we can filter for just those employees who work in department `1`. 

```sql
SELECT * from employee_information WHERE dept_id = 1;
``` 

---------------

{{< top >}}

## Continuous Queries

While not designed initially with streaming semantics in mind, SQL is a powerful tool for building continuous data pipelines. Where Flink SQL differs from traditional database queries is that is continuously consuming rows as the arrives and produces updates to its results. 

A [continuous query]({{< ref "docs/dev/table/concepts/dynamic_tables" >}}#continuous-queries) never terminates and produces a dynamic table as a result. [Dynamic tables]({{< ref "docs/dev/table/concepts/dynamic_tables" >}}#continuous-queries) are the core concept of Flink's Table API and SQL support for streaming data. 

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

When running this query, the SQL client provides output in real-time but in a read-only fashion. Storing results - to power a report or dashboard - requires writing out to another table. This can be achieved using an `INSERT INTO` statement. The table referenced in this clause is known as a sink table. An `INSERT INTO` statement will be submitted as a detached query to the Flink cluster. 

 ```sql
INSERT INTO department_counts
 SELECT 
	dept_id,
	COUNT(*) as emp_count 
FROM employee_information;
 ``` 
 
Once submitted, this will run and store the results into the sink table directly, instead of loading the results into the system memory. 

---------------

{{< top >}}

## Looking for Help! 

If you get stuck, check out the [community support resources](https://flink.apache.org/community.html).
In particular, Apache Flink's [user mailing list](https://flink.apache.org/community.html#mailing-lists) consistently ranks as one of the most active of any Apache project and a great way to get help quickly. 

## Resources to Learn more

* [SQL]({{< ref "docs/dev/table/sql/overview" >}}): Supported operations and syntax for SQL.
* [SQL Client]({{< ref "docs/dev/table/sqlClient" >}}): Play around with Flink SQL and submit a table program to a cluster without programming knowledge
* [Concepts & Common API]({{< ref "docs/dev/table/common" >}}): Shared concepts and APIs of the Table API and SQL.
* [Streaming Concepts]({{< ref "docs/dev/table/concepts/overview" >}}): Streaming-specific documentation for the Table API or SQL such as configuration of time attributes and handling of updating results.
* [Built-in Functions]({{< ref "docs/dev/table/functions/systemFunctions" >}}): Supported functions in Table API and SQL.
* [Connect to External Systems]({{< ref "docs/connectors/table/overview" >}}): Available connectors and formats for reading and writing data to external systems.

---------------

{{< top >}}
