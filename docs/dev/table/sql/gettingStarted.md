---
title: "Getting Started"
nav-parent_id: sql
nav-pos: 0
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

Flink SQL makes it simple to develop streaming applications using standard SQL. It is easy to learn Flink if you have ever worked with a database or SQL like system by remaining ANSI-SQL 2011 compliant. This tutorial will help you get started quickly with a Flink SQL development environment. 
 
* This will be replaced by the TOC
{:toc}


### Prerequisetes 

You only need to have basic knowledge of SQL to follow along. No other programming experience is assumed. 

### Installation

There are multiple ways to install Flink. For experimentation, the most common option is to download the binaries and run them locally. You can follow the steps in [local nstallation]({%link try-flink/local_installation.md %}) to set up an environment for the rest of the tutorial. 

Once you're all set, use the following command to start a local cluster from the installation folder:

{% highlight bash %}
./bin/start-cluster.sh
{% endhighlight %}
 
Once started, the Flink WebUI on [localhost:8081](localhost:8081) is available locally, from which you can monitor the different jobs.

### SQL Client

The [SQL Client]({% link dev/table/sqlClient.md %}) is an interactive client to submit SQL queries to Flink and visualize the results. 
To start the SQL client, run the `sql-client` script from the installation folder.

 {% highlight bash %}
./bin/sql-client.sh embedded
 {% endhighlight %} 

### Hello World
 
Once the SQL client, our query editor, is up and running, it's time to start writing queries.
Let's start with printing 'Hello World', using the following simple query:
 
{% highlight sql %}
SELECT 'Hello World';
{% endhighlight %}

Running the `HELP` command lists the full set of supported SQL statements. Let's run one such command, `SHOW`, to see a full list of Flink [built-in functions]({% link dev/table/functions/systemFunctions.md %}).

{% highlight sql %}
SHOW FUNCTIONS;
{% endhighlight %}

These functions provide users with a powerful toolbox of functionality when developing SQL queries. 
For example, `CURRENT_TIMESTAMP` will print the machine's current system time where it is executed. 

{% highlight sql %}
SELECT CURRENT_TIMESTAMP;
{% endhighlight %}

---------------

{% top %}

## Source Tables

As with all SQL engines, Flink queries operate on top of tables. 
It differs from a traditional database because Flink does not manage data at rest locally; instead, its queries operate continuously over external tables. 

Flink data processing pipelines begin with source tables. Source tables produce rows operated over during the query's execution; they are the tables referenced in the `FROM` clause of a query.  These could be Kafka topics, databases, filesystems, or any other system that Flink knows how to consume. 

Tables can be defined through the SQL client or using environment config file. The SQL client support [SQL DDL commands]({% link dev/table/sql/index.md %}) similar to traditional SQL. Standard SQL DDL is used to [create]({% link dev/table/sql/create.md %}), [alter]({% link dev/table/sql/alter.md %}), [drop]({% link dev/table/sql/drop.md %}) tables. 

Flink has a support for different [connectors]({% link dev/table/connect.md %}) and [formats]({% link dev/table/connectors/formats/index.md %}) that can be used with tables. Following is an example to define a source table backed by a [CSV file]({%link dev/table/connectors/formats/csv.md %}) with `emp_id`, `name`, `dept_id` as columns in a `CREATE` table statement.

{% highlight sql %}
CREATE TABLE employee_information (
    emp_id INT,
    name VARCHAR,
    dept_id INT
) WITH ( 
    'connector' = 'filesystem',
    'path' = '/path/to/something.csv',
    'format' = 'csv'
);
{% endhighlight %} 

A continuous query can be defined from this table that reads new rows as they are made available and immediately outputs their results. 
For example, we can filter for just those employees who work in department `1`. 

{% highlight sql %}
SELECT * from employee_information WHERE DeptId = 1;
{% endhighlight %} 

---------------

{% top %}

## Continuous Queries

While not designed initially with streaming semantics in mind, SQL is a powerful tool for building continuous data pipelines. Where Flink SQL differs from traditional database queries is that is continuously consuming rows as the arrives and produces updates to its results. 

A [continuous query]({% link dev/table/streaming/dynamic_tables.md %}#continuous-queries) never terminates and produces a dynamic table as a result. [Dynamic tables]({% link dev/table/streaming/dynamic_tables.md %}#continuous-queries) are the core concept of Flink's Table API and SQL support for streaming data. 

Aggregations on continuous streams need to store aggregated results continuously during the execution of the query. For example, suppose you need to count the number of employees for each department from an incoming data stream. The query needs to maintain the most up to date count for each department to output timely results as new rows are processed.

 {% highlight sql %}
 SELECT 
	dept_id,
	COUNT(*) as emp_count 
FROM employee_information 
GROUP BY dep_id;
 {% endhighlight %} 

Such queries are considered _stateful_. Flink's advanced fault-tolerance mechanism will maintain internal state and consistency, so queries always return the correct result, even in the face of hardware failure. 

## Sink Tables

When running this query, the SQL client provides output in real-time but in a read-only fashion. Storing results - to power a report or dashboard - requires writing out to another table. This can be achieved using an `INSERT INTO` statement. The table referenced in this clause is known as a sink table. An `INSERT INTO` statement will be submitted as a detached query to the Flink cluster. 

 {% highlight sql %}
 INSERT INTO department_counts
 SELECT 
	dept_id,
	COUNT(*) as emp_count 
FROM employee_information;
 {% endhighlight %} 
 
Once submitted, this will run and store the results into the sink table directly, instead of loading the results into the system memory. 

---------------

{% top %}

## Looking for Help! 

If you get stuck, check out the [community support resources](https://flink.apache.org/community.html).
In particular, Apache Flink's [user mailing list](https://flink.apache.org/community.html#mailing-lists) consistently ranks as one of the most active of any Apache project and a great way to get help quickly. 

## Resources to Learn more

* [SQL]({% link dev/table/sql/index.md %}): Supported operations and syntax for SQL.
* [SQL Client]({% link dev/table/sqlClient.md %}): Play around with Flink SQL and submit a table program to a cluster without programming knowledge
* [Concepts & Common API]({% link dev/table/common.md %}): Shared concepts and APIs of the Table API and SQL.
* [Streaming Concepts]({% link dev/table/streaming/index.md %}): Streaming-specific documentation for the Table API or SQL such as configuration of time attributes and handling of updating results.
* [Built-in Functions]({% link dev/table/functions/systemFunctions.md %}): Supported functions in Table API and SQL.
* [Connect to External Systems]({% link dev/table/connect.md %}): Available connectors and formats for reading and writing data to external systems.

---------------

{% top %}
