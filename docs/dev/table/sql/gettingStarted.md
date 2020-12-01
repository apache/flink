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


* This will be replaced by the TOC
{:toc}

Flink SQL make it easier to develop applications using standad SQL. Flink’s SQL support is based on [Apache Calcite](https://calcite.apache.org/) which implements the SQL standard and it's easy to learn even for users with no programming background. This tutorial helps you quickly set up  Flink SQL development environment and write hello world!

### Pre-requisites
You only need to have basic knowledge of SQL to follow along. You will not need to write Java or Scala code or use an IDE.

### Installation
There are multiple ways to install Flink. For experimentation, the most common option is to download the binaries and run them locally. You can follow the steps in ["Local Installation"]({%link try-flink/local_installation.md %}) to set up the environment for the rest of the tutorial. 

Once you're all set, use the following command to start a local cluster from the installation folder:

{% highlight bash %}
./bin/start-cluster.sh
{% endhighlight %}
 
Once the cluster is started, it will also start the Flink WebUI on [localhost:8081](localhost:8081) where you can manage settings and monitor the different jobs.

### SQL Client
The [SQL Client]({% link dev/table/sqlClient.md %}) is an interactive client to submit SQL queries to Flink and visualize the results. It’s like a query editor for any other database management system where you can write queries using standard SQL. To start the SQL client from the installation folder, run:

 {% highlight bash %}
./bin/sql-client.sh embedded
 {% endhighlight %} 

### Hello World
 
Once the SQL client, our query editor, is up and running it's time to start writing SQL queries. These queries will be submitted to Flink cluster for computation and results will be returned to the SQL Client UI. Let's start with printing 'Hello World', using the following simple query:
 
{% highlight sql %}
SELECT 'Hello World';
{% endhighlight %}

To see the different supported SQL statements, use the `HELP;` command. Furthermore, Flink SQL supports different [built-in functions]({% link dev/table/functions/systemFunctions.md %}) that you can list using the `SHOW` statement:

{% highlight sql %}
SHOW FUNCTIONS;
{% endhighlight %}

Flink SQL provides users with a set of [built-in functions]({%link dev/table/functions/systemFunctions.md %}) for data transformations. The following example will print the current timestamp using the `CURRENT_TIMESTAMP` function.

{% highlight sql %}
SELECT CURRENT_TIMESTAMP;
{% endhighlight %}

---------------

{% top %}

## Tables in Flink SQL
Although Flink is a stream processing engine, it allows you to define tables on top of streaming data (see ["Dynamic Tables"]({% link dev/table/streaming/dynamic_tables.md %}) and to then run [continuous queries]({% link dev/table/streaming/dynamic_tables.md %}) against these tables.

Flink data processing pipelines have three components: source, compute and sink. The source is where data will be read from (e.g. files, databases, Kafka topics). Then, you define some computations that will be performed on the input data to transform it. Finally, the sink defines what to do with the output — or, where to store the results. This is similar to a database query, where you read data from a table and perform a query on it to display the results or insert them somewhere else. 

In Flink SQL semantics, source and sink will be tables, but Flink isn’t a storage engine hence it cannot store the data. So Flink tables need to backed up with a [storage connector]({{ site.baseurl }}/dev/table/connect.html) like [file system]({{ site.baseurl }}/dev/table/connect.html#file-system-connector), [Kafka]({{ site.baseurl }}/dev/table/connect.html#kafka-connector) or [MySQL]({{ site.baseurl }}/dev/table/connect.html#jdbc-connector). While creating these tables, storage connector type, [format]({{ site.baseurl }}/dev/table/connect.html#table-formats) and schema for each table needs to be defined. 

 
### Source Tables
SQL API environment is configured via [YAML](https://yaml.org) configuration files. When the SQL client starts, it reads the default configuration from the `/conf/sql-client-defaults.yaml` but it can be overriden by user defined configuration file. These files are used to define different environment variables including table source, sinks, [catalogs]({{ site.baseurl }}/dev/table/catalogs.html), [user-defined functions]({{ site.baseurl }}/dev/table/functions/udfs.html).

Tables can be defined through the SQL client or using environment config file. The SQL client support [SQL DDL commands]({{ site.baseurl }}/dev/table/sql) similar to traditional SQL. Standard SQL DDL is used to [create]({{ site.baseurl }}/dev/table/sql/create.html), [alter]({{ site.baseurl }}/dev/table/sql/alter.html), [drop]({{ site.baseurl }}/dev/table/sql/drop.html) tables. 

Flink has a support for different [connectors]({% link dev/table/connect.md %}) and [formats]({%link dev/table/connectors/formats/index.md %}) that can be used with tables. Following is an example to define a source table backed by a [CSV file]({%link dev/table/connectors/formats/csv.md %}) with `EmpId`, `EmpName`, `DeptId` as header fields, using SQL DDL.

{% highlight sql %}
CREATE TABLE EmployeeTableSource (
    EmpId INT,
    EmpName VARCHAR,
    DeptId INT
) WITH ( 
    'connector' = 'filesystem',
    'path' = '/path/to/something.csv',
    'format' = 'csv'
);
{% endhighlight %} 

You can also choose to define this source table in the environment configuration file:

{% highlight yaml %}

tables:
  - name: EmployeeTableSource
    type: source-table
    update-mode: append
    connector:
      type: filesystem
      path: "/path/to/something.csv"
    format:
      type: csv
      fields:
        - name: EmpId
          data-type: INT
        - name: EmpName
          data-type: VARCHAR
        - name: DeptId
          data-type: INT          
      line-delimiter: "\n"
      comment-prefix: "#"
    schema:
      - name: EmpId
        data-type: INT
      - name: EmpName
        data-type: VARCHAR
      - name: DeptId
        data-type: INT
{% endhighlight %} 



If, for example, you need to filter all the employees who belong to a certain department, you can use the `WHERE` clause to filter the records on `DeptId` as follows:

{% highlight sql %}
SELECT * from EmployeeTableSource WHERE DeptId = 1;
{% endhighlight %} 

### Sink Tables
The SQL client provides us the output in real-time, but it’s view only. If results need to be stored, a sink or output table where the results of the query will be stored should be defined. Similar to input source tables, sink tables are defined in the environment file or using SQL DDL statements. See ["Table & SQL Connectors"]({%link dev/table/connect.md %}) for more information about supported external systems and their configuration.  

---------------

{% top %}

## Execution Modes
Once the source tables are defined, you can start writing SQL queries like you would in any other database! One important difference here is that, in most traditional database systems, queries are executed _once_ against a snapshot of the data at execution time and then finish, returning a final result. The query input is _bounded_ and data at rest is being processed in a _batch processing_ fashion. 

On the other hand, Flink can process data in motion (or, a continuous stream of data) in a _stream processing_ fashion. In this case, the query input is _unbounded_, so the result is continuously updated and processing never finishes. 

The SQL API has different [execution environments]({% link dev/table/common.md %}#create-a-tableenvironment) for **batch** and **stream** processing. However, the semantics of the query are the same, irrespective of whether it is executed one-time on a table snapshot or continuously on a changing table. In the SQL Client, all you have to do is change the [`execution type`]({% link dev/table/common.md %}#create-a-tableenvironment.md) in the configuration file.

{% highlight text %}
-- SQL Client Default: execution.type=streaming
SET execution.type=batch; 
{% endhighlight %}

Futhermore, the CLI supports [three modes]({%link dev/table/sqlClient.md %}#running-sql-queries) for maintaining and visualizing results. Most standard SQL operations are available for both stream and batch mode. Streaming has a few additional operators which are specific to continuous data processing, such as [Temporal Joins]({% link dev/table/streaming/temporal_tables.md%}) and [Pattern recognition]({%link dev/table/streaming/match_recognize.md%}).

## Dynamic tables and continuous queries
Standard SQL has not been designed with streaming data in mind. As a consequence, there are a few conceptual gaps between standard SQL and stream processing. SQL is designed for finite or data-at-rest processing. SQL queries are completed after processing the fixed size of the input data. On the contrary, streaming queries are designed for an infinite amount of data. Therefore, they continue execution forever as queries do not have access to all the data. 

A [continuous query](/table/streaming/dynamic_tables.html#continuous-queries) never terminates and produces a dynamic table as result. [Dynamic tables](/table/streaming/dynamic_tables.html) are the core concept of Flink’s Table API and SQL support for streaming data. 

Aggregations on continuous streams need to store aggregated results continuously during the execution of the query. For example, if you need to count the number of employees for each department from an incoming data stream, the aggregated count needs to be stored for each department during the execution of the query. 

 {% highlight sql %}
 SELECT count(1) as EmpCount, DeptId from EmployeeTableSource GROUP BY DeptId;
 {% endhighlight %} 

Such queries, where state needs to be stored, are referred to as _stateful_. Under the hood, this is similar to a materialized view that caches the result of the query such that the query doesn't need to be evaluated when the view is accessed. During the execution of the query, the state can grow with every new record processed. 

## Detached mode
So far, We have seen the query execution using the SQL client but does that mean that the SQL client needs to be running all the time? the answer is no. SQL `INSERT INTO` can be used to submit detached queries to Flink cluster. 

 {% highlight sql %}
 INSERT INTO MyTableSink SELECT * FROM MyTableSource
 {% endhighlight %} 
 
Once submitted, such a query can only be controlled from Flink cluster (for example through the Flink WebUI). It will run and store the results into the sink table directly, instead of loading the results into the system memory. 

---------------

{% top %}

## Looking for Help! 

If you get stuck, check out the [community support resources](https://flink.apache.org/community.html).
In particular, Apache Flink's [user mailing list](https://flink.apache.org/community.html#mailing-lists) consistently ranks as one of the most active of any Apache project and a great way to get help quickly. 

## Resources to Learn more

* [SQL]({{ site.baseurl }}/dev/table/sql/index.html): Supported operations and syntax for SQL.
* [SQL Client]({{ site.baseurl }}/dev/table/sqlClient.html): Play around with Flink SQL and submit a table program to a cluster without programming knowledge
* [Concepts & Common API]({{ site.baseurl }}/dev/table/common.html): Shared concepts and APIs of the Table API and SQL.
* [Streaming Concepts]({{ site.baseurl }}/dev/table/streaming): Streaming-specific documentation for the Table API or SQL such as configuration of time attributes and handling of updating results.
* [Built-in Functions]({{ site.baseurl }}/dev/table/functions/systemFunctions.html): Supported functions in Table API and SQL.
* [Connect to External Systems]({{ site.baseurl }}/dev/table/connect.html): Available connectors and formats for reading and writing data to external systems.

---------------

{% top %}
