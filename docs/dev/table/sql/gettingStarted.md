---
title: "Getting Started - Flink SQL"
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

Flink SQL enables SQL developers to design and develop the batch or streaming application without writing the Java, Scala, or any other programming language code. It provides a unified API for both stream and batch processing. As a user, you can perform powerful transformations. Flink’s SQL support is based on [Apache Calcite](https://calcite.apache.org/) which implements the SQL standard.

In addition to the SQL API, Flink also has a Table API with similar semantics as SQL. The Table API is a language-integrated API, where users develop in a specific programming language to write the queries or call the API. For example, jobs create a table environment, read a table, and apply different transformations and aggregations, and write the results back to another table. It supports different languages e.g. Java, Scala, Python. 
 
Flink SQL and Table API are just two different ways to write queries that use the same Flink runtime. All the queries are optimized for efficient execution. SQL API is a more descriptive way of writing queries using well-known SQL standards e.g. `select * from Table`. On the other hand, Table API queries start with from clause, followed by joins and where clause, and then finally projection or select at the last e.g. `Table.filter(...).select(...)`. Standard SQL is easy and quick to learn even for users with no programming background. This article will focus on Flink SQL API but Table API details can be found [here]({{ site.baseurl }}/dev/table/).

### Pre-requisites
You only need to have basic knowledge of SQL to follow along. You will not need to write Java or Scala code or use an IDE.

### Installation
There are various ways to [install]({{ site.baseurl }}/ops/deployment/) Flink. Probably the easiest one is to download the binaries and run them locally for experimentation. We assume [local installation]({{ site.baseurl }}/try-flink/local_installation.html) for the rest of the tutorial. You can start a local cluster using the following command from the installation folder
 
{% highlight bash %}
./bin/start-cluster.sh
{% endhighlight %}
 
Once the cluster is started, it will also start a web server on [localhost:8081](localhost:8081) to manage settings and monitor the different jobs.

### SQL Client
The SQL Client is an interactive client to submit SQL queries to Flink and visualize the results. It’s like a query editor for any other database management system where you can write queries using standard SQL. You can start the SQL client from the installation folder as follows

 {% highlight bash %}
./bin/sql-client.sh embedded
 {% endhighlight %} 

### Hello World query
 
Once the SQL client, our query editor, is up and running it's time to start writing SQL queries. These queries will be submitted to Flink cluster for computation and results will be returned to the SQL client UI. Let's start with printing 'Hello World'. You can print hello world using the following simple query
 
{% highlight sql %}
SELECT 'Hello World';
{% endhighlight %}

`Help;` command is used to see different supported DDL (Data definition language) commands. Furthermore, Flink SQL does support different built-in functions as well. The following query will show all the built-in and user-defined functions. 
{% highlight sql %}
SHOW FUNCTIONS;
{% endhighlight %}

Flink SQL provides users with a set of [built-in functions]({{ site.baseurl }}/dev/table/functions/systemFunctions.html) for data transformations. The following example will print the current timestamp using the `CURRENT_TIMESTAMP` function.

{% highlight sql %}
SELECT CURRENT_TIMESTAMP;
{% endhighlight %}

---------------

{% top %}
## Setting up tables
Real-world database queries are run against the SQL tables. Although Flink is a stream processing engine, users can define a table on top of the streaming data. Generally, Flink data processing pipelines have three components - source, compute, sink. 

The source is input or from where data is read e.g. a text file, Kafka topic. Then we define some computations that need to be performed on input data. Finally, the sink defines what to do with the output or where to store the results. A sink can be a console log, another output file, or a Kafka topic. It's similar to a database query that reads data from a table, performs a query on it, and then displays the results. 

In Flink SQL semantics, source and sink will be tables, but Flink isn’t a storage engine hence it cannot store the data. So Flink tables need to backed up with a [storage connector]({{ site.baseurl }}/dev/table/connect.html) like [file system]({{ site.baseurl }}/dev/table/connect.html#file-system-connector), [Kafka]({{ site.baseurl }}/dev/table/connect.html#kafka-connector) or [MySQL]({{ site.baseurl }}/dev/table/connect.html#jdbc-connector). While creating these tables, storage connector type, [format]({{ site.baseurl }}/dev/table/connect.html#table-formats) and schema for each table needs to be defined. 

 
### Input Source Tables
SQL API environment is configured via [YAML](https://yaml.org) configuration files. When the SQL client starts, it reads the default configuration from the `/conf/sql-client-defaults.yaml` but it can be overriden by user defined configuration file. These files are used to define different environment variables including table source, sinks, [catalogs]({{ site.baseurl }}/dev/table/catalogs.html), [user-defined functions]({{ site.baseurl }}/dev/table/functions/udfs.html).

Tables can be defined through the SQL client or using environment config file. The SQL client support [SQL DDL commands]({{ site.baseurl }}/dev/table/sql) similar to traditional SQL. Standard SQL DDL is used to [create]({{ site.baseurl }}/dev/table/sql/create.html), [alter]({{ site.baseurl }}/dev/table/sql/alter.html), [drop]({{ site.baseurl }}/dev/table/sql/drop.html) tables. 

Flink has a support for few [formats]({{ site.baseurl }}/dev/table/connectors/formats/) that can be used with tables. Following is an example to define a source table on [csv file]({{ site.baseurl }}/dev/table/connectors/formats/csv.html) using DDL with `EmpId, EmpName, DeptId` as header.

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

Following is an example to define a source table using file system with [csv format]({{ site.baseurl }}/dev/table/connectors/formats/csv.html) in the environment config file but Flink community has added the support for quite a few [formats]({{ site.baseurl }}/dev/table/connectors/formats/). 

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



Now if all employees who belong to a certain department needs to be filtered, then where by clause will be used to filter records on DeptId as follows

{% highlight sql %}
SELECT * from EmployeeTableSource WHERE DeptId = 1;
{% endhighlight %} 

### Output Tables
The SQL client provides us the output in real-time, but it’s view only. If results need to be stored, a sink or output table where the results of the query will be stored should be defined. Similar to input source tables, sink tables are defined in the environment file or using SQL DDL statements. See the [connection page]({{ site.baseurl }}/dev/table/connect.html) for more information about supported external systems and their configuration.  

---------------

{% top %}

## Execution modes 
Once source tables are defined, It's as simple as writing SQL queries in any of the other databases. Most of the traditional database systems execute the queries against all the stored data in the database and return a final result. The query input is finite and data at rest is being processed which is referred to as batch processing. Hence the result is final and finite. 

On the other hand, Flink can process data in motion or a continuous stream of data, known as stream processing. In the case of stream processing, query input is unbounded so the result is never final and continuously updated. Having said that, SQL API has different [execution mode](TODO.com) for both batch and streaming processing. However, the semantics of the query is the same, irrespective of whether it is executed one-time on table snapshot or continuously on a changing table. 

Most of the standard SQL operations are available in both batch and streaming mode. Streaming mode has few additional operators which are specific to continuous data processing such as [Temporal tables]({{ site.baseurl }}/dev/table/streaming/temporal_tables.html), [continuous joins queries]({{ site.baseurl }}/dev/table/streaming/joins.html) and [Pattern recognition]({{ site.baseurl }}/dev/table/streaming/match_recognize.html).
<!-- #### Processing mode (append, changelog, ..)-->

### SQL Client result modes
The CLI supports **three modes** for maintaining and visualizing results.

The **table mode** materializes results in memory and visualizes them in a regular, paginated table representation. It can be enabled by executing the following command in the CLI:

{% highlight text %}
SET execution.result-mode=table;
{% endhighlight %}

The **changelog mode** does not materialize results and visualizes the result stream that is produced by a [continuous query](streaming/dynamic_tables.html#continuous-queries) consisting of insertions (`+`) and retractions (`-`).

{% highlight text %}
SET execution.result-mode=changelog;
{% endhighlight %}

The **tableau mode** is more like a traditional way which will display the results on the screen directly with a tableau format.
The displaying content will be influenced by the query execution type(`execution.type`).

{% highlight text %}
SET execution.result-mode=tableau;
{% endhighlight %}

Note that when using **tableau mode** with the streaming query, the result will be continuously printed on the console. If the input data of 
this query is bounded, the job will terminate after Flink processed all input data, and the printing will also be stopped automatically.
Otherwise, if you want to terminate a running query, just type `CTRL-C` in this case, the job and the printing will be stopped.

## Dynamic tables and continuous queries
Standard SQL has not been designed with streaming data in mind. As a consequence, there are few conceptual gaps between standard SQL and stream processing. SQL is designed for finite or data-at-rest processing. SQL queries are completed after processing the fixed size of the input data. On the contrary, streaming queries are designed for an infinite amount of data. Therefore, they continue execution forever as queries do not have access to all the data. 

A [continuous query](/table/streaming/dynamic_tables.html#continuous-queries) never terminates and produces a dynamic table as result. [Dynamic tables](/table/streaming/dynamic_tables.html) are the core concept of Flink’s Table API and SQL support for streaming data. 

Aggregations on the continuous stream need to store aggregated results continuously during the execution of the query. For example, if the number of employees needs to be counted for each department from the data stream, the aggregated count needs to be stored for each department during the execution of the query. 

 {% highlight sql %}
 SELECT count(1) as EmpCount, DeptId from EmployeeTableSource GROUP BY DeptId;
 {% endhighlight %} 

Such queries or operators where the state needs to store are referred to as materialized operators. A materialized view caches the result of the query such that the query does not need to be evaluated when the view is accessed. During the execution of the query, the state can grow with every new record processed. 

## Detached mode
So far, We have seen the query execution using the SQL client but does that mean that the SQL client needs to be running all the time? the answer is no. SQL `INSERT INTO` can be used to submit detached queries to Flink cluster. 

 {% highlight sql %}
 INSERT INTO MyTableSink SELECT * FROM MyTableSource
 {% endhighlight %} 
 
Once submitted such a query, can only be controlled from Flink cluster.  It will run and store the results into the sink table directly instead of loading the results into the system memory. 

---------------

{% top %}