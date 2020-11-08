---
title: "Getting Started - Flink SQL"
nav-parent_id: sql
nav-pos: 3
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
Getting Started
---------------

* This will be replaced by the TOC
{:toc}

Flink SQL enables SQL developers to design and develop the batch or streaming application without writing the Java, Scala or any other programming language code. It provides a unified API for both batch and streaming APIs. As a user you can perform powerful transformations. Flink’s SQL support is based on [Apache Calcite](https://calcite.apache.org/) which implements the SQL standard.
In addition to the SQL API, Flink also has Table API as well with similar semantics as SQL. Table API is a Language integrated API, where we use a specific programming language to write the queries or call the API. For example, we create the table environment, get a table object and apply different methods which return table api objects. It supports different languages e.g. Java, Scala, Python. 
 
Flink SQL and Table API are just two ways to write queries which use the same API underneath. It wouldn’t be wrong if we say Table API is a wrapper on top of the streaming API. SQL API is more descriptive way of writing queries using well-known SQL standards where we usually have ‘select * from Table’ pattern.  Table API query starts with from clause, followed by joins and where clause, and then finally projection or select at the last. SQL API is easy to learn and almost everyone knows it already. All the queries are optimized for efficient execution. It has unified syntax for both batch and streaming data.

We will focus on the FLink SQL API, while you can read more about Table API [here]({{ site.baseurl }}/dev/table/).

### Pre-requisites
You only need to have basic knowledge of SQL. You will not need to write Java or Scala code or use an IDE.

### Installation
There are various ways to install the Flink. You can download the source code and compile it, run it inside the container, or the easiest one is to download the binaries and run it locally. We assume local installation for the rest of the tutorial. You can start a local cluster using the following command
 
{% highlight bash %}
./bin/start-cluster.sh
{% endhighlight %}
 
 
### SQL Client
SQL Client is an interactive client to submit SQL queries to Flink and visualize the results. It’s like a query editor for any other database management system where you can write queries using standard SQL. You can start the SQL client from the binary folder as follows

 {% highlight bash %}
./bin/sql-client.sh embedded
 {% endhighlight %}

 
This will start the client in embedded mode. In the future, a user will have two possibilities of starting the SQL Client CLI either by starting an embedded standalone process or by connecting to a remote SQL Client Gateway. At the moment only the embedded mode is supported. 

### Hello World query
 
Once SQL client is started, it's a query editor for writing queries. you can print hello world using the following query
 
{% highlight sql %}
SELECT 'Hello World';
{% endhighlight %}

Following example will print the current stamp

{% highlight sql %}
SELECT CURRENT_TIMESTAMP;
{% endhighlight %}


`Help;` is used to see different supported DDL (Data definiation language) commands. Furthermore, Flink SQL does support different built-in functions as well. Following query will show all the built-in and user defined functions. 
{% highlight sql %}
SHOW FUNCTIONS;
{% endhighlight %}
 
### Setting up tables
Real-world database queries are run against the SQL tables. Although Flink is a stream processing engine, users can define a table on top of the streaming data. As we know, all the Flink data processing pipeline generally have three components - source, compute, sink. 

Source is our input or from where we read the data e.g. a text file, kafka topic. Then we defined some computations that needed to be performed on those data elements. Finally, sink defines what to do with the output or where to store the results. Sink can be a console log, another output file or kafka topic. It's similar to a database query where we read data from a table, perform a query on it and then display or store the results. 
In Flink SQL semantics, source and sink will be tables, but we know Flink isn’t a storage engine and cannot store the data. So we need to back our table with a [storage connector]({{ site.baseurl }}/dev/table/connect.html) like file system, Kafka or MySQL. When we will be defining these tables, we will configure the storage connector type, format and schema for each table. 

 
#### Input tables with schema
SQL API environment is configured using the YAML configuration files. When we start SQL client, it reads the default configuration from the `/conf/sql-client-defaults.yaml` but it can be overriden by user defined configuration file. These files are used to define different environment variables including table source, sinks, catalogs, user-defined functions.
Tables can be defined through the environment config file or using the SQL Client. Environment file will have YAML format while SQL client will use SQL DDL format. 

Following is an example to define a source table using file system with [csv format]({{ site.baseurl }}/dev/table/connectors/formats/csv.html) in the environment config file but community has added the support for quite a few [formats]({{ site.baseurl }}/dev/table/connectors/formats/). 

{% highlight yaml %}
# Define tables here such as sources, sinks, views, or temporal tables.

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

Addittionally, we can use SQL DDL to [create]({{ site.baseurl }}/dev/table/sql/create.html) [alter]({{ site.baseurl }}/dev/table/sql/alter.html), [drop]({{ site.baseurl }}/dev/table/sql/drop.html) tables from the SQL client. Same table can be defined using DDL as follows

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

Now if we want to filter all the employees who belong to a certain department, then we will run a query with where by clause filtering records on DeptId as follows

{% highlight sql %}
SELECT * from EmployeeTableSource  WHERE DeptId = 1;
{% endhighlight %} 

##### Output Tables
SQL client provides us the output in real time, but it’s view only. If we want to store the results we will have to define the sink or output table where the results of the query will be stored. Similar to input source tables, sink tables are defined in the environment file or using SQL DDL statements. See the [connection page]({{ site.baseurl }}/dev/table/connect.html) for more information about supported external systems and their configuration.  

#### Execution modes 
Once you set up the source tables, It's as simple as writing SQL queries in any of the other database. Most of the traditional database execute the queries against all the stored data in the database and return a final result. The query input is finite and data at rest is being processed which is referred as batch processing. Hence the result is final and finite. On the other hand, Flink has the capabilities to process data in motion or continuous stream of data, known as stream processing. In the case of stream processing, query input is unbounded so result is never final and continuously updated. Having said that, SQL API has different [execution mode](TODO.com) for both batch and streaming processing. However, the semantics of the query are same, irrespective whether it is executed one-time on table snapshot or continuously on a changing table. 

Most of the standard SQL operations are available in both batch and streaming mode. Streaming mode has few additional operators which are specific to continuous data processing such as [Window aggregation], [time-based joins] and [Pattern recognition].


<! -- #### Processing mode (append, changelog, ..)-->

### Dynamic tables and continuous queries
Standard SQL has been designed for finite and data at rest processing. All the queries we have executed so far used filter or projections on the input source. If we have to do the aggregations on the continuous stream of the data, we need to store the aggregated results during the execution of the query. 

For example, if we want to count the number of employees for each department when processing a huge amount of data, we need to store the aggregated count for each department during the execution of the query.    
 {% highlight sql %}
 SELECT count(1) as EmpCount, DeptId from EmployeeTableSource GROUP BY DeptId;
 {% endhighlight %} 
 
 Such queries or operators where we need to store the state are referred as materialized operators. During the execution of the query, state can grow with every new record processed.   
 

### Pattern matching
### Temporal tables
### Detached mode
So far, We have seen that we are executing queries using the SQL client but does that mean we need to have SQL client running all the time?, the answer is no. SQL INSERT INTO can be used to submit detached queries to the Flink cluster. 

 {% highlight sql %}
 INSERT INTO MyTableSink SELECT * FROM MyTableSource
 {% endhighlight %} 

 
Once submitted such a query,it can only be controlled from the Flink cluster.  It will run and store the results into the sink table directly instead of loading the results into the system memory. 

---------------

{% top %}
