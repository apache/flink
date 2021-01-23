---
title: "Table API Tutorial"
nav-parent_id: python
nav-pos: 20
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

Apache Flink offers a Table API as a unified, relational API for batch and stream processing, i.e., queries are executed with the same semantics on unbounded, real-time streams or bounded, batch data sets and produce the same results. The Table API in Flink is commonly used to ease the definition of data analytics, data pipelining, and ETL applications.

* This will be replaced by the TOC
{:toc}

## What Will You Be Building? 

In this tutorial, you will learn how to build a pure Python Flink Table API project.
The pipeline will read data from an input csv file and write the results to an output csv file.

## Prerequisites

This walkthrough assumes that you have some familiarity with Python, but you should be able to follow along even if you come from a different programming language.
It also assumes that you are familiar with basic relational concepts such as `SELECT` and `GROUP BY` clauses.

## Help, I’m Stuck! 

If you get stuck, check out the [community support resources](https://flink.apache.org/community.html).
In particular, Apache Flink's [user mailing list](https://flink.apache.org/community.html#mailing-lists) consistently ranks as one of the most active of any Apache project and a great way to get help quickly. 

## How To Follow Along

If you want to follow along, you will require a computer with: 

* Java 8 or 11
* Python 3.5, 3.6 or 3.7

Using Python Table API requires installing PyFlink, which is available on [PyPI](https://pypi.org/project/apache-flink/) and can be easily installed using `pip`. 

{% highlight bash %}
$ python -m pip install apache-flink
{% endhighlight %}

Once PyFlink is installed, you can move on to write a Python Table API job.

## Writing a Flink Python Table API Program

Table API applications begin by declaring a table environment; either a `BatchTableEvironment` for batch applications or `StreamTableEnvironment` for streaming applications.
This serves as the main entry point for interacting with the Flink runtime.
It can be used for setting execution parameters such as restart strategy, default parallelism, etc.
The table config allows setting Table API specific configurations.

{% highlight python %}
exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
t_config = TableConfig()
t_env = BatchTableEnvironment.create(exec_env, t_config)
{% endhighlight %}

The table environment created, you can declare source and sink tables.

{% highlight python %}
t_env.connect(FileSystem().path('/tmp/input')) \
    .with_format(OldCsv()
                 .field('word', DataTypes.STRING())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())) \
    .create_temporary_table('mySource')

t_env.connect(FileSystem().path('/tmp/output')) \
    .with_format(OldCsv()
                 .field_delimiter('\t')
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .create_temporary_table('mySink')
{% endhighlight %}
You can also use the TableEnvironment.sql_update() method to register a source/sink table defined in DDL:
{% highlight python %}
my_source_ddl = """
    create table mySource (
        word VARCHAR
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '/tmp/input'
    )
"""

my_sink_ddl = """
    create table mySink (
        word VARCHAR,
        `count` BIGINT
    ) with (
        'connector' = 'filesystem',
        'format' = 'csv',
        'path' = '/tmp/output'
    )
"""

t_env.sql_update(my_source_ddl)
t_env.sql_update(my_sink_ddl)
{% endhighlight %}
This registers a table named `mySource` and a table named `mySink` in the execution environment.
The table `mySource` has only one column, word, and it consumes strings read from file `/tmp/input`.
The table `mySink` has two columns, word and count, and writes data to the file `/tmp/output`, with `\t` as the field delimiter.

You can now create a job which reads input from table `mySource`, preforms some transformations, and writes the results to table `mySink`.

Finally you must execute the actual Flink Python Table API job.
All operations, such as creating sources, transformations and sinks are lazy.
Only when `execute_insert(sink_name)` is called, the job will be submitted for execution.

{% highlight python %}
from pyflink.table.expressions import lit

tab = t_env.from_path('mySource')
tab.group_by(tab.word) \
   .select(tab.word, lit(1).count) \
   .execute_insert('mySink').wait()
{% endhighlight %}

The complete code so far:

{% highlight python %}
from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, FileSystem
from pyflink.table.expressions import lit

exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
t_config = TableConfig()
t_env = BatchTableEnvironment.create(exec_env, t_config)

t_env.connect(FileSystem().path('/tmp/input')) \
    .with_format(OldCsv()
                 .field('word', DataTypes.STRING())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())) \
    .create_temporary_table('mySource')

t_env.connect(FileSystem().path('/tmp/output')) \
    .with_format(OldCsv()
                 .field_delimiter('\t')
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .create_temporary_table('mySink')

tab = t_env.from_path('mySource')
tab.group_by(tab.word) \
   .select(tab.word, lit(1).count) \
   .execute_insert('mySink').wait()
{% endhighlight %}

## Executing a Flink Python Table API Program
Firstly, you need to prepare input data in the "/tmp/input" file. You can choose the following command line to prepare the input data:

{% highlight bash %}
$ echo -e  "flink\npyflink\nflink" > /tmp/input
{% endhighlight %}

Next, you can run this example on the command line (Note: if the result file "/tmp/output" has already existed, you need to remove the file before running the example):

{% highlight bash %}
$ python WordCount.py
{% endhighlight %}

The command builds and runs the Python Table API program in a local mini cluster.
You can also submit the Python Table API program to a remote cluster, you can refer
[Job Submission Examples]({% link deployment/cli.md %}#submitting-pyflink-jobs)
for more details.

Finally, you can see the execution result on the command line:

{% highlight bash %}
$ cat /tmp/output
flink	2
pyflink	1
{% endhighlight %}

This should get you started with writing your own Flink Python Table API programs.
To learn more about the Python Table API, you can refer
[Flink Python API Docs]({{ site.pythondocs_baseurl }}/api/python) for more details.
