---
title: "Python API Tutorial"
nav-title: Python API
nav-parent_id: apitutorials
nav-pos: 10
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

In this guide we will start from scratch and go from setting up a Flink Python project
to running a Python Table API program.

## Setting up a Python Project

Firstly, you can fire up your favorite IDE and create a Python project and then
you need to install the PyFlink package. Please
see [Build PyFlink]({{ site.baseurl }}/flinkDev/building.html#build-pyflink)
for more details about this.

## Writing a Flink Python Table API Program

The first step in a Flink Python Table API program is to create a `BatchTableEnvironment`
(or `StreamTableEnvironment` if you are writing a streaming job). It is the main entry point
for Python Table API jobs.

{% highlight python %}
exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
t_config = TableConfig()
t_env = BatchTableEnvironment.create(exec_env, t_config)
{% endhighlight %}

The `ExecutionEnvironment` (or `StreamExecutionEnvironment` if you are writing a streaming job)
can be used to set execution parameters, such as the restart strategy, default parallelism, etc.

The `TableConfig` can be used by setting the parameters such as the built-in catalog name, the
threshold where generating code, etc.

Next we will create a source table and a sink table.

{% highlight python %}
t_env.connect(FileSystem().path('/tmp/input')) \
    .with_format(OldCsv()
                 .line_delimiter(' ')
                 .field('word', DataTypes.STRING())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())) \
    .register_table_source('mySource')

t_env.connect(FileSystem().path('/tmp/output')) \
    .with_format(OldCsv()
                 .field_delimiter('\t')
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .register_table_sink('mySink')
{% endhighlight %}

This registers a table named `mySource` and a table named `mySink` in the
`ExecutionEnvironment`. The table `mySource` has only one column: word.
It represents the words read from file `/tmp/input`. The table `mySink` has two columns:
word and count. It writes data to file `/tmp/output`, with `\t` as the field delimiter.

Then we need to create a job which reads input from table `mySource`, preforms some
operations and writes the results to table `mySink`.

{% highlight python %}
t_env.scan('mySource') \
    .group_by('word') \
    .select('word, count(1)') \
    .insert_into('mySink')
{% endhighlight %}

The last thing is to start the actual Flink Python Table API job. All operations, such as
creating sources, transformations and sinks only build up a graph of internal operations.
Only when `t_env.execute(job_name)` is called, this graph of operations will be thrown on a cluster or
executed on your local machine.

{% highlight python %}
t_env.execute("tutorial_job")
{% endhighlight %}

The complete code so far is as follows:

{% highlight python %}
from pyflink.dataset import ExecutionEnvironment
from pyflink.table import TableConfig, DataTypes, BatchTableEnvironment
from pyflink.table.descriptors import Schema, OldCsv, FileSystem

exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
t_config = TableConfig()
t_env = BatchTableEnvironment.create(exec_env, t_config)

t_env.connect(FileSystem().path('/tmp/input')) \
    .with_format(OldCsv()
                 .line_delimiter(' ')
                 .field('word', DataTypes.STRING())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())) \
    .register_table_source('mySource')

t_env.connect(FileSystem().path('/tmp/output')) \
    .with_format(OldCsv()
                 .field_delimiter('\t')
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .with_schema(Schema()
                 .field('word', DataTypes.STRING())
                 .field('count', DataTypes.BIGINT())) \
    .register_table_sink('mySink')

t_env.scan('mySource') \
    .group_by('word') \
    .select('word, count(1)') \
    .insert_into('mySink')

t_env.execute("tutorial_job")
{% endhighlight %}

## Executing a Flink Python Table API Program

You can run this example in your IDE or on the command line (suppose the job script file is
WordCount.py):

{% highlight bash %}
$ python WordCount.py
{% endhighlight %}

The command builds and runs the Python Table API program in a local mini cluster.
You can also submit the Python Table API program to a remote cluster, you can refer
[Job Submission Examples]({{ site.baseurl }}/ops/cli.html#job-submission-examples)
for more details.

This should get you started with writing your own Flink Python Table API programs.
To learn more about the Python Table API, you can refer
[Flink Python Table API Docs]({{ site.pythondocs_baseurl }}/api/python) for more details.
