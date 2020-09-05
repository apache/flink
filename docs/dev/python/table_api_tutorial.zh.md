---
title: "Table API 教程"
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

* This will be replaced by the TOC
{:toc}

Apache Flink 提供 Table API 关系型 API 来统一处理流和批，即查询在无边界的实时流或有边界的批处理数据集上以相同的语义执行，并产生相同的结果。 Flink 的 Table API 易于编写，通常能简化数据分析，数据管道和ETL应用的编码。

## 概要

在该教程中，我们会从零开始，介绍如何创建一个Flink Python项目及运行Python Table API程序。该程序读取一个csv文件，处理后，将结果写到一个结果csv文件中。

## 先决条件

本练习假定您对Python有一定的了解，但是即使您来自其他编程语言，也应该能够继续学习。
它还假定您熟悉基本的关系操作，例如`SELECT`和`GROUP BY`子句。

## 如何寻求帮助

如果您遇到问题，可以访问 [社区信息页面](https://flink.apache.org/zh/community.html)。
与此同时，Apache Flink 的[用户邮件列表](https://flink.apache.org/zh/community.html#mailing-lists) 一直被列为Apache项目中最活跃的项目邮件列表之一，也是快速获得帮助的好方法。

## 继续我们的旅程

如果要继续我们的旅程，您需要一台具有以下功能的计算机：

* Java 8 or 11
* Python 3.5, 3.6 or 3.7

使用Python Table API需要安装PyFlink，它已经被发布到 [PyPi](https://pypi.org/project/apache-flink/)，您可以通过如下方式安装PyFlink：

{% highlight bash %}
$ python -m pip install apache-flink
{% endhighlight %}

安装PyFlink后，您便可以编写Python Table API作业了。

## 编写一个Flink Python Table API程序

编写Flink Python Table API程序的第一步是创建`BatchTableEnvironment`
(或者`StreamTableEnvironment`，如果你要创建一个流式作业)。这是Python Table API作业的入口类。

{% highlight python %}
exec_env = ExecutionEnvironment.get_execution_environment()
exec_env.set_parallelism(1)
t_config = TableConfig()
t_env = BatchTableEnvironment.create(exec_env, t_config)
{% endhighlight %}

`ExecutionEnvironment` (或者`StreamExecutionEnvironment`，如果你要创建一个流式作业)
可以用来设置执行参数，比如重启策略，缺省并发值等。

`TableConfig`可以用来设置缺省的catalog名字，自动生成代码时方法大小的阈值等.

接下来，我们将介绍如何创建源表和结果表。

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
        'connector.type' = 'filesystem',
        'format.type' = 'csv',
        'connector.path' = '/tmp/input'
    )
"""

my_sink_ddl = """
    create table mySink (
        word VARCHAR,
        `count` BIGINT
    ) with (
        'connector.type' = 'filesystem',
        'format.type' = 'csv',
        'connector.path' = '/tmp/output'
    )
"""

t_env.sql_update(my_source_ddl)
t_env.sql_update(my_sink_ddl)
{% endhighlight %}

上面的程序展示了如何创建及在`ExecutionEnvironment`中注册表名分别为`mySource`和`mySink`的表。
其中，源表`mySource`有一列: word，该表代表了从输入文件`/tmp/input`中读取的单词；
结果表`mySink`有两列: word和count，该表会将计算结果输出到文件`/tmp/output`中，字段之间使用`\t`作为分隔符。

接下来，我们介绍如何创建一个作业：该作业读取表`mySource`中的数据，进行一些变换，然后将结果写入表`mySink`。

{% highlight python %}
from pyflink.table.expressions import lit

tab = t_env.from_path('mySource')
tab.group_by(tab.word) \
   .select(tab.word, lit(1).count) \
   .insert_into('mySink')
{% endhighlight %}

最后，需要做的就是启动Flink Python Table API作业。上面所有的操作，比如创建源表
进行变换以及写入结果表的操作都只是构建作业逻辑图，只有当`t_env.execute(job_name)`被调用的时候，
作业才会被真正提交到集群或者本地进行执行。

{% highlight python %}
t_env.execute("python_job")
{% endhighlight %}

该教程的完整代码如下:

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
   .insert_into('mySink')

t_env.execute("python_job")
{% endhighlight %}

## 执行一个Flink Python Table API程序

首先，你需要在文件 “/tmp/input” 中准备好输入数据。你可以选择通过如下命令准备输入数据：

{% highlight bash %}
$ echo -e  "flink\npyflink\nflink" > /tmp/input
{% endhighlight %}

接下来，可以在命令行中运行作业（假设作业名为WordCount.py）（注意：如果输出结果文件“/tmp/output”已经存在，你需要先删除文件，否则程序将无法正确运行起来）：

{% highlight bash %}
$ python WordCount.py
{% endhighlight %}

上述命令会构建Python Table API程序，并在本地mini cluster中运行。如果想将作业提交到远端集群执行，
可以参考[作业提交示例]({{ site.baseurl }}/zh/ops/cli.html#job-submission-examples)。

最后，你可以通过如下命令查看你的运行结果：

{% highlight bash %}
$ cat /tmp/output
flink	2
pyflink	1
{% endhighlight %}

上述教程介绍了如何编写并运行一个Flink Python Table API程序，如果想了解Flink Python Table API
的更多信息，可以参考[Flink Python API文档]({{ site.pythondocs_baseurl }}/api/python)。
