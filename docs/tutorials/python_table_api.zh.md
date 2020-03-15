---
title: "Python API 教程"
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

在该教程中，我们会从零开始，介绍如何创建一个Flink Python项目及运行Python Table API程序。

<span class="label label-info">注意</span> PyFlink的运行需要Python 3.5及以上版本。

执行以下命令以确认当前环境下的指令“python”指向Python 3.5及以上版本：

{% highlight bash %}
$ python --version
# the version printed here must be 3.5+
{% endhighlight %}

## 创建一个Python Table API项目

首先，使用您最熟悉的IDE创建一个Python项目。之后执行命令`python -m pip install apache-flink`从PyPI下载安装PyFlink包。
如果您想从源码安装，请参考[构建PyFlink]({{ site.baseurl }}/zh/flinkDev/building.html#build-pyflink)了解详细信息。

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
                 .line_delimiter(' ')
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

上面的程序展示了如何创建及在`ExecutionEnvironment`中注册表名分别为`mySource`和`mySink`的表。
其中，源表`mySource`有一列: word，该表代表了从输入文件`/tmp/input`中读取的单词；
结果表`mySink`有两列: word和count，该表会将计算结果输出到文件`/tmp/output`中，字段之间使用`\t`作为分隔符。

接下来，我们介绍如何创建一个作业：该作业读取表`mySource`中的数据，进行一些变换，然后将结果写入表`mySink`。

{% highlight python %}
t_env.scan('mySource') \
    .group_by('word') \
    .select('word, count(1)') \
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

t_env.from_path('mySource') \
    .group_by('word') \
    .select('word, count(1)') \
    .insert_into('mySink')

t_env.execute("python_job")
{% endhighlight %}

## 执行一个Flink Python Table API程序

首先，你需要在文件 “/tmp/input” 中准备好输入数据。你可以选择通过如下命令准备输入数据：

{% highlight bash %}
$ echo "flink\npyflink\nflink" > /tmp/input
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
的更多信息，可以参考[Flink Python Table API文档]({{ site.pythondocs_baseurl }}/api/python)。
