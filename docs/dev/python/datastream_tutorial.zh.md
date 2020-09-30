---
title: "DataStream API 教程"
nav-parent_id: python
nav-pos: 21
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

Apache Flink 提供了 DataStream API，用于构建健壮的、有状态的流式应用程序。它提供了对状态和时间细粒度控制，从而允许实现高级事件驱动系统。
在这篇教程中，你将学习如何使用 PyFlink 和 DataStream API 构建一个简单的流式应用程序。

* This will be replaced by the TOC
{:toc}

## 你要搭建一个什么系统

在本教程中，你将学习如何编写一个简单的 Python DataStream 作业。
例子是从非空集合中读取数据，并将结果写入本地文件系统。

## 准备条件

本教程假设你对 Python 有一定的熟悉，但是即使你使用的是不同编程语言，你也应该能够学会。

## 困难求助

如果你有疑惑，可以查阅 [社区支持资源](https://flink.apache.org/zh/community.html)。
特别是，Apache Flink [用户邮件列表](https://flink.apache.org/zh/community.html#mailing-lists) 一直被评为所有Apache项目中最活跃的一个，也是快速获得帮助的好方法。

## 怎样跟着教程练习

首先，你需要在你的电脑上准备以下环境：

* Java 8 or 11
* Python 3.5, 3.6 or 3.7

使用 Python DataStream API 需要安装 PyFlink，PyFlink 发布在 [PyPI](https://pypi.org/project/apache-flink/)上，可以通过 `pip` 快速安装。 

{% highlight bash %}
$ python -m pip install apache-flink
{% endhighlight %}

一旦 PyFlink 安装完成之后，你就可以开始编写 Python DataStream 作业了。

## 编写一个 Flink Python DataStream API 程序

DataStream API 应用程序首先需要声明一个执行环境（`StreamExecutionEnvironment`），这是流式程序执行的上下文。你将通过它来设置作业的属性（例如默认并发度、重启策略等）、创建源、并最终触发作业的执行。

{% highlight python %}
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
{% endhighlight %}

一旦创建了 `StreamExecutionEnvironment` 之后，你可以使用它来声明数据源。数据源从外部系统（如 Apache Kafka、Rabbit MQ 或 Apache Pulsar）拉取数据到 Flink 作业里。

为了简单起见，本教程使用元素集合作为数据源。

{% highlight python %}
ds = env.from_collection(
    collection=[(1, 'aaa'), (2, 'bbb')],
    type_info=Types.ROW([Types.INT(), Types.STRING()]))
{% endhighlight %}

这里从相同类型数据集合中创建数据流（一个带有 INT 和 STRING 类型字段的 `ROW` 类型）。

你现在可以在这个数据流上执行转换操作，或者使用 _sink_ 将数据写入外部系统。本教程使用 `StreamingFileSink` 将数据写入 `/tmp/output` 文件目录中。

{% highlight python %}
ds.add_sink(StreamingFileSink
    .for_row_format('/tmp/output', SimpleStringEncoder())
    .build())
{% endhighlight %}

最后一步是执行真实的 PyFlink DataStream API 作业。PyFlink applications 是懒加载的，并且只有在完全构建之后才会提交给集群上执行。要执行一个应用程序，你只需简单地调用 `env.execute(job_name)`。

{% highlight python %}
env.execute("tutorial_job")
{% endhighlight %}

完整的代码如下:

{% highlight python %}
from pyflink.common.serialization import SimpleStringEncoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import StreamingFileSink


def tutorial():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    ds = env.from_collection(
        collection=[(1, 'aaa'), (2, 'bbb')],
        type_info=Types.ROW([Types.INT(), Types.STRING()]))
    ds.add_sink(StreamingFileSink
                .for_row_format('/tmp/output', SimpleStringEncoder())
                .build())
    env.execute("tutorial_job")


if __name__ == '__main__':
    tutorial()
{% endhighlight %}

## 执行一个 Flink Python DataStream API 程序

现在你已经编写好 PyFlink 程序，可以运行它了！首先，需要确保输出目录不存在:

{% highlight bash %}
rm -rf /tmp/output
{% endhighlight %}

接下来，可以使用如下命令运行刚刚创建的示例:

{% highlight bash %}
$ python datastream_tutorial.py
{% endhighlight %}

这个命令会在本地集群中构建并运行 PyFlink 程序。你也可以使用 [Job Submission Examples]({{ site.baseurl }}/zh/ops/cli.html#job-submission-examples) 中描述的命令将其提交到远程集群。

最后，你可以在命令行上看到执行结果:

{% highlight bash %}
$ find /tmp/output -type f -exec cat {} \;
1,aaa
2,bbb
{% endhighlight %}

本教程为你开始编写自己的 PyFlink DataStream API 程序提供了基础。如果需要了解更多关于 Python DataStream API 的使用，请查阅 [Flink Python API Docs]({{ site.pythondocs_baseurl }}/api/python)。
