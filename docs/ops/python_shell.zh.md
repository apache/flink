---
title: "Python REPL"
nav-parent_id: ops
nav-pos: 7
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

Flink附带了一个集成的交互式Python Shell。
它既能够运行在本地启动的local模式，也能够运行在集群启动的cluster模式下。

为了使用Flink的Python Shell，你只需要在Flink的binary目录下执行:

{% highlight bash %}
bin/pyflink-shell.sh local
{% endhighlight %}

关于如何在一个Cluster集群上运行Python shell，可以参考启动章节介绍。

## 使用

当前Python shell支持Table API的功能。
在启动之后，Table Environment的相关内容将会被自动加载。
可以通过变量"bt_env"来使用BatchTableEnvironment，通过变量"st_env"来使用StreamTableEnvironment。

### Table API

下面是一个通过Python Shell 运行的简单示例:
<div class="codetabs" markdown="1">
<div data-lang="stream" markdown="1">
{% highlight python %}
>>> import tempfile
>>> import os
>>> import shutil
>>> sink_path = tempfile.gettempdir() + '/streaming.csv'
>>> if os.path.exists(sink_path):
...     if os.path.isfile(sink_path):
...         os.remove(sink_path)
...     else:
...         shutil.rmtree(sink_path)
>>> s_env.set_parallelism(1)
>>> t = st_env.from_elements([(1, 'hi', 'hello'), (2, 'hi', 'hello')], ['a', 'b', 'c'])
>>> st_env.connect(FileSystem().path(sink_path))\
...     .with_format(OldCsv()
...         .field_delimiter(',')
...         .field("a", DataTypes.BIGINT())
...         .field("b", DataTypes.STRING())
...         .field("c", DataTypes.STRING()))\
...     .with_schema(Schema()
...         .field("a", DataTypes.BIGINT())
...         .field("b", DataTypes.STRING())
...         .field("c", DataTypes.STRING()))\
...     .register_table_sink("stream_sink")
>>> t.select("a + 1, b, c")\
...     .insert_into("stream_sink")
>>> st_env.execute("stream_job")
>>> # 如果作业运行在local模式, 你可以执行以下代码查看结果:
>>> with open(sink_path, 'r') as f:
...     print(f.read())
{% endhighlight %}
</div>
<div data-lang="batch" markdown="1">
{% highlight python %}
>>> import tempfile
>>> import os
>>> import shutil
>>> sink_path = tempfile.gettempdir() + '/batch.csv'
>>> if os.path.exists(sink_path):
...     if os.path.isfile(sink_path):
...         os.remove(sink_path)
...     else:
...         shutil.rmtree(sink_path)
>>> b_env.set_parallelism(1)
>>> t = bt_env.from_elements([(1, 'hi', 'hello'), (2, 'hi', 'hello')], ['a', 'b', 'c'])
>>> bt_env.connect(FileSystem().path(sink_path))\
...     .with_format(OldCsv()
...         .field_delimiter(',')
...         .field("a", DataTypes.BIGINT())
...         .field("b", DataTypes.STRING())
...         .field("c", DataTypes.STRING()))\
...     .with_schema(Schema()
...         .field("a", DataTypes.BIGINT())
...         .field("b", DataTypes.STRING())
...         .field("c", DataTypes.STRING()))\
...     .register_table_sink("batch_sink")
>>> t.select("a + 1, b, c")\
...     .insert_into("batch_sink")
>>> bt_env.execute("batch_job")
>>> # 如果作业运行在local模式, 你可以执行以下代码查看结果:
>>> with open(sink_path, 'r') as f:
...     print(f.read())
{% endhighlight %}
</div>
</div>

## 启动

查看Python Shell提供的可选参数，可以使用:

{% highlight bash %}
bin/pyflink-shell.sh --help
{% endhighlight %}

### Local

Python Shell运行在local模式下，只需要执行:

{% highlight bash %}
bin/pyflink-shell.sh local
{% endhighlight %}


### Remote

Python Shell运行在一个指定的JobManager上，通过关键字`remote`和对应的JobManager
的地址和端口号来进行指定:

{% highlight bash %}
bin/pyflink-shell.sh remote <hostname> <portnumber>
{% endhighlight %}

### Yarn Python Shell cluster

Python Shell可以运行在YARN集群之上。YARN的container的数量可以通过参数`-n <arg>`进行
指定。Python shell在Yarn上部署一个新的Flink集群，并进行连接。除了指定container数量，你也
可以指定JobManager的内存，YARN应用的名字等参数。
例如，在一个部署了两个TaskManager的Yarn集群上运行Python Shell:

{% highlight bash %}
 bin/pyflink-shell.sh yarn -n 2
{% endhighlight %}

关于所有可选的参数，可以查看本页面底部的完整说明。


### Yarn Session

如果你已经通过Flink Yarn Session部署了一个Flink集群，能够通过以下的命令连接到这个集群:

{% highlight bash %}
 bin/pyflink-shell.sh yarn
{% endhighlight %}


## 完整的参考

{% highlight bash %}
Flink Python Shell
使用: pyflink-shell.sh [local|remote|yarn] [options] <args>...

命令: local [选项]
启动一个部署在local的Flink Python shell
使用:
    -h,--help   查看所有可选的参数
命令: remote [选项] <host> <port>
启动一个部署在remote集群的Flink Python shell
  <host>
        JobManager的主机名
  <port>
        JobManager的端口号

使用:
    -h,--help   查看所有可选的参数

命令: yarn [选项]
启动一个部署在Yarn集群的Flink Python Shell
使用:
     -h,--help                       查看所有可选的参数
     -jm,--jobManagerMemory <arg>    具有可选单元的JobManager
                                     的container的内存（默认值：MB)
     -n,--container <arg>            需要分配的YARN container的
                                     数量 (=TaskManager的数量)            
     -nm,--name <arg>                自定义YARN Application的名字
     -qu,--queue <arg>               指定YARN的queue     
     -s,--slots <arg>                每个TaskManager上slots的数量     
     -tm,--taskManagerMemory <arg>   具有可选单元的每个TaskManager
                                     的container的内存（默认值：MB）
-h | --help
    打印输出使用文档
{% endhighlight %}

{% top %}
