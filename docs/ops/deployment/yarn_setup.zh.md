---
title:  "YARN 设置"
nav-title: YARN
nav-parent_id: deployment
nav-pos: 4
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

## 快速入门

### 在YARN上启动一个持久运行的Flink集群

启动一个YARN会话，其中job manager分配1GB的堆空间，task managers分配4GB：

{% highlight bash %}
# 如果 HADOOP_CLASSPATH 没有设置:
#   export HADOOP_CLASSPATH=`hadoop classpath`
./bin/yarn-session.sh -jm 1024m -tm 4096m
{% endhighlight %}

需要注意`-s`标志是设定每个Task Manager处理槽数量。我们建议把这个值设为每台机器的处理器数量。

一旦会话被启动，你就可以使用 `./bin/flink` 工具把job提交到集群当中。

### 在YARN上运行Flink作业

{% highlight bash %}
# 如果 HADOOP_CLASSPATH 没有设置:
#   export HADOOP_CLASSPATH=`hadoop classpath`
./bin/flink run -m yarn-cluster -p 4 -yjm 1024m -ytm 4096m ./examples/batch/WordCount.jar
{% endhighlight %}

## Flink YARN 会话

Apache [Hadoop YARN](http://hadoop.apache.org/) 是一个集群资源管理框架，它可使你在集群之上运行各种各样的分布式应用，Flink也是其中之一。在YARN部署完备的情况下，用户无需另外安装或者设置其他事项。

**需求**

- Apache Hadoop 至少为 2.4.1 版本
- HDFS (Hadoop 分布式文件系统) (或者其他Hadoop支持的分布式文件系统)

### 开启 Flink 会话

遵循一下指引来学习如何在你的YARN集群中启动一个Flink会话。

此会话会启动所有需要的Flink服务（ JobManager 和 TaskManagers ）以便于你提交程序到集群。记得在每个会话中你可以提交多个程序。

#### 下载 Flink

从[download page]({{ site.download_url }})下载包含必要文件的Flink软件包。

用以下命令解压：

{% highlight bash %}
tar xvzf flink-{{ site.version }}-bin-scala*.tgz
cd flink-{{site.version }}/
{% endhighlight %}


#### 启动会话

使用以下命令启动会话

{% highlight bash %}
./bin/yarn-session.sh
{% endhighlight %}

此命令会展示以下概述：

{% highlight bash %}
Usage:
   Optional
     -D <arg>                        Dynamic properties
     -d,--detached                   Start detached
     -jm,--jobManagerMemory <arg>    Memory for JobManager Container with optional unit (default: MB)
     -nm,--name                      Set a custom name for the application on YARN
     -at,--applicationType           Set a custom application type on YARN
     -q,--query                      Display available YARN resources (memory, cores)
     -qu,--queue <arg>               Specify YARN queue.
     -s,--slots <arg>                Number of slots per TaskManager
     -tm,--taskManagerMemory <arg>   Memory per TaskManager Container with optional unit (default: MB)
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths for HA mode
{% endhighlight %}

请注意客户端需要设置 `YARN_CONF_DIR` 或者 `HADOOP_CONF_DIR` 环境变量来读取YARN或者HDFS配置。

**例子:** 使用以下命令来启动YARN会话集群，其中每个task manager使用8G的内存和32个处理槽位：

{% highlight bash %}
./bin/yarn-session.sh -tm 8192 -s 32
{% endhighlight %}

该系统会使用`conf/flink-conf.yaml`中的配置。当你需要修改的时候请遵循我们的[配置指引]({{ site.baseurl }}/ops/config.html)。

基于YARN的Flink会覆盖以下配置参数`jobmanager.rpc.address`（因为JobManager总会被分配到不同的机器上）、`io.tmp.dirs`（我们使用YARN提供的临时目录）和`parallelism.default`（如果槽位数量被特别设置了）

如果你不想通过改变配置文件来设置参数，你还可以选择通过`-D`标志位来传递动态属性，例如这种方式`-Dfs.overwrite-files=true -Dtaskmanager.memory.network.min=536346624`。

该例子调用启动了一个单独的容器来提供给运行Job Manager的ApplicationMaster。

当作业提交到集群时，此会话集群会自动分配额外的容器来运行TaskManagers。

当Flink部署到你的YARN集群上，他会向你展示JobManager的连接详情。

通过停止unix进程（用CTRL+C）或在客户端中输入'stop'的方式来停止YARN会话。

基于YARN的Flink只会在集群能给ApplicationMaster提供足够的运行资源情况下启动。大多数YARN调度器会说明所需的容器内存，有些也包括虚拟核心的数量。默认情况下，虚拟核心的数量等于处理槽位参数（`-s`），而[`yarn.containers.vcores`]({{ site.baseurl }}/ops/config.html#yarn-containers-vcores)参数可以覆盖该值。要改参数生效，你需要在集群中开启CPU调度。

#### 分离式YARN会话

如果你不想一直保持Flink YARN客户端的运行，你可以启动一个*分离式*YARN会话。
调用参数为`-d`或者`--detached`。

在这种情况下，Flink YARN客户端只会提交Flink到集群上，然后就关闭了。

为了优雅停止Flink集群，需要使用以下命令：`echo "stop" | ./bin/yarn-session.sh -id <appId>`。

如果没法这样做，你也可以通过YARN的网站界面或者通过他的工具集来杀死Flink：`yarn application -kill <appId>`。
注意，尽管如此，这种方式很有可能不会清理所有作业以及临时文件。

#### 关联到一个已有的会话

使用以下命令来启动一个会话

{% highlight bash %}
./bin/yarn-session.sh
{% endhighlight %}

这条命令会向你展示以下信息：

{% highlight bash %}
Usage:
   Required
     -id,--applicationId <yarnAppId> YARN application Id
{% endhighlight %}

如上文所述，为了读取YARN和HDFS的配置，`YARN_CONF_DIR`或者`HADOOP_CONF_DIR`环境变量必须预先设置。

**例如：**使用以下命令来关联到正在运行的YARN会话`application_1463870264508_0029`：

{% highlight bash %}
./bin/yarn-session.sh -id application_1463870264508_0029
{% endhighlight %}

关联到正在运行的YARN会话会使用YARN的ResourceManager来确定Job Manager的RPC端口。

通过停止unix进程（用CTRL+C）或在客户端中输入'stop'的方式来停止YARN会话。

### 提交作业到Flink

使用以下命令来提交一个Flink程序到YARN集群：

{% highlight bash %}
./bin/flink
{% endhighlight %}

请查阅[命令行客户端]({{ site.baseurl }}/ops/cli.html)文档

此命令将会向你展示如下帮助菜单：

{% highlight bash %}
[...]
Action "run" compiles and runs a program.

  Syntax: run [OPTIONS] <jar-file> <arguments>
  "run" action arguments:
     -c,--class <classname>           Class with the program entry point ("main"
                                      method or "getPlan()" method. Only needed
                                      if the JAR file does not specify the class
                                      in its manifest.
     -m,--jobmanager <host:port>      Address of the JobManager to
                                      which to connect. Use this flag to connect
                                      to a different JobManager than the one
                                      specified in the configuration.
     -p,--parallelism <parallelism>   The parallelism with which to run the
                                      program. Optional flag to override the
                                      default value specified in the
                                      configuration
{% endhighlight %}

使用*run*操作来提交作业到YARN。该客户端可以确定JobManager的地址。在极少数情况下，你也可以使用`-m`标志位来制定JobManager的地址和。JobManager的地址可以在YARN控制台中看到。

**例子**

{% highlight bash %}
wget -O LICENSE-2.0.txt http://www.apache.org/licenses/LICENSE-2.0.txt
hadoop fs -copyFromLocal LICENSE-2.0.txt hdfs:/// ...
./bin/flink run ./examples/batch/WordCount.jar \
       --input hdfs:///..../LICENSE-2.0.txt --output hdfs:///.../wordcount-result.txt
{% endhighlight %}

如果出现任何以下错误，请确保所有的TaskManagers已被启动：

{% highlight bash %}
Exception in thread "main" org.apache.flink.compiler.CompilerException:
    Available instances could not be determined from job manager: Connection timed out.
{% endhighlight %}

你可以在JobManager的网站界面上检查TaskManager的数量。网页地址会在YARN会话控制台中打印。

如果TaskManager在一分钟后没有展示，你应该检查日志文件中的问题。

## 在YARN上运行单独的Flink作业

上述文档是描述如何在Hadoop YARN环境中启动Flink集群。我们也可以在YARN中启动Flink来执行一个单独作业。

***例子：***

{% highlight bash %}
./bin/flink run -m yarn-cluster ./examples/batch/WordCount.jar
{% endhighlight %}

YARN会话命令行中选项同样适用与`./bin/flink`工具。他们会以`y`或者`yarn`（作为长参数选项）作为前缀。

注意：你可以通过设置环境变量`FLINK_CONF_DIR`来使用不同的配置目录。从Flink发行版中拷贝`conf`文件夹并修改编辑来使用，例如针对每个作业的日志设置。

注意：也可以通过`-m yarn-cluster`和一个分离式的YARN提交(`-yd`)的组合来"启动并遗忘"一个Flink作业到YARN集群。在此情况下，你的应用将不会从ExecutionEnvironment.execute()调用中接收到任何进一步的结果或者异常。

### 用户的jar包 和 Classpath

默认情况下，Flink在启动单独的作业时，会引入用户在系统classpath中的jar包，此操作由`yarn.per-job-cluster.include-user-jar`参数控制。

当它被设置成`DISABLED`时，Flink会转而引入用户classpath的jar包。

用户自定义jar包在classpath中的位置由以下任一参数控制：

- `ORDER`:（默认）基于字典序添加jar包到系统classpath。
- `FIRST`: 把jar包添加到系统classpath的开头。
- `LAST`: 把jar包添加到系统classpath的末尾。

## 在应用模式中运行应用

你可以通过输入以下命令在[应用模式]({% link ops/deployment/index.zh.md %}#deployment-modes)下启动应用：

{% highlight bash %}
./bin/flink run-application -t yarn-application ./examples/batch/WordCount.jar
{% endhighlight %}

<div class="alert alert-info" markdown="span">
  <strong>注意：</strong> 除了`-t`，所有其他配置参数，例如用于引导应用状态的保存点路径、应用并行度或者作业管理器/任务管理器所需的内存大小，都可以通过前缀为`-D`配置选项指定。
</div>

例如, 可以通过如下命令指定JobManager和TaskManager的内存大小：

{% highlight bash %}
./bin/flink run-application -t yarn-application -Djobmanager.memory.process.size=2048m -Dtaskmanager.memory.process.size=4096m  ./examples/batch/WordCount.jar
{% endhighlight %}

你可以在[这里]({% link ops/config.zh.md %})查看可用的配置选项。要开启应用模式的所有功能，可以预先上传你应用的jar包到所有集群都能访问的地址，由`yarn.provided.lib.dirs`配置选项设置。如以下命令：

{% highlight bash %}
./bin/flink run-application -t yarn-application \
-Dyarn.provided.lib.dirs="hdfs://myhdfs/my-remote-flink-dist-dir" \
hdfs://myhdfs/jars/my-application.jar
{% endhighlight %}

上述命令会使得作业的提交额外轻量，因为所需的Flink jar包和应用jar包将会从指定的远程地址获取，而不是从客户端传输到集群。

对正在运行的应用的停止、取消或者查询状态都可以通过任一现有方式完成。

## 基于YARN的Flink的恢复操作

Flink的YARN客户端有以下配置参数可以控制如何进行容器故障恢复。这些参数既可以在`conf/flink-conf.yaml`设置，也可以在启动YARN会话时通过`-D`参数配置。

- `yarn.application-attempts`: ApplicationMaster(+ 它的 TaskManager 容器)的故障重试次数。如果设为1（默认值），当AplicationMaster故障时，整个YARN会话将会故障。比1大的值则是说明YARN对ApplicationMaster重启次数。

## YARN上应用优先级的设置

Flink的YARN客户端可以通过以下配置参数来设置应用优先级。这些参数既可以在`conf/flink-conf.yaml`设置，也可以在启动YARN会话时通过`-D`参数配置。

- `yarn.application.priority`: 用一个非负整数来说明Flink YARN应用的优先级。
它只会在YARN优先级调度设置启用的时候起作用。数字越大优先级越高。
如果优先级设为负数或者为'-1'(默认值)，Flink会还原YARN优先级设置，使用集群默认的优先级。
请查阅YARN的官方文档了解特定YARN版本启用优先级调度所需的具体配置。

## 调试一个故障的YARN会话

Flink YARN会话故障的原因很多，如Hadoop启动配置错误（HDFS权限、YARN配置）、版本不兼容（在Cloudera Hadoop上使用vanilla Hadoop依赖来运行Flink）以及其他错误。

### 日志文件

当Flink YARN会话在部署时故障，用户需要依赖于Hadoop YARN的日志功能来检查，其最有用的特性是[YARN日志聚合](http://hortonworks.com/blog/simplifying-user-logs-management-and-access-in-yarn/)
要开启此特性，用户需要在`yarn-site.xml`文件中把`yarn.log-aggregation-enable`属性设置成`true`。
一旦该配置启用，用户可以使用以下命令获取(故障的)YARN会话的所有日志文件。

{% highlight bash %}
yarn logs -applicationId <application ID>
{% endhighlight %}

注意会话结束后需要几秒钟日志才会展示。

### YARN客户端控制台 & 网站界面

如果运行时发生错误（例如经过一段时间后，某个TaskManager停止工作），Flink YARN客户端也会在终端中打印错误日志。

除此以外，还有YARN资源管理网站界面（默认端口为8088）。端口由`yarn.resourcemanager.webapp.address`配置值指定。

可以通过它访问运行中的YARN应用的日志文件，以及展示关于故障应用的诊断。

## 为一个特定的Hadoop版本构建YARN客户端

使用诸如Hortonworks, Cloudera或者MapR公司的Hadoop发行版的用户，可能会需要配合它们特定Hadoop (HDFS)和YARN版本来构建Flink。请查阅[构建指引]({{ site.baseurl }}/flinkDev/building.html)来获取更多详情。

## 在防火墙后运行基于YARN的Flink

有些YARN集群会使用防火墙来管控集群间的网络连接以及其他网络。
在那些设置中，Flink作业只能在（防火墙后的）集群网络中提交到YARN会话。
如果在生产环境使用中不可行，Flink允许配置为他的REST端点配置可供客户端-服务端相互通讯的端口范围。在配置的端口范围内，用户也可以透过防火墙向Flink提交作业。

配置REST端点端口的配置参数如下：

 * `rest.bind-port`

此配置选项接受单个端口（如: "50010"）、端口范围（"50000-50025"）、或者两者兼有（"50010,50011,50020-50025,50050-50075"）。

请确保`rest.port`配置项没有设置，因为他的优先级高于`rest.bind-port`且不接受范围配置。

（Hadoop使用类似的机制，使用`yarn.app.mapreduce.am.job.client.port-range`这个配置参数）。

## 背景 / 内幕

此章节会简要描述Flink和YARN如何交互。

<img src="{{ site.baseurl }}/fig/FlinkOnYarn.svg" class="img-responsive">

YARN客户端需要访问Hadoop配置来连接YARN的资源管理器和HDFS，且采用以下策略来决定Hadoop配置：

* 依次检测`YARN_CONF_DIR`、`HADOOP_CONF_DIR`或者`HADOOP_CONF_PATH`是否被设置。如果其中一个已经设置，就根据它来读取配置。
* 如果上述策略不成功（这不应该出现在正确的YARN启动情况），客户端会使用`HADOOP_HOME`环境变量。如果该变量已设置，客户端会尝试访问`$HADOOP_HOME/etc/hadoop` (Hadoop 2)和`$HADOOP_HOME/conf` (Hadoop 1)。

当启动一个新的Flink YARN会话时，客户端首先会检查请求的资源（ApplicationMaster的内存和虚拟核心）是否可用。然后，它会上传一个包含Flink和HDFS配置的jar包（第1步）。

下一步客户端会请求一个YARN容器（第2步），来启动*ApplicationMaster*（第3步）。当客户端把配置和jar包作为容器中的资源，在此机器上运行的YARN的NodeManager会管理好容器的准备过程（例如下载文件）。一旦完成，*ApplicationMaster* (AM)就会启动。

*JobManager*和AM运行在相同的容器上。一旦它们成功启动，AM就会获悉JobManager的地址（也就是他自己的主机）。他会为TaskManagers生成一个新的Flink配置文件（以使它们可以连接到JobManager）。这个文件也会被上传到HDFS。另外，*AM*容器也提供Flink的网页界面。YARN分配的所有端口都是临时端口，这是的用户可以并行执行多个Flink YARN会话。

然后，AM开始分配容器给Flink的TaskManagers，TaskManagers会从HDFS下载jar包和修改过的配置。当这些步骤完成，Flink就启动完成可以接收作业了。

{% top %}
