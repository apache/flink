---
title: "本地模式安装"
nav-title: '本地模式安装'
nav-parent_id: try-flink
nav-pos: 1
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
 
{% if site.version contains "SNAPSHOT" %}
<p style="border-radius: 5px; padding: 5px" class="bg-danger">
  <b>
  注意：Apache Flink 社区只发布 Apache Flink 发布版本的正式版本。
  </b><br>
  由于你当前正在查看文档的最新快照版本，因此以下所有版本引用都将不起作用。请通过菜单左侧的发布选择器将文档切换到最新发布的版本。
</p>
{% else %}
请按照以下几个步骤下载最新的稳定版本并开始使用。

<a name="step-1-download"></a>

## 步骤 1：下载

为了能够运行 Flink，唯一的要求就是安装有效的 __Java 8 或者 Java 11__。你可以通过发出以下命令来检查 Java 的正确安装。

{% highlight bash %}
java -version
{% endhighlight %}

[下载](https://flink.apache.org/downloads.html) {{ site.version }} 发行版本并解压。

{% highlight bash %}
$ tar -xzf flink-{{ site.version }}-bin-scala{{ site.scala_version_suffix }}.tgz
$ cd flink-{{ site.version }}-bin-scala{{ site.scala_version_suffix }}
{% endhighlight %}

<a name="step-2-start-a-cluster"></a>

## 步骤 2：启动集群

Flink 附带一个 bash 脚本来启动本地集群。

{% highlight bash %}
$ ./bin/start-cluster.sh
Starting cluster.
Starting standalonesession daemon on host.
Starting taskexecutor daemon on host.
{% endhighlight %}

<a name="step-3-submit-a-job"></a>

## 步骤 3：提交作业（Job）

Flink 的发行版本附带了许多示例作业。你可以将这些应用程序之一快速部署到正在运行的集群。

{% highlight bash %}
$ ./bin/flink run examples/streaming/WordCount.jar
$ tail log/flink-*-taskexecutor-*.out
  (to,1)
  (be,1)
  (or,1)
  (not,1)
  (to,2)
  (be,2)
{% endhighlight %}

另外，你可以检查 Flink 的 [Web UI](http://localhost:8080) 来监视集群的状态和正在运行的作业。

<a name="step-4-stop-the-cluster"></a>

## 步骤 4：停止集群

完成后，你可以快速停止集群和所有正在运行的组件。

{% highlight bash %}
$ ./bin/stop-cluster.sh
{% endhighlight %}
{% endif %}
