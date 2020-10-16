---
title: "独立集群"
nav-parent_id: deployment
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

本页面提供了关于如何在*静态*（但可能异构）集群上以*完全分布式方式*运行 Flink 的说明。

* This will be replaced by the TOC
{:toc}

<a name="requirements"></a>

## 需求

<a name="software-requirements"></a>

### 软件需求

Flink 运行在所有*类 UNIX 环境*下，例如 **Linux**，**Mac OS X** 和 **Cygwin** （Windows），集群由**一个 master 节点**以及**一个或多个 worker 节点**构成。在配置系统之前，请确保**在每个节点上**安装有以下软件：

- **Java 1.8.x** 或更高版本，
- **ssh** （必须运行 sshd 以执行用于管理 Flink 各组件的脚本）

如果集群不满足软件要求，那么你需要安装/更新这些软件。

使集群中所有节点使用**免密码 SSH** 以及拥有**相同的目录结构**可以让你使用脚本来控制一切。

{% top %}

<a name="java_home-configuration"></a>

### `JAVA_HOME` 配置

Flink 需要 master 和所有 worker 节点设置 `JAVA_HOME` 环境变量，并指向你的 Java 安装目录。

你可以在 `conf/flink-conf.yaml` 文件中通过 `env.java.home` 配置项来设置此变量。

{% top %}

<a name="flink-setup"></a>

## Flink 设置

前往 [下载页面]({{ site.zh_download_url }}) 获取可运行的软件包。

在下载完最新的发布版本后，复制压缩文件到 master 节点并解压：

{% highlight bash %}
tar xzf flink-*.tgz
cd flink-*
{% endhighlight %}

<a name="configuring-flink"></a>

### 配置 Flink

在解压完文件后，你需要编辑 *conf/flink-conf.yaml* 文件来为集群配置 Flink。

设置 `jobmanager.rpc.address` 配置项指向 master 节点。你也应该通过设置 `jobmanager.memory.process.size` 和 `taskmanager.memory.process.size` 配置项来定义 Flink 允许在每个节点上分配的最大内存值。

这些值的单位是 MB。如果一些 worker 节点上有你想分配到 Flink 系统的多余内存，你可以在这些特定节点的 *conf/flink-conf.yaml* 文件中重写 `taskmanager.memory.process.size` 或 `taskmanager.memory.flink.size` 的默认值。

最后，你必须提供集群上会被用作为 worker 节点的所有节点列表，也就是运行 TaskManager 的节点。编辑文件 *conf/workers* 并输入每个 worker 节点的 IP 或主机名。

以下例子展示了三个节点（IP 地址从 _10.0.0.1_ 到 _10.0.0.3_，主机名为 _master_、_worker1_、 _woker2_）的设置，以及配置文件（在所有机器上都需要在相同路径访问）的内容：

<div class="row">
  <div class="col-md-6 text-center">
    <img src="{% link /page/img/quickstart_cluster.png %}" style="width: 60%">
  </div>
<div class="col-md-6">
  <div class="row">
    <p class="lead text-center">
      /path/to/<strong>flink/conf/<br>flink-conf.yaml</strong>
    <pre>jobmanager.rpc.address: 10.0.0.1</pre>
    </p>
  </div>
<div class="row" style="margin-top: 1em;">
  <p class="lead text-center">
    /path/to/<strong>flink/<br>conf/workers</strong>
  <pre>
10.0.0.2
10.0.0.3</pre>
  </p>
</div>
</div>
</div>

Flink 目录必须放在所有 worker 节点的相同目录下。你可以使用共享的 NFS 目录，或将 Flink 目录复制到每个 worker 节点上。

请参考 [配置参数页面]({% link ops/config.zh.md %}) 获取更多细节以及额外的配置项。

特别地，

* 每个 JobManager 的可用内存值（`jobmanager.memory.process.size`），
* 每个 TaskManager 的可用内存值 （`taskmanager.memory.process.size`，并检查 [内存调优指南]({% link ops/memory/mem_tuning.zh.md %}#configure-memory-for-standalone-deployment)），
* 每台机器的可用 CPU 数（`taskmanager.numberOfTaskSlots`），
* 集群中所有 CPU 数（`parallelism.default`）和
* 临时目录（`io.tmp.dirs`）

的值都是非常重要的配置项。

{% top %}

<a name="starting-flink"></a>

### 启动 Flink

下面的脚本在本地节点启动了一个 JobManager 并通过 SSH 连接到 *workers* 文件中所有的 worker 节点，在每个节点上启动 TaskManager。现在你的 Flink 系统已经启动并运行着。可以通过配置的 RPC 端口向本地节点上的 JobManager 提交作业。

假定你在 master 节点并且在 Flink 目录下：

{% highlight bash %}
bin/start-cluster.sh
{% endhighlight %}

为了关闭 Flink，这里同样有一个 `stop-cluster.sh` 脚本。

{% top %}

<a name="adding-jobmanagertaskmanager-instances-to-a-cluster"></a>

### 为集群添加 JobManager/TaskManager 实例

你可以使用 `bin/jobmanager.sh` 和 `bin/taskmanager.sh` 脚本为正在运行的集群添加 JobManager 和 TaskManager 实例。

<a name="adding-a-jobmanager"></a>

#### 添加 JobManager

{% highlight bash %}
bin/jobmanager.sh ((start|start-foreground) [host] [webui-port])|stop|stop-all
{% endhighlight %}

<a name="adding-a-taskmanager"></a>

#### 添加 TaskManager

{% highlight bash %}
bin/taskmanager.sh start|start-foreground|stop|stop-all
{% endhighlight %}

确保在你想启动/关闭相应实例的主机上执行这些脚本。

{% top %}
