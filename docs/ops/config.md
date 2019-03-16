---
title: "Configuration"
nav-id: "config"
nav-parent_id: ops
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



**对于单节点设置，Flink已准备好开箱即用，您无需更改默认配置即可开始使用.**

开箱即用的配置将使用您的默认Java安装.您可以手动设置环境变量JAVA_HOME或配置项env.java.home中conf/flink-conf.yaml，如果你想手动覆盖Java运行时使用.

此页面列出了设置性能良好（分布式）安装通常所需的最常用选项.此外，此处还列出了所有可用配置参数的完整列表.

所有配置都已完成conf/flink-conf.yaml，预计将是具有格式的[YAML key value pairs](http://www.yaml.org/spec/1.2/spec.html)的扁平集合key: value.

系统和运行脚本在启动时解析配置.对配置文件的更改需要重新启动Flink JobManager和TaskManagers.

TaskManagers的配置文件可能不同，Flink不承担集群中的统一机器.

* This will be replaced by the TOC
{:toc}

## 常见选项

{% include generated/common_section.html %}

## 完整参考

### HDFS

<div class="alert alert-warning">
  <strong>注意:</strong> 不推荐使用这些Keys，建议使用环境变量配置Hadoop路径 <code>HADOOP_CONF_DIR</code>.
</div>

These parameters configure the default HDFS used by Flink. Setups that do not specify a HDFS configuration have to specify the full path to HDFS files (`hdfs://address:port/path/to/files`) Files will also be written with default HDFS parameters (block size, replication factor)。

这些参数配置Flink使用的默认HDFS.未指定HDFS配置的设置必须指定HDFS文件的完整路径（hdfs://address:port/path/to/files）文件也将使用默认HDFS参数（块大小，复制因子）编写.

- `fs.hdfs.hadoopconf`: Hadoop文件系统（HDFS）配置目录的绝对路径（可选值）.指定此值允许程序使用短URI引用HDFS文件（hdfs:///path/to/files不包括文件URI中NameNode的地址和端口）。如果没有此选项，则可以访问HDFS文件，但需要完全限定的URI hdfs://address:port/path/to/files.此选项还会导致文件编写者获取HDFS的块大小和复制因子的默认值。Flink将在指定目录中查找“core-site.xml”和“hdfs-site.xml”文件.

- `fs.hdfs.hdfsdefault`: Hadoop自己的配置文件“hdfs-default.xml”的绝对路径（DEFAULT：null）.

- `fs.hdfs.hdfssite`: Hadoop自己的配置文件“hdfs-site.xml”的绝对路径（DEFAULT：null）.

### 核心

{% include generated/core_configuration.html %}

### JobManager

{% include generated/job_manager_configuration.html %}

### TaskManager

{% include generated/task_manager_configuration.html %}

### 分布式协调（通过Akka）

{% include generated/akka_configuration.html %}

### REST

{% include generated/rest_configuration.html %}

### Blob服务器

{% include generated/blob_server_configuration.html %}

### 心跳管理器

{% include generated/heartbeat_manager_configuration.html %}

### SSL设置

{% include generated/security_configuration.html %}

### 网络通讯（通过Netty）

这些参数允许高级调整.在大型群集上运行并发高吞吐量作业时，默认值就足够了.

{% include generated/netty_configuration.html %}

### Web前端

{% include generated/web_configuration.html %}

### 文件系统

{% include generated/file_system_configuration.html %}

### 编译/优化

{% include generated/optimizer_configuration.html %}

### 运行时算法

{% include generated/algorithm_configuration.html %}

### Resource Manager

本节中的配置键独立于使用的资源管理框架（YARN，Mesos，Standalone，...）

{% include generated/resource_manager_configuration.html %}

### YARN

{% include generated/yarn_config_configuration.html %}

### Mesos

{% include generated/mesos_configuration.html %}

#### Mesos TaskManager

{% include generated/mesos_task_manager_configuration.html %}

### 高可用性（HA）

{% include generated/high_availability_configuration.html %}

#### 基于ZooKeeper的HA模式

{% include generated/high_availability_zookeeper_configuration.html %}

### ZooKeeper安全

{% include generated/zoo_keeper_configuration.html %}

### 基于Kerberos的安全性

{% include generated/kerberos_configuration.html %}

### 环境

{% include generated/environment_configuration.html %}

### 检查点

{% include generated/checkpointing_configuration.html %}

### RocksDB状态后端

{% include generated/rocks_db_configuration.html %}

### 可查询状态

{% include generated/queryable_state_configuration.html %}

### 度量

{% include generated/metric_configuration.html %}

### RocksDB度量标准
某些RocksDB本机指标可能会转发给Flink的指标报告者。
所有本机度量标准都限定为运算符，然后按列族进一步细分; 值报告为unsigned longs。

<div class="alert alert-warning">
  <strong>注意:</strong>启用本机指标可能会导致性能下降，应谨慎设置。
</div>

{% include generated/rocks_db_native_metric_configuration.html %}

### 历史服务器

You have to configure `jobmanager.archive.fs.dir` in order to archive terminated jobs and add it to the list of monitored directories via `historyserver.archive.fs.dir` if you want to display them via the HistoryServer's web frontend.

如果要通过HistoryServer的Web前端显示它们，则必须进行配置jobmanager.archive.fs.dir以存档已终止的作业并将其添加到受监视目录列表中historyserver.archive.fs.dir.

- `jobmanager.archive.fs.dir`: 将有关已终止作业的信息上载到的目录.您必须将此目录添加到历史服务器的受监视目录列表中historyserver.archive.fs.dir.

{% include generated/history_server_configuration.html %}

## 留存

- `mode`:Flink的执行模式.可能的值是legacy和new.要启动旧组件，您必须指定legacy（DEFAULT：）new.

## 背景

### 配置网络缓冲区

如果您看到异常java.io.IOException: Insufficient number of network buffers，则需要调整用于网络缓冲区的内存量，以便程序在您的TaskManager上运行.

网络缓冲区是通信层的关键资源.它们用于在通过网络传输之前缓冲记录，并在将传入数据解析为记录并将其传递给应用程序之前缓冲传入数据.足够数量的网络缓冲区对于实现良好的吞吐量至关重要.

<div class="alert alert-info">
从Flink 1.3开始，你可以遵循“越多越好”的成语而不会对延迟造成任何惩罚（我们通过限制每个通道使用的实际缓冲区数量来防止每个传出和传入通道中的过度缓冲，即*缓冲膨胀*）。
</div>

通常，将TaskManager配置为具有足够的缓冲区，以使您希望同时打开的每个逻辑网络连接都具有专用缓冲区.对于网络上的每个点对点数据交换存在逻辑网络连接，这通常发生在重新分区或广播步骤（混洗阶段）.在那些中，TaskManager中的每个并行任务必须能够与所有其他并行任务进行通信。

<div class="alert alert-warning">
  <strong>注意:</strong> 从Flink 1.5开始，网络缓冲区将始终在堆外分配，即在JVM堆之外，而不管其值是多少<code>taskmanager.memory.off-heap</code>. 这样，我们可以将这些缓冲区直接传递给底层网络堆栈层.
</div>

#### 设置内存分数

以前，手动设置网络缓冲区的数量，这成为一个非常容易出错的任务（见下文）.从Flink 1.3开始，可以使用以下配置参数定义用于网络缓冲区的一小部分内存：

- `taskmanager.network.memory.fraction`: 用于网络缓冲区的JVM内存的分数（DEFAULT：0.1）,
- `taskmanager.network.memory.min`: 网络缓冲区的最小内存大小（默认值：64MB）,
- `taskmanager.network.memory.max`: 网络缓冲区的最大内存大小（默认值：1GB），和
- `taskmanager.memory.segment-size`: 内存管理器和网络堆栈使用的内存缓冲区大小（以字节为单位）（默认值：32KB）。

#### 直接设置网络缓冲区的数量

<div class="alert alert-warning">
  <strong>注意:</strong> 不建议使用这种配置网络缓冲区使用的内存量的方法.请考虑使用上述方法定义要使用的内存部分。
</div>

缓冲器的上一个TaskManager所要求数量为 总度的平行度（数的目标）* 节点内并行性（源在一个TaskManager数）× N 与 N 是限定多少repartitioning-恒定/您希望同时处于活动状态的广播步骤.由于节点内并行性通常是核心数量，并且超过4个重新分区或广播频道很少并行活动，因此它经常归结为

{% highlight plain %}
#slots-per-TM^2 * #TMs * 4
{% endhighlight %}

哪里#slots per TM是 [每个TaskManager插槽数量](#configuring-taskmanager-processing-slots) and `#TMs` 是TaskManager的总数.

例如，为了支持20个8插槽机器的集群，您应该使用大约5000个网络缓冲区来获得最佳吞吐量.

默认情况下，每个网络缓冲区的大小为32 KiBytes.在上面的示例中，系统因此将为网络缓冲区分配大约300 MiBytes.

可以使用以下参数配置网络缓冲区的数量和大小：

- `taskmanager.network.numberOfBuffers`, 和
- `taskmanager.memory.segment-size`.

### 配置临时I/O目录

虽然Flink的目标是尽可能多地处理主内存中的数据，但是需要处理的内存比内存更多的数据并不少见.Flink的运行时用于将临时数据写入磁盘以处理这些情况.

该`taskmanager.tmp.dirs`参数指定Flink写入临时文件的目录列表.目录的路径需要用'：'（冒号字符）分隔.Flink将同时向（从）每个配置的目录写入（或读取）一个临时文件.这样，临时I / O可以均匀地分布在多个独立的I / O设备（如硬盘）上，以提高性能.要利用快速I / O设备（例如，SSD，RAID，NAS），可以多次指定目录.

如果`taskmanager.tmp.dirs`未显式指定参数，Flink会将临时数据写入 算子操作系统的临时目录，例如Linux系统中的*/tmp*.

### 配置TaskManager处理槽

Flink通过将程序拆分为子任务并将这些子任务调度到处理槽来并行执行程序.

Each Flink TaskManager provides processing slots in the cluster. The number of slots is typically proportional to the number of available CPU cores __of each__ TaskManager. As a general recommendation, the number of available CPU cores is a good default for `taskmanager.numberOfTaskSlots`.

每个Flink TaskManager都在集群中提供处理插槽.插槽数通常与每个 TaskManager 的可用CPU核心数成比例。作为一般建议，可用的CPU核心数量是一个很好的默认值`taskmanager.numberOfTaskSlots`.

启动Flink应用程序时，用户可以提供用于该作业的默认插槽数.因此调用命令行值-p（用于并行）.此外，可以为整个应用程序和各个算子[设置编程API中的插槽数]({{site.baseurl}}/dev/parallel.html).

<img src="{{ site.baseurl }}/fig/slots_parallelism.svg" class="img-responsive" />

{% top %}
