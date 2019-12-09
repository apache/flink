---
title: "State Backends"
nav-parent_id: ops_state
nav-pos: 11
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

用 [Data Stream API]({{ site.baseurl }}/zh/dev/datastream_api.html) 编写的程序通常以各种形式保存状态：

- 在 Window 触发之前要么收集元素、要么聚合
- 转换函数可以使用 key/value 格式的状态接口来存储状态
- 转换函数可以实现 `CheckpointedFunction` 接口，使其本地变量具有容错能力

另请参阅 Streaming API 指南中的 [状态部分]({{ site.baseurl }}/zh/dev/stream/state/index.html) 。

在启动 CheckPoint 机制时，状态会随着 CheckPoint 而持久化，以防止数据丢失、保障恢复时的一致性。
状态内部的存储格式、状态在 CheckPoint 时如何持久化以及持久化在哪里均取决于选择的 **State Backend**。

* ToC
{:toc}

## 可用的 State Backends

Flink 内置了以下这些开箱即用的 state backends ：

 - *MemoryStateBackend*
 - *FsStateBackend*
 - *RocksDBStateBackend*

如果不设置，默认使用 MemoryStateBackend。


### MemoryStateBackend

在 *MemoryStateBackend* 内部，数据以 Java 对象的形式存储在堆中。 Key/value 形式的状态和窗口算子持有存储着状态值、触发器的 hash table。

在 CheckPoint 时，State Backend 对状态进行快照，并将快照信息作为 CheckPoint 应答消息的一部分发送给 JobManager(master)，同时 JobManager 也将快照信息存储在堆内存中。

MemoryStateBackend 能配置异步快照。强烈建议使用异步快照来防止数据流阻塞，注意，异步快照默认是开启的。
用户可以在实例化 `MemoryStateBackend` 的时候，将相应布尔类型的构造参数设置为 `false` 来关闭异步快照（仅在 debug 的时候使用），例如：

{% highlight java %}
    new MemoryStateBackend(MAX_MEM_STATE_SIZE, false);
{% endhighlight %}

MemoryStateBackend 的限制：

  - 默认情况下，每个独立的状态大小限制是 5 MB。在 MemoryStateBackend 的构造器中可以增加其大小。
  - 无论配置的最大状态内存大小（MAX_MEM_STATE_SIZE）有多大，都不能大于 akka frame 大小（看[配置参数]({{ site.baseurl }}/zh/ops/config.html)）。
  - 聚合后的状态必须能够放进 JobManager 的内存中。

MemoryStateBackend 适用场景：

  - 本地开发和调试。
  - 状态很小的 Job，例如：由每次只处理一条记录的函数（Map、FlatMap、Filter 等）构成的 Job。Kafka Consumer 仅仅需要非常小的状态。


### FsStateBackend

*FsStateBackend* 需要配置一个文件系统的 URL（类型、地址、路径），例如："hdfs://namenode:40010/flink/checkpoints" 或 "file:///data/flink/checkpoints"。

FsStateBackend 将正在运行中的状态数据保存在 TaskManager 的内存中。CheckPoint 时，将状态快照写入到配置的文件系统目录中。
少量的元数据信息存储到 JobManager 的内存中（高可用模式下，将其写入到 CheckPoint 的元数据文件中）。

FsStateBackend 默认使用异步快照来防止 CheckPoint 写状态时对数据处理造成阻塞。
用户可以在实例化 `FsStateBackend` 的时候，将相应布尔类型的构造参数设置为 `false` 来关闭异步快照，例如：

{% highlight java %}
    new FsStateBackend(path, false);
{% endhighlight %}

FsStateBackend 适用场景:

  - 状态比较大、窗口比较长、key/value 状态比较大的 Job。
  - 所有高可用的场景。

### RocksDBStateBackend

*RocksDBStateBackend* 需要配置一个文件系统的 URL （类型、地址、路径），例如："hdfs://namenode:40010/flink/checkpoints" 或 "file:///data/flink/checkpoints"。

RocksDBStateBackend 将正在运行中的状态数据保存在 [RocksDB](http://rocksdb.org) 数据库中，RocksDB 数据库默认将数据存储在 TaskManager 的数据目录。
CheckPoint 时，整个 RocksDB 数据库被 checkpoint 到配置的文件系统目录中。
少量的元数据信息存储到 JobManager 的内存中（高可用模式下，将其存储到 CheckPoint 的元数据文件中）。 

RocksDBStateBackend 只支持异步快照。

RocksDBStateBackend 的限制：

  - 由于 RocksDB 的 JNI API 构建在 byte[] 数据结构之上, 所以每个 key 和 value 最大支持 2^31 字节。
    重要信息: RocksDB 合并操作的状态（例如：ListState）累积数据量大小可以超过 2^31 字节，但是会在下一次获取数据时失败。这是当前 RocksDB JNI 的限制。

RocksDBStateBackend 的适用场景：

  - 状态非常大、窗口非常长、key/value 状态非常大的 Job。
  - 所有高可用的场景。

注意，你可以保留的状态大小仅受磁盘空间的限制。与状态存储在内存中的 FsStateBackend 相比，RocksDBStateBackend 允许存储非常大的状态。
然而，这也意味着使用 RocksDBStateBackend 将会使应用程序的最大吞吐量降低。
所有的读写都必须序列化、反序列化操作，这个比基于堆内存的 state backend 的效率要低很多。

RocksDBStateBackend 是目前唯一支持增量 CheckPoint 的 State Backend (见 [这里](large_state_tuning.html))。

可以使用一些 RocksDB 的本地指标(metrics)，但默认是关闭的。你能在 [这里]({{ site.baseurl }}/zh/ops/config.html#rocksdb-native-metrics) 找到关于 RocksDB 本地指标的文档。

## 设置 State Backend

如果没有明确指定，将使用 jobmanager 做为默认的 state backend。你能在 **flink-conf.yaml** 中为所有 Job 设置其他默认的 State Backend。
每一个 Job 的 state backend 配置会覆盖默认的 state backend 配置，如下所示：

### 设置每个 Job 的 State Backend

`StreamExecutionEnvironment` 可以对每个 Job 的 State Backend 进行设置，如下所示：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStateBackend(new FsStateBackend("hdfs://namenode:40010/flink/checkpoints"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.setStateBackend(new FsStateBackend("hdfs://namenode:40010/flink/checkpoints"))
{% endhighlight %}
</div>
</div>

如果你想在 IDE 中使用 `RocksDBStateBackend`，或者需要在作业中通过编程方式动态配置 `RocksDBStateBackend`，必须添加以下依赖到 Flink 项目中。

{% highlight xml %}
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-statebackend-rocksdb{{ site.scala_version_suffix }}</artifactId>
    <version>{{ site.version }}</version>
    <scope>provided</scope>
</dependency>
{% endhighlight %}

<div class="alert alert-info" markdown="span">
  <strong>注意:</strong> 由于 RocksDB 是 Flink 默认分发包的一部分，所以如果你没在代码中使用 RocksDB，则不需要添加此依赖。而且可以在 `flink-conf.yaml` 文件中通过 `state.backend` 配置 State Backend，以及更多的 [checkpointing]({{ site.baseurl }}/zh/ops/config.html#checkpointing) 和 [RocksDB 特定的]({{ site.baseurl }}/zh/ops/config.html#rocksdb-state-backend) 参数。
</div>


### 设置默认的（全局的） State Backend

在 `flink-conf.yaml` 可以通过键 `state.backend` 设置默认的 State Backend。

可选值包括 *jobmanager* (MemoryStateBackend)、*filesystem* (FsStateBackend)、*rocksdb* (RocksDBStateBackend)，
或使用实现了 state backend 工厂 [StateBackendFactory](https://github.com/apache/flink/blob/master/flink-runtime/src/main/java/org/apache/flink/runtime/state/StateBackendFactory.java) 的类的全限定类名，
例如： RocksDBStateBackend 对应为 `org.apache.flink.contrib.streaming.state.RocksDBStateBackendFactory`。

`state.checkpoints.dir` 选项指定了所有 State Backend 写 CheckPoint 数据和写元数据文件的目录。
你能在 [这里](checkpoints.html#directory-structure) 找到关于 CheckPoint 目录结构的详细信息。

配置文件的部分示例如下所示：

{% highlight yaml %}
# The backend that will be used to store operator state checkpoints

state.backend: filesystem


# Directory for storing checkpoints

state.checkpoints.dir: hdfs://namenode:40010/flink/checkpoints
{% endhighlight %}

#### RocksDB State Backend Config Options

{% include generated/rocks_db_configuration.html %}

{% top %}
