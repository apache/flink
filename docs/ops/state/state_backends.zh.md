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

# 可用的 State Backends

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

建议同时将 [managed memory](../memory/mem_setup_tm.html#managed-memory) 设为0，以保证将最大限度的内存分配给 JVM 上的用户代码。

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

建议同时将 [managed memory](../memory/mem_setup_tm.html#managed-memory) 设为0，以保证将最大限度的内存分配给 JVM 上的用户代码。

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

请同时参考 [Task Executor 内存配置](../memory/mem_tuning.html#rocksdb-state-backend) 中关于 RocksDBStateBackend 的建议。

RocksDBStateBackend 是目前唯一支持增量 CheckPoint 的 State Backend (见 [这里](large_state_tuning.html))。

可以使用一些 RocksDB 的本地指标(metrics)，但默认是关闭的。你能在 [这里]({{ site.baseurl }}/zh/ops/config.html#rocksdb-native-metrics) 找到关于 RocksDB 本地指标的文档。

The total memory amount of RocksDB instance(s) per slot can also be bounded, please refer to documentation [here](large_state_tuning.html#bounding-rocksdb-memory-usage) for details.

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
# 用于存储 operator state 快照的 State Backend

state.backend: filesystem


# 存储快照的目录

state.checkpoints.dir: hdfs://namenode:40010/flink/checkpoints
{% endhighlight %}

# RocksDB State Backend 进阶

*该小节描述 RocksDBStateBackend 的更多细节*

### 增量快照

RocksDBStateBackend 支持*增量快照*。不同于产生一个包含所有数据的全量备份，增量快照中只包含自上一次快照完成之后被修改的记录，因此可以显著减少快照完成的耗时。

一个增量快照是基于（通常多个）前序快照构建的。由于 RocksDB 内部存在 compaction 机制对 sst 文件进行合并，Flink 的增量快照也会定期重新设立起点（rebase），因此增量链条不会一直增长，旧快照包含的文件也会逐渐过期并被自动清理。

和基于全量快照的恢复时间相比，如果网络带宽是瓶颈，那么基于增量快照恢复可能会消耗更多时间，因为增量快照包含的 sst 文件之间可能存在数据重叠导致需要下载的数据量变大；而当 CPU 或者 IO 是瓶颈的时候，基于增量快照恢复会更快，因为从增量快照恢复不需要解析 Flink 的统一快照格式来重建本地的 RocksDB 数据表，而是可以直接基于 sst 文件加载。

虽然状态数据量很大时我们推荐使用增量快照，但这并不是默认的快照机制，您需要通过下述配置手动开启该功能：
  - 在 `flink-conf.yaml` 中设置：`state.backend.incremental: true` 或者
  - 在代码中按照右侧方式配置（来覆盖默认配置）：`RocksDBStateBackend backend = new RocksDBStateBackend(filebackend, true);`

需要注意的是，一旦启用了增量快照，网页上展示的 `Checkpointed Data Size` 只代表增量上传的数据量，而不是一次快照的完整数据量。

### 内存管理

Flink 致力于控制整个进程的内存消耗，以确保 Flink 任务管理器（TaskManager）有良好的内存使用，从而既不会在容器（Docker/Kubernetes, Yarn等）环境中由于内存超用被杀掉，也不会因为内存利用率过低导致不必要的数据落盘或是缓存命中率下降，致使性能下降。

为了达到上述目标，Flink 默认将 RocksDB 的可用内存配置为任务管理器的单槽（per-slot）托管内存量。这将为大多数应用程序提供良好的开箱即用体验，即大多数应用程序不需要调整 RocksDB 配置，简单的增加 Flink 的托管内存即可改善内存相关性能问题。

当然，您也可以选择不使用 Flink 自带的内存管理，而是手动为 RocksDB 的每个列族（ColumnFamily）分配内存（每个算子的每个 state 都对应一个列族）。这为专业用户提供了对 RocksDB 进行更细粒度控制的途径，但同时也意味着用户需要自行保证总内存消耗不会超过（尤其是容器）环境的限制。请参阅 [large state tuning]({{ site.baseurl }}/ops/state/large_state_tuning.html#tuning-rocksdb-memory) 了解有关大状态数据性能调优的一些指导原则。

**RocksDB 使用托管内存**

这个功能默认打开，并且可以通过 `state.backend.rocksdb.memory.managed` 配置项控制。

Flink 并不直接控制 RocksDB 的 native 内存分配，而是通过配置 RocksDB 来确保其使用的内存正好与 Flink 的托管内存预算相同。这是在任务槽（per-slot）级别上完成的（托管内存以任务槽为粒度计算）。

为了设置 RocksDB 实例的总内存使用量，Flink 对同一个任务槽上的所有 RocksDB 实例使用共享的 [cache](https://github.com/facebook/RocksDB/wiki/Block-cache) 以及 [write buffer manager](https://github.com/facebook/rocksdb/wiki/write-buffer-manager)。
共享 cache 将对 RocksDB 中内存消耗的[三个主要来源](https://github.com/facebook/rocksdb/wiki/Memory-usage-in-rocksdb)（块缓存、索引和bloom过滤器、MemTables）设置上限。

Flink还提供了两个参数来控制*写路径*（MemTable）和*读路径*（索引及过滤器，读缓存）之间的内存分配。当您看到 RocksDB 由于缺少写缓冲内存（频繁刷新）或读缓存未命中而性能不佳时，可以使用这些参数调整读写间的内存分配。

  - `state.backend.rocksdb.memory.write-buffer-ratio`，默认值 `0.5`，即 50% 的给定内存会分配给写缓冲区使用。
  - `state.backend.rocksdb.memory.high-prio-pool-ratio`，默认值 `0.1`，即 10% 的 block cache 内存会优先分配给索引及过滤器。
  我们强烈建议不要将此值设置为零，以防止索引和过滤器被频繁踢出缓存而导致性能问题。此外，我们默认将L0级的过滤器和索引将被固定到缓存中以提高性能，更多详细信息请参阅 [RocksDB 文档](https://github.com/facebook/rocksdb/wiki/Block-Cache#caching-index-filter-and-compression-dictionary-blocks)。

<span class="label label-info">注意</span> 上述机制开启时将覆盖用户在 [`PredefinedOptions`](#predefined-per-columnfamily-options) 和 [`OptionsFactory`](#passing-options-factory-to-rocksdb) 中对 block cache 和 write buffer 进行的配置。

<span class="label label-info">注意</span> *仅面向专业用户*：若要手动控制内存，可以将 `state.backend.rocksdb.memory.managed` 设置为 `false`，并通过 [`ColumnFamilyOptions`](#passing-options-factory-to-rocksdb) 配置 RocksDB。
或者可以复用上述 cache/write-buffer-manager 机制，但将内存大小设置为与 Flink 的托管内存大小无关的固定大小（通过 `state.backend.rocksdb.memory.fixed-per-slot` 选项）。
注意在这两种情况下，用户都需要确保在 JVM 之外有足够的内存可供 RocksDB 使用。

### 计时器（内存 vs. RocksDB）

计时器（Timer）用于安排稍后的操作（基于事件时间或处理时间），例如触发窗口或回调 `ProcessFunction`。

当选择 RocksDBStateBackend 时，默认情况下计时器也存储在 RocksDB 中。这是一种健壮且可扩展的方式，允许应用程序使用很多个计时器。另一方面，在 RocksDB 中维护计时器会有一定的成本，因此 Flink 也提供了将计时器存储在 JVM 堆上而使用 RocksDB 存储其他状态的选项。当计时器数量较少时，基于堆的计时器可以有更好的性能。

您可以通过将 `state.backend.rocksdb.timer-service.factory` 配置项设置为 `heap`（而不是默认的 `rocksdb`）来将计时器存储在堆上。

<span class="label label-info">注意</span> *在 RocksDBStateBackend 中使用基于堆的计时器的组合当前不支持计时器状态的异步快照。其他状态（如 keyed state）可以被异步快照。*

### 开启 RocksDB 原生监控指标

您可以选择使用 Flink 的监控指标系统来汇报 RocksDB 的原生指标，并且可以选择性的指定特定指标进行汇报。
请参阅 [configuration docs]({{ site.baseurl }}/ops/config.html#rocksdb-native-metrics) 了解更多详情。

<div class="alert alert-warning">
  <strong>注意：</strong> 启用 RocksDB 的原生指标可能会对应用程序的性能产生负面影响。
</div>

### 列族（ColumnFamily）级别的预定义选项

<span class="label label-info">注意</span> 在引入 [RocksDB 使用托管内存](#memory-management) 功能后，此机制应限于在*专家调优*或*故障处理*中使用。

使用*预定义选项*，用户可以在每个 RocksDB 列族上应用一些预定义的配置，例如配置内存使用、线程、Compaction 设置等。目前每个算子的每个状态都在 RocksDB 中有专门的一个列族存储。

有两种方法可以选择要应用的预定义选项：
  - 通过 `state.backend.rocksdb.predefined-options` 配置项将选项名称设置进 `flink-conf.yaml` 。
  - 通过程序设置：`RocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM)` 。

该选项的默认值是 `DEFAULT` ，对应 `PredefinedOptions.DEFAULT` 。

### 通过 RocksDBOptionsFactory 配置 RocksDB 选项

<span class="label label-info">注意</span> 在引入 [RocksDB 使用托管内存](#memory-management) 功能后，此机制应限于在*专家调优*或*故障处理*中使用。

您也可以通过配置一个 `RocksDBOptionsFactory` 来手动控制 RocksDB 的选项。此机制使您可以对列族的设置进行细粒度控制，例如内存使用、线程、Compaction 设置等。目前每个算子的每个状态都在 RocksDB 中有专门的一个列族存储。

有两种方法可以将 `RocksDBOptionsFactory` 传递给 RocksDBStateBackend：

  - 通过 `state.backend.rocksdb.options-factory` 选项将工厂实现类的名称设置到`flink-conf.yaml` 。
  
  - 通过程序设置，例如 `RocksDBStateBackend.setOptions(new MyOptionsFactory());` 。
  
<span class="label label-info">注意</span> 通过程序设置的 `RocksDBOptionsFactory` 将覆盖 `flink-conf.yaml` 配置文件的设置，且 `RocksDBOptionsFactory` 设置的优先级高于预定义选项（`PredefinedOptions`）。

<span class="label label-info">注意</span> RocksDB是一个本地库，它直接从进程分配内存，
而不是从JVM分配内存。分配给 RocksDB 的任何内存都必须被考虑在内，通常需要将这部分内存从任务管理器（`TaskManager`）的JVM堆中减去。
不这样做可能会导致JVM进程由于分配的内存超过申请值而被 YARN/Mesos 等资源管理框架终止。

**从 flink-conf.yaml 中读取列族选项**

一个实现了 `ConfigurableRocksDBOptionsFactory` 接口的 `RocksDBOptionsFactory` 可以直接从配置文件（`flink-conf.yaml`）中读取设定。

`state.backend.rocksdb.options-factory` 的默认配置是 `org.apache.flink.contrib.streaming.state.DefaultConfigurableOptionsFactory`，它默认会将 [这里定义]({{ site.baseurl }}/ops/config.html#advanced-rocksdb-state-backends-options) 的所有配置项全部加载。
因此您可以简单的通过关闭 RocksDB 使用托管内存的功能并将需要的设置选项加入配置文件来配置底层的列族选项。

下面是自定义 `ConfigurableRocksDBOptionsFactory` 的一个示例 (开发完成后，请将您的实现类全名设置到 `state.backend.rocksdb.options-factory`).

{% highlight java %}

public class MyOptionsFactory implements ConfigurableRocksDBOptionsFactory {

    private static final long DEFAULT_SIZE = 256 * 1024 * 1024;  // 256 MB
    private long blockCacheSize = DEFAULT_SIZE;

    @Override
    public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return currentOptions.setIncreaseParallelism(4)
               .setUseFsync(false);
    }

    @Override
    public ColumnFamilyOptions createColumnOptions(
        ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
        return currentOptions.setTableFormatConfig(
            new BlockBasedTableConfig()
                .setBlockCacheSize(blockCacheSize)
                .setBlockSize(128 * 1024));            // 128 KB
    }

    @Override
    public OptionsFactory configure(Configuration configuration) {
        this.blockCacheSize =
            configuration.getLong("my.custom.rocksdb.block.cache.size", DEFAULT_SIZE);
        return this;
    }
}
{% endhighlight %}

{% top %}
