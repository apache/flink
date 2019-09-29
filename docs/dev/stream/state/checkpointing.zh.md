---
title: "Checkpointing"
nav-parent_id: streaming_state
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

* ToC
{:toc}

Flink 中的每个方法或算子都能够被**状态化**（阅读 [working with state](state.html) 查看详细）。
状态化的方法在处理单个 元素/事件 的时候存储数据，让状态成为使各个类型的算子更加精细的重要部分。
为了让状态容错，Flink 需要为状态添加**检查点（Checkpoint）**。检查点允许 Flink 恢复状态，并且能够在流中定位，让应用像无故障一样的运行。

[Documentation on streaming fault tolerance]({{ site.baseurl }}/internals/stream_checkpointing.html) 介绍了 Flink 流计算容错机制的内部技术原理。


## 前提条件

Flink 的检查点机制会与流和状态的持久化存储交互。一般需要：

  - 一个能够在一定时间内重新得到记录的持久化数据源，例如持久化消息队列（例如 Apache Kafka、RabbitMQ、 Amazon Kinesis、 Google PubSub 等）或文件系统（例如 HDFS、 S3、 GFS、 NFS、 Ceph 等）。
  - 存放状态的持久化存储，通常为分布式文件系统（比如 HDFS、 S3、 GFS、 NFS、 Ceph 等）。

## 激活与配置检查点

默认情况下，检查点是禁用的。通过调用 `StreamExecutionEnvironment` 的 `enableCheckpointing(n)` 来激活检查点，里面的 *n* 是进行检查点的间隔，单位毫秒。

检查点其他的属性包括：

  - *仅仅一次（exactly-once） 对比 至少一次（at-least-once）*：你可以选择向 `enableCheckpointing(n)` 方法中传入一个模式来选择使用两种保证等级中的哪一种。
    对于大多数应用来说，仅仅一次是较好的选择。至少一次可能与某些延迟超低（始终只有几毫秒）的应用的关联较大。
  
  - *检查点超时（checkpoint timeout）*：如果过了这个时间，还在进行中的检查点操作就会被抛弃。
  
  - *检查点之间的最小时间（minimum time between checkpoints）*： 为了确保流应用在检查点之间有足够的进展，可以定义在检查点之间需要多久的时间。如果值设置为了 *5000*，
    无论检查点持续时间与间隔是多久，在前一个检查点完成的五秒后才会开始下一个检查点。
    
    往往使用"检查点之间的时间"来配置应用会比检查点间隔容易很多，因为"检查点之间的时间"在检查点的执行时间在超过平均值时不会受到影响（例如如果目标的存储系统忽然变得很慢）。
    
    注意这个值也意味着并发检查点的数目是*一*。

  - *并发检查点的数目*: 默认情况下，系统不会在有一个检查点在进行时触发另一个检查点。这确保了拓扑不会再检查点上花费太多时间，并且不会和流处理一块进行。
    在感兴趣的 pipelines 上有确定的处理延迟（例如是因为某方法所调用的外部服务需要些时间来回复），但是仍然想进行频繁的检查点去最小化故障重跑时，允许多个检查点重叠是可能的。
    
    该选项在检查点间的最小时间被定义时不能使用。
    
  - *外部检查点（externalized checkpoints）*: 你可以配置周期检查点存储到外部系统中。外部检查点将他们的元数据写到持久化存储上并且在 job 失败的时候*不会*被自动删除。
    这种方式下，如果你的 job 失败，你将会有一个现有的检查点去恢复。更多的细节请看 [deployment notes on externalized checkpoints]({{ site.baseurl }}/ops/state/checkpoints.html#externalized-checkpoints)。
  
  - *在检查点出错时 置错/继续 task*：他决定了在 task 检查点的过程中发生的错误时，一个 task 是否会被置为失败。这个是默认的行为。
     或者被禁用时，这个任务将会简单的吧检查点削减为检查点协调器并继续运行。
     
  - *选择检查点进行恢复（prefer checkpoint for recovery）*：此属性将确定作业是否回退到最新检查点，即使有更多最近的保存点可用来潜在地减少恢复时间。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// 每 1000 ms 开始一个检查点
env.enableCheckpointing(1000);

// 高级选项：

// 设置模式为仅仅一次 (这是默认值)
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// 确认检查点之间的时间会进行 500 ms
env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

// 检查点必须在一分钟内完成，否则就会被丢弃
env.getCheckpointConfig().setCheckpointTimeout(60000);

// 同一时间只允许一个检查点进行
env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

// 激活在 job 中止后仍然保留的外部检查点
env.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

// 允许在有更近保存点时回退到检查点
env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()

// 每 1000 ms 开始一个检查点
env.enableCheckpointing(1000)

// 高级选项：

// 设置模式为仅仅一次 (这是默认值)
env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

// 确认检查点之间的时间会进行 500 ms
env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

// 检查点必须在一分钟内完成，否则就会被丢弃
env.getCheckpointConfig.setCheckpointTimeout(60000)

// 如果 task 的检查点发生错误，会阻止 task 失败，检查点仅仅会被丢弃
env.getCheckpointConfig.setFailTasksOnCheckpointingErrors(false)

// 同一时间只允许一个检查点进行
env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}
env = StreamExecutionEnvironment.get_execution_environment()

# 每 1000 ms 开始一个检查点
env.enable_checkpointing(1000)

# 高级选项：

# 设置模式为仅仅一次 (这是默认值)
env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

# 确认检查点之间的时间会进行 500 ms
env.get_checkpoint_config().set_min_pause_between_checkpoints(500)

# 检查点必须在一分钟内完成，否则就会被丢弃
env.get_checkpoint_config().set_checkpoint_timeout(60000)

# 同一时间只允许一个检查点进行
env.get_checkpoint_config().set_max_concurrent_checkpoints(1)

# 激活在 job 中止后仍然保留的外部检查点
env.get_checkpoint_config().enable_externalized_checkpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

# 允许在有更近保存点时回退到检查点
env.get_checkpoint_config().set_prefer_checkpoint_for_recovery(True)
{% endhighlight %}
</div>
</div>

### 相关的配置选项

更多的属性 与/或 默认值能在 `conf/flink-conf.yaml` 中设置（完整教程请阅读 [configuration]({{ site.baseurl }}/ops/config.html)）。

{% include generated/checkpointing_configuration.html %}

{% top %}


## Selecting a State Backend

Flink 的 [checkpointing mechanism]({{ site.baseurl }}/internals/stream_checkpointing.html) 存储在定时器与状态操作里的持久化快照，
包括连接器（connectors），窗口（windows）以及任何用户[自定义的状态](state.html)
检查点存储在那里取决于所配置的 **状态后端（State Backend）**（比如 JobManager memory、 file system、 database）。

默认情况下，状态是保持在 TaskManagers 的内存中，检查点保存在 JobManager 的内存中。为了体量大的状态的能完全恰当的持久化，
Flink 支持各种各样的途径去存储，检查点状态到其他的 state backends。通过 `StreamExecutionEnvironment.setStateBackend(…)` 来配置所选的 state backends。

阅读 [state backends]({{ site.baseurl }}/ops/state/state_backends.html) 来查看在可用 state backends 上的更多细节，选择 job范围 与 集群返回 的配置。

## 在 Iterative Jobs 中的状态检查点

Flink 现在只提供没有 iterations 的 job 的处理保证。在 iterative job 上激活检查点会导致异常。为了在迭代程序中强制进行检查点，用于需要在激活检查点时设置一个特殊的标志： `env.enableCheckpointing(interval, CheckpointingMode.EXACTLY_ONCE, force = true)`。

请注意在环形边上飞翔的记录（以及与之相关的状态变化）在故障时会丢失。

{% top %}

## 重启策略

Flink 支持不同的重启策略，来控制 job 万一故障时该如何重启。更多信息请阅读 [Restart Strategies]({{ site.baseurl }}/dev/restart_strategies.html)。

{% top %}

