---
title: "Task 故障恢复"
weight: 51
type: docs
aliases:
  - /zh/dev/task_failure_recovery.html
  - /zh/dev/restart_strategies.html
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

# Task 故障恢复

当 Task 发生故障时，Flink 需要重启出错的 Task 以及其他受到影响的 Task ，以使得作业恢复到正常执行状态。

Flink 通过重启策略和故障恢复策略来控制 Task 重启：重启策略决定是否可以重启以及重启的间隔；故障恢复策略决定哪些 Task 需要重启。



## Restart Strategies

Flink 作业如果没有定义重启策略，则会遵循集群启动时加载的默认重启策略。
如果提交作业时设置了重启策略，该策略将覆盖掉集群的默认策略。

通过 Flink 的配置文件 `flink-conf.yaml` 来设置默认的重启策略。配置参数 *restart-strategy.type* 定义了采取何种策略。
如果没有启用 checkpoint，就采用“不重启”策略。如果启用了 checkpoint 且没有配置重启策略，那么就采用固定延时重启策略，
此时最大尝试重启次数由 `Integer.MAX_VALUE` 参数设置。下表列出了可用的重启策略和与其对应的配置值。

每个重启策略都有自己的一组配置参数来控制其行为。
这些参数也在配置文件中设置。
后文的描述中会详细介绍每种重启策略的配置项。

{{< generated/restart_strategy_configuration >}}

除了定义默认的重启策略以外，还可以为每个 Flink 作业单独定义重启策略。
这个重启策略通过在程序中的 `StreamExecutionEnvironment` 对象上调用 `setRestartStrategy` 方法来设置。
当然，对于 `StreamExecutionEnvironment` 也同样适用。

下例展示了如何给我们的作业设置固定延时重启策略。
如果发生故障，系统会重启作业 3 次，每两次连续的重启尝试之间等待 10 秒钟。

{{< tabs "2b011473-9a34-4e7b-943b-be4a9071fe3c" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  3, // 尝试重启的次数
  Time.of(10, TimeUnit.SECONDS) // 延时
));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  3, // 尝试重启的次数
  Time.of(10, TimeUnit.SECONDS) // 延时
))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()
env.set_restart_strategy(RestartStrategies.fixed_delay_restart(
    3,  # 尝试重启的次数
    10000  # 延时(毫秒)
))
```
{{< /tab >}}
{{< /tabs >}}



以下部分详细描述重启策略的配置项。

### Fixed Delay Restart Strategy

固定延时重启策略按照给定的次数尝试重启作业。
如果尝试超过了给定的最大次数，作业将最终失败。
在连续的两次重启尝试之间，重启策略等待一段固定长度的时间。

通过在 `flink-conf.yaml` 中设置如下配置参数，默认启用此策略。

```yaml
restart-strategy.type: fixed-delay
```

{{< generated/fixed_delay_restart_strategy_configuration >}}

例如：

```yaml
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 10 s
```

固定延迟重启策略也可以在程序中设置：

{{< tabs "0877201b-96aa-4985-aebd-0780cf1d8e9e" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  3, // 尝试重启的次数
  Time.of(10, TimeUnit.SECONDS) // 延时
));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  3, // 尝试重启的次数
  Time.of(10, TimeUnit.SECONDS) // 延时
))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()
env.set_restart_strategy(RestartStrategies.fixed_delay_restart(
    3,  # 尝试重启的次数
    10000  # 延时(毫秒)
))
```
{{< /tab >}}
{{< /tabs >}}


### Failure Rate Restart Strategy

故障率重启策略在故障发生之后重启作业，但是当**故障率**（每个时间间隔发生故障的次数）超过设定的限制时，作业会最终失败。
在连续的两次重启尝试之间，重启策略等待一段固定长度的时间。

通过在 `flink-conf.yaml` 中设置如下配置参数，默认启用此策略。

```yaml
restart-strategy.type: failure-rate
```

{{< generated/failure_rate_restart_strategy_configuration >}}

例如：

```yaml
restart-strategy.failure-rate.max-failures-per-interval: 3
restart-strategy.failure-rate.failure-rate-interval: 5 min
restart-strategy.failure-rate.delay: 10 s
```

故障率重启策略也可以在程序中设置：

{{< tabs "f4fba671-e1a8-408d-9f3d-d679aa6473ea" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.failureRateRestart(
  3, // 每个时间间隔的最大故障次数
  Time.of(5, TimeUnit.MINUTES), // 测量故障率的时间间隔
  Time.of(10, TimeUnit.SECONDS) // 延时
));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.failureRateRestart(
  3, // 每个时间间隔的最大故障次数
  Time.of(5, TimeUnit.MINUTES), // 测量故障率的时间间隔
  Time.of(10, TimeUnit.SECONDS) // 延时
))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()
env.set_restart_strategy(RestartStrategies.failure_rate_restart(
    3,  # 每个时间间隔的最大故障次数
    300000,  # 测量故障率的时间间隔
    10000  # 延时(毫秒)
))
```
{{< /tab >}}
{{< /tabs >}}


### No Restart Strategy

作业直接失败，不尝试重启。

```yaml
restart-strategy.type: none
```

不重启策略也可以在程序中设置：

{{< tabs "46f873e1-9582-4303-9a5f-1cdaa31e7ac7" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.noRestart());
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.noRestart())
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()
env.set_restart_strategy(RestartStrategies.no_restart())
```
{{< /tab >}}
{{< /tabs >}}

### Fallback Restart Strategy

使用群集定义的重启策略。
这对于启用了 checkpoint 的流处理程序很有帮助。
如果没有定义其他重启策略，默认选择固定延时重启策略。

## Failover Strategies

Flink 支持多种不同的故障恢复策略，该策略需要通过 Flink 配置文件 `flink-conf.yaml` 中的 *jobmanager.execution.failover-strategy*
配置项进行配置。

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 50%">故障恢复策略</th>
      <th class="text-left">jobmanager.execution.failover-strategy 配置值</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>全图重启</td>
        <td>full</td>
    </tr>
    <tr>
        <td>基于 Region 的局部重启</td>
        <td>region</td>
    </tr>
  </tbody>
</table>

### Restart All Failover Strategy

在全图重启故障恢复策略下，Task 发生故障时会重启作业中的所有 Task 进行故障恢复。

### Restart Pipelined Region Failover Strategy

该策略会将作业中的所有 Task 划分为数个 Region。当有 Task 发生故障时，它会尝试找出进行故障恢复需要重启的最小 Region 集合。
相比于全局重启故障恢复策略，这种策略在一些场景下的故障恢复需要重启的 Task 会更少。

DataStream/Table/SQL 作业中的数据交换形式会根据 [ExecutionConfig]({{< ref "docs/dev/datastream/execution/execution_configuration" >}}) 
中配置的 `ExecutionMode` 决定。处于 STREAM 模式时，所有数据交换都是 Pipelined 形式；
处于 BATCH 模式时，所有数据交换默认都是 Batch 形式。

需要重启的 Region 的判断逻辑如下：
1. 出错 Task 所在 Region 需要重启。
2. 如果要重启的 Region 需要消费的数据有部分无法访问（丢失或损坏），产出该部分数据的 Region 也需要重启。
3. 需要重启的 Region 的下游 Region 也需要重启。这是出于保障数据一致性的考虑，因为一些非确定性的计算或者分发会导致同一个
   Result Partition 每次产生时包含的数据都不相同。

{{< top >}}
