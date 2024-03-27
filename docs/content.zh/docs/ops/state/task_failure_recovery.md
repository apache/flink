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

通过 [Flink 配置文件]({{< ref "docs/deployment/config#flink-配置文件" >}}) 来设置默认的重启策略。配置参数 *restart-strategy.type* 定义了采取何种策略。
如果没有启用 checkpoint，就采用 `不重启` 策略。如果启用了 checkpoint 且没有配置重启策略，默认采用
`exponential-delay` (指数延迟) 重启策略，且会使用 `exponential-delay` 相关配置项的默认值。
下表列出了可用的重启策略和与其对应的配置值。

每个重启策略都有自己的一组配置参数来控制其行为。
这些参数也在配置文件中设置。
后文的描述中会详细介绍每种重启策略的配置项。

{{< generated/restart_strategy_configuration >}}

除了定义默认的重启策略以外，还可以为每个 Flink 作业单独定义重启策略。

下例展示了如何给我们的作业设置固定延时重启策略。
如果发生故障，系统会重启作业 3 次，每两次连续的重启尝试之间等待 10 秒钟。

{{< tabs "2b011473-9a34-4e7b-943b-be4a9071fe3c" >}}
{{< tab "Java" >}}
```java
Configuration config = new Configuration();
config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3); // 尝试重启的次数
config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10)); // 延时
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
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
config = Configuration()
config.set_string('restart-strategy.type', 'fixed-delay')
config.set_string('restart-strategy.fixed-delay.attempts', '3') # 尝试重启的次数
config.set_string('restart-strategy.fixed-delay.delay', '10000 ms') # 延时
env = StreamExecutionEnvironment.get_execution_environment(config)
```
{{< /tab >}}
{{< /tabs >}}



以下部分详细描述重启策略的配置项。

### Fixed Delay Restart Strategy

固定延时重启策略按照给定的次数尝试重启作业。
如果尝试超过了给定的最大次数，作业将最终失败。
在连续的两次重启尝试之间，重启策略等待一段固定长度的时间。

通过在 [Flink 配置文件]({{< ref "docs/deployment/config#flink-配置文件" >}}) 中设置如下配置参数，默认启用此策略。

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
Configuration config = new Configuration();
config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3); // 尝试重启次数
config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10)); // 延时
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
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
config = Configuration()
config.set_string('restart-strategy.type', 'fixed-delay')
config.set_string('restart-strategy.fixed-delay.attempts', '3') # 尝试重启的次数
config.set_string('restart-strategy.fixed-delay.delay', '10000 ms') # 延时
env = StreamExecutionEnvironment.get_execution_environment(config)
```
{{< /tab >}}
{{< /tabs >}}

### Exponential Delay Restart Strategy

指数延迟重启策略在两次连续的重新启动尝试之间，重新启动的延迟时间不断呈指数增长，直到达到最大延迟时间。
然后，延迟时间将保持在最大延迟时间。

当作业正确地执行后，指数延迟时间会在一些时间后被重置为初始值，这些阈值可以被配置。

```yaml
restart-strategy.type: exponential-delay
```

{{< generated/exponential_delay_restart_strategy_configuration >}}

例如:

```yaml
restart-strategy.exponential-delay.initial-backoff: 10 s
restart-strategy.exponential-delay.max-backoff: 2 min
restart-strategy.exponential-delay.backoff-multiplier: 1.4
restart-strategy.exponential-delay.reset-backoff-threshold: 10 min
restart-strategy.exponential-delay.jitter-factor: 0.1
restart-strategy.exponential-delay.attempts-before-reset-backoff: 10
```

指数延迟重启策略可以在代码中被指定：

{{< tabs "e433f119-50e2-4eae-9977-7e6e44acab61" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
  Time.milliseconds(1),
  Time.milliseconds(1000),
  1.1, // exponential multiplier
  Time.milliseconds(2000), // 重置延迟时间到初始值的阈值
  0.1 // jitter
));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
  Time.of(1, TimeUnit.MILLISECONDS), // initial delay between restarts
  Time.of(1000, TimeUnit.MILLISECONDS), // maximum delay between restarts
  1.1, // exponential multiplier
  Time.of(2, TimeUnit.SECONDS), // 重置延迟时间到初始值的阈值
  0.1 // jitter
))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Python API 不支持。
```
{{< /tab >}}
{{< /tabs >}}

#### 示例

以下是一个示例，用于解释指数延迟重启策略的工作原理。

```yaml
restart-strategy.exponential-delay.initial-backoff: 1 s
restart-strategy.exponential-delay.backoff-multiplier: 2
restart-strategy.exponential-delay.max-backoff: 10 s
# 为了方便描述，这里关闭了 jitter
restart-strategy.exponential-delay.jitter-factor: 0
```

- `initial-backoff = 1s` 表示当作业第一次发生异常时会延迟 1 秒后进行重试。
- `backoff-multiplier = 2` 表示当作业连续异常时，每次的延迟时间翻倍。
- `max-backoff = 10 s` 表示重试的延迟时间最多为 10 秒。

基于这些参数：

- 当作业发生异常需要进行第 1 次重试时，作业会延迟 1 秒后重试。
- 当作业发生异常需要进行第 2 次重试时，作业会延迟 2 秒后重试(翻倍)。
- 当作业发生异常需要进行第 3 次重试时，作业会延迟 4 秒后重试(翻倍)。
- 当作业发生异常需要进行第 4 次重试时，作业会延迟 8 秒后重试(翻倍)。
- 当作业发生异常需要进行第 5 次重试时，作业会延迟 10 秒后重试(翻倍后超过上限，所以使用上限 10 秒做为延迟时间)。
- 在第 5 次重试时，延迟时间已经达到了 max-backoff(上限)，所以第 5 次重试以后，作业延迟时间会保持在 10 秒不变，每次失败后都会延迟 10 秒后重试。


```yaml
restart-strategy.exponential-delay.jitter-factor: 0.1
restart-strategy.exponential-delay.attempts-before-reset-backoff: 8
restart-strategy.exponential-delay.reset-backoff-threshold: 6 min
```

- `jitter-factor = 0.1` 表示每次的延迟时间会加减一个随机值，随机值的范围在 0.1 的比例内。
  - 例如第 3 次重试时，作业延迟时间在 3.6 秒到 4.4 秒之间( 3.6 = 4 * 0.9, 4.4 = 4 * 1.1)。
  - 例如第 4 次重试时，作业延迟时间在 7.2 秒到 8.8 秒之间 (7.2 = 8 * 0.9, 8.8 = 8 * 1.1)。
  - 随机值可以避免多个作业在同一时间重启，所以在生产环境不建议将 jitter-factor 设置为 0。
- `attempts-before-reset-backoff = 8` 表示如果作业连续重试了 8 次后仍然有异常，则会失败（不再重试）。
- `reset-backoff-threshold = 6 min` 表示当作业已经持续 6 分钟没发生异常时，则会重置延迟时间和重试计数。
  也就是当作业发生异常时，如果上一次异常发生在 6 分钟之前，则重试的延迟时间重置为 1 秒，当前的重试计数重置为 1。


### Failure Rate Restart Strategy

故障率重启策略在故障发生之后重启作业，但是当**故障率**（每个时间间隔发生故障的次数）超过设定的限制时，作业会最终失败。
在连续的两次重启尝试之间，重启策略等待一段固定长度的时间。

通过在 [Flink 配置文件]({{< ref "docs/deployment/config#flink-配置文件" >}}) 中设置如下配置参数，默认启用此策略。

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
Configuration config = new Configuration();
config.set(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate");
config.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL, 3); // 每个时间间隔的最大故障次数
config.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL, Duration.ofMinutes(5)); // 测量故障率的时间间隔
config.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY, Duration.ofSeconds(10)); // 延时
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
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
config = Configuration()
config.set_string('restart-strategy.type', 'failure-rate')
config.set_string('restart-strategy.failure-rate.max-failures-per-interval', '3') # 每个时间间隔的最大故障次数
config.set_string('restart-strategy.failure-rate.failure-rate-interval', '5 min') # 测量故障率的时间间隔
config.set_string('restart-strategy.failure-rate.delay', '10 s') # 延时
env = StreamExecutionEnvironment.get_execution_environment(config)
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
Configuration config = new Configuration();
config.set(RestartStrategyOptions.RESTART_STRATEGY, "none");
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
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
config = Configuration()
config.set_string('restart-strategy.type', 'none')
env = StreamExecutionEnvironment.get_execution_environment(config)
```
{{< /tab >}}
{{< /tabs >}}

### Fallback Restart Strategy

使用群集定义的重启策略。
这对于启用了 checkpoint 的流处理程序很有帮助。
如果没有定义其他重启策略，默认选择指数延迟重启策略。

### 默认重启策略

当 Checkpoint 开启且用户没有指定重启策略时，[`指数延迟重启策略`]({{< ref "docs/ops/state/task_failure_recovery" >}}#exponential-delay-restart-strategy) 
是当前默认的重启策略。我们强烈推荐 Flink 用户使用指数延迟重启策略，因为使用这个策略时，
作业偶尔异常可以快速重试，作业频繁异常可以避免外部组件发生雪崩。原因如下所示：

- 所有的重启策略在重启作业时都会延迟一定的时间来避免频繁重试对外部组件的产生较大压力。
- 除了指数延迟重启策略以外的所有重启策略延迟时间都是固定的。
  - 如果延迟时间设置的过短，当作业短时间内频繁异常时，会频繁重启访问外部组件的主节点，可能导致外部组件发生雪崩。
    例如：大量的 Flink 作业都在消费 Kafka，当 Kafka 集群出现故障时大量的 Flink 作业都在同一时间频繁重试，很可能导致雪崩。
  - 如果延迟时间设置的过长，当作业偶尔失败时需要等待很久才会重试，从而导致作业可用率降低。
- 指数延迟重启策略每次重试的延迟时间会指数递增，直到达到最大延迟时间。
  - 延迟时间的初始值较短，所以当作业偶尔失败时，可以快速重试，提升作业可用率。
  - 当作业短时间内频繁失败时，指数延迟重启策略会降低重试的频率，从而避免外部组件雪崩。
- 除此以外，指数延迟重启策略的延迟时间支持抖动因子 (jitter-factor) 的配置项。
  - 抖动因子会为每次的延迟时间加减一个随机值。
  - 即使多个作业使用指数延迟重启策略且所有的配置参数完全相同，抖动因子也会让这些作业分散在不同的时间重启。

## Failover Strategies

Flink 支持多种不同的故障恢复策略，该策略需要通过 [Flink 配置文件]({{< ref "docs/deployment/config#flink-配置文件" >}}) 中的 *jobmanager.execution.failover-strategy*
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
