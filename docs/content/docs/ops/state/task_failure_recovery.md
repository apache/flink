---
title: "Task Failure Recovery"
weight: 51
type: docs
aliases:
  - /dev/task_failure_recovery.html
  - /dev/restart_strategies.html
  - /docs/dev/execution_task_failure_recovery/
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

# Task Failure Recovery

When a task failure happens, Flink needs to restart the failed task and other affected tasks to recover the job to a normal state.

Restart strategies and failover strategies are used to control the task restarting.
Restart strategies decide whether and when the failed/affected tasks can be restarted.
Failover strategies decide which tasks should be restarted to recover the job.

## Restart Strategies

The cluster can be started with a default restart strategy which is always used when no job specific restart strategy has been defined.
In case that the job is submitted with a restart strategy, this strategy overrides the cluster's default setting.

The default restart strategy is set via Flink's configuration file `flink-conf.yaml`.
The configuration parameter *restart-strategy.type* defines which strategy is taken.
If checkpointing is not enabled, the "no restart" strategy is used.
If checkpointing is activated and the restart strategy has not been configured, the fixed-delay strategy is used with 
`1` restart attempts.
See the following list of available restart strategies to learn what values are supported.

Each restart strategy comes with its own set of parameters which control its behaviour.
These values are also set in the configuration file.
The description of each restart strategy contains more information about the respective configuration values.

{{< generated/restart_strategy_configuration >}}

Apart from defining a default restart strategy, it is possible to define for each Flink job a specific restart strategy.

The following example shows how we can set a fixed delay restart strategy for our job.
In case of a failure the system tries to restart the job 3 times and waits 10 seconds in-between successive restart attempts.

{{< tabs "4ab65f13-607a-411a-8d24-e709f701df6a" >}}
{{< tab "Java" >}}
```java
Configuration config = new Configuration();
config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3); // number of restart attempts
config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10)); // delay
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  3, // number of restart attempts
  Time.of(10, TimeUnit.SECONDS) // delay
))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
config = Configuration()
config.set_string('restart-strategy.type', 'fixed-delay')
config.set_string('restart-strategy.fixed-delay.attempts', '3') # number of restart attempts
config.set_string('restart-strategy.fixed-delay.delay', '10000 ms') # delay
env = StreamExecutionEnvironment.get_execution_environment(config)
```
{{< /tab >}}
{{< /tabs >}}


The following sections describe restart strategy specific configuration options.

### Fixed Delay Restart Strategy

The fixed delay restart strategy attempts a given number of times to restart the job.
If the maximum number of attempts is exceeded, the job eventually fails.
In-between two consecutive restart attempts, the restart strategy waits a fixed amount of time.

This strategy is enabled as default by setting the following configuration parameter in `flink-conf.yaml`.

```yaml
restart-strategy.type: fixed-delay
```

{{< generated/fixed_delay_restart_strategy_configuration >}}

For example:

```yaml
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 10 s
```

The fixed delay restart strategy can also be set programmatically:

{{< tabs "73f5d009-b9af-4bfe-be22-d1c4659fd1ec" >}}
{{< tab "Java" >}}
```java
Configuration config = new Configuration();
config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3); // number of restart attempts
config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(10)); // delay
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  3, // number of restart attempts
  Time.of(10, TimeUnit.SECONDS) // delay
))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
config = Configuration()
config.set_string('restart-strategy.type', 'fixed-delay')
config.set_string('restart-strategy.fixed-delay.attempts', '3') # number of restart attempts
config.set_string('restart-strategy.fixed-delay.delay', '10000 ms') # delay
env = StreamExecutionEnvironment.get_execution_environment(config)
```
{{< /tab >}}
{{< /tabs >}}


### Exponential Delay Restart Strategy

The exponential delay restart strategy attempts to restart the job infinitely, with increasing delay up to the maximum delay.
The job never fails.
In-between two consecutive restart attempts, the restart strategy keeps exponentially increasing until the maximum number is reached.
Then, it keeps the delay at the maximum number.

When the job executes correctly, the exponential delay value resets after some time; this threshold is configurable.

```yaml
restart-strategy.type: exponential-delay
```

{{< generated/exponential_delay_restart_strategy_configuration >}}

For example:

```yaml
restart-strategy.exponential-delay.initial-backoff: 10 s
restart-strategy.exponential-delay.max-backoff: 2 min
restart-strategy.exponential-delay.backoff-multiplier: 1.4
restart-strategy.exponential-delay.reset-backoff-threshold: 10 min
restart-strategy.exponential-delay.jitter-factor: 0.1
restart-strategy.exponential-delay.attempts-before-reset-backoff: 10
```

The exponential delay restart strategy can also be set programmatically:

{{< tabs "e433f119-50e2-4eae-9977-7e6e44acab61" >}}
{{< tab "Java" >}}
```java
Configuration config = new Configuration();
config.set(RestartStrategyOptions.RESTART_STRATEGY, "exponential-delay");
config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF, Durartion.ofMillis(1));
config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF, Durartion.ofMillis(1000));
config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER, 1.1); // exponential multiplier
config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD, Durartion.ofMillis(2000)); // threshold duration to reset delay to its initial value
config.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR, 0.1); // jitter        
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.exponentialDelayRestart(
  Time.of(1, TimeUnit.MILLISECONDS), // initial delay between restarts
  Time.of(1000, TimeUnit.MILLISECONDS), // maximum delay between restarts
  1.1, // exponential multiplier
  Time.of(2, TimeUnit.SECONDS), // threshold duration to reset delay to its initial value
  0.1 // jitter
))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}

### Failure Rate Restart Strategy

The failure rate restart strategy restarts job after failure, but when `failure rate` (failures per time interval) is exceeded, the job eventually fails.
In-between two consecutive restart attempts, the restart strategy waits a fixed amount of time.

This strategy is enabled as default by setting the following configuration parameter in `flink-conf.yaml`.

```yaml
restart-strategy.type: failure-rate
```

{{< generated/failure_rate_restart_strategy_configuration >}}

```yaml
restart-strategy.failure-rate.max-failures-per-interval: 3
restart-strategy.failure-rate.failure-rate-interval: 5 min
restart-strategy.failure-rate.delay: 10 s
```

The failure rate restart strategy can also be set programmatically:

{{< tabs "d8d547ce-003b-4821-afc0-3d95aca40f1e" >}}
{{< tab "Java" >}}
```java
Configuration config = new Configuration();
config.set(RestartStrategyOptions.RESTART_STRATEGY, "failure-rate");
config.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL, 3); // max failures per interval
config.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL, Duration.ofMinutes(5)); // time interval for measuring failure rate
config.set(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY, Duration.ofSeconds(10)); // delay
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.failureRateRestart(
  3, // max failures per unit
  Time.of(5, TimeUnit.MINUTES), //time interval for measuring failure rate
  Time.of(10, TimeUnit.SECONDS) // delay
))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
config = Configuration()
config.set_string('restart-strategy.type', 'failure-rate')
config.set_string('restart-strategy.failure-rate.max-failures-per-interval', '3') # max failures per interval
config.set_string('restart-strategy.failure-rate.failure-rate-interval', '5 min') # time interval for measuring failure rate
config.set_string('restart-strategy.failure-rate.delay', '10 s') # delay
env = StreamExecutionEnvironment.get_execution_environment(config)
```
{{< /tab >}}
{{< /tabs >}}


### No Restart Strategy

The job fails directly and no restart is attempted.

```yaml
restart-strategy.type: none
```

The no restart strategy can also be set programmatically:

{{< tabs "4812d55b-bb89-4000-be7c-d9dcdad6010e" >}}
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

The cluster defined restart strategy is used. 
This is helpful for streaming programs which enable checkpointing.
By default, a fixed delay restart strategy is chosen if there is no other restart strategy defined.

## Failover Strategies

Flink supports different failover strategies which can be configured via the configuration parameter
*jobmanager.execution.failover-strategy* in Flink's configuration file `flink-conf.yaml`.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 50%">Failover Strategy</th>
      <th class="text-left">Value for jobmanager.execution.failover-strategy</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>Restart all</td>
        <td>full</td>
    </tr>
    <tr>
        <td>Restart pipelined region</td>
        <td>region</td>
    </tr>
  </tbody>
</table>

### Restart All Failover Strategy

This strategy restarts all tasks in the job to recover from a task failure.

### Restart Pipelined Region Failover Strategy

This strategy groups tasks into disjoint regions. When a task failure is detected, 
this strategy computes the smallest set of regions that must be restarted to recover from the failure. 
For some jobs this can result in fewer tasks that will be restarted compared to the Restart All Failover Strategy.

A region is a set of tasks that communicate via pipelined data exchanges. 
That is, batch data exchanges denote the boundaries of a region.

DataStream/Table/SQL job data exchanges are determined by the `ExecutionMode`, 
which can be set through [ExecutionConfig]({{< ref "docs/dev/datastream/execution/execution_configuration" >}}),
which are pipelined in Streaming Mode, are batched by default in Batch Mode.

The regions to restart are decided as below:
1. The region containing the failed task will be restarted.
2. If a result partition is not available while it is required by a region that will be restarted,
   the region producing the result partition will be restarted as well.
3. If a region is to be restarted, all of its consumer regions will also be restarted. This is to guarantee
   data consistency because nondeterministic processing or partitioning can result in different partitions.

{{< top >}}
