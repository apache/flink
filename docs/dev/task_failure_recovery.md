---
title: "Task Failure Recovery"
nav-parent_id: execution
nav-pos: 50
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

When a task failure happens, Flink needs to restart the failed task and other affected tasks to recover the job to a normal state.

Restart strategies and failover strategies are used to control the task restarting.
Restart strategies decide whether and when the failed/affected tasks can be restarted.
Failover strategies decide which tasks should be restarted to recover the job.

* This will be replaced by the TOC
{:toc}

## Restart Strategies

The cluster can be started with a default restart strategy which is always used when no job specific restart strategy has been defined.
In case that the job is submitted with a restart strategy, this strategy overrides the cluster's default setting.

The default restart strategy is set via Flink's configuration file `flink-conf.yaml`.
The configuration parameter *restart-strategy* defines which strategy is taken.
If checkpointing is not enabled, the "no restart" strategy is used.
If checkpointing is activated and the restart strategy has not been configured, the fixed-delay strategy is used with 
`Integer.MAX_VALUE` restart attempts.
See the following list of available restart strategies to learn what values are supported.

Each restart strategy comes with its own set of parameters which control its behaviour.
These values are also set in the configuration file.
The description of each restart strategy contains more information about the respective configuration values.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 50%">Restart Strategy</th>
      <th class="text-left">Value for restart-strategy</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>Fixed delay</td>
        <td>fixed-delay</td>
    </tr>
    <tr>
        <td>Failure rate</td>
        <td>failure-rate</td>
    </tr>
    <tr>
        <td>No restart</td>
        <td>none</td>
    </tr>
  </tbody>
</table>

Apart from defining a default restart strategy, it is possible to define for each Flink job a specific restart strategy.
This restart strategy is set programmatically by calling the `setRestartStrategy` method on the `ExecutionEnvironment`.
Note that this also works for the `StreamExecutionEnvironment`.

The following example shows how we can set a fixed delay restart strategy for our job.
In case of a failure the system tries to restart the job 3 times and waits 10 seconds in-between successive restart attempts.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  3, // number of restart attempts
  Time.of(10, TimeUnit.SECONDS) // delay
));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  3, // number of restart attempts
  Time.of(10, TimeUnit.SECONDS) // delay
))
{% endhighlight %}
</div>
</div>

{% top %}

The following sections describe restart strategy specific configuration options.

### Fixed Delay Restart Strategy

The fixed delay restart strategy attempts a given number of times to restart the job.
If the maximum number of attempts is exceeded, the job eventually fails.
In-between two consecutive restart attempts, the restart strategy waits a fixed amount of time.

This strategy is enabled as default by setting the following configuration parameter in `flink-conf.yaml`.

{% highlight yaml %}
restart-strategy: fixed-delay
{% endhighlight %}

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Configuration Parameter</th>
      <th class="text-left" style="width: 40%">Description</th>
      <th class="text-left">Default Value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><code>restart-strategy.fixed-delay.attempts</code></td>
        <td>The number of times that Flink retries the execution before the job is declared as failed.</td>
        <td>1, or <code>Integer.MAX_VALUE</code> if activated by checkpointing</td>
    </tr>
    <tr>
        <td><code>restart-strategy.fixed-delay.delay</code></td>
        <td>Delaying the retry means that after a failed execution, the re-execution does not start immediately, but only after a certain delay. Delaying the retries can be helpful when the program interacts with external systems where for example connections or pending transactions should reach a timeout before re-execution is attempted.</td>
        <td><code>akka.ask.timeout</code>, or 10s if activated by checkpointing</td>
    </tr>
  </tbody>
</table>

For example:

{% highlight yaml %}
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 10 s
{% endhighlight %}

The fixed delay restart strategy can also be set programmatically:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  3, // number of restart attempts
  Time.of(10, TimeUnit.SECONDS) // delay
));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
  3, // number of restart attempts
  Time.of(10, TimeUnit.SECONDS) // delay
))
{% endhighlight %}
</div>
</div>

{% top %}

### Failure Rate Restart Strategy

The failure rate restart strategy restarts job after failure, but when `failure rate` (failures per time interval) is exceeded, the job eventually fails.
In-between two consecutive restart attempts, the restart strategy waits a fixed amount of time.

This strategy is enabled as default by setting the following configuration parameter in `flink-conf.yaml`.

{% highlight yaml %}
restart-strategy: failure-rate
{% endhighlight %}

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Configuration Parameter</th>
      <th class="text-left" style="width: 40%">Description</th>
      <th class="text-left">Default Value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td><it>restart-strategy.failure-rate.max-failures-per-interval</it></td>
        <td>Maximum number of restarts in given time interval before failing a job</td>
        <td>1</td>
    </tr>
    <tr>
        <td><it>restart-strategy.failure-rate.failure-rate-interval</it></td>
        <td>Time interval for measuring failure rate.</td>
        <td>1 minute</td>
    </tr>
    <tr>
        <td><it>restart-strategy.failure-rate.delay</it></td>
        <td>Delay between two consecutive restart attempts</td>
        <td><it>akka.ask.timeout</it></td>
    </tr>
  </tbody>
</table>

{% highlight yaml %}
restart-strategy.failure-rate.max-failures-per-interval: 3
restart-strategy.failure-rate.failure-rate-interval: 5 min
restart-strategy.failure-rate.delay: 10 s
{% endhighlight %}

The failure rate restart strategy can also be set programmatically:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.failureRateRestart(
  3, // max failures per interval
  Time.of(5, TimeUnit.MINUTES), //time interval for measuring failure rate
  Time.of(10, TimeUnit.SECONDS) // delay
));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.failureRateRestart(
  3, // max failures per unit
  Time.of(5, TimeUnit.MINUTES), //time interval for measuring failure rate
  Time.of(10, TimeUnit.SECONDS) // delay
))
{% endhighlight %}
</div>
</div>

{% top %}

### No Restart Strategy

The job fails directly and no restart is attempted.

{% highlight yaml %}
restart-strategy: none
{% endhighlight %}

The no restart strategy can also be set programmatically:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.noRestart());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.noRestart())
{% endhighlight %}
</div>
</div>

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
- All data exchanges in a DataStream job or Streaming Table/SQL job are pipelined.
- All data exchanges in a Batch Table/SQL job are batched by default.
- The data exchange types in a DataSet job are determined by the 
  [ExecutionMode]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/api/common/ExecutionMode.html) 
  which can be set through [ExecutionConfig]({{ site.baseurl }}/dev/execution_configuration.html).

The regions to restart are decided as below:
1. The region containing the failed task will be restarted.
2. If a result partition is not available while it is required by a region that will be restarted,
   the region producing the result partition will be restarted as well.
3. If a region is to be restarted, all of its consumer regions will also be restarted. This is to guarantee
   data consistency because nondeterministic processing or partitioning can result in different partitions.

{% top %}
