---
title: "Fault Tolerance"
is_beta: false

sub-nav-group: streaming
sub-nav-id: fault_tolerance
sub-nav-pos: 5
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

Flink's fault tolerance mechanism recovers programs in the presence of failures and
continues to execute them. Such failures include machine hardware failures, network failures,
transient program failures, etc.

* This will be replaced by the TOC
{:toc}


Streaming Fault Tolerance
-------------------------

Flink has a checkpointing mechanism that recovers streaming jobs after failues. The checkpointing mechanism requires a *persistent* (or *durable*) source that
can be asked for prior records again (Apache Kafka is a good example of such a source).

The checkpointing mechanism stores the progress in the data sources and data sinks, the state of windows, as well as the user-defined state (see [Working with State](state.html)) consistently to provide *exactly once* processing semantics. Where the checkpoints are stored (e.g., JobManager memory, file system, database) depends on the configured [state backend](state_backends.html).

The [docs on streaming fault tolerance]({{ site.baseurl }}/internals/stream_checkpointing.html) describe in detail the technique behind Flink's streaming fault tolerance mechanism.

To enable checkpointing, call `enableCheckpointing(n)` on the `StreamExecutionEnvironment`, where *n* is the checkpoint interval in milliseconds.

Other parameters for checkpointing include:

- *Number of retries*: The `setNumberOfExecutionRerties()` method defines how many times the job is restarted after a failure.
  When checkpointing is activated, but this value is not explicitly set, the job is restarted infinitely often.

- *exactly-once vs. at-least-once*: You can optionally pass a mode to the `enableCheckpointing(n)` method to choose between the two guarantee levels.
  Exactly-once is preferrable for most applications. At-least-once may be relevant for certain super-low-latency (consistently few milliseconds) applications.

- *number of concurrent checkpoints*: By default, the system will not trigger another checkpoint while one is still in progress. This ensures that the topology does not spend too much time on checkpoints and not make progress with processing the streams. It is possible to allow for multiple overlapping checkpoints, which is interesting for pipelines that have a certain processing delay (for example because the functions call external services that need some time to respond) but that still want to do very frequent checkpoints (100s of milliseconds) to re-process very little upon failures.

- *checkpoint timeout*: The time after which a checkpoint-in-progress is aborted, if it did not complete until then.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// start a checkpoint every 1000 ms
env.enableCheckpointing(1000);

// advanced options:

// set mode to exactly-once (this is the default)
env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// checkpoints have to complete within one minute, or are discarded
env.getCheckpointConfig().setCheckpointTimeout(60000);

// allow only one checkpoint to be in progress at the same time
env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()

// start a checkpoint every 1000 ms
env.enableCheckpointing(1000)

// advanced options:

// set mode to exactly-once (this is the default)
env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

// checkpoints have to complete within one minute, or are discarded
env.getCheckpointConfig.setCheckpointTimeout(60000)

// allow only one checkpoint to be in progress at the same time
env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
{% endhighlight %}
</div>
</div>

{% top %}

### Fault Tolerance Guarantees of Data Sources and Sinks

Flink can guarantee exactly-once state updates to user-defined state only when the source participates in the
snapshotting mechanism. This is currently guaranteed for the Kafka source (and internal number generators), but
not for other sources. The following table lists the state update guarantees of Flink coupled with the bundled sources:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Source</th>
      <th class="text-left" style="width: 25%">Guarantees</th>
      <th class="text-left">Notes</th>
    </tr>
   </thead>
   <tbody>
        <tr>
            <td>Apache Kafka</td>
            <td>exactly once</td>
            <td>Use the appropriate Kafka connector for your version</td>
        </tr>
        <tr>
            <td>RabbitMQ</td>
            <td>at most once (v 0.10) / exactly once (v 1.0) </td>
            <td></td>
        </tr>
        <tr>
            <td>Twitter Streaming API</td>
            <td>at most once</td>
            <td></td>
        </tr>
        <tr>
            <td>Collections</td>
            <td>exactly once</td>
            <td></td>
        </tr>
        <tr>
            <td>Files</td>
            <td>at least once</td>
            <td>At failure the file will be read from the beginning</td>
        </tr>
        <tr>
            <td>Sockets</td>
            <td>at most once</td>
            <td></td>
        </tr>
  </tbody>
</table>

To guarantee end-to-end exactly-once record delivery (in addition to exactly-once state semantics), the data sink needs
to take part in the checkpointing mechanism. The following table lists the delivery guarantees (assuming exactly-once
state updates) of Flink coupled with bundled sinks:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Sink</th>
      <th class="text-left" style="width: 25%">Guarantees</th>
      <th class="text-left">Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>HDFS rolling sink</td>
        <td>exactly once</td>
        <td>Implementation depends on Hadoop version</td>
    </tr>
    <tr>
        <td>Elasticsearch</td>
        <td>at least once</td>
        <td></td>
    </tr>
    <tr>
        <td>Kafka producer</td>
        <td>at least once</td>
        <td></td>
    </tr>
    <tr>
        <td>File sinks</td>
        <td>at least once</td>
        <td></td>
    </tr>
    <tr>
        <td>Socket sinks</td>
        <td>at least once</td>
        <td></td>
    </tr>
    <tr>
        <td>Standard output</td>
        <td>at least once</td>
        <td></td>
    </tr>
  </tbody>
</table>

{% top %}

## Restart Strategies

Flink supports different restart strategies which control how the jobs are restarted in case of a failure.
The cluster can be started with a default restart strategy which is always used when no job specific restart strategy has been defined.
In case that the job is submitted with a restart strategy, this strategy overrides the cluster's default setting.
 
The default restart strategy is set via Flink's configuration file `flink-conf.yaml`.
The configuration parameter *restart-strategy* defines which strategy is taken.
Per default, the no-restart strategy is used.
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
env.setRestartStrategy(RestartStrategies.fixedDelay(
  3, // number of restart attempts 
  10000 // delay in milliseconds
));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.fixedDelay(
  3, // number of restart attempts 
  10000 // delay in milliseconds
))
{% endhighlight %}
</div>
</div>

{% top %}

### Fixed Delay Restart Strategy

The fixed delay restart strategy attempts a given number of times to restart the job.
If the maximum number of attempts is exceeded, the job eventually fails.
In-between two consecutive restart attempts, the restart strategy waits a fixed amount of time.

This strategy is enabled as default by setting the following configuration parameter in `flink-conf.yaml`.

~~~
restart-strategy: fixed-delay
~~~

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
        <td><it>restart-strategy.fixed-delay.attempts</it></td>
        <td>Number of restart attempts</td>
        <td>1</td>
    </tr>
    <tr>
        <td><it>restart-strategy.fixed-delay.delay</it></td>
        <td>Delay between two consecutive restart attempts</td>
        <td><it>akka.ask.timeout</it></td>
    </tr>
  </tbody>
</table>

~~~
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 10 s
~~~

The fixed delay restart strategy can also be set programmatically:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
env.setRestartStrategy(RestartStrategies.fixedDelay(
  3, // number of restart attempts 
  10000 // delay in milliseconds
));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment()
env.setRestartStrategy(RestartStrategies.fixedDelay(
  3, // number of restart attempts 
  10000 // delay in milliseconds
))
{% endhighlight %}
</div>
</div>

#### Restart Attempts

The number of times that Flink retries the execution before the job is declared as failed is configurable via the *restart-strategy.fixed-delay.attempts* parameter.

The default value is **1**.

#### Retry Delays

Execution retries can be configured to be delayed. Delaying the retry means that after a failed execution, the re-execution does not start immediately, but only after a certain delay.

Delaying the retries can be helpful when the program interacts with external systems where for example connections or pending transactions should reach a timeout before re-execution is attempted.

The default value is the value of *akka.ask.timeout*.

{% top %}

### No Restart Strategy

The job fails directly and no restart is attempted.

~~~
restart-strategy: none
~~~

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

{% top %}
