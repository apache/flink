---
title: "Fault Tolerance"
is_beta: false

sub-nav-group: streaming
sub-nav-id: fault_tolerance
sub-nav-pos: 3
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


Streaming Fault Tolerance (DataStream API)
------------------------------------------

Flink has a checkpointing mechanism that recovers streaming jobs after failues. The checkpointing mechanism requires a *persistent* (or *durable*) source that
can be asked for prior records again (Apache Kafka is a good example of such a source).

The checkpointing mechanism stores the progress in the data sources and data sinks, the state of windows, as well as the user-defined state (see [Working with State]({{ site.baseurl }}/apis/streaming_guide.html#working-with-state)) consistently to provide *exactly once* processing semantics. Where the checkpoints are stored (e.g., JobManager memory, file system, database) depends on the configured [state backend]({{ site.baseurl }}/apis/state_backends.html).

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
