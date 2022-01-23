---
title: "Checkpointing under backpressure"
weight: 9
type: docs
aliases:
- /ops/state/unalgined_checkpoints.html
- /ops/state/checkpointing_under_backpressure.html
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
# Checkpointing under backpressure

Normally aligned checkpointing time is dominated by the synchronous and asynchronous parts of the 
checkpointing process. However, when a Flink job is running under heavy backpressure, the dominant 
factor in the end-to-end time of a checkpoint can be the time to propagate checkpoint barriers to 
all operators/subtasks. This is explained in the overview of the
[checkpointing process]({{< ref "docs/concepts/stateful-stream-processing" >}}#checkpointing)).
and can be observed by high
[alignment time and start delay metrics]({{< ref "docs/ops/monitoring/checkpoint_monitoring" >}}#history-tab).
When this happens and becomes an issue, there are three ways to address the problem:
1. Remove the backpressure source by optimizing the Flink job, by adjusting Flink or JVM configurations, or by scaling up.
2. Reduce the amount of buffered in-flight data in the Flink job.
3. Enable unaligned checkpoints.

These options are not mutually exclusive and can be combined together. This document
focuses on the latter two options.

## Buffer debloating

Flink 1.14 introduced a new tool to automatically control the amount of buffered in-flight data
between Flink operators/subtasks. The buffer debloating mechanism can be enabled by setting the property
`taskmanager.network.memory.buffer-debloat.enabled` to `true`. 

This feature works with both aligned and unaligned checkpoints and can improve checkpointing times
in both cases, but the effect of the debloating is most visible with aligned checkpoints.
When using buffer debloating with unaligned checkpoints, the added benefit will be smaller checkpoint
sizes and quicker recovery times (there will be less in-flight data to persist and recover). 

For more information on how the buffer debloating feature works and how to configure it, please refer to the 
[network memory tuning guide]({{< ref "docs/deployment/memory/network_mem_tuning" >}}).
Keep in mind that you can also manually reduce the amount of buffered in-flight data which is also
described in the aforementioned tuning guide.

## Unaligned checkpoints

Starting with Flink 1.11, checkpoints can be unaligned.
[Unaligned checkpoints]({{< ref "docs/concepts/stateful-stream-processing" >}}#unaligned-checkpointing) 
contain in-flight data (i.e., data stored in buffers) as part of the checkpoint state, allowing
checkpoint barriers to overtake these buffers. Thus, the checkpoint duration becomes independent of
the current throughput as checkpoint barriers are effectively not embedded into the stream of data
anymore.

You should use unaligned checkpoints if your checkpointing durations are very high due to
backpressure. Then, checkpointing time becomes mostly independent of the end-to-end latency. Be
aware unaligned checkpointing adds to I/O to the state storage, so you shouldn't use it when the
I/O to the state storage is actually the bottleneck during checkpointing.

In order to enable unaligned checkpoints you can:

{{< tabs "4b9c6a74-8a45-4ad2-9e80-52fe44a85991" >}}
{{< tab "Java" >}}
```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// enables the unaligned checkpoints
env.getCheckpointConfig().enableUnalignedCheckpoints();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()

// enables the unaligned checkpoints
env.getCheckpointConfig.enableUnalignedCheckpoints()
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()

# enables the unaligned checkpoints
env.get_checkpoint_config().enable_unaligned_checkpoints()
```
{{< /tab >}}
{{< /tabs >}}

or in the `flink-conf.yml` configuration file:

```
execution.checkpointing.unaligned: true
```

### Aligned checkpoint timeout

After enabling unaligned checkpoints, you can also specify the aligned checkpoint timeout
programmatically:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ofSeconds(30));
```

or in the `flink-conf.yml` configuration file:

```
execution.checkpointing.aligned-checkpoint-timeout: 30 s
```

When activated, each checkpoint will still begin as an aligned checkpoint, but if the time between
the start of the global checkpoint and the start of the checkpoint on a subtask exceeds the aligned
checkpoint timeout, then the checkpoint will proceed as an unaligned checkpoint.

### Limitations

#### Concurrent checkpoints

Flink currently does not support concurrent unaligned checkpoints. However, due to the more
predictable and shorter checkpointing times, concurrent checkpoints might not be needed at all.
However, savepoints can also not happen concurrently to unaligned checkpoints, so they will take
slightly longer.

#### Interplay with watermarks

Unaligned checkpoints break with an implicit guarantee in respect to watermarks during recovery.
Currently, Flink generates the watermark as the first step of recovery instead of storing the latest
watermark in the operators to ease rescaling. In unaligned checkpoints, that means on recovery,
**Flink generates watermarks after it restores in-flight data**. If your pipeline uses an
**operator that applies the latest watermark on each record** will produce **different results** than for
aligned checkpoints. If your operator depends on the latest watermark being always available, the
workaround is to store the watermark in the operator state. In that case, watermarks should be
stored per key group in a union state to support rescaling.

#### Interplay with long-running record processing

Despite that unaligned checkpoints barriers are able to overtake all other records in the queue. 
The handling of this barrier still can be delayed if the current record takes a lot of time to be processed. 
This situation can occur when firing many timers all at once, for example in windowed operations.
Second problematic scenario might occur when system is being blocked waiting for more than one
network buffer availability when processing a single input record. Flink can not interrupt processing of
a single input record, and unaligned checkpoints have to wait for the currently processed record to be
fully processed. This can cause problems in two scenarios. Either as a result of serialisation of a large
record that doesn't fit into single network buffer or in a flatMap operation, that produces many output
records for one input record. In such scenarios back pressure can block unaligned checkpoints until all
the network buffers required to process the single input record are available.
It also can happen in any other situation when the processing of the single record takes a while. 
As result, the time of the checkpoint can be higher than expected or it can vary.

#### Certain data distribution patterns are not checkpointed

There are types of connections with properties that are impossible to keep with channel data stored
in checkpoints. To preserve these characteristics and ensure no state corruption or unexpected
behaviour, unaligned checkpoints are disabled for such connections. All other exchanges still
perform unaligned checkpoints.

**Pointwise connections**

We currently do not have any hard guarantees on pointwise connections regarding data orderliness.
However, since data was structured implicitly in the same way as any preceding source or keyby, some
users relied on this behaviour to divide compute-intensive tasks into smaller chunks while depending
on orderliness guarantees.

As long as the parallelism does not change, unaligned checkpoints (UC) retain these properties. With
the addition of rescaling of UC that has changed.

Consider a job 

{{< img src="/fig/uc_pointwise.svg" alt="Pointwise connection" width="60%" >}}

If we want to rescale from parallelism p = 2 to p = 3, suddenly the records inside the keyby
channels need to be divided into three channels according to the key groups. That is easily possible by
using the key group ranges of the operators and a way to determine the key(group) of the record (
independent of the actual approach). For the forward channels, we lack the key context entirely. No
record in the forward channel has any key group assigned; it's also impossible to calculate it as
there is no guarantee that the key is still present.

**Broadcast connections**

Broadcast connections bring another problem to the table. There are no guarantees that records are
consumed at the same rate in all channels. This can result in some tasks applying state changes
corresponding to a specific broadcasted event while others don't, as depicted in the figure.

{{< img src="/fig/uc_broadcast.svg" alt="Broadcast connection" width="40%" >}}

Broadcast partitioning is often used to implement a broadcast state which should be equal across all
operators. Flink implements the broadcast state by checkpointing only a single copy of the state
from subtask 0 of the stateful operator. Upon restore, we send that copy to all of the
operators. Therefore it might happen that an operator will get the state with changes applied for a
record that it will soon consume from its checkpointed channels.

### Troubleshooting

#### Corrupted in-flight data
{{< hint warning >}}
Actions described below are a last resort as they will lead to data loss.
{{< /hint >}}
In case of the in-flight data corrupted or by another reason when the job should be restored without the in-flight data, 
it is possible to use  [recover-without-channel-state.checkpoint-id]({{< ref "docs/deployment/config" >}}#execution-checkpointing-recover-without-channel-state-checkpoint) property.
This property requires to specify a checkpoint id for which in-flight data will be ignored.
Do not set this property, unless a corruption inside the persisted in-flight data has lead to an otherwise unrecoverable situation.
The property can be applied only after the job will be redeployed which means this operation makes sense only if [externalized checkpoint]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}}#enabling-and-configuring-checkpointing) is enabled.
