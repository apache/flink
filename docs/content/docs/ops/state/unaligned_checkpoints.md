---
title: "Unaligned checkpoints"
weight: 9
type: docs
aliases:
- /ops/state/unalgined_checkpoints.html
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
# Unaligned checkpoints

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

## Limitations

### Concurrent checkpoints

Flink currently does not support concurrent unaligned checkpoints. However, due to the more
predictable and shorter checkpointing times, concurrent checkpoints might not be needed at all.
However, savepoints can also not happen concurrently to unaligned checkpoints, so they will take
slightly longer.

### Interplay with watermarks

Unaligned checkpoints break with an implicit guarantee in respect to watermarks during recovery.
Currently, Flink generates the watermark as the first step of recovery instead of storing the latest
watermark in the operators to ease rescaling. In unaligned checkpoints, that means on recovery,
**Flink generates watermarks after it restores in-flight data**. If your pipeline uses an
**operator that applies the latest watermark on each record** will produce **different results** than for
aligned checkpoints. If your operator depends on the latest watermark being always available, the
workaround is to store the watermark in the operator state. In that case, watermarks should be
stored per key group in a union state to support rescaling.

### Certain data distribution patterns are not checkpointed

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

## Troubleshooting

### Corrupted in-flight data
{{< hint warning >}}
Actions described below are a last resort as they will lead to data loss.
{{< /hint >}}
In case of the in-flight data corrupted or by another reason when the job should be restored without the in-flight data, 
it is possible to use  [recover-without-channel-state.checkpoint-id]({{< ref "docs/deployment/config" >}}#execution-checkpointing-recover-without-channel-state-checkpoint) property.
This property requires to specify a checkpoint id for which in-flight data will be ignored.
Do not set this property, unless a corruption inside the persisted in-flight data has lead to an otherwise unrecoverable situation.
The property can be applied only after the job will be redeployed which means this operation makes sense only if [externalized checkpoint]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}}#enabling-and-configuring-checkpointing) is enabled.
