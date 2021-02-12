---
title: "Checkpoints"
weight: 8
type: docs
aliases:
  - /ops/state/checkpoints.html
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

# Checkpoints

## Overview

Checkpoints make state in Flink fault tolerant by allowing state and the
corresponding stream positions to be recovered, thereby giving the application
the same semantics as a failure-free execution.

See [Checkpointing]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}}) for how to enable and
configure checkpoints for your program.

## Checkpoint Storage

When checkpointing is enabled, managed state is persisted to ensure consistent recovery in case of failures.
Where the state is persisted during checkpointing depends on the chosen **Checkpoint Storage**.

## Available Checkpoint Storage Options

Out of the box, Flink bundles these checkpoint storage types:

 - *JobManagerCheckpointStorage*
 - *FileSystemCheckpointStorage*

{{< hint info >}}
If a checkpoint directory is configured `FileSystemCheckpointStorage` will be used, otherwise the system will use the `JobManagerCheckpointStorage`.
{{< /hint >}}

### The JobManagerCheckpointStorage

The *JobManagerCheckpointStorage* stores checkpoint snapshots in the JobManager's heap.

It can be configured to fail the checkpoint if it goes over a certain size to avoid `OutOfMemoryError`'s on the JobManager. To set this feature, users can instantiate a `JobManagerCheckpointStorage` with the corresponding max size:

```java
new JobManagerCheckpointStorage(MAX_MEM_STATE_SIZE);
```

Limitations of the `JobManagerCheckpointStorage`:

  - The size of each individual state is by default limited to 5 MB. This value can be increased in the constructor of the `JobManagerCheckpointStorage`.
  - Irrespective of the configured maximal state size, the state cannot be larger than the Akka frame size (see [Configuration]({{< ref "docs/deployment/config" >}})).
  - The aggregate state must fit into the JobManager memory.

The JobManagerCheckpointStorage is encouraged for:

  - Local development and debugging
  - Jobs that use very little state, such as jobs that consist only of record-at-a-time functions (Map, FlatMap, Filter, ...). The Kafka Consumer requires very little state.

### The FileSystemCheckpointStorage

The *FileSystemCheckpointStorage* is configured with a file system URL (type, address, path), such as "hdfs://namenode:40010/flink/checkpoints" or "file:///data/flink/checkpoints".

Upon checkpointing, it writes state snapshots into files in the configured file system and directory. Minimal metadata is stored in the JobManager's memory (or, in high-availability mode, in the metadata checkpoint).

If a checkpoint directory is specified, `FileSystemCheckpointStorage` will be used to persist checkpoint snapshots. 

The `FileSystemCheckpointStorage` is encouraged for:

  - All high-availability setups.

It is also recommended to set [managed memory]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory) to zero.
This will ensure that the maximum amount of memory is allocated for user code on the JVM.

## Retained Checkpoints

Checkpoints are by default not retained and are only used to resume a
job from failures. They are deleted when a program is cancelled.
You can, however, configure periodic checkpoints to be retained.
Depending on the configuration these *retained* checkpoints are *not*
automatically cleaned up when the job fails or is canceled.
This way, you will have a checkpoint around to resume from if your job fails.

```java
CheckpointConfig config = env.getCheckpointConfig();
config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
```

The `ExternalizedCheckpointCleanup` mode configures what happens with checkpoints when you cancel the job:

- **`ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION`**: Retain the checkpoint when the job is cancelled. Note that you have to manually clean up the checkpoint state after cancellation in this case.

- **`ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION`**: Delete the checkpoint when the job is cancelled. The checkpoint state will only be available if the job fails.

### Directory Structure

Similarly to [savepoints]({{< ref "docs/ops/state/savepoints" >}}), a checkpoint consists
of a meta data file and, depending on the state backend, some additional data
files. The meta data file and data files are stored in the directory that is
configured via `state.checkpoints.dir` in the configuration files, 
and also can be specified for per job in the code.

The current checkpoint directory layout ([introduced by FLINK-8531](https://issues.apache.org/jira/browse/FLINK-8531)) is as follows:

```yaml
/user-defined-checkpoint-dir
    /{job-id}
        |
        + --shared/
        + --taskowned/
        + --chk-1/
        + --chk-2/
        + --chk-3/
        ...
```

The **SHARED** directory is for state that is possibly part of multiple checkpoints, **TASKOWNED** is for state that must never be dropped by the JobManager, and **EXCLUSIVE** is for state that belongs to one checkpoint only. 

{{< hint warning >}}
The checkpoint directory is not part of a public API and can be changed in the future release.
{{< /hint >}}

#### Configure globally via configuration files

```yaml
state.checkpoints.dir: hdfs:///checkpoints/
```

#### Configure for per job on the checkpoint configuration

```java
env.getCheckpointConfig().setCheckpointStorage("hdfs:///checkpoints-data/");
```

#### Configure with checkpoint storage instance

Alternatively, checkpoint storage can be set by specifying the desired checkpoint storage instance which allows for setting low level configurations such as write buffer sizes. 

```java
env.getCheckpointConfig().setCheckpointStorage(
  new FileSystemCheckpointStorage("hdfs:///checkpoints-data/", FILE_SIZE_THESHOLD));
```

### Difference to Savepoints

Checkpoints have a few differences from [savepoints]({{< ref "docs/ops/state/savepoints" >}}). They
- use a state backend specific (low-level) data format, may be incremental.
- do not support Flink specific features like rescaling.

### Resuming from a retained checkpoint

A job may be resumed from a checkpoint just as from a savepoint
by using the checkpoint's meta data file instead (see the
[savepoint restore guide]({{< ref "docs/deployment/cli" >}}#restore-a-savepoint)). Note that if the
meta data file is not self-contained, the jobmanager needs to have access to
the data files it refers to (see [Directory Structure](#directory-structure)
above).

```shell
$ bin/flink run -s :checkpointMetaDataPath [:runArgs]
```

### Unaligned checkpoints

{{< hint danger >}}
Unaligned checkpoints may produce corrupted checkpoints in 1.12.0 and 1.12.1 and we discourage use in production settings.
{{< /hint >}}

Starting with Flink 1.11, checkpoints can be unaligned. 
[Unaligned checkpoints]({{< ref "docs/concepts/stateful-stream-processing" >}}#unaligned-checkpointing) contain in-flight data (i.e., data stored in
buffers) as part of the checkpoint state, which allows checkpoint barriers to
overtake these buffers. Thus, the checkpoint duration becomes independent of the
current throughput as checkpoint barriers are effectively not embedded into 
the stream of data anymore.

You should use unaligned checkpoints if your checkpointing durations are very
high due to backpressure. Then, checkpointing time becomes mostly
independent of the end-to-end latency. Be aware unaligned checkpointing
adds to I/O to the state backends, so you shouldn't use it when the I/O to
the state backend is actually the bottleneck during checkpointing.

Note that unaligned checkpoints is a brand-new feature that currently has the
following limitations:

- You cannot rescale or change job graph with from unaligned checkpoints. You 
  have to take a savepoint before rescaling. Savepoints are always aligned 
  independent of the alignment setting of checkpoints.
- Flink currently does not support concurrent unaligned checkpoints. However, 
  due to the more predictable and shorter checkpointing times, concurrent 
  checkpoints might not be needed at all. However, savepoints can also not 
  happen concurrently to unaligned checkpoints, so they will take slightly 
  longer.
- Unaligned checkpoints break with an implicit guarantee in respect to 
  watermarks during recovery:

Currently, Flink generates the watermark as a first step of recovery instead of 
storing the latest watermark in the operators to ease rescaling. In unaligned 
checkpoints, that means on recovery, **Flink generates watermarks after it 
restores in-flight data**. If your pipeline uses an **operator that applies the
latest watermark on each record**, it will produce **different results** than 
for aligned checkpoints. If your operator depends on the latest watermark being
always available, then the workaround is to store the watermark in the operator 
state. To support rescaling, watermarks should be stored per key-group in a 
union-state. We most likely will implement this approach as a general solution 
(didn't make it into Flink 1.11.0).

In the upcoming release(s), Flink will address these limitations and will
provide a fine-grained way to trigger unaligned checkpoints only for the 
in-flight data that moves slowly with timeout mechanism. These options will
decrease the pressure on I/O in the state backends and eventually allow
unaligned checkpoints to become the default checkpointing. 

{{< top >}}
