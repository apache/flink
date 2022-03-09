---
title: "Savepoints"
weight: 9
type: docs
aliases:
  - /ops/state/savepoints.html
  - /apis/streaming/savepoints.html
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

# Savepoints

## What is a Savepoint? How is a Savepoint different from a Checkpoint?

A Savepoint is a consistent image of the execution state of a streaming job, created via Flink's [checkpointing mechanism]({{< ref "docs/learn-flink/fault_tolerance" >}}). You can use Savepoints to stop-and-resume, fork,
or update your Flink jobs. Savepoints consist of two parts: a directory with (typically large) binary files on stable storage (e.g. HDFS, S3, ...) and a (relatively small) meta data file. The files on stable storage represent the net data of the job's execution state
image. The meta data file of a Savepoint contains (primarily) pointers to all files on stable storage that are part of the Savepoint, in form of relative paths.

{{< hint info >}}
In order to allow upgrades between programs and Flink versions, it is important to check out the following section about [assigning IDs to your operators](#assigning-operator-ids).
{{< /hint >}}

Conceptually, Flink's Savepoints are different from Checkpoints in a similar way that backups are different from recovery logs in traditional database systems. The primary purpose of Checkpoints is to provide a recovery mechanism in case of
unexpected job failures. A Checkpoint's lifecycle is managed by Flink, i.e. a Checkpoint is created, owned, and deleted by Flink - without user interaction. As a method of recovery and being periodically triggered, two main
design goals for the Checkpoint implementation are i) being as lightweight to create and ii) being as fast to restore from as possible. Optimizations towards those goals can exploit certain properties, e.g. that the job code
does not change between the execution attempts. Checkpoints are usually dropped after the job was terminated by the user unless the job is explicitly configured to retain checkpoints upon failure or cancellation.

In contrast to all this, Savepoints are created, owned, and deleted by the user. Their use case  is planned, manual backup and resume. For example, this could be an update of your Flink version, changing your job graph,
changing parallelism, forking a second job like for a red/blue deployment, and so on. Of course, Savepoints must survive job termination. Conceptually, Savepoints can be a bit more expensive to produce and restore from and focus
more on portability and flexibility with respect to changes to the job.

## Assigning Operator IDs

It is **highly recommended** that you specify operator IDs via the **`uid(String)`** method. These IDs are used to scope the state of each operator.

```java
DataStream<String> stream = env.
  // Stateful source (e.g. Kafka) with ID
  .addSource(new StatefulSource())
  .uid("source-id") // ID for the source operator
  .shuffle()
  // Stateful mapper with ID
  .map(new StatefulMapper())
  .uid("mapper-id") // ID for the mapper
  // Stateless printing sink
  .print(); // Auto-generated ID
```

If you do not specify the IDs manually they will be generated automatically. You can automatically restore from the savepoint as long as these IDs do not change. The generated IDs depend on the structure of your program and are sensitive to program changes. Therefore, it is highly recommended assigning these IDs manually.

### Savepoint State

You can think of a savepoint as holding a map of `Operator ID -> State` for each stateful operator:

```plain
Operator ID | State
------------+------------------------
source-id   | State of StatefulSource
mapper-id   | State of StatefulMapper
```

In the above example, the print sink is stateless and hence not part of the savepoint state. By default, we try to map each entry of the savepoint back to the new program.

## Operations

You can use the [command line client]({{< ref "docs/deployment/cli" >}}#savepoints) to *trigger savepoints*, *cancel a job with a savepoint*, *resume from savepoints*, and *dispose savepoints*.

It is also possible to *resume from savepoints* using the webui.

### Triggering Savepoints

When triggering a savepoint, a new savepoint directory is created where the data as well as the meta data will be stored. The location of this directory can be controlled by [configuring a default target directory](#configuration) or by specifying a custom target directory with the trigger commands (see the [`:targetDirectory` argument](#trigger-a-savepoint)).

{{< hint warning >}}
**Attention:** The target directory has to be a location accessible by both the JobManager(s) and TaskManager(s) e.g. a location on a distributed file-system or Object Store.
{{< /hint >}}

For example with a `FsStateBackend` or `RocksDBStateBackend`:

```shell
# Savepoint target directory
/savepoints/

# Savepoint directory
/savepoints/savepoint-:shortjobid-:savepointid/

# Savepoint file contains the checkpoint meta data
/savepoints/savepoint-:shortjobid-:savepointid/_metadata

# Savepoint state
/savepoints/savepoint-:shortjobid-:savepointid/...
```

Savepoints can generally be moved by moving (or copying) the entire savepoint directory to a different location, and Flink will be able to restore from the moved savepoint.

{{< hint info >}}
There are two exceptions: 

1) if [*entropy injection*]({{< ref "docs/deployment/filesystems/s3" >}}#entropy-injection-for-s3-file-systems) is activated: In that case the savepoint directory will not contain all savepoint data files,
because the injected path entropy spreads the files over many directories. Lacking a common savepoint root directory, the savepoints will contain absolute path references, which prevent moving the directory.

2) The job contains task-owned state, such as `GenericWriteAhreadLog` sink.
{{< /hint >}}

Unlike savepoints, checkpoints cannot generally be moved to a different location, because checkpoints may include some absolute path references.

If you use `statebackend: jobmanager`, metadata *and* savepoint state will be stored in the `_metadata` file, so don't be confused by the absence of additional data files.

{{< hint warning  >}} 
Starting from Flink 1.15 intermediate savepoints (savepoints other than
created with [stop-with-savepoint](#stopping-a-job-with-savepoint)) are not used for recovery and do
not commit any side effects.

This has to be taken into consideration, especially when running multiple jobs in the same
checkpointing timeline. It is possible in that solution that if the original job (after taking a
savepoint) fails, then it will fall back to a checkpoint prior to the savepoint. However, if we now
resume a job from the savepoint, then we might commit transactions that might’ve never happened
because of falling back to a checkpoint before the savepoint (assuming non-determinism).

If one wants to be safe in those scenarios, we advise dropping the state of transactional sinks, by
changing sinks [uids](#assigning-operator-ids).

It should not require any additional steps if there is just a single job running in the same
checkpointing timeline, which means that you stop the original job before running a new job from the
savepoint. 
{{< /hint >}}

#### Savepoint format

You can choose between two binary formats of a savepoint:

* canonical format - a format that has been unified across all state backends, which lets you take a
  savepoint with one state backend and then restore it using another. This is the most stable
  format, that is targeted at maintaining the most compatibility with previous versions, schemas,
  modifications etc.

* native format - the downside of the canonical format is that often it is slow to take and restore
  from. Native format creates a snapshot in the format specific for the used state backend (e.g. SST
  files for RocksDB).

{{< hint info >}}
The possibility to trigger a savepoint in the native format was introduced in Flink 1.15. Up until
then savepoints were created in the canonical format.
{{< /hint >}}

#### Trigger a Savepoint

```shell
$ bin/flink savepoint :jobId [:targetDirectory]
```

This will trigger a savepoint for the job with ID `:jobId`, and returns the path of the created
savepoint. You need this path to restore and dispose savepoints. You can also pass a type in which
the savepoint should be taken. By default the savepoint will be taken in canonical format.

```shell
$ bin/flink savepoint --type [native/canonical] :jobId [:targetDirectory]
```

#### Trigger a Savepoint with YARN

```shell
$ bin/flink savepoint :jobId [:targetDirectory] -yid :yarnAppId
```

This will trigger a savepoint for the job with ID `:jobId` and YARN application ID `:yarnAppId`, and returns the path of the created savepoint.

#### Stopping a Job with Savepoint

```shell
$ bin/flink stop --type [native/canonical] --savepointPath [:targetDirectory] :jobId
```

This will atomically trigger a savepoint for the job with ID `:jobid` and stop the job. Furthermore,
you can specify a target file system directory to store the savepoint in. The directory needs to be
accessible by the JobManager(s) and TaskManager(s). You can also pass a type in which the savepoint
should be taken. By default the savepoint will be taken in canonical format.

### Resuming from Savepoints

```shell
$ bin/flink run -s :savepointPath [:runArgs]
```

This submits a job and specifies a savepoint to resume from. You may give a path to either the savepoint's directory or the `_metadata` file.

#### Allowing Non-Restored State

By default, the resume operation will try to map all state of the savepoint back to the program you are restoring with. If you dropped an operator, you can allow to skip state that cannot be mapped to the new program via `--allowNonRestoredState` (short: `-n`) option:

#### Restore mode

The `Restore Mode` determines who takes ownership of the files  that make up a Savepoint or [externalized checkpoints]({{< ref "docs/ops/state/checkpoints" >}}/#resuming-from-a-retained-checkpoint) after restoring it.
Both savepoints and externalized checkpoints behave similarly in this context. 
Here, they are just called "snapshots" unless explicitely noted otherwise.

As mentioned, the restore mode determines who takes over ownership of the files of the snapshots that we are restoring from. 
Snapshots can be owned either by a user or Flink itself. 
If a snapshot is owned by a user,  Flink will not delete its files, moreover, Flink can not depend on the existence of the files from such a snapshot, as it might be deleted outside of Flink's control. 

Each restore mode serves a specific purposes. 
Still, we believe the default *NO_CLAIM* mode is a good tradeoff in most situations, as it provides clear ownership with a small price for the first checkpoint after the restore.

You can pass the restore mode as:
```shell
$ bin/flink run -s :savepointPath -restoreMode :mode -n [:runArgs]
```

**NO_CLAIM (default)**

In the *NO_CLAIM* mode Flink will not assume ownership of the snapshot. It will leave the files in
user's control and never delete any of the files. In this mode you can start multiple jobs from the
same snapshot.

In order to make sure Flink does not depend on any of the files from that snapshot,
it will force the first (successful) checkpoint to be a full checkpoint as opposed to an incremental one. 
This only makes a difference for `state.backend: rocksdb`, because all other state backends always take full checkpoints.

Once the first full checkpoint completes, all subsequent checkpoints will be taken as usual/configured.
Consequently, once a checkpoint succeeds you can manually delete the original snapshot. You can not do
this earlier, because without any completed checkpoints Flink will - upon failure - try to recover from the initial snapshot.

<div style="text-align: center">
  {{< img src="/fig/restore-mode-no_claim.svg" alt="NO_CLAIM restore mode" width="70%" >}}
</div>

**CLAIM**

The other available mode is the *CLAIM* mode. In this mode Flink claims ownership of the snapshot
and essentially treats it like a checkpoint: its controls the lifecycle and might delete it if it is not needed for recovery anymore.
Hence, it is not safe to manually delete the snapshot or to start two jobs from the same snapshot. 
Flink keeps around
a [configured number]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}}/#state-checkpoints-num-retained)
of checkpoints.

<div style="text-align: center">
  {{< img src="/fig/restore-mode-claim.svg" alt="CLAIM restore mode" width="70%" >}}
</div>

{{< hint info >}}
**Attention:**
1. Retained checkpoints are stored in a path like `<checkpoint_dir>/<job_id>/chk_<x>`. Flink does not
take ownership of the `<checkpoint_dir>/<job_id>` directory, but only the `chk_<x>`. The directory
of the old job will not be deleted by Flink

2. [Native](#savepoint-format) format supports incremental RocksDB savepoints. For those savepoints Flink puts all
SST files inside the savepoints directory. This means such savepoints are self-contained and relocatable.
However, when restored in CLAIM mode, subsequent checkpoints might reuse some SST files, which
in turn might block deleting the savepoints directory at the time the savepoint is subsumed. Later
on Flink will delete the reused shared SST files, but it won't retry deleting the savepoints directory.
Therefore, it is possible Flink leaves an empty savepoints directory if it was restored in CLAIM mode.    
{{< /hint >}}

**LEGACY**

The legacy is mode is how Flink worked until 1.15. In this mode Flink will never delete the initial
checkpoint. At the same time, it is not clear if a user can ever delete it as well. The problem here,
is that Flink might immediately build an incremental checkpoint on top of the restored one. Therefore,
subsequent checkpoints depend on the restored checkpoint. Overall, the ownership is not well-defined.

<div style="text-align: center">
  {{< img src="/fig/restore-mode-legacy.svg" alt="LEGACY restore mode" width="70%" >}}
</div>


### Disposing Savepoints

```shell
$ bin/flink savepoint -d :savepointPath
```

This disposes the savepoint stored in `:savepointPath`.

Note that it is possible to also manually delete a savepoint via regular file system operations without affecting other savepoints or checkpoints (recall that each savepoint is self-contained).

### Configuration

You can configure a default savepoint target directory via the `state.savepoints.dir` key or `StreamExecutionEnvironment`. When triggering savepoints, this directory will be used to store the savepoint. You can overwrite the default by specifying a custom target directory with the trigger commands (see the [`:targetDirectory` argument](#trigger-a-savepoint)).

{{< tabs "config" >}}
{{< tab "flink-conf.yaml" >}}
```yaml
# Default savepoint target directory
state.savepoints.dir: hdfs:///flink/savepoints
```
{{< /tab >}}
{{< tab "Java" >}}
```java
env.setDefaultSavepointDir("hdfs:///flink/savepoints");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
env.setDefaultSavepointDir("hdfs:///flink/savepoints")
```
{{< /tab >}}
{{< /tabs >}}

If you neither configure a default nor specify a custom target directory, triggering the savepoint will fail.

{{< hint warning >}}
The target directory has to be a location accessible by both the JobManager(s) and TaskManager(s) e.g. a location on a distributed file-system.
{{< /hint >}}

## F.A.Q

### Should I assign IDs to all operators in my job?

As a rule of thumb, yes. Strictly speaking, it is sufficient to only assign IDs via the `uid` method to the stateful operators in your job. The savepoint only contains state for these operators and stateless operator are not part of the savepoint.

In practice, it is recommended to assign it to all operators, because some of Flink's built-in operators like the Window operator are also stateful and it is not obvious which built-in operators are actually stateful and which are not. If you are absolutely certain that an operator is stateless, you can skip the `uid` method.

### What happens if I add a new operator that requires state to my job?

When you add a new operator to your job, it will be initialized without any state. Savepoints contain the state of each stateful operator. Stateless operators are simply not part of the savepoint. The new operator behaves similar to a stateless operator.

### What happens if I delete an operator that has state from my job?

By default, a savepoint restore will try to match all state back to the restored job. If you restore from a savepoint that contains state for an operator that has been deleted, this will therefore fail. 

You can allow non restored state by setting the `--allowNonRestoredState` (short: `-n`) with the run command:

```shell
$ bin/flink run -s :savepointPath -n [:runArgs]
```

### What happens if I reorder stateful operators in my job?

If you assigned IDs to these operators, they will be restored as usual.

If you did not assign IDs, the auto generated IDs of the stateful operators will most likely change after the reordering. This would result in you not being able to restore from a previous savepoint.

### What happens if I add or delete or reorder operators that have no state in my job?

If you assigned IDs to your stateful operators, the stateless operators will not influence the savepoint restore.

If you did not assign IDs, the auto generated IDs of the stateful operators will most likely change after the reordering. This would result in you not being able to restore from a previous savepoint.

### What happens when I change the parallelism of my program when restoring?

You can simply restore the program from a savepoint and specify a new parallelism.

### Can I move the Savepoint files on stable storage?

The quick answer to this question is currently "yes". Savepoints are self-contained and relocatable. You can move the file and restore from any location.

{{< top >}}
