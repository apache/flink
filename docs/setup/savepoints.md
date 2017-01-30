---
title: "Savepoints"
nav-parent_id: setup
nav-pos: 8
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

* toc
{:toc}

## Overview

Savepoints are externally stored checkpoints that you can use to stop-and-resume or update your Flink programs. They use Flink's [checkpointing mechanism]({{ site.baseurl }}/internals/stream_checkpointing.html) to create a snapshot of the state of your streaming program and write the checkpoint meta data out to an external file system.

This page covers all steps involved in triggering, restoring, and disposing savepoints. In order to allow upgrades between programs and Flink versions, it is important to check out the section about [assigning IDs to your operators](#assigning-operator-ids).

For more details on how Flink handles state and failures in general, check out the [State in Streaming Programs]({{ site.baseurl }}/dev/stream/state.html) page.

## Assigning Operator IDs

It is **highly recommended** that you adjust your programs as described in this section in order to be able to upgrade your programs in the future. The main required change is to manually specify operator IDs via the **`uid(String)`** method. These IDs are used to scope the state of each operator.

{% highlight java %}
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
{% endhighlight %}

If you don't specify the IDs manually they will be generated automatically. You can automatically restore from the savepoint as long as these IDs do not change. The generated IDs depend on the structure of your program and are sensitive to program changes. Therefore, it is highly recommended to assign these IDs manually.

### Savepoint State

You can think of a savepoint as holding a map of `Operator ID -> State` for each stateful operator:

```
Operator ID | State
------------+------------------------
source-id   | State of StatefulSource
mapper-id   | State of StatefulMapper
```

In the above example, the print sink is stateless and hence not part of the savepoint state. By default, we try to map each entry of the savepoint back to the new program.

## Operations

You can use the [command line client]({{ site.baseurl }}/setup/cli.html#savepoints) to *trigger savepoints*, *cancel a job with a savepoint*, *resume from savepoints*, and *dispose savepoints*.

### Triggering Savepoints

When triggering a savepoint, a single savepoint file will be created that contains the checkpoint *meta data*. The actual checkpoint state will be kept around in the configured checkpoint directory. For example with a `FsStateBackend` or `RocksDBStateBackend`:

```sh
# Savepoint file contains the checkpoint meta data
/savepoints/savepoint-123123

# Checkpoint directory contains the actual state
/checkpoints/:jobid/chk-:id/...
```

The savepoint file is usually much smaller than the actual checkpointed state. Note that if you use the `MemoryStateBackend`, the savepoint file will be self-contained and contain all the state.

#### Trigger a Savepoint

```sh
$ bin/flink savepoint :jobId [:targetDirectory]
```

This will trigger a savepoint for the job with ID `:jobid`. Furthermore, you can specify a target file system directory to store the savepoint in. The directory needs to be accessible by the JobManager.

If you don't specify a target directory, you need to have [configured a default directory](#configuration). Otherwise, triggering the savepoint will fail.

#### Cancel Job with Savepoint

```sh
$ bin/flink cancel -s [:targetDirectory] :jobId
```

This will atomically trigger a savepoint for the job with ID `:jobid` and cancel the job. Furthermore, you can specify a target file system directory to store the savepoint in.  The directory needs to be accessible by the JobManager.

If you don't specify a target directory, you need to have [configured a default directory](#configuration). Otherwise, cancelling the job with a savepoint will fail.

### Resuming from Savepoints

```sh
$ bin/flink run -s :savepointPath [:runArgs]
```

This submits a job and specifies the savepoint path. The execution will resume from the respective savepoint state. The savepoint file holds the meta data of a checkpoint and points to the actual checkpoint files. This is why the savepoint file is usually much smaller than the actual checkpoint state.

#### Allowing Non-Restored State

By default the resume operation will try to map all state of the savepoint back to the program you are restoring with. If you dropped an operator, you can allow to skip state that cannot be mapped to the new program via `--allowNonRestoredState` (short: `-n`) option:

```sh
$ bin/flink run -s :savepointPath -n [:runArgs]
```

### Disposing Savepoints

```sh
$ bin/flink savepoint -d :savepointPath
```

This disposes the savepoint stored in `:savepointPath`.

Note that since savepoints always go to a file system it is possible to also manually delete the savepoint via a regular file system operation. Keep in mind though that the savepoint only stores meta data that points to the actual checkpoint data. Therefore, if you manually want to delete a savepoint, you would have to include the checkpoint files as well. Since there is currently no straight forward way to figure out how a savepoint maps to a checkpoint, it is recommended to use the savepoint tool for this as described above.

### Configuration

You can configure a default savepoint target directory via the `state.savepoints.dir` key. When triggering savepoints, this directory will be used to store the savepoint meta data. You can overwrite the default by specifying a custom target directory with the trigger commands (see the [`:targetDirectory` argument](#trigger-a-savepoint)).

```sh
# Default savepoint target directory
state.savepoints.dir: hdfs:///flink/savepoints
```

If you neither configure a default nor specify a custom target directory, triggering the savepoint will fail.

## F.A.Q

### Should I assign IDs to all operators in my job?

As a rule of thumb, yes. Strictly speaking, it is sufficient to only assign IDs via the `uid` method to the stateful operators in your job. The savepoint only contains state for these operators and stateless operator are not part of the savepoint.

In practice, it is recommended to assign it to all operators, because some of Flink's built-in operators like the Window operator are also stateful and it is not obvious which built-in operators are actually stateful and which are not. If you are absolutely certain that an operator is stateless, you can skip the `uid` method.

### Why is the savepoint file so small?

The savepoint file only contains the meta data of the checkpoint and has pointers to the checkpoint state, which is usually much larger. In case of using the `MemoryStateBackend`, the checkpoint will include all state, but is constrained by the backend to small state.

### What happens if I add a new operator that requires state to my job?

When you add a new operator to your job it will be initialized without any state. Savepoints contain the state of each stateful operator. Stateless operators are simply not part of the savepoint. The new operator behaves similar to a stateless operator.

### What happens if I delete an operator that has state from my job?

By default, a savepoint restore will try to match all state back to the restored job. If you restore from a savepoint that contains state for an operator that has been deleted, this will therefore fail. 

You can allow non restored state by setting the `--allowNonRestoredState` (short: `-n`) with the run command:

```sh
$ bin/flink run -s :savepointPath -n [:runArgs]
```

### What happens if I reorder stateful operators in my job?

If you assigned IDs to these operators, they will be restored as usual.

If you did not assign IDs, the auto generated IDs of the stateful operators will most likely change after the reordering. This would result in you not being able to restore from a previous savepoint.

### What happens if I add or delete or reorder operators that have no state in my job?

If you assigned IDs to your stateful operators, the stateless operators will not influence the savepoint restore.

If you did not assign IDs, the auto generated IDs of the stateful operators will most likely change after the reordering. This would result in you not being able to restore from a previous savepoint.

### What happens when I change the parallelism of my program when restoring?

If the savepoint was triggered with Flink >= 1.2.0 and using no deprecated state API like `Checkpointed`, you can simply restore the program from a savepoint and specify a new parallelism.

If you are resuming from a savepoint triggered with Flink < 1.2.0 or using now deprecated APIs you first have to migrate your job and savepoint to Flink 1.2.0 before being able to change the parallelism. See the [upgrading jobs and Flink versions guide]({{ site.baseurl }}/ops/upgrading.html).

## Current limitations

- **Chaining**: Chained operators are identified by the ID of the first task. It's not possible to manually assign an ID to an intermediate chained task, e.g. in the chain `[  a -> b -> c ]` only **a** can have its ID assigned manually, but not **b** or **c**. To work around this, you can [manually define the task chains](index.html#task-chaining-and-resource-groups). If you rely on the automatic ID assignment, a change in the chaining behaviour will also change the IDs.

