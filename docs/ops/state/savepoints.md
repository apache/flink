---
title: "Savepoints"
nav-parent_id: ops_state
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

## What is a Savepoint? How is a Savepoint different from a Checkpoint?

A Savepoint is a consistent image of the execution state of a streaming job, created via Flink's [checkpointing mechanism]({{ site.baseurl }}/training/fault_tolerance.html). You can use Savepoints to stop-and-resume, fork,
or update your Flink jobs. Savepoints consist of two parts: a directory with (typically large) binary files on stable storage (e.g. HDFS, S3, ...) and a (relatively small) meta data file. The files on stable storage represent the net data of the job's execution state
image. The meta data file of a Savepoint contains (primarily) pointers to all files on stable storage that are part of the Savepoint, in form of absolute paths.

<div class="alert alert-warning">
<strong>Attention:</strong> In order to allow upgrades between programs and Flink versions, it is important to check out the following section about <a href="#assigning-operator-ids">assigning IDs to your operators</a>.
</div>

Conceptually, Flink's Savepoints are different from Checkpoints in a similar way that backups are different from recovery logs in traditional database systems. The primary purpose of Checkpoints is to provide a recovery mechanism in case of
unexpected job failures. A Checkpoint's lifecycle is managed by Flink, i.e. a Checkpoint is created, owned, and released by Flink - without user interaction. As a method of recovery and being periodically triggered, two main
design goals for the Checkpoint implementation are i) being as lightweight to create and ii) being as fast to restore from as possible. Optimizations towards those goals can exploit certain properties, e.g. that the job code
doesn't change between the execution attempts. Checkpoints are usually dropped after the job was terminated by the user (except if explicitly configured as retained Checkpoints).

In contrast to all this, Savepoints are created, owned, and deleted by the user. Their use-case is for planned, manual backup and resume. For example, this could be an update of your Flink version, changing your job graph,
changing parallelism, forking a second job like for a red/blue deployment, and so on. Of course, Savepoints must survive job termination. Conceptually, Savepoints can be a bit more expensive to produce and restore and focus
more on portability and support for the previously mentioned changes to the job.

Those conceptual differences aside, the current implementations of Checkpoints and Savepoints are basically using the same code and produce the same format. However, there is currently one exception from this, and we might
introduce more differences in the future. The exception are incremental checkpoints with the RocksDB state backend. They are using some RocksDB internal format instead of Flink’s native savepoint format. This makes them the
first instance of a more lightweight checkpointing mechanism, compared to Savepoints.

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

{% highlight plain %}
Operator ID | State
------------+------------------------
source-id   | State of StatefulSource
mapper-id   | State of StatefulMapper
{% endhighlight %}

In the above example, the print sink is stateless and hence not part of the savepoint state. By default, we try to map each entry of the savepoint back to the new program.

## Operations

You can use the [command line client]({{ site.baseurl }}/ops/cli.html#savepoints) to *trigger savepoints*, *cancel a job with a savepoint*, *resume from savepoints*, and *dispose savepoints*.

With Flink >= 1.2.0 it is also possible to *resume from savepoints* using the webui.

### Triggering Savepoints

When triggering a savepoint, a new savepoint directory is created where the data as well as the meta data will be stored. The location of this directory can be controlled by [configuring a default target directory](#configuration) or by specifying a custom target directory with the trigger commands (see the [`:targetDirectory` argument](#trigger-a-savepoint)).

<div class="alert alert-warning">
<strong>Attention:</strong> The target directory has to be a location accessible by both the JobManager(s) and TaskManager(s) e.g. a location on a distributed file-system.
</div>

For example with a `FsStateBackend` or `RocksDBStateBackend`:

{% highlight shell %}
# Savepoint target directory
/savepoints/

# Savepoint directory
/savepoints/savepoint-:shortjobid-:savepointid/

# Savepoint file contains the checkpoint meta data
/savepoints/savepoint-:shortjobid-:savepointid/_metadata

# Savepoint state
/savepoints/savepoint-:shortjobid-:savepointid/...
{% endhighlight %}

<div class="alert alert-info">
  <strong>Note:</strong>
Although it looks as if the savepoints may be moved, it is currently not possible due to absolute paths in the <code>_metadata</code> file.
Please follow <a href="https://issues.apache.org/jira/browse/FLINK-5778">FLINK-5778</a> for progress on lifting this restriction.
</div>

Note that if you use the `MemoryStateBackend`, metadata *and* savepoint state will be stored in the `_metadata` file. Since it is self-contained, you may move the file and restore from any location.

<div class="alert alert-warning">
  <strong>Attention:</strong> It is discouraged to move or delete the last savepoint of a running job, because this might interfere with failure-recovery. Savepoints have side-effects on exactly-once sinks, therefore 
  to ensure exactly-once semantics, if there is no checkpoint after the last savepoint, the savepoint will be used for recovery. 
</div>

#### Trigger a Savepoint

{% highlight shell %}
$ bin/flink savepoint :jobId [:targetDirectory]
{% endhighlight %}

This will trigger a savepoint for the job with ID `:jobId`, and returns the path of the created savepoint. You need this path to restore and dispose savepoints.

#### Trigger a Savepoint with YARN

{% highlight shell %}
$ bin/flink savepoint :jobId [:targetDirectory] -yid :yarnAppId
{% endhighlight %}

This will trigger a savepoint for the job with ID `:jobId` and YARN application ID `:yarnAppId`, and returns the path of the created savepoint.

#### Cancel Job with Savepoint

{% highlight shell %}
$ bin/flink cancel -s [:targetDirectory] :jobId
{% endhighlight %}

This will atomically trigger a savepoint for the job with ID `:jobid` and cancel the job. Furthermore, you can specify a target file system directory to store the savepoint in.  The directory needs to be accessible by the JobManager(s) and TaskManager(s).

### Resuming from Savepoints

{% highlight shell %}
$ bin/flink run -s :savepointPath [:runArgs]
{% endhighlight %}

This submits a job and specifies a savepoint to resume from. You may give a path to either the savepoint's directory or the `_metadata` file.

#### Allowing Non-Restored State

By default the resume operation will try to map all state of the savepoint back to the program you are restoring with. If you dropped an operator, you can allow to skip state that cannot be mapped to the new program via `--allowNonRestoredState` (short: `-n`) option:

{% highlight shell %}
$ bin/flink run -s :savepointPath -n [:runArgs]
{% endhighlight %}

### Disposing Savepoints

{% highlight shell %}
$ bin/flink savepoint -d :savepointPath
{% endhighlight %}

This disposes the savepoint stored in `:savepointPath`.

Note that it is possible to also manually delete a savepoint via regular file system operations without affecting other savepoints or checkpoints (recall that each savepoint is self-contained). Up to Flink 1.2, this was a more tedious task which was performed with the savepoint command above.

### Configuration

You can configure a default savepoint target directory via the `state.savepoints.dir` key. When triggering savepoints, this directory will be used to store the savepoint. You can overwrite the default by specifying a custom target directory with the trigger commands (see the [`:targetDirectory` argument](#trigger-a-savepoint)).

{% highlight yaml %}
# Default savepoint target directory
state.savepoints.dir: hdfs:///flink/savepoints
{% endhighlight %}

If you neither configure a default nor specify a custom target directory, triggering the savepoint will fail.

<div class="alert alert-warning">
<strong>Attention:</strong> The target directory has to be a location accessible by both the JobManager(s) and TaskManager(s) e.g. a location on a distributed file-system.
</div>

## F.A.Q

### Should I assign IDs to all operators in my job?

As a rule of thumb, yes. Strictly speaking, it is sufficient to only assign IDs via the `uid` method to the stateful operators in your job. The savepoint only contains state for these operators and stateless operator are not part of the savepoint.

In practice, it is recommended to assign it to all operators, because some of Flink's built-in operators like the Window operator are also stateful and it is not obvious which built-in operators are actually stateful and which are not. If you are absolutely certain that an operator is stateless, you can skip the `uid` method.

### What happens if I add a new operator that requires state to my job?

When you add a new operator to your job it will be initialized without any state. Savepoints contain the state of each stateful operator. Stateless operators are simply not part of the savepoint. The new operator behaves similar to a stateless operator.

### What happens if I delete an operator that has state from my job?

By default, a savepoint restore will try to match all state back to the restored job. If you restore from a savepoint that contains state for an operator that has been deleted, this will therefore fail. 

You can allow non restored state by setting the `--allowNonRestoredState` (short: `-n`) with the run command:

{% highlight shell %}
$ bin/flink run -s :savepointPath -n [:runArgs]
{% endhighlight %}

### What happens if I reorder stateful operators in my job?

If you assigned IDs to these operators, they will be restored as usual.

If you did not assign IDs, the auto generated IDs of the stateful operators will most likely change after the reordering. This would result in you not being able to restore from a previous savepoint.

### What happens if I add or delete or reorder operators that have no state in my job?

If you assigned IDs to your stateful operators, the stateless operators will not influence the savepoint restore.

If you did not assign IDs, the auto generated IDs of the stateful operators will most likely change after the reordering. This would result in you not being able to restore from a previous savepoint.

### What happens when I change the parallelism of my program when restoring?

If the savepoint was triggered with Flink >= 1.2.0 and using no deprecated state API like `Checkpointed`, you can simply restore the program from a savepoint and specify a new parallelism.

If you are resuming from a savepoint triggered with Flink < 1.2.0 or using now deprecated APIs you first have to migrate your job and savepoint to Flink >= 1.2.0 before being able to change the parallelism. See the [upgrading jobs and Flink versions guide]({{ site.baseurl }}/ops/upgrading.html).

### Can I move the Savepoint files on stable storage?

The quick answer to this question is currently "no" because the meta data file references the files on stable storage as absolute paths for technical reasons. The longer answer is: if you MUST move the files for some reason there are two
potential approaches as workaround. First, simpler but potentially more dangerous, you can use an editor to find the old path in the meta data file and replace them with the new path. Second, you can use the class
SavepointV2Serializer as starting point to programmatically read, manipulate, and rewrite the meta data file with the new paths.

{% top %}
