---
title: "Checkpoints"
nav-parent_id: ops_state
nav-pos: 7
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

Checkpoints make state in Flink fault tolerant by allowing state and the
corresponding stream positions to be recovered, thereby giving the application
the same semantics as a failure-free execution.

See [Checkpointing]({{ site.baseurl }}/dev/stream/state/checkpointing.html) for how to enable and
configure checkpoints for your program.

## Externalized Checkpoints

Checkpoints are by default not persisted externally and are only used to
resume a job from failures. They are deleted when a program is cancelled.
You can, however, configure periodic checkpoints to be persisted externally
similarly to [savepoints](savepoints.html). These *externalized checkpoints*
write their meta data out to persistent storage and are *not* automatically
cleaned up when the job fails. This way, you will have a checkpoint around
to resume from if your job fails.

```java
CheckpointConfig config = env.getCheckpointConfig();
config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
```

The `ExternalizedCheckpointCleanup` mode configures what happens with externalized checkpoints when you cancel the job:

- ```ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION```: Retain the externalized checkpoint when the job is cancelled. Note that you have to manually clean up the checkpoint state after cancellation in this case.

- ```ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION```: Delete the externalized checkpoint when the job is cancelled. The checkpoint state will only be available if the job fails.

### Directory Structure

Similarly to [savepoints](savepoints.html), an externalized checkpoint consists
of a meta data file and, depending on the state back-end, some additional data
files. The ``target directory`` for the externalized checkpoint's meta data is
determined from the configuration key `state.checkpoints.dir` which, currently,
can only be set via the configuration files.

```
state.checkpoints.dir: hdfs:///checkpoints/
```

This directory will then contain the checkpoint meta data required to restore
the checkpoint. For the `MemoryStateBackend`, this meta data file will be
self-contained and no further files are needed.

`FsStateBackend` and `RocksDBStateBackend` write separate data files
and only write the paths to these files into the meta data file. These data
files are stored at the path given to the state back-end during construction.

```java
env.setStateBackend(new RocksDBStateBackend("hdfs:///checkpoints-data/");
```

### Difference to Savepoints

Externalized checkpoints have a few differences from [savepoints](savepoints.html). They
- use a state backend specific (low-level) data format,
- may be incremental,
- do not support Flink specific features like rescaling.

### Resuming from an externalized checkpoint

A job may be resumed from an externalized checkpoint just as from a savepoint
by using the checkpoint's meta data file instead (see the
[savepoint restore guide](../cli.html#restore-a-savepoint)). Note that if the
meta data file is not self-contained, the jobmanager needs to have access to
the data files it refers to (see [Directory Structure](#directory-structure)
above).

```sh
$ bin/flink run -s :checkpointMetaDataPath [:runArgs]
```

## Incremental Checkpoints

### Synopsis

Incremental checkpoints can significantly reduce checkpointing time in comparison to full checkpoints, at the cost of a
(potentially) longer recovery time. The core idea is that incremental checkpoints only record changes in state since the
previously-completed checkpoint instead of producing a full, self-contained backup of the state backend. In this way,
incremental checkpoints can build upon previous checkpoints.

RocksDBStateBackend is currently the only backend that supports incremental checkpoints.

Flink leverages RocksDB's internal backup mechanism in a way that is self-consolidating over time. As a result, the
incremental checkpoint history in Flink does not grow indefinitely, and old checkpoints are eventually subsumed and
pruned automatically.

``While we strongly encourage the use of incremental checkpoints for Flink jobs with large state, please note that this is
a new feature and currently not enabled by default``.

To enable this feature, users can instantiate a `RocksDBStateBackend` with the corresponding boolean flag in the
constructor set to `true`, e.g.:

```java
   RocksDBStateBackend backend =
       new RocksDBStateBackend(filebackend, true);
```

### Use-case for Incremental Checkpoints

Checkpoints are the centrepiece of Flink’s fault tolerance mechanism and each checkpoint represents a consistent
snapshot of the distributed state of a Flink job from which the system can recover in case of a software or machine
failure (see [here]({{ site.baseurl }}/internals/stream_checkpointing.html). 

Flink creates checkpoints periodically to track the progress of a job so that, in case of failure, only those
(hopefully few) *events that have been processed after the last completed checkpoint* must be reprocessed from the data
source. The number of events that must be reprocessed has implications for recovery time, and so for fastest recovery,
we want to *take checkpoints as often as possible*.

However, checkpoints are not without performance cost and can introduce *considerable overhead* to the system. This
overhead can lead to lower throughput and higher latency during the time that checkpoints are created. One reason is
that, traditionally, each checkpoint in Flink always represented the *complete state* of the job at the time of the
checkpoint, and all of the state had to be written to stable storage (typically some distributed file system) for every
single checkpoint. Writing multiple terabytes (or more) of state data for each checkpoint can obviously create
significant load for the I/O and network subsystems, on top of the normal load from pipeline’s data processing work.

Before incremental checkpoints, users were stuck with a suboptimal tradeoff between recovery time and checkpointing
overhead. Fast recovery and low checkpointing overhead were conflicting goals. And this is exactly the problem that
incremental checkpoints solve.


### Basics of Incremental Checkpoints

In this section, for the sake of getting the concept across, we will briefly discuss the idea behind incremental
checkpoints in a simplified manner.

Our motivation for incremental checkpointing stemmed from the observation that it is often wasteful to write the full
state of a job for every single checkpoint. In most cases, the state between two checkpoints is not drastically
different, and only a fraction of the state data is modified and some new data added. Instead of writing the full state
into each checkpoint again and again, we could record only changes in state since the previous checkpoint. As long as we
have the previous checkpoint and the state changes for the current checkpoint, we can restore the full, current state
for the job. This is the basic principle of incremental checkpoints, that each checkpoint can build upon a history of
previous checkpoints to avoid writing redundant information.

Figure 1 illustrates the basic idea of incremental checkpointing in comparison to full checkpointing.

The state of the job evolves over time and for checkpoints ``CP 1`` to ``CP 2``, a full checkpoint is simply a copy of the whole
state.

<p class="text-center">
   <img alt="Figure 1: Full Checkpoints vs Incremental Checkpoints" width="80%" src="{{ site.baseurl }}/fig/incremental_cp_basic.svg"/>
</p>

With incremental checkpointing, each checkpoint contains only the state change since the previous checkpoint.

* For the first checkpoint ``CP 1``, there is no difference between a full checkpoint and the complete state at the time the
checkpoint is written.

* For ``CP 2``, incremental checkpointing will write only the changes since ``CP 1``: the value for ``K1`` has changed and a mapping
for ``K3`` was added.

* For checkpoint ``CP 3``, incremental checkpointing only records the changes since ``CP 2``.

Notice that, unlike in full checkpoints, we also must record changes that delete state in an incremental checkpoint, as
in the case of ``K0``. In this simple example, we can see how incremental checkpointing can reduce the amount of data that
is written for each checkpoint.

The next interesting question: how does restoring from incremental checkpoints compare to restoring from full
checkpoints? Restoring a full checkpoint is as simple as loading all the data from the checkpoint back into the job
state because full checkpoints are self-contained. In contrast, to restore an incremental checkpoint, we need to replay
the history of all incremental checkpoints that are in the reference chain of the checkpoint we are trying to restore.
In our example from Figure 1, those connections are represented by the orange arrows. If we want to restore ``CP 3``, as a
first step, we need to apply all the changes of ``CP 1`` to the empty initial job state. On top of that, we apply the
changes from ``CP 2``, and then the changes from ``CP 3``.

A different way to think about basic incremental checkpoints is to imagine it as a changelog with some aggregation. What
we mean by aggregated is that for example, if the state under key ``K1`` is overwritten multiple times between two
consecutive checkpoints, we will only record the latest state value at the time in the checkpoint. All previous changes
are thereby subsumed.

This leads us to the discussion of the potential *disadvantages* of incremental checkpoints compared to full checkpoints.
While we save work in writing checkpoints, we have to do more work in reading the data from multiple checkpoints on
recovery. Furthermore, we can no longer simply delete old checkpoints because new checkpoints rely upon them and the
history of old checkpoints can grow indefinitely over time (like a changelog).

At this point, it looks like we didn’t gain too much from incremental checkpoints because we are again trading between
checkpointing overhead and recovery time. Fortunately, there are ways to improve on this naive approach to recovery. One
simple and obvious way to restrict recovery time and the length of the checkpoint history is to write a full checkpoint
from time to time. We can drop all checkpoints prior to the most recent full checkpoint, and the full checkpoint can
serve as a new basis for future incremental checkpoints.

Our actual implementation of incremental checkpoints in Flink is more involved and designed to address a number of
different tradeoffs. Our incremental checkpointing restricts the size of the checkpoint history and therefore never
needs take a full checkpoint to keep recovery efficiently! We present more detail about this in the next section, but
the high level idea is to accept a small amount of redundant state writing to incrementally introduce
merged/consolidated replacements for previous checkpoints. For now, you can think about Flink’s approach as stretching
out and distributing the consolidation work over several incremental checkpoints, instead of doing it all at once in a
full checkpoint. Every incremental checkpoint can contribute a share for consolidation. We also track when old
checkpoints data becomes obsolete and then prune the checkpoint history over time.

### Incremental Checkpoints in Flink

In the previous section, we discussed that incremental checkpointing is mainly about recording all effective state
modifications between checkpoints. This poses certain requirements on the underlying data structures in the state
backend that manages the job’s state. It goes without saying that the data structure should always provide a decent
read-write performance to keep state access swift. At the same time, for incremental checkpointing, the state backend
must be able to efficiently detect and iterate state modifications since the previous checkpoint.

One data structure that is very well-suited for this use case is the *log-structured-merge (LSM) tree* that is the core
data structure in Flink’s RocksDB-based state backend. Without going into too much detail, the write path of RocksDB
already roughly resembles a changelog with some pre-aggregation — which perfectly fits the needs of incremental
checkpoints. On top of that, RocksDB also has a *compaction mechanism* can be regarded as an elaborated form of log
compaction.

#### RocksDB Snapshots as a Foundation

In a nutshell, *RocksDB is a key-value store based on LSM trees*. The write path of RocksDB first collects all changes as
key-value pairs in a mutable, in-memory buffer called memtable. Writes to the same key in a memtable can simply replace
previous values (this is the pre-aggregation aspect). Once the memtable is full, it is written to disk completely with
all entries sorted by their key. Typically, RocksDB also applies a lightweight compression (e.g. snappy) in the write
process. After the memtable was written to disk, it becomes immutable and is now called a *sorted-string-table
(sstable)*. Figure 2 illustrates these basic RocksDB internals.

<p class="text-center">
   <img alt="Figure 2: RocksDB architecture (simplified)" width="75%" src="{{ site.baseurl }}/fig/rocksdb_architecture_simple.png"/>
</p>

To avoid the problem of collecting an infinite number of sstables over time, a background task called compaction is
constantly merging sstables to consolidate potential duplicate entries for each key from the merged tables. Once some
sstables have been merged, those original sstables become obsolete and are deleted by RocksDB. The newly created merged
sstable contains all their net information. We show an example for such a merge in Figure 3. SSTable-1 and SStable-2
contain some duplicate mappings for certain keys, such as key ``9``. The system can apply a sort-merge strategy in which
the newer mappings from ``SSTable-2`` overwrite mappings for keys that also existed in ``SSTable-1``. For key ``7``, we can also
see a delete (or antimatter) entry that, when merged, results in omitting key ``7`` in the merge result. Notice that the
merge in RocksDB is typically generalised to a multi-way merge. We won’t go into details about the read path here,
because it is irrelevant for the approach that we want to present. You can find more details about RocksDB internals in
their [documentation](http://rocksdb.org/).

<p class="text-center">
   <img alt="Figure 3: Merging SSTable files" width="50%" src="{{ site.baseurl }}/fig/sstable_merge.png"/>
</p>

#### Integrating RocksDB’s Snapshots with Flink’s Checkpoints

Flink’s incremental checkpointing logic operates on top of this mechanism that RocksDB provides. From a high-level
perspective, when taking a checkpoint, we track which sstable files have been created and deleted by RocksDB since the
previous checkpoint. This is sufficient for figuring out the effective state changes because sstables are immutable. Our
backend remembers the sstables that already existed in the last completed checkpoint in order to figure out which files
have been created or deleted in the current checkpoint interval. With this in mind, we will now explain the details of
checkpointing state in our RocksDB backend.

In the first step, Flink triggers a flush in RocksDB so that all all memtables are forced into sstables on disk, and all
sstables are hard-linked in a local temporary directory. This step of the checkpoint is synchronous to the processing
pipeline, and all further steps are performed asynchronously and will not block processing.

Then, all new sstables (w.r.t. the previous checkpoint) are copied to stable storage (e.g. HDFS) and referenced in the
new checkpoint. All sstables that already existed in the previous checkpoint will *not be copied again to stable
storage* but simply re-referenced. Deleted files will simply no longer receive a reference in the new checkpoint. Notice
that deleted sstables in RocksDB are always the result of compaction. This is the way in which Flink’s incremental
checkpoints can prune the checkpoint history. Old sstables are eventually replaced by the sstable that is the result of
merging them. Note that in a strict sense of tracking changes between checkpoints, this uploading of consolidated tables
is redundant work. But it is performed incrementally, typically adding only a small amount of overhead to some
checkpoints. However, we absolutely consider that overhead to be a worthwhile investment because it allows us to keep a
shorter history of checkpoints to consider in a recovery.

Another interesting point is how Flink can determine when it is safe to delete a shared file. Our solution works as
follows: for each file, we keep a reference count for each sstable file that we copied to stable storage. These counts
are maintained by the checkpoint coordinator on the job master in a *shared state registry*. This shared registry tracks
the number of checkpoints that reference a shared file in stable storage, e.g. an uploaded sstable. When a checkpoint is
completed, the checkpoint coordinator simply increases the counts for all files that are referenced in the new
checkpoint by 1. If a checkpoint is dropped, the count of all files it has referenced is decreased by 1. When the count
goes down to 0, the shared file is deleted from stable storage because it is no longer used by any checkpoint.

<p class="text-center">
   <img alt="Figure 4: Flink incremental checkpointing example" width="100%" src="{{ site.baseurl }}/fig/incremental_cp_impl_example.svg"/>
</p>

To make this idea a bit more complete, see Figure 4, where we show an example run over 4 incremental checkpoints to make
things a bit more concrete. We illustrate what is happening for one subtask (here: subtask index 1) of one operator
(called ``Operator-2``) with keyed state. Furthermore, for this example we assume that the number of retained
checkpoints is configured to 2, so that Flink will always keep the two latest checkpoints and older checkpoints are
pruned. The columns show, for each checkpoint, the state of the local RocksDB instance (i.e. the current sstable files),
the files that are referenced in the checkpoint, and the counts in the shared state registry after the checkpoint is
completed. For checkpoint 1 (``CP 1``)), we can see that the local RocksDB directory contains two sstable files, which
are considered as new and uploaded to stable storage. We upload the files under the checkpoint directory of the
corresponding checkpoint that first uploaded them, in this case ``cp-1``, and use unique filenames because they could
otherwise collide with identical sstable names from other subtasks. When the checkpoint completes, the two entries are
created in the shared state registry, one for each newly uploaded file, and their counts are set to 1. Notice that the
key in the shared state registry is a composite of operator, subtask, and the original sstable file name. In the actual
implementation, the shared state registry also keeps a mapping from the key to the file path in stable storage besides
the count, which is not shown to keep the graphic clearer.

At the time of the second checkpoint, two new sstable files have been created by RocksDB and the two older sstable files
from the previous checkpoint also still exist. For checkpoint ``CP 2``, Flink must now upload the two new files to
stable storage and can reference the ``sstable-(1)`` and ``sstable-(2)`` from the previous checkpoint. We can see that
the file references to previously existing sstable files point to existing files in the ``cp-1`` directory and 
references to new sstable files point to the newly uploaded files in directory ``cp-2``. When the checkpoint completes,
the counts for all referenced files are increased by 1.

For checkpoint ``CP 3``, we see that RocksDB’s compaction has merged ``sstable-(1)``, ``sstable-(2)``, and 
``sstable-(3)`` into ``sstable-(1,2,3)``. This merged table contains the same net information as the source files and
eliminates any duplicate entries for each key that might have existed across the three source files. The source files of
the merge have been deleted, ``sstable-(4)`` still exists, and one additional ``sstable-(5)`` was created. For the 
checkpoint, we need to upload the new files ``sstable-(1,2,3)`` and ``sstable-(5)`` and can re-reference ``sstable-(4)``
from a previous checkpoint. When this checkpoint completes, two things will happen at the checkpoint coordinator, in the
following order:

* First, the checkpoint registers the referenced files, increasing the count of those files by 1.

* Then, the older checkpoint ``CP 1`` will be deleted because we have configured the number of retained checkpoints to
two.

* As part of the deletion, the counts for all files referenced by ``CP 1``, (``sstable-(1)`` and ``sstable-(2)``), is
decreased by 1.

Even though ``CP 1`` is logically deleted, we can see that all the files it created have still a reference count greater 
0 and we cannot yet physically delete them from stable storage. Our graphic shows the counts after ``CP 3`` was
registered and ``CP 1`` was deleted.

For our last checkpoint ``CP 4``, RocksDB has merged ``sstable-(4)``, ``sstable-(5)``, and another ``sstable-(6)``,
which was never observed at the time of a checkpoint, into ``sstable-(4,5,6)``. This file ``sstable-(4,5,6)`` is new for
the checkpoint, and must be uploaded. We reference it together with ``sstable-(1,2,3)`` that was already known in
``CP 4``. In the checkpoint coordinator’s shared state registry, the counts for ``sstable-(1,2,3)`` and
``sstable-(4,5,6)`` are increased by 1. Then, ``CP 2`` is deleted as part of our retention policy. This decreases the
counts for ``sstable-(1)``, ``sstable-(2)``, ``sstable-(3)``, and ``sstable-(4)`` by 1. This means that the counts for
``sstable-(1)``, ``sstable-(2)``, and ``sstable-(3)`` have now dropped to 0 and they will be physically deleted from the
stable storage. The final counts for ``CP 4`` after this step  are shown in the figure. This concludes our example for a
sequence of 4 incremental checkpoints.

#### Resolving Races for Concurrent Checkpoints

We sometimes also have to resolve race conditions between concurrent checkpoints in incremental checkpointing. Flink can
execute multiple checkpoints in parallel, and new checkpoints can start before previous checkpoints are confirmed as
completed by the checkpoint coordinator to the backend. We need to consider this in our reasoning about which previous
checkpoint can serve as a basis for a new incremental checkpoint. We are only allowed to reference shared state from a
confirmed checkpoint, because otherwise we might attempt to reference a shared file that might still be deleted, e.g.
when the assumed predecessor checkpoint still fails.

This can lead to a situation were multiple checkpoints regard the same sstable files in RocksDB as new because no
checkpoint that attempted to upload and register those sstable files has been confirmed, yet. To be on the safe side,
checkpoints must always upload such files to stable storage independently, under unique names, until the sstable files
have been registered by a completed checkpoint and the confirmation reached the backend. Otherwise, pending previous
checkpoints might still fail, in which case their newly uploaded files are deleted, and future checkpoints would
potentially attempt to reference deleted data.

Sometimes, this upload policy will result in the same sstable file been uploaded more than once, from different
checkpoints. However, at least we can later de-duplicate the sstable files in the checkpoint coordinator because they
are accounted under the same key. Only the copy that was uploaded by the first-confirmed checkpoint survives and we can
replace reference to the duplicates in all checkpoints that register afterwards.

#### Recovering the Shared State Registry under Job Manager Failure

During recovery from a job manager failure, the shared state registry counts are simply recalculated from the completed
checkpoint store. We clear all counts and re-register all checkpoints contained in the checkpoints from the completed
checkpoint store to the registry.

### Known Limitations of Incremental Checkpointing

Incremental checkpoints are only available for checkpoints and not for savepoints. Savepoints are always self-contained
and record the full state of a job. However, it is possible to externalize incremental checkpoints. This is a way to use
them for manual restarts of a job.

Rescaling the parallelism of a job is an operation that is officially only supported by through savepoints and not from
incremental checkpoints. (Unofficially, it should still be possible, though.)

Users should not manually delete non-empty checkpoint directories when working with incremental checkpoints. A newer
checkpoint might still reference files from the doctor of an older checkpoint.

Very small checkpoint intervals can become another problem in connection with incremental checkpoints. As we have
previously explained, the first step of an incremental checkpoint is forcing RocksDB to flush all memtables to disk. If
this step is performed very frequently, it can created a lot of small files. Too many small files can become problematic
with many implementations of distributed filesystems. Furthermore, those artificial flushes also influence the
performance of RocksDB because memtables are emptied and more files on the disk must be considered in read accesses,
even though compaction typically takes care or those quickly.
