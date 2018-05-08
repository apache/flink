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

{% highlight java %}
CheckpointConfig config = env.getCheckpointConfig();
config.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
{% endhighlight %}

The `ExternalizedCheckpointCleanup` mode configures what happens with externalized checkpoints when you cancel the job:

- **`ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION`**: Retain the externalized checkpoint when the job is cancelled. Note that you have to manually clean up the checkpoint state after cancellation in this case.

- **`ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION`**: Delete the externalized checkpoint when the job is cancelled. The checkpoint state will only be available if the job fails.

### Directory Structure

Similarly to [savepoints](savepoints.html), an externalized checkpoint consists
of a meta data file and, depending on the state back-end, some additional data
files. The **target directory** for the externalized checkpoint's meta data is
determined from the configuration key `state.checkpoints.dir` which, currently,
can only be set via the configuration files.

{% highlight yaml %}
state.checkpoints.dir: hdfs:///checkpoints/
{% endhighlight %}

This directory will then contain the checkpoint meta data required to restore
the checkpoint. For the `MemoryStateBackend`, this meta data file will be
self-contained and no further files are needed.

`FsStateBackend` and `RocksDBStateBackend` write separate data files
and only write the paths to these files into the meta data file. These data
files are stored at the path given to the state back-end during construction.

{% highlight java %}
env.setStateBackend(new RocksDBStateBackend("hdfs:///checkpoints-data/");
{% endhighlight %}

### Difference to Savepoints

Externalized checkpoints have a few differences from [savepoints](savepoints.html). They
- use a state backend specific (low-level) data format, may be incremental.
- do not support Flink specific features like rescaling.

### Resuming from an externalized checkpoint

A job may be resumed from an externalized checkpoint just as from a savepoint
by using the checkpoint's meta data file instead (see the
[savepoint restore guide](../cli.html#restore-a-savepoint)). Note that if the
meta data file is not self-contained, the jobmanager needs to have access to
the data files it refers to (see [Directory Structure](#directory-structure)
above).

{% highlight shell %}
$ bin/flink run -s :checkpointMetaDataPath [:runArgs]
{% endhighlight %}

{% top %}
