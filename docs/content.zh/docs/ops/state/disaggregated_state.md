---
title: "Disaggregated State Management"
weight: 20
type: docs
aliases:
  - /ops/state/disaggregated_state.html
  - /apis/streaming/disaggregated_state.html
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

# Disaggregated State Management

## Overview

For the first ten years of Flink, the state management is based on memory or local disk of the TaskManager. 
This approach works well for most use cases, but it has some limitations:
 * **Local Disk Constraints**: The state size is limited by the memory or disk size of the TaskManager.
 * **Spiky Resource Usage**: The local state model triggers periodic CPU and network I/O bursts during checkpointing or SST files compaction.
 * **Heavy Recovery**: State needs to be downloaded during recovery. The recovery time is
proportional to the state size, which can be slow for large state sizes.

In Flink 2.0, we introduced the disaggregated state management. This feature allows users to store 
the state in external storage systems like S3, HDFS, etc. This is useful when the state size
is extremely large. It could be used to store the state in a more cost-effective way, or to
persist or recovery the state in a more lightweight way. The benefits of disaggregated state management are:
 * **Unlimited State Size**: The state size is only limited by the external storage system.
 * **Stable Resource Usage**: The state is stored in external storage, thus the checkpoint could be very lightweight.
And the SST files compaction could be done remotely (TODO).
 * **Fast Recovery**: No need to download the state during recovery. The recovery time is
independent of the state size.
 * **Flexible**: Users can easily choose different external storage systems or I/O performance levels,
or scale the storage based on their requirements without change their hardware.
 * **Cost-effective**: External storage are usually cheaper than local disk. Users can flexibly
adjust computing resources and storage resources independently if there is any bottleneck.

The disaggregated state management contains three parts:
 * **ForSt State Backend**: A state backend that stores the state in external storage systems. It 
can also leverage the local disk for caching and buffering. The asynchronous I/O model is used to
read and write the state. For more details, see [ForSt State Backend]({{< ref "docs/ops/state/state_backends#the-forststatebackend" >}}).
 * **New State APIs**: The new state APIs (State V2) are introduced to perform asynchronous state
reads and writes, which is essential for overcoming the high network latency when accessing
the disaggregated state. For more details, see [New State APIs]({{< ref "docs/dev/datastream/fault-tolerance/state_v2" >}}).
 * **SQL Support**: Many SQL operators are rewritten to support the disaggregated state management
and asynchronous state access. User can easily enable these by setting the configuration.

{{< hint info >}}
Disaggregated state and asynchronous state access are encouraged for large state. However, when
the state size is small, the local state management with synchronous state access is a better
choice.
{{< /hint >}}

{{< hint warning >}}
The disaggregated state management is still in experimental state. We are working on improving
the performance and stability of this feature. The APIs and configurations may change in future
release.
{{< /hint >}}

## Quick Start

### For SQL Jobs

To enable the disaggregated state management in SQL jobs, you can set the following configurations:
```yaml
state.backend.type: forst
table.exec.async-state.enabled: true

# enable checkpoints, checkpoint directory is required
execution.checkpointing.incremental: true
execution.checkpointing.dir: s3://your-bucket/flink-checkpoints

# We don't support the mini-batch and two-phase aggregation in asynchronous state access yet.
table.exec.mini-batch.enabled: false
table.optimizer.agg-phase-strategy: ONE_PHASE
```
Thus, you could leverage the disaggregated state management and asynchronous state access in
your SQL jobs. We haven't implemented the full support for the asynchronous state access
in SQL yet. If the SQL operators you are using are not supported, the operator will fall back
to the synchronous state implementation automatically. The performance may not be optimal in
this case. The supported stateful operators are:
 - Rank (Top1, Append TopN)
 - Row Time Deduplicate
 - Aggregate (without distinct)
 - Join
 - Window Join
 - Tumble / Hop / Cumulative Window Aggregate


### For DataStream Jobs

To enable the disaggregated state management in DataStream jobs, firstly you should use 
the `ForStStateBackend`. Configure via code in per-job mode:
```java
Configuration config = new Configuration();
config.set(StateBackendOptions.STATE_BACKEND, "forst");
config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "s3://your-bucket/flink-checkpoints");
config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
env.configure(config);
```
Or configure via `config.yaml`:
```yaml
state.backend.type: forst

# enable checkpoints, checkpoint directory is required
execution.checkpointing.incremental: true
execution.checkpointing.dir: s3://your-bucket/flink-checkpoints
```

Then, you should write your datastream jobs with the new state APIs. For more
details, see [State V2]({{< ref "docs/dev/datastream/fault-tolerance/state_v2" >}}).

## Advanced Tuning Options

### Tuning ForSt State Backend

The `ForStStateBackend` has many configurations to tune the performance.
The design of ForSt is very similar to RocksDB, and the configurable options are almost the same,
so you can refer to [large state tuning]({{< ref "docs/ops/state/large_state_tuning#tuning-rocksdb-or-forst" >}})
to tune the ForSt state backend.

Besides that, the following sections introduce some unique configurations for ForSt.

#### ForSt Primary Storage Location

By default, ForSt stores the state in the checkpoint directory. In this case,
ForSt could perform lightweight checkpoints and fast recovery. However, users may
want to store the state in a different location, e.g., a different bucket in S3.
You can set the following configuration to specify the primary storage location:
```yaml
state.backend.forst.primary-dir: s3://your-bucket/forst-state
```

**Note**: If you set this configuration, you may not be able to leverage the lightweight
checkpoint and fast recovery, since the ForSt will perform file copy between the primary
storage location and the checkpoint directory during checkpointing and recovery.

#### ForSt Local Storage Location

By default, ForSt will **ONLY** disaggregate state when asynchronous APIs (State V2) are used. When 
using synchronous state APIs in DataStream and SQL jobs, ForSt will only serve as **local state store**. 
Since a job may contain multiple ForSt instances with mixed API usage, synchronous local state access 
along with asynchronous remote state access could help achieve better overall throughput.
If you want the operators with synchronous state APIs to store state in remote, the following configuration will help:
```yaml
state.backend.forst.sync.enforce-local: false
```
And you can specify the local storage location via:
```yaml
state.backend.forst.local-dir: path-to-local-dir
```

#### ForSt File Cache

ForSt uses the local disk for caching and buffering. The granularity of the cache is whole file.
This is enabled by default, except when the primary storage location is set to local.
There are two capacity limit policies for the cache:
 - Size-based: The cache will evict the oldest files when the cache size exceeds the limit.
 - Reserved-based: The cache will evict the oldest files when the reserved space on disk
(the disk where cache directory is) is not enough.
Corresponding configurations are:
```yaml
state.backend.forst.cache.size-based-limit: 1GB
state.backend.forst.cache.reserve-size: 10GB
```
Those can take effect together. If so, the cache will evict the oldest files when the cache
size exceeds either the size-based limit or the reserved size limit.

One can also specify the cache directory via:
```yaml
state.backend.forst.cache.dir: /tmp/forst-cache
```

#### ForSt Asynchronous Threads

ForSt uses asynchronous I/O to read and write the state. There are three types of threads:
 - Coordinator thread: The thread that coordinates the asynchronous read and write.
 - Read thread: The thread that reads the state asynchronously.
 - Write thread: The thread that writes the state asynchronously.

The number of asynchronous threads is configurable. Typically, you don't need to adjust these
values since the default values are good enough for most cases.
In case for special needs, you can set the following configuration to specify the number of
asynchronous threads:
 - `state.backend.forst.executor.read-io-parallelism`: The number of asynchronous threads for read. Default is 3.
 - `state.backend.forst.executor.write-io-parallelism`: The number of asynchronous threads for write. Default is 1.
 - `state.backend.forst.executor.inline-write`: Whether to inline the write operation in the coordinator thread.
Default is true. Setting this to false will raise the CPU usage.
 - `state.backend.forst.executor.inline-coordinator`: Whether to let task thread be the coordinator thread.
Default is true. Setting this to false will raise the CPU usage.

{{< top >}}
