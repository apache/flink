---
title: "Batch Shuffle"
weight: 4
type: docs
aliases:
- /docs/ops/batch/batch_shuffle.html
- /docs/ops/batch/batch_shuffle
- /docs/ops/batch/blocking_shuffle
- /docs/ops/batch/blocking_shuffle.html
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

# Batch Shuffle

## Overview

Flink supports a batch execution mode in both [DataStream API]({{< ref "docs/dev/datastream/execution_mode" >}}) and [Table / SQL]({{< ref "/docs/dev/table/overview" >}}) for jobs executing across bounded input. In batch execution mode, Flink offers two modes for network exchanges: `Blocking Shuffle` and `Hybrid Shuffle`.

- `Blocking Shuffle` is the default data exchange mode for batch executions. It persists all intermediate data, and can be consumed only after fully produced.
- `Hybrid Shuffle` is the next generation data exchange mode for batch executions. It persists data more smartly, and allows consuming while being produced. This feature is still experimental and has some known [limitations](#limitations).

## Blocking Shuffle

Unlike the pipeline shuffle used for streaming applications, blocking exchanges persists data to some storage. Downstream tasks then fetch these values via the network. Such an exchange reduces the resources required to execute the job as it does not need the upstream and downstream tasks to run simultaneously.

Flink implements blocking exchanges with `Sort Shuffle`. It writes only one file for each result partition. When the result partition is read by multiple downstream tasks concurrently, the data file is opened only once and shared by all readers. As a result, the cluster uses fewer resources like inode and file descriptors, which improves stability. Furthermore, by writing fewer files and making a best effort to read data sequentially, `Sort Shuffle` performs well on HDD too. Additionally, `Sort Shuffle` uses a dedicated part of the framework off-heap memory as data reading buffer and does not rely on `sendfile` or `mmap` mechanism, thus it also works well with [SSL]({{< ref "docs/deployment/security/security-ssl" >}}). Please refer to [FLINK-19582](https://issues.apache.org/jira/browse/FLINK-19582) and [FLINK-19614](https://issues.apache.org/jira/browse/FLINK-19614) for more details about `Sort Shuffle`.

Here are some config options that might need adjustment:
- [taskmanager.network.sort-shuffle.min-buffers]({{< ref "docs/deployment/config" >}}#taskmanager-network-sort-shuffle-min-buffers): Config option to control data writing buffer size. For large scale jobs, you may need to increase this value, usually, several hundreds of megabytes memory is enough. Because this memory is allocated from network memory, to increase this value, you may also need to increase the total network memory by adjusting [taskmanager.memory.network.fraction]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-fraction), [taskmanager.memory.network.min]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-min) and [taskmanager.memory.network.max]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-max) to avoid the potential "Insufficient number of network buffers" error.
- [taskmanager.memory.framework.off-heap.batch-shuffle.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size): Config option to control data reading buffer size. For large scale jobs, you may need to increase this value, usually, several hundreds of megabytes memory is enough. Because this memory is cut from the framework off-heap memory, to increase this value, you need also to increase the total framework off-heap memory by adjusting [taskmanager.memory.framework.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size) to avoid the potential direct memory OOM error.

{{< hint info >}}
Currently `Sort Shuffle` only sort records by partition index instead of the records themselves, that is to say, the `sort` is only used as a data clustering algorithm.
{{< /hint >}}

## Hybrid Shuffle

{{< hint warning >}}
This feature is still experimental and has some known [limitations](#limitations).
{{< /hint >}}

Hybrid shuffle is the next generation of batch data exchanges. It combines the advantages of blocking shuffle and pipelined shuffle (in streaming mode).
- Like blocking shuffle, it does not require upstream and downstream tasks to run simultaneously, which allows executing a job with little resources.
- Like pipelined shuffle, it does not require downstream tasks to be executed after upstream tasks finish, which reduces the overall execution time of the job when given sufficient resources.
- It adapts to custom preferences between persisting less data and restarting less tasks on failures, by providing different spilling strategies.

To use hybrid shuffle mode, you need to configure the [execution.batch-shuffle-mode]({{< ref "docs/deployment/config" >}}#execution-batch-shuffle-mode) to `ALL_EXCHANGES_HYBRID_FULL` (full spilling strategy) or `ALL_EXCHANGES_HYBRID_SELECTIVE` (selective spilling strategy).

### Spilling Strategy

Hybrid shuffle provides two spilling strategies:

- **Selective Spilling Strategy** persists data only if they are not consumed by downstream tasks timely. This reduces the amount of data to persist, at the price that in case of failures upstream tasks need to be restarted to reproduce the complete intermediate results.
- **Full Spilling Strategy** persists all data, no matter they are consumed by downstream tasks or not. In case of failures, the persisted complete intermediate result can be re-consumed, without having to restart upstream tasks.

To use hybrid shuffle mode, you need to configure the [execution.batch-shuffle-mode]({{< ref "docs/deployment/config" >}}#execution-batch-shuffle-mode) to `ALL_EXCHANGES_HYBRID_FULL` (full spilling strategy) or `ALL_EXCHANGES_HYBRID_SELECTIVE` (selective spilling strategy).

### Data Consumption Constraints

Hybrid shuffle divides the partition data consumption constraints between producer and consumer into the following three cases:

- **ALL_PRODUCERS_FINISHED** : hybrid partition data can be consumed only when all producers are finished.
- **ONLY_FINISHED_PRODUCERS** : hybrid partition can only consume data from finished producers.
- **UNFINISHED_PRODUCERS** : hybrid partition can consume data from unfinished producers.

These could be configured via [jobmanager.partition.hybrid.partition-data-consume-constraint]({{< ref "docs/deployment/config" >}}#jobmanager-partition-hybrid-partition-data-consume-constraint).

- **For `AdaptiveBatchScheduler`** : The default constraint is `UNFINISHED_PRODUCERS` to perform pipelined-like shuffle. If the value is set to `ALL_PRODUCERS_FINISHED` or `ONLY_FINISHED_PRODUCERS`, performance may be degraded.
- **If `SpeculativeExecution` is enabled** : The default constraint is `ONLY_FINISHED_PRODUCERS` to bring some performance optimization compared with blocking shuffle. Since producers and consumers have the opportunity to run at the same time, more speculative execution tasks may be created, and the cost of failover will also increase. If you want to fall back to the same behavior as blocking shuffle, you can configure this value to `ALL_PRODUCERS_FINISHED`. It is also important to note that `UNFINISHED_PRODUCERS` is not supported in this mode.

### Remote Storage Support

Hybrid shuffle supports to store the shuffle data to the remote storage. The remote storage path can be configured by [taskmanager.network.hybrid-shuffle.remote.path]({{< ref "docs/deployment/config" >}}#taskmanager-network-hybrid-shuffle-remote-path). This feature supports various remote storage systems, including OSS, HDFS, S3, etc. See [Flink Filesystem]({{< ref "docs/deployment/filesystems/overview" >}}) for more information about the Flink supported filesystems.

### Limitations

Hybrid shuffle mode is still experimental and has some known limitations, which the Flink community is still working on eliminating.

- **No support for Slot Sharing.** In hybrid shuffle mode, Flink currently forces each task to be executed in a dedicated slot exclusively. If slot sharing is explicitly specified, an error will occur.
- **No pipelined execution for dynamic graph.** If auto-parallelism (dynamic graph) is enabled, Adaptive Batch Scheduler will wait until upstream tasks finish to decide parallelism of downstream tasks, which means hybrid shuffle effectively fallback to blocking shuffle (`ALL_PRODUCERS_FINISHED` constraint).

## Performance Tuning

The following guidelines may help you to achieve better performance especially for large scale batch jobs:

{{< tabs "Performance Tuning" >}}

{{< tab "Blocking Shuffle" >}}
1. Data compression is enabled by default and usually improves performance. If the data is hard to compress, you can disable it by setting [taskmanager.network.compression.codec]({{< ref "docs/deployment/config">}}#taskmanager-network-compression-codec) to `NONE`.
2. Increase the total size of network memory. Currently, the default network memory size is pretty modest. For large scale jobs, it's suggested to increase the total [network memory fraction]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-fraction) to at least 0.2 to achieve better performance. At the same time, you may also need to adjust the [lower bound]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-min) and [upper bound]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-max) of the network memory size, please refer to the [memory configuration document]({{< ref "docs/deployment/memory/mem_setup_tm" >}}) for more information.
3. Increase the memory size for shuffle data write. As mentioned in the above section, for large scale jobs, it's suggested to increase the number of [write buffers per result partition]({{< ref "docs/deployment/config" >}}#taskmanager-network-sort-shuffle-min-buffers) to at least (2 * parallelism) if you have enough memory. Note that you may also need to increase the total size of network memory to avoid the "Insufficient number of network buffers" error after you increase this config value.
4. Increase the memory size for shuffle data read. As mentioned in the above section, for large scale jobs, it's suggested to increase the size of the [shared read memory]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size) to a larger value (for example, 256M or 512M). Because this memory is cut from the framework off-heap memory, you must increase [taskmanager.memory.framework.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size) by the same size to avoid the direct memory OOM error.
{{< /tab >}}

{{< tab "Hybrid Shuffle" >}}
1. Increase the total size of network memory. Currently, the default network memory size is pretty modest. For large scale jobs, it's suggested to increase the total [network memory fraction]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-fraction) to at least 0.2 to achieve better performance. At the same time, you may also need to adjust the [lower bound]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-min) and [upper bound]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-max) of the network memory size, please refer to the [memory configuration document]({{< ref "docs/deployment/memory/mem_setup_tm" >}}) for more information.
2. Increase the memory size for shuffle data write. For large scale jobs, it's suggested to increase the total size of network memory, the larger the memory that can be used in the shuffle write phase, the more opportunities downstream to read data directly from memory.
3. Increase the memory size for shuffle data read. For large scale jobs, it's suggested to increase the size of the [shared read memory]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size) to a larger value (for example, 256M or 512M). Because this memory is cut from the framework off-heap memory, you must increase [taskmanager.memory.framework.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size) by the same size to avoid the direct memory OOM error.
{{< /tab >}}

{{< /tabs >}}

## Trouble Shooting

Here are some exceptions you may encounter (rarely) and the corresponding solutions that may help:

{{< tabs "Trouble Shooting" >}}
{{< tab "Blocking Shuffle" >}}

| Exceptions | Potential Solutions |
| :--------- | :------------------ |
| Insufficient number of network buffers | This means the amount of network memory is not enough to run the target job and you need to increase the total network memory size. |
| Too many open files | This means that there are not enough file descriptors. Please consider increasing the system limit for file descriptors and check if the user code consumes too many file descriptors. |
| Connection reset by peer | This usually means that the network is unstable or under heavy burden. Other issues like SSL handshake timeout may also cause this problem. |
| Network connection timeout | This usually means that the network is unstable or under heavy burden and increasing the [network connection timeout]({{< ref "docs/deployment/config" >}}#taskmanager-network-netty-client-connecttimeoutsec) or enable [connection retry]({{< ref "docs/deployment/config" >}}#taskmanager-network-retries) may help. |
| Socket read/write timeout | This may indicate that the network is slow or under heavy burden. If the job is running in Kubernetes environment, using [host network]({{< ref "docs/deployment/config" >}}#kubernetes-hostnetwork-enabled) may help. |
| Read buffer request timeout | This means a fierce contention of the shuffle read memory. To solve the issue, you can increase [taskmanager.memory.framework.off-heap.batch-shuffle.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size) together with [taskmanager.memory.framework.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size). |
| No space left on device | This usually means that the disk space or the inodes have been exhausted. Please consider extending the storage space or do some cleanup. |
| Out of memory error | If you are following the above guidelines, please consider increasing the corresponding memory size. For heap memory, you can increase [taskmanager.memory.task.heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-heap-size) and for direct memory, you can increase [taskmanager.memory.task.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-off-heap-size). |
| Container killed by external resource manager | There are several reasons which can lead to the killing of a container, for example, kill a low priority container to make room for high priority container or the container uses too many resources like memory and disk space. You may need to check both Flink log and the external resource manager log to find out the root cause and resolve it accordingly. |

{{< /tab >}}

{{< tab "Hybrid Shuffle" >}}

| Exceptions                                  | Potential Solutions                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
|:--------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Insufficient number of network buffers      | This means the amount of network memory is not enough to run the target job and you need to increase the total network memory size.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |                                                                                                                                                                                                                                                                               |
| Connection reset by peer                    | This usually means that the network is unstable or under heavy burden. Other issues like SSL handshake timeout may also cause this problem.                                                                                                                                                                                                                                                                                                                                             |
| Network connection timeout                  | This usually means that the network is unstable or under heavy burden and increasing the [network connection timeout]({{< ref "docs/deployment/config" >}}#taskmanager-network-netty-client-connecttimeoutsec) or enable [connection retry]({{< ref "docs/deployment/config" >}}#taskmanager-network-retries) may help.                                                                                                                                                                                                                                                                                              |
| Socket read/write timeout                   | This may indicate that the network is slow or under heavy burden. If the job is running in Kubernetes environment, using [host network]({{< ref "docs/deployment/config" >}}#kubernetes-hostnetwork-enabled) may help.                                                                                                                                                                                                                                     |
| Read buffer request timeout                 | This means a fierce contention of the shuffle read memory. To solve the issue, you can increase [taskmanager.memory.framework.off-heap.batch-shuffle.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size) together with [taskmanager.memory.framework.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size).                                                                                                                                                                                                               |
| No space left on device                     | This usually means that the disk space or the inodes have been exhausted. Please consider extending the storage space or do some cleanup.                                                                                                                                                                                                                                                                                                                                                                                                                                                                            |

{{< /tab >}}

{{< /tabs >}}
