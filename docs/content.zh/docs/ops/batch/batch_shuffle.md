---
title: "Batch Shuffle"
weight: 4
type: docs
aliases:
- zh/docs/ops/batch/batch_shuffle.html
- zh/docs/ops/batch/batch_shuffle
- zh/docs/ops/batch/blocking_shuffle
- zh/docs/ops/batch/blocking_shuffle.html
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

## 总览

Flink [DataStream API]({{< ref "docs/dev/datastream/execution_mode" >}}) 和 [Table / SQL]({{< ref "/docs/dev/table/overview" >}}) 都支持通过批处理执行模式处理有界输入。 在批处理模式下，Flink 提供了两种网络交换模式: `Blocking Shuffle` 和 `Hybrid Shuffle`.

- `Blocking Shuffle` 是批处理的默认数据交换模式。它会持久化所有的中间数据，只有当数据产出完全后才能被消费。
- `Hybrid Shuffle` 是下一代的批处理数据交换模式. 他会更加智能地持久化数据, 并且允许在数据生产的同时进行消费. 该特性目前仍处于实验阶段并且存在一些已知的 [限制](#limitations).

## Blocking Shuffle

与流式应用使用管道 shuffle 交换数据的方式不同，blocking 交换持久化数据到存储中，然后下游任务通过网络获取这些值。这种交换减少了执行作业所需的资源，因为它不需要同时运行上游和下游任务。

Flink 使用 `Sort Shuffle` 实现 blocking 交换。它为每个分区结果只写入一个文件。当多个下游任务同时读取结果分片，数据文件只会被打开一次并共享给所有的读请求。因此，集群使用更少的资源，例如 inode 和文件描述符，从而提升稳定性。此外，通过写更少的文件和尽可能顺序地读取数据，`Sort Shuffle` 在机械硬盘上也有很好的性能。另外，`Sort Shuffle` 使用框架堆外内存中专门的一部分作为读数据缓存，并不依赖 `sendfile` 或 `mmap` 机制，因此也适用于 [SSL]({{< ref "docs/deployment/security/security-ssl" >}})。关于 `Sort Shuffle` 的更多细节请参考 [FLINK-19582](https://issues.apache.org/jira/browse/FLINK-19582) 和 [FLINK-19614](https://issues.apache.org/jira/browse/FLINK-19614)。

以下配置可能需要调整：
- [taskmanager.network.sort-shuffle.min-buffers]({{< ref "docs/deployment/config" >}}#taskmanager-network-sort-shuffle-min-buffers): 配置该选项以控制数据写缓存大小。对于大规模的任务而言，你可能需要调大这个值，正常几百兆内存就足够了。因为这部分内存是从网络内存分配的，所以想要增大这个配置值，你可能还需要通过调整 [taskmanager.memory.network.fraction]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-fraction)，[taskmanager.memory.network.min]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-min) 和 [taskmanager.memory.network.max]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-max) 这几个参数来增大总的网络内存大小以避免出现 "Insufficient number of network buffers" 的错误。
- [taskmanager.memory.framework.off-heap.batch-shuffle.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size): 配置该选项以控制数据读取缓存大小。对于大规模的任务而言，你可能需要调大这个值，正常几百兆内存就足够了。因为这部分内存是从框架堆外内存中切分出来的，所以想要增大这个配置值，你还需要通过调整 [taskmanager.memory.framework.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size) 来增大框架堆外内存以避免出现直接内存溢出的错误。

{{< hint info >}}
目前 `Sort Shuffle` 只通过分区索引来排序而不是记录本身，也就是说 `sort` 只是被当成数据聚类算法使用。
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

## 性能调优

下面这些建议可以帮助你实现更好的性能，这些对于大规模批作业尤其重要：

{{< tabs "Performance Tuning" >}}

{{< tab "Blocking Shuffle" >}}
1. 数据压缩默认开启，通常能提升性能。如果数据很难压缩，可以将 [taskmanager.network.compression.codec]({{< ref "docs/deployment/config">}}#taskmanager-network-compression-codec) 设置为 `NONE` 来关闭压缩。
2. 增大总的网络内存。目前网络内存的大小是比较保守的。对于大规模作业，为了实现更好的性能，建议将 [网络内存比例]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-fraction) 增加至至少 0.2。为了使调整生效，你可能需要同时调整 [网络内存大小下界]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-min) 以及 [网络内存大小上界]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-max)。要获取更多信息，你可以参考这个 [内存配置文档]({{< ref "docs/deployment/memory/mem_setup_tm" >}})。
3. 增大数据写出内存。像上面提到的那样，对于大规模作业，如果有充足的空闲内存，建议增大 [数据写出内存]({{< ref "docs/deployment/config" >}}#taskmanager-network-sort-shuffle-min-buffers) 大小到至少 (2 * 并发数)。注意：在你增大这个配置后，为避免出现 "Insufficient number of network buffers" 错误，你可能还需要增大总的网络内存大小。
4. 增大数据读取内存。像上面提到的那样，对于大规模作业，建议增大 [数据读取内存]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size) 到一个较大的值 (比如，256M 或 512M)。因为这个内存是从框架的堆外内存切分出来的，因此你必须增加相同的内存大小到 [taskmanager.memory.framework.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size) 以避免出现直接内存溢出错误。
{{< /tab >}}

{{< tab "Hybrid Shuffle" >}}
1. 增大总的网络内存。目前网络内存的大小是比较保守的。对于大规模作业，为了实现更好的性能，建议将 [网络内存比例]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-fraction) 增加至至少 0.2。为了使调整生效，你可能需要同时调整 [网络内存大小下界]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-min) 以及 [网络内存大小上界]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-max)。要获取更多信息，你可以参考这个 [内存配置文档]({{< ref "docs/deployment/memory/mem_setup_tm" >}})。
2. 增大数据写出内存。对于大规模作业, 建议增大总内存大小，用于数据写入的内存越大, 下游越有机会直接从内存读取数据。
3. 增大数据读取内存。对于大规模作业，建议增大 [数据读取内存]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size) 到一个较大的值 (比如，256M 或 512M)。因为这个内存是从框架的堆外内存切分出来的，因此你必须增加相同的内存大小到 [taskmanager.memory.framework.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size) 以避免出现直接内存溢出错误。
{{< /tab >}}

{{< /tabs >}}

## Trouble Shooting

尽管十分罕见，下面列举了一些你可能会碰到的异常情况以及对应的处理策略：

{{< tabs "Trouble Shooting" >}}
{{< tab "Blocking Shuffle" >}}

| 异常情况 | 处理策略 |
| :--------- | :------------------ |
| Insufficient number of network buffers | 这意味着网络内存大小不足以支撑作业运行，你需要增加总的网络内存大小。|
| Too many open files | 这意味着文件句柄不够用了。请考虑增大操作系统文件句柄上限并且检查是否是作业代码占用了过多的文件句柄。|
| Connection reset by peer | 这通常意味着网络不太稳定或者压力较大。其他一些原因，比如 SSL 握手超时等也可能会导致这一问题。|
| Network connection timeout | 这通常意味着网络不太稳定或者压力较大。增大 [网络连接超时时间]({{< ref "docs/deployment/config" >}}#taskmanager-network-netty-client-connecttimeoutsec) 或者开启 [网络连接重试]({{< ref "docs/deployment/config" >}}#taskmanager-network-retries) 可能会有所帮助。|
| Socket read/write timeout | 这通常意味着网络传输速度较慢或者压力较大。如果作业运行在 Kubernetes 环境，使用 [host network]({{< ref "docs/deployment/config" >}}#kubernetes-hostnetwork-enabled) 可能会有所帮助。|
| Read buffer request timeout | 这意味着对数据读取缓冲区的激烈竞争。要解决这一问题，你可以增大 [taskmanager.memory.framework.off-heap.batch-shuffle.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size) 和 [taskmanager.memory.framework.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size)。|
| No space left on device | 这通常意味着磁盘存储空间或者 inodes 被耗尽。你可以考虑扩展磁盘存储空间或者做一些数据清理。|
| Out of memory error | 如果你已经遵循了上面章节的建议，你可以考虑增大相应的内存大小。对于堆上内存，你可以增大 [taskmanager.memory.task.heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-heap-size)，对于直接内存，你可以增大 [taskmanager.memory.task.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-off-heap-size)。|
| Container killed by external resource manager | 多种原因可能会导致容器被杀，比如，杀掉一个低优先级容器以释放资源启动高优先级容器，或者容器占用了过多的资源，比如内存、磁盘空间等。你可能需要同时检查 Flink 日志以及资源管理框架的日志以找出容器被杀的根因，并且做出相应的修复。|

{{< /tab >}}

{{< tab "Hybrid Shuffle" >}}

| 异常情况                                      | 处理策略                                                                                                                                                                                                                                                                                                        |
|:------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Insufficient number of network buffers    | 这意味着网络内存量不足以支撑作业运行，你需要增加总的内存大小。                                                                                                                                                                                                                                                                                            |                                                                                                                                                                                                                                                                               |
| Connection reset by peer                  | 这通常意味着网络不太稳定或者压力较大。其他一些原因，如SSL握手超时等也可能会导致这一问题。                                                                                                                                                                   |
| Network connection timeout                | 这通常意味着网络不太稳定或者压力较大。增大 [网络连接超时时间]({{< ref "docs/deployment/config" >}}#taskmanager-network-netty-client-connecttimeoutsec) 或者开启 [网络连接重试]({{< ref "docs/deployment/config" >}}#taskmanager-network-retries) 可能会有所帮助。                                                                                                         |
| Socket read/write timeout                 | 这通常意味着网络传输速度较慢或者压力较大。如果作业运行在 Kubernetes 环境，使用 [host network]({{< ref "docs/deployment/config" >}}#kubernetes-hostnetwork-enabled) 可能会有所帮助。                                                                    |
| Read buffer request timeout               | 这意味着对数据读取缓冲区的激烈竞争。要解决这一问题，你可以增大 [taskmanager.memory.framework.off-heap.batch-shuffle.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size) 和 [taskmanager.memory.framework.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size)。 |
| No space left on device                   | 这通常意味着磁盘存储空间或者 inodes 被耗尽。你可以考虑扩展磁盘存储空间或者做一些数据清理。                                                                                                                                                                                                                                                                          |

{{< /tab >}}

{{< /tabs >}}
