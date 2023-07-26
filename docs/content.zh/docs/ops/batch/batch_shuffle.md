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

总的来说，Flink 提供了两种不同类型的 blocking shuffles：`Hash shuffle` 和 `Sort shuffle`。

### Hash Shuffle

对于 1.14 以及更低的版本，`Hash Shuffle` 是 blocking shuffle 的默认实现，它为每个下游任务将每个上游任务的结果以单独文件的方式保存在 TaskManager 本地磁盘上。当下游任务运行时会向上游的 TaskManager 请求分片，TaskManager 读取文件之后通过网络传输（给下游任务）。

`Hash Shuffle` 为读写文件提供了不同的机制:

- `file`: 通过标准文件 IO 写文件，读取和传输文件需要通过 Netty 的 `FileRegion`。`FileRegion` 依靠系统调用 `sendfile` 来减少数据拷贝和内存消耗。
- `mmap`: 通过系统调用 `mmap` 来读写文件。
- `auto`: 通过标准文件 IO 写文件，对于文件读取，在 32 位机器上降级到 `file` 选项并且在 64 位机器上使用 `mmap` 。这是为了避免在 32 位机器上 java 实现 `mmap` 的文件大小限制。

可通过设置 [TaskManager 参数]({{< ref "docs/deployment/config#taskmanager-network-blocking-shuffle-type" >}}) 选择不同的机制。

{{< hint warning >}}
这个选项是实验性的，将来或许会有改动。
{{< /hint >}}

{{< hint warning >}}
如果开启 [SSL]({{< ref "docs/deployment/security/security-ssl" >}})，`file` 机制不能使用 `FileRegion` 而是在传输之前使用非池化的缓存去缓存数据。这可能会 [导致 direct memory OOM](https://issues.apache.org/jira/browse/FLINK-15981)。此外，因为同步读取文件有时会造成 netty 线程阻塞，[SSL handshake timeout]({{< ref "docs/deployment/config#security-ssl-internal-handshake-timeout" >}}) 配置需要调大以防 [connection reset 异常](https://issues.apache.org/jira/browse/FLINK-21416)。
{{< /hint >}}

{{< hint info >}}
`mmap`使用的内存不计算进已有配置的内存限制中，但是一些资源管理框架比如 YARN 将追踪这块内存使用，并且如果容器使用内存超过阈值会被杀掉。
{{< /hint >}}

`Hash Shuffle` 在小规模运行在固态硬盘的任务情况下效果显著，但是依旧有一些问题:

1. 如果任务的规模庞大将会创建很多文件，并且要求同时对这些文件进行大量的写操作。
2. 在机械硬盘情况下，当大量的下游任务同时读取数据，可能会导致随机读写问题。

### Sort Shuffle

`Sort Shuffle` 是 1.13 版中引入的另一种 blocking shuffle 实现，它在 1.15 版本成为默认。不同于 `Hash Shuffle`，`Sort Shuffle` 将每个分区结果写入到一个文件。当多个下游任务同时读取结果分片，数据文件只会被打开一次并共享给所有的读请求。因此，集群使用更少的资源。例如：节点和文件描述符以提升稳定性。此外，通过写更少的文件和尽可能线性的读取文件，尤其是在使用机械硬盘情况下 `Sort Shuffle` 可以获得比 `Hash Shuffle` 更好的性能。另外，`Sort Shuffle` 使用额外管理的内存作为读数据缓存并不依赖 `sendfile` 或 `mmap` 机制，因此也适用于 [SSL]({{< ref "docs/deployment/security/security-ssl" >}})。关于 `Sort Shuffle` 的更多细节请参考 [FLINK-19582](https://issues.apache.org/jira/browse/FLINK-19582) 和 [FLINK-19614](https://issues.apache.org/jira/browse/FLINK-19614)。

当使用sort blocking shuffle的时候有些配置需要适配:
- [taskmanager.network.sort-shuffle.min-buffers]({{< ref "docs/deployment/config" >}}#taskmanager-network-sort-shuffle-min-buffers): 配置该选项以控制数据写缓存大小。对于大规模的任务而言，你可能需要调大这个值，正常几百兆内存就足够了。因为这部分内存是从网络内存分配的，所以想要增大这个配置值，你可能还需要通过调整 [taskmanager.memory.network.fraction]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-fraction)，[taskmanager.memory.network.min]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-min) 和 [taskmanager.memory.network.max]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-max) 这几个参数来增大总的网络内存大小以避免出现 "Insufficient number of network buffers" 的错误。
- [taskmanager.memory.framework.off-heap.batch-shuffle.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size): 配置该选项以控制数据读取缓存大小。对于大规模的任务而言，你可能需要调大这个值，正常几百兆内存就足够了。因为这部分内存是从框架堆外内存中切分出来的，所以想要增大这个配置值，你还需要通过调整 [taskmanager.memory.framework.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size) 来增大框架堆外内存以避免出现直接内存溢出的错误。

{{< hint info >}}
目前 `Sort Shuffle` 只通过分区索引来排序而不是记录本身，也就是说 `sort` 只是被当成数据聚类算法使用。
{{< /hint >}}

### 如何选择 Blocking Shuffle

总的来说，

- 对于在固态硬盘上运行的小规模任务而言，两者都可以。
- 对于在机械硬盘上运行的大规模任务而言，`Sort Shuffle` 更为合适。

要在 `Sort Shuffle` 和 `Hash Shuffle` 间切换，你需要配置这个参数：[taskmanager.network.sort-shuffle.min-parallelism]({{< ref "docs/deployment/config" >}}#taskmanager-network-sort-shuffle-min-parallelism)。这个参数根据消费者Task的并发选择当前Task使用`Hash Shuffle` 或 `Sort Shuffle`，如果并发小于配置值则使用 `Hash Shuffle`，否则使用 `Sort Shuffle`。对于 1.15 以下版本，它的默认值是 `Integer.MAX_VALUE`，这意味着 `Hash Shuffle` 是默认实现。从 1.15 起，它的默认值是 1，这意味着 `Sort Shuffle` 是默认实现。

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

Note: The remote storage is only supported in the new hybrid shuffle mode. For more details about the new mode, please refer to the following [The New Mode And The Legacy Mode]({{< ref "docs/ops/batch/batch_shuffle#the-new-mode-and-the-legacy-mode" >}}).

### The New Mode And The Legacy Mode

We have refactored the architecture of Hybrid shuffle in Flink 1.18 (new mode), to resolve some existing issues. It has several advantages compared to the original architecture (legacy mode), such as requiring less network memory and supporting remote storage.

The new mode is enabled by default. To switch back to the legacy mode, set [taskmanager.network.hybrid-shuffle.enable-new-mode]({{< ref "docs/deployment/config" >}}#taskmanager-network-hybrid-shuffle-enable-new-mode) to `false`.

Note: The legacy mode is deprecated and can be removed in future releases.

### Limitations

Hybrid shuffle mode is still experimental and has some known limitations, which the Flink community is still working on eliminating.

- **No support for Slot Sharing.** In hybrid shuffle mode, Flink currently forces each task to be executed in a dedicated slot exclusively. If slot sharing is explicitly specified, an error will occur.
- **No pipelined execution for dynamic graph.** If auto-parallelism (dynamic graph) is enabled, Adaptive Batch Scheduler will wait until upstream tasks finish to decide parallelism of downstream tasks, which means hybrid shuffle effectively fallback to blocking shuffle (`ALL_PRODUCERS_FINISHED` constraint).

## 性能调优

下面这些建议可以帮助你实现更好的性能，这些对于大规模批作业尤其重要：

{{< tabs "Performance Tuning" >}}

{{< tab "Blocking Shuffle" >}}
1. 如果你使用机械硬盘作为存储设备，请总是使用 `Sort Shuffle`，因为这可以极大的提升稳定性和性能。从 1.15 开始，`Sort Shuffle` 已经成为默认实现，对于 1.14 以及更低版本，你需要通过将 [taskmanager.network.sort-shuffle.min-parallelism]({{< ref "docs/deployment/config" >}}#taskmanager-network-sort-shuffle-min-parallelism) 配置为 1 以手动开启 `Sort Shuffle`。
2. 对于 `Sort Shuffle` 和 `Hash Shuffle` 两种实现，你都可以考虑开启 [数据压缩]({{< ref "docs/deployment/config">}}#taskmanager-network-blocking-shuffle-compression-enabled) 除非数据本身无法压缩。从 1.15 开启，数据压缩是默认开启的，对于 1.14 以及更低版本你需要手动开启。
3. 当使用 `Sort Shuffle` 时，减少 [独占网络缓冲区]({{< ref "docs/deployment/config" >}}#taskmanager-network-memory-buffers-per-channel) 并增加 [流动网络缓冲区]({{< ref "docs/deployment/config" >}}#taskmanager-network-memory-floating-buffers-per-gate) 有利于性能提升。对于 1.14 以及更高版本，建议将 [taskmanager.network.memory.buffers-per-channel]({{< ref "docs/deployment/config" >}}#taskmanager-network-memory-buffers-per-channel) 设为 0 并且将 [taskmanager.network.memory.floating-buffers-per-gate]({{< ref "docs/deployment/config" >}}#taskmanager-network-memory-floating-buffers-per-gate) 设为一个较大的值 (比如，4096)。这有两个主要的好处：1) 首先这解耦了并发与网络内存使用量，对于大规模作业，这降低了遇到 "Insufficient number of network buffers" 错误的可能性；2) 网络缓冲区可以根据需求在不同的数据通道间共享流动，这可以提高了网络缓冲区的利用率，进而可以提高性能。
4. 增大总的网络内存。目前网络内存的大小是比较保守的。对于大规模作业，为了实现更好的性能，建议将 [网络内存比例]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-fraction) 增加至至少 0.2。为了使调整生效，你可能需要同时调整 [网络内存大小下界]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-min) 以及 [网络内存大小上界]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-max)。要获取更多信息，你可以参考这个 [内存配置文档]({{< ref "docs/deployment/memory/mem_setup_tm" >}})。
5. 增大数据写出内存。像上面提到的那样，对于大规模作业，如果有充足的空闲内存，建议增大 [数据写出内存]({{< ref "docs/deployment/config" >}}#taskmanager-network-sort-shuffle-min-buffers) 大小到至少 (2 * 并发数)。注意：在你增大这个配置后，为避免出现 "Insufficient number of network buffers" 错误，你可能还需要增大总的网络内存大小。
6. 增大数据读取内存。像上面提到的那样，对于大规模作业，建议增大 [数据读取内存]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size) 到一个较大的值 (比如，256M 或 512M)。因为这个内存是从框架的堆外内存切分出来的，因此你必须增加相同的内存大小到 [taskmanager.memory.framework.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size) 以避免出现直接内存溢出错误。
{{< /tab >}}

{{< tab "Hybrid Shuffle" >}}
1. 增大总的网络内存。目前网络内存的大小是比较保守的。对于大规模作业，为了实现更好的性能，建议将 [网络内存比例]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-fraction) 增加至至少 0.2。为了使调整生效，你可能需要同时调整 [网络内存大小下界]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-min) 以及 [网络内存大小上界]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-max)。要获取更多信息，你可以参考这个 [内存配置文档]({{< ref "docs/deployment/memory/mem_setup_tm" >}})。
2. 增大数据写出内存。对于大规模作业, 建议增大总内存大小，用于数据写入的内存越大, 下游越有机会直接从内存读取数据。注意如果你使用 legacy Hybrid shuffle 模式，你需要保证每个 `Result Partition` 至少能够分配到 `numSubpartition + 1` 个buffer, 否则可能会遇到 "Insufficient number of network buffers" 错误。
3. 增大数据读取内存。对于大规模作业，建议增大 [数据读取内存]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size) 到一个较大的值 (比如，256M 或 512M)。因为这个内存是从框架的堆外内存切分出来的，因此你必须增加相同的内存大小到 [taskmanager.memory.framework.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size) 以避免出现直接内存溢出错误。
4. 当使用 legacy Hybrid shuffle 模式时, 减少 [独占网络缓冲区]({{< ref "docs/deployment/config" >}}#taskmanager-network-memory-buffers-per-channel) 可能会严重影响性能。因此，最好不要将改值设置为 `0`, 并且对于大规模作业可以适当增加该值。同样需要注意的是: legacy `Hybrid Shuffle` 默认会将 [taskmanager.network.memory.read-buffer.required-per-gate.max]({{< ref "docs/deployment/config" >}}#taskmanager-network-memory-read-buffer-required-per-gate-max) 设置为 `Integer.MAX_VALUE`。最好不要去调整该配置，否则可能会造成性能的下降。
{{< /tab >}}

{{< /tabs >}}

## Trouble Shooting

尽管十分罕见，下面列举了一些你可能会碰到的异常情况以及对应的处理策略：

{{< tabs "Trouble Shooting" >}}
{{< tab "Blocking Shuffle" >}}

| 异常情况 | 处理策略 |
| :--------- | :------------------ |
| Insufficient number of network buffers | 这意味着网络内存大小不足以支撑作业运行，你需要增加总的网络内存大小。注意：从 1.15 开始，`Sort Shuffle` 已经成为默认实现，对于一些场景，`Sort Shuffle` 可能比 `Hash Shuffle` 需要更多的网络内存，因此当你的批作业升级到 1.15 以后可能会遇到这个网络内存不足的问题。这种情况下，你只需要增大总的网络内存大小即可。|
| Too many open files | 这意味着文件句柄不够用了。如果你使用的是 `Hash Shuffle`，请切换到 `Sort Shuffle`。如果你已经在使用 `Sort Shuffle`，请考虑增大操作系统文件句柄上限并且检查是否是作业代码占用了过多的文件句柄。|
| Connection reset by peer | 这通常意味着网络不太稳定或者压力较大。其他一些原因，比如上面提到的 SSL 握手超时等也可能会导致这一问题。如果你使用的是 `Hash Shuffle`，请切换到 `Sort Shuffle`。如果你已经在使用 `Sort Shuffle`，增大 [网络连接 backlog]({{< ref "docs/deployment/config" >}}#taskmanager-network-netty-server-backlog) 可能会有所帮助。|
| Network connection timeout | 这通常意味着网络不太稳定或者压力较大。增大 [网络连接超时时间]({{< ref "docs/deployment/config" >}}#taskmanager-network-netty-client-connectTimeoutSec) 或者开启 [网络连接重试]({{< ref "docs/deployment/config" >}}#taskmanager-network-retries) 可能会有所帮助。|
| Socket read/write timeout | 这通常意味着网络传输速度较慢或者压力较大。增大 [网络收发缓冲区]({{< ref "docs/deployment/config" >}}#taskmanager-network-netty-sendReceiveBufferSize) 大小可能会有所帮助。如果作业运行在 Kubernetes 环境，使用 [host network]({{< ref "docs/deployment/config" >}}#kubernetes-hostnetwork-enabled) 可能会有所帮助。|
| Read buffer request timeout | 这个问题只会出现在 `Sort Shuffle`，它意味着对数据读取缓冲区的激烈竞争。要解决这一问题，你可以增大 [taskmanager.memory.framework.off-heap.batch-shuffle.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size) 和 [taskmanager.memory.framework.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size)。|
| No space left on device | 这通常意味着磁盘存储空间或者 inodes 被耗尽。你可以考虑扩展磁盘存储空间或者做一些数据清理。|
| Out of memory error | 如果你使用的是 `Hash Shuffle`，请切换到 `Sort Shuffle`。如果你已经在使用 `Sort Shuffle` 并且遵循了上面章节的建议，你可以考虑增大相应的内存大小。对于堆上内存，你可以增大 [taskmanager.memory.task.heap.size]({{< ref "docs/deployment/config" >}}#ttaskmanager-memory-task-heap-size)，对于直接内存，你可以增大 [taskmanager.memory.task.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-off-heap-size)。|
| Container killed by external resource manger | 多种原因可能会导致容器被杀，比如，杀掉一个低优先级容器以释放资源启动高优先级容器，或者容器占用了过多的资源，比如内存、磁盘空间等。像上面章节所提到的那样，`Hash Shuffle` 可能会使用过多的内存而被 YARN 杀掉。所以，如果你使用的是 `Hash Shuffle`，请切换到 `Sort Shuffle`。如果你已经在使用 `Sort Shuffle`，你可能需要同时检查 Flink 日志以及资源管理框架的日志以找出容器被杀的根因，并且做出相应的修复。|

{{< /tab >}}

{{< tab "Hybrid Shuffle" >}}

| 异常情况                                      | 处理策略                                                                                                                                                                                                                                                                                                        |
|:------------------------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Insufficient number of network buffers    | 这意味着网络内存量不足以支撑作业运行，你需要增加总的内存大小。                                                                                                                                                                                                                                                                                            |                                                                                                                                                                                                                                                                               |
| Connection reset by peer                  | 这通常意味着网络不太稳定或者压力较大。其他一些原因，如SSL握手超时等也可能会导致这一问题。 增大 [网络连接 backlog]({{< ref "docs/deployment/config" >}}#taskmanager-network-netty-server-backlog) 可能会有所帮助。                                                                                                                                                                   |
| Network connection timeout                | 这通常意味着网络不太稳定或者压力较大。增大 [网络连接超时时间]({{< ref "docs/deployment/config" >}}#taskmanager-network-netty-client-connectTimeoutSec) 或者开启 [网络连接重试]({{< ref "docs/deployment/config" >}}#taskmanager-network-retries) 可能会有所帮助。                                                                                                         |
| Socket read/write timeout                 | 这通常意味着网络传输速度较慢或者压力较大。增大 [网络收发缓冲区]({{< ref "docs/deployment/config" >}}#taskmanager-network-netty-sendReceiveBufferSize) 大小可能会有所帮助。如果作业运行在 Kubernetes 环境，使用 [host network]({{< ref "docs/deployment/config" >}}#kubernetes-hostnetwork-enabled) 可能会有所帮助。                                                                    |
| Read buffer request timeout               | 这意味着对数据读取缓冲区的激烈竞争。要解决这一问题，你可以增大 [taskmanager.memory.framework.off-heap.batch-shuffle.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size) 和 [taskmanager.memory.framework.off-heap.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size)。 |
| No space left on device                   | 这通常意味着磁盘存储空间或者 inodes 被耗尽。你可以考虑扩展磁盘存储空间或者做一些数据清理。                                                                                                                                                                                                                                                                          |

{{< /tab >}}

{{< /tabs >}}
