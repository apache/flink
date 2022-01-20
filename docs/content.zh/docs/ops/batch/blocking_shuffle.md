---
title: "Blocking Shuffle"
weight: 4
type: docs
aliases:
- /ops/batch/blocking_shuffle.html
- /ops/batch/blocking_shuffle
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

# Blocking Shuffle

## 总览

Flink [DataStream API]({{< ref "docs/dev/datastream/execution_mode" >}}) 和 [Table / SQL]({{< ref "/docs/dev/table/overview" >}}) 都支持通过批处理执行模式处理有界输入。此模式是通过 blocking shuffle 进行网络传输。与流式应用使用管道 shuffle 阻塞交换的数据并存储，然后下游任务通过网络获取这些值的方式不同。这种交换减少了执行作业所需的资源，因为它不需要同时运行上游和下游任务。 

总的来说，Flink 提供了两种不同类型的 blocking shuffles：`Hash shuffle` 和 `Sort shuffle`。

在下面章节会详细说明它们。

## Hash Shuffle

对于 1.14 以及更低的版本，`Hash Shuffle` 是 blocking shuffle 的默认实现，它为每个下游任务将每个上游任务的结果以单独文件的方式保存在 TaskManager 本地磁盘上。当下游任务运行时会向上游的 TaskManager 请求分片，TaskManager 读取文件之后通过网络传输（给下游任务）。

`Hash Shuffle` 为读写文件提供了不同的机制:

- `file`: 通过标准文件 IO 写文件，读取和传输文件需要通过 Netty 的 `FileRegion`。`FileRegion` 依靠系统调用 `sendfile` 来减少数据拷贝和内存消耗。
- `mmap`: 通过系统调用 `mmap` 来读写文件。
- `Auto`: 通过标准文件 IO 写文件，对于文件读取，在 32 位机器上降级到 `file` 选项并且在 64 位机器上使用 `mmap` 。这是为了避免在 32 位机器上 java 实现 `mmap` 的文件大小限制。

可通过设置 [TaskManager 参数]({{< ref "docs/deployment/config#taskmanager-network-blocking-shuffle-type" >}}) 选择不同的机制。

{{< hint warning >}}
这个选项是实验性的，将来或许会有改动。
{{< /hint >}}

{{< hint warning >}}
如果开启 [SSL]({{< ref "docs/deployment/security/security-ssl" >}})，`file` 机制不能使用 `FileRegion` 而是在传输之前使用非池化的缓存去缓存数据。这可能会 [导致 direct memory OOM](https://issues.apache.org/jira/browse/FLINK-15981)。此外，因为同步读取文件有时会造成 netty 线程阻塞，[SSL handshake timeout]({{< ref "docs/deployment/config#security-ssl-internal-handshake-timeout" >}}) 配置需要调大以防 [connection reset 异常](https://issues.apache.org/jira/browse/FLINK-21416)。
{{< /hint >}}

{{< hint info >}}
`mmap`使用的内存不计算进已有配置的内存限制中，但是一些资源管理框架比如 yarn 将追踪这块内存使用，并且如果容器使用内存超过阈值会被杀掉。
{{< /hint >}}

为了进一步的提升性能，对于绝大多数的任务我们推荐 [启用压缩]({{< ref "docs/deployment/config">}}#taskmanager-network-blocking-shuffle-compression-enabled) ，除非数据很难被压缩。

`Hash Shuffle` 在小规模运行在固态硬盘的任务情况下效果显著，但是依旧有一些问题:

1. 如果任务的规模庞大将会创建很多文件，并且要求同时对这些文件进行大量的写操作。
2. 在机械硬盘情况下，当大量的下游任务同时读取数据，可能会导致随机读写问题。

## Sort Shuffle

`Sort Shuffle` 是 1.13 版中引入的另一种 blocking shuffle 实现，它在 1.15 版本成为默认。不同于 `Hash Shuffle`，sort shuffle 将每个分区结果写入到一个文件。当多个下游任务同时读取结果分片，数据文件只会被打开一次并共享给所有的读请求。因此，集群使用更少的资源。例如：节点和文件描述符以提升稳定性。此外，通过写更少的文件和尽可能线性的读取文件，尤其是在使用机械硬盘情况下 sort shuffle 可以获得比 hash shuffle 更好的性能。另外，`sort shuffle` 使用额外管理的内存作为读数据缓存并不依赖 `sendfile` 或 `mmap` 机制，因此也适用于 [SSL]({{< ref "docs/deployment/security/security-ssl" >}})。关于 sort shuffle 的更多细节请参考 [FLINK-19582](https://issues.apache.org/jira/browse/FLINK-19582) 和 [FLINK-19614](https://issues.apache.org/jira/browse/FLINK-19614)。

当使用sort blocking shuffle的时候有些配置需要适配:
- [taskmanager.network.blocking-shuffle.compression.enabled]({{< ref "docs/deployment/config" >}}#taskmanager-network-blocking-shuffle-compression-enabled): 配置该选项以启用 shuffle data 压缩，大部分任务建议开启除非你的数据压缩比率比较低。对于 1.14 以及更低的版本默认为 false，1.15 版本起默认为 true。
- [taskmanager.network.sort-shuffle.min-parallelism]({{< ref "docs/deployment/config" >}}#taskmanager-network-sort-shuffle-min-parallelism): 根据下游任务的并行度配置该选项以启用 sort shuffle。如果并行度低于设置的值，则使用 `hash shuffle`，否则 `sort shuffle`。对于 1.15 以下的版本，它的默认值是 `Integer.MAX_VALUE`，所以默认情况下总是会使用 `hash shuffle`。从 1.15 开始，它的默认值是 1, 所以默认情况下总是会使用 `sort shuffle`。
- [taskmanager.network.sort-shuffle.min-buffers]({{< ref "docs/deployment/config" >}}#taskmanager-network-sort-shuffle-min-buffers): 配置该选项以控制数据写缓存大小。对于大规模的任务而言，你可能需要调大这个值，正常几百兆内存就足够了。
- [taskmanager.memory.framework.off-heap.batch-shuffle.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size): 配置该选项以控制数据读取缓存大小。对于大规模的任务而言，你可能需要调大这个值，正常几百兆内存就足够了。

{{< hint info >}}
目前 `sort shuffle` 只通过分区索引来排序而不是记录本身，也就是说 `sort` 只是被当成数据聚类算法使用。
{{< /hint >}}

## 如何选择 Blocking Shuffle

总的来说，

- 对于在固态硬盘上运行的小规模任务而言，两者都可以。
- 对于在机械硬盘上运行的大规模任务而言，`sort shuffle` 更为合适。
- 在这两种情况下，你可以考虑 [enabling compression]({{< ref "docs/deployment/config">}}#taskmanager-network-blocking-shuffle-compression-enabled) 来提升性能，除非数据很难被压缩。
