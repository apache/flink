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

## Overview

Flink supports a batch execution mode in both [DataStream API]({{< ref "docs/dev/datastream/execution_mode" >}}) and [Table / SQL]({{< ref "/docs/dev/table/overview" >}}) for jobs executing across bounded input. In this mode, network exchanges occur via a blocking shuffle. Unlike the pipeline shuffle used for streaming applications, blocking exchanges persists data to some storage. Downstream tasks then fetch these values via the network. Such an exchange reduces the resources required to execute the job as it does not need the upstream and downstream tasks to run simultaneously.

As a whole, Flink provides two different types of blocking shuffles; `Hash shuffle` and `Sort shuffle`.

They will be detailed in the following sections.

## Hash Shuffle

The default blocking shuffle implementation, `Hash Shuffle`, has each upstream task persist its results in a separate file for each downstream task on the local disk of the TaskManager. When the downstream tasks run, they will request partitions from the upstream TaskManager's, which read the files and transmit data via the network.

`Hash Shuffle` provides different mechanisms for writing and reading files:

- `file`: Writes files with the normal File IO, reads and transmits files with Netty `FileRegion`. `FileRegion` relies on `sendfile` system call to reduce the number of data copies and memory consumption.
- `mmap`: Writes and reads files with `mmap` system call.
- `Auto`: Writes files with the normal File IO, for file reading, it falls back to normal `file` option on 32 bit machine and use `mmap` on 64 bit machine. This is to avoid file size limitation of java `mmap` implementation on 32 bit machine.

The different mechanism could be chosen via [TaskManager configurations]({{< ref "docs/deployment/config#taskmanager-network-blocking-shuffle-type" >}}).

{{< hint warning >}}
This option is experimental and might be changed future.
{{< /hint >}}

{{< hint warning >}}
If [SSL]({{< ref "docs/deployment/security/security-ssl" >}}) is enabled, the `file` mechanism can not use `FileRegion` and instead uses an un-pooled buffer to cache data before transmitting. This might [cause direct memory OOM](https://issues.apache.org/jira/browse/FLINK-15981). Additionally, since the synchronous file reading might block Netty threads for some time, the [SSL handshake timeout]({{< ref "docs/deployment/config#security-ssl-internal-handshake-timeout" >}}) needs to be increased to avoid [connection reset errors](https://issues.apache.org/jira/browse/FLINK-21416).
{{< /hint >}}

{{< hint info >}}
The memory usage of `mmap` is not accounted for by configured memory limits, but some resource frameworks like Yarn will track this memory usage and kill the container if memory exceeds some threshold.
{{< /hint >}}

To further improve the performance, for most jobs we also recommend [enabling compression]({{< ref "docs/deployment/config">}}#taskmanager-network-blocking-shuffle-compression-enabled) unless the data is hard to compress.

`Hash Shuffle` works well for small scale jobs with SSD, but it also have some disadvantages:

1. If the job scale is large, it might create too many files, and it requires a large write buffer to write these files at the same time.
2. On HDD, when multiple downstream tasks fetch their data simultaneously, it might incur the issue of random IO.

## Sort Shuffle

`Sort Shuffle` is another blocking shuffle implementation introduced in version 1.13. Different from `Hash Shuffle`, sort shuffle writes only one file for each result partition. When the result partition is read by multiple downstream tasks concurrently, the data file is opened only once and shared by all readers. As a result, the cluster uses fewer resources like inode and file descriptors, which improves stability. Furthermore, by writing fewer files and making a best effort to read data sequentially, sort shuffle can achieve better performance than hash shuffle, especially on HDD. Additionally, `sort shuffle` uses extra managed memory as data reading buffer and does not rely on `sendfile` or `mmap` mechanism, thus it also works well with [SSL]({{< ref "docs/deployment/security/security-ssl" >}}). Please refer to [FLINK-19582](https://issues.apache.org/jira/browse/FLINK-19582) and [FLINK-19614](https://issues.apache.org/jira/browse/FLINK-19614) for more details about sort shuffle.

There are several config options that might need adjustment when using sort blocking shuffle:
- [taskmanager.network.blocking-shuffle.compression.enabled]({{< ref "docs/deployment/config" >}}#taskmanager-network-blocking-shuffle-compression-enabled): Config option for shuffle data compression. it is suggested to enable it for most jobs except that the compression ratio of your data is low.
- [taskmanager.network.sort-shuffle.min-parallelism]({{< ref "docs/deployment/config" >}}#taskmanager-network-sort-shuffle-min-parallelism): Config option to enable sort shuffle depending on the parallelism of downstream tasks. If parallelism is lower than the configured value, `hash shuffle` will be used, otherwise `sort shuffle` will be used.
- [taskmanager.network.sort-shuffle.min-buffers]({{< ref "docs/deployment/config" >}}#taskmanager-network-sort-shuffle-min-buffers): Config option to control data writing buffer size. For large scale jobs, you may need to increase this value, usually, several hundreds of megabytes memory is enough.
- [taskmanager.memory.framework.off-heap.batch-shuffle.size]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-batch-shuffle-size): Config option to control data reading buffer size. For large scale jobs, you may need to increase this value, usually, several hundreds of megabytes memory is enough.

{{< hint info >}}
Currently `sort shuffle` only sort records by partition index instead of the records themselves, that is to say, the `sort` is only used as a data clustering algorithm.
{{< /hint >}}

## Choices of Blocking Shuffle

As a summary,

- For small scale jobs running on SSD, both implementation should work.
- For large scale jobs or for jobs running on HDD, `sort shuffle` should be more suitable.
- In both case, you may consider [enabling compression]({{< ref "docs/deployment/config">}}#taskmanager-network-blocking-shuffle-compression-enabled) to improve the performance unless the data is hard to compress.
