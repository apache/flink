---
title: "Recovery job progress from job master failures"
weight: 4
type: docs
aliases:

- zh/docs/ops/batch/recovery_from_job_master_failure.html
- zh/docs/ops/batch/recovery_from_job_master_failure

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

# 批作业在 job master failover 后的进度恢复

## 背景

在 Flink 1.20 版本之前，如果 Flink 的 job master 发生故障导致被终止，将会发生如下两种情况：

- 如果作业未启用高可用性（HA），作业将失败。
- 如果作业启用了 HA，在 job master 重启后，流作业将从最后一个成功的检查点恢复。然而由于批作业没有检查点，将不得不从头开始运行，
  之前的所有进度会全数丢失。这对于那些需长时间运行的批作业来说，意味着严重的回退。

为了解决这一问题，我们在 Flink 1.20 版本中引入了批作业在 JM failover 后的进度恢复功能。这项功能的主要目的是使批作业在
job master failover 后能够尽可能的恢复出错前的进度，避免重新运行已完成的任务。

为了实现这个功能，我们引入了一个 JobEventStore 组件，使得 Flink 可以记录 JobMaster（ExecutionGraph、OperatorCoordinator等）的
状态变更事件到外部文件系统中。在 job master 崩溃和随后的重启期间，TaskManager 会保留该作业产出的中间结果数据，并尝试不断重新连接。
一旦 job master 重启，它将重新与 TaskManager 建立连接，并根据存留的中间结果以及此前在 JobEventStore 记录的事件来恢复作业状态，
从而恢复作业的执行进度。

## 使用方法

本章节描述了如何启用，配置调优，以及开发 source 来使用批作业在 job master failover 后的进度恢复功能。

### 如何启用

- 启用集群高可用：

  要使用 Flink 批作业的状态恢复功能，需首先确保已启用集群高可用性（HA）。Flink 支持基于 Zookeeper 和 Kubernetes 的高可用服务。请参考
  Flink 官方文档中关于[集群高可用性]({{< ref "docs/deployment/ha/overview#如何启用集群高可用" >}})的章节配置集群高可用服务。
- 配置 [execution.batch.job-recovery.enabled]({{< ref "docs/deployment/config" >}}#execution-batch-job-recovery-enabled): true

需要注意的是，当前只有 [Adaptive Batch Scheduler]({{< ref "docs/deployment/elastic_scaling" >}}#adaptive-batch-scheduler)
支持此功能。不过 Flink 批作业会默认使用该调度器，除非显式配置了其他调度器。

### 配置调优

考虑到不同集群和作业的差异，为了让批作业在 job master failover 后能够尽可能的恢复出错前的进度，避免重新运行已完成的任务，你可以配置下列配置项进行调优：

- [execution.batch.job-recovery.snapshot.min-pause]({{< ref "docs/deployment/config" >}}#execution-batch-job-recovery-snapshot-min-pause)：
  允许 OperatorCoordinator、ShuffleMaster 快照之间的最小暂停时间。该参数可以根据系统预期的 I/O 负载以及状态回滚的可容忍程度进行调整。
  如果你希望更小的状态回滚并且可以接受更高的 I/O 负载，你可以适当减少这个时间间隔。
- [execution.batch.job-recovery.previous-worker.recovery.timeout]({{< ref "docs/deployment/config" >}}#execution-batch-job-recovery-previous-worker-recovery-timeout)：
  允许 Shuffle Worker 重新连接的超时时间。在状态恢复过程中时，Flink 会向 Shuffle Master 请求存留的中间结果数据信息，并会一直等待直至超时。
  如果达到超时时间，Flink 将使用已获取的所有中间结果数据来恢复状态。
- [job-event.store.write-buffer.flush-interval]({{< ref "docs/deployment/config" >}}#job-event-store-write-buffer-flush-interval)：
  JobEventStore 的写出缓冲区中的内容刷新到外部文件系统的周期大小。
- [job-event.store.write-buffer.size]({{< ref "docs/deployment/config" >}}#job-event-store-write-buffer-size)：
  JobEventStore 写出缓冲区的大小。一旦缓冲区满了，内容将被刷新到外部文件系统。

### 让 Source 支持 job master failover 后进度恢复

目前，仅 new source (FLIP-27) 支持批处理作业的进度恢复。为了实现这一功能，new source 的
{{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/SplitEnumerator.java" name="SplitEnumerator" >}}
需要能够支持在批处理场景下做状态快照（此时 checkpointId = -1），并且实现
{{< gh_link file="flink-core/src/main/java/org/apache/flink/api/connector/source/SupportsBatchSnapshot.java" name="SupportsBatchSnapshot" >}}
接口，从而能够在重启后恢复到出错前的状态。
否则，为了保证数据的正确性，当 job master failover 后，将会发生以下两种情况：
1. 如果这个 source 的所有 task 没有完成，我们将重置并重新运行这个 source 的所有 task。
2. 如果这个 source 的所有 task 都完成了，则不需要采取额外行动，作业仍可以基于此进度继续运行。然而，如果这些 task 中的任何一个 task 在将来某个
时刻需要重启（例如发生了PartitionNotFound异常），那么这个 source 的所有 task 都需要被重置并重新运行。

## 局限性

- 只支持 new Source（FLIP-27）：由于 legacy Source 已经被废弃，所以此功能只支持 new Source。
- 仅适用于 [Adaptive Batch Scheduler]({{< ref "docs/deployment/elastic_scaling" >}}#adaptive-batch-scheduler)：当前只有
  Adaptive Batch Scheduler 支持 Batch 作业在 JM 故障后状态恢复，因此该功能受限于所有 Adaptive Batch Scheduler
  的[局限性]({{< ref "docs/deployment/elastic_scaling" >}}#局限性-1)。
- 暂不支持 Remote Shuffle Service。