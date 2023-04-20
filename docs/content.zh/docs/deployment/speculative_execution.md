---
title: 预测执行
weight: 5
type: docs

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

# 预测执行
这个文档描述了预测执行的背景，使用方法，以及如何验证其有效性。

## 背景
预测执行是一种用于缓解异常机器节点导致作业执行缓慢的机制。机器节点异常包括硬件异常，偶发的输入输出繁忙，高 CPU 负载等问题。
这些问题会导致运行在其上的任务比起在其他节点上运行的任务慢很多，从而影响到整个作业的执行时长。

在这种情况下，预测执行会为这些慢任务创建一些新的执行实例并部署在正常的机器节点上。这些新的执行实例和其对应的老执行实例(慢任务)
会消费相同的数据，并产出相同的结果。而那些老执行实例也会被保留继续执行。这些执行实例(包括新实例和老实例)中首先成功结束的执行
实例会被认可，其产出的结果会对下游任务可见，其他实例则会被取消掉。

为了实现这个机制，Flink 会通过一个慢任务检测器来检测慢任务。检测到的慢任务位于的机器节点会被识别为异常机器节点，并被加入机器
节点黑名单中。调度器则会为这些慢节点创建新的执行实例，并将其部署到未被加黑的机器节点上。

## 使用方法
本章节描述了如何使用预测执行，包含如何启用，调优，以及开发/改进自定义 source 来支持预测执行。

{{< hint warning >}}
注意：Flink 不支持 DataSet 作业的预测执行，因为 DataSet API 在不久的将来会被废弃。现在推荐使用 DataStream API 来开发 Flink 批处理作业。
{{< /hint >}}

### 启用预测执行
你可以通过以下配置项启用预测执行：
- `execution.batch.speculative.enabled: true`

需要注意的是，当前只有 [Adaptive Batch Scheduler]({{< ref "docs/deployment/elastic_scaling" >}}#adaptive-batch-scheduler) 支持预测执行。不过 Flink 批作业会默认使用该调度器，除非显式配置了其他调度器。

### 配置调优
考虑到不同作业的差异，为了让预测执行获得更好的效果，你可以调优下列调度器配置项：
- [`execution.batch.speculative.max-concurrent-executions`]({{< ref "docs/deployment/config" >}}#execution-batch-speculative-speculative-max-concurrent-e)
- [`execution.batch.speculative.block-slow-node-duration`]({{< ref "docs/deployment/config" >}}#execution-batch-speculative-speculative-block-slow-node)

你也可以通过下列配置项来对慢任务检测器进行调优：
- [`slow-task-detector.check-interval`]({{< ref "docs/deployment/config" >}}#slow-task-detector-check-interval)
- [`slow-task-detector.execution-time.baseline-lower-bound`]({{< ref "docs/deployment/config" >}}#slow-task-detector-execution-time-baseline-lower-bound)
- [`slow-task-detector.execution-time.baseline-multiplier`]({{< ref "docs/deployment/config" >}}#slow-task-detector-execution-time-baseline-multiplier)
- [`slow-task-detector.execution-time.baseline-ratio`]({{< ref "docs/deployment/config" >}}#slow-task-detector-execution-time-baseline-ratio)

目前，预测执行通过基于执行时间的慢任务检测器来检测慢任务，检测器将定期统计所有已执行完成的节点，当完成率达到基线比率(`slow-task-detector.execution-time.baseline-ratio`)时，
基线将被定义为执行时间中位数乘以系数(`slow-task-detector.execution-time.baseline-multiplier`)，若运行中节点的执行时间超过基线则会被判定为慢节点。值得一提的是，
处理执行时间时会将其与节点实际输入数据量进行加权，若发生数据倾斜，数据量差异较大但算力接近的节点并不会被检测为慢任务，从而避免拉起无效预测执行实例浪费资源。

{{< hint warning >}}
注意：若节点为 Source 或使用了 Hybrid Shuffle 模式，执行时间与数据量加权优化将不生效，因为无法判断输入数据量。
{{< /hint >}}

### 让 Source 支持预测执行
如果你的作业有用到自定义 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/Source.java" name="Source" >}}, 
并且这个 Source 用到了自定义的 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/SourceEvent.java" name="SourceEvent" >}},
你需要修改该 Source 的 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/SplitEnumerator.java" name="SplitEnumerator" >}} 
实现接口 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/SupportsHandleExecutionAttemptSourceEvent.java" name="SupportsHandleExecutionAttemptSourceEvent" >}}。
```java
public interface SupportsHandleExecutionAttemptSourceEvent {
    void handleSourceEvent(int subtaskId, int attemptNumber, SourceEvent sourceEvent);
}
```
这意味着 SplitEnumerator 需要知道是哪个执行实例发出了这个事件。否则，JobManager 会在收到 SourceEvent 的时候报错从而导致作业失败。

除此之外的 Source 不需要额外的改动就可以进行预测执行，包括 
{{< gh_link file="/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/source/SourceFunction.java" name="SourceFunction Source" >}}, 
{{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/common/io/InputFormat.java" name="InputFormat Source" >}}, 
和 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/Source.java" name="新版 Source" >}}.
Apache Flink 官方提供的 Source 都支持预测执行。

### 让 Sink 支持预测执行
Sink 的预测执行默认是关闭的，除非 Sink 实现了 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/common/SupportsConcurrentExecutionAttempts.java" name="SupportsConcurrentExecutionAttempts" >}}
接口。这里主要是兼容性方面的考虑。
```java
public interface SupportsConcurrentExecutionAttempts {}
```
接口 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/common/SupportsConcurrentExecutionAttempts.java" name="SupportsConcurrentExecutionAttempts" >}}
适用于 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/sink2/Sink.java" name="Sink" >}}
，{{< gh_link file="/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/sink/SinkFunction.java" name="SinkFunction" >}}
以及 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/common/io/OutputFormat.java" name="OutputFormat" >}}。

{{< hint info >}}
如果作业节点中有任何算子不支持预测执行，那么该节点都将被认为不支持预测执行。这意味着如果 Sink 不支持预测执行，那么包含其的整个节点都无法进行预测执行。
{{< /hint >}}

{{< hint info >}}
对于 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/sink2/Sink.java" name="Sink" >}} 实现, 
Flink 会关闭 {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/sink2/Committer.java" name="Committer" >}} 的预测执行，
（包括被 {{< gh_link file="/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/connector/sink2/WithPreCommitTopology.java" name="WithPreCommitTopology" >}} 和 {{< gh_link file="/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/connector/sink2/WithPostCommitTopology.java" name="WithPostCommitTopology" >}} 扩展的算子）。
因为如果用户对并行提交理解不深的话，这里可能会引起意料之外的问题。另外一个原因是提交的部分往往不是批作业的瓶颈所在。
{{< /hint >}}

## 检查预测执行的效果
在启用预测执行后，当出现慢任务触发预测执行时，Web UI 会在作业页面的节点信息的 `SubTasks` 分页展示预测执行实例。Web UI 
还会在 `Overview` 和 `Task Managers` 页面展示当前被加黑的 TaskManager。

你还可以通过检查这些 [`指标`]({{< ref "docs/ops/metrics" >}}#预测执行) 来判断预测执行的有效性。

{{< top >}}
