---
title: Speculative Execution
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

# Speculative Execution
This page describes the background of speculative execution, how to use it, and how to check the effectiveness of it.

## Background
Speculative execution is a mechanism to mitigate job slowness which is caused by problematic nodes. 
A problematic node may have hardware problems, accident I/O busy, or high CPU load. These problems may
make the hosted tasks run much slower than tasks on other nodes, and affect the overall execution time 
of a batch job.

In such cases, speculative execution will start new attempts of the slow task on nodes that are not 
detected as problematic. The new attempts process the same input data and produces the same data as the 
old one. The old attempt will not be affected and will keep running. The first finished attempt will be 
admitted, its output will be seen and consumed by the downstream tasks, and the remaining attempts will be 
canceled.

To achieve this, Flink uses the slow task detector to detect slow tasks. The nodes that the slow tasks
locate in will be identified as problematic nodes and get blocked via the blocklist mechanism. The scheduler 
will create new attempts for the slow tasks and deploy them on nodes that are not blocked.

## Usage
This section describes how to use speculative execution, including how to enable it, how to tune it, and
how to develop/improve custom sources to work with speculative execution.

{{< hint warning >}}
Note: Flink does not support speculative execution of DataSet jobs because DataSet will be deprecated 
in near future. DataStream API is now the recommended low level API to develop Flink batch jobs.
{{< /hint >}}

### Enable Speculative Execution
You can enable speculative execution through the following configuration items：
- `execution.batch.speculative.enabled: true`

Note that currently only [Adaptive Batch Scheduler]({{< ref "docs/deployment/elastic_scaling" >}}#adaptive-batch-scheduler) supports speculative execution. And Flink batch jobs will use this scheduler by default unless another scheduler is explicitly configured.

### Tuning Configuration
To make speculative execution work better for different jobs, you can tune below configuration options of the scheduler:
- [`execution.batch.speculative.max-concurrent-executions`]({{< ref "docs/deployment/config" >}}#execution-batch-speculative-speculative-max-concurrent-e)
- [`execution.batch.speculative.block-slow-node-duration`]({{< ref "docs/deployment/config" >}}#execution-batch-speculative-speculative-block-slow-node)

You can also tune below configuration options of the slow task detector:
- [`slow-task-detector.check-interval`]({{< ref "docs/deployment/config" >}}#slow-task-detector-check-interval)
- [`slow-task-detector.execution-time.baseline-lower-bound`]({{< ref "docs/deployment/config" >}}#slow-task-detector-execution-time-baseline-lower-bound)
- [`slow-task-detector.execution-time.baseline-multiplier`]({{< ref "docs/deployment/config" >}}#slow-task-detector-execution-time-baseline-multiplier)
- [`slow-task-detector.execution-time.baseline-ratio`]({{< ref "docs/deployment/config" >}}#slow-task-detector-execution-time-baseline-ratio)

Currently, speculative execution uses the slow task detector based on execution time to detect slow tasks.
The detector will periodically count all finished executions, if the finished execution ratio reaches the
configured ratio(`slow-task-detector.execution-time.baseline-ratio`), the baseline will be defined as
the execution time median multiplied by the configured multiplier(`slow-task-detector.execution-time.baseline-multiplier`),
then the running task whose execution time exceeds the baseline will be detected as a slow task.
It is worth mentioning that the execution time will be weighted with the input data volume of the execution vertex,
so the executions with large data volume differences but close computing power will not be detected as a slow task,
when data skew occurs. This helps to avoid starting unnecessary speculative attempts.

{{< hint warning >}}
Note: If the node is Source or the Hybrid Shuffle mode is used, the optimization that execution time
weighted with input data volume will not take effect, because the input data volume cannot be judged.
{{< /hint >}}

### Enable Sources for Speculative Execution
If your job uses a custom {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/Source.java" name="Source" >}}, 
and the source uses custom {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/SourceEvent.java" name="SourceEvent" >}},
you need to change the {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/SplitEnumerator.java" name="SplitEnumerator" >}} 
of that source to implement {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/SupportsHandleExecutionAttemptSourceEvent.java" name="SupportsHandleExecutionAttemptSourceEvent" >}} 
interface.
```java
public interface SupportsHandleExecutionAttemptSourceEvent {
    void handleSourceEvent(int subtaskId, int attemptNumber, SourceEvent sourceEvent);
}
```
This means the SplitEnumerator should be aware of the attempt which sends the event. Otherwise, exceptions 
will happen when the job manager receives a source event from the tasks and lead to job failures.

No extra change is required for other sources to work with speculative execution, including 
{{< gh_link file="/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/source/SourceFunction.java" name="SourceFunction sources" >}}, 
{{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/common/io/InputFormat.java" name="InputFormat sources" >}}, 
and {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/source/Source.java" name="new sources" >}}. 
All the source connectors offered by Apache Flink can work with speculative execution.

### Enable Sinks for Speculative Execution
Speculative execution is disabled by default for a sink unless it implements {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/common/SupportsConcurrentExecutionAttempts.java" name="SupportsConcurrentExecutionAttempts" >}} 
interface. This is due to compatibility considerations.
```java
public interface SupportsConcurrentExecutionAttempts {}
```
The {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/common/SupportsConcurrentExecutionAttempts.java" name="SupportsConcurrentExecutionAttempts" >}} 
works for {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/sink2/Sink.java" name="Sink" >}} 
, {{< gh_link file="/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/sink/SinkFunction.java" name="SinkFunction" >}} 
and {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/common/io/OutputFormat.java" name="OutputFormat" >}}.

{{< hint info >}}
If any operator in a task does not support speculative execution, the entire task would be marked as not supporting speculative execution.
That means if the Sink does not support speculative execution, the task containing the Sink operator cannot be speculatively executed.
{{< /hint >}}

{{< hint info >}}
For the {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/sink2/Sink.java" name="Sink" >}} implementation, 
Flink disables speculative execution for {{< gh_link file="/flink-core/src/main/java/org/apache/flink/api/connector/sink2/Committer.java" name="Committer" >}}(
including the operators extended by {{< gh_link file="/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/connector/sink2/WithPreCommitTopology.java" name="WithPreCommitTopology" >}} and {{< gh_link file="/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/connector/sink2/WithPostCommitTopology.java" name="WithPostCommitTopology" >}}).
Because the concurrent committing may cause some unexpected problems if the user is not experienced with it. 
And the committer is very unlikely to be the bottleneck of the batch job.
{{< /hint >}}

## Checking the Effectiveness of Speculative Execution
After enabling speculative execution, when there are slow tasks that trigger speculative execution, 
the web UI will show the speculative attempts on the `SubTasks` tab of vertices on the job page. The web UI 
also shows the blocked taskmanagers on the Flink cluster `Overview` and `Task Managers` pages.

You can also check these [`metrics`]({{< ref "docs/ops/metrics" >}}#speculative-execution) to see the effectiveness of speculative execution.

{{< top >}}
