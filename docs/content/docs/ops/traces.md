---
title: "Traces"
weight: 6
type: docs
aliases:
  - /ops/traces.html
  - /apis/traces.html
  - /monitoring/traces.html
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

# Traces

Flink exposes a tracing system that allows gathering and exposing traces to external systems.

## Reporting traces

You can access the tracing system from any user function that extends [RichFunction]({{< ref "docs/dev/datastream/user_defined_functions" >}}#rich-functions) by calling `getRuntimeContext().getMetricGroup()`.
This method returns a `MetricGroup` object via which you can report a new single span trace.

### Reporting single Span


A `Span` represents something that happened in Flink at certain point of time, that will be reported to a `TraceReporter`.
To report a `Span` you can use the `MetricGroup#addSpan(SpanBuilder)` method.

Currently we don't support traces with multiple spans. Each `Span` is self-contained and represents things like a checkpoint or recovery.
{{< tabs "9612d275-bdda-4322-a01f-ae6da805e917" >}}
{{< tab "Java" >}}
```java
public class MyClass {
    void doSomething() {
        // (...)
        metricGroup.addSpan(
                Span.builder(MyClass.class, "SomeAction")
                        .setStartTsMillis(startTs) // Optional
                        .setEndTsMillis(endTs) // Optional
                        .setAttribute("foo", "bar");
    }
}
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Currently reporting Spans from Python is not supported.
```
{{< /tab >}}
{{< /tabs >}}

## Reporter

For information on how to set up Flink's trace reporters please take a look at the [trace reporters documentation]({{< ref "docs/deployment/trace_reporters" >}}).

## System traces

Flink reports traces listed below.

The tables below generally feature 5 columns:

* The "Scope" column describes what is that trace reported scope.

* The "Name" column describes the name of the reported trace.

* The "Attributes" column lists the names of all attributes that are reported with the given trace.

* The "Description" column provides information as to what a given attribute is reporting.

### Checkpointing and initialization

Flink reports a single span trace for the whole checkpoint and job initialization events once that event reaches a terminal state: COMPLETED or FAILED.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 22%">Name</th>
      <th class="text-left" style="width: 20%">Attributes</th>
      <th class="text-left" style="width: 32%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th rowspan="18">org.apache.flink.</br>runtime.checkpoint.</br>CheckpointStatsTracker</th>
      <th rowspan="6"><strong>Checkpoint</strong></th>
      <td>startTs</td>
      <td>Timestamp when the checkpoint has started.</td>
    </tr>
    <tr>
      <td>endTs</td>
      <td>Timestamp when the checkpoint has finished.</td>
    </tr>
    <tr>
      <td>checkpointId</td>
      <td>Id of the checkpoint.</td>
    </tr>
    <tr>
      <td>checkpointedSize</td>
      <td>Size in bytes of checkpointed state during this checkpoint. Might be smaller than fullSize if incremental checkpoints are used.</td>
    </tr>
    <tr>
      <td>fullSize</td>
      <td>Full size in bytes of the referenced state by this checkpoint. Might be larger than checkpointSize if incremental checkpoints are used.</td>
    </tr>
    <tr>
      <td>checkpointStatus</td>
      <td>What was the state of this checkpoint: FAILED or COMPLETED.</td>
    </tr>
    <tr>
      <th rowspan="12"><strong>JobInitialization</strong></th>
      <td>startTs</td>
      <td>Timestamp when the job initialization has started.</td>
    </tr>
    <tr>
      <td>endTs</td>
      <td>Timestamp when the job initialization has finished.</td>
    </tr>
    <tr>
      <td>checkpointId (optional)</td>
      <td>Id of the checkpoint that the job recovered from (if any).</td>
    </tr>
    <tr>
      <td>fullSize</td>
      <td>Full size in bytes of the referenced state by the checkpoint that was used during recovery (if any).</td>
    </tr>
    <tr>
      <td>(Max/Sum)MailboxStartDurationMs</td>
      <td>The aggregated (max and sum) across all subtasks duration between subtask being created until all classes and objects of that subtask are initialize.</td>
    </tr>
    <tr>
      <td>(Max/Sum)ReadOutputDataDurationMs</td>
      <td>The aggregated (max and sum) across all subtasks duration of reading unaligned checkpoint's output buffers.</td>
    </tr>
    <tr>
      <td>(Max/Sum)InitializeStateDurationMs</td>
      <td>The aggregated (max and sum) across all subtasks duration to initialize a state backend (including state files download time)</td>
    </tr>
    <tr>
      <td>(Max/Sum)GateRestoreDurationMs</td>
      <td>The aggregated (max and sum) across all subtasks duration of reading unaligned checkpoint's input buffers.</td>
    </tr>
    <tr>
      <td>(Max/Sum)DownloadStateDurationMs<br><br>(optional - currently only supported by RocksDB Incremental)</td>
      <td>The aggregated (max and sum) duration across all subtasks of downloading state files from the DFS.</td>
    </tr>
    <tr>
      <td>(Max/Sum)RestoreStateDurationMs<br><br>(optional - currently only supported by RocksDB Incremental)</td>
      <td>The aggregated (max and sum) duration across all subtasks of restoring the state backend from fully localized state, i.e. after all remote state was downloaded.</td>
    </tr>
    <tr>
      <td>(Max/Sum)RestoredStateSizeBytes.[location]</td>
      <td>The aggregated (max and sum) across all subtasks size of restored state by location. Possible locations are defined in Enum StateObjectSizeStatsCollector as 
        LOCAL_MEMORY,
        LOCAL_DISK,
        REMOTE,
        UNKNOWN.</td>
    </tr>
    <tr>
      <td>(Max/Sum)RestoreAsyncCompactionDurationMs<br><br>(optional - currently only supported by RocksDB Incremental)</td>
      <td>The aggregated (max and sum) duration across all subtasks for async compaction after incremental restore.</td>
    </tr>
  </tbody>
</table>

{{< top >}}
