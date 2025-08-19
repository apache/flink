---
title: "Events"
weight: 7
type: docs
aliases:
  - /ops/events.html
  - /apis/events.html
  - /monitoring/events.html
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

# Events

Flink exposes a event reporting system that allows gathering and exposing events to external systems.

## Reporting events

You can access the event system from any user function that extends [RichFunction]({{< ref "docs/dev/datastream/user_defined_functions" >}}#rich-functions) by calling `getRuntimeContext().getMetricGroup()`.
This method returns a `MetricGroup` object via which you can report a new single event.

### Reporting single Event


An `Event` represents something that happened in Flink at certain point of time, that will be reported to a `TraceReporter`.
To report an `Event` you can use the `MetricGroup#addEvent(EventBuilder)` method.

{{< tabs "9612d275-bdda-4322-a01f-ae6da805e917" >}}
{{< tab "Java" >}}
```java
public class MyClass {
    void doSomething() {
        // (...)
        metricGroup.addEvent(
                Event.builder(MyClass.class, "SomeEvent")
                        .setObservedTsMillis(observedTs) // Optional
                        .setAttribute("foo", "bar")); // Optional
    }
}
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Currently reporting Events from Python is not supported.
```
{{< /tab >}}
{{< /tabs >}}

## Reporter

For information on how to set up Flink's event reporters please take a look at the [event reporters documentation]({{< ref "docs/deployment/event_reporters" >}}).

## System traces

Flink reports events listed below.

The tables below generally feature 5 columns:

* The "Scope" column describes what is that trace reported scope.

* The "Name" column describes the name of the reported trace.

* The "Attributes" column lists the names of all attributes that are reported with the given trace.

* The "Description" column provides information as to what a given attribute is reporting.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 18%">Scope</th>
      <th class="text-left" style="width: 22%">Name</th>
      <th class="text-left" style="width: 5%">Severity</th>
      <th class="text-left" style="width: 20%">Attributes</th>
      <th class="text-left" style="width: 32%">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td rowspan="8">org.apache.flink.</br>runtime.checkpoint.</br>CheckpointStatsTracker</td>
      <td rowspan="8"><strong>CheckpointEvent</strong></td>
      <td rowspan="8"><strong>INFO</strong></td>
    </tr>
    <tr>
      <td>observedTs</td>
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
      <td>checkpointType</td>
      <td>Type of the checkpoint. For example: "Checkpoint", "Full Checkpoint" or "Terminate Savepoint" ...</td>
    </tr>
    <tr>
      <td>isUnaligned</td>
      <td>Whether checkpoint was aligned or unaligned.</td>
    </tr>
    <tr>
      <td rowspan="3">org.apache.flink.</br>runtime.jobmaster.</br>JobMaster</td>
      <td rowspan="3"><strong>JobStatusChangeEvent</strong></td>
      <td rowspan="3"><strong>INFO</strong></td>
    </tr>
    <tr>
      <td>observedTs</td>
      <td>Timestamp when the job's status has changed.</td>
    </tr>
    <tr>
      <td>newJobStatus</td>
      <td>New job status that is being reported by this event.</td>
    </tr>
    <tr>
      <td rowspan="5">org.apache.flink.</br>runtime.scheduler.adaptive.</br>JobFailureMetricReporter</td>
      <td rowspan="5"><strong>JobFailureEvent</strong></td>
      <td rowspan="5"><strong>INFO</strong></td>
    </tr>
    <tr>
      <td>observedTs</td>
      <td>Timestamp when the job has failed.</td>
    </tr>
    <tr>
      <td>canRestart</td>
      <td>(optional) Whether the failure is terminal.</td>
    </tr>
    <tr>
      <td>isGlobalFailure</td>
      <td>(optional) Whether the failure is global. Global failover requires all tasks to failver.</td>
    </tr>
    <tr>
      <td>failureLabel.KEY</td>
      <td>(optional) For every failure label attached to this failure with a given KEY, the value of that label is attached as an attribute value.</td>
    </tr>
    <tr>
      <td rowspan="4">org.apache.flink.</br>runtime.scheduler.metrics.</br>AllSubTasksRunningOrFinishedStateTimeMetrics</td></td>
      <td rowspan="4"><strong>AllSubtasksStatusChangeEvent</strong></br>(streaming jobs only)</td>
      <td rowspan="4"><strong>INFO</strong></td>
    </tr>
    <tr>
      <td>observedTs</td>
      <td>Timestamp when all subtasks reached given status.</td>
    </tr>
    <tr>
      <td rowspan="2">status</td>
      <td><strong>ALL_RUNNING_OR_FINISHED</strong> means all subtasks are RUNNING or have already FINISHED</td>
    </tr>
    <tr>
      <td><strong>NOT_ALL_RUNNING_OR_FINISHED</strong> means at least one subtask has switched away from RUNNING or FINISHED, after previously <strong>ALL_RUNNING_OR_FINISHED</strong> being reported</td>
    </tr>
  </tbody>
</table>

{{< top >}}
