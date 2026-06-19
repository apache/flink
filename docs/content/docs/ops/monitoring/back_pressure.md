---
title: "Monitoring Back Pressure"
weight: 3
type: docs
aliases:
  - /ops/monitoring/back_pressure.html
  - /internals/back_pressure_monitoring.html
  - /ops/monitoring/back_pressure
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

# Monitoring Back Pressure

Flink's web interface provides a tab to monitor the back pressure behaviour of running jobs.

## Back Pressure

If you see a **back pressure warning** (e.g. `High`) for a task, this means that it is producing data faster than the downstream operators can consume. Records in your job flow downstream (e.g. from sources to sinks) and back pressure is propagated in the opposite direction, up the stream.

Take a simple `Source -> Sink` job as an example. If you see a warning for `Source`, this means that `Sink` is consuming data slower than `Source` is producing. `Sink` is back pressuring the upstream operator `Source`.


## Task performance metrics

Every parallel instance of a task (subtask) is exposing a group of three metrics:
- `backPressuredTimeMsPerSecond`, time that subtask spent being back pressured
- `idleTimeMsPerSecond`, time that subtask spent waiting for something to process
- `busyTimeMsPerSecond`, time that subtask was busy doing some actual work
At any point of time these three metrics are adding up approximately to `1000ms`.

These metrics are being updated every couple of seconds, and the reported value represents the
average time that subtask was back pressured (or idle or busy) during the last couple of seconds.
Keep this in mind if your job has a varying load. For example, a subtask with a constant load of 50%
and another subtask that is alternating every second between fully loaded and idling will both have
the same value of `busyTimeMsPerSecond`: around `500ms`.

Internally, back pressure is judged based on the availability of output buffers.
If a task has no available output buffers, then that task is considered back pressured.
Idleness, on the other hand, is determined by whether or not there is input available.

## Example

The WebUI aggregates the maximum value of the back pressure and busy metrics from all of the
subtasks and presents those aggregated values inside the JobGraph. Besides displaying the raw
values, tasks are also color-coded to make the investigation easier.

{{< img src="/fig/back_pressure_job_graph.png" class="img-responsive" >}}

Idling tasks are blue, fully back pressured tasks are black, and fully busy tasks are colored red.
All values in between are represented as shades between those three colors.

### Back Pressure Status

In the *Back Pressure* tab next to the job overview you can find more detailed metrics.

{{< img src="/fig/back_pressure_subtasks.png" class="img-responsive" >}}

For subtasks whose status is **OK**, there is no indication of back pressure. **HIGH**, on the
other hand, means that a subtask is back pressured. Status is defined in the following way:

- **OK**: 0% <= back pressured <= 10%
- **LOW**: 10% < back pressured <= 50%
- **HIGH**: 50% < back pressured <= 100%

Additionally, you can find the percentage of time each subtask is back pressured, idle, or busy.

{{< top >}}
