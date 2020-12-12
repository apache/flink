---
title: "Debugging Windows & Event Time"
nav-parent_id: debugging
nav-pos: 1
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

* ToC
{:toc}

## Monitoring Current Event Time

Flink's [event time]({% link dev/event_time.md %}) and watermark support are powerful features for handling
out-of-order events. However, it's harder to understand what exactly is going on because the progress of time
is tracked within the system.

Low watermarks of each task can be accessed through Flink web interface or [metrics system]({% link ops/metrics.md %}).

Each Task in Flink exposes a metric called `currentInputWatermark` that represents the lowest watermark received
by this task. This long value represents the "current event time".
The value is calculated by taking the minimum of all watermarks received by upstream operators. This means that 
the event time tracked with watermarks is always dominated by the furthest-behind source.

The low watermark metric is accessible **using the web interface**, by choosing a task in the metric tab,
and selecting the `<taskNr>.currentInputWatermark` metric. In the new box you'll now be able to see 
the current low watermark of the task.

Another way of getting the metric is using one of the **metric reporters**, as described in the documentation
for the [metrics system]({% link ops/metrics.md %}).
For local setups, we recommend using the JMX metric reporter and a tool like [VisualVM](https://visualvm.github.io/).




## Handling Event Time Stragglers

  - Approach 1: Watermark stays late (indicated completeness), windows fire early
  - Approach 2: Watermark heuristic with maximum lateness, windows accept late data

{% top %}
