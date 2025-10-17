---
title: Tasks Balanced Scheduling
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

# Tasks Balanced Scheduling

This page describes the background and principle of tasks balanced scheduling, 
how to use it when running streaming jobs.

## Background

When the parallelism of all vertices within a Flink streaming job is inconsistent,
the [default strategy]({{< ref "docs/deployment/config" >}}#taskmanager-load-balance-mode)
of Flink to deploy tasks sometimes leads some `TaskManagers` have more tasks while others have fewer tasks, 
resulting in excessive resource utilization at some `TaskManagers` 
that contain more tasks and becoming a bottleneck for the entire job processing.

{{< img src="/fig/deployments/tasks-scheduling/skew_tasks_allocation_case.svg" alt="The case of tasks allocation skew" class="offset" width="55%" >}}

As shown in figure (a), given a Flink job comprising two vertices, `JobVertex-A (JV-A)` and `JobVertex-B (JV-B)`, 
with parallelism degrees of `6` and `3` respectively,
and both vertices sharing the same slot sharing group.
Under the default scheduling strategy, as illustrated in figure (b), 
the distribution of tasks across `TaskManagers` may result in significant disparities in task load. 
Specifically, the `TaskManager`s with the highest number of tasks may host `4` tasks, 
while the one with the lowest load may have only `2` tasks. 
Consequently, the `TaskManager`s bearing 4 tasks is prone to become a performance bottleneck for the entire job.

Therefore, Flink provides a task-number-based balanced scheduling capability. 
Within the job's resource view, it aims to ensure that the number of tasks 
scheduled to each `TaskManager` is as close as possible, thereby improving the resource usage skew among `TaskManagers`.

## Principle

The task-quantity-based load balancing scheduling strategy completes the assignment of tasks to `TaskManagers` in two phases: 
- The tasks-to-slots assignment phase 
- The slots-to-TaskManagers assignment phase

This section will use two examples to illustrate the simplified process and principle of 
how the task-quantity-based scheduling strategy handles the assignments in these two phases.

### The tasks-to-slots assignment phase

Taking the job shown in figure (c) as an example, it contains five job vertices with parallelism degrees of `1`, `4`, `4`, `2`, and `3`, respectively.
All five job vertices belong to the default slot sharing group.  

{{< img src="/fig/deployments/tasks-scheduling/principle_tasks_to_slots_allocation.svg" alt="The tasks to slots allocation demo" class="offset" width="65%" >}}

During the tasks-to-slots assignment phase, this scheduling strategy:  
- First directly assigns the tasks of the vertices with the highest parallelism to the `i-th` slot. 

  That is, task `JV-Bi` is assigned directly to `sloti`, and task `JV-Ci` is assigned directly to `sloti`.

- Next, for tasks belonging to job vertices with sub-maximal parallelism, they are assigned in a round-robin fashion across the slots within the current
slot sharing group until all tasks are allocated.

As shown in figure (e), under the task-quantity-based assignment strategy, the range (max-min difference) of the number of tasks per slot is `1`, 
which is better than the range of `3` under the default strategy shown in figure (d).

Thus, this ensures a more balanced distribution of the number of tasks across slots.

### The slots-to-TaskManagers assignment phase

As shown in figure (f), given a Flink job comprising two vertices, `JV-A` and `JV-B`, with parallelism of `6` and `3` respectively,
and both vertices sharing the same slot sharing group.

{{< img src="/fig/deployments/tasks-scheduling/principle_slots_to_taskmanagers_allocation.svg" alt="The slots to TaskManagers allocation demo" class="offset" width="75%" >}}

The assignment result after the first phase is shown in figure (g), 
where `Slot0`, `Slot1`, and `Slot2` each contain `2` tasks, while the remaining slots contain `1` task each.

Subsequently:
- the strategy sorts slot requests in descending order based on the number of tasks they contain and submits the requests, 
waiting until all required slot resources for the current job are ready. 

Once the slot resources are ready:  
- the strategy sequentially assigns each slot request to the `TaskManager` with the smallest current task load, 
continuing this process until all tasks are allocated.

The final assignment result is shown in figure (i), where each `TaskManager` ends up with exactly `3` tasks, 
resulting in a task count difference of `0` between `TaskManagers`. In contrast, the scheduling result under the default strategy, 
shown in figure (h), has a task count difference of `2` between `TaskManagers`. 

Therefore, using this load balancing scheduling strategy could effectively mitigate the issue of 
resource usage skew caused by significant disparities in the number of tasks across `TaskManagers`.

## Usage

You can enable tasks balanced scheduling through the following configuration item: 

- `taskmanager.load-balance.mode`: `tasks`

{{< top >}}
