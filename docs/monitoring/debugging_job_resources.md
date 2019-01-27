---
title: "Debugging Job Resources"
nav-parent_id: monitoring
nav-pos: 16
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

In the [Distributed Runtime Environment](../concepts/runtime.html#task-slots-and-resources), we learn some basic concepts about task slots and resources. And in [TaskManager Resource](../internals/taskmanager_resource.html) we could get more information about TaskManager Resource configuration and know how to specify resources for each operator. After all of this, we come to a conclusion that quantitative resource management could help a lot in avoiding `OutOfMemory`. However, at the same time it raises the bar to debug the job scheduling problems. So we enhance some web pages of Flink dashboard to make debugging easier.

## Resource Overview

<img src="{{ site.baseurl }}/fig/debugging_job_resources_overview.png" class="img-responsive">

As shown in the figure above, the resource section of Flink cluster overview contains total/available slots and total/available resources(cpu cores, heap memory, direct memory, native memory, managed memory and network memory). 
* In session mode, the total resources are calculated based on the configuration. The total resources minus the resources already used by the running job, the remaining is available resources. We strongly encourage that in the same session all jobs set resources or no one set it up.
* In per-job mode, if no operator has set the resources, the total and available resources are both zero. Otherwise, the total resources are the summation of all operator resources. And the available resources are zero or close to zero. It is just because one or more slot requests will be combined into a Yarn container or kubernetes pod and allocate from resource management framework on demand.  


## Pending Slots

<img src="{{ site.baseurl }}/fig/debugging_job_resources_pending_slots.png" class="img-responsive">

Each `SlotRequest` has a corresponding `ResourceProfile` which describes resource requirements of tasks. As shown in the figure above, all the requested but not fulfilled slots will show in a list of pending slots. The request time and resources of each pending slot could also be found.

## TaskManagers

<img src="{{ site.baseurl }}/fig/debugging_job_resources_taskmanagers.png" class="img-responsive">

Each TaskManager represents a subset of resources of the Flink cluster. A slot can only be assigned to one TaskManager and allocation across multiple TaskManagers is not possible. When we submit a job to an existing session, even though the Flink cluster have enough resources to fulfill the pending `SlotRequest`, it may not be allocated due to resource fragmentation. So we need the TaskManagers resources list(including total/available amounts of all kinds of resources) to help diagnose this situation.

## Procedure of diagnosis
1. Visit `Running Jobs Page` of Flink dashboard to check whether all tasks are running. If not, go to the next step.
2. Click the job name to visit `Job Details Page` and switch to the `Exceptions` tab. Check whether the `Root Exceptions` or `Exception History` is empty. If it is empty, just go to the next step. Otherwise, please try to fix all these exceptions besides `TimeoutException` before continuing.
3. Switch to the `Pending Slots` tab and check whether the pending list is empty. If not, the not running tasks may be due to insufficient resources. 
    * In per-job mode, we should inspect the resource quota limit of resource management cluster, such as [queue limit in Yarn](http://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/CapacityScheduler.html) or [ResourceQuota in kubernetes](https://kubernetes.io/docs/concepts/policy/resource-quotas/).
    * In session mode, just move on to next step.
4. Return to the `Overview Page` and check if all kinds of available resources are enough to fulfill the pending slots. As shown in the figure 2, each pending slot request needs the resources of <0.1 Core, 32MB Direct Memory, 256MB Heap Memory, 32MB Native Memory>. The Available UserNative memory is zero in figure 1. So the two `SlotRequest`s could not be fulfilled and remain pending.
5. There is another scenario, all the kinds of resources are enough, however, the slot could not be allocated. Probably it is due to resource fragmentation. Visit the `Task Managers Page` and we should find that no TaskManager has enough available resources to fulfill the pending slot requests.
6. In both cases above, we need to increase cluster resources(scale up/out TaskManagers) or reducing operator resources request.


{% top %}
