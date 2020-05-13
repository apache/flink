---
title: "Set up Flink's Process Memory"
nav-parent_id: ops_mem
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

Apache Flink provides efficient workloads on top of the JVM by tightly controlling the memory usage of its various components.
While the community strives to offer sensible defaults to all configurations, the full breadth of applications
that users deploy on Flink means this isn't always possible. To provide the most production value to our users,
Flink allows both high level and fine-grained tuning of memory allocation within clusters.

* toc
{:toc}

The further described memory configuration is applicable starting with the release version *1.10* for TaskManager and
*1.11* for Master processes. If you upgrade Flink from earlier versions, check the [migration guide](mem_migration.html)
because many changes were introduced with the *1.10* and *1.11* releases.

## Configure Total Memory

The *total process memory* of Flink JVM processes consists of memory consumed by the Flink application (*total Flink memory*)
and by the JVM to run the process. The *total Flink memory* consumption includes usage of *JVM Heap* and *Off-heap*
(*Direct* or *Native*) memory.

<center>
  <img src="{{ site.baseurl }}/fig/process_mem_model.svg" width="300px" alt="Flink's process memory model" usemap="#process-mem-model">
</center>
<br />

The simplest way to setup memory in Flink is to configure either of the two following options:

| &nbsp;&nbsp;**Component**&nbsp;&nbsp; | &nbsp;&nbsp;**Option for TaskManager**&nbsp;&nbsp;                                 | &nbsp;&nbsp;**Option for Master**&nbsp;&nbsp;                                |
| :------------------------------------ | :---------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------- |
| Total Flink memory                    | [`taskmanager.memory.flink.size`](../config.html#taskmanager-memory-flink-size)     | [`jobmanager.memory.flink.size`](../config.html#jobmanager-memory-flink-size)     |
| Total process memory                  | [`taskmanager.memory.process.size`](../config.html#taskmanager-memory-process-size) | [`jobmanager.memory.process.size`](../config.html#jobmanager-memory-process-size) |
{:.table-bordered}
<br/>

<span class="label label-info">Note</span> For local execution, see detailed information for [TaskManager](mem_setup_tm.html#local-execution) and [Master](mem_setup_master.html#local-execxution) processes.

The rest of the memory components will be adjusted automatically, based on default values or additionally configured options.
See also how to set up other components for [TaskManager](mem_setup_tm.html) and [Master](mem_setup_master.html) memory.

Configuring *total Flink memory* is better suited for [standalone deployments](../deployment/cluster_setup.html)
where you want to declare how much memory is given to Flink itself. The *total Flink memory* splits up into *JVM Heap*
and *Off-heap* memory.
See also [how to configure memory for standalone deployments](mem_tuning.html#configure-memory-for-standalone-deployment).

If you configure *total process memory* you declare how much memory in total should be assigned to the Flink *JVM process*.
For the containerized deployments it corresponds to the size of the requested container, see also
[how to configure memory for containers](mem_tuning.html#configure-memory-for-containers)
([Kubernetes](../deployment/kubernetes.html), [Yarn](../deployment/yarn_setup.html) or [Mesos](../deployment/mesos.html)).

Another way to set up the memory is to configure the required internal components of the *total Flink memory* which are
specific to the concrete Flink process. Check how to configure them for [TaskManager](mem_setup_tm.html#configure-heap-and-managed-memory)
and for [Master](mem_setup_master.html#configure-jvm-heap).

<span class="label label-info">Note</span> One of the three mentioned ways has to be used to configure Flink’s memory
(except for local execution), or the Flink startup will fail. This means that one of the following option subsets,
which do not have default values, have to be configured explicitly:

| &nbsp;&nbsp;**for TaskManager:**&nbsp;&nbsp;                                                                                                                                        | &nbsp;&nbsp;**for Master:**&nbsp;&nbsp;                                      |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :-------------------------------------------------------------------------------- |
| [`taskmanager.memory.flink.size`](../config.html#taskmanager-memory-flink-size)                                                                                                       | [`jobmanager.memory.flink.size`](../config.html#jobmanager-memory-flink-size)     |
| [`taskmanager.memory.process.size`](../config.html#taskmanager-memory-process-size)                                                                                                   | [`jobmanager.memory.process.size`](../config.html#jobmanager-memory-process-size) |
| [`taskmanager.memory.task.heap.size`](../config.html#taskmanager-memory-task-heap-size) <br/> and [`taskmanager.memory.managed.size`](../config.html#taskmanager-memory-managed-size) | [`jobmanager.memory.heap.size`](../config.html#jobmanager-memory-heap-size)       |
{:.table-bordered}
<br/>

<span class="label label-info">Note</span> Explicitly configuring both *total process memory* and *total Flink memory*
is not recommended. It may lead to deployment failures due to potential memory configuration conflicts. 
Configuring other memory components also requires caution as it can produce further configuration conflicts.

## JVM Parameters

Flink explicitly adds the following memory related JVM arguments while starting its processes, based on the configured
or derived memory component sizes:

| &nbsp;&nbsp;**JVM Arguments**&nbsp;&nbsp; | &nbsp;&nbsp;**Value for TaskManager**&nbsp;&nbsp; | &nbsp;&nbsp;**Value for Master**&nbsp;&nbsp; |
| :---------------------------------------- | :------------------------------------------------- | :------------------------------------------------ |
| *-Xmx* and *-Xms*                         | Framework + Task Heap Memory                       | JVM Heap Memory                                   |
| *-XX:MaxDirectMemorySize*                 | Framework + Task Off-heap (*) + Network Memory     | Off-heap Memory (*)                               |
| *-XX:MaxMetaspaceSize*                    | JVM Metaspace                                      | JVM Metaspace                                     |
{:.table-bordered}
(*) Notice, that the native non-direct usage of memory in user code can be also accounted for as a part of the off-heap memory.
<br/><br/>

Check also the detailed memory model for [TaskManager](mem_setup_tm.html#detailed-memory-model) and
[Master](mem_setup_master.html#detailed-configuration) to understand how to configure the relevant components.

## Capped Fractionated Components

This section describes the configuration details of options which can be a fraction of some other memory size while being constrained by a min-max range:

* *JVM Overhead* can be a fraction of the *total process memory*
* *Network memory* can be a fraction of the *total Flink memory* (only for TaskManager)

Check also the detailed memory model for [TaskManager](mem_setup_tm.html#detailed-memory-model) and
[Master](mem_setup_master.html#detailed-configuration) to understand how to configure the relevant components.

The size of those components always has to be between its maximum and minimum value, otherwise Flink startup will fail.
The maximum and minimum values have defaults or can be explicitly set by corresponding configuration options.
For example, if you only set the following memory options:
- *total Process memory* = 1000Mb,
- *JVM Overhead min* = 64Mb,
- *JVM Overhead max* = 128Mb,
- *JVM Overhead fraction* = 0.1

then the *JVM Overhead* will be 1000Mb x 0.1 = 100Mb which is within the range 64-128Mb.

Notice if you configure the same maximum and minimum value it effectively fixes the size to that value.

If you do not explicitly configure the component memory, then Flink will use the fraction to calculate the memory size
based on the total memory. The calculated value is capped by its corresponding min/max options.
For example, if only the following memory options are set:
- *total Process memory* = 1000Mb,
- *JVM Overhead min* = 128Mb,
- *JVM Overhead max* = 256Mb,
- *JVM Overhead fraction* = 0.1

then the *JVM Overhead* will be 128Mb because the size derived from fraction is 100Mb, and it is less than the minimum.

It can also happen that the fraction is ignored if the sizes of the total memory and its other components are defined.
In this case, the *JVM Overhead* is the rest of the total memory. The derived value still has to be within its min/max
range otherwise the configuration fails. For example, suppose only the following memory options are set:
- *total Process memory* = 1000Mb,
- *task heap* = 100Mb, (similar example can be for *JVM Heap* in Flink Master)
- *JVM Overhead min* = 64Mb,
- *JVM Overhead max* = 256Mb,
- *JVM Overhead fraction* = 0.1

All other components of the *total Process memory* have default values, including the default *Managed Memory* fraction
(or *Off-heap* memory in Flink Master). Then the *JVM Overhead* is not the fraction (1000Mb x 0.1 = 100Mb), but the rest
of the *total Process memory* which will either be within the range 64-256Mb or fail.
