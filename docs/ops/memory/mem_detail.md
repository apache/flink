---
title: "Detailed Memory Model"
nav-parent_id: ops_mem
nav-pos: 2
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

This section gives a detailed description of all components in Flink’s memory model of task executor.
Check [memory configuration guide](mem_setup.html) for the basic memory setup.

* toc
{:toc}

## Overview

<br />
<center>
  <img src="{{ site.baseurl }}/fig/detailed-mem-model.svg" width="300px" alt="Simple memory model" usemap="#simple-mem-model">
</center>
<br />

The following table lists all memory components, depicted above, and references Flink configuration options
which affect the size of the respective components:

| &nbsp;&nbsp;**Component**&nbsp;&nbsp;                                             | &nbsp;&nbsp;**Configuration options**&nbsp;&nbsp;                                                                                                                                                                                                                                     | &nbsp;&nbsp;**Description**&nbsp;&nbsp;                                                                                                                                                                                                                                  |
| :-------------------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Framework Heap Memory](#framework-memory)                                        | [`taskmanager.memory.framework.heap.size`](../config.html#taskmanager-memory-framework-heap-size)                                                                                                                                                                                       | JVM heap memory dedicated to Flink framework (advanced option)                                                                                                                                                                                                           |
| [Task Heap Memory](mem_setup.html#task-operator-heap-memory)                      | [`taskmanager.memory.task.heap.size`](../config.html#taskmanager-memory-task-heap-size)                                                                                                                                                                                                 | JVM heap memory dedicated to Flink application to run operators and user code                                                                                                                                                                                            |
| [Managed memory](mem_setup.html#managed-memory)                                   | [`taskmanager.memory.managed.size`](../config.html#taskmanager-memory-managed-size) <br/> [`taskmanager.memory.managed.fraction`](../config.html#taskmanager-memory-managed-fraction)                                                                                                     | Native memory managed by Flink, reserved for sorting, hash tables, caching of intermediate results and RocksDB state backend                                                                                                                                             |
| [Framework Off-heap Memory](#framework-memory)                                    | [`taskmanager.memory.framework.off-heap.size`](../config.html#taskmanager-memory-framework-off-heap-size)                                                                                                                                                                               | [Off-heap direct (or native) memory](mem_setup.html#configure-off-heap-memory-direct-or-native) dedicated to Flink framework (advanced option)                                                                                                                           |
| [Task Off-heap Memory](mem_setup.html#configure-off-heap-memory-direct-or-native) | [`taskmanager.memory.task.off-heap.size`](../config.html#taskmanager-memory-task-off-heap-size)                                                                                                                                                                                         | [Off-heap direct (or native) memory](mem_setup.html#configure-off-heap-memory-direct-or-native) dedicated to Flink application to run operators                                                                                                                          |
| Network Memory                                                                    | [`taskmanager.memory.network.min`](../config.html#taskmanager-memory-network-min) <br/> [`taskmanager.memory.network.max`](../config.html#taskmanager-memory-network-max) <br/> [`taskmanager.memory.network.fraction`](../config.html#taskmanager-memory-network-fraction)                 | Direct memory reserved for data record exchange between tasks (e.g. buffering for the transfer over the network), it is a [capped fractionated component](#capped-fractionated-components) of the [total Flink memory](mem_setup.html#configure-total-memory)            |
| [JVM metaspace](#jvm-parameters)                                                  | [`taskmanager.memory.jvm-metaspace.size`](../config.html#taskmanager-memory-jvm-metaspace-size)                                                                                                                                                                                         | Metaspace size of the Flink JVM process                                                                                                                                                                                                                                  |
| JVM Overhead                                                                      | [`taskmanager.memory.jvm-overhead.min`](../config.html#taskmanager-memory-jvm-overhead-min) <br/> [`taskmanager.memory.jvm-overhead.max`](../config.html#taskmanager-memory-jvm-overhead-max) <br/> [`taskmanager.memory.jvm-overhead.fraction`](../config.html#taskmanager-memory-jvm-overhead-fraction) | Native memory reserved for other JVM overhead: e.g. thread stacks, code cache, garbage collection space etc, it is a [capped fractionated component](#capped-fractionated-components) of the [total process memory](mem_setup.html#configure-total-memory) |
{:.table-bordered}
<br/>

As you can see, the size of some memory components can be simply set by the respective option.
Other components can be tuned using multiple options.

## Framework Memory

The *framework heap memory* and *framework off-heap memory* options are not supposed to be changed without a good reason.
Adjust them only if you are sure that Flink needs more memory for some internal data structures or operations.
It can be related to a particular deployment environment or job structure, like high parallelism.
In addition, Flink dependencies, such as Hadoop may consume more direct or native memory in certain setups.

<span class="label label-info">Note</span> Neither heap nor off-heap versions of framework and task memory are currently isolated within Flink.
The separation of framework and task memory can be used in future releases for further optimizations.

## Capped Fractionated Components

This section describes the configuration details of the following options which can be a fraction of a certain
[total memory](mem_setup.html#configure-total-memory):

* *Network memory* can be a fraction of the *total Flink memory*
* *JVM overhead* can be a fraction of the *total process memory*

See also [detailed memory model](#overview).

The size of those components always has to be between its maximum and minimum value, otherwise Flink startup will fail.
The maximum and minimum values have defaults or can be explicitly set by corresponding configuration options.
For example, if only the following memory options are set:
- total Flink memory = 1000Mb,
- network min = 64Mb,
- network max = 128Mb,
- network fraction = 0.1

then the network memory will be 1000Mb x 0.1 = 100Mb which is within the range 64-128Mb.

Notice if you configure the same maximum and minimum value it effectively means that its size is fixed to that value.

If the component memory is not explicitly configured, then Flink will use the fraction to calculate the memory size
based on the total memory. The calculated value is capped by its corresponding min/max options.
For example, if only the following memory options are set:
- total Flink memory = 1000Mb,
- network min = 128Mb,
- network max = 256Mb,
- network fraction = 0.1

then the network memory will be 128Mb because the size derived from fraction is 100Mb and it is less than the minimum.

It can also happen that the fraction is ignored if the sizes of the total memory and its other components are defined.
In this case, the network memory is the rest of the total memory. The derived value still has to be within its min/max
range otherwise the configuration fails. For example, suppose only the following memory options are set:
- total Flink memory = 1000Mb,
- task heap = 100Mb,
- network min = 64Mb,
- network max = 256Mb,
- network fraction = 0.1

All other components of the total Flink memory have default values, including the default managed memory fraction.
Then the network memory is not the fraction (1000Mb x 0.1 = 100Mb) but the rest of the total Flink memory
which will either be within the range 64-256Mb or fail.

## JVM Parameters

Flink explicitly adds the following memory related JVM arguments while starting the task executor process,
based on the configured or derived memory component sizes:

| &nbsp;&nbsp;**JVM Arguments**&nbsp;&nbsp; | &nbsp;&nbsp;**Value**&nbsp;&nbsp;          |
| :---------------------------------------- | :----------------------------------------- |
| *-Xmx* and *-Xms*                         | Framework + Task Heap Memory               |
| *-XX:MaxDirectMemorySize*                 | Framework + Task Off-Heap + Network Memory |
| *-XX:MaxMetaspaceSize*                    | JVM Metaspace                              |
{:.table-bordered}
<br/>

See also [detailed memory model](#overview).

## Local Execution
If you start Flink locally on your machine as a single java program without creating a cluster (e.g. from your IDE)
then all components are ignored except for the following:

| &nbsp;&nbsp;**Memory component**&nbsp;&nbsp; | &nbsp;&nbsp;**Relevant options**&nbsp;&nbsp;                                                  | &nbsp;&nbsp;**Default value for the local execution**&nbsp;&nbsp;             |
| :------------------------------------------- | :-------------------------------------------------------------------------------------------- | :---------------------------------------------------------------------------- |
| Task heap                                    | [`taskmanager.memory.task.heap.size`](../config.html#taskmanager-memory-task-heap-size)         | infinite                                                                      |
| Task off-heap                                | [`taskmanager.memory.task.off-heap.size`](../config.html#taskmanager-memory-task-off-heap-size) | infinite                                                                      |
| Managed memory                               | [`taskmanager.memory.managed.size`](../config.html#taskmanager-memory-managed-size)             | 128Mb                                                                         |
| Network memory                               | [`taskmanager.memory.network.min`](../config.html#taskmanager-memory-network-min) <br /> [`taskmanager.memory.network.max`](../config.html#taskmanager-memory-network-max) | 64Mb |
{:.table-bordered}
<br/>

All of the components listed above can be but do not have to be explicitly configured for the local execution.
If they are not configured they are set to their default values. [Task heap memory](mem_setup.html#task-operator-heap-memory) and
*task off-heap memory* are considered to be infinite (*Long.MAX_VALUE* bytes) and [managed memory](mem_setup.html#managed-memory)
has a default value of 128Mb only for the local execution mode.

<span class="label label-info">Note</span> The task heap size is not related in any way to the real heap size in this case.
It can become relevant for future optimizations coming with next releases. The actual JVM heap size of the started
local process is not controlled by Flink and depends on how you start the process.
If you want to control the JVM heap size you have to explicitly pass the corresponding JVM arguments, e.g. *-Xmx*, *-Xms*.
