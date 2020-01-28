---
title: "Task Executor Memory Model"
nav-parent_id: internals
nav-pos: 11
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

This document describes the memory model of Flink [TaskExecutor]({{ site.baseurl }}/concepts/glossary.html#taskexecutor).

* Replaced by the TOC
{:toc}

# Memory Breakdown

Flink breaks memory usages of a TaskExecutor process down to the following components.
- **Framework Heap Memory** - JVM heap memory reserved for TaskExecutor framework.
- **Framework Off-Heap Memory** - Direct and native memory reserved for TaskExecutor framework.
- **Task Heap Memory** - JVM heap memory reserved for [tasks]({{ site.baseurl }}/concepts/glossary.html#task).
- **Task Off-Heap Memory** - Direct and native memory reserved for tasks.
- **Network Memory** - Direct memory reserved for ShuffleEnvironment (e.g., network buffers).
- **Managed Memory** - Native memory managed by the Flink MemoryManager, reserved for sorting, hash tables, caching of intermediate results and [RocksDB state backend]({{ site.baseurl }}/ops/state/state_backends.html#the-rocksdbstatebackend).
- **JVM Metaspace** - JVM metaspace size.
- **JVM Overhead** - Native memory reserved for other JVM overheads, e.g., thread stacks, code cache, garbage collection space.

For simplicity when configuring Flink, the term **Process Memory** refers to the total memory usage of a TaskExecutor, and **Flink Memory** refers to that excluding JVM Metaspace and JVM Overhead.

<div style="text-align: center;">
<img src="{{ site.baseurl }}/fig/tm_memory_model.svg" alt="Task Executor Memory Model" height="400px" style="text-align: center;"/>
</div>

Note: There’s no isolation between Framework Heap Memory and Task Heap Memory, neither between Framework Off-Heap Memory and Task Off-Heap Memory.
The separation of the framework and task memory is in preparation for dynamic slot allocation, an upcoming feature that will allow task memory to be allocated to slots while framework memory will not.

# Memory Configuration

For all types of deployments, except for local execution, Flink requires either of the following three (combination of) memory sizes to be explicitly configured.
- Task Heap Memory and Managed Memory
- Flink Memory
- Process Memory

Note: Explicitly configuring more than one of the above three (combination of) memory sizes is not recommended.
It may lead to deployment failures due to potential memory configuration conflicts.

The following memory components will always be explicitly configured.
If no configuration changes are made the following default values will be used.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 40%">Memory Component</th>
      <th class="text-left" style="width: 60%">Configuration Option</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>Framework Heap Memory</td>
      <td>‘taskmanager.memory.framework.heap.size’ (default 128MB)</td>
    </tr>
    <tr>
      <td>Framework Off-Heap Memory</td>
      <td>‘taskmanager.memory.framework.off-heap.size’ (default 128MB)</td>
    </tr>
    <tr>
      <td>Task Off-Heap Memory</td>
      <td>‘taskmanager.memory.task.off-heap.size’ (default 0)</td>
    </tr>
    <tr>
      <td>JVM Metaspace</td>
      <td>‘taskmanager.memory.jvm-metaspace.size’ (default 96MB)</td>
    </tr>
  </tbody>
</table>

The following components will always be allocated memory within a specified range and targeting a specific amount, specified as a fraction of total memory, on a best effort basis.
If the ranges and fractions are not explicitly specified by the user, the following default values will be used.
Note that the fractions may not always be respected even if explicitly configured.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Memory Component</th>
      <th class="text-left" style="width: 50%">Configuration Option</th>
      <th class="text-left" style="width: 25%">Base of Fraction</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td rowspan="3">Network Memory</td>
      <td>‘taskmanager.memory.network.min’ (default 64MB)</td>
      <td rowspan="3">Flink Memory</td>
    </tr>
    <tr>
      <td>‘taskmanager.memory.network.max’ (default 1GB)</td>
    </tr>
    <tr>
      <td>‘taskmanager.memory.network.fraction’’ (default 0.1)</td>
    </tr>
    <tr>
      <td rowspan="3">JVM Overhead</td>
      <td>‘taskmanager.memory.jvm-overhead.min’ (default 192MB)</td>
      <td rowspan="3">Process Memory</td>
    </tr>
    <tr>
      <td>‘taskmanager.memory.jvm-overhead.max’ (default 1GB)</td>
    </tr>
    <tr>
      <td>‘taskmanager.memory.jvm-overhead.fraction’ (default 0.1)</td>
    </tr>
  </tbody>
</table>

Imagine Flink Memory is 1000MB and Network Memory is configured to be in the range of 64MB to 1GB with a fraction of 0.08.
In this case, Flink will attempt to allocate 80MB for Network Memory but guarantee the runtime amount is within the range of 64MB to 1GB.  

The size of Managed Memory can either be configured directly (‘taskmanager.memory.managed.size’), or derived to make up the configured fraction (‘taskmanager.memory.managed.fraction’) of Flink Memory.
If both size and fraction are explicitly specified by user, the size will be used.
If neither of size and fraction is explicitly configured, it will be derived with the default fraction (0.4).

The size of Task Heap Memory can be configured directly (‘taskmanager.memory.task.heap.size’).
If not explicitly specified by user, it will use whatever left in Flink Memory after sizes of other memory components are decided.

Users can also directly configure the size of Process Memory (‘taskmanager.memory.process.size’) or Flink Memory (‘taskmanager.memory.flink.size’).
Flink will then automatically derive the memory component sizes.

Note: All the explicitly configured sizes, default sizes (if a default absolute size exists), min/max constraints and the Managed Memory fraction (if the size is not specified) will be respected (excluding Network Memory and JVM Overhead fractions).
Flink cannot be deployed and exceptions will be thrown on conflictions.

## Memory Configuration for Local Execution

In local execution environments, all TaskExecutors run in the same JVM process that the program is executed.
Therefore, the configuration cannot affect the JVM heap size, direct memory/metaspace limits, and the overall memory reserved for the process.
In other words, configurations on the following memory components will not take effect.
- Framework Heap Memory
- Framework Off-Heap Memory
- Task Heap Memory
- Task Off-Heap Memory
- JVM Metaspace
- JVM Overhead

Unless explicitly configured, Flink will internally set the sizes of Network Memory and Managed Memory to 64MB and 16MB respectively, for the purpose of trying out Flink locally with less memory consumption. 

# JVM Parameters
Flink explicitly sets the following memory related JVM parameters for TaskExecutor processes, w.r.t. the configured / derived memory component sizes.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 30%">JVM Parameters</th>
      <th class="text-left" style="width: 70%">Value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>-Xmx / -Xms</td>
      <td>Framework Heap Memory + Task Heap Memory</td>
    </tr>
    <tr>
      <td>-XX:MaxDirectMemorySize</td>
      <td>Framework Off-Heap Memory + Task Off-Heap Memory + Network Memory</td>
    </tr>
    <tr>
      <td>-XX:MaxMetaspaceSize</td>
      <td>JVM Metaspace</td>
    </tr>
  </tbody>
</table>
