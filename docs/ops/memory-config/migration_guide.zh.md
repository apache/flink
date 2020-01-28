---
title: "迁移指南"
nav-title: 迁移指南
nav-parent_id: ops_mem_config
nav-pos: 1
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in complian
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

This document gives guidelines on how to migrate memory configurations from a previous version (Flink 1.9 and below), to the current [TaskExecutors memory model]({{ site.baseurl }}/internals/memory_model.html) (Flink 1.10 and above).
Please refer to [configuration guidelines]({{ site.baseurl }}/ops/memory-config/config_guide.html) for new jobs that are not migrated from previous versions.

* toc
{:toc}

## Changes to be Noticed

### Managed Memory

Managed Memory is now always off-heap.
The configuration option ‘taskmanager.memory.off-heap’ has been removed.
In addition, the off-heap Managed Memory now uses native memory rather than direct memory, which means it is no longer accounted for in JVM max direct memory limits.

Managed Memory is always lazily allocated now and the configuration option ‘taskmanager.memory.preallocate’ has been removed.

RocksDBStatebackend now uses Managed Memory.

### Container Cut Off Memory

For the containerized deployments, TaskExecutors no long have cut-off memory.
The configuration options ‘containerized.heap-cutoff-ratio’ and ‘containerized.heap-cutoff-min’ do not affect TaskExecutors now.

The JobManager still does have container cut off memory and the configuration options work the same way as before.

### Configuration Options

The table below lists all the deprecated and removed TaskExecutor memory configuration options.
For the deprecated options, the table also shows which new option it will be interpreted as if used.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 50%">Deprecated / Removed Option</th>
      <th class="text-left" style="width: 50%">Interpreted as New Option</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td rowspan="2">taskmanager.heap.size</td>
      <td>Standalone: taskmanager.memory.flink.size</td>
    </tr>
    <tr>
      <td>Containerized: taskmanager.memory.process.size</td>
    </tr>
    <tr>
      <td>taskmanager.memory.fraction</td>
      <td>none</td>
    </tr>
    <tr>
      <td>taskmanager.memory.off-heap</td>
      <td>none</td>
    </tr>
    <tr>
      <td>taskmanager.memory.preallocate</td>
      <td>none</td>
    </tr>
    <tr>
      <td>taskmanager.memory.size</td>
      <td>taskmanager.memory.managed.size</td>
    </tr>
    <tr>
      <td>taskmanager.network.memory.fraction</td>
      <td>taskmanager.memory.network.fraction</td>
    </tr>
    <tr>
      <td>taskmanager.network.memory.max</td>
      <td>taskmanager.memory.network.max</td>
    </tr>
    <tr>
      <td>taskmanager.network.memory.min</td>
      <td>taskmanager.memory.network.min</td>
    </tr>
  </tbody>
</table>

Note: If ‘taskmanager.heap.size’ is configured, it will be interpreted differently on standalone and containerized deployments (Yarm/Mesos/Kubernetes).
The configured size will be set to Flink Memory on standalone deployment, and Process Memory on containerized deployments.
This aligns with the previous behaviors.

Note: A new configuration option ‘taskmanager.memory.managed.fraction’ is introduced.
However, it is not equivalent with the removed option ‘taskmanager.memory.fraction’, and should not directly use the value previously configured for the removed fraction.
To be specific, the new option defines the fraction of Flink Memory that should be reserved for Managed Memory, while the old option defines the fraction of total memory (‘taskmanager.heap.size’) mius network memory (and container cut off memory on containerized deployments) for Managed Memory.

### Out-of-Box Configurations

In the default ‘flink-conf.yaml’, ‘taskmanager.heap.size’ is replaced by ‘taskmanager.memory.process.size’, with the value increased from 1024MB to 1568MB.

As an integrated outcome of making Managed Memory always off-heap, increasing default Process Memory size, and removing container cut off memory, Flink’s out-of-box configuration now results in different overall and component individual memory sizes on TaskExecutors.
Performance regression could be observed in some scenarios.

A detailed comparison of out-of-box overall and component individual memory sizes before and after FLIP-49 can be found in the [calculation spreadsheet](https://docs.google.com/spreadsheets/d/1mJaMkMPfDJJ-w6nMXALYmTc4XxiV30P5U7DzgwLkSoE/edit?usp=sharing).

## Migration Tips

- Use the [calculation spreadsheet](https://docs.google.com/spreadsheets/d/1mJaMkMPfDJJ-w6nMXALYmTc4XxiV30P5U7DzgwLkSoE/edit?usp=sharing) to get the overall and individual component memory sizes from the old configuration.
- To have the same overall memory size, one can try to set the previous configured value of ‘taskmanager.heap.size’ to Flink Memory on standalone deployment, and to Process Memory on containerized deployments.
- To have the same JVM heap size, one can try to configure Framework Heap Memory and Task Heap Memory, so that these two added together have the size of previous JVM heap size.
- Managed Memory
  - For streaming jobs, they should not use managed memory in the previous model. If managed memory was configured non-zero, off-heap and not preallocated (reserving for some other off-heap memory usages), one could try to configure this memory to Framework Off-Heap Memory or Task Off-Heap Memory if MemoryStateBackend or FsStateBackend is used, or keep it for Managed Memory if RocksDBStateBackend is used. Otherwise, it is recommended to simply configure the Managed Memory size to 0.
  - For batch jobs, it is recommended to keep the same size of Managed Memory.
- For Network Memory, the configuration options are not changed a lot. However, one should be aware that the same configured values do not necessarily lead to the same size of Network Memory, due to the base of Network Memory fraction could be different.
- Container cut off memory (on containerized deployments only)
  - For streaming jobs with RocksDBStateBackend, it is recommended to configure the majority of previous container cut off memory to Managed Memory, leaving only the default JVM Metaspace and JVM Overhead.
  - For other jobs not using RocksDBStateBackend, it is recommended to configure the majority of previous container cut off memory to JVM Overhead, leaving only the default JVM Metaspace. None of the container cut off memory should go to Managed Memory in these use cases.
