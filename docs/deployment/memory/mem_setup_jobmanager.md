---
title: "Set up JobManager Memory"
nav-parent_id: ops_mem
nav-pos: 3
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

The JobManager is the controlling element of the Flink Cluster. 
It consists of three distinct components: Resource Manager, Dispatcher and one JobMaster per running Flink Job.
This guide walks you through high level and fine-grained memory configurations for the JobManager.

* toc
{:toc}

The further described memory configuration is applicable starting with the release version *1.11*. If you upgrade Flink
from earlier versions, check the [migration guide]({% link deployment/memory/mem_migration.md %}) because many changes were introduced with the *1.11* release.

<span class="label label-info">Note</span> This memory setup guide is relevant <strong>only for the JobManager</strong>!
The JobManager memory components have a similar but simpler structure compared to the [TaskManagers' memory configuration]({% link deployment/memory/mem_setup_tm.md %}).

## Configure Total Memory

The simplest way to set up the memory configuration is to configure the [total memory]({% link deployment/memory/mem_setup.md %}#configure-total-memory) for the process.
If you run the JobManager process using local [execution mode](#local-execution) you do not need to configure memory options, they will have no effect.

## Detailed configuration

<center>
  <img src="{% link /fig/process_mem_model.svg %}" width="300px" alt="Flink's process memory model" usemap="#process-mem-model">
</center>
<br />

The following table lists all memory components, depicted above, and references Flink configuration options which
affect the size of the respective components:

| &nbsp;&nbsp;**Component**&nbsp;&nbsp;                          | &nbsp;&nbsp;**Configuration options**&nbsp;&nbsp;                                                                                                                                                                                                                                                   | &nbsp;&nbsp;**Description**&nbsp;&nbsp;                                                                                                                                                                                                                                  |
| :------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [JVM Heap](#configure-jvm-heap)                                | [`jobmanager.memory.heap.size`]({% link deployment/config.md %}#jobmanager-memory-heap-size)                                                                                                                                                                                                                         | *JVM Heap* memory size for job manager.                                                                                                                                                                                                                                  |
| [Off-heap Memory](#configure-off-heap-memory)                  | [`jobmanager.memory.off-heap.size`]({% link deployment/config.md %}#jobmanager-memory-off-heap-size)                                                                                                                                                                                                                 | *Off-heap* memory size for job manager. This option covers all off-heap memory usage including direct and native memory allocation.                                                                                                                                      |
| [JVM metaspace]({% link deployment/memory/mem_setup.md %}#jvm-parameters)                 | [`jobmanager.memory.jvm-metaspace.size`]({% link deployment/config.md %}#jobmanager-memory-jvm-metaspace-size)                                                                                                                                                                                                       | Metaspace size of the Flink JVM process                                                                                                                                                                                                                                  |
| JVM Overhead                                                   | [`jobmanager.memory.jvm-overhead.min`]({% link deployment/config.md %}#jobmanager-memory-jvm-overhead-min) <br/> [`jobmanager.memory.jvm-overhead.max`]({% link deployment/config.md %}#jobmanager-memory-jvm-overhead-max) <br/> [`jobmanager.memory.jvm-overhead.fraction`]({% link deployment/config.md %}#jobmanager-memory-jvm-overhead-fraction) | Native memory reserved for other JVM overhead: e.g. thread stacks, code cache, garbage collection space etc, it is a [capped fractionated component]({% link deployment/memory/mem_setup.md %}#capped-fractionated-components) of the [total process memory]({% link deployment/memory/mem_setup.md %}#configure-total-memory) |
{:.table-bordered}
<br/>

### Configure JVM Heap

As mentioned before in the [total memory description]({% link deployment/memory/mem_setup.md %}#configure-total-memory), another way to set up the memory
for the JobManager is to specify explicitly the *JVM Heap* size ([`jobmanager.memory.heap.size`]({% link deployment/config.md %}#jobmanager-memory-heap-size)).
It gives more control over the available *JVM Heap* which is used by:

* Flink framework
* User code executed during job submission (e.g. for certain batch sources) or in checkpoint completion callbacks

The required size of *JVM Heap* is mostly driven by the number of running jobs, their structure, and requirements for
the mentioned user code.

<span class="label label-info">Note</span> If you have configured the *JVM Heap* explicitly, it is recommended to set
neither *total process memory* nor *total Flink memory*. Otherwise, it may easily lead to memory configuration conflicts.

The Flink scripts and CLI set the *JVM Heap* size via the JVM parameters *-Xms* and *-Xmx* when they start the JobManager process, see also [JVM parameters]({% link deployment/memory/mem_setup.md %}#jvm-parameters).

### Configure Off-heap Memory

The *Off-heap* memory component accounts for any type of *JVM direct memory* and *native memory* usage. Therefore,
you can also enable the *JVM Direct Memory* limit by setting the [`jobmanager.memory.enable-jvm-direct-memory-limit`]({% link deployment/config.md %}#jobmanager-memory-enable-jvm-direct-memory-limit) option.
If this option is configured, Flink will set the limit to the *Off-heap* memory size via the corresponding JVM argument: *-XX:MaxDirectMemorySize*.
See also [JVM parameters]({% link deployment/memory/mem_setup.md %}#jvm-parameters).

The size of this component can be configured by [`jobmanager.memory.off-heap.size`]({% link deployment/config.md %}#jobmanager-memory-off-heap-size)
option. This option can be tuned e.g. if the JobManager process throws ‘OutOfMemoryError: Direct buffer memory’, see
[the troubleshooting guide]({% link deployment/memory/mem_trouble.md %}#outofmemoryerror-direct-buffer-memory) for more information.

There can be the following possible sources of *Off-heap* memory consumption:

* Flink framework dependencies (e.g. Akka network communication)
* User code executed during job submission (e.g. for certain batch sources) or in checkpoint completion callbacks

<span class="label label-info">Note</span> If you have configured the [Total Flink Memory]({% link deployment/memory/mem_setup.md %}#configure-total-memory)
and the [JVM Heap](#configure-jvm-heap) explicitly but you have not configured the *Off-heap* memory, the size of the *Off-heap* memory
will be derived as the [Total Flink Memory]({% link deployment/memory/mem_setup.md %}#configure-total-memory) minus the [JVM Heap](#configure-jvm-heap).
The default value of the *Off-heap* memory option will be ignored.

## Local Execution

If you run Flink locally (e.g. from your IDE) without creating a cluster, then the JobManager memory configuration options are ignored.
