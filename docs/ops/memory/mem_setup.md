---
title: "Set up Task Executor Memory"
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

The further described memory configuration is applicable starting with the release version *1.10*. If you upgrade Flink
from earlier versions, check the [migration guide](mem_migration.html) because many changes were introduced with the *1.10* release.

<span class="label label-info">Note</span> This memory setup guide is relevant <strong>only for task executors</strong>!
Check [job manager related configuration options](../config.html#jobmanager-heap-size) for the memory setup of job manager.

## Configure Total Memory

The *total process memory* of Flink JVM processes consists of memory consumed by Flink application (*total Flink memory*)
and by the JVM to run the process. The *total Flink memory* consumption includes usage of JVM heap,
*managed memory* (managed by Flink) and other direct (or native) memory.

<center>
  <img src="{{ site.baseurl }}/fig/simple_mem_model.svg" width="300px" alt="Simple memory model" usemap="#simple-mem-model">
</center>
<br />

If you run Flink locally (e.g. from your IDE) without creating a cluster, then only a subset of the memory configuration
options are relevant, see also [local execution](mem_detail.html#local-execution) for more details.

Otherwise, the simplest way to setup memory in Flink is to configure either of the two following options:
* Total Flink memory ([`taskmanager.memory.flink.size`](../config.html#taskmanager-memory-flink-size))
* Total process memory ([`taskmanager.memory.process.size`](../config.html#taskmanager-memory-process-size))

The rest of the memory components will be adjusted automatically, based on default values or additionally configured options.
[Here](mem_detail.html) are more details about the other memory components.

Configuring *total Flink memory* is better suited for standalone deployments where you want to declare how much memory
is given to Flink itself. The *total Flink memory* splits up into JVM heap, [managed memory size](#managed-memory)
and *direct memory*.

If you configure *total process memory* you declare how much memory in total should be assigned to the Flink *JVM process*.
For the containerized deployments it corresponds to the size of the requested container, see also
[how to configure memory for containers](mem_tuning.html#configure-memory-for-containers)
([Kubernetes](../deployment/kubernetes.html), [Yarn](../deployment/yarn_setup.html) or [Mesos](../deployment/mesos.html)).

Another way to setup the memory is to set [task heap](#task-operator-heap-memory) and [managed memory](#managed-memory)
([`taskmanager.memory.task.heap.size`](../config.html#taskmanager-memory-task-heap-size) and [`taskmanager.memory.managed.size`](../config.html#taskmanager-memory-managed-size)).
This more fine-grained approach is described in more detail [here](#configure-heap-and-managed-memory).

<span class="label label-info">Note</span> One of the three mentioned ways has to be used to configure Flink’s memory (except for local execution), or the Flink startup will fail.
This means that one of the following option subsets, which do not have default values, have to be configured explicitly:
* [`taskmanager.memory.flink.size`](../config.html#taskmanager-memory-flink-size)
* [`taskmanager.memory.process.size`](../config.html#taskmanager-memory-process-size)
* [`taskmanager.memory.task.heap.size`](../config.html#taskmanager-memory-task-heap-size) and [`taskmanager.memory.managed.size`](../config.html#taskmanager-memory-managed-size)

<span class="label label-info">Note</span> Explicitly configuring both *total process memory* and *total Flink memory* is not recommended.
It may lead to deployment failures due to potential memory configuration conflicts. Additional configuration
of other memory components also requires caution as it can produce further configuration conflicts.

## Configure Heap and Managed Memory

As mentioned before in [total memory description](#configure-total-memory), another way to setup memory in Flink is
to specify explicitly both [task heap](#task-operator-heap-memory) and [managed memory](#managed-memory).
It gives more control over the available JVM heap to Flink’s tasks and its [managed memory](#managed-memory).

The rest of the memory components will be adjusted automatically, based on default values or additionally configured options.
[Here](mem_detail.html) are more details about the other memory components.

<span class="label label-info">Note</span> If you have configured the task heap and managed memory explicitly, it is recommended to set neither
*total process memory* nor *total Flink memory*. Otherwise, it may easily lead to memory configuration conflicts.

### Task (Operator) Heap Memory

If you want to guarantee that a certain amount of JVM heap is available for your user code, you can set the *task heap memory*
explicitly ([`taskmanager.memory.task.heap.size`](../config.html#taskmanager-memory-task-heap-size)).
It will be added to the JVM heap size and will be dedicated to Flink’s operators running the user code.

### Managed Memory

*Managed memory* is managed by Flink and is allocated as native memory (off-heap). The following workloads use *managed memory*:
* Streaming jobs can use it for [RocksDB state backend](../state/state_backends.html#the-rocksdbstatebackend).
* [Batch jobs](../../dev/batch) can use it for sorting, hash tables, caching of intermediate results.

The size of *managed memory* can be
* either configured explicitly via [`taskmanager.memory.managed.size`](../config.html#taskmanager-memory-managed-size)
* or computed as a fraction of *total Flink memory* via [`taskmanager.memory.managed.fraction`](../config.html#taskmanager-memory-managed-fraction).

*Size* will override *fraction*, if both are set.
If neither *size* nor *fraction* is explicitly configured, the [default fraction](../config.html#taskmanager-memory-managed-fraction) will be used.

See also [how to configure memory for state backends](mem_tuning.html#configure-memory-for-state-backends) and [batch jobs](mem_tuning.html#configure-memory-for-batch-jobs).

## Configure Off-Heap Memory (direct or native)

The off-heap memory which is allocated by user code should be accounted for in *task off-heap memory*
([`taskmanager.memory.task.off-heap.size`](../config.html#taskmanager-memory-task-off-heap-size)).

<span class="label label-info">Note</span> You can also adjust the [framework off-heap memory](mem_detail.html#framework-memory). This option is advanced
and only recommended to be changed if you are sure that the Flink framework needs more memory.

Flink includes the *framework off-heap memory* and *task off-heap memory* into the *direct memory* limit of the JVM,
see also [JVM parameters](mem_detail.html#jvm-parameters).

<span class="label label-info">Note</span> Although, native non-direct memory usage can be accounted for as a part of the
*framework off-heap memory* or *task off-heap memory*, it will result in a higher JVM's *direct memory* limit in this case.

<span class="label label-info">Note</span> The *network memory* is also part of JVM *direct memory* but it is managed by Flink and guaranteed
to never exceed its configured size. Therefore, resizing the *network memory* will not help in this situation.

See also [the detailed memory model](mem_detail.html).
