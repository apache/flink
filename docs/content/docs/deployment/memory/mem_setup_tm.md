---
title: "Set up TaskManager Memory"
weight: 3
type: docs
aliases:
  - /deployment/memory/mem_setup_tm.html
  - /ops/memory/mem_setup_tm.html
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

# Set up TaskManager Memory

The TaskManager runs user code in Flink.
Configuring memory usage for your needs can greatly reduce Flink's resource footprint and improve Job stability.

The further described memory configuration is applicable starting with the release version *1.10*. If you upgrade Flink
from earlier versions, check the [migration guide]({{< ref "docs/deployment/memory/mem_migration" >}}) because many changes were introduced with the *1.10* release.

{{< hint info >}}
This memory setup guide is relevant **only for TaskManagers**!
The TaskManager memory components have a similar but more sophisticated structure compared to the [memory model of the JobManager process]({{< ref "docs/deployment/memory/mem_setup_jobmanager" >}}).
{{< /hint >}}

## Configure Total Memory

The *total process memory* of Flink JVM processes consists of memory consumed by Flink application (*total Flink memory*)
and by the JVM to run the process. The *total Flink memory* consumption includes usage of JVM Heap,
*managed memory* (managed by Flink) and other direct (or native) memory.

{{< img src="/fig/simple_mem_model.svg" width="300px" alt="Simple TaskManager Memory Model" usemap="#simple-mem-model" >}}

<br />

If you run Flink locally (e.g. from your IDE) without creating a cluster, then only a subset of the memory configuration
options are relevant, see also [local execution](#local-execution) for more details.

Otherwise, the simplest way to setup memory for TaskManagers is to [configure the total memory]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory).
A more fine-grained approach is described in more detail [here](#configure-heap-and-managed-memory).

The rest of the memory components will be adjusted automatically, based on default values or additionally configured options.
See next chapters for more details about the other memory components.

## Configure Heap and Managed Memory

As mentioned before in [total memory description](#configure-total-memory), another way to setup memory in Flink is
to specify explicitly both [task heap](#task-operator-heap-memory) and [managed memory](#managed-memory).
It gives more control over the available JVM Heap to Flink’s tasks and its [managed memory](#managed-memory).

The rest of the memory components will be adjusted automatically, based on default values or additionally configured options.
[Here](#detailed-memory-model) are more details about the other memory components.

{{< hint info >}}
If you have configured the task heap and managed memory explicitly, it is recommended to set neither
*total process memory* nor *total Flink memory*. Otherwise, it may easily lead to memory configuration conflicts.
{{< /hint >}}

### Task (Operator) Heap Memory

If you want to guarantee that a certain amount of JVM Heap is available for your user code, you can set the *task heap memory*
explicitly ([`taskmanager.memory.task.heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-heap-size)).
It will be added to the JVM Heap size and will be dedicated to Flink’s operators running the user code.

### Managed Memory

*Managed memory* is managed by Flink and is allocated as native memory (off-heap). The following workloads use *managed memory*:
* Streaming jobs can use it for [RocksDB state backend]({{< ref "docs/ops/state/state_backends" >}}#the-rocksdbstatebackend).
* Both streaming and batch jobs can use it for sorting, hash tables, caching of intermediate results.
* Both streaming and batch jobs can use it for executing [User Defined Functions in Python processes]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}).

The size of *managed memory* can be
* either configured explicitly via [`taskmanager.memory.managed.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-size)
* or computed as a fraction of *total Flink memory* via [`taskmanager.memory.managed.fraction`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-fraction).

*Size* will override *fraction*, if both are set.
If neither *size* nor *fraction* is explicitly configured, the [default fraction]({{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-fraction) will be used.

See also [how to configure memory for state backends]({{< ref "docs/deployment/memory/mem_tuning" >}}#configure-memory-for-state-backends) and [batch jobs]({{< ref "docs/deployment/memory/mem_tuning" >}}#configure-memory-for-batch-jobs).

#### Consumer Weights

If your job contains multiple types of managed memory consumers, you can also control how managed memory should be shared across these types.
The configuration option [`taskmanager.memory.managed.consumer-weights`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-consumer-weights) allows you to set a weight for each type, to which Flink will reserve managed memory proportionally.
Valid consumer types are:
* `OPERATOR`: for built-in algorithms.
* `STATE_BACKEND`: for RocksDB state backend in streaming
* `PYTHON`: for Python processes.

E.g. if a streaming job uses both RocksDB state backend and Python UDFs, and the consumer weights are configured as `STATE_BACKEND:70,PYTHON:30`, Flink will reserve `70%` of the total managed memory for RocksDB state backend and `30%` for Python processes.

For each type, Flink reserves managed memory only if the job contains managed memory consumers of that type.
E.g, if a streaming job uses the heap state backend and Python UDFs, and the consumer weights are configured as `STATE_BACKEND:70,PYTHON:30`, Flink will use all of its managed memory for Python processes, because the heap state backend does not use managed memory.

{{< hint warning >}}
Flink will not reserve managed memory for consumer types that are not included in the consumer weights.
If the missing type is actually needed by the job, it can lead to memory allocation failures.
By default, all consumer types are included.
This could only happen when the weights are explicitly configured/overwritten.
{{< /hint >}}

## Configure Off-heap Memory (direct or native)

The off-heap memory which is allocated by user code should be accounted for in *task off-heap memory*
([`taskmanager.memory.task.off-heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-off-heap-size)).

You can also adjust the [framework off-heap memory](#framework-memory).
You should only change this value if you are sure that the Flink framework needs more memory. 

Flink includes the *framework off-heap memory* and *task off-heap memory* into the *direct memory* limit of the JVM,
see also [JVM parameters]({{< ref "docs/deployment/memory/mem_setup" >}}#jvm-parameters).

<span class="label label-info">Note</span> Although, native non-direct memory usage can be accounted for as a part of the
*framework off-heap memory* or *task off-heap memory*, it will result in a higher JVM's *direct memory* limit in this case.

<span class="label label-info">Note</span> The *network memory* is also part of JVM *direct memory*, but it is managed by Flink and guaranteed
to never exceed its configured size. Therefore, resizing the *network memory* will not help in this situation.

See also [the detailed memory model](#detailed-memory-model).

## Detailed Memory Model

{{< img src="/fig/detailed-mem-model.svg" width="300px" alt="Simple memory model" usemap="#simple-mem-model" >}}

The following table lists all memory components, depicted above, and references Flink configuration options
which affect the size of the respective components:

| &nbsp;&nbsp;**Component**&nbsp;&nbsp;                              | &nbsp;&nbsp;**Configuration options**&nbsp;&nbsp;                                                                                                                                                                                                                                                         | &nbsp;&nbsp;**Description**&nbsp;&nbsp;                                                                                                                                                                                                                                     |
| :------------------------------------------------------------------| :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | :-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Framework Heap Memory](#framework-memory)                         | [`taskmanager.memory.framework.heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-heap-size)                                                                                                                                                                                                         | JVM Heap memory dedicated to Flink framework (advanced option)                                                                                                                                                                                                              |
| [Task Heap Memory](#task-operator-heap-memory)                     | [`taskmanager.memory.task.heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-heap-size)                                                                                                                                                                                                                   | JVM Heap memory dedicated to Flink application to run operators and user code                                                                                                                                                                                               |
| [Managed memory](#managed-memory)                                  | [`taskmanager.memory.managed.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-size) <br/> [`taskmanager.memory.managed.fraction`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-fraction)                                                                                                                     | Native memory managed by Flink, reserved for sorting, hash tables, caching of intermediate results and RocksDB state backend                                                                                                                                                |
| [Framework Off-heap Memory](#framework-memory)                     | [`taskmanager.memory.framework.off-heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size)                                                                                                                                                                                                 | [Off-heap direct (or native) memory](#configure-off-heap-memory-direct-or-native) dedicated to Flink framework (advanced option)                                                                                                                                            |
| [Task Off-heap Memory](#configure-off-heap-memory-direct-or-native)| [`taskmanager.memory.task.off-heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-off-heap-size)                                                                                                                                                                                                           | [Off-heap direct (or native) memory](#configure-off-heap-memory-direct-or-native) dedicated to Flink application to run operators                                                                                                                                           |
| Network Memory                                                     | [`taskmanager.memory.network.min`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-min) <br/> [`taskmanager.memory.network.max`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-max) <br/> [`taskmanager.memory.network.fraction`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-fraction)                               | Direct memory reserved for data record exchange between tasks (e.g. buffering for the transfer over the network), it is a [capped fractionated component]({{< ref "docs/deployment/memory/mem_setup" >}}#capped-fractionated-components) of the [total Flink memory]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory) |
| [JVM metaspace]({{< ref "docs/deployment/memory/mem_setup" >}}#jvm-parameters)                     | [`taskmanager.memory.jvm-metaspace.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-jvm-metaspace-size)                                                                                                                                                                                                           | Metaspace size of the Flink JVM process                                                                                                                                                                                                                                     |
| JVM Overhead                                                       | [`taskmanager.memory.jvm-overhead.min`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-jvm-overhead-min) <br/> [`taskmanager.memory.jvm-overhead.max`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-jvm-overhead-max) <br/> [`taskmanager.memory.jvm-overhead.fraction`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-jvm-overhead-fraction) | Native memory reserved for other JVM overhead: e.g. thread stacks, code cache, garbage collection space etc, it is a [capped fractionated component]({{< ref "docs/deployment/memory/mem_setup" >}}#capped-fractionated-components) of the [total process memory]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory)    |

<br/>

As you can see, the size of some memory components can be simply set by the respective option.
Other components can be tuned using multiple options.

## Framework Memory

You should not change the *framework heap memory* and *framework off-heap memory* without a good reason.
Adjust them only if you are sure that Flink needs more memory for some internal data structures or operations.
It can be related to a particular deployment environment or job structure, like high parallelism.
In addition, Flink dependencies, such as Hadoop may consume more direct or native memory in certain setups.

<span class="label label-info">Note</span> Flink neither isolates heap nor off-heap versions of framework and task memory at the moment.
The separation of framework and task memory can be used in future releases for further optimizations.

## Local Execution
If you start Flink locally on your machine as a single java program without creating a cluster (e.g. from your IDE)
then all components are ignored except for the following:

| &nbsp;&nbsp;**Memory component**&nbsp;&nbsp; | &nbsp;&nbsp;**Relevant options**&nbsp;&nbsp;                                                    | &nbsp;&nbsp;**Default value for the local execution**&nbsp;&nbsp;               |
| :------------------------------------------- | :---------------------------------------------------------------------------------------------- | :------------------------------------------------------------------------------ |
| Task heap                                    | [`taskmanager.memory.task.heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-heap-size)         | infinite                                                                        |
| Task off-heap                                | [`taskmanager.memory.task.off-heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-off-heap-size) | infinite                                                                        |
| Managed memory                               | [`taskmanager.memory.managed.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-size)             | 128Mb                                                                           |
| Network memory                               | [`taskmanager.memory.network.min`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-min) <br /> [`taskmanager.memory.network.max`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-max) | 64Mb |

<br/>

All of the components listed above can be but do not have to be explicitly configured for local execution.
If they are not configured they are set to their default values. [Task heap memory](#task-operator-heap-memory) and
*task off-heap memory* are considered to be infinite (*Long.MAX_VALUE* bytes) and [managed memory](#managed-memory)
has a default value of 128Mb only for the local execution mode.

<span class="label label-info">Note</span> The task heap size is not related in any way to the real heap size in this case.
It can become relevant for future optimizations coming with next releases. The actual JVM Heap size of the started
local process is not controlled by Flink and depends on how you start the process.
If you want to control the JVM Heap size you have to explicitly pass the corresponding JVM arguments, e.g. *-Xmx*, *-Xms*.
