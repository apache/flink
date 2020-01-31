---
title: "Task Manager Memory Configuration"
nav-parent_id: ops
nav-pos: 5
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

See also task manager [configuration options](config.html#taskmanager).

## Total memory

The *total memory* in Flink consists of JVM heap, [managed memory](#managed-memory) and [network buffers](#network-buffers).
[Managed memory](#managed-memory) can be either part of the JVM heap or direct off-heap memory.
For containerized deployments, the *total memory* can additionally include a [container cut-off](#container-cut-off).

All other memory components are computed from the *total memory* before starting the Flink process.
After the start, the [managed memory](#managed-memory) and [network memory](#network-buffers) are adjusted in certain cases
based on available JVM memory inside the process (see [Adjustments inside Flink process](#adjustments-inside-flink-process)).

<center>
  <img src="{{ site.baseurl }}/fig/mem_model.svg" width="500px" alt="Simple memory model" usemap="#simple-mem-model">
</center>
<br />

When the Flink JVM process is started in [standalone mode](deployment/cluster_setup.html),
on [Yarn](deployment/yarn_setup.html) or on [Mesos](deployment/mesos.html), the *total memory* is defined by
the configuration option [`taskmanager.heap.size`](config.html#taskmanager-heap-size-1) (or deprecated `taskmanager.heap.mb`).
In case of a containerized deployment ([Yarn](deployment/yarn_setup.html) or [Mesos](deployment/mesos.html)),
this is the size of the requested container.

### Container cut-off

In case of a containerized deployment, the [total memory](#total-memory) is reduced by the *cut-off*. The *cut-off* is
the fraction ([containerized.heap-cutoff-ratio](config.html#containerized-heap-cutoff-ratio) of the [total memory](#total-memory)
but always greater or equal than its minimum value ([containerized.heap-cutoff-min](config.html#containerized-heap-cutoff-min)).

The cut-off is introduced to accommodate for other types of consumed memory which is not accounted for in this memory model,
e.g. RocksDB native memory, JVM overhead, etc. It is also a safety margin to prevent the container from exceeding
its memory limit and being killed by the container manager.

## Network buffers

The *network memory* is used for buffering records while shuffling them between operator tasks and their executors over the network.
It is calculated as:
```
network = Min(max, Max(min, fraction x total)
```
where `fraction` is [`taskmanager.network.memory.fraction`](config.html#taskmanager-network-memory-fraction),
`min` is [`taskmanager.network.memory.min`](config.html#taskmanager-network-memory-min) and
`max` is [`taskmanager.network.memory.max`](config.html#taskmanager-network-memory-max).

See also [setting memory fractions](config.html#setting-memory-fractions).

If the above mentioned options are not set but the legacy option is used then the *network memory* is assumed
to be set explicitly without fraction as:
```
network = legacy buffers x page
```
where `legacy buffers` are `taskmanager.network.numberOfBuffers` and `page` is
[`taskmanager.memory.segment-size`](config.html#taskmanager-memory-segment-size).
See also [setting number of network buffers directly](config.html#setting-the-number-of-network-buffers-directly).

## Managed memory

The *managed memory* is used for *batch* jobs. It helps Flink to run the batch operators efficiently and prevents
`OutOfMemoryErrors` because Flink knows how much memory it can use to execute operations. If Flink runs out of *managed memory*,
it utilizes disk space. Using *managed memory*, some operations can be performed directly on the raw data without having
to deserialize the data to convert it into Java objects. All in all, *managed memory* improves the robustness and speed of the system.

The *managed memory* can be either part of the JVM heap (on-heap) or off-heap. It is on-heap by default
([`taskmanager.memory.off-heap`](config.html#taskmanager-memory-off-heap), default: `false`).

The managed memory size can be either set **explicitly** by [`taskmanager.memory.size`](config.html#taskmanager-memory-size)
or if not set explicitly then it is defined as a **fraction** ([`taskmanager.memory.fraction`](config.html#taskmanager-memory-fraction))
of [total memory](#total-memory) minus [network memory](#network-buffers) and calculated the following way:
```
managed = (total - network) x fraction
```

## JVM heap

The heap is set by JVM command line arguments (`-Xmx` and `-Xms`) to:
```
heap = total - managed (if off-heap) - network
```
See also [managed memory](#managed-memory) and [network buffers](#network-buffers).

## Adjustments inside Flink process

When the Flink process has been started, the size of [managed memory](#managed-memory) and [network buffers](#network-buffers),
eventually used in the process, are calculated in a slightly different way if the size is defined as a fraction. The values
are derived from the available JVM heap size. They should be close to the values calculated before starting the process but can differ.

The **JVM heap** is estimated in two ways for further computations:
* **Max heap**: if `-Xmx` is set then it is its value else ¼ of physical machine memory estimated by the JVM
* **Free heap**: same as the previous but reduced by the heap memory which is still in use after triggering garbage collection during the process startup

Then the **managed memory** is a fraction of
* (if **on-heap**) the max JVM heap
* (if **off-heap**) the [total memory](#total-memory) minus [network memory](#network-buffers)

The free JVM heap is used to derive the *total memory* for the calculation of the off-heap [managed memory](#managed-memory).

The max JVM heap is used to derive the *total memory* for the calculation of [network buffers](#network-buffers).
