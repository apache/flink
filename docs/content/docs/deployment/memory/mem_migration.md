---
title: "Migration Guide"
weight: 7
type: docs
aliases:
  - /deployment/memory/mem_migration.html
  - /ops/memory/mem_migration.html
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

# Migration Guide

The memory setup has changed a lot with the *1.10* release for [TaskManagers]({{< ref "docs/deployment/memory/mem_setup_tm" >}}) and with the *1.11*
release for [JobManagers]({{< ref "docs/deployment/memory/mem_setup_jobmanager" >}}). Many configuration options were removed or their semantics changed.
This guide will help you to migrate the TaskManager memory configuration from Flink
[<= *1.9*](https://nightlies.apache.org/flink/flink-docs-release-1.9/ops/mem_setup.html) to >= *1.10* and
the JobManager memory configuration from Flink <= *1.10* to >= *1.11*.

{{< hint warning >}}
It is important to review this guide because the legacy and new memory configuration can
result in different sizes of memory components. If you try to reuse your Flink configuration from older versions
before 1.10 for TaskManagers or before 1.11 for JobManagers, it can result in changes to the behavior, performance or even configuration failures of your application.
{{< /hint >}}

<span class="label label-info">Note</span> Before version *1.10* for TaskManagers and before *1.11* for JobManagers,
Flink did not require that memory related options are set at all as they all had default values.
The [new memory configuration]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory) requires that at least one subset of
the following options is configured explicitly, otherwise the configuration will fail:

| &nbsp;&nbsp;**for TaskManager:**&nbsp;&nbsp;                                                                                                                                        | &nbsp;&nbsp;**for JobManager:**&nbsp;&nbsp;                                      |
| :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | :-------------------------------------------------------------------------------- |
| [`taskmanager.memory.flink.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-flink-size)                                                                                                       | [`jobmanager.memory.flink.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-flink-size)     |
| [`taskmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-process-size)                                                                                                   | [`jobmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-process-size) |
| [`taskmanager.memory.task.heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-heap-size) <br/> and [`taskmanager.memory.managed.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-size) | [`jobmanager.memory.heap.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-heap-size)       |

<br/>

The [default `flink-conf.yaml`](#default-configuration-in-flink-confyaml) shipped with Flink sets
[`taskmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-process-size) (since *1.10*) and
[`jobmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-process-size) (since *1.11*)
to make the default memory configuration consistent.

This [spreadsheet](https://docs.google.com/spreadsheets/d/1mJaMkMPfDJJ-w6nMXALYmTc4XxiV30P5U7DzgwLkSoE) can also help
to evaluate and compare the results of the legacy and new memory computations.

## Migrate Task Manager Memory Configuration

### Changes in Configuration Options

This chapter shortly lists all changes to Flink's memory configuration options introduced with the *1.10* release.
It also references other chapters for more details about migrating to the new configuration options.

The following options are completely removed. If they are still used, they will be ignored.

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left">Removed option</th>
            <th class="text-left">Note</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>taskmanager.memory.fraction</h5></td>
            <td>
                Check the description of the new option <a href="{{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-fraction">taskmanager.memory.managed.fraction</a>.
                The new option has different semantics and the value of the deprecated option usually has to be adjusted.
                See also <a href="#managed-memory">how to migrate managed memory</a>.
            </td>
        </tr>
        <tr>
             <td><h5>taskmanager.memory.off-heap</h5></td>
             <td>On-heap <i>managed memory</i> is no longer supported. See also <a href="#managed-memory">how to migrate managed memory</a>.</td>
        </tr>
        <tr>
             <td><h5>taskmanager.memory.preallocate</h5></td>
             <td>Pre-allocation is no longer supported and <i>managed memory</i> is always allocated lazily. See also <a href="#managed-memory">how to migrate managed memory</a>.</td>
        </tr>
    </tbody>
</table>

The following options are deprecated but if they are still used they will be interpreted as new options for backwards compatibility:

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left">Deprecated option</th>
            <th class="text-left">Interpreted as</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>taskmanager.heap.size</h5></td>
            <td>
                <ul>
                  <li><a href="{{< ref "docs/deployment/config" >}}#taskmanager-memory-flink-size">taskmanager.memory.flink.size</a> for <a href="{{< ref "docs/deployment/resource-providers/standalone/overview" >}}">standalone deployment</a></li>
                  <li><a href="{{< ref "docs/deployment/config" >}}#taskmanager-memory-process-size">taskmanager.memory.process.size</a> for containerized deployments</li>
                </ul>
                See also <a href="#total-memory-previously-heap-memory">how to migrate total memory</a>.
            </td>
        </tr>
        <tr>
             <td><h5>taskmanager.memory.size</h5></td>
             <td><a href="{{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-size">taskmanager.memory.managed.size</a>, see also <a href="#managed-memory">how to migrate managed memory</a>.</td>
        </tr>
        <tr>
             <td><h5>taskmanager.network.memory.min</h5></td>
             <td><a href="{{< ref "docs/deployment/config" >}}#taskmanager-memory-network-min">taskmanager.memory.network.min</a></td>
        </tr>
        <tr>
             <td><h5>taskmanager.network.memory.max</h5></td>
             <td><a href="{{< ref "docs/deployment/config" >}}#taskmanager-memory-network-max">taskmanager.memory.network.max</a></td>
        </tr>
        <tr>
             <td><h5>taskmanager.network.memory.fraction</h5></td>
             <td><a href="{{< ref "docs/deployment/config" >}}#taskmanager-memory-network-fraction">taskmanager.memory.network.fraction</a></td>
        </tr>
    </tbody>
</table>

Although, the network memory configuration has not changed too much it is recommended to verify its configuration.
It can change if other memory components have new sizes, e.g. the total memory which the network can be a fraction of.
See also [new detailed memory model]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#detailed-memory-model).

The container cut-off configuration options, `containerized.heap-cutoff-ratio` and `containerized.heap-cutoff-min`,
have no effect anymore for TaskManagers. See also [how to migrate container cut-off](#container-cut-off-memory).

### Total Memory (Previously Heap Memory)

The previous options which were responsible for the total memory used by Flink are `taskmanager.heap.size` or `taskmanager.heap.mb`.
Despite their naming, they included not only JVM Heap but also other off-heap memory components. The options have been deprecated.

If you use the mentioned legacy options without specifying the corresponding new options,
they will be directly translated into the following new options:
* Total Flink memory ([`taskmanager.memory.flink.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-flink-size)) for standalone deployments
* Total process memory ([`taskmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-process-size)) for containerized deployments (Yarn)

It is also recommended using these new options instead of the legacy ones as they might be completely removed in the following releases.

See also [how to configure total memory now]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory).

### JVM Heap Memory

JVM Heap memory previously consisted of the managed memory (if configured to be on-heap) and the rest
which included any other usages of heap memory. This rest was the remaining part of the total memory,
see also [how to migrate managed memory](#managed-memory).

Now, if only *total Flink memory* or *total process memory* is configured, then the JVM Heap is the rest of
what is left after subtracting all other components from the total memory, see also [how to configure total memory]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory).

Additionally, you can now have more direct control over the JVM Heap assigned to the operator tasks
([`taskmanager.memory.task.heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-heap-size)),
see also [Task (Operator) Heap Memory]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#task-operator-heap-memory).
The JVM Heap memory is also used by the heap state backends ([MemoryStateBackend]({{< ref "docs/ops/state/state_backends" >}}#the-memorystatebackend)
or [FsStateBackend]({{< ref "docs/ops/state/state_backends" >}}#the-fsstatebackend)) if it is chosen for streaming jobs.

A part of the JVM Heap is now always reserved for the Flink framework
([`taskmanager.memory.framework.heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-heap-size)).
See also [Framework memory]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#framework-memory).

### Managed Memory

See also [how to configure managed memory now]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory).

#### Explicit Size

The previous option to configure managed memory size (`taskmanager.memory.size`) was renamed to
[`taskmanager.memory.managed.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-size) and deprecated.
It is recommended to use the new option because the legacy one can be removed in future releases.

#### Fraction

If not set explicitly, the managed memory could be previously specified as a fraction (`taskmanager.memory.fraction`)
of the total memory minus network memory and container cut-off (only for [Yarn]({{< ref "docs/deployment/resource-providers/yarn" >}}) deployments). This option has been completely removed and will have no effect if still used.
Please, use the new option [`taskmanager.memory.managed.fraction`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-fraction) instead.
This new option will set the [managed memory]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory) to the specified fraction of the
[total Flink memory]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory) if its size is not set explicitly by
[`taskmanager.memory.managed.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-managed-size).

#### RocksDB state

If the [RocksDBStateBackend]({{< ref "docs/ops/state/state_backends" >}}#the-rocksdbstatebackend) is chosen for a streaming job,
its native memory consumption should now be accounted for in [managed memory]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory).
The RocksDB memory allocation is limited by the [managed memory]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory) size.
This should prevent the killing of containers on [Yarn]({{< ref "docs/deployment/resource-providers/yarn" >}}).
You can disable the RocksDB memory control by setting [state.backend.rocksdb.memory.managed]({{< ref "docs/deployment/config" >}}#state-backend-rocksdb-memory-managed)
to `false`. See also [how to migrate container cut-off](#container-cut-off-memory).

#### Other changes

Additionally, the following changes have been made:
* The [managed memory]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory) is always off-heap now. The configuration option `taskmanager.memory.off-heap` is removed and will have no effect anymore.
* The [managed memory]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory) now uses native memory which is not direct memory. It means that the managed memory is no longer accounted for in the JVM direct memory limit.
* The [managed memory]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory) is always lazily allocated now. The configuration option `taskmanager.memory.preallocate` is removed and will have no effect anymore.

## Migrate Job Manager Memory Configuration

Previously, there were options responsible for setting the *JVM Heap* size of the JobManager:
* `jobmanager.heap.size`
* `jobmanager.heap.mb`

Despite their naming, they represented the *JVM Heap* only for [standalone deployments]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}).
For the containerized deployments ([Kubernetes]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}) and [Yarn]({{< ref "docs/deployment/resource-providers/yarn" >}})),
they also included other off-heap memory consumption. The size of *JVM Heap* was additionally reduced by the container
cut-off which has been completely removed after *1.11*.

The mentioned legacy options have been deprecated. If they are used without specifying the corresponding new options,
they will be directly translated into the following new options:
* JVM Heap ([`jobmanager.memory.heap.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-heap-size)) for [standalone]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}) deployments
* Total process memory ([`jobmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-process-size)) for containerized deployments ([Kubernetes]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}) and [Yarn]({{< ref "docs/deployment/resource-providers/yarn" >}}))

It is also recommended using these new options instead of the legacy ones as they might be completely removed in the following releases.

Now, if only the *total Flink memory* or *total process memory* is configured, then the [JVM Heap]({{< ref "docs/deployment/memory/mem_setup_jobmanager" >}}#configure-jvm-heap)
is also derived as the rest of what is left after subtracting all other components from the total memory, see also
[how to configure total memory]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory). Additionally, you can now have more direct
control over the [JVM Heap]({{< ref "docs/deployment/memory/mem_setup_jobmanager" >}}#configure-jvm-heap) by adjusting the
[`jobmanager.memory.heap.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-heap-size) option.

## Flink JVM process memory limits

Since *1.10* release, Flink sets the *JVM Metaspace* and *JVM Direct Memory* limits for the TaskManager process
by adding the corresponding JVM arguments. Since *1.11* release, Flink also sets the *JVM Metaspace* limit for the JobManager process.
You can enable the *JVM Direct Memory* limit for JobManager process if you set the
[`jobmanager.memory.enable-jvm-direct-memory-limit`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-enable-jvm-direct-memory-limit) option.
See also [JVM parameters]({{< ref "docs/deployment/memory/mem_setup" >}}#jvm-parameters).

Flink sets the mentioned JVM memory limits to simplify debugging of the corresponding memory leaks and avoid
[the container out-of-memory errors]({{< ref "docs/deployment/memory/mem_trouble" >}}#container-memory-exceeded).
See also the troubleshooting guide for details about the [JVM Metaspace]({{< ref "docs/deployment/memory/mem_trouble" >}}#outofmemoryerror-metaspace)
and [JVM Direct Memory]({{< ref "docs/deployment/memory/mem_trouble" >}}#outofmemoryerror-direct-buffer-memory) *OutOfMemoryErrors*.

## Container Cut-Off Memory

For containerized deployments, you could previously specify a cut-off memory. This memory could accommodate for unaccounted memory allocations.
Dependencies which were not directly controlled by Flink were the main source of those allocations, e.g. RocksDB, JVM internals, etc.
This is no longer available, and the related configuration options (`containerized.heap-cutoff-ratio` and `containerized.heap-cutoff-min`)
will have no effect anymore. The new memory model introduced more specific memory components to address these concerns.

### for TaskManagers

In streaming jobs which use [RocksDBStateBackend]({{< ref "docs/ops/state/state_backends" >}}#the-rocksdbstatebackend), the RocksDB
native memory consumption should be accounted for as a part of the [managed memory]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory) now.
The RocksDB memory allocation is also limited by the configured size of the [managed memory]({{< ref "docs/deployment/memory/mem_setup" >}}#managed-memory).
See also [migrating managed memory](#managed-memory) and [how to configure managed memory now]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory).

The other direct or native off-heap memory consumers can now be addressed by the following new configuration options:
* Task off-heap memory ([`taskmanager.memory.task.off-heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-task-off-heap-size))
* Framework off-heap memory ([`taskmanager.memory.framework.off-heap.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-framework-off-heap-size))
* JVM metaspace ([`taskmanager.memory.jvm-metaspace.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-jvm-metaspace-size))
* [JVM overhead]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#detailed-memory-model)

### for JobManagers

The direct or native off-heap memory consumers can now be addressed by the following new configuration options:
* Off-heap memory ([`jobmanager.memory.off-heap.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-off-heap-size))
* JVM metaspace ([`jobmanager.memory.jvm-metaspace.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-jvm-metaspace-size))
* [JVM overhead]({{< ref "docs/deployment/memory/mem_setup_jobmanager" >}}#detailed-configuration)

## Default Configuration in flink-conf.yaml

This section describes the changes of the default `flink-conf.yaml` shipped with Flink.

The total memory for TaskManagers (`taskmanager.heap.size`) is replaced by [`taskmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-process-size)
in the default `flink-conf.yaml`. The value increased from 1024Mb to 1728Mb.

The total memory for JobManagers (`jobmanager.heap.size`) is replaced by [`jobmanager.memory.process.size`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-process-size)
in the default `flink-conf.yaml`. The value increased from 1024Mb to 1600Mb.

See also [how to configure total memory now]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory).

{{< hint warning >}}
**Warning:** If you use the new default `flink-conf.yaml` it can result in different sizes of memory components and can lead to performance changes.
{{< /hint >}}
