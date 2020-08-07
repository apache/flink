---
title: "Memory tuning guide"
nav-parent_id: ops_mem
nav-pos: 4
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

In addition to the [main memory setup guide](mem_setup.html), this section explains how to set up memory
depending on the use case and which options are important for each case.

* toc
{:toc}

## Configure memory for standalone deployment

It is recommended to configure [total Flink memory](mem_setup.html#configure-total-memory)
([`taskmanager.memory.flink.size`](../config.html#taskmanager-memory-flink-size) or [`jobmanager.memory.flink.size`](../config.html#jobmanager-memory-flink-size))
or its components for [standalone deployment](../deployment/cluster_setup.html) where you want to declare how much memory
is given to Flink itself. Additionally, you can adjust *JVM metaspace* if it causes [problems](mem_trouble.html#outofmemoryerror-metaspace).

The *total Process memory* is not relevant because *JVM overhead* is not controlled by Flink or the deployment environment,
only physical resources of the executing machine matter in this case.

## Configure memory for containers

It is recommended to configure [total process memory](mem_setup.html#configure-total-memory)
([`taskmanager.memory.process.size`](../config.html#taskmanager-memory-process-size) or [`jobmanager.memory.process.size`](../config.html#jobmanager-memory-process-size))
for the containerized deployments ([Kubernetes](../deployment/kubernetes.html), [Yarn](../deployment/yarn_setup.html) or [Mesos](../deployment/mesos.html)).
It declares how much memory in total should be assigned to the Flink *JVM process* and corresponds to the size of the requested container.

<span class="label label-info">Note</span> If you configure the *total Flink memory* Flink will implicitly add JVM memory components
to derive the *total process memory* and request a container with the memory of that derived size.

<div class="alert alert-warning">
  <strong>Warning:</strong> If Flink or user code allocates unmanaged off-heap (native) memory beyond the container size
  the job can fail because the deployment environment can kill the offending containers.
</div>
See also description of [container memory exceeded](mem_trouble.html#container-memory-exceeded) failure.

## Configure memory for state backends

This is only relevant for TaskManagers.

When deploying a Flink streaming application, the type of [state backend](../state/state_backends.html) used
will dictate the optimal memory configurations of your cluster.

### Heap state backend

When running a stateless job or using a heap state backend ([MemoryStateBackend](../state/state_backends.html#the-memorystatebackend)
or [FsStateBackend](../state/state_backends.html#the-fsstatebackend)), set [managed memory](mem_setup_tm.html#managed-memory) to zero.
This will ensure that the maximum amount of heap memory is allocated for user code on the JVM.

### RocksDB state backend

The [RocksDBStateBackend](../state/state_backends.html#the-rocksdbstatebackend) uses native memory. By default,
RocksDB is set up to limit native memory allocation to the size of the [managed memory](mem_setup_tm.html#managed-memory).
Therefore, it is important to reserve enough *managed memory* for your state. If you disable the default RocksDB memory control,
TaskManagers can be killed in containerized deployments if RocksDB allocates memory above the limit of the requested container size
(the [total process memory](mem_setup.html#configure-total-memory)).
See also [how to tune RocksDB memory](../state/large_state_tuning.html#tuning-rocksdb-memory)
and [state.backend.rocksdb.memory.managed](../config.html#state-backend-rocksdb-memory-managed).

## Configure memory for batch jobs

This is only relevant for TaskManagers.

Flink's batch operators leverage [managed memory](../memory/mem_setup_tm.html#managed-memory) to run more efficiently.
In doing so, some operations can be performed directly on raw data without having to be deserialized into Java objects.
This means that [managed memory](../memory/mem_setup_tm.html#managed-memory) configurations have practical effects
on the performance of your applications. Flink will attempt to allocate and use as much [managed memory](../memory/mem_setup_tm.html#managed-memory)
as configured for batch jobs but not go beyond its limits. This prevents `OutOfMemoryError`'s because Flink knows precisely
how much memory it has to leverage. If the [managed memory](../memory/mem_setup_tm.html#managed-memory) is not sufficient,
Flink will gracefully spill to disk.
