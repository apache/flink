---
title: "Troubleshooting"
nav-parent_id: ops_mem
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

* toc
{:toc}

## IllegalConfigurationException

If you see an *IllegalConfigurationException* thrown from *TaskExecutorProcessUtils* or *JobManagerProcessUtils*, it
usually indicates that there is either an invalid configuration value (e.g. negative memory size, fraction that is
greater than 1, etc.) or configuration conflicts. Check the documentation chapters or
[configuration options]({% link deployment/config.md %}#memory-configuration) related to the memory components mentioned in the exception message.

## OutOfMemoryError: Java heap space

The exception usually indicates that the *JVM Heap* is too small. You can try to increase the JVM Heap size
by increasing [total memory]({% link deployment/memory/mem_setup.md %}#configure-total-memory). You can also directly increase
[task heap memory]({% link deployment/memory/mem_setup_tm.md %}#task-operator-heap-memory) for TaskManagers or
[JVM Heap memory]({% link deployment/memory/mem_setup_jobmanager.md %}#configure-jvm-heap) for JobManagers.

<span class="label label-info">Note</span> You can also increase the [framework heap memory]({% link deployment/memory/mem_setup_tm.md %}#framework-memory)
for TaskManagers, but you should only change this option if you are sure the Flink framework itself needs more memory.

## OutOfMemoryError: Direct buffer memory

The exception usually indicates that the JVM *direct memory* limit is too small or that there is a *direct memory leak*.
Check whether user code or other external dependencies use the JVM *direct memory* and that it is properly accounted for.
You can try to increase its limit by adjusting direct off-heap memory.
See also how to configure off-heap memory for [TaskManagers]({% link deployment/memory/mem_setup_tm.md %}#configure-off-heap-memory-direct-or-native),
[JobManagers]({% link deployment/memory/mem_setup_jobmanager.md %}#configure-off-heap-memory) and the [JVM arguments]({% link deployment/memory/mem_setup.md %}#jvm-parameters) which Flink sets.

## OutOfMemoryError: Metaspace

The exception usually indicates that [JVM metaspace limit]({% link deployment/memory/mem_setup.md %}#jvm-parameters) is configured too small.
You can try to increase the JVM metaspace option for [TaskManagers]({% link deployment/config.md %}#taskmanager-memory-jvm-metaspace-size)
or [JobManagers]({% link deployment/config.md %}#jobmanager-memory-jvm-metaspace-size).

## IOException: Insufficient number of network buffers

This is only relevant for TaskManagers.

The exception usually indicates that the size of the configured [network memory]({% link deployment/memory/mem_setup_tm.md %}#detailed-memory-model)
is not big enough. You can try to increase the *network memory* by adjusting the following options:
* [`taskmanager.memory.network.min`]({% link deployment/config.md %}#taskmanager-memory-network-min)
* [`taskmanager.memory.network.max`]({% link deployment/config.md %}#taskmanager-memory-network-max)
* [`taskmanager.memory.network.fraction`]({% link deployment/config.md %}#taskmanager-memory-network-fraction)

## Container Memory Exceeded

If a Flink container tries to allocate memory beyond its requested size (Yarn, Mesos or Kubernetes),
this usually indicates that Flink has not reserved enough native memory. You can observe this either by using an external
monitoring system or from the error messages when a container gets killed by the deployment environment.

If you encounter this problem in the *JobManager* process, you can also enable the *JVM Direct Memory* limit by setting the
[`jobmanager.memory.enable-jvm-direct-memory-limit`]({% link deployment/config.md %}#jobmanager-memory-enable-jvm-direct-memory-limit) option
to exclude possible *JVM Direct Memory* leak.

If [RocksDBStateBackend]({% link ops/state/state_backends.md %}#the-rocksdbstatebackend) is used, and the memory controlling is disabled,
you can try to increase the TaskManager's [managed memory]({% link deployment/memory/mem_setup.md %}#managed-memory).

Alternatively, you can increase the [JVM Overhead]({% link deployment/memory/mem_setup.md %}#capped-fractionated-components).

See also [how to configure memory for containers]({% link deployment/memory/mem_tuning.md %}#configure-memory-for-containers).
