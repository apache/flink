---
title: "Troubleshooting"
weight: 6
type: docs
aliases:
  - /deployment/memory/mem_trouble.html
  - /ops/memory/mem_trouble.html
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

# Troubleshooting

## IllegalConfigurationException

If you see an *IllegalConfigurationException* thrown from *TaskExecutorProcessUtils* or *JobManagerProcessUtils*, it
usually indicates that there is either an invalid configuration value (e.g. negative memory size, fraction that is
greater than 1, etc.) or configuration conflicts. Check the documentation chapters or
[configuration options]({{< ref "docs/deployment/config" >}}#memory-configuration) related to the memory components mentioned in the exception message.

## OutOfMemoryError: Java heap space

The exception usually indicates that the *JVM Heap* is too small. You can try to increase the JVM Heap size
by increasing [total memory]({{< ref "docs/deployment/memory/mem_setup" >}}#configure-total-memory). You can also directly increase
[task heap memory]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#task-operator-heap-memory) for TaskManagers or
[JVM Heap memory]({{< ref "docs/deployment/memory/mem_setup_jobmanager" >}}#configure-jvm-heap) for JobManagers.

{{< hint info >}}
You can also increase the [framework heap memory]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#framework-memory)
for TaskManagers, but you should only change this option if you are sure the Flink framework itself needs more memory.
{{< /hint >}}

## OutOfMemoryError: Direct buffer memory

The exception usually indicates that the JVM *direct memory* limit is too small or that there is a *direct memory leak*.
Check whether user code or other external dependencies use the JVM *direct memory* and that it is properly accounted for.
You can try to increase its limit by adjusting direct off-heap memory.
See also how to configure off-heap memory for [TaskManagers]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#configure-off-heap-memory-direct-or-native),
[JobManagers]({{< ref "docs/deployment/memory/mem_setup_jobmanager" >}}#configure-off-heap-memory) and the [JVM arguments]({{< ref "docs/deployment/memory/mem_setup" >}}#jvm-parameters) which Flink sets.

## OutOfMemoryError: Metaspace

The exception usually indicates that [JVM metaspace limit]({{< ref "docs/deployment/memory/mem_setup" >}}#jvm-parameters) is configured too small.
You can try to increase the JVM metaspace option for [TaskManagers]({{< ref "docs/deployment/config" >}}#taskmanager-memory-jvm-metaspace-size)
or [JobManagers]({{< ref "docs/deployment/config" >}}#jobmanager-memory-jvm-metaspace-size).

## IOException: Insufficient number of network buffers

This is only relevant for TaskManagers.

The exception usually indicates that the size of the configured [network memory]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#detailed-memory-model)
is not big enough. You can try to increase the *network memory* by adjusting the following options:
* [`taskmanager.memory.network.min`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-min)
* [`taskmanager.memory.network.max`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-max)
* [`taskmanager.memory.network.fraction`]({{< ref "docs/deployment/config" >}}#taskmanager-memory-network-fraction)

## Container Memory Exceeded

If a Flink container tries to allocate memory beyond its requested size (Yarn, Mesos or Kubernetes),
this usually indicates that Flink has not reserved enough native memory. You can observe this either by using an external
monitoring system or from the error messages when a container gets killed by the deployment environment.

If you encounter this problem in the *JobManager* process, you can also enable the *JVM Direct Memory* limit by setting the
[`jobmanager.memory.enable-jvm-direct-memory-limit`]({{< ref "docs/deployment/config" >}}#jobmanager-memory-enable-jvm-direct-memory-limit) option
to exclude possible *JVM Direct Memory* leak.

If [RocksDBStateBackend]({{< ref "docs/ops/state/state_backends" >}}#the-rocksdbstatebackend) is used, and the memory controlling is disabled,
you can try to increase the TaskManager's [managed memory]({{< ref "docs/deployment/memory/mem_setup" >}}#managed-memory).

Alternatively, you can increase the [JVM Overhead]({{< ref "docs/deployment/memory/mem_setup" >}}#capped-fractionated-components).

See also [how to configure memory for containers]({{< ref "docs/deployment/memory/mem_tuning" >}}#configure-memory-for-containers).
