---
title: "配置指南"
nav-title: 配置指南
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

This document gives guidelines on how to configure memory usages of Flink [TaskExecutors]({{ site.baseurl }}/concepts/glossary.html#taskexecutor).
Please refer to [TaskExecutors memory model]({{ site.baseurl }}/internals/memory_model.html) for basic concepts and corresponding configuration options.

* toc
{:toc}

The easiest way to configure Flink TaskExecutor memory is to specify Process Memory and let Flink decide how to allocate memory to each of the components.
This should work for most use cases.

If you are less concerned with overall memory consumption, you can specify Flink Memory instead of Process Memory, to focus more on the performance-critical memory setting.
For containerized deployments, Flink will request containers with derived Process Memory, thus, the containers will have more memory than the configured Flink Memory.

*Tips: It is not recommended to configure Process Memory and Flink Memory at the same time, which may easily cause conflicts.*

While setting Process / Flink Memory, users can also customize whatever detailed memory components they want to.
E.g., one can explicitly specify Process Memory and Managed Memory sizes at the same time.
Flink will automatically adjust the other memory components to fit in the explicit configurations, as long as there is no conflict.

*Tips: If both Task Heap Memory and Managed Memory sizes are explicitly configured, it is recommended to unset Process Memory and Flink Memory to avoid potential conflicts.*

*Tips: The absolute size of Network Memory and JVM Overhead can be specified by setting the min/max constraints to the same value.*

For streaming jobs with MemoryStateBackend or FsStateBackend, there should not be any usage of Managed Memory.
Therefore, it is recommended to set Managed Memory size/fraction to 0 for better memory utilization.

For streaming jobs with RocksDBStateBackend, it is recommended to reserve enough Managed Memory for RockDB.
Otherwise, the Process Memory could be exceeded.
On containerized deployments, TaskExecutors could be killed due to exceeding memory limits.
The RocksDB state backend includes a feature to limit total memory consumption, please look [here]({{ site.baseurl }}/ops/state/state_backends.html#state-backend-rocksdb-memory-managed) for more details.

## Trouble Shootings

### IllegalConfigurationException

If you see an `IllegalConfigurationException` thrown from `TaskExecutorResourceUtils`, it usually indicates that there’s either an invalid configuration value (e.g., negative memory size, a fraction that is greater than 1, etc.) or conflicting configurations.
Details on the problem and suggested solution should be found in the exception message.

### OutOfMemoryError: Java heap space

The exception usually indicates that JVM heap is not large enough for your workload.
You can try to increase the JVM heap size by configuring larger Framework Heap Memory or Task Heap Memory.

Note: If Task Heap Memory is not explicitly configured, but rather derived from the remaining of Flink Memory, increasing Framework Heap Memory alone may not change the JVM heap size but only result in Task Heap Memory.
You will need to either also increase the Process Memory / Flink Memory, or explicitly configure the Task Heap Memory.

### OutOfMemoryError: Direct buffer memory

The exception usually indicates that JVM max direct memory limit is configured too small.
You can try to increase the JVM max direct memory limit by increasing Framework Off-Heap Memory or Task Off-Heap Memory.

Note: Despite that Network Memory also uses JVM direct memory, it is guaranteed by Flink to always allocated the configured size.
Therefore, resizing Network Memory will not help on this exception.

### OutOfMemoryError: Metaspace

The exception usually indicates that JVM max metaspace limit is configured too small.
You can try to increase the JVM max metaspace limit by increasing JVM Metaspace.

### IOException: Insufficient number of network buffers

The exception usually indicates that Network Memory is not configured with enough size.

### Container Memory Exceeded

If a TaskExecutor memory usage exceeds its container on Yarn/Mesos/Kubernetes, this usually indicates that Flink did not reserve enough native memory.
The memory exceeding can be observed either from external monitoring systems or error messages if containers get killed due to this issue.
