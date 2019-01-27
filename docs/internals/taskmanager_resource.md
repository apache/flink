---
title:  "TaskManager Resource"
nav-title: TaskManager Resource
nav-parent_id: internals
nav-pos: 6
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

The **TaskManager** is responsible for executing individual tasks of a Flink job. Each TaskManager will have at least one task slot and manage a subset of computing resources (cpu, memory, etc.) of underlying machines. How many tasks a TaskManger can hold is limited by both its slot number and resources. This document describes Flink's TaskManager resource calculation methods, to help the user better understand how many resources the Flink ResourceManager will allocate for each TaskManager from resource management frameworks like YARN, Mesos or Kubernetes, and how many of which are supplied for user jobs.

For session and perjob modes, the calculation methods of TaskManager resources is totally different.

* In session mode, the resources are calculated primarily based on the various configurations specified by the user.
* In perjob mode, if resource matching is enabled, the total resources of a TaskManager are dynamically calculated based on the Slot ResourceProfiles and the max resource limits of container or pod. Otherwise, it will be calculated according to various configurations, which is consistent with the calculation method in session mode.

Note: Setting resources of any operator in your job will enable the resource matching.

* This will be replaced by the TOC
{:toc}

## Session Mode

Use the following configurations to set TaskManager cpu resource and the number of task slots.

{% highlight yaml %}
taskmanager.cpu.core: 1
taskmanager.numberOfTaskSlots: 10
{% endhighlight %}

<img src="{{ site.baseurl }}/fig/taskmanager_resource_mem.png" class="img-responsive">

As shown in the figure above, the memory of each TaskManager consists of three main parts: JVM heap, JVM direct, and native.

Use the following configurations to set TaskManager total and fine-grained memory resource.

### Specify the total memory size of each container

{% highlight yaml %}
-tm  Total memory per TaskManager Container or Pod at executing `yarn-session.sh` or `kubernetes-session.sh`.
{% endhighlight %}

### Specify the fine-grained memory resource

* Framework Resource
{% highlight yaml %}
1. taskmanager.process.netty.memory.mb: 64
2. taskmanager.process.heap.memory.mb: 128
3. taskmanager.process.native.memory.mb: 0
{% endhighlight %}

* User Resource
{% highlight yaml %}
4. taskmanager.direct.memory.mb: 0
5. taskmanager.heap.mb: 1024
6. taskmanager.native.memory.mb: 0
7. taskmanager.network.memory.min: 67108864
8. taskmanager.managed.memory.size： 0
{% endhighlight %}

## PerJob Mode

If the resource matching is disabled, the resources of all operators will be set to UNKNOWN. The same method as session will be used to calculate the resources of each TaskManager.

If the resource matching is enabled, you could set up the resources for operator through ResourceSpec, including CpuCores, HeapMemory, DirectMemory, NativeMemory and ManagedMemory. The JobManager will allocate slots with corresponding resources from ResourceManager. The ResourceManager will combine the slot requests into Yarn container or kubernetes pod requests and send to the resource management framework.

Using the following two configurations to control how slot requests should be combined into a container or pod.

{% highlight yaml %}
taskmanager.multi-slots.max.memory.mb: 8192
taskmanager.multi-slots.max.cpu.core: 1
{% endhighlight %}

Using the following code to set resources for operator.
{% highlight java %}
ResourceSpec resourceSpec = ResourceSpec.newBuilder()
    .setCpuCores(1)
    .setHeapMemoryInMB(1024)
    .setDirectMemoryInMB(128)
    .setNativeMemoryInMB(64)
    .addExtendedResource(new CommonExtendedResource(ResourceSpec.MANAGED_MEMORY_NAME, 10))
    .build();

((SingleOutputStreamOperator<String>) text).setResources(resourceSpec)
    .flatMap(new Tokenizer()).setResources(resourceSpec)
    .keyBy(0).sum(1).setResources(resourceSpec);
{% endhighlight %}

Note: In perjob mode, the slot number of each TaskManager is dynamically calculated. The configuration option `taskmanager.numberOfTaskSlots` will not take effect.

{% top %}
