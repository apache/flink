---
title: Fine-Grained Resource Management
weight: 5
type: docs

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


# 细粒度资源管理

Apache Flink 努力为所有开箱即用的应用程序自动派生合理的默认资源需求。对于希望更精细化调节资源消耗的用户，基于对特定场景的了解，Flink 提供了**细粒度资源管理**。
本文介绍了细粒度资源管理的使用、适用场景以及工作原理。

{{< hint warning >}}
**Note:** 本特性是当前的一个最简化产品(版本)的特性，它支持只在DataStream API [DataStream API]({{< ref "docs/dev/datastream/overview" >}})中使用。
{{< /hint >}}

## 使用场景

可能从细粒度资源管理中受益的典型场景包括：

  - Tasks 有显著不同的并行度的场景。

  - 整个pipeline需要的资源太大了以致不能和单一的slot/task Manager相适应的场景。

  - 批处理作业，其中不同stage的task所需的资源差异明显。

在它如何提高资源利用率 [How it improves resource efficiency](#how-it-improves-resource-efficiency)部分将会对细粒度资源管理为什么在以上使用场景中可以提高资源利用率作深入的讨论。


## 工作原理

如Flink架构 [Flink Architecture]({{< ref "docs/concepts/flink-architecture" >}}#anatomy-of-a-flink-cluster)中描述,
在一个TaskManager中,执行task时使用的资源被分割成许多个slots.
slot既是资源调度的基本单元,又是flink运行时申请资源的基本单元.
{{< img src="/fig/dynamic_slot_alloc.png" class="center" >}}

对于细粒度资源管理,Slot资源请求包含用户指定的特定的资源配置文件。Flink会遵从这些用户指定的资源请求并从TaskManager可用的资源中动态地切分出精确匹配的slot。如上图所示，对于一个slot，0.25core和1G内存的资源申请，Flink为它分配一个slot。

{{< hint info >}}
Flink之前的资源申请只包含必须指定的slots,但没有精细化的资源配置,这是一种粗粒度的资源管理.在这种管理方式下, TaskManager以固定相同的slots的个数的方式来满足资源需求。
{{< /hint >}}

对于没有指定资源配置的资源请求，Flink会自动决定资源配置。粗粒度资源管理当前被计算的资源来自TaskManager总资源[TaskManager’s total resource]({{< ref "docs/deployment/memory/mem_setup_tm" >}})和TaskManager的总slot数[taskmanager.numberOfTaskSlots]({{< ref "docs/deployment/config" >}}#taskmanager-numberoftaskslots)。
如上所示，TaskManager的总资源是1Core和4G内存，task的slot数设置为2，*Slot 2* 被创建，并申请0.5core和2G的内存而没有指定资源配置。
在分配slot1和slot2后，在TaskManager留下0.25核和1G的内存作为未使用资源.

详情请参考资源分配策略 [Resource Allocation Strategy](#resource-allocation-strategy)。


## 用法

为了可以使用细粒度的资源管理,需要做以下步骤:

  - 配置细粒度的资源管理

  - 指定资源请求

### Enable 细粒度资源管理

为了enable细粒度的资源管理配置,需要将[cluster.fine-grained-resource-management.enabled]的值设置为true({{< ref "docs/deployment/config" >}}#cluster-fine-grained-resource-management-enabled)。
{{< hint danger >}}
没有该配置,Flink运行job时并不能按照你指定的资源需求分配slots,并且job会失败抛出异常。
{{< /hint >}}

### 为Slot共享组指定资源请求

细粒度资源请求是基于slot共享组定义的。一个slot共享组是一个切入点，这意味着在TaskManager中的算子和tasks可以被置于相同的slot。

对于指定资源请求,应该:

  - 定义Slot共享组和它所包含的操作算子

  - 指定Slot共享组的资源

目前有两种方法定义槽共享组和它所包含的操作算子:
  
  - 可以只用它的名字定义一个Slot共享组并将它与一个算子绑定通过[slotSharingGroup(String name)]({{< ref "docs/dev/datastream/operators/overview" >}}#set-slot-sharing-group)。
  - 可以构造一个SlotSharingGroup的实例，该实例包含slot共享组的名字属性和一个可选的资源配置属性。SlotSharingGroup可以通过slotSharingGroup(SlotSharingGroup ssg)这个构造方法来绑定一个算子。

为Slot共享组指定资源配置:
  
  - 如果通过SlotSharingGroup来设置Slot共享组,可以在构建SlotSharingGroup实例的时候指定资源配置。

  - 构造一个SlotSharingGroup的实例，该实例包含slot共享组的名字属性和可选的资源配置属性。`SlotSharingGroup`可以通过slotSharingGroup(SlotSharingGroup ssg)构造方法绑定一个算子。
{{< tabs "configure-ssg" >}}
{{< tab "Java" >}}
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

SlotSharingGroup ssgA = SlotSharingGroup.newBuilder("a")
  .setCpuCores(1.0)
  .setTaskHeapMemoryMB(100)
  .build();

SlotSharingGroup ssgB = SlotSharingGroup.newBuilder("b")
  .setCpuCores(0.5)
  .setTaskHeapMemoryMB(100)
  .build();

someStream.filter(...).slotSharingGroup("a") // 设置Slot共享组的名字为‘a’ 
.map(...).slotSharingGroup(ssgB); // 直接设置Slot共享组的名字和资源.

env.registerSlotSharingGroup(ssgA); // 注册共享组的资源
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment

val ssgA = SlotSharingGroup.newBuilder("a")
  .setCpuCores(1.0)
  .setTaskHeapMemoryMB(100)
  .build()

val ssgB = SlotSharingGroup.newBuilder("b")
  .setCpuCores(0.5)
  .setTaskHeapMemoryMB(100)
  .build()

someStream.filter(...).slotSharingGroup("a") // 设置Slot共享组的名字为‘a’ 
.map(...).slotSharingGroup(ssgB) // 直接设置Slot共享组的名字和资源.

env.registerSlotSharingGroup(ssgA) // 注册共享组的资源
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()

ssg_a = SlotSharingGroup.builder('a') \
            .set_cpu_cores(1.0) \
            .set_task_heap_memory_mb(100) \
            .build()
ssg_b = SlotSharingGroup.builder('b') \
            .set_cpu_cores(0.5) \
            .set_task_heap_memory_mb(100) \
            .build()

some_stream.filter(...).slot_sharing_group('a') # 设置Slot共享组的名字为‘a’ 
.map(...).slot_sharing_group(ssg_b) # 直接设置Slot共享组的名字和资源.

env.register_slot_sharing_group(ssg_a) # 注册共享组的资源
```
{{< /tab >}}
{{< /tabs >}}

{{< hint warning >}}
**提示:** 每个共享组只能绑定一个指定的资源组,任何冲突将会导致job的编译的失败。
{{< /hint >}}

在构造SlotSharingGroup时,可以为Slot共享组设置以下资源:
  - **CPU核数**. 定义作业所需要的CPU核数. 该设置务必是明确配置的正值。
  - **[Task堆内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#task-operator-heap-memory)** 定义作业所需要的堆内存. 该设置务必是明确配置的正值。
  - **[堆外内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#configure-off-heap-memory-direct-or-native)** 定义作业所需要的堆外内存. 该设置可设置为0。
  - **[管理内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)** 定义作业所需要的管理内存,可设置为0。
  - **[外部资源]({{< ref "docs/deployment/advanced/external_resources" >}})**. 定义需要的外部资源,可设置为空。
{{< tabs "configure-resource" >}}
{{< tab "Java" >}}
```java
// Directly build a slot sharing group with specific resource
// 直接构建一个slot共享组，通过指定资源
SlotSharingGroup ssgWithResource =
    SlotSharingGroup.newBuilder("ssg")
        .setCpuCores(1.0) // required
        .setTaskHeapMemoryMB(100) // required
        .setTaskOffHeapMemoryMB(50)
        .setManagedMemory(MemorySize.ofMebiBytes(200))
        .setExternalResource("gpu", 1.0)
        .build();

// Build a slot sharing group without specific resource and then register the resource of it in StreamExecutionEnvironment
SlotSharingGroup ssgWithName = SlotSharingGroup.newBuilder("ssg").build();
env.registerSlotSharingGroup(ssgWithResource);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// Directly build a slot sharing group with specific resource
val ssgWithResource =
    SlotSharingGroup.newBuilder("ssg")
        .setCpuCores(1.0) // required
        .setTaskHeapMemoryMB(100) // required
        .setTaskOffHeapMemoryMB(50)
        .setManagedMemory(MemorySize.ofMebiBytes(200))
        .setExternalResource("gpu", 1.0)
        .build()

// Build a slot sharing group without specific resource and then register the resource of it in StreamExecutionEnvironment
val ssgWithName = SlotSharingGroup.newBuilder("ssg").build()
env.registerSlotSharingGroup(ssgWithResource)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# Directly build a slot sharing group with specific resource
ssg_with_resource = SlotSharingGroup.builder('ssg') \
            .set_cpu_cores(1.0) \
            .set_task_heap_memory_mb(100) \
            .set_task_off_heap_memory_mb(50) \
            .set_managed_memory(MemorySize.of_mebi_bytes(200)) \
            .set_external_resource('gpu', 1.0) \
            .build()

# Build a slot sharing group without specific resource and then register the resource of it in StreamExecutionEnvironment
# 构建一个slot共享组未指定资源，然后在StreamExecutionEnvironment中注册资源
ssg_with_name = SlotSharingGroup.builder('ssg').build()
env.register_slot_sharing_group(ssg_with_resource)
```
{{< /tab >}}
{{< /tabs >}}

{{< hint warning >}}
**提示:** 可以指定或者不指定资源配置构造SlotSharingGroup。
对于指定资源配置，必须明确地将 **CPU cores*** 和 **Task Heap Memory** 设置成正数值，其它设置则是可选的。
{{< /hint >}}

## 局限

因为细粒度资源管理是新的实验性特性,并不是所有的特性都被默认的调度器所支持.Flink社区正努力解决并突破这些限制。
  - **不支持弹性伸缩[Elastic Scaling]({{< ref "docs/deployment/elastic_scaling" >}})**. 弹性伸缩目前只支持不指定资源的slot请求。
  - **不支持TaskManager的冗余** TaskManager冗余 [slotmanager.redundant-taskmanager-num]({{< ref "docs/deployment/config" >}}#slotmanager-redundant-taskmanager-num) 用于启动冗余的TaskManager以加速job恢复。当前该配置在细粒度资源管理中不生效。
  - **不支持均匀分布的插槽策略** 此策略试图在所有可用的TaskManager中均匀分配插槽 [cluster.evenly-spread-out-slots]({{< ref "docs/deployment/config" >}}#cluster-evenly-spread-out-slots)。该策略在细粒度资源管理的第一个版本中不受支持，目前不会生效。
  - **与Flink Web UI有限的集成** 在细粒度的资源管理中,Slots会有不同的资源规格.目前Web UI页面只显示slot数量而不显示具体详情。
  - **与批作业有限的集成** 目前，细粒度资源管理需要在所有边缘都被阻塞的情况下执行批处理工作负载。为了达到该实现，需要将配置fine-grained.shuffle-mode.all-blocking[fine-grained.shuffle-mode.all-blocking]({{< ref "docs/deployment/config" >}}#fine-grained-shuffle-mode-all-blocking)设置为true。注意这样可能会影响性能。详情请见[FLINK-20865](https://issues.apache.org/jira/browse/FLINK-20865)。
  - **不建议使用混合资源需求** 不建议仅为工作的某些部分指定资源需求，而未指定其余部分的需求。目前，任何资源的插槽都可以满足未指定的要求。它获取的实际资源可能在不同的作业执行或故障切换中不一致。
  - **Slot分配结果可能不是最优** 正因为Slot需求包含资源的多维度方面,所以,Slot分配实际上是一个多维度问题,这是一个NP难题. 因些,在一些使用场景中,默认的资源分配策略[resource allocation strategy](#resource-allocation-strategy)可能不会使得Slot分配达到最优,而且还会导致资源碎片或者资源分配失败.

## 注意
  - **设置slot共享组可能改变性能** 为可链式操作的算子设置不同的slot共享组可能会导致链式操作[operator chains]({{< ref "docs/dev/datastream/operators/overview" >}}#task-chaining-and-resource-groups)产生割裂,从而改变性能.
  - **Slot 共享组不会限制算子的调度** Slot共享组仅仅意味调度器总是..... 但无法保证调度器总是和被分组的算子部署绑定在一起. 如果被分组算子被部署到单独的插槽中，Slot资源将从特定的资源组需求中派生而来。

## 深入讨论

### 如何提高资源使用率

这部分，我们对细粒度资源管理如何提高资源利用率作深入讨论，这会有助于你理解它对我们的jobs是否有益。
之前的Flink采用的一种粗粒度的资源管理，tasks被提前定义部署，通常被分配相同的slots而没有每个slot包含多少资源的概念。
对于许多jobs，使用粗粒度的资源管理并简单地把所有的tasks放入一个slot共享组[slot sharing group]({{< ref "docs/dev/datastream/operators/overview" >}}#set-slot-sharing-group)中运行，
就资源利用率而言，也能运行得很好。

  - 对于许多有相同并行度的tasks的流作业而言，每个slot会包含entire pipeline。[entire pipeline](https://flink.apache.org/2020/12/15/pipelined-region-sheduling.html#pipelined-regions). 理想情况条件下，所有的pipelines应该使用大致相同的资源，这可以容易被满足通过调节相同slot的资源。

  - tasks的资源消耗随时间变化不同。当一个task的资源消耗减少，其他的资源可以被另外一个task使用，该task的消耗增加。这就是被称为“调峰填谷效应”的现象，它降低了所需要的总体需求。
 
尽管如此，有些情况下使用粗粒度资源管理效果并不好。
  
  - Tasks会有不同的并行度。有时，这种不同的并行度是不可避免的。例如，source/sink/lookup这些类别的tasks的并行度可能被分区数和外部上下游系统的IO负载所限制。在这种情况下，拥有更少的tasks的slots会需要更少的资源相比tasks的整个pipeline[entire pipeline](https://flink.apache.org/2020/12/15/pipelined-region-sheduling.html#pipelined-regions)。
  - 有时整个pipeline[entire pipeline](https://flink.apache.org/2020/12/15/pipelined-region-sheduling.html#pipelined-regions)需要的资源可能会太大以致难于与单一的slot/TaskManager相适应。在这种情况下，pipeline需要被分割成多个SSGs，它们可能不总是有相同的资源需求。
  - 对于批作业，不是所有的tasks能够在同时被执行。因此，整个pipeline的瞬时资源需求而时间变化。

试图以相同的slots执行所有的tasks,这样会造成非最优的资源利用率。相同slots的资源能够满足最高的资源需求，这对于其他资源需求将是浪费的。当涉及到像GPU这样昂贵的外部资源时，这样的浪费将是难以承受的。细粒度资源管理运用不同资源的slots提高了资源利用率在种使用场景中。

### 资源分配策略

本节讨论的是Flink作业运行时的Slot分区机制和资源分配策略,包括在YARN和Kubernetes中运行Flink作业时,Flink如何选择TaskManager来切分成Slots和如何分配TaskManager的.({{< ref "docs/deployment/resource-providers/native_kubernetes" >}})
and [YARN]({{< ref "docs/deployment/resource-providers/yarn" >}})。值得注意的是,Flink运行时的资源分配策略是可插拔的以及我们在细粒度资源管理中的第一步引入它的默认实现。不久的将来,会有不同的策略供用户针对不同的使用场景选择使用。
{{< img src="/fig/resource_alloc.png" class="center" >}}

正如在[How it works](#how-it-works)中的描述，Flink会精确地从TaskManager中切分出匹配的slot为指定的slot资源请求。如上图内部进程所示，TaskManager将以总资源的形式被启动，但是没有预定义slots.当一个slot请求 0.25 core和1G的内存，
Flink将会选择一个有足够可用资源的TaskManger和创建一个新的已经被资源申请的slot。如果一个slot未被使用，它会将它的资源返回到TaskManager中的可用资源中去。

在当前的资源分配策略中，Flink会遍历所有被注册的TaskMangers并选择第一个有充足资源的TaskManager来满足Slot资源请求。当没有TaskManager有足够的资源时，Flink将会从Native Kubernetes[Native Kubernetes]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}})或者YARN[YARN]({{< ref "docs/deployment/resource-providers/yarn" >}})分配一个新的TaskManager.在当前的策略中，Flink会根据用户的配置[user’s configuration]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#set-up-taskmanager-memory)分配相同的TaskManagers
因为TaskManagers的规格组合是预定义的。

  - 集群中可能会存在资源碎片。例如，两个slots请求3G的堆内存，然而TaskManager总共的堆内存是4G,Flink会启动两个TaskManagers，每个TaskManager会有1G的堆内存被浪费。未来，可能会有一种资源分配策略，可以根据job的slot请求分配异构TaskManagers，从而缓解资源碎片。

  - 确保配置的slot共享组的资源组成不能大于TaskManager的总资源。否则，job会失败，并抛出异常。

{{< top >}}
