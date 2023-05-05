---
title: 细粒度资源管理
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

Apache Flink 致力于自动分配的默认资源合理且都能满足所有应用需求,做到开箱即用。对于希望更精细化调节资源消耗的用户，基于对特定场景的了解，Flink 提供了**细粒度资源管理**。
本文介绍了细粒度资源管理的使用、适用场景以及工作原理。

{{< hint warning >}}
**注意:** 本特性是当前的一个最简化产品(版本)的特性，它只支持在 [DataStream API]({{< ref "docs/dev/datastream/overview" >}})中使用。
{{< /hint >}}

## 使用场景

可能从细粒度资源管理中受益的典型场景包括：

- Tasks 有显著不同的并行度的场景。

- 整个 pipeline 需要的资源太大了以致不能和单一的 Slot/TaskManager 相适应的场景。

- 批处理作业，其中不同 stage 的tasks 所需的资源差异明显。

在[如何提高资源利用率](#how-it-improves-resource-efficiency)部分将会对细粒度资源管理为什么在以上使用场景中可以提高资源利用率作深入的讨论。


## <a name="how-it-works">工作原理</a>

如[Flink架构]({{< ref "docs/concepts/flink-architecture" >}}#anatomy-of-a-flink-cluster)中描述,
在一个 TaskManager 中,执行 task 时使用的资源被分割成许多个 slots。
Slot 既是资源调度的基本单元,又是Flink运行时申请资源的基本单元。

{{< img src="/fig/dynamic_slot_alloc.png" class="center" >}}

对于细粒度资源管理,Slot 资源请求包含用户指定的特定的资源配置文件。Flink 会遵从这些用户指定的资源请求并从 TaskManager 可用的资源中动态地切分出精确匹配的 slot。如上图所示，对于一个 slot，0.25Core 和 1GB 内存的资源申请，Flink 为它分配 slot 1。

{{< hint info >}}
Flink之前的资源申请只包含必须指定的 slots,但没有精细化的资源配置,这是一种粗粒度的资源管理.在这种管理方式下, TaskManager 以固定相同的 slots 的个数的方式来满足资源需求。
{{< /hint >}}

对于没有指定资源配置的资源请求，Flink会自动决定资源配置。粗粒度资源管理当前被计算的资源来自 [TaskManager总资源]({{< ref "docs/deployment/memory/mem_setup_tm" >}})和TaskManager的总 slot 数 [taskmanager.numberOfTaskSlots]({{< ref "docs/deployment/config" >}}#taskmanager-numberoftaskslots)。
如上所示，TaskManager 的总资源是 1Core 和 4GB 内存，task 的 slot 数设置为2，*Slot 2* 被创建，并申请 0.5 Core和 2GB 的内存而没有指定资源配置。
在分配 Slot 1和 Slot 2后，在 TaskManager 留下 0.25 Core 和 1GB 的内存作为未使用资源。

详情请参考[资源分配策略](#resource-allocation-strategy)。


## 用法

使用细粒度的资源管理，需要指定资源请求。

细粒度资源请求是基于 slot 共享组定义的。一个 slot 共享组是一个切入点，这意味着在 TaskManager 中的算子和 tasks 可以被置于相同的 slot。

对于指定资源请求,应该:

- 定义 Slot 共享组和它所包含的操作算子

- 指定 Slot 共享组的资源

目前有两种方法定义槽共享组和它所包含的操作算子:

- 可以只用它的名字定义一个 Slot 共享组并将它与一个算子绑定通过 [slotSharingGroup(String name)]({{< ref "docs/dev/datastream/operators/overview" >}}#set-slot-sharing-group)。

- 可以构造 SlotSharingGroup 的一个实例，该实例包含 slot 共享组的名字属性和一个可选的资源配置属性。SlotSharingGroup 可以通过slotSharingGroup(SlotSharingGroup ssg)这个构造方法来绑定一个算子。

为 Slot 共享组指定资源配置:

- 如果通过 SlotSharingGroup 来设置 Slot 共享组,可以在构建 SlotSharingGroup 实例的时候指定资源配置。

- 构造一个 SlotSharingGroup 的实例，该实例包含 slot 共享组的名字属性和可选的资源配置属性。`SlotSharingGroup`可以通过 slotSharingGroup(SlotSharingGroup ssg)构造方法绑定一个算子。

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
**提示:** 每个共享组只能绑定一个指定的资源组，任何冲突将会导致 job 的编译的失败。
{{< /hint >}}

在构造 SlotSharingGroup 时，可以为 Slot 共享组设置以下资源:

- **CPU核数** 定义作业所需要的CPU核数， 该设置务必是明确配置的正值。
- **[Task堆内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#task-operator-heap-memory)** 定义作业所需要的堆内存，该设置务必是明确配置的正值。
- **[堆外内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#configure-off-heap-memory-direct-or-native)** 定义作业所需要的堆外内存，该设置可设置为0。
- **[管理内存]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)** 定义作业所需要的管理内存，可设置为0。
- **[外部资源]({{< ref "docs/deployment/advanced/external_resources" >}})** 定义需要的外部资源，可设置为空。
  {{< tabs "configure-resource" >}}
  {{< tab "Java" >}}
```java
// 通过指定资源直接构建一个 slot 共享组
SlotSharingGroup ssgWithResource =
    SlotSharingGroup.newBuilder("ssg")
        .setCpuCores(1.0) // required
        .setTaskHeapMemoryMB(100) // required
        .setTaskOffHeapMemoryMB(50)
        .setManagedMemory(MemorySize.ofMebiBytes(200))
        .setExternalResource("gpu", 1.0)
        .build();

// 构建一个 slot 共享组未指定资源，然后在 StreamExecutionEnvironment 中注册资源
SlotSharingGroup ssgWithName = SlotSharingGroup.newBuilder("ssg").build();
env.registerSlotSharingGroup(ssgWithResource);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// 通过指定资源直接构建一个 slot 共享组
val ssgWithResource =
    SlotSharingGroup.newBuilder("ssg")
        .setCpuCores(1.0) // required
        .setTaskHeapMemoryMB(100) // required
        .setTaskOffHeapMemoryMB(50)
        .setManagedMemory(MemorySize.ofMebiBytes(200))
        .setExternalResource("gpu", 1.0)
        .build()

// 构建一个 slot 共享组未指定资源，然后在 StreamExecutionEnvironment中注册资源
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

# 构建一个 slot 共享组未指定资源，然后在 StreamExecutionEnvironment中注册资源
ssg_with_name = SlotSharingGroup.builder('ssg').build()
env.register_slot_sharing_group(ssg_with_resource)
```
{{< /tab >}}
{{< /tabs >}}

{{< hint warning >}}
**提示:** 可以指定或者不指定资源配置构造 SlotSharingGroup。
对于指定资源配置，必须明确地将 **CPU cores**和 **Task Heap Memory**设置成正数值，其它设置则是可选的。
{{< /hint >}}

## 局限

因为细粒度资源管理是新的实验性特性,并不是所有的特性都被默认的调度器所支持.Flink 社区正努力解决并突破这些限制。
- **不支持[弹性伸缩]({{< ref "docs/deployment/elastic_scaling" >}})**. 弹性伸缩目前只支持不指定资源的slot请求。
- **与Flink Web UI有限的集成** 在细粒度的资源管理中,Slots会有不同的资源规格.目前Web UI页面只显示 slot 数量而不显示具体详情。
- **与批作业有限的集成** 目前，细粒度资源管理需要在所有边缘都被阻塞的情况下执行批处理工作负载。为了达到该实现，需要将配置 [fine-grained.shuffle-mode.all-blocking]({{< ref "docs/deployment/config" >}}#fine-grained-shuffle-mode-all-blocking)设置为 true。注意这样可能会影响性能。详情请见[FLINK-20865](https://issues.apache.org/jira/browse/FLINK-20865)。
- **不建议使用混合资源需求** 不建议仅为工作的某些部分指定资源需求，而未指定其余部分的需求。目前，任何资源的插槽都可以满足未指定的要求。它获取的实际资源可能在不同的作业执行或故障切换中不一致。
- **Slot分配结果可能不是最优** 正因为Slot需求包含资源的多维度方面,所以,Slot分配实际上是一个多维度问题,这是一个NP难题. 因些,在一些使用场景中,默认的[资源分配策略](#resource-allocation-strategy)可能不会使得Slot分配达到最优,而且还会导致资源碎片或者资源分配失败.

## 注意
- **设置 Slot 共享组可能改变性能** 为可链式操作的算子设置不同的 slot 共享组可能会导致链式操作 [operator chains]({{< ref "docs/dev/datastream/operators/overview" >}}#task-chaining-and-resource-groups)产生割裂,从而改变性能.
- **Slot 共享组不会限制算子的调度** Slot 共享组仅仅意味着调度器可以使被分组的算子被部署到中一个Slot中,但无法保证调度器总是和被分组的算子部署绑定在一起。如果被分组算子被部署到单独的Slot中，Slot资源将从特定的资源组需求中派生而来。

## 深入讨论

### <a name="how-it-improves-resource-efficiency">如何提高资源使用率</a>  

这部分，我们对细粒度资源管理如何提高资源利用率作深入讨论，这会有助于你理解它对我们的 jobs 是否有益。
之前的 Flink 采用的一种粗粒度资源管理的方式，tasks 被提前定义部署，通常被分配相同的 slots 而没有每个 slot 包含多少资源的概念。
对于许多jobs，使用粗粒度的资源管理并简单地把所有的tasks放入一个 [Slot 共享组]({{< ref "docs/dev/datastream/operators/overview" >}}#set-slot-sharing-group)中运行，就资源利用率而言，也能运行得很好。

- 对于许多有相同并行度的 tasks 的流作业而言，每个 slot 会包含[整个 pipeline](https://flink.apache.org/2020/12/15/pipelined-region-sheduling.html#pipelined-regions)。理想情况条件下，所有的 pipelines 应该使用大致相同的资源，这可以容易被满足通过调节相同 slot 的资源。

- tasks 的资源消耗随时间变化不同。当一个 task 的资源消耗减少，其他的资源可以被另外一个 task 使用，该 task 的消耗增加。这就是被称为“调峰填谷效应”的现象，它降低了所需要的总体需求。

尽管如此，有些情况下使用粗粒度资源管理效果并不好。

- Tasks 会有不同的并行度。有时，这种不同的并行度是不可避免的。例如，象 source/sink/lookup 这些类别的 tasks 的并行度可能被分区数和外部上下游系统的 IO 负载所限制。在这种情况下，拥有更少的 tasks 的 slots 会需要更少的资源相比 tasks 的 [整个 pipeline](https://flink.apache.org/2020/12/15/pipelined-region-sheduling.html#pipelined-regions)。
- 有时[整个pipeline](https://flink.apache.org/2020/12/15/pipelined-region-sheduling.html#pipelined-regions) 需要的资源可能会太大以致难于与单一的 slot/TaskManager 的场景相适应。在这种情况下，pipeline 需要被分割成多个 SSGs，它们可能不总是有相同的资源需求。
- 对于批作业，不是所有的 tasks 能够在同时被执行。因此，整个 pipeline 的瞬时资源需求而时间变化。

试图以相同的 slots 执行所有的tasks,这样会造成非最优的资源利用率。相同 slot 的资源能够满足最高的资源需求，这对于其他资源需求将是浪费的。当涉及到像 GPU 这样昂贵的外部资源时，这样的浪费将是难以承受的。细粒度资源管理运用不同资源的 slots 提高了资源利用率在种使用场景中。

### <a name="resource-allocation-strategy">资源分配策略</a> 

本节讨论的是 Flink 作业运行时的 Slot 分区机制和资源分配策略,包括在 YARN 和 Kubernetes 中运行 Flink 作业时,Flink 如何选择 TaskManager 来切分成 Slots 和如何分配 TaskManager 的。[Native Kubernetes]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}})
and [YARN]({{< ref "docs/deployment/resource-providers/yarn" >}})。值得注意的是,Flink运行时的资源分配策略是可插拔的以及我们在细粒度资源管理中的第一步引入它的默认实现。不久的将来,会有不同的策略供用户针对不同的使用场景选择使用。
{{< img src="/fig/resource_alloc.png" class="center" >}}

正如在[工作原理](#how-it-works)中的描述，Flink 会精确地从 TaskManager 中切分出匹配的 slot 为指定的 slot 资源请求。如上图内部进程所示，TaskManager将以总资源的形式被启动，但是没有提前指定 slots。当一个 slot 请求 0.25 Core 和 1GB 的内存，Flink 将会选择一个有足够可用资源的 TaskManger 和创建一个新的已经被资源申请的 slot。如果一个 slot 未被使用，它会将它的资源返回到 TaskManager 中的可用资源中去。

在当前的资源分配策略中，Flink 会遍历所有被注册的 TaskMangers 并选择第一个有充足资源的 TaskManager 来满足 Slot 资源请求。当没有 TaskManager 有足够的资源时，Flink将会从 [Native Kubernetes]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}})或者 [YARN]({{< ref "docs/deployment/resource-providers/yarn" >}})分配一个新的 TaskManager。在当前的策略中，Flink 会根据 [用户配置]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#set-up-taskmanager-memory)分配相同的TaskManagers，

因为 TaskManagers 的规格组合是预定义的。

- 集群中可能会存在资源碎片。例如，两个 slots 请求 3GB 堆内存，然而 TaskManager 总共的堆内存是 4GB，Flink 会启动两个 TaskManagers，每个 TaskManager 会有1G的堆内存被浪费。未来，可能会有一种资源分配策略，可以根据 job 的 slot 请求分配异构 TaskManagers，从而缓解资源碎片。

- 确保配置的 Slot 共享组的资源组成不能大于 TaskManager 的总资源。否则，job 会失败，并抛出异常。

{{< top >}}
