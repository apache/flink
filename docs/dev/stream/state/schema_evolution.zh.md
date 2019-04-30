---
title: "状态数据结构升级"
nav-parent_id: streaming_state
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

* ToC
{:toc}

Apache Flink 流应用通常被设计为永远或者长时间运行。
与所有长期运行的服务一样，需要更新应用程序以适应不断变化的需求。
对于应用程序处理的数据结构来说，这是相同的；它们也随应用程序一起升级。

此页面概述了如何升级状态类型的数据结构。
目前的限制因不同类型和状态结构而异（`ValueState`、`ListState` 等）。

请注意，此页面的信息只与 Flink 自己生成的状态序列化器相关 [类型序列化框架]({{ site.baseurl }}/zh/dev/types_serialization.html)。
也就是说，在声明状态时，状态描述符不可以配置为使用特定的 TypeSerializer 或 TypeInformation ，
在这种情况下，Flink 会推断状态类型的信息：

<div data-lang="java" markdown="1">
{% highlight java %}
ListStateDescriptor<MyPojoType> descriptor =
    new ListStateDescriptor<>(
        "state-name",
        MyPojoType.class);

checkpointedState = getRuntimeContext().getListState(descriptor);
{% endhighlight %}
</div>

在内部，状态是否可以进行升级取决于用于读写持久化状态字节的序列化器。
简而言之，状态数据结构只有在其序列化器正确支持时才能升级。
这一过程是被 Flink 的类型序列化框架生成的序列化器透明处理的（[下面]({{ site.baseurl }}/zh/dev/stream/state/schema_evolution.html#数据结构升级支持的数据类型) 列出了当前的支持范围）。

如果你想要为你的状态类型实现自定义的 `TypeSerializer` 并且想要学习如何实现支持状态数据结构升级的序列化器，
可以参考 [自定义状态序列化器]({{ site.baseurl }}/zh/dev/stream/state/custom_serialization.html)。
本文档也包含一些用于支持状态数据结构升级的状态序列化器与 Flink 状态后端存储相互作用的必要内部细节。

## 升级状态数据结构

为了对给定的状态类型进行升级，你需要采取以下几个步骤：

 1. 对 Flink 流作业进行 savepoint 操作。
 2. 升级程序中的状态类型（例如：修改你的 Avro 结构）。
 3. 从 savepoint 处重启作业。当第一次访问状态数据时，Flink 会评估状态数据结构是否已经改变，并在必要的时候进行状态结构迁移。

用来适应状态结构的改变而进行的状态迁移过程是自动发生的，并且状态之间是互相独立的。
Flink 内部是这样来进行处理的，首先会检查新的序列化器相对比之前的序列化器是否有不同的状态结构；如果有，
那么之前的序列化器用来读取状态数据字节到对象，然后使用新的序列化器将对象回写为字节。

更多的迁移过程细节不在本文档谈论的范围；可以参考[文档]({{ site.baseurl }}/zh/dev/stream/state/custom_serialization.html)。

## 数据结构升级支持的数据类型

目前，仅对于 POJO 以及 Avro 类型支持数据结构升级。
因此，如果你比较关注于状态数据结构的升级，那么目前来看强烈推荐使用 Pojo 或者 Avro 状态数据类型。

我们有计划支持更多的复合类型；更多的细节可以参考 [FLINK-10896](https://issues.apache.org/jira/browse/FLINK-10896)。

### POJO 类型

Flink 基于下面的规则来支持 [POJO 类型]({{ site.baseurl }}/zh/dev/types_serialization.html#pojo-类型的规则)结构的升级:

 1. 不可以删除字段。一旦删除，被删除字段的前值将会在将来的 checkpoints 以及 savepoints 中删除。
 2. 可以添加字段。新字段会使用类型对应的默认值进行初始化，比如 [Java 类型](https://docs.oracle.com/javase/tutorial/java/nutsandbolts/datatypes.html)。   
 3. 不可以修改字段的声明类型。
 4. 不可以改变 POJO 类型的类名，包括类的命名空间。

需要注意，只有对使用1.8.0及以上版本的 Flink savepoint 进行恢复时，POJO 类型的状态才可以进行升级。
对1.8.0版本之前的 Flink 是没有办法进行 POJO 类型升级的。

### Avro 类型

Flink 完全支持 Avro 状态类型的升级，只要数据结构的修改是被
[Avro 的数据结构解析规则](http://avro.apache.org/docs/current/spec.html#Schema+Resolution)认为兼容的即可。

一个限制是，当作业恢复时，Avro 生成的用作状态类型的类无法重定位或具有不同的命名空间。

{% top %}
