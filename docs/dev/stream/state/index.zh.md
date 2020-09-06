---
title: "状态与容错"
nav-id: streaming_state
nav-title: "状态与容错"
nav-parent_id: streaming
nav-pos: 3
nav-show_overview: true
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

你将在本节中了解到 Flink 提供的用于编写有状态程序的 API，想了解更多有状态流处理的概念，请查看[有状态的流处理]({% link concepts/stateful-stream-processing.zh.md %})
{% top %}

接下来看什么?
-----------------

* [Working with State](state.html): 描述了如何在 Flink 应用程序中使用状态，以及不同类型的状态。
* [The Broadcast State 模式](broadcast_state.html): 描述了如何将广播流和非广播流进行连接从而交换数据。
* [Checkpointing](checkpointing.html): 介绍了如何开启和配置 checkpoint，以实现状态容错。
* [Queryable State](queryable_state.html): 介绍了如何从外围访问 Flink 的状态。
* [状态数据结构升级](schema_evolution.html): 介绍了状态数据结构升级相关的内容。
* [Managed State 的自定义序列化器](custom_serialization.html): 介绍了如何实现自定义的序列化器，尤其是如何支持状态数据结构升级。

{% top %}
