---
title: "应用程序生命周期"
weight: 9
type: docs
aliases:
  - /internals/application_lifecycle.html
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

# 应用程序生命周期

Flink 中的应用程序代表一段用于执行的用户自定义逻辑。它提供了统一的抽象来跟踪用户 `main()` 方法的执行状态并管理其关联的作业。更多详情请参阅 [FLIP-549](https://cwiki.apache.org/confluence/display/FLINK/FLIP-549%3A+Support+Application+Management)
和 [FLIP-560](https://cwiki.apache.org/confluence/display/FLINK/FLIP-560%3A+Application+Capability+Enhancement)。

## 集群-应用程序-作业架构

Flink 现在使用三层结构：**集群-应用程序-作业**。该结构统一了不同的部署模式，并提供了用户逻辑执行的可观测性和可管理性。

集群可以在两种模式下运行：
- **应用程序模式**：每个应用程序一个集群
- **会话模式**：一个集群用于多个应用程序

一个应用程序可以包含 0 到 N 个作业，每个作业与一个应用程序关联。

## 应用程序实现

{{< gh_link file="/flink-runtime/src/main/java/org/apache/flink/runtime/application/AbstractApplication.java" name="AbstractApplication" >}} 是所有应用程序的基类。Flink 提供了两个具体实现：

- **PackagedProgramApplication**：封装用户 JAR 并执行其 `main()` 方法。适用于**应用程序模式**或**通过 REST API `/jars/:jarid/run-application` 提交的会话模式**。应用程序的生命周期与用户 `main()` 方法的执行绑定。

- **SingleJobApplication**：将单个作业的提交封装为一个轻量级的 `main()` 方法。适用于提交单个作业的情况，例如**通过 CLI 提交的会话模式**。应用程序的生命周期与作业的执行状态绑定。

## 应用程序状态

应用程序从 *已创建* (created) 状态开始，一旦执行开始则切换到 *运行中* (running) 状态。
当执行正常完成且与该应用程序关联的所有作业都已达到终止状态时，应用程序转换到 *已结束* (finished) 状态。
如果发生故障，应用程序首先切换到 *失败中* (failing) 状态，在此状态下它会取消所有非终止状态的作业。所有作业达到终止状态后，应用程序转换到 *已失败* (failed) 状态。

如果用户取消应用程序，它将进入 *取消中* (canceling) 状态。
这也意味着取消其所有非终止状态的作业。
一旦所有作业达到终止状态，应用程序转换到 *已取消* (canceled) 状态。

状态 *已结束*、*已取消* 和 *已失败* 是终止状态，会触发应用程序的归档和清理操作。

{{< img src="/fig/application_status.png" alt="应用程序的状态和转换" width="50%" >}}

## 应用程序提交

应用程序提交到集群并通过不同的机制开始执行，具体取决于部署模式和提交方式。

在**应用程序模式**中，集群是为单个应用程序专门启动的。在集群启动期间，会自动从用户的 JAR 文件生成一个 `PackagedProgramApplication`。集群就绪后应用程序立即开始执行，其生命周期与用户 `main()` 方法的执行绑定。

在**会话模式**中，多个应用程序可以共享同一个集群。应用程序可以通过各种接口提交：

- **REST API (`/jars/:jarid/run-application`)**：此端点从上传的用户 JAR 创建一个 `PackagedProgramApplication` 并开始执行。与应用程序模式类似，应用程序的生命周期与用户的 `main()` 方法绑定。

- **REST API (`/jars/:jarid/run`)** 和 **CLI 提交**：这些接口直接执行用户的 `main()` 方法。当方法调用 `execute()` 提交作业时，该作业被封装为 `SingleJobApplication`。在这种情况下，应用程序的生命周期与作业的执行状态绑定，使其成为单作业提交的轻量级封装。

{{< gh_link file="/flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/Dispatcher.java" name="Dispatcher" >}} 管理集群中的所有应用程序。它提供查询应用程序状态、管理应用程序生命周期以及处理取消和恢复等操作的接口。

{{< top >}}
