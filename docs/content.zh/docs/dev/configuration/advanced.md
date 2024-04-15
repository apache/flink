---
title: "高级配置"
weight: 10
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

# 高级配置主题

## Flink 依赖剖析

Flink 自身由一组类和依赖项组成，这些共同构成了 Flink 运行时的核心，在 Flink 应用程序启动时必须存在，会提供诸如通信协调、网络管理、检查点、容错、API、算子（如窗口）、资源管理等领域的服务。

这些核心类和依赖项都打包在 `flink-dist.jar`，可以在下载的发行版 `/lib` 文件夹中找到，也是 Flink 容器镜像的基础部分。您可以将其近似地看作是包含 `String` 和 `List` 等公用类的 Java 核心库。

为了保持核心依赖项尽可能小并避免依赖冲突，Flink Core Dependencies 不包含任何连接器或库（如 CEP、SQL、ML），以避免在类路径中有过多的类和依赖项。

Flink 发行版的 `/lib` 目录里还有包括常用模块在内的各种 JAR 文件，例如 [执行 Table 作业的必需模块](#Table-依赖剖析) 、一组连接器和 format。默认情况下会自动加载，若要禁止加载只需将它们从 classpath 中的 `/lib` 目录中删除即可。

Flink 还在 `/opt` 文件夹下提供了额外的可选依赖项，可以通过移动这些 JAR 文件到 `/lib` 目录来启用这些依赖项。

有关类加载的更多细节，请查阅 [Flink 类加载]({{< ref "docs/ops/debugging/debugging_classloading.zh.md" >}})。

## Scala 版本

不同的 Scala 版本二进制不兼容，所有（传递地）依赖于 Scala 的 Flink 依赖项都以它们构建的 Scala 版本为后缀（如 `flink-streaming-scala_2.12`）。

如果您只使用 Flink 的 Java API，您可以使用任何 Scala 版本。如果您使用 Flink 的 Scala API，则需要选择与应用程序的 Scala 匹配的 Scala 版本。

有关如何为特定 Scala 版本构建 Flink 的细节，请查阅[构建指南]({{< ref "docs/flinkDev/building" >}}#scala-versions)。

2.12.8 之后的 Scala 版本与之前的 2.12.x 版本二进制不兼容，使 Flink 项目无法将其 2.12.x 版本直接升级到 2.12.8 以上。您可以按照[构建指南]({{< ref "docs/flinkDev/building" >}}#scala-versions)在本地为更高版本的 Scala 构建 Flink 。为此，您需要在构建时添加 `-Djapicmp.skip` 以跳过二进制兼容性检查。

有关更多细节，请查阅 [Scala 2.12.8 版本说明](https://github.com/scala/scala/releases/tag/v2.12.8)。相关部分指出：

第二项修改是二进制不兼容的：2.12.8 编译器忽略了由更早版本的 2.12 编译器生成的某些方法。然而我们相信这些方法永远不会被使用，现有的编译代码仍可工作。有关更多详细信息，请查阅[pull request 描述](https://github.com/scala/scala/pull/7469)。

## Table 依赖剖析

Flink 发行版默认包含执行 Flink SQL 任务的必要 JAR 文件（位于 `/lib` 目录），主要有：

- `flink-table-api-java-uber-{{< version >}}.jar` &#8594; 包含所有的 Java API；
- `flink-table-runtime-{{< version >}}.jar` &#8594; 包含 Table 运行时;
- `flink-table-planner-loader-{{< version >}}.jar` &#8594; 包含查询计划器。

{{< hint warning >}}
以前，这些 JAR 都打包进了 `flink-table.jar`，自从 Flink 1.15 开始，已将其划分成三个 JAR，以允许用户使用 `flink-table-planner-loader-{{< version >}}.jar` 充当 `flink-table-planner{{< scala_version >}}-{{< version >}}.jar`。
{{< /hint >}}

虽然 Table Java API 内置于发行版中，但默认情况下不包含 Table Scala API。在 Flink Scala API 中使用格式和连接器时，您需要手动下载这些 JAR 包并将其放到发行版的 `/lib` 文件夹中（推荐），或者将它们打包为 Flink SQL 作业的 uber/fat JAR 包中的依赖项。

有关更多细节，请查阅如何[连接外部系统]({{< ref "docs/connectors/table/overview" >}})。

### Table Planner 和 Table Planner 加载器

从 Flink 1.15 开始，发行版包含两个 planner:

- `flink-table-planner{{< scala_version >}}-{{< version >}}.jar`, 位于 `/opt` 目录, 包含查询计划器；
- `flink-table-planner-loader-{{< version >}}.jar`, 位于 `/lib` 目录默认被加载, 包含隐藏在单独的 classpath 里的查询计划器 (您无法直接使用 `io.apache.flink.table.planner` 包)。

这两个 planner JAR 文件的代码功能相同，但打包方式不同。若使用第一个文件，您必须使用与其相同版本的 Scala；若使用第二个，由于 Scala 已经被打包进该文件里，您不需要考虑 Scala 版本问题。

默认情况下，发行版使用 `flink-table-planner-loader`。如果想使用内部查询计划器，您可以换掉 JAR 包（拷贝 `flink-table-planner{{< scala_version >}}.jar` 并复制到发行版的 `/lib` 目录）。请注意，此时会被限制用于 Flink 发行版的 Scala 版本。

{{< hint danger >}}
这两个 planner 无法同时存在于 classpath，如果您在 `/lib` 目录同时加载他们，Table 任务将会失败。
{{< /hint >}}

{{< hint warning >}}
在即将发布的 Flink 版本中，我们将停止在 Flink 发行版中发布 `flink-table-planner{{< scala_version >}}` 组件。我们强烈建议迁移您的作业/自定义连接器/格式以使用前述 API 模块，而不依赖此内部 planner。如果您需要 planner 中尚未被 API 模块暴露的一些功能，请与社区讨论。
{{< /hint >}}

## Hadoop 依赖

**一般规则：** 没有必要直接添加 Hadoop 依赖到您的应用程序里，如果您想将 Flink 与 Hadoop 一起使用，您需要有一个包含 Hadoop 依赖项的 Flink 系统，而不是添加 Hadoop 作为应用程序依赖项。换句话说，Hadoop 必须是 Flink 系统本身的依赖，而不是用户代码的依赖。Flink 将使用 `HADOOP_CLASSPATH` 环境变量指定 Hadoop 依赖项，可以这样设置：

```bash
export HADOOP_CLASSPATH=`hadoop classpath`
```

这样设计有两个主要原因：

- 一些 Hadoop 交互可能在用户应用程序启动之前就发生在 Flink 内核。其中包括为检查点配置 HDFS、通过 Hadoop 的 Kerberos 令牌进行身份验证或在 YARN 上部署；

- Flink 的反向类加载方式在核心依赖项中隐藏了许多传递依赖项。这不仅适用于 Flink 自己的核心依赖项，也适用于已有的 Hadoop 依赖项。这样，应用程序可以使用相同依赖项的不同版本，而不会遇到依赖项冲突。当依赖树变得非常大时，这非常有用。

如果您在 IDE 内开发或测试期间需要 Hadoop 依赖项（比如用于 HDFS 访问），应该限定这些依赖项的使用范围（如 *test* 或 *provided*）。
