---
title: "使用 Gradle"
weight: 3
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

# 如何使用 Gradle 配置您的项目

您可能需要一个构建工具来配置您的 Flink 项目，本指南将向您展示如何使用 [Gradle](https://gradle.org) 执行此操作。Gradle 是一个开源的通用构建工具，可用于在开发过程中自动化执行任务。

## 要求

- Gradle 7.x 
- Java 8 (deprecated) 或 Java 11

## 将项目导入 IDE

创建[项目目录和文件]({{< ref "docs/dev/configuration/overview#getting-started" >}})后，我们建议您将此项目导入到 IDE 进行开发和测试。

IntelliJ IDEA 通过 `Gradle` 插件支持 Gradle 项目。

Eclipse 通过 [Eclipse Buildship](https://projects.eclipse.org/projects/tools.buildship) 插件执行此操作（确保在导入向导的最后一步中指定 Gradle 版本 >= 3.0，`shadow` 插件会用到它）。您还可以使用 [Gradle 的 IDE 集成](https://docs.gradle.org/current/userguide/userguide.html#ide-integration) 来使用 Gradle 创建项目文件。

**注意：** Java 的默认 JVM 堆大小对于 Flink 来说可能太小，您应该手动增加它。在 Eclipse 中，选中 `Run Configurations -> Arguments` 并在 `VM Arguments` 框里填上：`-Xmx800m`。在 IntelliJ IDEA 中，推荐选中 `Help | Edit Custom VM Options` 菜单修改 JVM 属性。详情请查阅[本文](https://intellij-support.jetbrains.com/hc/en-us/articles/206544869-Configuring-JVM-options-and-platform-properties)。

**关于 IntelliJ 的注意事项：** 要使应用程序在 IntelliJ IDEA 中运行，需要在运行配置中的 `Include dependencies with "Provided" scope` 打勾。如果此选项不可用（可能是由于使用了较旧的 IntelliJ IDEA 版本），可创建一个调用应用程序 `main()` 方法的测试用例。

## 构建项目

如果您想 __构建/打包__ 您的项目，请转到您的项目目录并运行 '`gradle clean shadowJar`' 命令。您将 __找到一个 JAR 文件__，其中包含您的应用程序，还有已作为依赖项添加到应用程序的连接器和库：`build/libs/<project-name>-<version>-all.jar`。

__注意：__ 如果您使用不同于 *StreamingJob* 的类作为应用程序的主类/入口点，我们建议您对 `build.gradle` 文件里的 `mainClassName` 配置进行相应的修改。这样，Flink 可以通过 JAR 文件运行应用程序，而无需额外指定主类。

## 向项目添加依赖项

在 `build.gradle` 文件的 dependencies 块中配置依赖项

例如，如果您使用我们的 Gradle 构建脚本或快速启动脚本创建了项目，如下所示，可以将 Kafka 连接器添加为依赖项：

**build.gradle**

```gradle
...
dependencies {
    ...  
    flinkShadowJar "org.apache.flink:flink-connector-kafka:${flinkVersion}"
    ...
}
...
```

**重要提示：** 请注意，应将所有这些（核心）依赖项的生效范围置为 [*provided*](https://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html#dependency-scope)。这意味着需要对它们进行编译，但不应将它们打包进项目生成的应用程序 JAR 文件中。如果不设置为 *provided*，最好的情况是生成的 JAR 变得过大，因为它还包含所有 Flink 核心依赖项。最坏的情况是添加到应用程序 JAR 文件中的 Flink 核心依赖项与您自己的一些依赖项的版本冲突（通常通过反向类加载来避免）。

要将依赖项正确地打包进应用程序 JAR 中，必须把应用程序依赖项的生效范围设置为 *compile* 。

## 打包应用程序

在部署应用到 Flink 环境之前，您需要根据使用场景用不同的方式打包 Flink 应用程序。

如果您想为 Flink 作业创建 JAR 并且只使用 Flink 依赖而不使用任何第三方依赖（比如使用 JSON 格式的文件系统连接器），您不需要创建一个 uber/fat JAR 或将任何依赖打进包。

您可以使用 `gradle clean installDist` 命令，如果您使用的是 [Gradle Wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html) ，则用 `./gradlew clean installDist`。

如果您想为 Flink 作业创建 JAR 并使用未内置在 Flink 发行版中的外部依赖项，您可以将它们添加到发行版的类路径中，或者将它们打包进您的 uber/fat 应用程序 JAR 中。

您可以使用该命令 `gradle clean installShadowDist`，该命令将在 `/build/install/yourProject/lib` 目录生成一个 fat JAR。如果您使用的是 [Gradle Wrapper](https://docs.gradle.org/current/userguide/gradle_wrapper.html) ，则用 `./gradlew clean installShadowDist`。

您可以将生成的 uber/fat JAR 提交到本地或远程集群：

```sh
bin/flink run -c org.example.MyJob myFatJar.jar
```

要了解有关如何部署 Flink 作业的更多信息，请查看[部署指南]({{< ref "docs/deployment/cli" >}})。
