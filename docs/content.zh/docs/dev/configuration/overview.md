---
title: "概览"
weight: 1
type: docs
aliases:
- /dev/project-configuration.html
- /start/dependencies.html
- /getting-started/project-setup/dependencies.html
- /quickstart/java_api_quickstart.html
- /dev/projectsetup/java_api_quickstart.html
- /dev/linking_with_flink.html
- /dev/linking.html
- /dev/projectsetup/dependencies.html
- /dev/projectsetup/java_api_quickstart.html
- /getting-started/project-setup/java_api_quickstart.html
- /dev/getting-started/project-setup/scala_api_quickstart.html
- /getting-started/project-setup/scala_api_quickstart.html
- /quickstart/scala_api_quickstart.html
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

<a name="project-configuration"></a>

# 项目配置

本节将向你展示如何通过流行的构建工具 ([Maven]({{< ref "docs/dev/configuration/maven" >}})、[Gradle]({{< ref "docs/dev/configuration/gradle" >}})) 配置你的项目，必要的依赖项（比如[连接器和格式]({{< ref "docs/dev/configuration/connector" >}})），以及覆盖一些[高级]({{< ref "docs/dev/configuration/advanced" >}})配置主题。

每个 Flink 应用程序都依赖于一组 Flink 库。应用程序至少依赖于 Flink API，此外还依赖于某些连接器库（比如 Kafka、Cassandra），以及用户开发的自定义的数据处理逻辑所需要的第三方依赖项。

<a name="getting-started"></a>

## 开始

要开始使用 Flink 应用程序，请使用以下命令、脚本和模板来创建 Flink 项目。

{{< tabs "creating project" >}}
{{< tab "Maven" >}}

你可以使用如下的 Maven 命令或快速启动脚本，基于[原型](https://maven.apache.org/guides/introduction/introduction-to-archetypes.html)创建一个项目。

<a name="maven-command"></a>

{{< hint warning >}}
All Flink Scala APIs are deprecated and will be removed in a future Flink version. You can still build your application in Scala, but you should move to the Java version of either the DataStream and/or Table API.

See <a href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-265+Deprecate+and+remove+Scala+API+support">FLIP-265 Deprecate and remove Scala API support</a>
{{< /hint >}}

### Maven 命令
```bash
$ mvn archetype:generate                \
  -DarchetypeGroupId=org.apache.flink   \
  -DarchetypeArtifactId=flink-quickstart-java \
  -DarchetypeVersion={{< version >}}
```
这允许你命名新建的项目，而且会交互式地询问 groupId、artifactId、package 的名字。

<a name="quickstart-script"></a>

### 快速启动脚本
```bash
$ curl https://flink.apache.org/q/quickstart.sh | bash -s {{< version >}}
```

{{< /tab >}}
{{< tab "Gradle" >}}
你可以创建一个空项目，你需要在其中手动创建 `src/main/java` 和 `src/main/resources` 目录并开始在其中编写一些类，使用如下 Gradle 构建脚本或下面提供的快速启动脚本以获得功能齐全的启动项目。

<a name="gradle-build-script"></a>

### Gradle 构建脚本

请在脚本的所在目录执行 `gradle` 命令来执行这些构建配置脚本。

**build.gradle**

```gradle
plugins {
    id 'java'
    id 'application'
    // shadow plugin to produce fat JARs
    id 'com.github.johnrengelman.shadow' version '7.1.2'
}
// artifact properties
group = 'org.quickstart'
version = '0.1-SNAPSHOT'
mainClassName = 'org.quickstart.DataStreamJob'
description = """Flink Quickstart Job"""
ext {
    javaVersion = '1.8'
    flinkVersion = '{{< version >}}'
    scalaBinaryVersion = '{{< scala_version >}}'
    slf4jVersion = '1.7.36'
    log4jVersion = '2.17.1'
}
sourceCompatibility = javaVersion
targetCompatibility = javaVersion
tasks.withType(JavaCompile) {
    options.encoding = 'UTF-8'
}
applicationDefaultJvmArgs = ["-Dlog4j.configurationFile=log4j2.properties"]

// declare where to find the dependencies of your project
repositories {
    mavenCentral()
    maven {
        url "https://repository.apache.org/content/repositories/snapshots"
        mavenContent {
            snapshotsOnly()
        }
    }
}
// NOTE: We cannot use "compileOnly" or "shadow" configurations since then we could not run code
// in the IDE or with "gradle run". We also cannot exclude transitive dependencies from the
// shadowJar yet (see https://github.com/johnrengelman/shadow/issues/159).
// -> Explicitly define the // libraries we want to be included in the "flinkShadowJar" configuration!
configurations {
    flinkShadowJar // dependencies which go into the shadowJar
    // always exclude these (also from transitive dependencies) since they are provided by Flink
    flinkShadowJar.exclude group: 'org.apache.flink', module: 'force-shading'
    flinkShadowJar.exclude group: 'com.google.code.findbugs', module: 'jsr305'
    flinkShadowJar.exclude group: 'org.slf4j'
    flinkShadowJar.exclude group: 'org.apache.logging.log4j'
}
// declare the dependencies for your production and test code
dependencies {
    // --------------------------------------------------------------
    // Compile-time dependencies that should NOT be part of the
    // shadow (uber) jar and are provided in the lib folder of Flink
    // --------------------------------------------------------------
    implementation "org.apache.flink:flink-streaming-java:${flinkVersion}"
    implementation "org.apache.flink:flink-clients:${flinkVersion}"
    // --------------------------------------------------------------
    // Dependencies that should be part of the shadow jar, e.g.
    // connectors. These must be in the flinkShadowJar configuration!
    // --------------------------------------------------------------
    //flinkShadowJar "org.apache.flink:flink-connector-kafka:${flinkVersion}"
    runtimeOnly "org.apache.logging.log4j:log4j-slf4j-impl:${log4jVersion}"
    runtimeOnly "org.apache.logging.log4j:log4j-api:${log4jVersion}"
    runtimeOnly "org.apache.logging.log4j:log4j-core:${log4jVersion}"
    // Add test dependencies here.
    // testCompile "junit:junit:4.12"
}
// make compileOnly dependencies available for tests:
sourceSets {
    main.compileClasspath += configurations.flinkShadowJar
    main.runtimeClasspath += configurations.flinkShadowJar
    test.compileClasspath += configurations.flinkShadowJar
    test.runtimeClasspath += configurations.flinkShadowJar
    javadoc.classpath += configurations.flinkShadowJar
}
run.classpath = sourceSets.main.runtimeClasspath

jar {
    manifest {
        attributes 'Built-By': System.getProperty('user.name'),
                'Build-Jdk': System.getProperty('java.version')
    }
}

shadowJar {
    configurations = [project.configurations.flinkShadowJar]
}
```

**settings.gradle**

```gradle
rootProject.name = 'quickstart'
```

<a name="quickstart-script"></a>

### 快速启动脚本

```bash
bash -c "$(curl https://flink.apache.org/q/gradle-quickstart.sh)" -- {{< version >}} {{< scala_version >}}
```
{{< /tab >}}
{{< /tabs >}}

<a name="which-dependencies-do-you-need"></a>

## 需要哪些依赖项？

要开始一个 Flink 作业，你通常需要如下依赖项：

* Flink API，用来开发你的作业
* [连接器和格式]({{< ref "docs/dev/configuration/connector" >}})，以将你的作业与外部系统集成
* [测试实用程序]({{< ref "docs/dev/configuration/testing" >}})，以测试你的作业

除此之外，若要开发自定义功能，你还要添加必要的第三方依赖项。

<a name="flink-apis"></a>

### Flink API

Flink提供了两大 API：[Datastream API]({{< ref "docs/dev/datastream/overview" >}}) 和 [Table API & SQL]({{< ref "docs/dev/table/overview" >}})，它们可以单独使用，也可以混合使用，具体取决于你的使用场景：

| 你要使用的 API                                                                      | 你需要添加的依赖项                                     |
|-----------------------------------------------------------------------------------|-----------------------------------------------------|
| [DataStream]({{< ref "docs/dev/datastream/overview" >}})                          | `flink-streaming-java`                              |  
| [DataStream Scala 版]({{< ref "docs/dev/datastream/scala_api_extensions" >}})     | `flink-streaming-scala{{< scala_version >}}`        |   
| [Table API]({{< ref "docs/dev/table/common" >}})                                  | `flink-table-api-java`                              |   
| [Table API Scala 版]({{< ref "docs/dev/table/common" >}})                         | `flink-table-api-scala{{< scala_version >}}`        |
| [Table API + DataStream]({{< ref "docs/dev/table/data_stream_api" >}})            | `flink-table-api-java-bridge`                       |
| [Table API + DataStream Scala 版]({{< ref "docs/dev/table/data_stream_api" >}})   | `flink-table-api-scala-bridge{{< scala_version >}}` |

你只需将它们包含在你的构建工具脚本/描述符中，就可以开发你的作业了！

<a name="running-and-packaging"></a>

## 运行和打包

如果你想通过简单地执行主类来运行你的作业，你需要 classpath 里包含 `flink-clients`。对于 Table API 程序，你还需要在 classpath 中包含 `flink-table-runtime` 和 `flink-table-planner-loader`。

根据经验，我们**建议**将应用程序代码及其所有必需的依赖项打包进一个 fat/uber JAR 中。这包括打包你作业用到的连接器、格式和第三方依赖项。此规则**不适用于** Java API、DataStream Scala API 以及前面提到的运行时模块，它们已经由 Flink 本身提供，**不应**包含在作业的 uber JAR 中。你可以把该作业 JAR 提交到已经运行的 Flink 集群，也可以轻松将其添加到 Flink 应用程序容器镜像中，而无需修改发行版。

<a name="whats-next"></a>

## 下一步是什么？

* 要开发你的作业，请查阅 [DataStream API]({{< ref "docs/dev/datastream/overview" >}}) 和 [Table API & SQL]({{< ref "docs/dev/table/overview" >}})；
* 关于如何使用特定的构建工具打包你的作业的更多细节，请查阅如下指南：
  * [Maven]({{< ref "docs/dev/configuration/maven" >}})
  * [Gradle]({{< ref "docs/dev/configuration/gradle" >}})
* 关于项目配置的高级内容，请查阅[高级主题]({{< ref "docs/dev/configuration/advanced" >}})部分。
