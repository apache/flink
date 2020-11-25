---
title: "程序打包和分布式运行"
nav-title: 程序打包
nav-parent_id: execution
nav-pos: 20
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


正如之前所描述的，Flink 程序可以使用 `remote environment` 在集群上执行。或者，程序可以被打包成 JAR 文件（Java Archives）执行。如果使用[命令行]({% link deployment/cli.zh.md %})的方式执行程序，将程序打包是必需的。

<a name="packaging-programs"></a>

### 打包程序

为了能够通过命令行或 web 界面执行打包的 JAR 文件，程序必须使用通过 `StreamExecutionEnvironment.getExecutionEnvironment()` 获取的 environment。当 JAR 被提交到命令行或 web 界面后，该 environment 会扮演集群环境的角色。如果调用 Flink 程序的方式与上述接口不同，该 environment 会扮演本地环境的角色。

打包程序只要简单地将所有相关的类导出为 JAR 文件，JAR 文件的 manifest 必须指向包含程序*入口点*（拥有公共 `main` 方法）的类。实现的最简单方法是将 *main-class* 写入 manifest 中（比如 `main-class: org.apache.flinkexample.MyProgram`）。*main-class* 属性与 Java 虚拟机通过指令 `java -jar pathToTheJarFile` 执行 JAR 文件时寻找 main 方法的类是相同的。大多数 IDE 提供了在导出 JAR 文件时自动包含该属性的功能。

<a name="summary"></a>

### 总结

调用打包后程序的完整流程包括两步：

1. 搜索 JAR 文件 manifest 中的 *main-class* 或 *program-class* 属性。如果两个属性同时存在，*program-class* 属性会优先于 *main-class* 属性。对于 JAR manifest 中两个属性都不存在的情况，命令行和 web 界面支持手动传入入口点类名参数。

2. 系统接着调用该类的 main 方法。

{% top %}
