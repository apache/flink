---
title:  "集群执行"
weight: 13
type: docs
aliases:
  - /zh/dev/cluster_execution.html
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

{{< hint warning >}}
Starting with Flink 1.12 the DataSet API has been soft deprecated.

We recommend that you use the [Table API and SQL]({{< ref "docs/dev/table/overview" >}}) to run efficient
batch pipelines in a fully unified API. Table API is well integrated with common batch connectors and
catalogs.

Alternatively, you can also use the DataStream API with `BATCH` [execution mode]({{< ref "docs/dev/datastream/execution_mode" >}}).
The linked section also outlines cases where it makes sense to use the DataSet API but those cases will
become rarer as development progresses and the DataSet API will eventually be removed. Please also
see [FLIP-131](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741) for
background information on this decision.
{{< /hint >}}

# 集群执行



Flink 程序可以分布式运行在多机器集群上。有两种方式可以将程序提交到集群上执行：

<a name="command-line-interface"></a>

## 命令行界面（Interface）

命令行界面使你可以将打包的程序（JARs）提交到集群（或单机设置）。

有关详细信息，请参阅[命令行界面]({{< ref "docs/deployment/cli" >}})文档。

<a name="remote-environment"></a>

## 远程环境（Remote Environment）

远程环境使你可以直接在集群上执行 Flink Java 程序。远程环境指向你要执行程序的集群。

<a name="maven-dependency"></a>

### Maven Dependency

如果将程序作为 Maven 项目开发，则必须添加 `flink-clients` 模块的依赖：

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients</artifactId>
  <version>{{< version >}}</version>
</dependency>
```

<a name="example"></a>

### 示例

下面演示了 `RemoteEnvironment` 的用法：

```java
public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment
        .createRemoteEnvironment("flink-jobmanager", 8081, "/home/user/udfs.jar");

    DataSet<String> data = env.readTextFile("hdfs://path/to/file");

    data
        .filter(new FilterFunction<String>() {
            public boolean filter(String value) {
                return value.startsWith("http://");
            }
        })
        .writeAsText("hdfs://path/to/result");

    env.execute();
}
```

请注意，该程序包含用户自定义代码，因此需要一个带有附加代码类的 JAR 文件。远程环境的构造函数使用 JAR 文件的路径进行构造。

{{< top >}}
