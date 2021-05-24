---
title: '本地模式安装'
weight: 1
type: docs
aliases:
  - /zh/try-flink/local_installation.html
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

# 本地模式安装
 
{{< unstable >}}
{{< hint info >}}
  注意：Apache Flink 社区只发布 Apache Flink 的 release 版本。


  由于你当前正在查看的是文档最新的 SNAPSHOT 版本，因此相关内容会被隐藏。请通过左侧菜单底部的版本选择将文档切换到最新的 release 版本。

{{< /hint >}}
{{< /unstable >}}

请按照以下几个步骤下载最新的稳定版本开始使用。

<a name="step-1-download"></a>

## 步骤 1：下载

为了运行Flink，只需提前安装好 __Java 8 或者 Java 11__。你可以通过以下命令来检查 Java 是否已经安装正确。

```bash
java -version
```

[下载](https://flink.apache.org/downloads.html) release {{ site.version }} 并解压。

```bash
$ tar -xzf flink-{{ site.version }}-bin-scala{{< scala_version >}}.tgz
$ cd flink-{{ site.version }}-bin-scala{{< scala_version >}}
```

<a name="step-2-start-a-cluster"></a>

## 步骤 2：启动集群

Flink 附带了一个 bash 脚本，可以用于启动本地集群。

```bash
$ ./bin/start-cluster.sh
Starting cluster.
Starting standalonesession daemon on host.
Starting taskexecutor daemon on host.
```

<a name="step-3-submit-a-job"></a>

## 步骤 3：提交作业（Job）

Flink 的 Releases 附带了许多的示例作业。你可以任意选择一个，快速部署到已运行的集群上。

```bash
$ ./bin/flink run examples/streaming/WordCount.jar
$ tail log/flink-*-taskexecutor-*.out
  (to,1)
  (be,1)
  (or,1)
  (not,1)
  (to,2)
  (be,2)
```

另外，你可以通过 Flink 的 [Web UI](http://localhost:8081) 来监视集群的状态和正在运行的作业。

<a name="step-4-stop-the-cluster"></a>

## 步骤 4：停止集群

完成后，你可以快速停止集群和所有正在运行的组件。

```bash
$ ./bin/stop-cluster.sh
```
