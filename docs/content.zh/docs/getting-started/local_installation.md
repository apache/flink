---
title: '第一步'
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

# 第一步

欢迎使用 Apache Flink！本指南将帮助您启动并运行 Flink 集群，以便开始探索 Flink 的功能。

## 前提条件

选择以下安装方式之一：

- **Docker**：无需安装 Java，包含 SQL 客户端
- **本地安装**：需要 Java 11、17 或 21
- **PyFlink**：需要 Java 和 Python 3.9+

## 选项 A：Docker 安装

最快的 Flink 入门方式。无需安装 Java。

### 步骤 1：获取 docker-compose.yml 文件

[下载 docker-compose.yml](/downloads/docker-compose.yml) 或创建一个名为 `docker-compose.yml` 的文件，内容如下：

```yaml
services:
  jobmanager:
    image: flink:latest
    ports:
      - "8081:8081"
    command: jobmanager
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager

  taskmanager:
    image: flink:latest
    depends_on:
      - jobmanager
    command: taskmanager
    scale: 1
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        taskmanager.numberOfTaskSlots: 2

  sql-client:
    image: flink:latest
    depends_on:
      - jobmanager
    command: bin/sql-client.sh
    environment:
      - |
        FLINK_PROPERTIES=
        jobmanager.rpc.address: jobmanager
        rest.address: jobmanager
```

### 步骤 2：启动集群

```bash
$ docker compose up -d
```

### 步骤 3：验证集群正在运行

打开 Flink Web UI [http://localhost:8081](http://localhost:8081) 以验证集群正在运行。

### 使用 SQL 客户端

启动交互式 SQL 会话：

```bash
$ docker compose run sql-client
```

要退出 SQL 客户端，输入 `exit;` 并按回车键。

### 停止集群

```bash
$ docker compose down
```

有关更多 Docker 选项（扩缩容、Application 模式），请参阅 [Docker 部署指南]({{< ref "docs/deployment/resource-providers/standalone/docker" >}})。

## 选项 B：本地安装

如果您更喜欢在本机上直接运行 Flink 而不使用 Docker。

### 步骤 1：下载 Flink

Flink 可在所有类 UNIX 环境中运行，包括 Linux、Mac OS X 和 Cygwin（Windows）。

首先，验证您的 Java 版本：

```bash
$ java -version
```

您需要安装 Java 11、17 或 21。然后，[下载最新的二进制发行版]({{< downloads >}}) 并解压：

```bash
$ tar -xzf flink-*.tgz
$ cd flink-*
```

### 步骤 2：启动集群

启动本地 Flink 集群：

```bash
$ ./bin/start-cluster.sh
```

### 步骤 3：验证集群正在运行

打开 Flink Web UI [http://localhost:8081](http://localhost:8081) 以验证集群正在运行。您应该看到 Flink 仪表板显示一个 TaskManager 和可用的任务槽。

### 使用 SQL 客户端

启动交互式 SQL 会话：

```bash
$ ./bin/sql-client.sh
```

要退出 SQL 客户端，输入 `exit;` 并按回车键。

### 停止集群

完成后，使用以下命令停止集群：

```bash
$ ./bin/stop-cluster.sh
```

## 选项 C：PyFlink 安装

用于使用 Table API 或 DataStream API 进行 Python 开发。无需 Flink 集群——PyFlink 在开发过程中以本地模式运行。

### 步骤 1：验证前提条件

PyFlink 需要 Java 和 Python：

```bash
$ java -version
# Java 11、17 或 21

$ python --version
# Python 3.9、3.10、3.11 或 3.12
```

### 步骤 2：安装 PyFlink

{{< stable >}}
```bash
$ python -m pip install apache-flink=={{< version >}}
```
{{< /stable >}}
{{< unstable >}}
```bash
$ python -m pip install apache-flink
```
{{< /unstable >}}

{{< hint info >}}
**提示：** 我们建议在 [虚拟环境](https://docs.python.org/zh-cn/3/library/venv.html) 中安装 PyFlink，以保持项目依赖隔离。
{{< /hint >}}

### 步骤 3：验证安装

```bash
$ python -c "import pyflink; print(pyflink.__version__)"
```

您现在可以使用 Python 选项卡来学习 [Table API 教程]({{< ref "docs/getting-started/table_api" >}}) 或 [DataStream API 教程]({{< ref "docs/getting-started/datastream" >}})。

## 下一步

选择一个教程开始学习：

| 教程 | 描述 | 所需设置 |
|------|------|---------|
| [Flink SQL 教程]({{< ref "docs/getting-started/quickstart-sql" >}}) | 使用 SQL 交互式查询数据。无需编码。 | 选项 A 或 B（集群） |
| [Table API 教程]({{< ref "docs/getting-started/table_api" >}}) | 使用 Java 或 Python 构建流处理管道。 | Maven（Java）或选项 C（Python） |
| [DataStream API 教程]({{< ref "docs/getting-started/datastream" >}}) | 使用 Java 或 Python 构建有状态流处理应用。 | Maven（Java）或选项 C（Python） |
| [Flink 运维练习场]({{< ref "docs/getting-started/flink-operations-playground" >}}) | 学习操作 Flink：扩缩容、故障恢复和升级。 | Docker |

要了解有关 Flink 核心概念的更多信息，请访问 [概念]({{< ref "docs/concepts/overview" >}}) 部分。
