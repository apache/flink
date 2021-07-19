---
title: "History Server"
weight: 3
type: docs
aliases:
  - /zh/deployment/advanced/historyserver.html
  - /zh/monitoring/historyserver.html
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

# History Server

Flink 提供了 history server，用以当对应 Flink 集群关闭之后查询已完成作业的统计信息。

并且，它暴露了一套 REST API，该 API 支持 HTTP 请求并返回 JSON 格式的数据。



<a name="overview"></a>

## 概览

HistoryServer 支持查询被 JobManager 存档的已完成作业的状态和统计信息。

在完成 HistoryServer 和 JobManager 配置之后，可以使用相应的脚本来启动或停止 HistoryServer：

```shell
# 启动或者停止 HistoryServer
bin/historyserver.sh (start|start-foreground|stop)
```

默认情况下，此服务器会绑定到 `localhost` 的 `8082` 端口。

目前，只支持将 HistoryServer 作为独立的进程运行。

<a name="configuration"></a>

## 配置参数

如果要存档和展示已完成的作业，需要配置 `jobmanager.archive.fs.dir` 和 `historyserver.archive.fs.refresh-interval` 配置项。

**JobManager**

JobManager 会对已完成的作业进行存档，并将作业的存档信息上传到文件系统的某个目录中。该目录可以在 `flink-conf.yaml` 文件中 `jobmanager.archive.fs.dir` 配置项上设置。

```yaml
# 上传已完成作业信息的目录
jobmanager.archive.fs.dir: hdfs:///completed-jobs
```

**HistoryServer**

可以通过 `historyserver.archive.fs.dir` 设置 HistoryServer 监视的目录，多个路径以逗号分隔。HistoryServer 会定期轮询配置的目录以寻找新的存档；通过 `historyserver.archive.fs.refresh-interval` 来配置轮询的时间间隔。

```yaml
# 监视以下目录中已完成的作业
historyserver.archive.fs.dir: hdfs:///completed-jobs

# 每 10 秒刷新一次
historyserver.archive.fs.refresh-interval: 10000
```

这些存档将被下载并缓存到本地文件系统中。本地目录通过 `historyserver.web.tmpdir` 配置。

请查看配置页面以获取[配置选项的完整列表]({{< ref "docs/deployment/config" >}}#history-server)。

<a name="available-requests"></a>

## 支持的请求

以下是支持的请求列表，并包含了一个 JSON 格式的返回示例。所有请求样例的格式均为 `http://hostname:8082/jobs`，以下列表仅包含了 URLs 的 *path* 部分。
尖括号中的值表示变量，例如作业 `7684be6004e4e955c2a558a9bc463f65` 的 
`http://hostname:port/jobs/<jobid>/exceptions` 请求为 `http://hostname:port/jobs/7684be6004e4e955c2a558a9bc463f65/exceptions`。

  - `/config`
  - `/jobs/overview`
  - `/jobs/<jobid>`
  - `/jobs/<jobid>/vertices`
  - `/jobs/<jobid>/config`
  - `/jobs/<jobid>/exceptions`
  - `/jobs/<jobid>/accumulators`
  - `/jobs/<jobid>/vertices/<vertexid>`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasktimes`
  - `/jobs/<jobid>/vertices/<vertexid>/taskmanagers`
  - `/jobs/<jobid>/vertices/<vertexid>/accumulators`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/accumulators`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>/attempts/<attempt>`
  - `/jobs/<jobid>/vertices/<vertexid>/subtasks/<subtasknum>/attempts/<attempt>/accumulators`
  - `/jobs/<jobid>/plan`

{{< top >}}
