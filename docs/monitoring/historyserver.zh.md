---
title: "History Server"
nav-parent_id: monitoring
nav-pos: 3
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

Flink 自带的 history server 可以在已执行完作业对应的 Flink 集群关闭之后查询该作业的统计信息。

除此之外，它还提供了一个 REST API，可以通过 HTTP 以 JSON 格式发送和接收数据。

* This will be replaced by the TOC
{:toc}

## 概览

History server 可以查询被 JobManager 存档的已执行完作业的状态和统计信息。

在配置完 History server 和 JobManager 之后，用户可以通过下面的启动脚本来开启和关停 History server：

{% highlight shell %}
# 开启或关停 History Server
bin/historyserver.sh (start|start-foreground|stop)
{% endhighlight %}

默认情况下，History server 绑定到本机 `localhost` 的 `8082` 端口。

目前，只能当做单独的进程来运行。

## 配置

如果要存档和展示已完成的作业 需要调配 `jobmanager.archive.fs.dir` 和 `historyserver.archive.fs.refresh-interval` 这俩配置项。

**JobManager**

JobManager 会进行已完成作业的存档，把已存档作业的信息上传到一个文件系统目录上。用户可以通过设置 `jobmanager.archive.fs.dir` 来配置这个存档目录，将 `flink-conf.yaml` 中已完成的作业都存档下来。

{% highlight yaml %}
# 已完成作业信息的上传目录
jobmanager.archive.fs.dir: hdfs:///completed-jobs
{% endhighlight %}

**HistoryServer**

History Server 可以监控 `historyserver.archive.fs.dir` 配置的用逗号分隔的文件目录列表。History Server 会定期轮询配置的目录以发现新存档，轮询的间隔可以通过 `historyserver.archive.fs.refresh-interval` 来配置。

{% highlight yaml %}
# 监控已完成作业的目录
historyserver.archive.fs.dir: hdfs:///completed-jobs

# 每10秒刷新一次
historyserver.archive.fs.refresh-interval: 10000
{% endhighlight %}

相关的存档都下载和缓存到本地文件系统里。这个本地文件目录可以通过 `historyserver.web.tmpdir` 来配置。

这里可以查看[完整的配置项列表]({{ site.baseurl }}/ops/config.html#history-server)。

## 提供的 REST API 列表

下面是 History server 提供的 REST API 列表。所有的请求前缀都是 `http://hostname:8082/jobs`，下面是相关的 URL 路径。

尖括号里的值是待替换的变量，比如 `http://hostname:port/jobs/<jobid>/exceptions` 在实际请求过程时替换为 `http://hostname:port/jobs/7684be6004e4e955c2a558a9bc463f65/exceptions`。

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

{% top %}
