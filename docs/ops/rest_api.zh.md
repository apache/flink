---
title: "REST API"
nav-parent_id: ops
nav-pos: 6
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

Flink 具有监控 API ，可用于查询正在运行的作业以及最近完成的作业的状态和统计信息。该监控 API 被用于 Flink 自己的仪表盘，同时也可用于自定义监控工具。

该监控 API 是 REST-ful 风格的，可以接受 HTTP 请求并返回 JSON 格式的数据。

* This will be replaced by the TOC
{:toc}

<a name="overview"></a>

## 概览

该监控 API 由作为 *JobManager* 一部分运行的 web 服务器提供支持。默认情况下，该服务器监听 8081 端口，端口号可以通过修改 `flink-conf.yaml` 文件的 `rest.port` 进行配置。请注意，该监控 API 的 web 服务器和仪表盘的 web 服务器目前是相同的，因此在同一端口一起运行。不过，它们响应不同的 HTTP URL 。

在多个 JobManager 的情况下（为了高可用），每个 JobManager 将运行自己的监控 API 实例，当 JobManager 被选举成为集群 leader 时，该实例将提供已完成和正在运行作业的相关信息。

<a name="developing"></a>

## 拓展

该 REST API 后端位于 `flink-runtime` 项目中。核心类是 `org.apache.flink.runtime.webmonitor.WebMonitorEndpoint` ，用来配置服务器和请求路由。

我们使用 *Netty* 和 *Netty Router* 库来处理 REST 请求和转换 URL 。选择该选项是因为这种组合具有轻量级依赖关系，并且 Netty HTTP 的性能非常好。

添加新的请求，需要
* 添加一个新的 `MessageHeaders` 类，作为新请求的接口，
* 添加一个新的 `AbstractRestHandler` 类，该类接收并处理 `MessageHeaders` 类的请求，
* 将处理程序添加到 `org.apache.flink.runtime.webmonitor.WebMonitorEndpoint#initializeHandlers()` 中。

一个很好的例子是使用 `org.apache.flink.runtime.rest.messages.JobExceptionsHeaders` 的 `org.apache.flink.runtime.rest.handler.job.JobExceptionsHandler` 。

<a name="api"></a>

## API

该 REST API 已版本化，可以通过在 URL 前面加上版本前缀来查询指定版本。前缀格式始终为 `v[version_number]` 。
例如，要访问版本 1 的 `/foo/bar` 接口，将查询 `/v1/foo/bar` 。

如果未指定版本， Flink 将默认使用支持该请求的最旧版本。

查询 不支持/不存在 的版本将返回 404 错误。

这些 API 中存在几种异步操作，例如：`trigger savepoint` 、 `rescale a job` 。它们将返回 `triggerid` 来标识你刚刚执行的 POST 请求，然后你需要使用该 `triggerid` 查询该操作的状态。

<div class="codetabs" markdown="1">

<div data-lang="v1" markdown="1">
#### JobManager

{% include generated/rest_v1_dispatcher.html %}
</div>

</div>

