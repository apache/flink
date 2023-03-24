---
title: 概览
weight: 1
type: docs
aliases:
- /dev/table/sql-gateway.html
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

介绍
----------------

SQL Gateway 服务支持并发执行从多个client提交的 SQL。它提供了一种简单的方法来提交 Flink 作业、查找元数据和在线分析数据。

SQL Gateway 由插件化的 endpoint 和 `SqlGatewayService` 组成。多个 endpoint 可以复用 `SqlGatewayService` 处理请求。endpoint 是用户连接的入口。
用户可以使用不同的工具连接不同类型的 endpoint。

{{< img width="80%" src="/fig/sql-gateway-architecture.png" alt="SQL Gateway Architecture" >}}

开始
---------------

这个章节描述如何通过命令行启动和执行你的第一个 Flink SQL 作业。
SQL Gateway 和 Flink 版本一起发布，开箱即用。它只需要一个正在运行的 Flink 集群，可以执行 Flink SQL 作业。
更多启动 Flink 集群的信息可以查看 [Cluster & Deployment]({{< ref "docs/deployment/resource-providers/standalone/overview" >}})。
如果你只是想简单尝试 SQL Client，你也可以使用以下命令启动只有一个 worker 的本地集群。

```bash
$ ./bin/start-cluster.sh
```
### Starting the SQL Gateway

SQL Gateway 脚本也在 Flink 二进制包的目录中。用户通过以下命令启动：

```bash
$ ./bin/sql-gateway.sh start -Dsql-gateway.endpoint.rest.address=localhost
```

这个命令启动 SQL Gateway 和 REST Endpoint，监听 localhost:8083 地址。你可以使用 curl 命令检查 REST Endpoint 是否存活。

```bash
$ curl http://localhost:8083/v1/info
{"productName":"Apache Flink","version":"{{< version >}}"}
```

### 执行 SQL 查询

你可以通过以下步骤来验证集群配置和连接。

**Step 1: Open a session**

```bash
$ curl --request POST http://localhost:8083/v1/sessions
{"sessionHandle":"..."}
```

SQL Gateway 返回结果中的 `sessionHandle` 用来唯一标识每个活跃用户。

**Step 2: Execute a query**

```bash
$ curl --request POST http://localhost:8083/v1/sessions/${sessionHandle}/statements/ --data '{"statement": "SELECT 1"}'
{"operationHandle":"..."}
```

SQL Gateway 返回结果中的 `operationHandle` 用来唯一标识提交的 SQL。


**Step 3: Fetch results**

通过上述 `sessionHandle` 和 `operationHandle`，你能获取相应的结果。

```bash
$ curl --request GET http://localhost:8083/v1/sessions/${sessionHandle}/operations/${operationHandle}/result/0
{
  "results": {
    "columns": [
      {
        "name": "EXPR$0",
        "logicalType": {
          "type": "INTEGER",
          "nullable": false
        }
      }
    ],
    "data": [
      {
        "kind": "INSERT",
        "fields": [
          1
        ]
      }
    ]
  },
  "resultType": "PAYLOAD",
  "nextResultUri": "..."
}
```

结果中的 `nextResultUri` 不是null时，用于获取下一批结果。

```bash
$ curl --request GET ${nextResultUri}
```

配置
----------------

### SQL Gateway 启动参数

目前 SQL Gateway 有以下可选命令，它们将在下文详细讨论。

```bash
$ ./bin/sql-gateway.sh --help

Usage: sql-gateway.sh [start|start-foreground|stop|stop-all] [args]
  commands:
    start               - Run a SQL Gateway as a daemon
    start-foreground    - Run a SQL Gateway as a console application
    stop                - Stop the SQL Gateway daemon
    stop-all            - Stop all the SQL Gateway daemons
    -h | --help         - Show this help message
```

"start" 或者 "start-foreground" 命令可以使你在 CLI 中配置 SQL Gateway。

```bash
$ ./bin/sql-gateway.sh start --help

Start the Flink SQL Gateway as a daemon to submit Flink SQL.

  Syntax: start [OPTIONS]
     -D <property=value>   Use value for given property
     -h,--help             Show the help message with descriptions of all
                           options.
```

### SQL Gateway 配置

你可以通过以下方式在启动时配置 SQL Gateway，或者任意合法的 [Flink configuration]({{< ref "docs/dev/table/config" >}}) 配置：

```bash
$ ./sql-gateway -Dkey=value
```

<table class="configuration table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 20%">Key</th>
            <th class="text-left" style="width: 15%">Default</th>
            <th class="text-left" style="width: 10%">Type</th>
            <th class="text-left" style="width: 55%">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><h5>sql-gateway.session.check-interval</h5></td>
            <td style="word-wrap: break-word;">1 min</td>
            <td>Duration</td>
            <td>定时检查空闲 session 是否超时的间隔时间，设置为 0 时关闭检查。</td>
        </tr>
        <tr>
            <td><h5>sql-gateway.session.idle-timeout</h5></td>
            <td style="word-wrap: break-word;">10 min</td>
            <td>Duration</td>
            <td>session 超时时间，在这个时间区间内没有被访问过的 session 会被关闭。如果设置为 0，session 将不会被关闭。</td>
        </tr>
        <tr>
            <td><h5>sql-gateway.session.max-num</h5></td>
            <td style="word-wrap: break-word;">1000000</td>
            <td>Integer</td>
            <td>SQL Gateway 服务中存活 session 的最大数量。</td>
        </tr>
        <tr>
            <td><h5>sql-gateway.worker.keepalive-time</h5></td>
            <td style="word-wrap: break-word;">5 min</td>
            <td>Duration</td>
            <td>空闲工作线程的存活时间。当工作线程数量超过了配置的最小值，超过存活时间的多余空闲工作线程会被杀掉。</td>
        </tr>
        <tr>
            <td><h5>sql-gateway.worker.threads.max</h5></td>
            <td style="word-wrap: break-word;">500</td>
            <td>Integer</td>
            <td>SQL Gateway 服务中工作线程的最大数量。</td>
        </tr>
        <tr>
            <td><h5>sql-gateway.worker.threads.min</h5></td>
            <td style="word-wrap: break-word;">5</td>
            <td>Integer</td>
            <td>SQL Gateway 服务中工作线程的最小数量。</td>
        </tr>
    </tbody>
</table>

已支持的 Endpoints
----------------

Flink 原生支持 [REST Endpoint]({{< ref "docs/dev/table/sql-gateway/rest" >}}) 和 [HiveServer2 Endpoint]({{< ref "docs/dev/table/hive-compatibility/hiveserver2" >}})。
SQL Gateway 默认集成 REST Endpoint。由于架构的可扩展性，用户可以通过指定 endpoint 来启动 SQL Gateway。

```bash
$ ./bin/sql-gateway.sh start -Dsql-gateway.endpoint.type=hiveserver2
```

或者在 `conf/flink-conf.yaml` 中增加如下配置：

```yaml
sql-gateway.endpoint.type: hiveserver2
```

{{< hint info >}}
Notice: 如果 CLI 命令和 flink-conf.yaml 都有 `sql-gateway.endpoint.type`，CLI 的优先级比 flink-conf.yaml 更高。
{{< /hint >}}

具体的 endpoint 请参考相应页面。

{{< top >}}
