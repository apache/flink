---
title: REST Endpoint
weight: 2
type: docs
aliases:
- /dev/table/sql-gateway/rest.html
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

# REST Endpoint

REST endpoint 允许用户通过 REST API 连接 SQL Gateway。

Overview of SQL Processing
----------------

### Open Session

当客户端连接到 SQL Gateway 时，SQL Gateway 会创建一个 `Session`，存储客户端和 SQL Gateway 交互期间的用户相关信息。
创建 `Session` 后，SQL Gateway 会返回 `SessionHandle` 标识，用于后续的交互。

### Submit SQL

注册 `Session` 后，客户端能够提交 SQL 到 SQL Gateway。提交 SQL 后，SQL 会被转换成 `Operation`，并且返回 `OperationHandle` 标识，用于用户后续获取结果。
Operation 有它的生命周期，客户端可以取消正在执行的 `Operation`，或者关闭 `Operation` 并释放它使用的资源。

### Fetch Results

客户端可以通过 `OperationHandle` 从 `Operation` 获取结果。当一个 `Operation` 已经就绪，SQL Gateway 将返回一个包含对应 schema 和 URI 的批式数据，
URI 可以被用来获取下一个批式数据。当所有结果已经获取完成，SQL Gateway 会将结果中的 `resultType` 设置为 `EOS`，并且将获取下一个批式数据的 URI 设置为 null。

{{< img width="100%" src="/fig/sql-gateway-interactions.png" alt="SQL Gateway Interactions" >}}

Endpoint Options
----------------

<table class="table table-bordered">
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
            <td><h5>sql-gateway.endpoint.rest.address</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>客户端通过该地址连接到 SQL Gateway 服务。</td>
        </tr>
        <tr>
            <td><h5>sql-gateway.endpoint.rest.bind-address</h5></td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>SQL Gateway 服务绑定的地址。</td>
        </tr>
        <tr>
            <td><h5>sql-gateway.endpoint.rest.bind-port</h5></td>
            <td style="word-wrap: break-word;">"8083"</td>
            <td>String</td>
            <td>SQL Gateway 服务绑定的端口号。接受端口列表 （“50100,50101”）或端口区间（“50100-50200”），也可以两种方式混用。为了避免同一台机器上运行多个 SQL Gateway 服务引起的端口冲突，建议设置为端口区间。</td>
        </tr>
        <tr>
            <td><h5>sql-gateway.endpoint.rest.port</h5></td>
            <td style="word-wrap: break-word;">8083</td>
            <td>Integer</td>
            <td>客户端连接的端口号。如果 bind-port 没有被指定，SQL Gateway 服务将会绑定这个端口。</td>
        </tr>
    </tbody>
</table>

REST API
----------------

OpenAPI 规范如下，默认版本是 v2。

| Version                       | Description                    |
| ----------- |--------------------------------|
| [Open API v1 specification]({{< ref_static "generated/rest_v1_sql_gateway.yml" >}}) | 允许用户提交 SQL 语句到 Gateway 并执行。 |
| [Open API v2 specification]({{< ref_static "generated/rest_v2_sql_gateway.yml" >}}) | 支持 SQL 客户端连接到 Gateway。         |

{{< hint warning >}}
OpenAPI 规范目前仍处于实验阶段。
{{< /hint >}}

#### API reference

{{< tabs "f00ed142-b05f-44f0-bafc-799080c1d40d" >}}
{{< tab "v2" >}}

{{< generated/rest_v2_sql_gateway >}}

{{< /tab >}}
{{< tab "v1" >}}

{{< generated/rest_v1_sql_gateway >}}

{{< /tab >}}
{{< /tabs >}}

Data Type Mapping
----------------

目前 REST endpoint 支持使用查询参数 `rowFormat` 序列化 `RowData`。REST endpoint 使用 JSON 序列化 Table 对象。
请参考 [JSON format]({{< ref "docs/connectors/table/formats/json#data-type-mapping" >}}) 查看类型映射关系。

REST endpoint 也支持 `PLAIN_TEXT` 序列化 `RowData`，将所有列自动转换成 `String`。

{{< top >}}
