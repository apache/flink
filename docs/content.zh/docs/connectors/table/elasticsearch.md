---
title: Elasticsearch
weight: 7
type: docs
aliases:
  - /zh/dev/table/connectors/elasticsearch.html
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

# Elasticsearch SQL 连接器

{{< label "Sink: Batch" >}}
{{< label "Sink: Streaming Append & Upsert Mode" >}}

Elasticsearch 连接器允许将数据写入到 Elasticsearch 引擎的索引中。本文档描述运行 SQL 查询时如何设置 Elasticsearch 连接器。

连接器可以工作在 upsert 模式，使用 DDL 中定义的主键与外部系统交换 UPDATE/DELETE 消息。

如果 DDL 中没有定义主键，那么连接器只能工作在 append 模式，只能与外部系统交换 INSERT 消息。

依赖
------------

{{< sql_download_table "elastic" >}}

如何创建 Elasticsearch 表
----------------

以下示例展示了如何创建 Elasticsearch sink 表：

```sql
CREATE TABLE myUserTable (
  user_id STRING,
  user_name STRING
  uv BIGINT,
  pv BIGINT,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector' = 'elasticsearch-7',
  'hosts' = 'http://localhost:9200',
  'index' = 'users'
);
```

连接器参数
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">参数</th>
        <th class="text-center" style="width: 8%">是否必选</th>
        <th class="text-center" style="width: 7%">默认值</th>
        <th class="text-center" style="width: 10%">数据类型</th>
        <th class="text-center" style="width: 50%">描述</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定要使用的连接器，有效值为：
      <ul>
      <li><code>elasticsearch-6</code>：连接到 Elasticsearch 6.x 的集群。</li>
      <li><code>elasticsearch-7</code>：连接到 Elasticsearch 7.x 及更高版本的集群。</li>
      </ul></td>
    </tr>
    <tr>
      <td><h5>hosts</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>要连接到的一台或多台 Elasticsearch 主机，例如 <code>'http://host_name:9092;http://host_name:9093'</code>。</td>
    </tr>
    <tr>
      <td><h5>index</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Elasticsearch 中每条记录的索引。可以是一个静态索引（例如 <code>'myIndex'</code>）或一个动态索引（例如 <code>'index-{log_ts|yyyy-MM-dd}'</code>）。
       更多详细信息，请参见下面的<a href="#动态索引">动态索引</a>部分。</td>
    </tr>
    <tr>
      <td><h5>document-type</h5></td>
      <td>6.x 版本中必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Elasticsearch 文档类型。在 <code>elasticsearch-7</code> 中不再需要。</td>
    </tr>
    <tr>
      <td><h5>document-id.key-delimiter</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">_</td>
      <td>String</td>
      <td>复合键的分隔符（默认为"_"），例如，指定为"$"将导致文档 ID 为"KEY1$KEY2$KEY3"。</td>
    </tr>
    <tr>
      <td><h5>username</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于连接 Elasticsearch 实例的用户名。请注意，Elasticsearch 没有预绑定安全特性，但你可以通过如下<a href="https://www.elastic.co/guide/en/elasticsearch/reference/master/configuring-security.html">指南</a>启用它来保护 Elasticsearch 集群。</td>
    </tr>
    <tr>
      <td><h5>password</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于连接 Elasticsearch 实例的密码。如果配置了<code>username</code>，则此选项也必须配置为非空字符串。</td>
    </tr>
    <tr>
      <td><h5>failure-handler</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">fail</td>
      <td>String</td>
      <td>对 Elasticsearch 请求失败情况下的失败处理策略。有效策略为：
      <ul>
        <li><code>fail</code>：如果请求失败并因此导致作业失败，则抛出异常。</li>
        <li><code>ignore</code>：忽略失败并放弃请求。</li>
        <li><code>retry-rejected</code>：重新添加由于队列容量饱和而失败的请求。</li>
        <li>自定义类名称：使用 ActionRequestFailureHandler 的子类进行失败处理。</li>
      </ul>
      </td>
    </tr>
    <tr>
      <td><h5>sink.flush-on-checkpoint</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>是否在 checkpoint 时执行 flush。禁用后，在 checkpoint 时 sink 将不会等待所有的 pending 请求被 Elasticsearch 确认。因此，sink 不会为请求的 at-least-once 交付提供任何有力保证。
      </td>
    </tr>
    <tr>
      <td><h5>sink.bulk-flush.max-actions</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">1000</td>
      <td>Integer</td>
      <td>每个批量请求的最大缓冲操作数。
      可以设置为<code>'0'</code>来禁用它。
      </td>
    </tr>
    <tr>
      <td><h5>sink.bulk-flush.max-size</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">2mb</td>
      <td>MemorySize</td>
      <td>每个批量请求的缓冲操作在内存中的最大值。单位必须为 MB。
      可以设置为<code>'0'</code>来禁用它。
      </td>
    </tr>
    <tr>
      <td><h5>sink.bulk-flush.interval</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">1s</td>
      <td>Duration</td>
      <td>flush 缓冲操作的间隔。
        可以设置为<code>'0'</code>来禁用它。注意，<code>'sink.bulk-flush.max-size'</code>和<code>'sink.bulk-flush.max-actions'</code>都设置为<code>'0'</code>的这种 flush 间隔设置允许对缓冲操作进行完全异步处理。
      </td>
    </tr>
    <tr>
      <td><h5>sink.bulk-flush.backoff.strategy</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">DISABLED</td>
      <td>String</td>
      <td>指定在由于临时请求错误导致任何 flush 操作失败时如何执行重试。有效策略为：
      <ul>
        <li><code>DISABLED</code>：不执行重试，即第一次请求错误后失败。</li>
        <li><code>CONSTANT</code>：等待重试之间的回退延迟。</li>
        <li><code>EXPONENTIAL</code>：先等待回退延迟，然后在重试之间指数递增。</li>
      </ul>
      </td>
    </tr>
    <tr>
      <td><h5>sink.bulk-flush.backoff.max-retries</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">8</td>
      <td>Integer</td>
      <td>最大回退重试次数。</td>
    </tr>
    <tr>
      <td><h5>sink.bulk-flush.backoff.delay</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">50ms</td>
      <td>Duration</td>
      <td>每次回退尝试之间的延迟。对于 <code>CONSTANT</code> 回退策略，该值是每次重试之间的延迟。对于 <code>EXPONENTIAL</code> 回退策略，该值是初始的延迟。</td>
    </tr>
    <tr>
      <td><h5>connection.max-retry-timeout</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Duration</td>
      <td>最大重试超时时间。</td>
    </tr>
    <tr>
      <td><h5>connection.path-prefix</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>添加到每个 REST 通信中的前缀字符串，例如，<code>'/v1'</code>。</td>
    </tr>
    <tr>
      <td><h5>format</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">json</td>
      <td>String</td>
      <td>Elasticsearch 连接器支持指定格式。该格式必须生成一个有效的 json 文档。
       默认使用内置的 <code>'json'</code> 格式。更多详细信息，请参阅 <a href="{{< ref "docs/connectors/table/formats/overview" >}}">JSON Format</a> 页面。
      </td>
    </tr>
    </tbody>
</table>

特性
----------------

### Key 处理

Elasticsearch sink 可以根据是否定义了主键来确定是在 upsert 模式还是 append 模式下工作。
如果定义了主键，Elasticsearch sink 将以 upsert 模式工作，该模式可以消费包含 UPDATE/DELETE 消息的查询。
如果未定义主键，Elasticsearch sink 将以 append 模式工作，该模式只能消费包含 INSERT 消息的查询。

在 Elasticsearch 连接器中，主键用于计算 Elasticsearch 的文档 id，文档 id 为最多 512 字节且不包含空格的字符串。
Elasticsearch 连接器通过使用 `document-id.key-delimiter` 指定的键分隔符按照 DDL 中定义的顺序连接所有主键字段，为每一行记录生成一个文档 ID 字符串。
某些类型不允许作为主键字段，因为它们没有对应的字符串表示形式，例如，`BYTES`，`ROW`，`ARRAY`，`MAP` 等。
如果未指定主键，Elasticsearch 将自动生成文档 id。

有关 PRIMARY KEY 语法的更多详细信息，请参见 [CREATE TABLE DDL]({{< ref "docs/dev/table/sql/create" >}}#create-table)。

### 动态索引

Elasticsearch sink 同时支持静态索引和动态索引。

如果你想使用静态索引，则 `index` 选项值应为纯字符串，例如 `'myusers'`，所有记录都将被写入到 "myusers" 索引中。

如果你想使用动态索引，你可以使用 `{field_name}` 来引用记录中的字段值来动态生成目标索引。
你也可以使用 `'{field_name|date_format_string}'` 将 `TIMESTAMP/DATE/TIME` 类型的字段值转换为 `date_format_string` 指定的格式。
`date_format_string` 与 Java 的 [DateTimeFormatter](https://docs.oracle.com/javase/8/docs/api/index.html) 兼容。
例如，如果选项值设置为 `'myusers-{log_ts|yyyy-MM-dd}'`，则 `log_ts` 字段值为 `2020-03-27 12:25:55` 的记录将被写入到 "myusers-2020-03-27" 索引中。


数据类型映射
----------------

Elasticsearch 将文档存储在 JSON 字符串中。因此数据类型映射介于 Flink 数据类型和 JSON 数据类型之间。
Flink 为 Elasticsearch 连接器使用内置的 `'json'` 格式。更多类型映射的详细信息，请参阅 [JSON Format]({{< ref "docs/connectors/table/formats/json" >}}) 页面。

{{< top >}}
