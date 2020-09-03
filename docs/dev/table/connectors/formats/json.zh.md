---
title: "JSON Format"
nav-title: JSON
nav-parent_id: sql-formats
nav-pos: 2
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

<span class="label label-info">Format: Serialization Schema</span>
<span class="label label-info">Format: Deserialization Schema</span>

* This will be replaced by the TOC
{:toc}

[JSON](https://www.json.org/json-en.html) Format 能读写 JSON 格式的数据。当前，JSON schema 是从 table schema 中自动推导而得的。

依赖
------------

为了使用 JSON format，下表列出了利用自动化构建工具（例如，Maven 或者 SBT ）构建项目以及 SQL Client 所需要的依赖。

| Maven 依赖          | SQL Client JAR         |
| :----------------- | :----------------------|
| `flink-json`       | 内置                    |

如何创建一张基于 JSON Format 的表
----------------

以下是一个利用 Kafka 以及 JSON Format 构建表的例子。

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE user_behavior (
  user_id BIGINT,
  item_id BIGINT,
  category_id BIGINT,
  behavior STRING,
  ts TIMESTAMP(3)
) WITH (
 'connector' = 'kafka',
 'topic' = 'user_behavior',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'testGroup',
 'format' = 'json',
 'json.fail-on-missing-field' = 'false',
 'json.ignore-parse-errors' = 'true'
)
{% endhighlight %}
</div>
</div>

Format 参数
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">参数</th>
        <th class="text-center" style="width: 10%">是否必须</th>
        <th class="text-center" style="width: 10%">默认值</th>
        <th class="text-center" style="width: 10%">类型</th>
        <th class="text-center" style="width: 45%">描述</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>format</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>声明使用的格式，这里应为<code>'json'</code>。</td>
    </tr>
    <tr>
      <td><h5>json.fail-on-missing-field</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>当解析字段缺失时，是跳过当前字段或行，还是抛出错误失败（默认为 false，即抛出错误失败）。</td>
    </tr>
    <tr>
      <td><h5>json.ignore-parse-errors</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>当解析异常时，是跳过当前字段或行，还是抛出错误失败（默认为 false，即抛出错误失败）。如果忽略字段的解析异常，则会将该字段值设置为<code>null</code>。</td>
    </tr>
    <tr>
      <td><h5>json.timestamp-format.standard</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;"><code>'SQL'</code></td>
      <td>String</td>
      <td>声明输入和输出的 <code>TIMESTAMP</code> 和 <code>TIMESTAMP WITH LOCAL TIME ZONE</code> 的格式。当前支持的格式为<code>'SQL'</code> 以及 <code>'ISO-8601'</code>：
      <ul>
        <li>可选参数 <code>'SQL'</code> 将会以 "yyyy-MM-dd HH:mm:ss.s{precision}" 的格式解析 TIMESTAMP, 例如 "2020-12-30 12:13:14.123"，
        以 "yyyy-MM-dd HH:mm:ss.s{precision}'Z'" 的格式解析 TIMESTAMP WITH LOCAL TIME ZONE, 例如 "2020-12-30 12:13:14.123Z" 且会以相同的格式输出。</li>
        <li>可选参数 <code>'ISO-8601'</code> 将会以 "yyyy-MM-ddTHH:mm:ss.s{precision}" 的格式解析输入 TIMESTAMP, 例如 "2020-12-30T12:13:14.123" ，
        以 "yyyy-MM-ddTHH:mm:ss.s{precision}'Z'" 的格式解析 TIMESTAMP WITH LOCAL TIME ZONE, 例如 "2020-12-30T12:13:14.123Z" 且会以相同的格式输出。</li>
      </ul>
      </td>
    </tr>
    </tbody>
</table>

数据类型映射关系
----------------

当前，JSON schema 将会自动从 table schema 之中自动推导得到。不支持显式地定义 JSON schema。

在 Flink 中，JSON Format 使用 [jackson databind API](https://github.com/FasterXML/jackson-databind) 去解析和生成 JSON。

下表列出了 Flink 中的数据类型与 JSON 中的数据类型的映射关系。

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink SQL 类型</th>
        <th class="text-left">JSON 类型</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>CHAR / VARCHAR / STRING</code></td>
      <td><code>string</code></td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>boolean</code></td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY</code></td>
      <td><code>string with encoding: base64</code></td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>string with format: date</code></td>
    </tr>
    <tr>
      <td><code>TIME</code></td>
      <td><code>string with format: time</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code></td>
      <td><code>string with format: date-time</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP_WITH_LOCAL_TIME_ZONE</code></td>
      <td><code>string with format: date-time (with UTC time zone)</code></td>
    </tr>
    <tr>
      <td><code>INTERVAL</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td><code>array</code></td>
    </tr>
    <tr>
      <td><code>MAP / MULTISET</code></td>
      <td><code>object</code></td>
    </tr>
    <tr>
      <td><code>ROW</code></td>
      <td><code>object</code></td>
    </tr>
    </tbody>
</table>





