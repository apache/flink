---
title: "JSON格式"
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

[JSON](https://www.json.org/json-en.html)格式将会使用JSON数据进行读写。当前，JSON scheme是从table schema中自动推导而得的。

依赖
------------

为了使用JSON格式，下表中列出了利用自动化构建工具（例如，Maven或者SBT）构建项目以及SQL Client所需要的依赖。

| Maven 依赖          | SQL Client JAR         |
| :----------------- | :----------------------|
| `flink-json`       | 内建                    |

利用JSON格式构建表
----------------

以下是利用Kafka以及Json构建表的一个例子。

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

格式选项
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">选项名字</th>
        <th class="text-center" style="width: 10%">是否必须</th>
        <th class="text-center" style="width: 10%">默认参数</th>
        <th class="text-center" style="width: 10%">数据类型</th>
        <th class="text-center" style="width: 45%">选项描述</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>format</h5></td>
      <td>必须</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>声明使用的格式，这里应该是<code>'json'</code>。</td>
    </tr>
    <tr>
      <td><h5>json.fail-on-missing-field</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>决定是否在缺失属性上解析失败或成功。</td>
    </tr>
    <tr>
      <td><h5>json.ignore-parse-errors</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>决定是否忽略在属性或者行上的解析失败。如果忽略在属性上解析失败，则会将该属性的值设置为null。</td>
    </tr>
    <tr>
      <td><h5>json.timestamp-format.standard</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;"><code>'SQL'</code></td>
      <td>String</td>
      <td>声明输入和输出的时间戳格式。当前支持的格式为<code>'SQL'</code> 以及 <code>'ISO-8601'</code>：
      <ul>
        <li>可选参数 <code>'SQL'</code> 将会以 "yyyy-MM-dd HH:mm:ss.s{precision}" 的格式解析时间戳, 例如 '2020-12-30 12:13:14.123'，且会以相同的格式输出。</li>
        <li>可选参数 <code>'ISO-8601'</code> 将会以 "yyyy-MM-ddTHH:mm:ss.s{precision}" 的格式解析输入时间戳, 例如 '2020-12-30T12:13:14.123' ，且会以相同的格式输出。</li>
      </ul>
      </td>
    </tr>
    </tbody>
</table>

数据类型映射关系
----------------

当前，JSON schema将会自动从table schema之中自动推导得到。显式地定义JSON schema不再得到支持。

Flink中的JSON格式使用 [jackson databind API](https://github.com/FasterXML/jackson-databind)去解析并生成JSON。

下表列出了Flink中的数据类型与JSON中的数据类型的映射关系。

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





