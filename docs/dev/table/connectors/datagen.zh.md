---
title: "DataGen SQL 连接器"
nav-title: DataGen
nav-parent_id: sql-connectors
nav-pos: 12
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

<span class="label label-primary">Scan Source: 有界</span>
<span class="label label-primary">Scan Source: 无界</span>

* This will be replaced by the TOC
{:toc}

DataGen 连接器允许按数据生成规则进行读取。

DataGen 连接器可以使用[计算列语法]({% link dev/table/sql/create.zh.md %}#create-table)。
这使您可以灵活地生成记录。

DataGen 连接器是内置的。

<span class="label label-danger">注意</span> 不支持复杂类型: Array，Map，Row。 请用计算列构造这些类型。

怎么创建一个 DataGen 的表
----------------

表的有界性：当表中字段的数据全部生成完成后，source 就结束了。 因此，表的有界性取决于字段的有界性。

每个列，都有两种生成数据的方法：

- 随机生成器是默认的生成器，您可以指定随机生成的最大和最小值。char、varchar、string （类型）可以指定长度。它是无界的生成器。

- 序列生成器，您可以指定序列的起始和结束值。它是有界的生成器，当序列数字达到结束值，读取结束。
<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE datagen (
 f_sequence INT,
 f_random INT,
 f_random_str STRING,
 ts AS localtimestamp,
 WATERMARK FOR ts AS ts
) WITH (
 'connector' = 'datagen',

 -- optional options --

 'rows-per-second'='5',

 'fields.f_sequence.kind'='sequence',
 'fields.f_sequence.start'='1',
 'fields.f_sequence.end'='1000',

 'fields.f_random.min'='1',
 'fields.f_random.max'='1000',

 'fields.f_random_str.length'='10'
)
{% endhighlight %}
</div>
</div>


连接器参数
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">参数</th>
        <th class="text-center" style="width: 10%">是否必选</th>
        <th class="text-center" style="width: 10%">默认参数</th>
        <th class="text-center" style="width: 10%">数据类型</th>
        <th class="text-center" style="width: 45%">描述</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>必须</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定要使用的连接器，这里是 'datagen'。</td>
    </tr>
    <tr>
      <td><h5>rows-per-second</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">10000</td>
      <td>Long</td>
      <td>每秒生成的行数，用以控制数据发出速率。</td>
    </tr>
    <tr>
      <td><h5>fields.#.kind</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">random</td>
      <td>String</td>
      <td>指定 '#' 字段的生成器。可以是 'sequence' 或 'random'。</td>
    </tr>
    <tr>
      <td><h5>fields.#.min</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(Minimum value of type)</td>
      <td>(Type of field)</td>
      <td>随机生成器的最小值，适用于数字类型。</td>
    </tr>
    <tr>
      <td><h5>fields.#.max</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(Maximum value of type)</td>
      <td>(Type of field)</td>
      <td>随机生成器的最大值，适用于数字类型。</td>
    </tr>
    <tr>
      <td><h5>fields.#.length</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">100</td>
      <td>Integer</td>
      <td>随机生成器生成字符的长度，适用于 char、varchar、string。</td>
    </tr>
    <tr>
      <td><h5>fields.#.start</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>(Type of field)</td>
      <td>序列生成器的起始值。</td>
    </tr>
    <tr>
      <td><h5>fields.#.end</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>(Type of field)</td>
      <td>序列生成器的结束值。</td>
    </tr>
    </tbody>
</table>
