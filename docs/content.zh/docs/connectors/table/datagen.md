---
title: DataGen
weight: 13
type: docs
aliases:
  - /zh/dev/table/connectors/datagen.html
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

# DataGen SQL 连接器

{{< label "Scan Source: 有界" >}}
{{< label "Scan Source: 无界" >}}

DataGen 连接器允许基于内存生成数据来创建表。
在本地开发时，若不访问外部系统（如 Kafka），这会非常有用。
可以使用[计算列语法]({{< ref "docs/dev/table/sql/create" >}}#create-table)灵活地生成记录。

DataGen 连接器是内置的，不需要额外的依赖项。

用法
-----

默认情况下，DataGen 表将创建无限数量的行，每列都有一个随机值。
还可以指定总行数，从而生成有界表。

DataGen 连接器可以生成符合其 schema 的数据，应该注意的是，它按如下方式处理长度受限的字段：

* 对于固定长度的数据类型（char、binary），字段长度只能由 schema 定义，且不支持自定义；
* 对于可变长度数据类型 （varchar、varbinary），字段默认长度由 schema 定义，且自定义长度不能大于 schema 定义；
* 对于超长字段（string、bytes），字段默认长度为 100，但可以定义为小于 2^31 的长度。

还支持序列生成器，您可以指定序列的起始和结束值。
如果表中有任一列是序列类型，则该表将是有界的，并在第一个序列完成时结束。

时间类型字段对应的值始终是本地机器当前系统时间。

```sql
CREATE TABLE Orders (
    order_number BIGINT,
    price        DECIMAL(32,2),
    buyer        ROW<first_name STRING, last_name STRING>,
    order_time   TIMESTAMP(3)
) WITH (
  'connector' = 'datagen'
)
```

DataGen 连接器通常与 ``LIKE`` 子句结合使用，以模拟物理表。

```sql
CREATE TABLE Orders (
    order_number BIGINT,
    price        DECIMAL(32,2),
    buyer        ROW<first_name STRING, last_name STRING>,
    order_time   TIMESTAMP(3)
) WITH (...)

-- create a bounded mock table
CREATE TEMPORARY TABLE GenOrders
WITH (
    'connector' = 'datagen',
    'number-of-rows' = '10'
)
LIKE Orders (EXCLUDING ALL)
```

此外，对于可变长度类型（varchar、string、varbinary 和 bytes），您可以指定是否生成可变长度的数据。

```sql
CREATE TABLE Orders (
    order_number BIGINT,
    price        DECIMAL(32,2),
    buyer        ROW<first_name STRING, last_name STRING>,
    order_time   TIMESTAMP(3),
    seller       VARCHAR(150)
) WITH (
  'connector' = 'datagen',
  'fields.seller.var-len' = 'true'
)
```

字段类型
-----

<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">Type</th>
            <th class="text-center" style="width: 25%">Supported Generators</th>
            <th class="text-center" style="width: 50%">Notes</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td>BOOLEAN</td>
            <td>random</td>
            <td></td>
        </tr>
        <tr>
            <td>CHAR</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>VARCHAR</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>BINARY</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>VARBINARY</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>STRING</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>DECIMAL</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>TINYINT</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>SMALLINT</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>INT</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>BIGINT</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>FLOAT</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>DOUBLE</td>
            <td>random / sequence</td>
            <td></td>
        </tr>
        <tr>
            <td>DATE</td>
            <td>random</td>
            <td>总是解析为本地机器的当前日期。</td>
        </tr>
        <tr>
            <td>TIME</td>
            <td>random</td>
            <td>总是解析为本地机器的当前时间。</td>
        </tr>
        <tr>
            <td>TIMESTAMP</td>
            <td>random</td>
            <td>
                解析为相对于本地机器的当前时间戳向过去偏移的时间戳。偏移的最大值可以通过 'max-past' 选项指定。
            </td>
        </tr>
        <tr>
            <td>TIMESTAMP_LTZ</td>
            <td>random</td>
            <td>
                解析为相对于本地机器的当前时间戳向过去偏移的时间戳。偏移的最大值可以通过 'max-past' 选项指定。
            </td>
        </tr>
        <tr>
            <td>INTERVAL YEAR TO MONTH</td>
            <td>random</td>
            <td></td>
        </tr>
        <tr>
            <td>INTERVAL DAY TO MONTH</td>
            <td>random</td>
            <td></td>
        </tr>
        <tr>
            <td>ROW</td>
            <td>random</td>
            <td>生成具有随机字段数据的行。</td>
        </tr>
        <tr>
            <td>ARRAY</td>
            <td>random</td>
            <td>生成具有随机元素的数组。</td>
        </tr>
        <tr>
            <td>MAP</td>
            <td>random</td>
            <td>生成具有随机元素的 Map。</td>
        </tr>
        <tr>
            <td>MULTISET</td>
            <td>random</td>
            <td>生成具有随机元素的多重集。</td>
        </tr>
    </tbody>
</table>

连接器参数
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">参数</th>
        <th class="text-center" style="width: 10%">是否必选</th>
        <th class="text-center" style="width: 10%">默认值</th>
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
      <td><h5>number-of-rows</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>生成数据的总行数。默认情况下，该表是无界的。</td>
    </tr>
    <tr>
      <td><h5>scan.parallelism</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>定义算子并行度。不设置将使用全局默认并发。</td>
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
      <td><h5>fields.#.max-past</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">0</td>
      <td>Duration</td>
      <td>随机生成器生成相对当前时间向过去偏移的最大值，适用于 timestamp 类型。</td>
    </tr>
    <tr>
      <td><h5>fields.#.length</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">100</td>
      <td>Integer</td>
      <td>
          随机生成器生成字符的长度，适用于 varchar、varbinary、string、bytes、array、map、multiset。
          请注意对于可变长字段（varchar、varbinary），默认长度由 schema 定义，且长度不可设置为大于它；
          对于超长字段（string、bytes），默认长度是 100 且可设置为小于 2^31 的长度；
          对于结构化字段（数组、Map、多重集），默认元素数量为 3 且可以自定义。
      </td>
    </tr>
    <tr>
      <td><h5>fields.#.var-len</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>是否生成变长数据，请注意只能用于变长类型（varchar、string、varbinary、bytes）。</td>
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
    <tr>
      <td><h5>fields.#.null-rate</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>(Type of field)</td>
      <td>空值比例。</td>
    </tr>
    </tbody>
</table>
