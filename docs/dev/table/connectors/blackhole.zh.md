---
title: "BlackHole SQL 连接器"
nav-title: BlackHole
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

<span class="label label-primary">Sink: Bounded</span>
<span class="label label-primary">Sink: UnBounded</span>

* This will be replaced by the TOC
{:toc}

BlackHole 连接器允许接收所有输入记录。它被设计用于：

- 高性能测试。
- UDF 输出，而不是实质性 sink。

就像类 Unix 操作系统上的 /dev/null。

BlackHole 连接器是内置的。

如何创建 BlackHole 表
----------------

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE blackhole_table (
 f0 INT,
 f1 INT,
 f2 STRING,
 f3 DOUBLE
) WITH (
 'connector' = 'blackhole'
)
{% endhighlight %}
</div>
</div>

或者，可以基于现有模式使用 [LIKE 子句]({% link dev/table/sql/create.zh.md %}#create-table) 创建。

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE blackhole_table WITH ('connector' = 'blackhole')
LIKE source_table (EXCLUDING ALL)
{% endhighlight %}
</div>
</div>

连接器选项
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">选项</th>
        <th class="text-center" style="width: 9%">是否必要</th>
        <th class="text-center" style="width: 7%">默认值</th>
        <th class="text-center" style="width: 10%">类型</th>
        <th class="text-center" style="width: 50%">描述</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>必要</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定需要使用的连接器，此处应为‘blackhole’。</td>
    </tr>
    </tbody>
</table>
