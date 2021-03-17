---
title: Print
weight: 14
type: docs
aliases:
  - /zh/dev/table/connectors/print.html
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

# Print SQL 连接器

{{< label "Sink" >}}

Print 连接器允许将每一行写入标准输出流或者标准错误流。

设计目的：

- 简单的流作业测试。
- 对生产调试带来极大便利。

四种 format 选项：

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 40%">打印内容</th>
        <th class="text-center" style="width: 30%">条件 1</th>
        <th class="text-center" style="width: 30%">条件 2</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>标识符:任务 ID> 输出数据</h5></td>
      <td>需要提供前缀打印标识符</td>
      <td>parallelism > 1</td>
    </tr>
    <tr>
      <td><h5>标识符> 输出数据</h5></td>
      <td>需要提供前缀打印标识符</td>
      <td>parallelism == 1</td>
    </tr>
    <tr>
      <td><h5>任务 ID> 输出数据</h5></td>
      <td>不需要提供前缀打印标识符</td>
      <td>parallelism > 1</td>
    </tr>
    <tr>
      <td><h5>输出数据</h5></td>
      <td>不需要提供前缀打印标识符</td>
      <td>parallelism == 1</td>
    </tr>
    </tbody>
</table>

输出字符串格式为 "$row_kind(f0,f1,f2...)"，row_kind是一个 `RowKind` 类型的短字符串，例如："+I(1,1)"。

Print 连接器是内置的。

<span class="label label-danger">注意</span> 在任务运行时使用 Print Sinks 打印记录，你需要注意观察任务日志。

如何创建一张基于 Print 的表
----------------

```sql
CREATE TABLE print_table (
 f0 INT,
 f1 INT,
 f2 STRING,
 f3 DOUBLE
) WITH (
 'connector' = 'print'
)
```

或者，也可以通过 [LIKE子句]({{< ref "docs/dev/table/sql/create" >}}#create-table) 基于已有表的结构去创建新表。

{{< tabs "0baef2bc-71e4-4507-9152-349bdf2420a4" >}}
{{< tab "SQL" >}}
```sql
CREATE TABLE print_table WITH ('connector' = 'print')
LIKE source_table (EXCLUDING ALL)
```
{{< /tab >}}
{{< /tabs >}}

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
      <td>必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定要使用的连接器，此处应为 'print'</td>
    </tr>
    <tr>
      <td><h5>print-identifier</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>配置一个标识符作为输出数据的前缀。</td>
    </tr>
    <tr>
      <td><h5>standard-error</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>如果 format 需要打印为标准错误而不是标准输出，则为 True 。</td>
    </tr>
    <tr>
      <td><h5>sink.parallelism</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>为 Print sink operator 定义并行度。默认情况下，并行度由框架决定，和链在一起的上游 operator 一致。</td>
    </tr>
    </tbody>
</table>
