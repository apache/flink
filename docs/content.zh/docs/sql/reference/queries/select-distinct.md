---
title: "SELECT DISTINCT"
weight: 5
type: docs
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

# SELECT DISTINCT

{{< label Batch >}} {{< label Streaming >}}

如果使用”SELECT DISTINCT“查询,所有的复制行都会从结果集(每个分组只会保留一行)中被删除.

```sql
SELECT DISTINCT id FROM Orders
```
对于流式查询, 计算查询结果所需要的状态可能会源源不断地增长,而状态大小又依赖不同行的数量.此时,可以通过配置文件为状态设置合适的存活时间(TTL),以防止过大的状态可能对查询结果的正确性的影响.具体配置可参考:[查询相关的配置]({{< ref "docs/dev/table/config" >}}#table-exec-state-ttl).

{{< top >}}
