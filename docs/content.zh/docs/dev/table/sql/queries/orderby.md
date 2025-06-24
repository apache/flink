---
title: "ORDER BY 语句"
weight: 12
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

# ORDER BY 语句

{{< label Batch >}} {{< label Streaming >}}

`ORDER BY` 子句使结果行根据指定的表达式进行排序。 如果两行根据最左边的表达式相等，则根据下一个表达式进行比较，依此类推。 如果根据所有指定的表达式它们相等，则它们以与实现相关的顺序返回。

在流模式下运行时，表的主要排序顺序必须按[时间属性]({{< ref "docs/dev/table/concepts/time_attributes" >}})升序。 所有后续的 orders 都可以自由选择。 但是批处理模式没有这个限制。

```sql
SELECT *
FROM Orders
ORDER BY order_time, order_id
```

{{< top >}}
