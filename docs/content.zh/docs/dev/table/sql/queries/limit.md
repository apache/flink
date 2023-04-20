---
title: "LIMIT 语句"
weight: 13
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

# LIMIT 语句

{{< label Batch >}}

`LIMIT` 子句限制 `SELECT` 语句返回的行数。 通常，此子句与 ORDER BY 结合使用，以确保结果是确定性的。

以下示例选择 `Orders` 表中的前 3 行。

```sql
SELECT *
FROM Orders
ORDER BY orderTime
LIMIT 3
```

{{< top >}}
