---
title: "ORDER BY clause"
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

# ORDER BY clause

{{< label Batch >}} {{< label Streaming >}}

The `ORDER BY` clause causes the result rows to be sorted according to the specified expression(s). If two rows are equal according to the leftmost expression, they are compared according to the next expression and so on. If they are equal according to all specified expressions, they are returned in an implementation-dependent order.

When running in streaming mode, the primary sort order of a table must be ascending on a [time attribute]({{< ref "docs/dev/table/concepts/time_attributes" >}}). All subsequent orders can be freely chosen. But there is no this limitation in batch mode.

```sql
SELECT *
FROM Orders
ORDER BY order_time, order_id
```

{{< top >}}
