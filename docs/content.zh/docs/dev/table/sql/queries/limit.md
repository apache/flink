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

`LIMIT` clause constrains the number of rows returned by the `SELECT` statement. In general, this clause is used in conjunction with ORDER BY to ensure that the results are deterministic.

The following example selects the first 3 rows in `Orders` table.

```sql
SELECT *
FROM Orders
ORDER BY orderTime
LIMIT 3
```

{{< top >}}
