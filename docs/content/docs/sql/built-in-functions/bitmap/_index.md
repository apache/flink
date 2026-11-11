---
title: "Bitmap"
weight: 2
type: docs
bookCollapseSection: true
sqlFunctions: [bitmap, bitmapagg]
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

# Bitmap Functions

Bitmap functions work on [`BITMAP`]({{< ref "docs/sql/reference/data-types" >}}#bitmap) values, which store a set of 32-bit integers in a compressed form. The scalar functions build, combine, convert and inspect bitmaps, such as `BITMAP_BUILD` and `BITMAP_CARDINALITY`. The [aggregate functions](#bitmap-aggregate-functions) combine the values of many rows into one bitmap or its cardinality.

{{< sql_functions "bitmap" >}}

## Bitmap Aggregate Functions

**Performance Tips:**

- It is strongly recommended to enable [MiniBatch aggregation]({{< ref "docs/dev/table/tuning" >}}) or use bitmap aggregate functions within [window aggregations]({{< ref "docs/sql/reference/queries/window-agg" >}}) to optimize state access overhead and significantly improve performance.
- Bitmap aggregate functions perform best with append-only input. Performance degrades noticeably with retraction input, so avoid multi-level GROUP BY aggregations on BITMAP columns when possible.
- For cardinality-only scenarios where the intermediate bitmap is not needed, prefer `BITMAP_XX_CARDINALITY_AGG()` over `BITMAP_CARDINALITY(BITMAP_XX_AGG())`. They are functionally equivalent, but the former avoids materializing the intermediate bitmap and performs better.

{{< sql_functions "bitmapagg" >}}
