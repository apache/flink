---
title: "位图"
weight: 2
type: docs
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

# 位图函数

{{< sql_functions_zh "bitmap" >}}

## 位图聚合函数

**性能建议：**

- 强烈建议开启 [MiniBatch 聚合]({{< ref "docs/dev/table/tuning" >}})或在[窗口聚合]({{< ref "docs/sql/reference/queries/window-agg" >}})中使用位图聚合函数，以优化状态访问开销、显著提升性能。
- 位图聚合函数在处理仅追加（Append-Only）输入时性能最佳。处理回撤（Retraction）输入时性能会明显下降，因此应尽量避免对 BITMAP 列进行多级 GROUP BY 聚合。
- 在仅需要基数统计而不需要中间位图的场景中，建议使用 `BITMAP_XX_CARDINALITY_AGG()` 替代 `BITMAP_CARDINALITY(BITMAP_XX_AGG())`。两者功能一致，但前者避免了中间位图的物化，性能更优。

{{< sql_functions_zh "bitmapagg" >}}
