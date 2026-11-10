---
title: "JSON"
weight: 10
type: docs
sqlFunctions: json
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

# JSON 函数

JSON 函数使用符合 ISO/IEC TR 19075-6 SQL标准的 JSON 路径表达式。 它们的语法受到 ECMAScript 的启发，并
采用了 ECMAScript 的许多功能，但不是其子集或超集。

路径表达式有宽容模式和严格模式两种模式。 当不指定时，默认使用严格模式。
严格模式旨在从 Schema 的角度检查数据，并且只要数据不符合路径表达式就会抛出错误。 但是像`JSON_VALUE`的函数
允许定义遇到错误时的后备行为。 另一方面，宽容模式更加宽容，并将错误转换为空序列。

特殊字符`$`表示 JSON 路径中的根节点。 路径可以访问属性（`$.a`）， 数组元素 (`$.a[0].b`)，或遍历数组中的
所有元素 (`$.a[*].b`)。

已知限制：
* 目前，并非宽容模式的所有功能都被正确支持。 这是一个上游的错误（CALCITE-4717）。无法保证行为符合标准。

{{< sql_functions_zh "json" >}}
