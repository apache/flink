---
title: "内置函数"
weight: 1
type: docs
aliases:
  - /zh/dev/table/functions/systemFunctions.html
  - /zh/docs/sql/built-in-functions/
  - /zh/dev/python/table-api-users-guide/built_in_functions.html
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

# 系统（内置）函数

Flink Table API & SQL 为用户提供了一组内置的数据转换函数。本页简要介绍了它们。如果你需要的函数尚不支持，你可以实现
[用户自定义函数]({{< ref "docs/dev/table/functions/udfs" >}})。如果你觉得这个函数够通用，请
<a href="https://issues.apache.org/jira/secure/CreateIssue!default.jspa">创建一个 Jira issue</a>并详细
说明。

标量函数
----------------

标量函数将零、一个或多个值作为输入并返回单个值作为结果。

### 比较函数

{{< sql_functions_zh "comparison" >}}

### 逻辑函数

{{< sql_functions_zh "logical" >}}

### 算术函数

{{< sql_functions_zh "arithmetic" >}}

### 字符串函数

{{< sql_functions_zh "string" >}}

### 时间函数

{{< sql_functions_zh "temporal" >}}

### 条件函数

{{< sql_functions_zh "conditional" >}}

### 类型转换函数

{{< sql_functions_zh "conversion" >}}

### 集合函数

{{< sql_functions_zh "collection" >}}

### JSON 函数

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

### Variant 函数

{{< sql_functions_zh "variant" >}}

#### XML to VARIANT

The `PARSE_XML` function maps the root element of an XML document to a `VARIANT` object. For
example:

```sql
-- {"book":{"#":{"price":[1,2],"title":0},"@pages":"320","price":["12.50","13.50"],"title":"Dune"}}
PARSE_XML('<book pages="320"><title>Dune</title><price>12.50</price><price>13.50</price></book>')
```

Attributes become fields with an `@` prefix, and a child element that occurs more than once becomes
an array. The `#` field records the order of the children, as described below. It can be ignored
when reading fields.

Values are strings, unless the document declares a type with `xsi:type`, see below. Since a cast
from a `VARIANT` never parses a string, cast a value to `STRING` first and convert it with a regular
cast:

```sql
CAST(v['book']['title'] AS STRING)                -- 'Dune'
CAST(v['book']['price'][2] AS STRING)             -- '13.50'
CAST(CAST(v['book']['@pages'] AS STRING) AS INT)  -- 320
```

Each element is mapped by the following rules:

| XML input                                                          | Stored `VARIANT` value                                      |
|--------------------------------------------------------------------|-------------------------------------------------------------|
| Element with `xsi:nil="true"`                                      | `null`, or field `$` with `null` if it has other attributes |
| Element without attributes and child elements                      | its text, or `""` if it is empty                            |
| Any other element                                                  | object                                                      |
| Attribute `name`                                                   | field `@name`, a `STRING`                                   |
| Child element `name`                                               | field `name`                                                |
| Text of an element with attributes or child elements               | field `$`                                                   |
| Child element or text that occurs more than once                   | array, in document order                                    |
| Element with differently named children, or with children and text | field `#` with the order of its content                     |

A `VARIANT` object doesn't keep the order of its fields, so the `#` field records it. It maps each
key to the position of its item, or to an array of positions if the key occurs more than once.
Positions count the child elements and texts of the element, starting at 0. For example:

```sql
-- {"book":{"#":{"$":[1,3],"price":[2,4],"title":0},
--          "$":["This is a description","Text in between"],
--          "@pages":"320",
--          "price":["12.50",{"$":"13.50","@currency":"EUR"}],
--          "title":"Dune"}}
PARSE_XML('
  <book pages="320">
    <title>Dune</title>
    This is a description
    <price>12.50</price>
    Text in between
    <price currency="EUR">13.50</price>
  </book>')
```

Here, `title` is the first item, the two texts are the second and the fourth, and the two `price`
elements are the third and the fifth. The second `price` has an attribute, so it is stored as an
object, while the first one is a string. An element whose content has only one key has no `#` field,
since the order of an array is the document order.

Whether a child element is stored as a single value or as an array depends on how often it occurs,
so two documents that follow the same schema can map to different shapes. The default behavior
ensures a space-efficient representation. With `PARSE_XML(xml, TRUE)`, every child element, text,
and position in a `#` field is stored as an array, even if it occurs only once. The root element is never wrapped, since a document has exactly one.

```sql
-- {"book":{"title":["Dune"]}}
PARSE_XML('<book><title>Dune</title></book>', TRUE)
```

An `xsi:type` attribute parses the text of an element as the matching kind, which replaces the text,
or the `$` field if the element has other attributes. The prefix of the type is ignored, so `xs:int`,
`xsd:int`, and `int` are the same type. If the element has child elements or no text, the type isn't
listed below, or the text isn't a valid value of the type, the text stays a `STRING` and `xsi:type`
is kept as an attribute. For example, `<a xsi:type="int">5</a>` maps to `{"a":5}`, and
`<a xsi:type="int">five</a>` maps to `{"a":{"$":"five","@xsi:type":"int"}}`. The text has to be in
the lexical form that XML Schema defines for the type, so `1e5` isn't a `decimal`, `1f` isn't a
`float`, and `12:30` isn't a `time`. Years after 9999 and the time `24:00:00` stay a `STRING` as well.

| `xsi:type`                   | Stored `VARIANT` kind                        |
|------------------------------|----------------------------------------------|
| `string`                     | `STRING`                                     |
| `boolean`                    | `BOOLEAN`, from `true`, `false`, `1`, or `0` |
| `byte`                       | `TINYINT`                                    |
| `short`                      | `SMALLINT`                                   |
| `int`                        | `INT`                                        |
| `long`                       | `BIGINT`                                     |
| `integer`, `decimal`         | `DECIMAL`, with at most 38 digits            |
| `float`                      | `FLOAT`, except for `NaN` and infinity       |
| `double`                     | `DOUBLE`, except for `NaN` and infinity      |
| `date` without an offset     | `DATE`                                       |
| `time` without an offset     | `TIME`                                       |
| `dateTime` with an offset    | `TIMESTAMP_LTZ`                              |
| `dateTime` without an offset | `TIMESTAMP`                                  |

The offset of a `dateTime` is `Z` or a signed `hh:mm` of at most 14 hours. It is used to compute the
instant and isn't stored, since a `TIMESTAMP_LTZ` has no offset. For example,
`2026-09-30T12:30:00+02:00`, `2026-09-30T10:30:00Z`, and `2026-09-30T06:00:00-04:30` map to the same
`TIMESTAMP_LTZ`. A `date` or `time` with an offset stays a `STRING`, since it can't be converted to an
instant without a date or a time.

`TIME` keeps microseconds, so `12:30:45.1234567` is stored as `12:30:45.123456`. `TIMESTAMP` and
`TIMESTAMP_LTZ` keep nanoseconds, or microseconds outside the nanosecond range from 1677 to 2262.
Further digits are truncated, like in a cast, instead of keeping the text as a `STRING`.

A typed value casts directly to its type, without the cast to `STRING`:

```sql
CAST(PARSE_XML('<a xsi:type="int">5</a>')['a'] AS INT)                                  -- 5
CAST(PARSE_XML('<a xsi:type="dateTime">2026-09-30T12:30:00</a>')['a'] AS TIMESTAMP(3))  -- 2026-09-30 12:30:00.000
```

An `xsi:nil="true"` attribute takes precedence over the content of an element, so
`<price xsi:nil="true">12.50</price>` maps to `{"price":null}`. If the element has other attributes,
its content is replaced with a `null` text, so `<price xsi:nil="true" currency="EUR">12.50</price>`
maps to `{"price":{"$":null,"@currency":"EUR"}}`. In both cases, `xsi:nil` and `xsi:type` are dropped,
and so is `xsi:nil="false"`. Like in XML Schema, `xsi:nil` accepts `true`, `false`, `1`, and `0`,
and any other value is kept as a regular attribute.

`xsi:type` and `xsi:nil` are recognized by their literal `xsi:` prefix, even if the document doesn't
declare it or binds it to a different namespace. The `xmlns:xsi` declaration is dropped.

`PARSE_XML` also normalizes the document:

- Text is trimmed of spaces, tabs, and line breaks, and whitespace-only text between tags is dropped.
- Comments and processing instructions are dropped. CDATA sections are text.
- Entities and character references are expanded, including the entities that the document declares
  in its DTD.
- Names are kept as written, including their namespace prefix. Namespace declarations are regular
  attributes, for example `@xmlns:ns`.

A document fails to parse if it:

- references an external entity or an external DTD,
- expands more than 64,000 entities, or more than about 16 million characters of entity text,
- nests elements more than 500 levels deep,
- is an XML 1.1 document.

In all of these cases, `TRY_PARSE_XML` returns `NULL`.

#### Variant paths

`VARIANT_GET` and `TRY_VARIANT_GET` take a path that names one place in a Variant. A path starts
with `$`, the whole value, followed by steps: `.name`, `['name']` or `["name"]` for a field, and
`[n]` for an array element. For example, on
`PARSE_JSON('{"user": {"name": "ada"}, "items": [{"qty": 2}]}')`:

| Path                | Points at                          |
|---------------------|------------------------------------|
| `$`                 | the whole value                    |
| `$.user.name`       | `"ada"`                            |
| `$['user']["name"]` | `"ada"`                            |
| `$.items[0].qty`    | `2`                                |
| `$.items[1]`        | nothing, the index is past the end |
| `$.user[0]`         | nothing, `user` is not an array    |

- Array indices start at 0, as in `JSON_VALUE`, while `variant[INT]` starts at 1. So
  `VARIANT_GET(v, '$.items[0]')` and `v['items'][1]` are the same element.
- Use brackets with single or double quotes for a name with whitespace or one of the characters
  `. [ ] ( ) * ' " \`, for example `$['first name']` or `$["first name"]`. Inside the quotes, `\\`,
  `\'` and `\"` stand for a backslash and a quote.
- Wildcards, filters, slices, negative indices and the `lax` and `strict` modes are not supported,
  so a path always names one place.
- Trailing spaces are ignored.
- A malformed path literal fails at planning time. Any other malformed path fails when the function
  runs.

A path points at nothing if a field is missing, an index is past the end, or a step does not match
the kind of value, such as a field of an array. Each function describes what it does in that case.

### 值构建函数

{{< sql_functions_zh "valueconstruction" >}}

### 值获取函数

{{< sql_functions_zh "valueaccess" >}}

### 分组函数

{{< sql_functions_zh "grouping" >}}

### 哈希函数

{{< sql_functions_zh "hashfunctions" >}}

### 位图函数

{{< sql_functions_zh "bitmap" >}}

### 辅助函数

{{< sql_functions_zh "auxiliary" >}}

聚合函数
-------------------

聚合函数将所有的行作为输入，并返回单个聚合值作为结果。

{{< sql_functions_zh "aggregate" >}}

### 位图聚合函数

**性能建议：**

- 强烈建议开启 [MiniBatch 聚合]({{< ref "docs/dev/table/tuning" >}})或在[窗口聚合]({{< ref "docs/sql/reference/queries/window-agg" >}})中使用位图聚合函数，以优化状态访问开销、显著提升性能。
- 位图聚合函数在处理仅追加（Append-Only）输入时性能最佳。处理回撤（Retraction）输入时性能会明显下降，因此应尽量避免对 BITMAP 列进行多级 GROUP BY 聚合。
- 在仅需要基数统计而不需要中间位图的场景中，建议使用 `BITMAP_XX_CARDINALITY_AGG()` 替代 `BITMAP_CARDINALITY(BITMAP_XX_AGG())`。两者功能一致，但前者避免了中间位图的物化，性能更优。

{{< sql_functions_zh "bitmapagg" >}}

Table Functions
---------------

Table functions take zero, one, or more values as input and return multiple rows (a table) as the result. Most built-in table functions take a table as an input argument.
Table functions can be used in two ways: as stand-alone inputs, where they are invoked just once, or in a `LATERAL` context, where they are invoked for each row of an outer table.

| Function                                   | Description                                                                                                                                                                                                                                                                                                     |
|--------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `TUMBLE(data => TABLE t, ...)`             | Assigns each row of the `data` table to a tumbling window specified by additional window columns (`window_start`, `window_end`, `window_time`). See [Window TVF]({{< ref "docs/sql/reference/queries/window-tvf" >}}#tumble) for the full list of arguments, semantics, and usage.                              |
| `HOP(data => TABLE t, ...)`                | Assigns each row of the `data` table to a hopping window specified by additional window columns (`window_start`, `window_end`, `window_time`). See [Window TVF]({{< ref "docs/sql/reference/queries/window-tvf" >}}#hop) for the full list of arguments, semantics, and usage.                                  |
| `CUMULATE(data => TABLE t, ...)`           | Assigns each row of the `data` table to a cumulating window specified by additional window columns (`window_start`, `window_end`, `window_time`). See [Window TVF]({{< ref "docs/sql/reference/queries/window-tvf" >}}#cumulate) for the full list of arguments, semantics, and usage.                          |
| `SESSION(data => TABLE t, ...)`            | Assigns each row of the `data` table to a session window specified by additional window columns (`window_start`, `window_end`, `window_time`). See [Window TVF]({{< ref "docs/sql/reference/queries/window-tvf" >}}#session) for the full list of arguments, semantics, and usage.                              |
| `FROM_CHANGELOG(input => TABLE t [, ...])` | Converts an append-only table with an explicit operation column into a dynamic table. See Changelog Conversion for the full list of arguments, semantics, and usage.                                                                       |
| `TO_CHANGELOG(input => TABLE t [, ...])`   | Converts a dynamic table into an append-only table with an explicit operation column. See Changelog Conversion for the full list of arguments, semantics, and usage.                                                                         |
| `SNAPSHOT(input => TABLE t [, ...])`       | Returns the current state of a dynamic table `t`. `SNAPSHOT` can only be used in a `LATERAL` context and not as a stand-alone table function. See [LATERAL SNAPSHOT join]({{< ref "docs/sql/reference/queries/joins" >}}#lateral-snapshot-join) for the full list of arguments, the join semantics, and usage. |
| `DEDUPLICATE_KEEP_FIRST(input => TABLE t [, ...])` | Keeps the first record per key as an append-only table (first arrival, or earliest event time with `on_time`). See [Deduplicate Keep First]({{< ref "docs/sql/reference/queries/deduplicate-keep-first" >}}#deduplicate_keep_first) for the full list of arguments, semantics, and usage. |

To implement your own table functions, see [user-defined table functions]({{< ref "docs/dev/table/functions/udfs" >}}#table-functions).

时间间隔单位和时间点单位标识符
---------------------------------------

下表列出了时间间隔单位和时间点单位标识符。

对于 Table API，请使用 `_` 代替空格（例如 `DAY_TO_HOUR`）。
Plural works for SQL only.

| 时间间隔单位                   | 时间点单位                        |
|:-------------------------|:-----------------------------|
| `MILLENNIUM`             |                              |
| `CENTURY`                |                              |
| `DECADE`                 |                              |
| `YEAR(S)`                | `YEAR`                       |
| `YEAR(S) TO MONTH(S)`    |                              |
| `QUARTER(S)`             | `QUARTER`                    |
| `MONTH(S)`               | `MONTH`                      |
| `WEEK(S)`                | `WEEK`                       |
| `DAY(S)`                 | `DAY`                        |
| `DAY(S) TO HOUR(S)`      |                              |
| `DAY(S) TO MINUTE(S)`    |                              |
| `DAY(S) TO SECOND(S)`    |                              |
| `HOUR(S)`                | `HOUR`                       |
| `HOUR(S) TO MINUTE(S)`   |                              |
| `HOUR(S) TO SECOND(S)`   |                              |
| `MINUTE(S)`              | `MINUTE`                     |
| `MINUTE(S) TO SECOND(S)` |                              |
| `SECOND(S)`              | `SECOND`                     |
| `MILLISECOND`            | `MILLISECOND`                |
| `MICROSECOND`            | `MICROSECOND`                |
| `NANOSECOND`             |                              |
| `EPOCH`                  |                              |
| `DOY` _（仅适用SQL）_         |                              |
| `DOW` _（仅适用SQL）_         |                              |
| `ISODOW` _（仅适用SQL）_      |                              |
| `ISOYEAR` _（仅适用SQL）_     |                              |
|                          | `SQL_TSI_YEAR` _（仅适用SQL）_    |
|                          | `SQL_TSI_QUARTER` _（仅适用SQL）_ |
|                          | `SQL_TSI_MONTH` _（仅适用SQL）_   |
|                          | `SQL_TSI_WEEK` _（仅适用SQL）_    |
|                          | `SQL_TSI_DAY` _（仅适用SQL）_     |
|                          | `SQL_TSI_HOUR` _（仅适用SQL）_    |
|                          | `SQL_TSI_MINUTE` _（仅适用SQL）_  |
|                          | `SQL_TSI_SECOND ` _（仅适用SQL）_ |

{{< top >}}

列函数
---------------------------------------

列函数用于选择或丢弃表的列。

{{< hint info >}}
列函数仅在 Table API 中使用。
{{< /hint >}}

| 语法                      | 描述                         |
| :----------------------- | :--------------------------- |
| withColumns(...)         | 选择指定的列                   |
| withoutColumns(...)      | 选择除指定列以外的列            |
| withAllColumns()    | select all columns (like `SELECT *` in SQL) |

详细语法如下：

```text
列函数:
    withColumns(columnExprs)
    withoutColumns(columnExprs)
    withAllColumns()

多列表达式:
    columnExpr [, columnExpr]*

单列表达式:
    columnRef | columnIndex to columnIndex | columnName to columnName

列引用:
    columnName(The field name that exists in the table) | columnIndex(a positive integer starting from 1)
```
列函数的用法如下表所示（假设我们有一个包含 5 列的表：`(a: Int, b: Long, c: String, d:String, e: String)`）：

| 接口 | 用法举例 | 描述 |
|-|-|-|
| withColumns($(*)) | select(withColumns($("*")))  = select($("a"), $("b"), $("c"), $("d"), $("e")) | 全部列 |
| withColumns(m to n) | select(withColumns(range(2, 4))) = select($("b"), $("c"), $("d")) | 第 m 到第 n 列 |
| withColumns(m, n, k)  | select(withColumns(lit(1), lit(3), $("e"))) = select($("a"), $("c"), $("e")) | 第 m、n、k 列 |
| withColumns(m, n to k)  | select(withColumns(lit(1), range(3, 5))) = select($("a"), $("c"), $("d"), $("e")) |  以上两种用法的混合 |
| withoutColumns(m to n) | select(withoutColumns(range(2, 4))) = select($("a"), $("e")) |  不选从第 m 到第 n 列 |
| withoutColumns(m, n, k) | select(withoutColumns(lit(1), lit(3), lit(5))) = select($("b"), $("d")) |  不选第 m、n、k 列 |
| withoutColumns(m, n to k) | select(withoutColumns(lit(1), range(3, 5))) = select($("b")) |  以上两种用法的混合 |

列函数可用于所有需要列字段的地方，例如 `select、groupBy、orderBy、UDFs` 等函数，例如：

{{< tabs "402fe551-5fb9-4b17-bd64-e05cbd56b4cc" >}}
{{< tab "Java" >}}
```java
table
    .groupBy(withColumns(range(1, 3)))
    .select(withColumns(range("a", "b")), myUDAgg(myUDF(withColumns(range(5, 20)))));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
table
    .groupBy(withColumns(range(1, 3)))
    .select(withColumns('a to 'b), myUDAgg(myUDF(withColumns(5 to 20))))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
table
    .group_by(with_columns(range_(1, 3)))
    .select(with_columns(range_('a', 'b')), myUDAgg(myUDF(with_columns(range_(5, 20)))))
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

Named Arguments
---------------------------------------

By default, values and expressions are mapped to a function's arguments based on the position in the function call,
for example `f(42, true)`. All functions in both SQL and Table API support position-based arguments.

If the function declares a static signature, named arguments are available as a convenient alternative.
The framework is able to reorder named arguments and consider optional arguments accordingly, before passing them
into the function call. Thus, the order of arguments doesn't matter when calling a function and optional arguments
don't have to be provided.

In `DESCRIBE FUNCTION` and documentation a static signature is indicated by the `=>` assignment operator,
for example `f(left => INT, right => BOOLEAN)`. Note that not every function supports named arguments. Named
arguments are not available for signatures that are overloaded, use varargs, or any other kind of input type strategy.
User-defined functions with a single `eval()` method usually qualify for named arguments.

Named arguments can be used as shown below:

{{< tabs "902fe991-5fb9-4b17-ae99-f05cbd48b4dd" >}}
{{< tab "SQL" >}}
```text
SELECT MyUdf(input => my_column, threshold => 42)
```
{{< /tab >}}
{{< tab "Table API" >}}
```java
table.select(
  call(
    MyUdf.class,
    $("my_column").asArgument("input"),
    lit(42).asArgument("threshold")
  )
);
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
