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

表函数
---------------------------------------

表函数接受零个、一个或多个标量值或表作为输入参数,并返回一个或多个行(结构化类型)作为结果。

### APPLY_WATERMARK

**语法:**

```sql
APPLY_WATERMARK(
    表表达式,
    DESCRIPTOR(时间字段名),
    水印表达式
)
```

**描述:**

`APPLY_WATERMARK` 是一个内置表函数,用于在表、视图和子查询上灵活地分配水印。它允许用户在不修改目录表 DDL 定义的情况下分配或覆盖水印。

**参数:**

- `表表达式`: 需要分配水印的表引用、视图或子查询。
- `DESCRIPTOR(时间字段名)`: 时间字段的名称(必须是 `TIMESTAMP` 或 `TIMESTAMP_LTZ` 类型)。
- `水印表达式`: 基于时间字段生成水印值的表达式(必须返回 `TIMESTAMP` 或 `TIMESTAMP_LTZ` 类型)。

**返回类型:**

返回一个与输入表相同 schema 的表,但在指定的时间字段上分配了水印。

**行为:**

- **验证**: 函数会验证时间字段是否存在、类型是否为 `TIMESTAMP` 或 `TIMESTAMP_LTZ`,以及水印表达式是否返回有效的时间戳类型。
- **水印覆盖**: 如果输入表的 DDL 中已经定义了水印,`APPLY_WATERMARK` 将使用新的水印策略覆盖它。
- **编译时检查**: 所有验证都在查询编译时进行,确保尽早发现错误。

**使用场景:**

1. **为目录表分配水印** - 当你没有权限修改目录表定义时很有用:

```sql
SELECT * FROM APPLY_WATERMARK(
    catalog_table,
    DESCRIPTOR(event_time),
    event_time - INTERVAL '5' SECOND
);
```

2. **覆盖视图上的水印** - 在不修改视图定义的情况下应用不同的水印策略:

```sql
CREATE VIEW recent_orders AS
  SELECT * FROM orders
  WHERE order_time > CURRENT_TIMESTAMP - INTERVAL '1' DAY;

SELECT * FROM APPLY_WATERMARK(
    recent_orders,
    DESCRIPTOR(order_time),
    order_time - INTERVAL '10' SECOND
);
```

3. **为子查询分配水印** - 为经过过滤或转换的数据分配水印:

```sql
SELECT * FROM APPLY_WATERMARK(
    (SELECT * FROM orders WHERE amount > 100),
    DESCRIPTOR(order_time),
    order_time - INTERVAL '3' SECOND
);
```

4. **动态水印策略** - 根据运行时条件调整水印延迟:

```sql
SELECT * FROM APPLY_WATERMARK(
    orders,
    DESCRIPTOR(order_time),
    CASE
        WHEN order_source = 'mobile' THEN order_time - INTERVAL '15' SECOND
        WHEN order_source = 'web' THEN order_time - INTERVAL '5' SECOND
        ELSE order_time - INTERVAL '10' SECOND
    END
);
```

**窗口聚合示例:**

```sql
-- DDL 中没有水印的表
CREATE TABLE events (
    id INT,
    event_time TIMESTAMP(3),
    data STRING
);

-- 带水印和窗口的查询
SELECT
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    COUNT(*) AS cnt
FROM APPLY_WATERMARK(
    events,
    DESCRIPTOR(event_time),
    event_time - INTERVAL '5' SECOND
)
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE);
```

**与 DDL 水印的对比:**

| 特性 | DDL 水印 | `APPLY_WATERMARK` |
|------|----------|-------------------|
| **定义位置** | 表 DDL | 查询 |
| **灵活性** | 每个表静态 | 每个查询动态 |
| **所需权限** | 目录写权限 | 无 |
| **适用范围** | 仅基表 | 表、视图、子查询 |
| **覆盖能力** | 否 | 是 |

**限制:**

- 时间字段必须是 `TIMESTAMP` 或 `TIMESTAMP_LTZ` 类型。
- 水印表达式必须返回 `TIMESTAMP` 或 `TIMESTAMP_LTZ` 类型。
- 必须使用 `DESCRIPTOR` 关键字指定列名。
- 不支持嵌套的 `APPLY_WATERMARK` 调用。

**另请参阅:**

- [CREATE TABLE 与水印]({{< ref "docs/sql/reference/ddl/create" >}}#watermark)
- [时间和水印]({{< ref "docs/concepts/time" >}})
- [窗口聚合]({{< ref "docs/sql/reference/queries/window-agg" >}})

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
