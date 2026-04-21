---
title: "Built-in Functions"
weight: 1
type: docs
aliases:
  - /dev/table/functions/systemFunctions.html
  - /docs/sql/built-in-functions/
  - /dev/python/table-api-users-guide/built_in_functions.html
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

# System (Built-in) Functions

Flink Table API & SQL provides users with a set of built-in functions for data transformations. This page gives a brief overview of them.
If a function that you need is not supported yet, you can implement a [user-defined function]({{< ref "docs/dev/table/functions/udfs" >}}).
If you think that the function is general enough, please <a href="https://issues.apache.org/jira/secure/CreateIssue!default.jspa">open a Jira issue</a> for it with a detailed description.

Scalar Functions
----------------

The scalar functions take zero, one or more values as the input and return a single value as the result.

### Comparison Functions

{{< sql_functions "comparison" >}}

### Logical Functions

{{< sql_functions "logical" >}}

### Arithmetic Functions

{{< sql_functions "arithmetic" >}}

### String Functions

{{< sql_functions "string" >}}

### Temporal Functions

{{< sql_functions "temporal" >}}

### Conditional Functions

{{< sql_functions "conditional" >}}

### Type Conversion Functions

{{< sql_functions "conversion" >}}

### Collection Functions

{{< sql_functions "collection" >}}

### JSON Functions

JSON functions make use of JSON path expressions as described in ISO/IEC TR 19075-6 of the SQL
standard. Their syntax is inspired by and adopts many features of ECMAScript, but is neither a
subset nor superset thereof.

Path expressions come in two flavors, lax and strict. When omitted, it defaults to the strict mode.
Strict mode is intended to examine data from a schema perspective and will throw errors whenever
data does not adhere to the path expression. However, functions like `JSON_VALUE` allow defining
fallback behavior if an error is encountered. Lax mode, on the other hand, is more forgiving and
converts errors to empty sequences.

The special character `$` denotes the root node in a JSON path. Paths can access properties (`$.a`),
array elements (`$.a[0].b`), or branch over all elements in an array (`$.a[*].b`).

Known Limitations:
* Not all features of Lax mode are currently supported correctly. This is an upstream bug
  (CALCITE-4717). Non-standard behavior is not guaranteed.

{{< sql_functions "json" >}}

### Variant Functions

{{< sql_functions "variant" >}}

### Value Construction Functions

{{< sql_functions "valueconstruction" >}}

### Value Access Functions

{{< sql_functions "valueaccess" >}}

### Grouping Functions

{{< sql_functions "grouping" >}}

### Hash Functions

{{< sql_functions "hashfunctions" >}}

### Bitmap Functions

{{< sql_functions "bitmap" >}}

### Auxiliary Functions

{{< sql_functions "auxiliary" >}}

Aggregate Functions
-------------------

The aggregate functions take an expression across all the rows as the input and return a single aggregated value as the result. 

{{< sql_functions "aggregate" >}}

### Bitmap Aggregate Functions

**Performance Tips:**

- It is strongly recommended to enable [MiniBatch aggregation]({{< ref "docs/dev/table/tuning" >}}) or use bitmap aggregate functions within [window aggregations]({{< ref "docs/sql/reference/queries/window-agg" >}}) to optimize state access overhead and significantly improve performance.
- Bitmap aggregate functions perform best with append-only input. Performance degrades noticeably with retraction input, so avoid multi-level GROUP BY aggregations on BITMAP columns when possible.
- For cardinality-only scenarios where the intermediate bitmap is not needed, prefer `BITMAP_XX_CARDINALITY_AGG()` over `BITMAP_CARDINALITY(BITMAP_XX_AGG())`. They are functionally equivalent, but the former avoids materializing the intermediate bitmap and performs better.

{{< sql_functions "bitmapagg" >}}

Time Interval and Point Unit Specifiers
---------------------------------------

The following table lists specifiers for time interval and time point units. 

For Table API, please use `_` for spaces (e.g., `DAY_TO_HOUR`).
Plural works for SQL only. 

| Time Interval Unit       | Time Point Unit                |
|:-------------------------|:-------------------------------|
| `MILLENNIUM`             |                                |
| `CENTURY`                |                                |
| `DECADE`                 |                                |
| `YEAR(S)`                | `YEAR`                         |
| `YEAR(S) TO MONTH(S)`    |                                |
| `QUARTER(S)`             | `QUARTER`                      |
| `MONTH(S)`               | `MONTH`                        |
| `WEEK(S)`                | `WEEK`                         |
| `DAY(S)`                 | `DAY`                          |
| `DAY(S) TO HOUR(S)`      |                                |
| `DAY(S) TO MINUTE(S)`    |                                |
| `DAY(S) TO SECOND(S)`    |                                |
| `HOUR(S)`                | `HOUR`                         |
| `HOUR(S) TO MINUTE(S)`   |                                |
| `HOUR(S) TO SECOND(S)`   |                                |
| `MINUTE(S)`              | `MINUTE`                       |
| `MINUTE(S) TO SECOND(S)` |                                |
| `SECOND(S)`              | `SECOND`                       |
| `MILLISECOND`            | `MILLISECOND`                  |
| `MICROSECOND`            | `MICROSECOND`                  |
| `NANOSECOND`             |                                |
| `EPOCH`                  |                                |
| `DOY` _(SQL-only)_       |                                |
| `DOW` _(SQL-only)_       |                                |
| `EPOCH` _(SQL-only)_     |                                |
| `ISODOW` _(SQL-only)_    |                                |
| `ISOYEAR` _(SQL-only)_   |                                |
|                          | `SQL_TSI_YEAR` _(SQL-only)_    |
|                          | `SQL_TSI_QUARTER` _(SQL-only)_ |
|                          | `SQL_TSI_MONTH` _(SQL-only)_   |
|                          | `SQL_TSI_WEEK` _(SQL-only)_    |
|                          | `SQL_TSI_DAY` _(SQL-only)_     |
|                          | `SQL_TSI_HOUR` _(SQL-only)_    |
|                          | `SQL_TSI_MINUTE` _(SQL-only)_  |
|                          | `SQL_TSI_SECOND ` _(SQL-only)_ |

{{< top >}}

Table Functions
---------------------------------------

Table functions take zero, one, or more scalar values or tables as input arguments and return one or more rows (structured types) as the result.

### APPLY_WATERMARK

**Syntax:**

```sql
APPLY_WATERMARK(
    table_expression,
    DESCRIPTOR(rowtime_column),
    watermark_expression
)
```

**Description:**

`APPLY_WATERMARK` is a built-in table function that enables flexible watermark assignment on tables, views, and subqueries. It allows users to assign or override watermarks without modifying the DDL definition of catalog tables.

**Parameters:**

- `table_expression`: A table reference, view, or subquery that will have a watermark assigned.
- `DESCRIPTOR(rowtime_column)`: The name of the rowtime column (must be of type `TIMESTAMP` or `TIMESTAMP_LTZ`).
- `watermark_expression`: An expression that generates the watermark value based on the rowtime column (must return `TIMESTAMP` or `TIMESTAMP_LTZ`).

**Return Type:**

Returns a table with the same schema as the input table, but with an assigned watermark on the specified rowtime column.

**Behavior:**

- **Validation**: The function validates that the rowtime column exists, is of type `TIMESTAMP` or `TIMESTAMP_LTZ`, and the watermark expression returns a valid timestamp type.
- **Watermark Override**: If the input table already has a watermark defined in its DDL, `APPLY_WATERMARK` will override it with the new watermark strategy.
- **Compile-time Checking**: All validations happen at query compilation time, ensuring early error detection.

**Use Cases:**

1. **Assign Watermark to Catalog Tables** - Useful when you don't have permission to modify the catalog table definition:

```sql
SELECT * FROM APPLY_WATERMARK(
    catalog_table,
    DESCRIPTOR(event_time),
    event_time - INTERVAL '5' SECOND
);
```

2. **Override Watermark on Views** - Apply a different watermark strategy without changing the view definition:

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

3. **Watermark Subqueries** - Assign watermarks to filtered or transformed data:

```sql
SELECT * FROM APPLY_WATERMARK(
    (SELECT * FROM orders WHERE amount > 100),
    DESCRIPTOR(order_time),
    order_time - INTERVAL '3' SECOND
);
```

4. **Dynamic Watermark Strategies** - Adjust watermark delays based on runtime conditions:

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

**Example with Window Aggregation:**

```sql
-- Table without watermark in DDL
CREATE TABLE events (
    id INT,
    event_time TIMESTAMP(3),
    data STRING
);

-- Query with watermark and window
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

**Comparison with DDL Watermarks:**

| Feature | DDL Watermark | `APPLY_WATERMARK` |
|---------|---------------|-------------------|
| **Definition Location** | Table DDL | Query |
| **Flexibility** | Static per table | Dynamic per query |
| **Permission Required** | Catalog write access | None |
| **Applicability** | Base tables only | Tables, views, subqueries |
| **Override Capability** | No | Yes |

**Limitations:**

- The rowtime column must be of type `TIMESTAMP` or `TIMESTAMP_LTZ`.
- The watermark expression must return a `TIMESTAMP` or `TIMESTAMP_LTZ` type.
- The `DESCRIPTOR` keyword is required to specify the column name.
- Nested `APPLY_WATERMARK` calls are not supported.

**See Also:**

- [CREATE TABLE with WATERMARK]({{< ref "docs/sql/reference/ddl/create" >}}#watermark)
- [Time and Watermarks]({{< ref "docs/concepts/time" >}})
- [Window Aggregation]({{< ref "docs/sql/reference/queries/window-agg" >}})

{{< top >}}

Column Functions
---------------------------------------

The column functions are used to select or deselect table columns.

{{< hint info >}}
Column functions are only used in Table API.
{{< /hint >}}

| SYNTAX              | DESC                         |
| :--------------------- | :-------------------------- |
| withColumns(...)         | select the specified columns                  |
| withoutColumns(...)        | deselect the columns specified                  |
| withAllColumns()    | select all columns (like `SELECT *` in SQL) |

The detailed syntax is as follows:

```text
columnFunction:
    withColumns(columnExprs)
    withoutColumns(columnExprs)
    withAllColumns()

columnExprs:
    columnExpr [, columnExpr]*

columnExpr:
    columnRef | columnIndex to columnIndex | columnName to columnName

columnRef:
    columnName(The field name that exists in the table) | columnIndex(a positive integer starting from 1)
```

The usage of the column function is illustrated in the following table. (Suppose we have a table with 5 columns: `(a: Int, b: Long, c: String, d:String, e: String)`):

| API | Usage | Description |
|-|-|-|
| withColumns($(*)) | select(withColumns($("*")))  = select($("a"), $("b"), $("c"), $("d"), $("e")) | all the columns |
| withColumns(m to n) | select(withColumns(range(2, 4))) = select($("b"), $("c"), $("d")) | columns from m to n |
| withColumns(m, n, k)  | select(withColumns(lit(1), lit(3), $("e"))) = select($("a"), $("c"), $("e")) |  columns m, n, k |
| withColumns(m, n to k)  | select(withColumns(lit(1), range(3, 5))) = select($("a"), $("c"), $("d"), $("e")) |  mixing of the above two representation |
| withoutColumns(m to n) | select(withoutColumns(range(2, 4))) = select($("a"), $("e")) |  deselect columns from m to n |
| withoutColumns(m, n, k) | select(withoutColumns(lit(1), lit(3), lit(5))) = select($("b"), $("d")) |  deselect columns m, n, k |
| withoutColumns(m, n to k) | select(withoutColumns(lit(1), range(3, 5))) = select($("b")) |  mixing of the above two representation |

The column functions can be used in all places where column fields are expected, such as `select, groupBy, orderBy, UDFs etc.` e.g.:

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
table \
    .group_by(with_columns(range_(1, 3))) \
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
