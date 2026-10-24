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
| `base64Binary`               | `BYTES`                                      |

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
declare it or binds it to a different namespace. The declarations
`xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"` and `xmlns:xs="http://www.w3.org/2001/XMLSchema"`
are dropped, since `XML_STRING` declares them again where needed. Declarations with other namespaces
are kept, for example as `@xmlns:xsi`.

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

#### VARIANT to XML

The `XML_STRING` function converts a `VARIANT` into an XML string. It reverses the mapping of
`PARSE_XML`, so `PARSE_XML(XML_STRING(v))` gives `v` for any `v` from `PARSE_XML`, with or without
`force_array`, apart from the whitespace described below. The other way around,
`XML_STRING(PARSE_XML(xml))` gives the normalized document, without the XML declaration, comments,
and whitespace between tags, and with the attributes in the order of their keys. For example:

```sql
-- '<book pages="320"><title>Dune</title>This is a description<price>12.50</price>Text in between<price currency="EUR">13.50</price></book>'
XML_STRING(PARSE_XML('
  <book pages="320">
    <title>Dune</title>
    This is a description
    <price>12.50</price>
    Text in between
    <price currency="EUR">13.50</price>
  </book>'))
```

`XML_STRING` writes a `VARIANT` by the following rules:

- `@` fields are written as attributes, `$` fields as text, and the other fields as child elements.
- The content of an element is written in the order of its `#` field. Content without a position in
  the `#` field follows in the order of its keys, so without a `#` field, all content is written in
  key order.
- Each item of an array is written as an element or a text, so an empty array writes nothing.
- An empty string is written as `<a></a>`. A `NULL`, and a `$` field with `NULL`, are written with
  `xsi:nil="true"`, for example `<a xsi:nil="true"/>` and `<a id="1" xsi:nil="true"/>`.
- A scalar other than a string gets the matching `xsi:type` from the table above with the `xs`
  prefix, for example `<a xsi:type="xs:int">5</a>`. A `DECIMAL` gets `xs:decimal`, and a
  `TIMESTAMP_LTZ` gets `xs:dateTime` and is written in UTC, for example `2026-09-30T10:30:00Z`. Like in `PARSE_XML`, only the text of an
  element without child elements is typed.
- `NaN` and infinity are written as `NaN`, `INF`, and `-INF`, like in XML Schema. `PARSE_XML` reads
  them back as strings, like dates and timestamps after the year 9999.
- XML Schema has no type for a `UUID`, so it is written as text without `xsi:type`, and reads back
  as a `STRING`.
- If the result uses the `xsi` or `xs` prefix, the root element declares it as
  `xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"` or `xmlns:xs="http://www.w3.org/2001/XMLSchema"`,
  unless the `VARIANT` declares the prefix on the root element itself, for example with an
  `@xmlns:xs` field.
- `<`, `>`, and `&` are escaped, and so is `"` in attribute values. Other characters are written as
  they are, and XML parsers normalize some of them: tabs and line breaks in attribute values come
  back as spaces, and carriage returns in text come back as line feeds.

`XML_STRING` fails if the `VARIANT` has no XML representation, which is the case if:

- it isn't an object with a single field for the root element, or the root element is an array,
- a key isn't an XML name that `PARSE_XML` accepts, for example `1a` or `a b`, apart from the `@`
  prefix of an attribute,
- a string has a character that XML 1.0 doesn't allow, for example `U+0000`,
- an array is nested in an array,
- an `@` field holds an object, an array, or `NULL`, or a `$` field holds an object,
- a `$` field with `NULL` isn't the only content of its element, or an element has an `@xsi:nil`
  field that is `true` and content,
- a `#` field isn't an object, doesn't have one integer position for each item of a key, or has a
  position more than once,
- an element would need two `xsi:type` or two `xsi:nil` attributes, for example
  `{"a":{"$":5,"@xsi:type":"T"}}`, where the `5` needs `xsi:type="xs:int"` as well.

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
| `FROM_CHANGELOG(input => TABLE t [, ...])` | Converts an append-only table with an explicit operation column into a dynamic table. See [Changelog Conversion]({{< ref "docs/sql/reference/queries/changelog" >}}#from_changelog) for the full list of arguments, semantics, and usage.                                                                       |
| `TO_CHANGELOG(input => TABLE t [, ...])`   | Converts a dynamic table into an append-only table with an explicit operation column. See [Changelog Conversion]({{< ref "docs/sql/reference/queries/changelog" >}}#to_changelog) for the full list of arguments, semantics, and usage.                                                                         |
| `SNAPSHOT(input => TABLE t [, ...])`       | Returns the current state of a dynamic table `t`. `SNAPSHOT` can only be used in a `LATERAL` context and not as a stand-alone table function. See [LATERAL SNAPSHOT join]({{< ref "docs/sql/reference/queries/joins" >}}#lateral-snapshot-join) for the full list of arguments, the join semantics, and usage. |

To implement your own table functions, see [user-defined table functions]({{< ref "docs/dev/table/functions/udfs" >}}#table-functions).

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
