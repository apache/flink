---
title: "Data Types"
nav-parent_id: tableapi
nav-pos: 20
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

Due to historical reasons, before Flink 1.9, Flink's Table & SQL API data types were
tightly coupled to Flink's `TypeInformation`. `TypeInformation` is used in the DataStream
and DataSet API and is sufficient to describe all information needed to serialize and
deserialize JVM-based objects in a distributed setting.

However, `TypeInformation` was not designed to represent logical types independent of
an actual JVM class. In the past, it was difficult to map SQL standard types to this
abstraction. Furthermore, some types were not SQL-compliant and introduced without a
bigger picture in mind.

Starting with Flink 1.9, the Table & SQL API will receive a new type system that serves as a long-term
solution for API stability and standard compliance.

Reworking the type system is a major effort that touches almost all user-facing interfaces. Therefore, its
introduction spans multiple releases, and the community aims to finish this effort by Flink 1.12.

Due to the simultaneous addition of a new planner for table programs (see [FLINK-11439](https://issues.apache.org/jira/browse/FLINK-11439)),
not every combination of planner and data type is supported. Furthermore, planners might not support every
data type with the desired precision or parameter.

<span class="label label-danger">Attention</span> Please see the planner compatibility table and limitations
section before using a data type.

* This will be replaced by the TOC
{:toc}

Data Type
---------

A *data type* describes the logical type of a value in the table ecosystem. It can be used to declare input and/or
output types of operations.

Flink's data types are similar to the SQL standard's *data type* terminology but also contain information
about the nullability of a value for efficient handling of scalar expressions.

Examples of data types are:
- `INT`
- `INT NOT NULL`
- `INTERVAL DAY TO SECOND(3)`
- `ROW<myField ARRAY<BOOLEAN>, myOtherField TIMESTAMP(3)>`

A list of all pre-defined data types can be found [below](#list-of-data-types).

### Data Types in the Table API

Users of the JVM-based API work with instances of `org.apache.flink.table.types.DataType` within the Table API or when
defining connectors, catalogs, or user-defined functions.

A `DataType` instance has two responsibilities:
- **Declaration of a logical type** which does not imply a concrete physical representation for transmission
or storage but defines the boundaries between JVM-based languages and the table ecosystem.
- *Optional:* **Giving hints about the physical representation of data to the planner** which is useful at the edges to other APIs .

For JVM-based languages, all pre-defined data types are available in `org.apache.flink.table.api.DataTypes`.

It is recommended to add a star import to your table programs for having a fluent API:

<div class="codetabs" markdown="1">

<div data-lang="Java" markdown="1">
{% highlight java %}
import static org.apache.flink.table.api.DataTypes.*;

DataType t = INTERVAL(DAY(), SECOND(3));
{% endhighlight %}
</div>

<div data-lang="Scala" markdown="1">
{% highlight scala %}
import org.apache.flink.table.api.DataTypes._

val t: DataType = INTERVAL(DAY(), SECOND(3));
{% endhighlight %}
</div>

</div>

#### Physical Hints

Physical hints are required at the edges of the table ecosystem where the SQL-based type system ends and
programming-specific data types are required. Hints indicate the data format that an implementation
expects.

For example, a data source could express that it produces values for logical `TIMESTAMP`s using a `java.sql.Timestamp` class
instead of using `java.time.LocalDateTime` which would be the default. With this information, the runtime is able to convert
the produced class into its internal data format. In return, a data sink can declare the data format it consumes from the runtime.

Here are some examples of how to declare a bridging conversion class:

<div class="codetabs" markdown="1">

<div data-lang="Java" markdown="1">
{% highlight java %}
// tell the runtime to not produce or consume java.time.LocalDateTime instances
// but java.sql.Timestamp
DataType t = DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class);

// tell the runtime to not produce or consume boxed integer arrays
// but primitive int arrays
DataType t = DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(int[].class);
{% endhighlight %}
</div>

<div data-lang="Scala" markdown="1">
{% highlight scala %}
// tell the runtime to not produce or consume java.time.LocalDateTime instances
// but java.sql.Timestamp
val t: DataType = DataTypes.TIMESTAMP(3).bridgedTo(classOf[java.sql.Timestamp]);

// tell the runtime to not produce or consume boxed integer arrays
// but primitive int arrays
val t: DataType = DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(classOf[Array[Int]]);
{% endhighlight %}
</div>

</div>

<span class="label label-danger">Attention</span> Please note that physical hints are usually only required if the
API is extended. Users of predefined sources/sinks/functions do not need to define such hints. Hints within
a table program (e.g. `field.cast(TIMESTAMP(3).bridgedTo(Timestamp.class))`) are ignored.

Planner Compatibility
---------------------

As mentioned in the introduction, reworking the type system will span multiple releases, and the support of each data
type depends on the used planner. This section aims to summarize the most significant differences.

### Old Planner

Flink's old planner, introduced before Flink 1.9, primarily supports type information. It has only limited
support for data types. It is possible to declare data types that can be translated into type information such that the
old planner understands them.

The following table summarizes the difference between data type and type information. Most simple types, as well as the
row type remain the same. Time types, array types, and the decimal type need special attention. Other hints as the ones
mentioned are not allowed.

For the *Type Information* column the table omits the prefix `org.apache.flink.table.api.Types`.

For the *Data Type Representation* column the table omits the prefix `org.apache.flink.table.api.DataTypes`.

| Type Information | Java Expression String | Data Type Representation | Remarks for Data Type |
|:-----------------|:-----------------------|:-------------------------|:----------------------|
| `STRING()` | `STRING` | `STRING()` | |
| `BOOLEAN()` | `BOOLEAN` | `BOOLEAN()` | |
| `BYTE()` | `BYTE` | `TINYINT()` | |
| `SHORT()` | `SHORT` | `SMALLINT()` | |
| `INT()` | `INT` | `INT()` | |
| `LONG()` | `LONG` | `BIGINT()` | |
| `FLOAT()` | `FLOAT` | `FLOAT()` | |
| `DOUBLE()` | `DOUBLE` | `DOUBLE()` | |
| `ROW(...)` | `ROW<...>` | `ROW(...)` | |
| `BIG_DEC()` | `DECIMAL` | [`DECIMAL()`] | Not a 1:1 mapping as precision and scale are ignored and Java's variable precision and scale are used. |
| `SQL_DATE()` | `SQL_DATE` | `DATE()`<br>`.bridgedTo(java.sql.Date.class)` | |
| `SQL_TIME()` | `SQL_TIME` | `TIME(0)`<br>`.bridgedTo(java.sql.Time.class)` | |
| `SQL_TIMESTAMP()` | `SQL_TIMESTAMP` | `TIMESTAMP(3)`<br>`.bridgedTo(java.sql.Timestamp.class)` | |
| `INTERVAL_MONTHS()` | `INTERVAL_MONTHS` | `INTERVAL(MONTH())`<br>`.bridgedTo(Integer.class)` | |
| `INTERVAL_MILLIS()` | `INTERVAL_MILLIS` | `INTERVAL(DataTypes.SECOND(3))`<br>`.bridgedTo(Long.class)` | |
| `PRIMITIVE_ARRAY(...)` | `PRIMITIVE_ARRAY<...>` | `ARRAY(DATATYPE.notNull()`<br>`.bridgedTo(PRIMITIVE.class))` | Applies to all JVM primitive types except for `byte`. |
| `PRIMITIVE_ARRAY(BYTE())` | `PRIMITIVE_ARRAY<BYTE>` | `BYTES()` | |
| `OBJECT_ARRAY(...)` | `OBJECT_ARRAY<...>` | `ARRAY(`<br>`DATATYPE.bridgedTo(OBJECT.class))` | |
| `MULTISET(...)` | | `MULTISET(...)` | |
| `MAP(..., ...)` | `MAP<...,...>` | `MAP(...)` | |
| other generic types | | `RAW(...)` | |

<span class="label label-danger">Attention</span> If there is a problem with the new type system. Users
can fallback to type information defined in `org.apache.flink.table.api.Types` at any time.

### New Blink Planner

The new Blink planner supports all of types of the old planner. This includes in particular
the listed Java expression strings and type information.

The following data types are supported:

| Data Type | Remarks for Data Type |
|:----------|:----------------------|
| `STRING` | `CHAR` and `VARCHAR` are not supported yet. |
| `BOOLEAN` | |
| `BYTES` | `BINARY` and `VARBINARY` are not supported yet. |
| `DECIMAL` | Supports fixed precision and scale. |
| `TINYINT` | |
| `SMALLINT` | |
| `INTEGER` | |
| `BIGINT` | |
| `FLOAT` | |
| `DOUBLE` | |
| `DATE` | |
| `TIME` | Supports only a precision of `0`. |
| `TIMESTAMP` | |
| `TIMESTAMP WITH LOCAL TIME ZONE` | |
| `INTERVAL` | Supports only interval of `MONTH` and `SECOND(3)`. |
| `ARRAY` | |
| `MULTISET` | |
| `MAP` | |
| `ROW` | |
| `RAW` | |
| stuctured types | Only exposed in user-defined functions yet. |

Limitations
-----------

**Java Expression String**: Java expression strings in the Table API such as `table.select("field.cast(STRING)")`
have not been updated to the new type system yet. Use the string representations declared in
the [old planner section](#old-planner).

**User-defined Functions**: User-defined aggregate functions cannot declare a data type yet. Scalar and table functions fully support data types.

List of Data Types
------------------

This section lists all pre-defined data types. For the JVM-based Table API those types are also available in `org.apache.flink.table.api.DataTypes`.

### Character Strings

#### `CHAR`

Data type of a fixed-length character string.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
CHAR
CHAR(n)
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.CHAR(n)
{% endhighlight %}
</div>

</div>

The type can be declared using `CHAR(n)` where `n` is the number of code points. `n` must have a value between `1`
and `2,147,483,647` (both inclusive). If no length is specified, `n` is equal to `1`.

**Bridging to JVM Types**

| Java Type                               | Input | Output | Remarks                  |
|:----------------------------------------|:-----:|:------:|:-------------------------|
|`java.lang.String`                       | X     | X      | *Default*                |
|`byte[]`                                 | X     | X      | Assumes UTF-8 encoding.  |
|`org.apache.flink.table.data.StringData` | X     | X      | Internal data structure. |

#### `VARCHAR` / `STRING`

Data type of a variable-length character string.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
VARCHAR
VARCHAR(n)

STRING
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.VARCHAR(n)

DataTypes.STRING()
{% endhighlight %}
</div>

</div>

The type can be declared using `VARCHAR(n)` where `n` is the maximum number of code points. `n` must have a value
between `1` and `2,147,483,647` (both inclusive). If no length is specified, `n` is equal to `1`.

`STRING` is a synonym for `VARCHAR(2147483647)`.

**Bridging to JVM Types**

| Java Type                               | Input | Output | Remarks                  |
|:----------------------------------------|:-----:|:------:|:-------------------------|
|`java.lang.String`                       | X     | X      | *Default*                |
|`byte[]`                                 | X     | X      | Assumes UTF-8 encoding.  |
|`org.apache.flink.table.data.StringData` | X     | X      | Internal data structure. |

### Binary Strings

#### `BINARY`

Data type of a fixed-length binary string (=a sequence of bytes).

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
BINARY
BINARY(n)
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.BINARY(n)
{% endhighlight %}
</div>

</div>

The type can be declared using `BINARY(n)` where `n` is the number of bytes. `n` must have a value
between `1` and `2,147,483,647` (both inclusive). If no length is specified, `n` is equal to `1`.

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                 |
|:-------------------|:-----:|:------:|:------------------------|
|`byte[]`            | X     | X      | *Default*               |

#### `VARBINARY` / `BYTES`

Data type of a variable-length binary string (=a sequence of bytes).

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
VARBINARY
VARBINARY(n)

BYTES
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.VARBINARY(n)

DataTypes.BYTES()
{% endhighlight %}
</div>

</div>

The type can be declared using `VARBINARY(n)` where `n` is the maximum number of bytes. `n` must
have a value between `1` and `2,147,483,647` (both inclusive). If no length is specified, `n` is
equal to `1`.

`BYTES` is a synonym for `VARBINARY(2147483647)`.

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                 |
|:-------------------|:-----:|:------:|:------------------------|
|`byte[]`            | X     | X      | *Default*               |

### Exact Numerics

#### `DECIMAL`

Data type of a decimal number with fixed precision and scale.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
DECIMAL
DECIMAL(p)
DECIMAL(p, s)

DEC
DEC(p)
DEC(p, s)

NUMERIC
NUMERIC(p)
NUMERIC(p, s)
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.DECIMAL(p, s)
{% endhighlight %}
</div>

</div>

The type can be declared using `DECIMAL(p, s)` where `p` is the number of digits in a
number (*precision*) and `s` is the number of digits to the right of the decimal point
in a number (*scale*). `p` must have a value between `1` and `38` (both inclusive). `s`
must have a value between `0` and `p` (both inclusive). The default value for `p` is 10.
The default value for `s` is `0`.

`NUMERIC(p, s)` and `DEC(p, s)` are synonyms for this type.

**Bridging to JVM Types**

| Java Type                                | Input | Output | Remarks                  |
|:-----------------------------------------|:-----:|:------:|:-------------------------|
|`java.math.BigDecimal`                    | X     | X      | *Default*                |
|`org.apache.flink.table.data.DecimalData` | X     | X      | Internal data structure. |

#### `TINYINT`

Data type of a 1-byte signed integer with values from `-128` to `127`.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
TINYINT
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.TINYINT()
{% endhighlight %}
</div>

</div>

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Byte`    | X     | X      | *Default*                                    |
|`byte`              | X     | (X)    | Output only if type is not nullable.         |

#### `SMALLINT`

Data type of a 2-byte signed integer with values from `-32,768` to `32,767`.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
SMALLINT
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.SMALLINT()
{% endhighlight %}
</div>

</div>

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Short`   | X     | X      | *Default*                                    |
|`short`             | X     | (X)    | Output only if type is not nullable.         |

#### `INT`

Data type of a 4-byte signed integer with values from `-2,147,483,648` to `2,147,483,647`.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
INT

INTEGER
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.INT()
{% endhighlight %}
</div>

</div>

`INTEGER` is a synonym for this type.

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Integer` | X     | X      | *Default*                                    |
|`int`               | X     | (X)    | Output only if type is not nullable.         |

#### `BIGINT`

Data type of an 8-byte signed integer with values from `-9,223,372,036,854,775,808` to
`9,223,372,036,854,775,807`.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
BIGINT
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.BIGINT()
{% endhighlight %}
</div>

</div>

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Long`    | X     | X      | *Default*                                    |
|`long`              | X     | (X)    | Output only if type is not nullable.         |

### Approximate Numerics

#### `FLOAT`

Data type of a 4-byte single precision floating point number.

Compared to the SQL standard, the type does not take parameters.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
FLOAT
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.FLOAT()
{% endhighlight %}
</div>

</div>

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Float`   | X     | X      | *Default*                                    |
|`float`             | X     | (X)    | Output only if type is not nullable.         |

#### `DOUBLE`

Data type of an 8-byte double precision floating point number.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
DOUBLE

DOUBLE PRECISION
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.DOUBLE()
{% endhighlight %}
</div>

</div>

`DOUBLE PRECISION` is a synonym for this type.

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Double`  | X     | X      | *Default*                                    |
|`double`            | X     | (X)    | Output only if type is not nullable.         |

### Date and Time

#### `DATE`

Data type of a date consisting of `year-month-day` with values ranging from `0000-01-01`
to `9999-12-31`.

Compared to the SQL standard, the range starts at year `0000`.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
DATE
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.DATE()
{% endhighlight %}
</div>

</div>

**Bridging to JVM Types**

| Java Type            | Input | Output | Remarks                                      |
|:---------------------|:-----:|:------:|:---------------------------------------------|
|`java.time.LocalDate` | X     | X      | *Default*                                    |
|`java.sql.Date`       | X     | X      |                                              |
|`java.lang.Integer`   | X     | X      | Describes the number of days since epoch.    |
|`int`                 | X     | (X)    | Describes the number of days since epoch.<br>Output only if type is not nullable. |

#### `TIME`

Data type of a time *without* time zone consisting of `hour:minute:second[.fractional]` with
up to nanosecond precision and values ranging from `00:00:00.000000000` to
`23:59:59.999999999`.

Compared to the SQL standard, leap seconds (`23:59:60` and `23:59:61`) are not supported as
the semantics are closer to `java.time.LocalTime`. A time *with* time zone is not provided.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
TIME
TIME(p)
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.TIME(p)
{% endhighlight %}
</div>

</div>

The type can be declared using `TIME(p)` where `p` is the number of digits of fractional
seconds (*precision*). `p` must have a value between `0` and `9` (both inclusive). If no
precision is specified, `p` is equal to `0`.

**Bridging to JVM Types**

| Java Type            | Input | Output | Remarks                                             |
|:---------------------|:-----:|:------:|:----------------------------------------------------|
|`java.time.LocalTime` | X     | X      | *Default*                                           |
|`java.sql.Time`       | X     | X      |                                                     |
|`java.lang.Integer`   | X     | X      | Describes the number of milliseconds of the day.    |
|`int`                 | X     | (X)    | Describes the number of milliseconds of the day.<br>Output only if type is not nullable. |
|`java.lang.Long`      | X     | X      | Describes the number of nanoseconds of the day.     |
|`long`                | X     | (X)    | Describes the number of nanoseconds of the day.<br>Output only if type is not nullable. |

#### `TIMESTAMP`

Data type of a timestamp *without* time zone consisting of `year-month-day hour:minute:second[.fractional]`
with up to nanosecond precision and values ranging from `0000-01-01 00:00:00.000000000` to
`9999-12-31 23:59:59.999999999`.

Compared to the SQL standard, leap seconds (`23:59:60` and `23:59:61`) are not supported as
the semantics are closer to `java.time.LocalDateTime`.

A conversion from and to `BIGINT` (a JVM `long` type) is not supported as this would imply a time
zone. However, this type is time zone free. For more `java.time.Instant`-like semantics use
`TIMESTAMP WITH LOCAL TIME ZONE`.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
TIMESTAMP
TIMESTAMP(p)

TIMESTAMP WITHOUT TIME ZONE
TIMESTAMP(p) WITHOUT TIME ZONE
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.TIMESTAMP(p)
{% endhighlight %}
</div>

</div>

The type can be declared using `TIMESTAMP(p)` where `p` is the number of digits of fractional
seconds (*precision*). `p` must have a value between `0` and `9` (both inclusive). If no precision
is specified, `p` is equal to `6`.

`TIMESTAMP(p) WITHOUT TIME ZONE` is a synonym for this type.

**Bridging to JVM Types**

| Java Type                                  | Input | Output | Remarks                  |
|:-------------------------------------------|:-----:|:------:|:-------------------------|
|`java.time.LocalDateTime`                   | X     | X      | *Default*                |
|`java.sql.Timestamp`                        | X     | X      |                          |
|`org.apache.flink.table.data.TimestampData` | X     | X      | Internal data structure. |

#### `TIMESTAMP WITH TIME ZONE`

Data type of a timestamp *with* time zone consisting of `year-month-day hour:minute:second[.fractional] zone`
with up to nanosecond precision and values ranging from `0000-01-01 00:00:00.000000000 +14:59` to
`9999-12-31 23:59:59.999999999 -14:59`.

Compared to the SQL standard, leap seconds (`23:59:60` and `23:59:61`) are not supported as the semantics
are closer to `java.time.OffsetDateTime`.

Compared to `TIMESTAMP WITH LOCAL TIME ZONE`, the time zone offset information is physically
stored in every datum. It is used individually for every computation, visualization, or communication
to external systems.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
TIMESTAMP WITH TIME ZONE
TIMESTAMP(p) WITH TIME ZONE
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.TIMESTAMP_WITH_TIME_ZONE(p)
{% endhighlight %}
</div>

</div>

The type can be declared using `TIMESTAMP(p) WITH TIME ZONE` where `p` is the number of digits of
fractional seconds (*precision*). `p` must have a value between `0` and `9` (both inclusive). If no
precision is specified, `p` is equal to `6`.

**Bridging to JVM Types**

| Java Type                 | Input | Output | Remarks              |
|:--------------------------|:-----:|:------:|:---------------------|
|`java.time.OffsetDateTime` | X     | X      | *Default*            |
|`java.time.ZonedDateTime`  | X     |        | Ignores the zone ID. |

#### `TIMESTAMP WITH LOCAL TIME ZONE`

Data type of a timestamp *with local* time zone consisting of `year-month-day hour:minute:second[.fractional] zone`
with up to nanosecond precision and values ranging from `0000-01-01 00:00:00.000000000 +14:59` to
`9999-12-31 23:59:59.999999999 -14:59`.

Leap seconds (`23:59:60` and `23:59:61`) are not supported as the semantics are closer to `java.time.OffsetDateTime`.

Compared to `TIMESTAMP WITH TIME ZONE`, the time zone offset information is not stored physically
in every datum. Instead, the type assumes `java.time.Instant` semantics in UTC time zone at
the edges of the table ecosystem. Every datum is interpreted in the local time zone configured in
the current session for computation and visualization.

This type fills the gap between time zone free and time zone mandatory timestamp types by allowing
the interpretation of UTC timestamps according to the configured session time zone.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
TIMESTAMP WITH LOCAL TIME ZONE
TIMESTAMP(p) WITH LOCAL TIME ZONE
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(p)
{% endhighlight %}
</div>

</div>

The type can be declared using `TIMESTAMP(p) WITH LOCAL TIME ZONE` where `p` is the number
of digits of fractional seconds (*precision*). `p` must have a value between `0` and `9`
(both inclusive). If no precision is specified, `p` is equal to `6`.

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                                           |
|:-------------------|:-----:|:------:|:--------------------------------------------------|
|`java.time.Instant` | X     | X      | *Default*                                         |
|`java.lang.Integer` | X     | X      | Describes the number of seconds since epoch.      |
|`int`               | X     | (X)    | Describes the number of seconds since epoch.<br>Output only if type is not nullable. |
|`java.lang.Long`    | X     | X      | Describes the number of milliseconds since epoch. |
|`long`              | X     | (X)    | Describes the number of milliseconds since epoch.<br>Output only if type is not nullable. |
|`org.apache.flink.table.data.TimestampData` | X     | X      | Internal data structure. |

#### `INTERVAL YEAR TO MONTH`

Data type for a group of year-month interval types.

The type must be parameterized to one of the following resolutions:
- interval of years,
- interval of years to months,
- or interval of months.

An interval of year-month consists of `+years-months` with values ranging from `-9999-11` to
`+9999-11`.

The value representation is the same for all types of resolutions. For example, an interval
of months of 50 is always represented in an interval-of-years-to-months format (with default
year precision): `+04-02`.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
INTERVAL YEAR
INTERVAL YEAR(p)
INTERVAL YEAR(p) TO MONTH
INTERVAL MONTH
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.INTERVAL(DataTypes.YEAR())
DataTypes.INTERVAL(DataTypes.YEAR(p))
DataTypes.INTERVAL(DataTypes.YEAR(p), DataTypes.MONTH())
DataTypes.INTERVAL(DataTypes.MONTH())
{% endhighlight %}
</div>

</div>

The type can be declared using the above combinations where `p` is the number of digits of years
(*year precision*). `p` must have a value between `1` and `4` (both inclusive). If no year precision
is specified, `p` is equal to `2`.

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                            |
|:-------------------|:-----:|:------:|:-----------------------------------|
|`java.time.Period`  | X     | X      | Ignores the `days` part. *Default* |
|`java.lang.Integer` | X     | X      | Describes the number of months.    |
|`int`               | X     | (X)    | Describes the number of months.<br>Output only if type is not nullable. |

#### `INTERVAL DAY TO MONTH`

Data type for a group of day-time interval types.

The type must be parameterized to one of the following resolutions with up to nanosecond precision:
- interval of days,
- interval of days to hours,
- interval of days to minutes,
- interval of days to seconds,
- interval of hours,
- interval of hours to minutes,
- interval of hours to seconds,
- interval of minutes,
- interval of minutes to seconds,
- or interval of seconds.

An interval of day-time consists of `+days hours:months:seconds.fractional` with values ranging from
`-999999 23:59:59.999999999` to `+999999 23:59:59.999999999`. The value representation is the same
for all types of resolutions. For example, an interval of seconds of 70 is always represented in
an interval-of-days-to-seconds format (with default precisions): `+00 00:01:10.000000`.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
INTERVAL DAY
INTERVAL DAY(p1)
INTERVAL DAY(p1) TO HOUR
INTERVAL DAY(p1) TO MINUTE
INTERVAL DAY(p1) TO SECOND(p2)
INTERVAL HOUR
INTERVAL HOUR TO MINUTE
INTERVAL HOUR TO SECOND(p2)
INTERVAL MINUTE
INTERVAL MINUTE TO SECOND(p2)
INTERVAL SECOND
INTERVAL SECOND(p2)
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.INTERVAL(DataTypes.DAY())
DataTypes.INTERVAL(DataTypes.DAY(p1))
DataTypes.INTERVAL(DataTypes.DAY(p1), DataTypes.HOUR())
DataTypes.INTERVAL(DataTypes.DAY(p1), DataTypes.MINUTE())
DataTypes.INTERVAL(DataTypes.DAY(p1), DataTypes.SECOND(p2))
DataTypes.INTERVAL(DataTypes.HOUR())
DataTypes.INTERVAL(DataTypes.HOUR(), DataTypes.MINUTE())
DataTypes.INTERVAL(DataTypes.HOUR(), DataTypes.SECOND(p2))
DataTypes.INTERVAL(DataTypes.MINUTE())
DataTypes.INTERVAL(DataTypes.MINUTE(), DataTypes.SECOND(p2))
DataTypes.INTERVAL(DataTypes.SECOND())
DataTypes.INTERVAL(DataTypes.SECOND(p2))
{% endhighlight %}
</div>

</div>

The type can be declared using the above combinations where `p1` is the number of digits of days
(*day precision*) and `p2` is the number of digits of fractional seconds (*fractional precision*).
`p1` must have a value between `1` and `6` (both inclusive). `p2` must have a value between `0`
and `9` (both inclusive). If no `p1` is specified, it is equal to `2` by default. If no `p2` is
specified, it is equal to `6` by default.

**Bridging to JVM Types**

| Java Type           | Input | Output | Remarks                               |
|:--------------------|:-----:|:------:|:--------------------------------------|
|`java.time.Duration` | X     | X      | *Default*                             |
|`java.lang.Long`     | X     | X      | Describes the number of milliseconds. |
|`long`               | X     | (X)    | Describes the number of milliseconds.<br>Output only if type is not nullable. |

### Constructured Data Types

#### `ARRAY`

Data type of an array of elements with same subtype.

Compared to the SQL standard, the maximum cardinality of an array cannot be specified but is
fixed at `2,147,483,647`. Also, any valid type is supported as a subtype.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
ARRAY<t>
t ARRAY
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.ARRAY(t)
{% endhighlight %}
</div>

</div>

The type can be declared using `ARRAY<t>` where `t` is the data type of the contained
elements.

`t ARRAY` is a synonym for being closer to the SQL standard. For example, `INT ARRAY` is
equivalent to `ARRAY<INT>`.

**Bridging to JVM Types**

| Java Type                              | Input | Output | Remarks                           |
|:---------------------------------------|:-----:|:------:|:----------------------------------|
|*t*`[]`                                 | (X)   | (X)    | Depends on the subtype. *Default* |
| `java.util.List<t>`                    | X     | X      |                                   |
| *subclass* of `java.util.List<t>`      | X     |        |                                   |
|`org.apache.flink.table.data.ArrayData` | X     | X      | Internal data structure.          |

#### `MAP`

Data type of an associative array that maps keys (including `NULL`) to values (including `NULL`). A map
cannot contain duplicate keys; each key can map to at most one value.

There is no restriction of element types; it is the responsibility of the user to ensure uniqueness.

The map type is an extension to the SQL standard.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
MAP<kt, vt>
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.MAP(kt, vt)
{% endhighlight %}
</div>

</div>

The type can be declared using `MAP<kt, vt>` where `kt` is the data type of the key elements
and `vt` is the data type of the value elements.

**Bridging to JVM Types**

| Java Type                             | Input | Output | Remarks                  |
|:--------------------------------------|:-----:|:------:|:-------------------------|
| `java.util.Map<kt, vt>`               | X     | X      | *Default*                |
| *subclass* of `java.util.Map<kt, vt>` | X     |        |                          |
|`org.apache.flink.table.data.MapData`  | X     | X      | Internal data structure. |

#### `MULTISET`

Data type of a multiset (=bag). Unlike a set, it allows for multiple instances for each of its
elements with a common subtype. Each unique value (including `NULL`) is mapped to some multiplicity.

There is no restriction of element types; it is the responsibility of the user to ensure uniqueness.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
MULTISET<t>
t MULTISET
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.MULTISET(t)
{% endhighlight %}
</div>

</div>

The type can be declared using `MULTISET<t>` where `t` is the data type
of the contained elements.

`t MULTISET` is a synonym for being closer to the SQL standard. For example, `INT MULTISET` is
equivalent to `MULTISET<INT>`.

**Bridging to JVM Types**

| Java Type                             | Input | Output | Remarks                                                  |
|:--------------------------------------|:-----:|:------:|:---------------------------------------------------------|
|`java.util.Map<t, java.lang.Integer>`  | X     | X      | Assigns each value to an integer multiplicity. *Default* |
| *subclass* of `java.util.Map<t, java.lang.Integer>>` | X     |        |                                           |
|`org.apache.flink.table.data.MapData`  | X     | X      | Internal data structure.                                 |

#### `ROW`

Data type of a sequence of fields.

A field consists of a field name, field type, and an optional description. The most specific type
of a row of a table is a row type. In this case, each column of the row corresponds to the field
of the row type that has the same ordinal position as the column.

Compared to the SQL standard, an optional field description simplifies the handling with complex
structures.

A row type is similar to the `STRUCT` type known from other non-standard-compliant frameworks.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight sql %}
ROW<n0 t0, n1 t1, ...>
ROW<n0 t0 'd0', n1 t1 'd1', ...>

ROW(n0 t0, n1 t1, ...>
ROW(n0 t0 'd0', n1 t1 'd1', ...)
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.ROW(DataTypes.FIELD(n0, t0), DataTypes.FIELD(n1, t1), ...)
DataTypes.ROW(DataTypes.FIELD(n0, t0, d0), DataTypes.FIELD(n1, t1, d1), ...)
{% endhighlight %}
</div>

</div>

The type can be declared using `ROW<n0 t0 'd0', n1 t1 'd1', ...>` where `n` is the unique name of
a field, `t` is the logical type of a field, `d` is the description of a field.

`ROW(...)` is a synonym for being closer to the SQL standard. For example, `ROW(myField INT, myOtherField BOOLEAN)` is
equivalent to `ROW<myField INT, myOtherField BOOLEAN>`.

**Bridging to JVM Types**

| Java Type                            | Input | Output | Remarks                  |
|:-------------------------------------|:-----:|:------:|:-------------------------|
|`org.apache.flink.types.Row`          | X     | X      | *Default*                |
|`org.apache.flink.table.data.RowData` | X     | X      | Internal data structure. |

### User-Defined Data Types

<span class="label label-danger">Attention</span> User-defined data types are not fully supported yet. They are
currently (as of Flink 1.11) only exposed as unregistered structured types in parameters and return types of functions.

A structured type is similar to an object in an object-oriented programming language. It contains
zero, one or more attributes. Each attribute consists of a name and a type.

There are two kinds of structured types:

- Types that are stored in a catalog and are identified by a _catalog identifer_ (like `cat.db.MyType`). Those
are equal to the SQL standard definition of structured types.

- Anonymously defined, unregistered types (usually reflectively extracted) that are identified by
an _implementation class_ (like `com.myorg.model.MyType`). Those are useful when programmatically
defining a table program. They enable reusing existing JVM classes without manually defining the
schema of a data type again.

#### Registered Structured Types

Currently, registered structured types are not supported. Thus, they cannot be stored in a catalog
or referenced in a `CREATE TABLE` DDL.

#### Unregistered Structured Types

Unregistered structured types can be created from regular POJOs (Plain Old Java Objects) using automatic reflective extraction.

The implementation class of a structured type must meet the following requirements:
- The class must be globally accessible which means it must be declared `public`, `static`, and not `abstract`.
- The class must offer a default constructor with zero arguments or a full constructor that assigns all
fields.
- All fields of the class must be readable by either `public` declaration or a getter that follows common
coding style such as `getField()`, `isField()`, `field()`.
- All fields of the class must be writable by either `public` declaration, fully assigning constructor,
or a setter that follows common coding style such as `setField(...)`, `field(...)`.
- All fields must be mapped to a data type either implicitly via reflective extraction or explicitly
using the `@DataTypeHint` [annotations](#data-type-annotations).
- Fields that are declared `static` or `transient` are ignored.

The reflective extraction supports arbitrary nesting of fields as long as a field type does not
(transitively) refer to itself.

The declared field class (e.g. `public int age;`) must be contained in the list of supported JVM
bridging classes defined for every data type in this document (e.g. `java.lang.Integer` or `int` for `INT`).

For some classes an annotation is required in order to map the class to a data type (e.g. `@DataTypeHint("DECIMAL(10, 2)")`
to assign a fixed precision and scale for `java.math.BigDecimal`).

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="Java" markdown="1">
{% highlight java %}
class User {

    // extract fields automatically
    public int age;
    public String name;

    // enrich the extraction with precision information
    public @DataTypeHint("DECIMAL(10, 2)") BigDecimal totalBalance;

    // enrich the extraction with forcing using RAW types
    public @DataTypeHint("RAW") Class<?> modelClass;
}

DataTypes.of(User.class);
{% endhighlight %}
</div>

<div data-lang="Scala" markdown="1">
{% highlight scala %}
case class User(

    // extract fields automatically
    age: Int,
    name: String,

    // enrich the extraction with precision information
    @DataTypeHint("DECIMAL(10, 2)") totalBalance: java.math.BigDecimal,

    // enrich the extraction with forcing using a RAW type
    @DataTypeHint("RAW") modelClass: Class[_]
)

DataTypes.of(classOf[User])
{% endhighlight %}
</div>

</div>

**Bridging to JVM Types**

| Java Type                            | Input | Output | Remarks                                 |
|:-------------------------------------|:-----:|:------:|:----------------------------------------|
|*class*                               | X     | X      | Originating class or subclasses (for input) or <br>superclasses (for output). *Default* |
|`org.apache.flink.types.Row`          | X     | X      | Represent the structured type as a row. |
|`org.apache.flink.table.data.RowData` | X     | X      | Internal data structure.                |

### Other Data Types

#### `BOOLEAN`

Data type of a boolean with a (possibly) three-valued logic of `TRUE`, `FALSE`, and `UNKNOWN`.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
BOOLEAN
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.BOOLEAN()
{% endhighlight %}
</div>

</div>

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                              |
|:-------------------|:-----:|:------:|:-------------------------------------|
|`java.lang.Boolean` | X     | X      | *Default*                            |
|`boolean`           | X     | (X)    | Output only if type is not nullable. |

#### `RAW`

Data type of an arbitrary serialized type. This type is a black box within the table ecosystem
and is only deserialized at the edges.

The raw type is an extension to the SQL standard.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
RAW('class', 'snapshot')
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.RAW(class, serializer)

DataTypes.RAW(class)
{% endhighlight %}
</div>

</div>

The type can be declared using `RAW('class', 'snapshot')` where `class` is the originating class and
`snapshot` is the serialized `TypeSerializerSnapshot` in Base64 encoding. Usually, the type string is not
declared directly but is generated while persisting the type.

In the API, the `RAW` type can be declared either by directly supplying a `Class` + `TypeSerializer` or
by passing `Class` and letting the framework extract `Class` + `TypeSerializer` from there.

**Bridging to JVM Types**

| Java Type         | Input | Output | Remarks                              |
|:------------------|:-----:|:------:|:-------------------------------------------|
|*class*            | X     | X      | Originating class or subclasses (for input) or <br>superclasses (for output). *Default* |
|`byte[]`           |       | X      |                                      |
|`org.apache.flink.table.data.RawValueData` | X     | X      | Internal data structure. |

#### `NULL`

Data type for representing untyped `NULL` values.

The null type is an extension to the SQL standard. A null type has no other value
except `NULL`, thus, it can be cast to any nullable type similar to JVM semantics.

This type helps in representing unknown types in API calls that use a `NULL` literal
as well as bridging to formats such as JSON or Avro that define such a type as well.

This type is not very useful in practice and is just mentioned here for completeness.

**Declaration**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
NULL
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.NULL()
{% endhighlight %}
</div>

</div>

**Bridging to JVM Types**

| Java Type         | Input | Output | Remarks                              |
|:------------------|:-----:|:------:|:-------------------------------------|
|`java.lang.Object` | X     | X      | *Default*                            |
|*any class*        |       | (X)    | Any non-primitive type.              |

Data Type Annotations
---------------------

At many locations in the API, Flink tries to automatically extract data type from class information using
reflection to avoid repetitive manual schema work. However, extracting a data type reflectively is not always
successful because logical information might be missing. Therefore, it might be necessary to add additional
information close to a class or field declaration for supporting the extraction logic.

The following table lists classes that can be implicitly mapped to a data type without requiring further information:

| Class                       | Data Type                           |
|:----------------------------|:------------------------------------|
| `java.lang.String`          | `STRING`                            |
| `java.lang.Boolean`         | `BOOLEAN`                           |
| `boolean`                   | `BOOLEAN NOT NULL`                  |
| `java.lang.Byte`            | `TINYINT`                           |
| `byte`                      | `TINYINT NOT NULL`                  |
| `java.lang.Short`           | `SMALLINT`                          |
| `short`                     | `SMALLINT NOT NULL`                 |
| `java.lang.Integer`         | `INT`                               |
| `int`                       | `INT NOT NULL`                      |
| `java.lang.Long`            | `BIGINT`                            |
| `long`                      | `BIGINT NOT NULL`                   |
| `java.lang.Float`           | `FLOAT`                             |
| `float`                     | `FLOAT NOT NULL`                    |
| `java.lang.Double`          | `DOUBLE`                            |
| `double`                    | `DOUBLE NOT NULL`                   |
| `java.sql.Date`             | `DATE`                              |
| `java.time.LocalDate`       | `DATE`                              |
| `java.sql.Time`             | `TIME(0)`                           |
| `java.time.LocalTime`       | `TIME(9)`                           |
| `java.sql.Timestamp`        | `TIMESTAMP(9)`                      |
| `java.time.LocalDateTime`   | `TIMESTAMP(9)`                      |
| `java.time.OffsetDateTime`  | `TIMESTAMP(9) WITH TIME ZONE`       |
| `java.time.Instant`         | `TIMESTAMP(9) WITH LOCAL TIME ZONE` |
| `java.time.Duration`        | `INVERVAL SECOND(9)`                |
| `java.time.Period`          | `INTERVAL YEAR(4) TO MONTH`         |
| `byte[]`                    | `BYTES`                             |
| `T[]`                       | `ARRAY<T>`                          |
| `java.lang.Map<K, V>`       | `MAP<K, V>`                         |
| structured type `T`         | anonymous structured type `T`       |

Other JVM bridging classes mentioned in this document require a `@DataTypeHint` annotation.

_Data type hints_ can parameterize or replace the default extraction logic of individual function parameters
and return types, structured classes, or fields of structured classes. An implementer can choose to what
extent the default extraction logic should be modified by declaring a `@DataTypeHint` annotation.

The `@DataTypeHint` annotation provides a set of optional hint parameters. Some of those parameters are shown in the
following example. More information can be found in the documentation of the annotation class.

<div class="codetabs" markdown="1">

<div data-lang="Java" markdown="1">
{% highlight java %}
import org.apache.flink.table.annotation.DataTypeHint;

class User {

    // defines an INT data type with a default conversion class `java.lang.Integer`
    public @DataTypeHint("INT") Object o;

    // defines a TIMESTAMP data type of millisecond precision with an explicit conversion class
    public @DataTypeHint(value = "TIMESTAMP(3)", bridgedTo = java.sql.Timestamp.class) Object o;

    // enrich the extraction with forcing using a RAW type
    public @DataTypeHint("RAW") Class<?> modelClass;

    // defines that all occurrences of java.math.BigDecimal (also in nested fields) will be
    // extracted as DECIMAL(12, 2)
    public @DataTypeHint(defaultDecimalPrecision = 12, defaultDecimalScale = 2) AccountStatement stmt;

    // defines that whenever a type cannot be mapped to a data type, instead of throwing
    // an exception, always treat it as a RAW type
    public @DataTypeHint(allowRawGlobally = HintFlag.TRUE) ComplexModel model;
}
{% endhighlight %}
</div>

<div data-lang="Scala" markdown="1">
{% highlight java %}
import org.apache.flink.table.annotation.DataTypeHint

class User {

    // defines an INT data type with a default conversion class `java.lang.Integer`
    @DataTypeHint("INT")
    var o: AnyRef

    // defines a TIMESTAMP data type of millisecond precision with an explicit conversion class
    @DataTypeHint(value = "TIMESTAMP(3)", bridgedTo = java.sql.Timestamp.class)
    var o: AnyRef

    // enrich the extraction with forcing using a RAW type
    @DataTypeHint("RAW")
    var modelClass: Class[_]

    // defines that all occurrences of java.math.BigDecimal (also in nested fields) will be
    // extracted as DECIMAL(12, 2)
    @DataTypeHint(defaultDecimalPrecision = 12, defaultDecimalScale = 2)
    var stmt: AccountStatement

    // defines that whenever a type cannot be mapped to a data type, instead of throwing
    // an exception, always treat it as a RAW type
    @DataTypeHint(allowRawGlobally = HintFlag.TRUE)
    var model: ComplexModel
}
{% endhighlight %}
</div>

</div>

{% top %}
