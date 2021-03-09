---
title: "Data Types"
weight: 21
type: docs
aliases:
  - /dev/table/types.html
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

# Data Types

Flink SQL has a rich set of native data types available to users.

Data Type
---------

A *data type* describes the logical type of a value in the table ecosystem.
It can be used to declare input and/or output types of operations.

Flink's data types are similar to the SQL standard's *data type* terminology but also contain information
about the nullability of a value for efficient handling of scalar expressions.

Examples of data types are:
- `INT`
- `INT NOT NULL`
- `INTERVAL DAY TO SECOND(3)`
- `ROW<myField ARRAY<BOOLEAN>, myOtherField TIMESTAMP(3)>`

A list of all pre-defined data types can be found [below](#list-of-data-types).

### Data Types in the Table API

{{< tabs "dataytes" >}}
{{< tab "Java/Scala" >}}
Users of the JVM-based API work with instances of `org.apache.flink.table.types.DataType` within the Table API or when
defining connectors, catalogs, or user-defined functions. 

A `DataType` instance has two responsibilities:
- **Declaration of a logical type** which does not imply a concrete physical representation for transmission
or storage but defines the boundaries between JVM-based/Python languages and the table ecosystem.
- *Optional:* **Giving hints about the physical representation of data to the planner** which is useful at the edges to other APIs.

For JVM-based languages, all pre-defined data types are available in `org.apache.flink.table.api.DataTypes`.
{{< /tab >}}
{{< tab "Python" >}}
Users of the Python API work with instances of `pyflink.table.types.DataType` within the Python Table API or when 
defining Python user-defined functions.

A `DataType` instance has such a responsibility:
- **Declaration of a logical type** which does not imply a concrete physical representation for transmission
or storage but defines the boundaries between Python languages and the table ecosystem.

For Python language, those types are available in `pyflink.table.types.DataTypes`.
{{< /tab >}}
{{< /tabs >}}

{{< tabs "84cf5e1c-c899-42cb-8fdf-6ae59fdd012c" >}}
{{< tab "Java" >}}
It is recommended to add a star import to your table programs for having a fluent API:

```java
import static org.apache.flink.table.api.DataTypes.*;

DataType t = INTERVAL(DAY(), SECOND(3));
```
{{< /tab >}}
{{< tab "Scala" >}}
It is recommended to add a star import to your table programs for having a fluent API:

```scala
import org.apache.flink.table.api.DataTypes._

val t: DataType = INTERVAL(DAY(), SECOND(3));
```
{{< /tab >}}
{{< tab "Python" >}}

```python
from pyflink.table.types import DataTypes

t = DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND(3))
```
{{< /tab >}}
{{< /tabs >}}


#### Physical Hints

Physical hints are required at the edges of the table ecosystem where the SQL-based type system ends and
programming-specific data types are required. Hints indicate the data format that an implementation
expects.

For example, a data source could express that it produces values for logical `TIMESTAMP`s using a `java.sql.Timestamp` class
instead of using `java.time.LocalDateTime` which would be the default. With this information, the runtime is able to convert
the produced class into its internal data format. In return, a data sink can declare the data format it consumes from the runtime.

Here are some examples of how to declare a bridging conversion class:

{{< tabs "hints" >}}
{{< tab "Java" >}}
```java
// tell the runtime to not produce or consume java.time.LocalDateTime instances
// but java.sql.Timestamp
DataType t = DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class);

// tell the runtime to not produce or consume boxed integer arrays
// but primitive int arrays
DataType t = DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(int[].class);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// tell the runtime to not produce or consume java.time.LocalDateTime instances
// but java.sql.Timestamp
val t: DataType = DataTypes.TIMESTAMP(3).bridgedTo(classOf[java.sql.Timestamp]);

// tell the runtime to not produce or consume boxed integer arrays
// but primitive int arrays
val t: DataType = DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(classOf[Array[Int]]);
```
{{< /tab >}}
{{< /tabs >}}

<span class="label label-danger">Attention</span> Please note that physical hints are usually only required if the
API is extended. Users of predefined sources/sinks/functions do not need to define such hints. Hints within
a table program (e.g. `field.cast(TIMESTAMP(3).bridgedTo(Timestamp.class))`) are ignored.

{{< tabs "table" >}}
{{< tab "Java/Scala" >}}
The default planner supports the following set of SQL types:

| Data Type | Remarks for Data Type |
|:----------|:----------------------|
| `CHAR` | |
| `VARCHAR` | |
| `STRING` | |
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
| `TIMESTAMP_LTZ` | |
| `INTERVAL` | Supports only interval of `MONTH` and `SECOND(3)`. |
| `ARRAY` | |
| `MULTISET` | |
| `MAP` | |
| `ROW` | |
| `RAW` | |
| structured types | Only exposed in user-defined functions yet. |

{{< /tab >}}
{{< tab "Python" >}}
N/A
{{< /tab >}}
{{< /tabs >}}

List of Data Types
------------------

This section lists all pre-defined data types.
{{< tabs "datatypesimport" >}}
{{< tab "Java/Scala" >}}
For the JVM-based Table API those types are also available in `org.apache.flink.table.api.DataTypes`.
{{< /tab >}}
{{< tab "Python" >}}
For the Python Table API, those types are available in `pyflink.table.types.DataTypes`.
{{< /tab >}}
{{< /tabs >}}

### Character Strings

#### `CHAR`

Data type of a fixed-length character string.

**Declaration**

{{< tabs "d937c413-bace-4c17-9bc0-8899dac482ae" >}}
{{< tab "SQL" >}}
```text
CHAR
CHAR(n)
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.CHAR(n)
```

**Bridging to JVM Types**

| Java Type                               | Input | Output | Remarks                  |
|:----------------------------------------|:-----:|:------:|:-------------------------|
|`java.lang.String`                       | X     | X      | *Default*                |
|`byte[]`                                 | X     | X      | Assumes UTF-8 encoding.  |
|`org.apache.flink.table.data.StringData` | X     | X      | Internal data structure. |

{{< /tab >}}
{{< tab "Python" >}}
```python
Not supported.
```
{{< /tab >}}
{{< /tabs >}}


The type can be declared using `CHAR(n)` where `n` is the number of code points. `n` must have a value between `1`
and `2,147,483,647` (both inclusive). If no length is specified, `n` is equal to `1`.


#### `VARCHAR` / `STRING`

Data type of a variable-length character string.

**Declaration**

{{< tabs "3e9c301a-f481-477f-aaa1-f26239481b74" >}}
{{< tab "SQL" >}}
```text
VARCHAR
VARCHAR(n)

STRING
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.VARCHAR(n)

DataTypes.STRING()
```

**Bridging to JVM Types**

| Java Type                               | Input | Output | Remarks                  |
|:----------------------------------------|:-----:|:------:|:-------------------------|
|`java.lang.String`                       | X     | X      | *Default*                |
|`byte[]`                                 | X     | X      | Assumes UTF-8 encoding.  |
|`org.apache.flink.table.data.StringData` | X     | X      | Internal data structure. |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.VARCHAR(n)

DataTypes.STRING()
```

<span class="label label-danger">Attention</span> The specified maximum number of code points `n` in `DataTypes.VARCHAR(n)` must be `2,147,483,647` currently.
{{< /tab >}}
{{< /tabs >}}

The type can be declared using `VARCHAR(n)` where `n` is the maximum number of code points. `n` must have a value
between `1` and `2,147,483,647` (both inclusive). If no length is specified, `n` is equal to `1`.

`STRING` is a synonym for `VARCHAR(2147483647)`.

### Binary Strings

#### `BINARY`

Data type of a fixed-length binary string (=a sequence of bytes).

**Declaration**

{{< tabs "2152b311-40e1-4d92-8b99-1b84a4692073" >}}
{{< tab "SQL" >}}
```text
BINARY
BINARY(n)
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.BINARY(n)
```

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                 |
|:-------------------|:-----:|:------:|:------------------------|
|`byte[]`            | X     | X      | *Default*               |

{{< /tab >}}
{{< tab "Python" >}}
```python
Not supported.
```
{{< /tab >}}
{{< /tabs >}}

The type can be declared using `BINARY(n)` where `n` is the number of bytes. `n` must have a value
between `1` and `2,147,483,647` (both inclusive). If no length is specified, `n` is equal to `1`.


#### `VARBINARY` / `BYTES`

Data type of a variable-length binary string (=a sequence of bytes).

**Declaration**

{{< tabs "5761633b-a968-4328-81f8-1f36ab67bcab" >}}
{{< tab "SQL" >}}
```text
VARBINARY
VARBINARY(n)

BYTES
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.VARBINARY(n)

DataTypes.BYTES()
```

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                 |
|:-------------------|:-----:|:------:|:------------------------|
|`byte[]`            | X     | X      | *Default*               |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.VARBINARY(n)

DataTypes.BYTES()
```

<span class="label label-danger">Attention</span> The specified maximum number of bytes `n` in `DataTypes.VARBINARY(n)` must be `2,147,483,647` currently.
{{< /tab >}}
{{< /tabs >}}

The type can be declared using `VARBINARY(n)` where `n` is the maximum number of bytes. `n` must
have a value between `1` and `2,147,483,647` (both inclusive). If no length is specified, `n` is
equal to `1`.

`BYTES` is a synonym for `VARBINARY(2147483647)`.

### Exact Numerics

#### `DECIMAL`

Data type of a decimal number with fixed precision and scale.

**Declaration**

{{< tabs "923d4502-4db8-462d-9d4c-4d3d3445ddac" >}}
{{< tab "SQL" >}}
```text
DECIMAL
DECIMAL(p)
DECIMAL(p, s)

DEC
DEC(p)
DEC(p, s)

NUMERIC
NUMERIC(p)
NUMERIC(p, s)
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.DECIMAL(p, s)
```

**Bridging to JVM Types**

| Java Type                                | Input | Output | Remarks                  |
|:-----------------------------------------|:-----:|:------:|:-------------------------|
|`java.math.BigDecimal`                    | X     | X      | *Default*                |
|`org.apache.flink.table.data.DecimalData` | X     | X      | Internal data structure. |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.DECIMAL(p, s)
```

<span class="label label-danger">Attention</span> The `precision` and `scale` specified in `DataTypes.DECIMAL(p, s)` must be `38` and `18` separately currently.
{{< /tab >}}
{{< /tabs >}}

The type can be declared using `DECIMAL(p, s)` where `p` is the number of digits in a
number (*precision*) and `s` is the number of digits to the right of the decimal point
in a number (*scale*). `p` must have a value between `1` and `38` (both inclusive). `s`
must have a value between `0` and `p` (both inclusive). The default value for `p` is 10.
The default value for `s` is `0`.

`NUMERIC(p, s)` and `DEC(p, s)` are synonyms for this type.

#### `TINYINT`

Data type of a 1-byte signed integer with values from `-128` to `127`.

**Declaration**

{{< tabs "5754537d-071a-4793-a6a4-bf61c546799c" >}}
{{< tab "SQL" >}}
```text
TINYINT
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.TINYINT()
```

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Byte`    | X     | X      | *Default*                                    |
|`byte`              | X     | (X)    | Output only if type is not nullable.         |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.TINYINT()
```
{{< /tab >}}
{{< /tabs >}}

#### `SMALLINT`

Data type of a 2-byte signed integer with values from `-32,768` to `32,767`.

**Declaration**

{{< tabs "4678d5ed-d2e1-4e47-bd74-7a2ce4e5f624" >}}
{{< tab "SQL" >}}
```text
SMALLINT
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.SMALLINT()
```

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Short`   | X     | X      | *Default*                                    |
|`short`             | X     | (X)    | Output only if type is not nullable.         |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.SMALLINT()
```
{{< /tab >}}
{{< /tabs >}}

#### `INT`

Data type of a 4-byte signed integer with values from `-2,147,483,648` to `2,147,483,647`.

**Declaration**

{{< tabs "68c86706-bd0c-4fdc-a113-950c6bd68402" >}}
{{< tab "SQL" >}}
```text
INT

INTEGER
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.INT()
```

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Integer` | X     | X      | *Default*                                    |
|`int`               | X     | (X)    | Output only if type is not nullable.         |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.INT()
```
{{< /tab >}}
{{< /tabs >}}

`INTEGER` is a synonym for this type.

#### `BIGINT`

Data type of an 8-byte signed integer with values from `-9,223,372,036,854,775,808` to
`9,223,372,036,854,775,807`.

**Declaration**

{{< tabs "1506658c-be8d-472c-aacd-1479af0850fa" >}}
{{< tab "SQL" >}}
```text
BIGINT
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.BIGINT()
```

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Long`    | X     | X      | *Default*                                    |
|`long`              | X     | (X)    | Output only if type is not nullable.         |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.BIGINT()
```
{{< /tab >}}
{{< /tabs >}}

### Approximate Numerics

#### `FLOAT`

Data type of a 4-byte single precision floating point number.

Compared to the SQL standard, the type does not take parameters.

**Declaration**

{{< tabs "b0b09ce1-0ab5-4dda-983d-5cec7b9cd49f" >}}
{{< tab "SQL" >}}
```text
FLOAT
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.FLOAT()
```

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Float`   | X     | X      | *Default*                                    |
|`float`             | X     | (X)    | Output only if type is not nullable.         |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.FLOAT()
```
{{< /tab >}}
{{< /tabs >}}

#### `DOUBLE`

Data type of an 8-byte double precision floating point number.

**Declaration**

{{< tabs "10f7cbc5-8269-44d1-9295-9219ceb77a2f" >}}
{{< tab "SQL" >}}
```text
DOUBLE

DOUBLE PRECISION
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.DOUBLE()
```

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Double`  | X     | X      | *Default*                                    |
|`double`            | X     | (X)    | Output only if type is not nullable.         |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.DOUBLE()
```
{{< /tab >}}
{{< /tabs >}}

`DOUBLE PRECISION` is a synonym for this type.

### Date and Time

#### `DATE`

Data type of a date consisting of `year-month-day` with values ranging from `0000-01-01`
to `9999-12-31`.

Compared to the SQL standard, the range starts at year `0000`.

**Declaration**

{{< tabs "ce6ab001-794e-4124-985b-d942bc6727e3" >}}
{{< tab "SQL" >}}
```text
DATE
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.DATE()
```

**Bridging to JVM Types**

| Java Type            | Input | Output | Remarks                                      |
|:---------------------|:-----:|:------:|:---------------------------------------------|
|`java.time.LocalDate` | X     | X      | *Default*                                    |
|`java.sql.Date`       | X     | X      |                                              |
|`java.lang.Integer`   | X     | X      | Describes the number of days since epoch.    |
|`int`                 | X     | (X)    | Describes the number of days since epoch.<br>Output only if type is not nullable. |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.DATE()
```
{{< /tab >}}
{{< /tabs >}}

#### `TIME`

Data type of a time *without* time zone consisting of `hour:minute:second[.fractional]` with
up to nanosecond precision and values ranging from `00:00:00.000000000` to
`23:59:59.999999999`.

{{< tabs "time" >}}
{{< tab "SQL/Java/Scala" >}}
Compared to the SQL standard, leap seconds (`23:59:60` and `23:59:61`) are not supported as
the semantics are closer to `java.time.LocalTime`. A time *with* time zone is not provided.
{{< /tab >}}
{{< tab "Python" >}}
Compared to the SQL standard, leap seconds (`23:59:60` and `23:59:61`) are not supported.
A time *with* time zone is not provided.
{{< /tab >}}
{{< /tabs >}}

**Declaration**

{{< tabs "c44284f9-1674-45e7-87b2-3e1da2e8a182" >}}
{{< tab "SQL" >}}
```text
TIME
TIME(p)
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.TIME(p)
```

**Bridging to JVM Types**

| Java Type            | Input | Output | Remarks                                             |
|:---------------------|:-----:|:------:|:----------------------------------------------------|
|`java.time.LocalTime` | X     | X      | *Default*                                           |
|`java.sql.Time`       | X     | X      |                                                     |
|`java.lang.Integer`   | X     | X      | Describes the number of milliseconds of the day.    |
|`int`                 | X     | (X)    | Describes the number of milliseconds of the day.<br>Output only if type is not nullable. |
|`java.lang.Long`      | X     | X      | Describes the number of nanoseconds of the day.     |
|`long`                | X     | (X)    | Describes the number of nanoseconds of the day.<br>Output only if type is not nullable. |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.TIME(p)
```

<span class="label label-danger">Attention</span> The `precision` specified in `DataTypes.TIME(p)` must be `0` currently.
{{< /tab >}}
{{< /tabs >}}

The type can be declared using `TIME(p)` where `p` is the number of digits of fractional
seconds (*precision*). `p` must have a value between `0` and `9` (both inclusive). If no
precision is specified, `p` is equal to `0`.

#### `TIMESTAMP`

Data type of a timestamp *without* time zone consisting of `year-month-day hour:minute:second[.fractional]`
with up to nanosecond precision and values ranging from `0000-01-01 00:00:00.000000000` to
`9999-12-31 23:59:59.999999999`.

{{< tabs "timestamps" >}}
{{< tab "SQL/Java/Scala" >}}
Compared to the SQL standard, leap seconds (`23:59:60` and `23:59:61`) are not supported as
the semantics are closer to `java.time.LocalDateTime`.

A conversion from and to `BIGINT` (a JVM `long` type) is not supported as this would imply a time
zone. However, this type is time zone free. For more `java.time.Instant`-like semantics use
`TIMESTAMP_LTZ`.
{{< /tab >}}
{{< tab "Python" >}}
Compared to the SQL standard, leap seconds (`23:59:60` and `23:59:61`) are not supported.

A conversion from and to `BIGINT` is not supported as this would imply a time zone.
However, this type is time zone free. If you have such a requirement please use `TIMESTAMP_LTZ`.
{{< /tab >}}
{{< /tabs >}}

**Declaration**

{{< tabs "5ba9017d-ae5f-4129-b7d7-bed06e6e2598" >}}
{{< tab "SQL" >}}
```text
TIMESTAMP
TIMESTAMP(p)

TIMESTAMP WITHOUT TIME ZONE
TIMESTAMP(p) WITHOUT TIME ZONE
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.TIMESTAMP(p)
```

**Bridging to JVM Types**

| Java Type                                  | Input | Output | Remarks                  |
|:-------------------------------------------|:-----:|:------:|:-------------------------|
|`java.time.LocalDateTime`                   | X     | X      | *Default*                |
|`java.sql.Timestamp`                        | X     | X      |                          |
|`org.apache.flink.table.data.TimestampData` | X     | X      | Internal data structure. |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.TIMESTAMP(p)
```

<span class="label label-danger">Attention</span> The `precision` specified in `DataTypes.TIMESTAMP(p)` must be `3` currently.
{{< /tab >}}
{{< /tabs >}}

The type can be declared using `TIMESTAMP(p)` where `p` is the number of digits of fractional
seconds (*precision*). `p` must have a value between `0` and `9` (both inclusive). If no precision
is specified, `p` is equal to `6`.

`TIMESTAMP(p) WITHOUT TIME ZONE` is a synonym for this type.

#### `TIMESTAMP WITH TIME ZONE`

Data type of a timestamp *with* time zone consisting of `year-month-day hour:minute:second[.fractional] zone`
with up to nanosecond precision and values ranging from `0000-01-01 00:00:00.000000000 +14:59` to
`9999-12-31 23:59:59.999999999 -14:59`.

{{< tabs "timestamps" >}}
{{< tab "SQL/Java/Scala" >}}
Compared to the SQL standard, leap seconds (`23:59:60` and `23:59:61`) are not supported as the semantics
are closer to `java.time.OffsetDateTime`.
{{< /tab >}}
{{< tab "Python" >}}
Compared to the SQL standard, leap seconds (`23:59:60` and `23:59:61`) are not supported.
{{< /tab >}}
{{< /tabs >}}

Compared to `TIMESTAMP_LTZ`, the time zone offset information is physically
stored in every datum. It is used individually for every computation, visualization, or communication
to external systems.

**Declaration**

{{< tabs "47663464-5865-44be-b23d-c03c9ec2b186" >}}
{{< tab "SQL" >}}
```text
TIMESTAMP WITH TIME ZONE
TIMESTAMP(p) WITH TIME ZONE
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.TIMESTAMP_WITH_TIME_ZONE(p)
```

**Bridging to JVM Types**

| Java Type                 | Input | Output | Remarks              |
|:--------------------------|:-----:|:------:|:---------------------|
|`java.time.OffsetDateTime` | X     | X      | *Default*            |
|`java.time.ZonedDateTime`  | X     |        | Ignores the zone ID. |

{{< /tab >}}
{{< tab "Python" >}}
```python
Not supported.
```
{{< /tab >}}
{{< /tabs >}}

{{< tabs "timestamps" >}}
{{< tab "SQL/Java/Scala" >}}
The type can be declared using `TIMESTAMP(p) WITH TIME ZONE` where `p` is the number of digits of
fractional seconds (*precision*). `p` must have a value between `0` and `9` (both inclusive). If no
precision is specified, `p` is equal to `6`.
{{< /tab >}}
{{< tab "Python" >}}
{{< /tab >}}
{{< /tabs >}}

#### `TIMESTAMP_LTZ`

Data type of a timestamp *with local* time zone consisting of `year-month-day hour:minute:second[.fractional] zone`
with up to nanosecond precision and values ranging from `0000-01-01 00:00:00.000000000 +14:59` to
`9999-12-31 23:59:59.999999999 -14:59`.

{{< tabs "timestamps" >}}
{{< tab "SQL/Java/Scala" >}}
Leap seconds (`23:59:60` and `23:59:61`) are not supported as the semantics are closer to `java.time.OffsetDateTime`.

Compared to `TIMESTAMP WITH TIME ZONE`, the time zone offset information is not stored physically
in every datum. Instead, the type assumes `java.time.Instant` semantics in UTC time zone at
the edges of the table ecosystem. Every datum is interpreted in the local time zone configured in
the current session for computation and visualization.
{{< /tab >}}
{{< tab "Python" >}}
Leap seconds (`23:59:60` and `23:59:61`) are not supported.

Compared to `TIMESTAMP WITH TIME ZONE`, the time zone offset information is not stored physically
in every datum. 
Every datum is interpreted in the local time zone configured in the current session for computation and visualization.
{{< /tab >}}
{{< /tabs >}}

This type fills the gap between time zone free and time zone mandatory timestamp types by allowing
the interpretation of UTC timestamps according to the configured session time zone.

**Declaration**

{{< tabs "75734ebe-7f16-4df8-83e9-99fcd2a28527" >}}
{{< tab "SQL" >}}
```text
TIMESTAMP_LTZ
TIMESTAMP_LTZ(p)

TIMESTAMP WITH LOCAL TIME ZONE
TIMESTAMP(p) WITH LOCAL TIME ZONE
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.TIMESTAMP_LTZ(p)
DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(p)
```

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                                           |
|:-------------------|:-----:|:------:|:--------------------------------------------------|
|`java.time.Instant` | X     | X      | *Default*                                         |
|`java.lang.Integer` | X     | X      | Describes the number of seconds since epoch.      |
|`int`               | X     | (X)    | Describes the number of seconds since epoch.<br>Output only if type is not nullable. |
|`java.lang.Long`    | X     | X      | Describes the number of milliseconds since epoch. |
|`long`              | X     | (X)    | Describes the number of milliseconds since epoch.<br>Output only if type is not nullable. |
|`org.apache.flink.table.data.TimestampData` | X     | X      | Internal data structure. |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.TIMESTAMP_LTZ(p)
DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(p)
```

<span class="label label-danger">Attention</span> The `precision` specified in `DataTypes.TIMESTAMP_LTZ(p)` must be `3` currently.
{{< /tab >}}
{{< /tabs >}}

The type can be declared using `TIMESTAMP_LTZ(p)` where `p` is the number
of digits of fractional seconds (*precision*). `p` must have a value between `0` and `9`
(both inclusive). If no precision is specified, `p` is equal to `6`.

`TIMESTAMP(p) WITH LOCAL TIME ZONE` is a synonym for this type.

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

{{< tabs "6d3ddd2e-5751-45af-a153-aecfaf561f41" >}}
{{< tab "SQL" >}}
```text
INTERVAL YEAR
INTERVAL YEAR(p)
INTERVAL YEAR(p) TO MONTH
INTERVAL MONTH
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.INTERVAL(DataTypes.YEAR())
DataTypes.INTERVAL(DataTypes.YEAR(p))
DataTypes.INTERVAL(DataTypes.YEAR(p), DataTypes.MONTH())
DataTypes.INTERVAL(DataTypes.MONTH())
```

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                            |
|:-------------------|:-----:|:------:|:-----------------------------------|
|`java.time.Period`  | X     | X      | Ignores the `days` part. *Default* |
|`java.lang.Integer` | X     | X      | Describes the number of months.    |
|`int`               | X     | (X)    | Describes the number of months.<br>Output only if type is not nullable. |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.INTERVAL(DataTypes.YEAR())
DataTypes.INTERVAL(DataTypes.YEAR(p))
DataTypes.INTERVAL(DataTypes.YEAR(p), DataTypes.MONTH())
DataTypes.INTERVAL(DataTypes.MONTH())
```
{{< /tab >}}
{{< /tabs >}}

The type can be declared using the above combinations where `p` is the number of digits of years
(*year precision*). `p` must have a value between `1` and `4` (both inclusive). If no year precision
is specified, `p` is equal to `2`.

#### `INTERVAL DAY TO SECOND`

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

{{< tabs "89aea284-08df-4671-b323-366f668304e7" >}}
{{< tab "SQL" >}}
```text
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
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
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
```

**Bridging to JVM Types**

| Java Type           | Input | Output | Remarks                               |
|:--------------------|:-----:|:------:|:--------------------------------------|
|`java.time.Duration` | X     | X      | *Default*                             |
|`java.lang.Long`     | X     | X      | Describes the number of milliseconds. |
|`long`               | X     | (X)    | Describes the number of milliseconds.<br>Output only if type is not nullable. |

{{< /tab >}}
{{< tab "Python" >}}
```python
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
```
{{< /tab >}}
{{< /tabs >}}

The type can be declared using the above combinations where `p1` is the number of digits of days
(*day precision*) and `p2` is the number of digits of fractional seconds (*fractional precision*).
`p1` must have a value between `1` and `6` (both inclusive). `p2` must have a value between `0`
and `9` (both inclusive). If no `p1` is specified, it is equal to `2` by default. If no `p2` is
specified, it is equal to `6` by default.

### Constructured Data Types

#### `ARRAY`

Data type of an array of elements with same subtype.

Compared to the SQL standard, the maximum cardinality of an array cannot be specified but is
fixed at `2,147,483,647`. Also, any valid type is supported as a subtype.

**Declaration**

{{< tabs "74fa3a61-58ee-4ba6-b757-d0a401f20f7a" >}}
{{< tab "SQL" >}}
```text
ARRAY<t>
t ARRAY
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.ARRAY(t)
```

**Bridging to JVM Types**

| Java Type                              | Input | Output | Remarks                           |
|:---------------------------------------|:-----:|:------:|:----------------------------------|
|*t*`[]`                                 | (X)   | (X)    | Depends on the subtype. *Default* |
| `java.util.List<t>`                    | X     | X      |                                   |
| *subclass* of `java.util.List<t>`      | X     |        |                                   |
|`org.apache.flink.table.data.ArrayData` | X     | X      | Internal data structure.          |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.ARRAY(t)
```
{{< /tab >}}
{{< /tabs >}}

The type can be declared using `ARRAY<t>` where `t` is the data type of the contained
elements.

`t ARRAY` is a synonym for being closer to the SQL standard. For example, `INT ARRAY` is
equivalent to `ARRAY<INT>`.

#### `MAP`

Data type of an associative array that maps keys (including `NULL`) to values (including `NULL`). A map
cannot contain duplicate keys; each key can map to at most one value.

There is no restriction of element types; it is the responsibility of the user to ensure uniqueness.

The map type is an extension to the SQL standard.

**Declaration**

{{< tabs "a1a3a740-4781-4098-9cef-ef860e728514" >}}
{{< tab "SQL" >}}
```text
MAP<kt, vt>
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.MAP(kt, vt)
```

**Bridging to JVM Types**

| Java Type                             | Input | Output | Remarks                  |
|:--------------------------------------|:-----:|:------:|:-------------------------|
| `java.util.Map<kt, vt>`               | X     | X      | *Default*                |
| *subclass* of `java.util.Map<kt, vt>` | X     |        |                          |
|`org.apache.flink.table.data.MapData`  | X     | X      | Internal data structure. |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.MAP(kt, vt)
```
{{< /tab >}}
{{< /tabs >}}

The type can be declared using `MAP<kt, vt>` where `kt` is the data type of the key elements
and `vt` is the data type of the value elements.

#### `MULTISET`

Data type of a multiset (=bag). Unlike a set, it allows for multiple instances for each of its
elements with a common subtype. Each unique value (including `NULL`) is mapped to some multiplicity.

There is no restriction of element types; it is the responsibility of the user to ensure uniqueness.

**Declaration**

{{< tabs "a6db836a-7554-4dab-8f5f-3fefabb4fc27" >}}
{{< tab "SQL" >}}
```text
MULTISET<t>
t MULTISET
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.MULTISET(t)
```

**Bridging to JVM Types**

| Java Type                             | Input | Output | Remarks                                                  |
|:--------------------------------------|:-----:|:------:|:---------------------------------------------------------|
|`java.util.Map<t, java.lang.Integer>`  | X     | X      | Assigns each value to an integer multiplicity. *Default* |
| *subclass* of `java.util.Map<t, java.lang.Integer>>` | X     |        |                                           |
|`org.apache.flink.table.data.MapData`  | X     | X      | Internal data structure.                                 |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.MULTISET(t)
```
{{< /tab >}}
{{< /tabs >}}

The type can be declared using `MULTISET<t>` where `t` is the data type
of the contained elements.

`t MULTISET` is a synonym for being closer to the SQL standard. For example, `INT MULTISET` is
equivalent to `MULTISET<INT>`.

#### `ROW`

Data type of a sequence of fields.

A field consists of a field name, field type, and an optional description. The most specific type
of a row of a table is a row type. In this case, each column of the row corresponds to the field
of the row type that has the same ordinal position as the column.

Compared to the SQL standard, an optional field description simplifies the handling with complex
structures.

A row type is similar to the `STRUCT` type known from other non-standard-compliant frameworks.

**Declaration**

{{< tabs "d81eee80-a9a8-421e-b976-977de85bde77" >}}
{{< tab "SQL" >}}
```sql
ROW<n0 t0, n1 t1, ...>
ROW<n0 t0 'd0', n1 t1 'd1', ...>

ROW(n0 t0, n1 t1, ...>
ROW(n0 t0 'd0', n1 t1 'd1', ...)
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.ROW(DataTypes.FIELD(n0, t0), DataTypes.FIELD(n1, t1), ...)
DataTypes.ROW(DataTypes.FIELD(n0, t0, d0), DataTypes.FIELD(n1, t1, d1), ...)
```

**Bridging to JVM Types**

| Java Type                            | Input | Output | Remarks                  |
|:-------------------------------------|:-----:|:------:|:-------------------------|
|`org.apache.flink.types.Row`          | X     | X      | *Default*                |
|`org.apache.flink.table.data.RowData` | X     | X      | Internal data structure. |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.ROW([DataTypes.FIELD(n0, t0), DataTypes.FIELD(n1, t1), ...])
DataTypes.ROW([DataTypes.FIELD(n0, t0, d0), DataTypes.FIELD(n1, t1, d1), ...])
```
{{< /tab >}}
{{< /tabs >}}

The type can be declared using `ROW<n0 t0 'd0', n1 t1 'd1', ...>` where `n` is the unique name of
a field, `t` is the logical type of a field, `d` is the description of a field.

`ROW(...)` is a synonym for being closer to the SQL standard. For example, `ROW(myField INT, myOtherField BOOLEAN)` is
equivalent to `ROW<myField INT, myOtherField BOOLEAN>`.

### User-Defined Data Types

{{< tabs "udf" >}}
{{< tab "Java/Scala" >}}
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
{{< /tab >}}
{{< tab "Python" >}}
{{< /tab >}}
{{< /tabs >}}

**Declaration**

{{< tabs "c5e5527b-b09d-4dc5-9549-8fd2bfc7cc2a" >}}
{{< tab "Java" >}}
```java
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
```

**Bridging to JVM Types**

| Java Type                            | Input | Output | Remarks                                 |
|:-------------------------------------|:-----:|:------:|:----------------------------------------|
|*class*                               | X     | X      | Originating class or subclasses (for input) or <br>superclasses (for output). *Default* |
|`org.apache.flink.types.Row`          | X     | X      | Represent the structured type as a row. |
|`org.apache.flink.table.data.RowData` | X     | X      | Internal data structure.                |

{{< /tab >}}
{{< tab "Scala" >}}
```scala
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
```

**Bridging to JVM Types**

| Java Type                            | Input | Output | Remarks                                 |
|:-------------------------------------|:-----:|:------:|:----------------------------------------|
|*class*                               | X     | X      | Originating class or subclasses (for input) or <br>superclasses (for output). *Default* |
|`org.apache.flink.types.Row`          | X     | X      | Represent the structured type as a row. |
|`org.apache.flink.table.data.RowData` | X     | X      | Internal data structure.                |

{{< /tab >}}
{{< tab "Python" >}}
```python
Not supported.
```
{{< /tab >}}
{{< /tabs >}}

### Other Data Types

#### `BOOLEAN`

Data type of a boolean with a (possibly) three-valued logic of `TRUE`, `FALSE`, and `UNKNOWN`.

**Declaration**

{{< tabs "8dfc33fb-c7e5-43c4-ade0-bfc197d46aef" >}}
{{< tab "SQL" >}}
```text
BOOLEAN
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.BOOLEAN()
```

**Bridging to JVM Types**

| Java Type          | Input | Output | Remarks                              |
|:-------------------|:-----:|:------:|:-------------------------------------|
|`java.lang.Boolean` | X     | X      | *Default*                            |
|`boolean`           | X     | (X)    | Output only if type is not nullable. |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.BOOLEAN()
```
{{< /tab >}}
{{< /tabs >}}

#### `RAW`

Data type of an arbitrary serialized type. This type is a black box within the table ecosystem
and is only deserialized at the edges.

The raw type is an extension to the SQL standard.

**Declaration**

{{< tabs "529ac5b5-0fb4-4004-a2da-0a9915b2b36b" >}}
{{< tab "SQL" >}}
```text
RAW('class', 'snapshot')
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.RAW(class, serializer)

DataTypes.RAW(class)
```

**Bridging to JVM Types**

| Java Type         | Input | Output | Remarks                              |
|:------------------|:-----:|:------:|:-------------------------------------------|
|*class*            | X     | X      | Originating class or subclasses (for input) or <br>superclasses (for output). *Default* |
|`byte[]`           |       | X      |                                      |
|`org.apache.flink.table.data.RawValueData` | X     | X      | Internal data structure. |

{{< /tab >}}
{{< tab "Python" >}}
```python
Not supported.
```
{{< /tab >}}
{{< /tabs >}}

{{< tabs "raw" >}}
{{< tab "SQL/Java/Scala" >}}
The type can be declared using `RAW('class', 'snapshot')` where `class` is the originating class and
`snapshot` is the serialized `TypeSerializerSnapshot` in Base64 encoding. Usually, the type string is not
declared directly but is generated while persisting the type.

In the API, the `RAW` type can be declared either by directly supplying a `Class` + `TypeSerializer` or
by passing `Class` and letting the framework extract `Class` + `TypeSerializer` from there.
{{< /tab >}}
{{< tab "Python" >}}
{{< /tab >}}
{{< /tabs >}}

#### `NULL`

Data type for representing untyped `NULL` values.

The null type is an extension to the SQL standard. A null type has no other value
except `NULL`, thus, it can be cast to any nullable type similar to JVM semantics.

This type helps in representing unknown types in API calls that use a `NULL` literal
as well as bridging to formats such as JSON or Avro that define such a type as well.

This type is not very useful in practice and is just mentioned here for completeness.

**Declaration**

{{< tabs "45f4daee-66be-4a39-a445-874d79e7b248" >}}
{{< tab "SQL" >}}
```text
NULL
```
{{< /tab >}}
{{< tab "Java/Scala" >}}
```java
DataTypes.NULL()
```

**Bridging to JVM Types**

| Java Type         | Input | Output | Remarks                              |
|:------------------|:-----:|:------:|:-------------------------------------|
|`java.lang.Object` | X     | X      | *Default*                            |
|*any class*        |       | (X)    | Any non-primitive type.              |

{{< /tab >}}
{{< tab "Python" >}}
```python
Not supported.
```
{{< /tab >}}
{{< /tabs >}}

Data Type Extraction
--------------------

{{< tabs "extraction" >}}
{{< tab "Java/Scala" >}}
At many locations in the API, Flink tries to automatically extract data type from class information using
reflection to avoid repetitive manual schema work. However, extracting a data type reflectively is not always
successful because logical information might be missing. Therefore, it might be necessary to add additional
information close to a class or field declaration for supporting the extraction logic.

The following table lists classes that can be implicitly mapped to a data type without requiring further information.

If you intend to implement classes in Scala, *it is recommended to use boxed types* (e.g. `java.lang.Integer`)
instead of Scala's primitives. Scala's primitives (e.g. `Int` or `Double`) are compiled to JVM primitives (e.g.
`int`/`double`) and result in `NOT NULL` semantics as shown in the table below. Furthermore, Scala primitives that
are used in generics (e.g. `java.util.Map[Int, Double]`) are erased during compilation and lead to class
information similar to `java.util.Map[java.lang.Object, java.lang.Object]`.

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
| `java.time.Instant`         | `TIMESTAMP_LTZ(9)`                  |
| `java.time.Duration`        | `INVERVAL SECOND(9)`                |
| `java.time.Period`          | `INTERVAL YEAR(4) TO MONTH`         |
| `byte[]`                    | `BYTES`                             |
| `T[]`                       | `ARRAY<T>`                          |
| `java.util.Map<K, V>`       | `MAP<K, V>`                         |
| structured type `T`         | anonymous structured type `T`       |

Other JVM bridging classes mentioned in this document require a `@DataTypeHint` annotation.

_Data type hints_ can parameterize or replace the default extraction logic of individual function parameters
and return types, structured classes, or fields of structured classes. An implementer can choose to what
extent the default extraction logic should be modified by declaring a `@DataTypeHint` annotation.

The `@DataTypeHint` annotation provides a set of optional hint parameters. Some of those parameters are shown in the
following example. More information can be found in the documentation of the annotation class.
{{< /tab >}}
{{< tab "Python" >}}
{{< /tab >}}
{{< /tabs >}}

{{< tabs "1387e5a8-9081-43be-8e11-e5fbeab0f123" >}}
{{< tab "Java" >}}
```java
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
```
{{< /tab >}}
{{< tab "Scala" >}}
```java
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
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Not supported.
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
