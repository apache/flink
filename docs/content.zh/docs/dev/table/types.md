---
title: "数据类型"
weight: 21
type: docs
aliases:
  - /zh/dev/table/types.html
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

<a name="data-types"></a>

# 数据类型

Flink SQL 为用户提供了一系列丰富的原始数据类型。

<a name="data-type"></a>

数据类型
---------

在 Flink 的 Table 生态系统中，*数据类型* 描述了数据的逻辑类型，可以用来表示转换过程中输入、输出的类型。

Flink 的数据类型类似于 SQL 标准中的术语*数据类型*，但包含了值的可空性，以便于更好地处理标量表达式。

以下是一些数据类型的例子：
- `INT`
- `INT NOT NULL`
- `INTERVAL DAY TO SECOND(3)`
- `ROW<myField ARRAY<BOOLEAN>, myOtherField TIMESTAMP(3)>`

可在[下文](#list-of-data-types)中找到所有预先定义好的数据类型。

<a name="data-types-in-the-table-api"></a>

### Table API 中的数据类型

{{< tabs "datatypes" >}}
{{< tab "Java/Scala" >}}
在定义 connector、catalog、用户自定义函数时，使用 JVM 相关 API 的用户可能会使用到 Table API 中基于 `org.apache.flink.table.types.DataType` 的一些实例。

`数据类型` 实例有两个职责：
- **作为逻辑类型的表现形式**，定义 JVM 类语言或 Python 语言与 Table 生态系统的边界，而不是以具体的物理表现形式存在于数据的传输过程或存储中。
- *可选的:* 在与其他 API 进行数据交换时，**为 Planner 提供这些数据物理层面的相关提示**。

对于基于 JVM 的语言，所有预定义的数据类型都可以在 `org.apache.flink.table.api.DataTypes` 下找到。
{{< /tab >}}
{{< tab "Python" >}}
在 Python 语言定义用户自定义函数时，使用 Python API 的用户
可能会使用到 Python API 中基于 `pyflink.table.types.DataType` 的一些实例。

`数据类型` 实例有如下职责：
- **作为逻辑类型的表现形式**，定义 JVM 类语言或 Python 语言与 Table 生态系统的边界，而不是以具体的物理表现形式存在于数据的传输过程或存储中。

对于 Python 语言，这些类型可以在 `pyflink.table.types.DataTypes` 下找到。
{{< /tab >}}
{{< /tabs >}}

{{< tabs "84cf5e1c-c899-42cb-8fdf-6ae59fdd012c" >}}
{{< tab "Java" >}}
使用 Table API 编程时，建议使用星号引入所有相关依赖，以获得更流畅的 API 使用体验：
```java
import static org.apache.flink.table.api.DataTypes.*;

DataType t = INTERVAL(DAY(), SECOND(3));
```
{{< /tab >}}
{{< tab "Scala" >}}
使用 Table API 编程时，建议使用星号引入所有相关依赖，以获得更流畅的 API 使用体验：

```scala
import org.apache.flink.table.api.DataTypes._

val t: DataType = INTERVAL(DAY(), SECOND(3))
```
{{< /tab >}}
{{< tab "Python" >}}

```python
from pyflink.table.types import DataTypes

t = DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND(3))
```
{{< /tab >}}
{{< /tabs >}}

<a name="physical-hints"></a>

#### 物理提示

在Table 生态系统中，当需要将 SQL 中的数据类型对应到实际编程语言中的数据类型时，就需要有物理提示。物理提示明确了对应过程中应该使用哪种数据格式。

比如，在 source 端产生数据时，可以规定：`TIMESTAMP` 的逻辑类型，在底层要使用 `java.sql.Timestamp` 这个类表示，而不是使用默认的 `java.time.LocalDateTime` 类。有了物理提示，可以帮助 Flink 运行时根据提供的类将数据转换为其内部数据格式。同样在 sink 端，定义好数据格式，以便能从 Flink 运行时获取、转换数据。

下面的例子展示了如何声明一个桥接转换类：

{{< tabs "hints" >}}
{{< tab "Java" >}}
```java
// 告诉 Flink 运行时使用 java.sql.Timestamp 处理数据，而不是 java.time.LocalDateTime
DataType t = DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class);

// 告诉 Flink 运行时使用基本的 int 数组来处理数据，而不是用包装类 Integer 数组
DataType t = DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(int[].class);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// 告诉 Flink 运行时使用 java.sql.Timestamp 处理数据，而不是 java.time.LocalDateTime
val t: DataType = DataTypes.TIMESTAMP(3).bridgedTo(classOf[java.sql.Timestamp])

// 告诉 Flink 运行时使用基本的 int 数组来处理数据，而不是用包装类 Integer 数组
val t: DataType = DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(classOf[Array[Int]])
```
{{< /tab >}}
{{< /tabs >}}

<span class="label label-danger">注意</span> 请记住，只有在扩展 API 时才需要使用到物理提示。使用预定义的 source、sink 以及 Flink 函数时，不需要用到物理提示。在使用 Table API 编写程序时，Flink 会忽略物理提示（例如 `field.cast(TIMESTAMP(3).bridgedTo(Timestamp.class))`）。

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

The default planner supports the following set of SQL types:

| Data Type        | Remarks for Data Type                              |
|:-----------------|:---------------------------------------------------|
| `CHAR`           |                                                    |
| `VARCHAR`        |                                                    |
| `STRING`         |                                                    |
| `BOOLEAN`        |                                                    |
| `BINARY`         |                                                    |
| `VARBINARY`      |                                                    |
| `BYTES`          |                                                    |
| `DECIMAL`        | Supports fixed precision and scale.                |
| `TINYINT`        |                                                    |
| `SMALLINT`       |                                                    |
| `INTEGER`        |                                                    |
| `BIGINT`         |                                                    |
| `FLOAT`          |                                                    |
| `DOUBLE`         |                                                    |
| `DATE`           |                                                    |
| `TIME`           | Supports only a precision of `0`.                  |
| `TIMESTAMP`      |                                                    |
| `TIMESTAMP_LTZ`  |                                                    |
| `INTERVAL`       | Supports only interval of `MONTH` and `SECOND(3)`. |
| `ARRAY`          |                                                    |
| `MULTISET`       |                                                    |
| `MAP`            |                                                    |
| `ROW`            |                                                    |
| `RAW`            |                                                    |
| Structured types | Only exposed in user-defined functions yet.        |

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
|`java.sql.Timestamp`| X     | X      | Describes the number of milliseconds since epoch. |
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

ROW(n0 t0, n1 t1, ...)
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

- Types that are stored in a catalog and are identified by a _catalog identifier_ (like `cat.db.MyType`). Those
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

<a name="casting"></a>

CAST 方法
-------

Flink Table API 和 Flink SQL 支持从 `输入` 数据类型 到 `目标` 数据类型的转换。有的转换
无论输入值是什么都能保证转换成功，而有些转换则会在运行时失败（即不可能转换为 `目标` 数据类型对应的值）。
例如，将 `INT` 数据类型的值转换为 `STRING` 数据类型一定能转换成功，但无法保证将 `STRING` 数据类型转换为 `INT` 数据类型。

在生成执行计划时，Flink 的 SQL 检查器会拒绝提交那些不可能直接转换为 `目标` 数据类型的SQL，并抛出 `ValidationException` 异常，
例如从 `TIMESTAMP` 类型转化到 `INTERVAL` 类型。
然而有些查询即使通过了 SQL 检查器的验证，依旧可能会在运行期间转换失败，这就需要用户正确处理这些失败了。

在 Flink Table API 和 Flink SQL 中，可以用下面两个内置方法来进行转换操作：

* `CAST`：定义在 SQL 标准的 CAST 方法。在某些容易发生转换失败的查询场景中，当实际输入数据不合法时，作业便会运行失败。类型推导会保留输入类型的可空性。
* `TRY_CAST`：常规 CAST 方法的扩展，当转换失败时返回 `NULL`。该方法的返回值允许为空。

例如：

```sql
CAST('42' AS INT) --- 结果返回数字 42 的 INT 格式（非空）
CAST(NULL AS VARCHAR) --- 结果返回 VARCHAR 类型的空值
CAST('non-number' AS INT) --- 抛出异常，并停止作业

TRY_CAST('42' AS INT) --- 结果返回数字 42 的 INT 格式
TRY_CAST(NULL AS VARCHAR) --- 结果返回 VARCHAR 类型的空值
TRY_CAST('non-number' AS INT) --- 结果返回 INT 类型的空值
COALESCE(TRY_CAST('non-number' AS INT), 0) --- 结果返回数字 0 的 INT 格式（非空）
```

下表展示了各个类型的转换程度，"Y" 表示支持，"!" 表示转换可能会失败，"N" 表示不支持：

| 输入类型\目标类型                              | `CHAR`¹/<br/>`VARCHAR`¹/<br/>`STRING` | `BINARY`¹/<br/>`VARBINARY`¹/<br/>`BYTES` | `BOOLEAN` | `DECIMAL` | `TINYINT` | `SMALLINT` | `INTEGER` | `BIGINT` | `FLOAT` | `DOUBLE` | `DATE` | `TIME` | `TIMESTAMP` | `TIMESTAMP_LTZ` | `INTERVAL` | `ARRAY` | `MULTISET` | `MAP` | `ROW` | `STRUCTURED` | `RAW` |
|:---------------------------------------|:-------------------------------------:|:----------------------------------------:|:---------:|:---------:|:---------:|:----------:|:---------:|:--------:|:-------:|:--------:|:------:|:------:|:-----------:|:---------------:|:----------:|:-------:|:----------:|:-----:|:-----:|:------------:|:-----:|
| `CHAR`/<br/>`VARCHAR`/<br/>`STRING`    |                   Y                   |                    !                     |     !     |     !     |     !     |     !      |     !     |    !     |    !    |    !     |   !    |   !    |      !      |        !        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `BINARY`/<br/>`VARBINARY`/<br/>`BYTES` |                   Y                   |                    Y                     |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `BOOLEAN`                              |                   Y                   |                    N                     |     Y     |     Y     |     Y     |     Y      |     Y     |    Y     |    Y    |    Y     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `DECIMAL`                              |                   Y                   |                    N                     |     N     |     Y     |     Y     |     Y      |     Y     |    Y     |    Y    |    Y     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `TINYINT`                              |                   Y                   |                    N                     |     Y     |     Y     |     Y     |     Y      |     Y     |    Y     |    Y    |    Y     |   N    |   N    |     N²      |       N²        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `SMALLINT`                             |                   Y                   |                    N                     |     Y     |     Y     |     Y     |     Y      |     Y     |    Y     |    Y    |    Y     |   N    |   N    |     N²      |       N²        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `INTEGER`                              |                   Y                   |                    N                     |     Y     |     Y     |     Y     |     Y      |     Y     |    Y     |    Y    |    Y     |   N    |   N    |     N²      |       N²        |     Y⁵     |    N    |     N      |   N   |   N   |      N       |   N   |
| `BIGINT`                               |                   Y                   |                    N                     |     Y     |     Y     |     Y     |     Y      |     Y     |    Y     |    Y    |    Y     |   N    |   N    |     N²      |       N²        |     Y⁶     |    N    |     N      |   N   |   N   |      N       |   N   |
| `FLOAT`                                |                   Y                   |                    N                     |     N     |     Y     |     Y     |     Y      |     Y     |    Y     |    Y    |    Y     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `DOUBLE`                               |                   Y                   |                    N                     |     N     |     Y     |     Y     |     Y      |     Y     |    Y     |    Y    |    Y     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `DATE`                                 |                   Y                   |                    N                     |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   Y    |   N    |      Y      |        Y        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `TIME`                                 |                   Y                   |                    N                     |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   N    |   Y    |      Y      |        Y        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `TIMESTAMP`                            |                   Y                   |                    N                     |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   Y    |   Y    |      Y      |        Y        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `TIMESTAMP_LTZ`                        |                   Y                   |                    N                     |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   Y    |   Y    |      Y      |        Y        |     N      |    N    |     N      |   N   |   N   |      N       |   N   |
| `INTERVAL`                             |                   Y                   |                    N                     |     N     |     N     |     N     |     N      |    Y⁵     |    Y⁶    |    N    |    N     |   N    |   N    |      N      |        N        |     Y      |    N    |     N      |   N   |   N   |      N       |   N   |
| `ARRAY`                                |                   Y                   |                    N                     |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   N    |   N    |      N      |        N        |     N      |   !³    |     N      |   N   |   N   |      N       |   N   |
| `MULTISET`                             |                   Y                   |                    N                     |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   N    |   N    |      N      |        N        |     N      |    N    |     !³     |   N   |   N   |      N       |   N   |
| `MAP`                                  |                   Y                   |                    N                     |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |  !³   |   N   |      N       |   N   |
| `ROW`                                  |                   Y                   |                    N                     |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |   N   |  !³   |      N       |   N   |
| `STRUCTURED`                           |                   Y                   |                    N                     |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |   N   |   N   |      !³      |   N   |
| `RAW`                                  |                   Y                   |                    !                     |     N     |     N     |     N     |     N      |     N     |    N     |    N    |    N     |   N    |   N    |      N      |        N        |     N      |    N    |     N      |   N   |   N   |      N       |  Y⁴   |

备注：

1. 所有转化到具有固长或变长的类型时会根据类型的定义来裁剪或填充数据。
2. 使用 `TO_TIMESTAMP` 方法和 `TO_TIMESTAMP_LTZ` 方法的场景，不要使用 `CAST` 或 `TRY_CAST`。
3. 支持转换，当且仅当用其内部数据结构也支持转化时。转换可能会失败，当且仅当用其内部数据结构也可能会转换失败。
4. 支持转换，当且仅当用使用 `RAW` 的类和类的序列化器一样。
5. 支持转换，当且仅当用使用 `INTERVAL` 做“月”到“年”的转换。
6. 支持转换，当且仅当用使用 `INTERVAL` 做“天”到“时间”的转换。

请注意：无论是 `CAST` 还是 `TRY_CAST`，当输入为 `NULL` ，输出也为 `NULL`。

<a name="legacy-casting"></a>

### 旧版本 CAST 方法

用户可以通过将参数 `table.exec.legacy-cast-behaviour` 设置为 `enabled` 来启用 1.15 版本之前的 CAST 行为。
在 Flink 1.15 版本此参数默认为 disabled。

如果设置为 enabled，请注意以下问题：

* 转换为 `CHAR`/`VARCHAR`/`BINARY`/`VARBINARY` 数据类型时，不再自动修剪（trim）或填充（pad）。
* 使用 `CAST` 时不再会因为转化失败而停止作业，只会返回 `NULL`，但不会像 `TRY_CAST` 那样推断正确的类型。
* `CHAR`/`VARCHAR`/`STRING` 的转换结果会有一些细微的差别。

{{< hint warning >}}
我们 **不建议** 配置此参数，而是 **强烈建议** 在新项目中保持这个参数为默认禁用，以使用最新版本的 CAST 方法。
在下一个版本，这个参数会被移除。
{{< /hint >}}

数据类型提取
--------------------

{{< tabs "extraction" >}}
{{< tab "Java/Scala" >}}
在 API 中的很多地方，Flink 都尝试利用反射机制从类信息中自动提取数据类型，以避免重复地手动定义 schema。但是，通过反射提取数据类型并不总是有效的，因为有可能会缺失逻辑信息。因此，可能需要在类或字段声明的附近添加额外信息以支持提取逻辑。

下表列出了无需更多信息即可隐式映射到数据类型的类。

如果你打算在 Scala 中实现类，*建议使用包装类型*（例如 `java.lang.Integer`）而不是 Scala 的基本类型。如下表所示，Scala 的基本类型（例如 `Int` 或 `Double`）会被编译为 JVM 基本类型（例如 `int`/`double`）并产生 `NOT NULL` 语义。此外，在泛型中使用的 Scala 基本类型（例如 `java.util.Map[Int, Double]`）在编译期间会被擦除，导致类信息类似于 `java.util.Map[java.lang.Object, java.lang.Object]`。

| 类                          | 数据类型                             |
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
| `java.time.Duration`        | `INTERVAL SECOND(9)`                |
| `java.time.Period`          | `INTERVAL YEAR(4) TO MONTH`         |
| `byte[]`                    | `BYTES`                             |
| `T[]`                       | `ARRAY<T>`                          |
| `java.util.Map<K, V>`       | `MAP<K, V>`                         |
| 结构化类型       `T`         | 匿名结构化类型 `T`                    |

本文档中提到的其他 JVM 桥接类需要 `@DataTypeHint` 注释。

_数据类型 hints_ 可以参数化或替换单个函数参数和返回类型、结构化类或结构化类的字段的默认提取逻辑。实现者可以通过声明 `@DataTypeHint` 注解来选择默认提取逻辑的修改程度。

`@DataTypeHint` 注解提供了一组可选的 hint 参数。其中一些参数如以下示例所示。更多信息可以在注解类的文档中找到。
{{< /tab >}}
{{< tab "Python" >}}
{{< /tab >}}
{{< /tabs >}}

{{< tabs "1387e5a8-9081-43be-8e11-e5fbeab0f123" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.table.annotation.DataTypeHint;

class User {

    // 使用默认转换类 `java.lang.Integer` 定义 INT 数据类型
    public @DataTypeHint("INT") Object o;

    // 使用显式转换类定义毫秒精度的 TIMESTAMP 数据类型
    public @DataTypeHint(value = "TIMESTAMP(3)", bridgedTo = java.sql.Timestamp.class) Object o;

    // 通过强制使用 RAW 类型来丰富提取
    public @DataTypeHint("RAW") Class<?> modelClass;

    // 定义所有出现的 java.math.BigDecimal（包含嵌套字段）都将被提取为 DECIMAL(12, 2)
    public @DataTypeHint(defaultDecimalPrecision = 12, defaultDecimalScale = 2) AccountStatement stmt;

    // 定义当类型不能映射到数据类型时，总是将其视为 RAW 类型，而不是抛出异常
    public @DataTypeHint(allowRawGlobally = HintFlag.TRUE) ComplexModel model;
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.table.annotation.DataTypeHint

class User {

    // 使用默认转换类 `java.lang.Integer` 定义 INT 数据类型
    @DataTypeHint("INT")
    var o: AnyRef

    // 使用显式转换类定义毫秒精度的 TIMESTAMP 数据类型
    @DataTypeHint(value = "TIMESTAMP(3)", bridgedTo = java.sql.Timestamp.class)
    var o: AnyRef

    // 通过强制使用 RAW 类型来丰富提取
    @DataTypeHint("RAW")
    var modelClass: Class[_]

    // 定义所有出现的 java.math.BigDecimal（包含嵌套字段）都将被提取为 DECIMAL(12, 2)
    @DataTypeHint(defaultDecimalPrecision = 12, defaultDecimalScale = 2)
    var stmt: AccountStatement

    // 定义当类型不能映射到数据类型时，总是将其视为 RAW 类型，而不是抛出异常
    @DataTypeHint(allowRawGlobally = HintFlag.TRUE)
    var model: ComplexModel
}
```
{{< /tab >}}
{{< tab "Python" >}}
```python
不支持。
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
