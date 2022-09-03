---
title: "Data Types"
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

{{< tabs "datatypes" >}}
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
val t: DataType = DataTypes.TIMESTAMP(3).bridgedTo(classOf[java.sql.Timestamp])

// tell the runtime to not produce or consume boxed integer arrays
// but primitive int arrays
val t: DataType = DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(classOf[Array[Int]])
```
{{< /tab >}}
{{< /tabs >}}

<span class="label label-danger">Attention</span> Please note that physical hints are usually only required if the
API is extended. Users of predefined sources/sinks/functions do not need to define such hints. Hints within
a table program (e.g. `field.cast(TIMESTAMP(3).bridgedTo(Timestamp.class))`) are ignored.

数据类型列表
------------------
这部分列举了所有预定义的数据类型。
{{< tabs "datatypesimport" >}}
{{< tab "Java/Scala" >}}
Java/Scala Table API 的数据类型详见： `org.apache.flink.table.api.DataTypes`.
{{< /tab >}}
{{< tab "Python" >}}
Python Table API, 的数据类型详见： `pyflink.table.types.DataTypes`.
{{< /tab >}}
{{< /tabs >}}

默认 `Planner` 支持下列 SQL 数据类型：

| 数据类型             | 备注                                    |
|:-----------------|:--------------------------------------|
| `CHAR`           |                                       |
| `VARCHAR`        |                                       |
| `STRING`         |                                       |
| `BOOLEAN`        |                                       |
| `BINARY`         |                                       |
| `VARBINARY`      |                                       |
| `BYTES`          |                                       |
| `DECIMAL`        | 支持固定精度和范围。                            |
| `TINYINT`        |                                       |
| `SMALLINT`       |                                       |
| `INTEGER`        |                                       |
| `BIGINT`         |                                       |
| `FLOAT`          |                                       |
| `DOUBLE`         |                                       |
| `DATE`           |                                       |
| `TIME`           | 仅支持到0.的精度。                            |
| `TIMESTAMP`      |                                       |
| `TIMESTAMP_LTZ`  |                                       |
| `INTERVAL`       | 仅支持从 `MONTH` 详细到 `SECOND(3）`而组成的时间范围。 |
| `ARRAY`          |                                       |
| `MULTISET`       |                                       |
| `MAP`            |                                       |
| `ROW`            |                                       |
| `RAW`            |                                       |
| Structured types | 目前只在用户自定义函数中开放。                       |

### 字符串

#### `CHAR`
定长字符串的数据类型。

**声明**

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

**JVM Types 桥接**

| Java Type                                | 输入 | 输入 | 备注           |
|:-----------------------------------------|:-----:|:------:|:-------------|
| `java.lang.String`                       |   X   |   X    | *默认*         |
| `byte[]`                                 |   X   |   X    | 假定 UTF-8 编码。 |
| `org.apache.flink.table.data.StringData` |   X   |   X    | 内部数据结构。      |

{{< /tab >}}
{{< tab "Python" >}}
```python
不支持。
```
{{< /tab >}}
{{< /tabs >}}

可以使用 `CHAR(n)` 声明该类型，其中 n 表示其长度，`n` 的取值范围为 `1` 到 `2,147,483,647` (包括两数在内) ，如果没有指定 `n` 的值，n 等于 1。 


#### `VARCHAR` / `STRING`
变长字符串的数据类型。

**声明**

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

**JVM Types 桥接**

| Java Type                                | 输入 | 输入 | 备注           |
|:-----------------------------------------|:-----:|:------:|:-------------|
| `java.lang.String`                       |   X   |   X    | *默认*         |
| `byte[]`                                 |   X   |   X    | 假定 UTF-8 编码。 |
| `org.apache.flink.table.data.StringData` |   X   |   X    | 内部数据结构。      |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.VARCHAR(n)

DataTypes.STRING()
```

<span class="label label-danger">请注意</span> 目前 `DataTypes.VARCHAR(n)` 中指定的最大值 `n` 必须为 `2,147,483,647`。
{{< /tab >}}
{{< /tabs >}}

可以使用 `VARCHAR(n)` 声明该类型，其中 `n` 表示其最大值。`n` 的取值范围为 `1` 到 `2,147,483,647` (包括两数在内) ，如果没有指定 `n` 的值，n 等于 1。

`STRING` 等价于 `VARCHAR(2147483647)`。

### 二进制字符串

#### `BINARY`
固定长度二进制字符串的数据类型（可以理解为是一种字节序列）。

**声明**

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

**JVM Types 桥接**

| Java Type          | 输入 | 输入 | 备注   |
|:-------------------|:-----:|:------:|:-----|
|`byte[]`            | X     | X      | *默认* |

{{< /tab >}}
{{< tab "Python" >}}
```python
不支持。
```
{{< /tab >}}
{{< /tabs >}}

可以使用 `BINARY(n)` 声明该类型，其中 `n` 表示字节数。 `n` 的取值范围为 `1` 到 `2,147,483,647` (包括两数在内) ，如果没有指定 `n` 的值， n 等于 1。


#### `VARBINARY` / `BYTES`

可变长度的二进制字符串的数据类型（可以理解为是一种字节序列）。

**声明**

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

**JVM Types 桥接**

| Java Type          | 输入 | 输出 | 备注 |
|:-------------------|:-----:|:------:|:--------|
|`byte[]`            | X     | X      | *默认*    |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.VARBINARY(n)

DataTypes.BYTES()
```

<span class="label label-danger">请注意</span> 当前，`DataTypes.VARBINARY(n)` 中指定的最大字节数 `n` 必须是 `2,147,483,647`。
{{< /tab >}}
{{< /tabs >}}

可以使用 `VARBINARY(n)` 声明该类型，其中 `n` 表示字节数的最大值。 `n` 的取值范围为 `1` 到 `2,147,483,647` (包括两数在内) ，如果没有指定 `n` 的值， n 等于 1 。


`BYTES` 等价于 `VARBINARY(2147483647)`。

### 数值

#### `DECIMAL`

具有固定精度和范围的十进制数。

**声明**

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

**JVM Types 桥接**

| Java Type                                | 输入 | 输出 | 备注                  |
|:-----------------------------------------|:-----:|:------:|:-------------------------|
|`java.math.BigDecimal`                    | X     | X      | *默认*                |
|`org.apache.flink.table.data.DecimalData` | X     | X      | 内部数据结构。 |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.DECIMAL(p, s)
```

<span class="label label-danger">请注意</span> 当前， `DataTypes.DECIMAL(p, s)` 中指定的 `precision` 和 `scale` 当前必须分别为`38`和`18`。
{{< /tab >}}
{{< /tabs >}}

可以使用 `DECIMAL(p, s)` 声明类型，其中 `p` 是该数字中整数部分的位数(*precision*) ， `s` 是该数字小数点右边的位数（*scale*）。
`p` 的取值范围是`1`到`38`(包括两数在内) , `s` 的取值范围是 `0`到 `p` (包括两值在内) 。 `p` 的默认值为 10。 `s` 的默认值为 0。


`NUMERIC(p, s)` 等价于 `DEC(p, s)` 。

#### `TINYINT`

单字节的有符号整数类型，存储 `-128` 到 `127` 的整数。

**声明**

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

**JVM Types 桥接**

| Java Type          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Byte`    | X     | X      | *默认*                                    |
|`byte`              | X     | (X)    | 仅当类型不可为空时才输出。         |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.TINYINT()
```
{{< /tab >}}
{{< /tabs >}}

#### `SMALLINT`
2字节的有符号整数类型，存储`-32,768` 到 `32,767` 的整数。

**声明**

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

**JVM Types 桥接**

| Java Type          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Short`   | X     | X      | *默认*                                    |
|`short`             | X     | (X)    | 仅当类型不可为空时才输出。         |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.SMALLINT()
```
{{< /tab >}}
{{< /tabs >}}

#### `INT`
4字节的有符号整数类型，存储 `-2,147,483,648` 到 `2,147,483,647` 的整数。

**声明**

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

**JVM Types 桥接**

| Java Type          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Integer` | X     | X      | *默认*                                    |
|`int`               | X     | (X)    | 仅当类型不可为空时才输出。         |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.INT()
```
{{< /tab >}}
{{< /tabs >}}

`INTEGER` 是这种类型的同义词。

#### `BIGINT`

8字节的有符号整数类型，存储 `-9,223,372,036,854,775,808` 到 `9,223,372,036,854,775,807`的整数 。

**声明**

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

**JVM Types 桥接**

| Java Type          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Long`    | X     | X      | *默认*                                    |
|`long`              | X     | (X)    | 仅当类型不可为空时才输出。         |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.BIGINT()
```
{{< /tab >}}
{{< /tabs >}}

### 近似数值

#### `FLOAT`

4字节单精度浮点数类型。
与 SQL 标准相比，该类型不带参数。

**声明**

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

**JVM Types 桥接**

| Java Type          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Float`   | X     | X      | *默认*                                    |
|`float`             | X     | (X)    | 仅当类型不可为空时才输出。         |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.FLOAT()
```
{{< /tab >}}
{{< /tabs >}}

#### `DOUBLE`

8字节单精度浮点数类型

**声明**

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

**JVM Types 桥接**

| Java Type          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Double`  | X     | X      | *默认*                                    |
|`double`            | X     | (X)    | 仅当类型不可为空时才输出。         |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.DOUBLE()
```
{{< /tab >}}
{{< /tabs >}}

`DOUBLE PRECISION` 是这种类型的同义词。

### 日期和时间

#### `DATE`

日期的数据类型，由 `年-月-日` 组成，取值范围为 `0000-01-01` 到 `9999-12-31` 。
与 SQL 标准相比，范围从 `0000` 年开始。

**声明**

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

**JVM Types 桥接**

| Java Type            | 输入 | 输出 | 备注                              |
|:---------------------|:-----:|:------:|:--------------------------------|
|`java.time.LocalDate` | X     | X      | *默认*                            |
|`java.sql.Date`       | X     | X      |                                 |
|`java.lang.Integer`   | X     | X      | 描述自基准时间点以来的天数。                  |
|`int`                 | X     | (X)    | 描述自基准时间点以来的天数。<br>仅当类型不可为空时才输出。 |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.DATE()
```
{{< /tab >}}
{{< /tabs >}}

#### `TIME`

无时区的时间类型，包括 `小时:分钟:秒[.小数秒]`，精度可达纳秒，取值范围为` 00:00:00.000000000` 到 `23:59:59.999999999`。


{{< tabs "time" >}}
{{< tab "SQL/Java/Scala" >}}
与SQL标准相比，因为闰秒（23:59:60和23:59:61）语义接近 java.time.LocalTime 而不被支持。没有提供带时区的时间。

{{< /tab >}}
{{< tab "Python" >}}

与 SQL 标准相比，不支持闰秒（`23:59:60` 和 `23:59:61`）。不提供 *带有* 时区的时间。

{{< /tab >}}
{{< /tabs >}}

**声明**

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

**JVM Types 桥接**

| Java Type            | 输入 | 输出 | 备注                                                                                      |
|:---------------------|:-----:|:------:|:----------------------------------------------------------------------------------------|
|`java.time.LocalTime` | X     | X      | *默认*                                                                                    |
|`java.sql.Time`       | X     | X      |                                                                                         |
|`java.lang.Integer`   | X     | X      | 描述一天中的毫秒数。                                                                              |
|`int`                 | X     | (X)    | 描述一天中的毫秒数。<br>仅当类型不可为空时才输出。                                      |
|`java.lang.Long`      | X     | X      | 描述一天中的纳秒数。                                         |
|`long`                | X     | (X)    | 描述一天中的纳秒数。<br>仅当类型不可为空时才输出。 |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.TIME(p)
```

<span class="label label-danger">请注意</span> 当前，`DataTypes.TIME(p)` 中指定的 `精度` 必须为 `0`。
{{< /tab >}}
{{< /tabs >}}

可以使用 `TIME(p)` 声明该类型，其中 `p` 是小数秒的位数（精度）。 p 的取值范围为`0`到`9`(包括两数在内)。 如果未指定精度，则 p 等于 0。

#### `TIMESTAMP`

有时区的时间类型，由 `年-月-日 小时:分钟:秒[.小数秒]`组成，可达到纳秒级别的精度，范围从`0000-01-01 00:00:00.000000000`到 `9999-12-31 23:59:59.999999999`。

{{< tabs "timestamps" >}}
{{< tab "SQL/Java/Scala" >}}
与SQL标准相比，因为闰秒（23:59:60和23:59:61）语义接近 `java.time.LocalDateTime` 而不被支持。

不支持 `BIGINT` 转为其他类型或其他类型转为 `BIGINT` ，因为这意味着时区。 但是，这种类型是无时区限制。 有关更多类似`java.time.Instant”的语义`，请使用`TIMESTAMP_LTZ`。

{{< /tab >}}
{{< tab "Python" >}}

与 SQL 标准相比，不支持闰秒（`23:59:60` 和 `23:59:61`）。

不支持 `BIGINT` 转为其他类型或其他类型转为 `BIGINT` ，因为这意味着时区。 但是，这种类型无时区限制。 如果您有这样的需求，请使用 `TIMESTAMP_LTZ`。

{{< /tab >}}
{{< /tabs >}}

**声明**

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

**JVM Types 桥接**

| Java Type                                  | 输入 | 输出 | 备注                  |
|:-------------------------------------------|:-----:|:------:|:-------------------------|
|`java.time.LocalDateTime`                   | X     | X      | *默认*                |
|`java.sql.Timestamp`                        | X     | X      |                          |
|`org.apache.flink.table.data.TimestampData` | X     | X      | 内部数据结构。 |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.TIMESTAMP(p)
```

<span class="label label-danger">请注意</span> 当前，`DataTypes.TIMESTAMP(p)` 中指定的 `精度` 必须为 `3`。
{{< /tab >}}
{{< /tabs >}}

可以使用 `TIMESTAMP(p)` 声明该类型，其中 `p` 是小数秒的位数（*精度*）。 `p` 的取值范围为`0`到`9`(包括两数在内)。如果没有指定精度，`p` 等于 6。

`TIMESTAMP(p) WITHOUT TIME ZONE` 是这种类型的同义词。

#### `TIMESTAMP WITH TIME ZONE`

包含时区的时间戳的数据类型，格式为 `年-月-日 时：分：秒[.小数秒]`，精度可达纳秒，取值范围为 `0000-01-01 00:00:00.000000000 +14:59` 至 `9999 -12-31 23:59:59.999999999 -14:59`。

{{< tabs "timestamps" >}}
{{< tab "SQL/Java/Scala" >}}
与SQL标准相比，因为闰秒（23:59:60和23:59:61）语义接近 `java.time.OffsetDateTime` 而不被支持。
{{< /tab >}}
{{< tab "Python" >}}
与 SQL 标准相比，不支持闰秒（`23:59:60` 和 `23:59:61`）。
{{< /tab >}}
{{< /tabs >}}

与 `TIMESTAMP_LTZ` 相比，时区偏移信息物理存储在每条数据中，便于每一次计算、数据显示、与外部系统进行通信。


**声明**

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

**JVM Types 桥接**

| Java Type                 | 输入 | 输出 | 备注        |
|:--------------------------|:-----:|:------:|:----------|
|`java.time.OffsetDateTime` | X     | X      | *默认*      |
|`java.time.ZonedDateTime`  | X     |        | 忽略时区 ID 。 |

{{< /tab >}}
{{< tab "Python" >}}
```python
不支持。
```
{{< /tab >}}
{{< /tabs >}}

{{< tabs "timestamps" >}}
{{< tab "SQL/Java/Scala" >}}
{{< /tab >}}
{{< tab "Python" >}}
{{< /tab >}}
{{< /tabs >}}

可以使用 `TIMESTAMP(p) WITH TIME ZONE` 声明该类型，其中 `p` 是小数秒的位数（*precision*）。 `p` 取值范围为`0`到`9`(包括两数在内)。如果不指定精度，`p` 等于 6。

#### `TIMESTAMP_LTZ`

带有*本地*时区的时间戳数据类型 ，格式为`年-月-日 时:分:秒[.小数秒] zone` ，精度高达纳秒，值范围从 `0000-01-01 00:00:00.000000000 +14:59` 到 `9999-12-31 23:59:59.999999999 -14:59`。

{{< tabs "timestamps" >}}
{{< tab "SQL/Java/Scala" >}}

因为闰秒（23:59:60和23:59:61）语义接近 `java.time.OffsetDateTime` 而不被支持。

与 `TIMESTAMP WITH TIME ZONE` 相比，时区偏移信息没有物理存储在每个数据中。 相反，该类型在 UTC 时区假定为 `java.time.Instant` 的语义处于 table 生态系统的边缘。
每个数据都会配置成当前会话中配置的本地时区，以进行计算和可视化。

{{< /tab >}}
{{< tab "Python" >}}

不支持闰秒（`23:59:60` 和 `23:59:61`）。
与 `TIMESTAMP WITH TIME ZONE` 相比，时区偏移信息没有物理存储在每条数据中。每个数据都在当前会话中配置的本地时区进行解释，以进行计算和可视化。

{{< /tab >}}
{{< /tabs >}}

此类型采用：根据配置好的会话时区来解析 UTC 时间戳的方式，填补了无时区和时区强制时间戳类型之间的空白。

**声明**

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

**JVM Types 桥接**

| Java Type          | 输入 | 输出 | 备注                                           |
|:-------------------|:-----:|:------:|:--------------------------------------------------|
|`java.time.Instant` | X     | X      | *默认*                                         |
|`java.lang.Integer` | X     | X      | 描述自基准时间点以来的秒数。      |
|`int`               | X     | (X)    | 描述自基准时间点以来的秒数。<br>仅当类型不可为空时才输出。 |
|`java.lang.Long`    | X     | X      | 描述自基准时间点以来的毫秒数。 |
|`long`              | X     | (X)    | 描述自基准时间点以来的毫秒数。<br>仅当类型不可为空时才输出。 |
|`java.sql.Timestamp`| X     | X      | 描述自基准时间点以来的毫秒数。 |
|`org.apache.flink.table.data.TimestampData` | X     | X      | 内部数据结构。 |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.TIMESTAMP_LTZ(p)
DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(p)
```

<span class="label label-danger">请注意</span> 目前 `DataTypes.TIMESTAMP_LTZ(p)` 中指定的 `精度` 必须为 `3`。
{{< /tab >}}
{{< /tabs >}}

可以使用 `TIMESTAMP_LTZ(p)` 声明该类型，其中 `p` 是小数秒的位数（精度）。 `p` 取值范围为`0`到`9`(包括两数在内)。如果未指定精度，则 `p` 等于 6。

`TIMESTAMP(p) WITH LOCAL TIME ZONE` 是这种类型的同义词。

#### `INTERVAL YEAR TO MONTH`
一组年月间隔类型的数据类型。

必须将类型参数化为下列粒度中的一种：
- 年间隔，
- 年-月间隔，
- 月的间隔。
年-月的间隔由 `+年-月`组成，取值范围从 `-9999-11` 到 `+9999-11` 。

所有类型的粒度的值表示都是一样的。 例如，50 个月的间隔总是以年-月的格式表示（默认年份精度）：`+04-02`。

**声明**

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

**JVM Types 桥接**

| Java Type          | 输入 | 输出 | 备注                     |
|:-------------------|:-----:|:------:|:-----------------------|
|`java.time.Period`  | X     | X      | 忽略 `天` 的部分。 *默认*       |
|`java.lang.Integer` | X     | X      | 描述月数。                  |
|`int`               | X     | (X)    | 描述月数。<br>仅当类型不可为空时才输出。 |

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


可以使用上述组合声明类型，其中 `p` 是年份的位数（年份精度）。 `p` 取值范围为`1`到`4`(包括两数在内)。 如果未指定年份精度，则 `p` 等于 2。

#### `INTERVAL DAY TO SECOND`

一组日时间间隔类型的数据类型。

必须将类型参数化为以下粒度之一，精度最高可达纳秒：
- 天的间隔，
- 天小时间隔，
- 天-分间隔，
- 天-秒间隔，
- 小时间隔，
- 小时-分钟间隔，
- 小时-秒间隔，
- 分钟间隔，
- 分钟-秒间隔，
- 或秒的间隔。

白天时间间隔由 `+days hours:months:seconds.fractional` 组成，其值范围为
`-999999 23:59:59.999999999` 到`+999999 23:59:59.999999999`。 所有类型的粒度的值表示都是一样的。例如，70秒的间隔始终表示为天-秒的间隔格式（具有默认精度）：`+00 00:01:10.000000`。

**声明**

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

**JVM Types 桥接**

| Java Type           | 输入 | 输出 | 备注                               |
|:--------------------|:-----:|:------:|:--------------------------------------|
|`java.time.Duration` | X     | X      | *默认*                             |
|`java.lang.Long`     | X     | X      | 描述毫秒数。 |
|`long`               | X     | (X)    | 描述毫秒数。<br>仅当类型不可为空时才输出。 |

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


可以使用上述组合声明类型，其中 `p1` 是天数 (*日精度*）和 `p2` 是小数秒的位数（*小数精度*）。
`p1` 的取值范围为`1`到`6`(包括两数在内)。 `p2` 的值取值范围为`0`到`9`(包括两数在内)。
如果没有指定 `p1` 的值， `p1` 默认等于`2`。 如果没有指定 `p2` 的值，默认等于 `6`。

### 结构型数据类型

#### `ARRAY`
具有相同子类型的元素数组的数据类型。

与 SQL 标准相比，数组的最大基数不能指定，而是固定为 `2,147,483,647` 。 此外，支持任何有效类型作为子类型。

**声明**

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

**JVM Types 桥接**

| Java Type                              | 输入 | 输出 | 备注                           |
|:---------------------------------------|:-----:|:------:|:----------------------------------|
|*t*`[]`                                 | (X)   | (X)    | 取决于子类型。 *默认* |
| `java.util.List<t>`                    | X     | X      |                                   |
| *subclass* of `java.util.List<t>`      | X     |        |                                   |
|`org.apache.flink.table.data.ArrayData` | X     | X      | 内部数据结构。          |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.ARRAY(t)
```
{{< /tab >}}
{{< /tabs >}}


可以使用 `ARRAY<t>` 声明类型，`t` 是其中元素的数据类型。。
为了更接近 SQL 标准，也可以使用 t ARRAY 的写法表达，比如，INT ARRAY 等同于 ARRAY。

#### `MAP`
将键（包括 `NULL` ）映射到值（包括 `NULL` ）的关联数组的数据类型。
map 中不能包含重复的键；每个键最多可以映射到一个值。
元素类型没有限制；用户应确保其唯一性。
map 类型是 SQL 标准的扩展。
**声明**

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

**JVM Types 桥接**

| Java Type                             | 输入 | 输出 | 备注                  |
|:--------------------------------------|:-----:|:------:|:-------------------------|
| `java.util.Map<kt, vt>`               | X     | X      | *默认*                |
| *subclass* of `java.util.Map<kt, vt>` | X     |        |                          |
|`org.apache.flink.table.data.MapData`  | X     | X      | 内部数据结构。 |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.MAP(kt, vt)
```
{{< /tab >}}
{{< /tabs >}}

可以使用 `MAP<kt, vt>` 声明类型，其中 `kt` 是键的数据类型 ， `vt` 是值的数据类型。


#### `MULTISET`

多重集 (=bag) 的数据类型。 与集合不同的是，它允许每个具有共同子类型的元素有多个实例。 每个唯一值（包括 `NULL` ）都映射到某个多重性。
元素类型没有限制；用户应确保其唯一性。


**声明**

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

**JVM Types 桥接**

| Java Type                             | 输入 | 输出 | 备注                          |
|:--------------------------------------|:-----:|:------:|:----------------------------|
|`java.util.Map<t, java.lang.Integer>`  | X     | X      | 将每个值指定成一个 integer 类的值。 *默认* |
| *subclass* of `java.util.Map<t, java.lang.Integer>>` | X     |        |                             |
|`org.apache.flink.table.data.MapData`  | X     | X      | 内部数据结构。                     |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.MULTISET(t)
```
{{< /tab >}}
{{< /tabs >}}


可以使用 `MULTISET<t>` 声明类型，其中 `t` 是包含元素的数据类型。
`t MULTISET` 是为更接近 SQL 标准而使用的同义写法。 例如，`INT MULTISET` 等同于 `MULTISET<INT>`。


#### `ROW`

字段序列的数据类型。

字段由字段名称、字段类型和可选描述组成。 表中的行的最特有的类型就是 row 类型。 在这种情况下，行的每一列对应于字段与列具有相同序号位置的行类型。
与 SQL 标准相比，可选的字段描述可以简化复杂结构的处理过程。
row 类型类似于其他非标准兼容框架中的 `STRUCT` 类型。

**声明**

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

**JVM Types 桥接**

| Java Type                            | 输入 | 输出 | 备注                  |
|:-------------------------------------|:-----:|:------:|:-------------------------|
|`org.apache.flink.types.Row`          | X     | X      | *默认*                |
|`org.apache.flink.table.data.RowData` | X     | X      | 内部数据结构。 |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.ROW([DataTypes.FIELD(n0, t0), DataTypes.FIELD(n1, t1), ...])
DataTypes.ROW([DataTypes.FIELD(n0, t0, d0), DataTypes.FIELD(n1, t1, d1), ...])
```
{{< /tab >}}
{{< /tabs >}}


可以使用 `ROW<n0 t0 'd0', n1 t1 'd1', ...>` 声明类型，其中 `n` 代表一个字段，`t` 是字段的逻辑类型，`d` 是字段的描述。
`ROW(...)` 是为更接近 SQL 标准而使用的同义写法。 例如，`ROW(myField INT, myOtherField BOOLEAN)` 等同于 `ROW<myField INT, myOtherField BOOLEAN>` 。


### 用户自定义的数据类型

{{< tabs "udf" >}}
{{< tab "Java/Scala" >}}
<span class="label label-danger">请注意用户定义的数据类型尚未完全支持。 目前（从 Flink 1.11 开始）仅在参数和函数返回类型中作为未注册的结构化类型开放。

结构化类型类似于面向对象编程语言中的对象。 它包含零个、一个或多个属性。 每个属性都由名称和类型组成。

有两种结构化类型:

- 存储在目录中并由 _目录标识符_ 标识的类型（如 `cat.db.MyType` ）。 这些等同于结构化类型的 SQL 标准定义。

- 匿名定义的、未注册的类型（通常是反射提取的），由 一个 _实现类_ （如 `com.myorg.model.MyType` ）。 
  这些在以编程方式时很有用定义表程序。 它们可以重用现有的 JVM 类，而无需手动定义数据类型的模式。

#### 注册的结构化类型

不支持注册的结构化类型。 因此该类型无法存储在 catalog 中，也无法在 `CREATE TABLE` 这样的 `DDL` 语句中引用。


#### 未注册的结构化类型
可以使用自动反射提取从常规 POJO（普通旧 Java 对象）创建未注册的结构化类型。

结构化类型的实现类必须满足以下要求：
- 必须保证全局可访问，因此必须被修饰为 `public` ， `static` 而不能使用 `abstract` 。
- 必须提供一个缺省构造函数或一个完整构造函数。
- 所有字段都必须可读 ，可以通过 `public` 修饰 或遵循 common 的 getter 读取编码风格，例如 `getField()`、`isField()`、`field()`。
- 所有字段都必须可写 ，可以通过 `public` 修饰、完全赋值构造函数写入，或遵循常见编码样式的设置器，例如 `setField(...)`、`field(...)`。
- 所有字段必须通过反射提取隐式或显式映射到数据类型 使用 `@DataTypeHint` [注释](#data-type-annotations)。
- 使用 `static` or `transient` 修饰的字段将被忽略。

反射提取支持字段的任意嵌套，只要字段类型不（及物）指代自己。

声明的字段类（例如 `public int age;`）必须包含在支持的 JVM 列表中 
为本文档中的每种数据类型定义的桥接类（例如，`java.lang.Integer` 或 `int` for `INT`）。

对于某些类，需要注释才能将类映射到数据类型（例如 `@DataTypeHint("DECIMAL(10, 2)")` 为 `java.math.BigDecimal` 分配一个固定的精度和范围）。

{{< /tab >}}
{{< tab "Python" >}}
{{< /tab >}}
{{< /tabs >}}

**声明**

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

**JVM Types 桥接**

| Java Type                            | 输入 | 输出 | 备注                               |
|:-------------------------------------|:-----:|:------:|:---------------------------------|
|*class*                               | X     | X      | 原始类或子类（用于输入）或者<br>父类 (用于输出)。*默认* |
|`org.apache.flink.types.Row`          | X     | X      | 将结构化类型表示为一行。                     |
|`org.apache.flink.table.data.RowData` | X     | X      | 内部数据结构。                          |

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

**JVM Types 桥接**

| Java Type                            | 输入 | 输出 | 备注                            |
|:-------------------------------------|:-----:|:------:|:------------------------------|
|*class*                               | X     | X      | 原始类或子类（用于输入）<br>或者父类 (用于输出)。 *默认* |
|`org.apache.flink.types.Row`          | X     | X      | 将结构化类型表示为一行。                  |
|`org.apache.flink.table.data.RowData` | X     | X      | 内部数据结构。                       |

{{< /tab >}}
{{< tab "Python" >}}
```python
Not supported.
```
{{< /tab >}}
{{< /tabs >}}

### 其他

#### `BOOLEAN`

布尔值的数据类型，具有（可能）三值逻辑 `TRUE`、`FALSE` 和 `UNKNOWN`。

**声明**

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

**JVM Types 桥接**

| Java Type          | 输入 | 输出 | 备注                              |
|:-------------------|:-----:|:------:|:-------------------------------------|
|`java.lang.Boolean` | X     | X      | *默认*                            |
|`boolean`           | X     | (X)    | 仅当类型不可为空时才输出。 |

{{< /tab >}}
{{< tab "Python" >}}
```python
DataTypes.BOOLEAN()
```
{{< /tab >}}
{{< /tabs >}}

#### `RAW`

任意序列化类型的数据类型。 这种类型是表生态系统中的黑匣子并且仅在边缘反序列化。原始类型是 SQL 标准的扩展。

**声明**

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

**JVM Types 桥接**

| Java Type         | 输入 | 输出 | 备注                              |
|:------------------|:-----:|:------:|:-------------------------------------------|
|*class*            | X     | X      | 原始类或子类（用于输入）<br>或者父类 (用于输出)。 *默认* |
|`byte[]`           |       | X      |                                      |
|`org.apache.flink.table.data.RawValueData` | X     | X      | 内部数据结构。 |

{{< /tab >}}
{{< tab "Python" >}}
```python
不支持。
```
{{< /tab >}}
{{< /tabs >}}

{{< tabs "raw" >}}
{{< tab "SQL/Java/Scala" >}}
可以使用 `RAW('class', 'snapshot')` 声明类型，其中 `class` 是原始类，并且 `snapshot` 是 Base64 编码的序列化 `TypeSerializerSnapshot`。 
通常，类型字符串不是 直接声明但在持久化类型时生成。

在 API 中，可以通过直接提供 `Class` + `TypeSerializer` 或通过传递 `Class` 的方式，使框架可以提取到 `Class` + `TypeSerializer`。
{{< /tab >}}
{{< tab "Python" >}}
{{< /tab >}}
{{< /tabs >}}

#### `NULL`

用于表示无类型 `NULL` 值的数据类型。

null 类型是对 SQL 标准的扩展。 空类型没有其他值。
除了 `NULL`，因此，它可以转换为类似于 JVM 语义的任何可空类型。

此类型有助于在使用 `NULL` 文字的 API 调用中表示未知类型 以及桥接到 JSON 或 Avro 等定义此类类型的格式。
这种类型在实践中一般不会用到，这里只是为了完整性而提到。

**声明**

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

**JVM Types 桥接**

| Java Type         | 输入 | 输出 | 备注                              |
|:------------------|:-----:|:------:|:-------------------------------------|
|`java.lang.Object` | X     | X      | *默认*                            |
|*any class*        |       | (X)    | 任何非原始类型。              |

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

Flink Table API 和 Flink SQL 支持从 `输入` 数据类型 到 `目标` 数据类型的转换。有的转换无论输入值是什么都能保证转换成功，而有些转换则会在运行时失败（即不可能转换为 `目标` 数据类型对应的值）。
例如，将 `INT` 数据类型的值转换为 `STRING` 数据类型一定能转换成功，但无法保证将 `STRING` 数据类型转换为 `INT` 数据类型。

在生成执行计划时，Flink 的 SQL 检查器会拒绝提交那些不可能直接转换为 `目标` 数据类型的SQL，并抛出 `ValidationException` 异常，
例如从 `TIMESTAMP` 类型转化到 `INTERVAL` 类型。
然而有些查询即使通过了 SQL 检查器的验证，依旧可能会在运行期间转换失败，这就需要用户正确处理这些失败了。

在 Flink Table API 和 Flink SQL 中，可以用下面两个内置方法来进行转换操作：

* `CAST` ：定义在 SQL 标准的 CAST 方法。在某些容易发生转换失败的查询场景中，当实际输入数据不合法时，作业便会运行失败。类型推导会保留输入类型的可空性。
* `TRY_CAST` ：常规 CAST 方法的扩展，当转换失败时返回 `NULL`。该方法的返回值允许为空。

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
| `java.time.Duration`        | `INTERVAL SECOND(9)`                |
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
```scala
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
