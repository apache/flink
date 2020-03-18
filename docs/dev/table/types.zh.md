---
title: "数据类型"
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

由于历史原因，在 Flink 1.9 之前，Flink Table & SQL API 的数据类型与 Flink 的 `TypeInformation` 耦合紧密。`TypeInformation` 在 DataStream 和 DataSet API 中被使用，并且足以用来用于描述分布式环境中 JVM 对象的序列化和反序列化操作所需的全部信息。

然而,`TypeInformation` 并不是为独立于 JVM class 的逻辑类型而设计的。之前很难将 SQL 的标准类型映射到 `TypeInformation` 抽象。此外，有一些类型并不是兼容 SQL 的并且引入的时候没有长远规划过。

从 Flink 1.9 开始，Table & SQL API 开始启用一种新的类型系统作为长期解决方案，用来保持 API 稳定性和 SQL 标准的兼容性。

重新设计类型系统是一项涉及几乎所有的面向用户接口的重大工作。因此，它的引入跨越多个版本，社区的目标是在 Flink 1.10 完成这项工作。

同时由于为 Table 编程添加了新的 Planner 详见（[FLINK-11439](https://issues.apache.org/jira/browse/FLINK-11439)）, 并不是每种 Planner 都支持所有的数据类型。此外,Planner 对于数据类型的精度和参数化支持也可能是不完整的。

<span class="label label-danger">注意</span> 在使用数据类型之前请参阅 Planner 的兼容性表和局限性章节。

* This will be replaced by the TOC
{:toc}

数据类型
---------

*数据类型* 描述 Table 编程环境中的值的逻辑类型。它可以被用来声明操作的输入输出类型。

Flink 的数据类型和 SQL 标准的 *数据类型* 术语类似，但也包含了可空属性，可以被用于标量表达式（scalar expression）的优化。

数据类型的示例:
- `INT`
- `INT NOT NULL`
- `INTERVAL DAY TO SECOND(3)`
- `ROW<myField ARRAY<BOOLEAN>, myOtherField TIMESTAMP(3)>`

全部的预定义数据类型见[下面](#数据类型列表)列表。

### Table API 的数据类型

JVM API 的用户可以在 Table API 中使用 `org.apache.flink.table.types.DataType` 的实例，以及定义连接器（Connector）、Catalog 或者用户自定义函数（User-Defined Function）。

一个 `DataType` 实例有两个作用：
- **逻辑类型的声明**，它不表达具体物理类型的存储和转换，但是定义了基于 JVM 的语言和 Table 编程环境之间的边界。
- *可选的：* **向 Planner 提供有关数据的物理表示的提示**，这对于边界 API 很有用。

对于基于 JVM 的语言，所有预定义的数据类型都在 `org.apache.flink.table.api.DataTypes` 里提供。

建议使用星号将全部的 API 导入到 Table 程序中以便于使用：

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

#### 物理提示

在 Table 编程环境中，基于 SQL 的类型系统与程序指定的数据类型之间需要物理提示。该提示指出了实现预期的数据格式。

例如，Data Source 能够使用类 `java.sql.Timestamp` 来表达逻辑上的 `TIMESTAMP` 产生的值，而不是使用缺省的 `java.time.LocalDateTime`。有了这些信息，运行时就能够将产生的类转换为其内部数据格式。反过来，Data Sink 可以声明它从运行时消费的数据格式。

下面是一些如何声明桥接转换类的示例：

<div class="codetabs" markdown="1">

<div data-lang="Java" markdown="1">
{% highlight java %}
// 告诉运行时不要产生或者消费 java.time.LocalDateTime 实例
// 而是使用 java.sql.Timestamp
DataType t = DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class);

// 告诉运行时不要产生或者消费装箱的整数数组
// 而是使用基本数据类型的整数数组
DataType t = DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(int[].class);
{% endhighlight %}
</div>

<div data-lang="Scala" markdown="1">
{% highlight scala %}
// 告诉运行时不要产生或者消费 java.time.LocalDateTime 实例
// 而是使用 java.sql.Timestamp
val t: DataType = DataTypes.TIMESTAMP(3).bridgedTo(classOf[java.sql.Timestamp]);

// 告诉运行时不要产生或者消费装箱的整数数组 
// 而是使用基本数据类型的整数数组
val t: DataType = DataTypes.ARRAY(DataTypes.INT().notNull()).bridgedTo(classOf[Array[Int]]);
{% endhighlight %}
</div>

</div>

<span class="label label-danger">注意</span> 请注意，通常只有在扩展 API 时才需要物理提示。
预定义的 Source、Sink、Function 的用户不需要定义这样的提示。在 Table 编程中（例如 `field.cast(TIMESTAMP(3).bridgedTo(Timestamp.class))`）这些提示将被忽略。

Planner 兼容性
---------------------

正如简介里提到的，重新开发类型系统将跨越多个版本，每个数据类型的支持取决于使用的 Planner。本节旨在总结最重要的差异。

### 旧的 Planner

Flink 1.9 之前引入的旧的 Planner 主要支持类型信息（Type Information），它只对数据类型提供有限的支持，可以声明能够转换为类型信息的数据类型，以便旧的 Planner 能够理解它们。

下表总结了数据类型和类型信息之间的区别。大多数简单类型以及 Row 类型保持不变。Time 类型、 Array 类型和 Decimal 类型需要特别注意。不允许使用其他的类型提示。

对于 *类型信息* 列，该表省略了前缀 `org.apache.flink.table.api.Types`。

对于 *数据类型表示* 列，该表省略了前缀 `org.apache.flink.table.api.DataTypes`。

| 类型信息 | Java 表达式字符串 | 数据类型表示 | 数据类型备注 |
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
| `BIG_DEC()` | `DECIMAL` | [`DECIMAL()`] | 不是 1:1 的映射，因为精度和小数位被忽略，Java 的可变精度和小数位被使用。 |
| `SQL_DATE()` | `SQL_DATE` | `DATE()`<br>`.bridgedTo(java.sql.Date.class)` | |
| `SQL_TIME()` | `SQL_TIME` | `TIME(0)`<br>`.bridgedTo(java.sql.Time.class)` | |
| `SQL_TIMESTAMP()` | `SQL_TIMESTAMP` | `TIMESTAMP(3)`<br>`.bridgedTo(java.sql.Timestamp.class)` | |
| `INTERVAL_MONTHS()` | `INTERVAL_MONTHS` | `INTERVAL(MONTH())`<br>`.bridgedTo(Integer.class)` | |
| `INTERVAL_MILLIS()` | `INTERVAL_MILLIS` | `INTERVAL(DataTypes.SECOND(3))`<br>`.bridgedTo(Long.class)` | |
| `PRIMITIVE_ARRAY(...)` | `PRIMITIVE_ARRAY<...>` | `ARRAY(DATATYPE.notNull()`<br>`.bridgedTo(PRIMITIVE.class))` | 应用于除 `byte` 外的全部 JVM 基本数据类型。 |
| `PRIMITIVE_ARRAY(BYTE())` | `PRIMITIVE_ARRAY<BYTE>` | `BYTES()` | |
| `OBJECT_ARRAY(...)` | `OBJECT_ARRAY<...>` | `ARRAY(`<br>`DATATYPE.bridgedTo(OBJECT.class))` | |
| `MULTISET(...)` | | `MULTISET(...)` | |
| `MAP(..., ...)` | `MAP<...,...>` | `MAP(...)` | |
| 其他通用类型 | | `RAW(...)` | |

<span class="label label-danger">注意</span> 如果对于新的类型系统有任何疑问，用户可以随时切换到 `org.apache.flink.table.api.Types` 中定义的 type information。

### 新的 Blink Planner

新的 Blink Planner 支持旧的 Planner 的全部类型，尤其包括列出的 Java 表达式字符串和类型信息。

支持以下数据类型：

| 数据类型 | 数据类型的备注 |
|:----------|:----------------------|
| `STRING` | `CHAR` 和 `VARCHAR` 暂不支持。 |
| `BOOLEAN` | |
| `BYTES` | `BINARY` 和 `VARBINARY` 暂不支持。 |
| `DECIMAL` | 支持固定精度和小数位数。 |
| `TINYINT` | |
| `SMALLINT` | |
| `INTEGER` | |
| `BIGINT` | |
| `FLOAT` | |
| `DOUBLE` | |
| `DATE` | |
| `TIME` | 支持的精度仅为 `0`。 |
| `TIMESTAMP` | 支持的精度仅为 `3`。 |
| `TIMESTAMP WITH LOCAL TIME ZONE` | 支持的精度仅为 `3`。 |
| `INTERVAL` | 仅支持 `MONTH` 和 `SECOND(3)` 区间。 |
| `ARRAY` | |
| `MULTISET` | |
| `MAP` | |
| `ROW` | |
| `RAW` | |

局限性
-----------

**Java 表达式字符串**：Table API 中的 Java 表达式字符串，例如 `table.select("field.cast(STRING)")`，尚未被更新到新的类型系统中，使用[旧的 Planner 章节](#旧的-planner)中声明的字符串来表示。

**连接器描述符和 SQL 客户端**：描述符字符串的表示形式尚未更新到新的类型系统。使用在[连接到外部系统章节](./connect.html#type-strings)中声明的字符串表示。

**用户自定义函数**：用户自定义函数尚不能声明数据类型。

数据类型列表
------------------

本节列出了所有预定义的数据类型。对于基于 JVM 的 Table API，这些类型也可以从 `org.apache.flink.table.api.DataTypes` 中找到。

### 字符串

#### `CHAR`

固定长度字符串的数据类型。

**声明**

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

此类型用 `CHAR(n)` 声明，其中 `n` 表示字符数量。`n` 的值必须在 `1` 和 `2,147,483,647` 之间（含边界值）。如果未指定长度，`n` 等于 `1`。

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                 |
|:-------------------|:-----:|:------:|:------------------------|
|`java.lang.String`  | X     | X      | *缺省*               |
|`byte[]`            | X     | X      | 假设使用 UTF-8 编码。 |

#### `VARCHAR` / `STRING`

可变长度字符串的数据类型。

**声明**

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

此类型用 `VARCHAR(n)` 声明，其中 `n` 表示最大的字符数量。`n` 的值必须在 `1` 和 `2,147,483,647` 之间（含边界值）。如果未指定长度，`n` 等于 `1`。

`STRING` 等价于 `VARCHAR(2147483647)`.

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                 |
|:-------------------|:-----:|:------:|:------------------------|
|`java.lang.String`  | X     | X      | *缺省*               |
|`byte[]`            | X     | X      | 假设使用 UTF-8 编码。 |

### 二进制字符串

#### `BINARY`

固定长度二进制字符串的数据类型（=字节序列）。

**声明**

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

此类型用 `BINARY(n)` 声明，其中 `n` 是字节数量。`n` 的值必须在 `1` 和 `2,147,483,647` 之间（含边界值）。如果未指定长度，`n` 等于 `1`。

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                 |
|:-------------------|:-----:|:------:|:------------------------|
|`byte[]`            | X     | X      | *缺省*               |

#### `VARBINARY` / `BYTES`

可变长度二进制字符串的数据类型（=字节序列）。

**声明**

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

此类型用 `VARBINARY(n)` 声明，其中 `n` 是最大的字节数量。`n` 的值必须在 `1` 和 `2,147,483,647` 之间（含边界值）。如果未指定长度，`n` 等于 `1`。

`BYTES` 等价于 `VARBINARY(2147483647)`。

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                 |
|:-------------------|:-----:|:------:|:------------------------|
|`byte[]`            | X     | X      | *缺省*               |

### 精确数值

#### `DECIMAL`

精度和小数位数固定的十进制数字的数据类型。

**声明**

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

此类型用 `DECIMAL(p, s)` 声明，其中 `p` 是数字的位数（*精度*），`s` 是数字中小数点右边的位数（*尾数*）。`p` 的值必须介于 `1` 和 `38` 之间（含边界值）。`s` 的值必须介于 `0` 和 `p` 之间（含边界值）。其中 `p` 的缺省值是 `10`，`s` 的缺省值是 `0`。

`NUMERIC(p, s)` 和 `DEC(p, s)` 都等价于这个类型。

**JVM 类型**

| Java 类型             | 输入 | 输出 | 备注                 |
|:----------------------|:-----:|:------:|:------------------------|
|`java.math.BigDecimal` | X     | X      | *缺省*               |

#### `TINYINT`

1 字节有符号整数的数据类型，其值从 `-128` to `127`。

**声明**

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

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Byte`    | X     | X      | *缺省*                                    |
|`byte`              | X     | (X)    | 仅当类型不可为空时才输出。 |

#### `SMALLINT`

2 字节有符号整数的数据类型，其值从 `-32,768` 到 `32,767`。

**声明**

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

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Short`   | X     | X      | *缺省*                                    |
|`short`             | X     | (X)    | 仅当类型不可为空时才输出。 |

#### `INT`

4 字节有符号整数的数据类型，其值从 `-2,147,483,648` 到 `2,147,483,647`。

**声明**

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

`INTEGER` 等价于此类型。

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Integer` | X     | X      | *缺省*                                    |
|`int`               | X     | (X)    | 仅当类型不可为空时才输出。 |

#### `BIGINT`

8 字节有符号整数的数据类型，其值从 `-9,223,372,036,854,775,808` 到 `9,223,372,036,854,775,807`。

**声明**

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

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Long`    | X     | X      | *缺省*                                    |
|`long`              | X     | (X)    | 仅当类型不可为空时才输出。 |

### 近似数值

#### `FLOAT`

4 字节单精度浮点数的数据类型。

与 SQL 标准相比，该类型不带参数。

**声明**

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

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Float`   | X     | X      | *缺省*                                    |
|`float`             | X     | (X)    | 仅当类型不可为空时才输出。 |

#### `DOUBLE`

8 字节双精度浮点数的数据类型。

**声明**

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

`DOUBLE PRECISION` 等价于此类型。

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Double`  | X     | X      | *缺省*                                    |
|`double`            | X     | (X)    | 仅当类型不可为空时才输出。 |

### 日期和时间

#### `DATE`

日期的数据类型由 `year-month-day` 组成，范围从 `0000-01-01` 到 `9999-12-31`。

与 SQL 标准相比，年的范围从 `0000` 开始。

**声明**

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

**JVM 类型**

| Java 类型            | 输入 | 输出 | 备注                                      |
|:---------------------|:-----:|:------:|:---------------------------------------------|
|`java.time.LocalDate` | X     | X      | *缺省*                                    |
|`java.sql.Date`       | X     | X      |                                              |
|`java.lang.Integer`   | X     | X      | 描述从 Epoch 算起的天数。    |
|`int`                 | X     | (X)    | 描述从 Epoch 算起的天数。<br>仅当类型不可为空时才输出。 |

#### `TIME`

*不带*时区的时间数据类型，由 `hour:minute:second[.fractional]` 组成，精度达到纳秒，范围从 `00:00:00.000000000` 到 `23:59:59.999999999`。

与 SQL 标准相比，不支持闰秒（`23:59:60` 和 `23:59:61`），语义上更接近于 `java.time.LocalTime`。没有提供*带有*时区的时间。

**声明**

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

此类型用 `TIME(p)` 声明，其中 `p` 是秒的小数部分的位数（*精度*）。`p` 的值必须介于 `0` 和 `9` 之间（含边界值）。如果未指定精度，则 `p` 等于 `0`。

**JVM 类型**

| Java 类型            |输入 |输出 |备注                                             |
|:---------------------|:-----:|:------:|:----------------------------------------------------|
|`java.time.LocalTime` | X     | X      | *缺省*                                           |
|`java.sql.Time`       | X     | X      |                                                     |
|`java.lang.Integer`   | X     | X      | 描述自当天以来的毫秒数。    |
|`int`                 | X     | (X)    | 描述自当天以来的毫秒数。<br>仅当类型不可为空时才输出。 |
|`java.lang.Long`      | X     | X      | 描述自当天以来的纳秒数。     |
|`long`                | X     | (X)    | 描述自当天以来的纳秒数。<br>仅当类型不可为空时才输出。 |

#### `TIMESTAMP`

*不带*时区的时间戳数据类型，由 `year-month-day hour:minute:second[.fractional]` 组成，精度达到纳秒，范围从 `0000-01-01 00:00:00.000000000` 到 `9999-12-31 23:59:59.999999999`。

与 SQL 标准相比，不支持闰秒（`23:59:60` 和 `23:59:61`），语义上更接近于 `java.time.LocalDateTime`。

不支持和 `BIGINT`（JVM `long` 类型）互相转换，因为这意味着有时区，然而此类型是无时区的。对于语义上更接近于 `java.time.Instant` 的需求请使用 `TIMESTAMP WITH LOCAL TIME ZONE`。

**声明**

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

此类型用 `TIMESTAMP(p)` 声明，其中 `p` 是秒的小数部分的位数（*精度*）。`p` 的值必须介于 `0` 和 `9` 之间（含边界值）。如果未指定精度，则 `p` 等于 `6`。

`TIMESTAMP(p) WITHOUT TIME ZONE` 等价于此类型。

**JVM 类型**

| Java 类型                | 输入 | 输出 | 备注                                             |
|:-------------------------|:-----:|:------:|:----------------------------------------------------|
|`java.time.LocalDateTime` | X     | X      | *缺省*                                           |
|`java.sql.Timestamp`      | X     | X      |                                                     |

#### `TIMESTAMP WITH TIME ZONE`

*带有*时区的时间戳数据类型，由 `year-month-day hour:minute:second[.fractional] zone` 组成，精度达到纳秒，范围从 `0000-01-01 00:00:00.000000000 +14:59` 到 
`9999-12-31 23:59:59.999999999 -14:59`。

与 SQL 标准相比，不支持闰秒（`23:59:60` 和 `23:59:61`），语义上更接近于 `java.time.OffsetDateTime`。

与 `TIMESTAMP WITH LOCAL TIME ZONE` 相比，时区偏移信息物理存储在每个数据中。它单独用于每次计算、可视化或者与外部系统的通信。

**声明**

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

此类型用 `TIMESTAMP(p) WITH TIME ZONE` 声明，其中 `p` 是秒的小数部分的位数（*精度*）。`p` 的值必须介于 `0` 和 `9` 之间（含边界值）。如果未指定精度，则 `p` 等于 `6`。

**JVM 类型**

| Java 类型                 | 输入 | 输出 | 备注              |
|:--------------------------|:-----:|:------:|:---------------------|
|`java.time.OffsetDateTime` | X     | X      | *缺省*            |
|`java.time.ZonedDateTime`  | X     |        | 忽略时区 ID。 |

#### `TIMESTAMP WITH LOCAL TIME ZONE`

*带有本地*时区的时间戳数据类型，由 `year-month-day hour:minute:second[.fractional] zone` 组成，精度达到纳秒，范围从 `0000-01-01 00:00:00.000000000 +14:59` 到 
`9999-12-31 23:59:59.999999999 -14:59`。

不支持闰秒（`23:59:60` 和 `23:59:61`），语义上更接近于 `java.time.OffsetDateTime`。

与 `TIMESTAMP WITH TIME ZONE` 相比，时区偏移信息并非物理存储在每个数据中。相反，此类型在 Table 编程环境的 UTC 时区中采用 `java.time.Instant` 语义。每个数据都在当前会话中配置的本地时区中进行解释，以便用于计算和可视化。

此类型允许根据配置的会话时区来解释 UTC 时间戳，从而填补了时区无关和时区相关的时间戳类型之间的鸿沟。

**声明**

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

此类型用 `TIMESTAMP(p) WITH LOCAL TIME ZONE` 声明，其中 `p` 是秒的小数部分的位数（*精度*）。`p` 的值必须介于 `0` 和 `9` 之间（含边界值）。如果未指定精度，则 `p` 等于 `6`。

**JVM 类型**

| Java 类型          |输入 |输出 |备注                                           |
|:-------------------|:-----:|:------:|:--------------------------------------------------|
|`java.time.Instant` | X     | X      | *缺省*                                         |
|`java.lang.Integer` | X     | X      | 描述从 Epoch 算起的秒数。      |
|`int`               | X     | (X)    | 描述从 Epoch 算起的秒数。<br>仅当类型不可为空时才输出。 |
|`java.lang.Long`    | X     | X      | 描述从 Epoch 算起的毫秒数。 |
|`long`              | X     | (X)    | 描述从 Epoch 算起的毫秒数。<br>仅当类型不可为空时才输出。 |

#### `INTERVAL YEAR TO MONTH`

一组 Year-Month Interval 数据类型。

此类型必被参数化为以下情况中的一种：
- Year 时间间隔、
- Year-Month 时间间隔、
- Month 时间间隔。

Year-Month Interval 由 `+years-months` 组成，其范围从 `-9999-11` 到 `+9999-11`。

所有类型的表达能力均相同。例如，Month 时间间隔下的 `50` 等价于 Year-Month 时间间隔（缺省年份精度）下的 `+04-02`。

**声明**

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

可以使用以上组合来声明类型，其中 `p` 是年数（*年精度*）的位数。`p` 的值必须介于 `1` 和 `4` 之间（含边界值）。如果未指定年精度，`p` 则等于 `2`。

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                            |
|:-------------------|:-----:|:------:|:-----------------------------------|
|`java.time.Period`  | X     | X      | 忽略 `days` 部分。 *缺省* |
|`java.lang.Integer` | X     | X      | 描述月的数量。    |
|`int`               | X     | (X)    | 描述月的数量。<br>仅当类型不可为空时才输出。 |

#### `INTERVAL DAY TO MONTH`

一组 Day-Time Interval 数据类型。

此类型达到纳秒精度，必被参数化为以下情况中的一种：
- Day 时间间隔、
- Day-Hour 时间间隔、
- Day-Minute 时间间隔、
- Day-Second 时间间隔、
- Hour 时间间隔、
- Hour-Minute 时间间隔、
- Hour-Second 时间间隔、
- Minute 时间间隔、
- Minute-Second 时间间隔、
- Second 时间间隔。

Day-Time 时间间隔由 `+days hours:months:seconds.fractional` 组成，其范围从 `-999999 23:59:59.999999999` 到 `+999999 23:59:59.999999999`。

所有类型的表达能力均相同。例如，Second 时间间隔下的 `70` 等价于 Day-Second 时间间隔（缺省精度）下的 `+00 00:01:10.000000`。

**声明**

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

可以使用以上组合来声明类型，其中 `p1` 是天数（*天精度*）的位数，`p2` 是秒的小数部分的位数（*小数精度*）。`p1` 的值必须介于 `1` 和之间 `6`（含边界值），`p2` 的值必须介于 `0` 和之间 `9`（含边界值）。如果 `p1` 未指定值，则缺省等于 `2`，如果 `p2` 未指定值，则缺省等于 `6`。

**JVM 类型**

| Java 类型           | 输入 | 输出 | 备注                               |
|:--------------------|:-----:|:------:|:--------------------------------------|
|`java.time.Duration` | X     | X      | *缺省*                             |
|`java.lang.Long`     | X     | X      | 描述毫秒数。 |
|`long`               | X     | (X)    | 描述毫秒数。<br>仅当类型不可为空时才输出。 |

### 结构化的数据类型

#### `ARRAY`

具有相同子类型元素的数组的数据类型。

与 SQL 标准相比，无法指定数组的最大长度，而是被固定为 `2,147,483,647`。另外，任何有效类型都可以作为子类型。

**声明**

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

此类型用 `ARRAY<t>` 声明，其中 `t` 是所包含元素的数据类型。

`t ARRAY` 接近等价于 SQL 标准。例如，`INT ARRAY` 等价于 `ARRAY<INT>`。

**JVM 类型**

| Java 类型 | 输入 | 输出 | 备注                           |
|:----------|:-----:|:------:|:----------------------------------|
|*t*`[]`    | (X)   | (X)    | 依赖于子类型。 *缺省* |

#### `MAP`

将键（包括 `NULL`）映射到值（包括 `NULL`）的关联数组的数据类型。映射不能包含重复的键；每个键最多可以映射到一个值。

元素类型没有限制；确保唯一性是用户的责任。

Map 类型是 SQL 标准的扩展。

**声明**

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

此类型用 `MAP<kt, vt>` 声明，其中 `kt` 是键的数据类型，`vt` 是值的数据类型。

**JVM 类型**

| Java 类型                             | 输入 | 输出 | 备注   |
|:--------------------------------------|:-----:|:------:|:----------|
| `java.util.Map<kt, vt>`               | X     | X      | *缺省* |
| `java.util.Map<kt, vt>` 的*子类型* | X     |        |           |

#### `MULTISET`

多重集合的数据类型（=bag）。与集合不同的是，它允许每个具有公共子类型的元素有多个实例。每个唯一值（包括 `NULL`）都映射到某种多重性。

元素类型没有限制；确保唯一性是用户的责任。

**声明**

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

此类型用 `MULTISET<t>` 声明，其中 `t` 是所包含元素的数据类型。

`t MULTISET` 接近等价于 SQL 标准。例如，`INT MULTISET` 等价于 `MULTISET<INT>`。

**JVM 类型**

| Java 类型                            | 输入 | 输出 | 备注                                                  |
|:-------------------------------------|:-----:|:------:|:---------------------------------------------------------|
|`java.util.Map<t, java.lang.Integer>` | X     | X      | 将每个值可多重地分配给一个整数 *缺省* |
|`java.util.Map<kt, java.lang.Integer>` 的*子类型*| X     |        | 将每个值可多重地分配给一个整数 |

#### `ROW`

字段序列的数据类型。

字段由字段名称、字段类型和可选的描述组成。表中的行的是最特殊的类型是 Row 类型。在这种情况下，行中的每一列对应于相同位置的列的 Row 类型的字段。

与 SQL 标准相比，可选的字段描述简化了复杂结构的处理。

Row 类型类似于其他非标准兼容框架中的 `STRUCT` 类型。

**声明**

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

此类型用 `ROW<n0 t0 'd0', n1 t1 'd1', ...>` 声明，其中 `n` 是唯一的字段名称，`t` 是字段的逻辑类型，`d` 是字段的描述。

`ROW(...)` 接近等价于 SQL 标准。例如，`ROW(myField INT, myOtherField BOOLEAN)` 等价于 `ROW<myField INT, myOtherField BOOLEAN>`。

**JVM 类型**

| Java 类型                   | 输入 | 输出 | 备注                 |
|:----------------------------|:-----:|:------:|:------------------------|
|`org.apache.flink.types.Row` | X     | X      | *缺省*               |

### 其他数据类型

#### `BOOLEAN`

（可能）具有 `TRUE`、`FALSE` 和 `UNKNOWN` 三值逻辑的布尔数据类型。

**声明**

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

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                              |
|:-------------------|:-----:|:------:|:-------------------------------------|
|`java.lang.Boolean` | X     | X      | *缺省*                            |
|`boolean`           | X     | (X)    | 仅当类型不可为空时才输出。 |

#### `NULL`

表示空类型 `NULL` 值的数据类型。

NULL 类型是 SQL 标准的扩展。NULL 类型除 `NULL` 值以外没有其他值，因此可以将其强制转换为 JVM 里的任何可空类型。

此类型有助于使用 `NULL` 字面量表示 `API` 调用中的未知类型，以及桥接到定义该类型的 JSON 或 Avro 等格式。

这种类型在实践中不是很有用，为完整起见仅在此提及。

**声明**

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

**JVM 类型**

| Java 类型         | 输入 | 输出 | 备注                              |
|:------------------|:-----:|:------:|:-------------------------------------|
|`java.lang.Object` | X     | X      | *缺省*                            |
|*任何类型*        |       | (X)    | 任何非基本数据类型              |

#### `RAW`

任意序列化类型的数据类型。此类型是 Table 编程环境中的黑箱，仅在边缘反序列化。

Raw 类型是 SQL 标准的扩展。

**声明**

<div class="codetabs" markdown="1">

<div data-lang="SQL" markdown="1">
{% highlight text %}
RAW('class', 'snapshot')
{% endhighlight %}
</div>

<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
DataTypes.RAW(class, serializer)

DataTypes.RAW(typeInfo)
{% endhighlight %}
</div>

</div>

此类型用 `RAW('class', 'snapshot')` 声明，其中 `class` 是原始类，`snapshot` 是 Base64 编码的序列化的 `TypeSerializerSnapshot`。通常，类型字符串不是直接声明的，而是在保留类型时生成的。

在 API 中，可以通过直接提供 `Class` + `TypeSerializer` 或通过传递 `TypeInformation` 并让框架从那里提取 `Class` + `TypeSerializer` 来声明 `RAW` 类型。

**JVM 类型**

| Java 类型         | 输入 | 输出 | 备注                              |
|:------------------|:-----:|:------:|:-------------------------------------------|
|*类型*            | X     | X      | 原始类或子类（用于输入）或超类（用于输出）。 *缺省* |
|`byte[]`           |       | X      |                                      |

{% top %}
