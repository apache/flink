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

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
由于历史原因，在 Flink 1.9 之前，Flink Table & SQL API 的数据类型与 Flink 的 `TypeInformation` 耦合紧密。`TypeInformation` 在 DataStream 和 DataSet API 中被使用，并且足以用来用于描述分布式环境中 JVM 对象的序列化和反序列化操作所需的全部信息。

然而,`TypeInformation` 并不是为独立于 JVM class 的逻辑类型而设计的。之前很难将 SQL 的标准类型映射到 `TypeInformation` 抽象。此外，有一些类型并不是兼容 SQL 的并且引入的时候没有长远规划过。
</div>
<div data-lang="Python" markdown="1">
</div>
</div>
从 Flink 1.9 开始，Table & SQL API 开始启用一种新的类型系统作为长期解决方案，用来保持 API 稳定性和 SQL 标准的兼容性。

重新设计类型系统是一项涉及几乎所有的面向用户接口的重大工作。因此，它的引入跨越多个版本，社区的目标是在 Flink 1.12 完成这项工作。

同时由于为 Table 编程添加了新的 Planner 详见（[FLINK-11439](https://issues.apache.org/jira/browse/FLINK-11439)）, 并不是每种 Planner 都支持所有的数据类型。此外,Planner 对于数据类型的精度和参数化支持也可能是不完整的。

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="Java/Scala" markdown="1">
<span class="label label-danger">注意</span> 在使用数据类型之前请参阅 Planner 的兼容性表和局限性章节。
</div>
<div data-lang="Python" markdown="1">
</div>
</div>

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

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="Java/Scala" markdown="1">
JVM API 的用户可以在 Table API 中、定义连接器（Connector）、Catalog 或者用户自定义函数（User-Defined Function）中使用
`org.apache.flink.table.types.DataType` 的实例。

一个 `DataType` 实例有两个作用：
- **逻辑类型的声明**，它不表达具体物理类型的存储和转换，但是定义了基于 JVM 的语言或者 Python 语言和 Table 编程环境之间的边界。
- *可选的：* **向 Planner 提供有关数据的物理表示的提示**，这对于边界 API 很有用。

对于基于 JVM 的语言，所有预定义的数据类型都在 `org.apache.flink.table.api.DataTypes` 里提供。
</div>
<div data-lang="Python" markdown="1">
Python API的用户可以在 Python Table API中、Python 用户自定义函数（Python User-Defined Function）中使用
`pyflink.table.types.DataType` 的实例。

`DataType` 实例的作用：
- **逻辑类型的声明**，它不表达具体物理类型的存储和转换，但是定义了基于 JVM 的语言或者 Python 语言和 Table 编程环境之间的边界。

对于 Python 的语言，所有预定义的数据类型都在 `pyflink.table.types.DataTypes` 里提供。
</div>
</div>

<div class="codetabs" markdown="1">

<div data-lang="Java" markdown="1">
建议使用星号将全部的 API 导入到 Table 程序中以便于使用：

{% highlight java %}
import static org.apache.flink.table.api.DataTypes.*;

DataType t = INTERVAL(DAY(), SECOND(3));
{% endhighlight %}
</div>

<div data-lang="Scala" markdown="1">
建议使用星号将全部的 API 导入到 Table 程序中以便于使用：

{% highlight scala %}
import org.apache.flink.table.api.DataTypes._

val t: DataType = INTERVAL(DAY(), SECOND(3));
{% endhighlight %}
</div>

<div data-lang="Python" markdown="1">

{% highlight python %}
from pyflink.table.types import DataTypes

t = DataTypes.INTERVAL(DataTypes.DAY(), DataTypes.SECOND(3))
{% endhighlight %}

</div>
</div>

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="Java/Scala" markdown="1">
#### 物理提示

在 Table 编程环境中，基于 SQL 的类型系统与程序指定的数据类型之间需要物理提示。该提示指出了实现预期的数据格式。

例如，Data Source 能够使用类 `java.sql.Timestamp` 来表达逻辑上的 `TIMESTAMP` 产生的值，而不是使用缺省的 `java.time.LocalDateTime`。有了这些信息，运行时就能够将产生的类转换为其内部数据格式。反过来，Data Sink 可以声明它从运行时消费的数据格式。

下面是一些如何声明桥接转换类的示例：

<div class="codetabs" data-hide-tabs="1" markdown="1">

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

</div>
<div data-lang="Python" markdown="1">
</div>
</div>

Planner 兼容性
---------------------

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="Java/Scala" markdown="1">
正如简介里提到的，重新开发类型系统将跨越多个版本，每个数据类型的支持取决于使用的 Planner。本节旨在总结最重要的差异。
</div>
<div data-lang="Python" markdown="1">
本节仅适用于Java/Scala用户。
目前Python Table API的类型系统上还未发现类似的Planner兼容性问题。
</div>
</div>

### 旧的 Planner

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="Java/Scala" markdown="1">
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
</div>
<div data-lang="Python" markdown="1">
N/A
</div>
</div>

### 新的 Blink Planner

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="Java/Scala" markdown="1">
新的 Blink Planner 支持旧的 Planner 的全部类型，尤其包括列出的 Java 表达式字符串和类型信息。

支持以下数据类型：

| 数据类型 | 数据类型的备注 |
|:----------|:----------------------|
| `CHAR` | |
| `VARCHAR` | |
| `STRING` | |
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
| `TIMESTAMP` | |
| `TIMESTAMP WITH LOCAL TIME ZONE` | |
| `INTERVAL` | 仅支持 `MONTH` 和 `SECOND(3)` 区间。 |
| `ARRAY` | |
| `MULTISET` | |
| `MAP` | |
| `ROW` | |
| `RAW` | |
| structured types | 暂只能在用户自定义函数里使用。 |
</div>
<div data-lang="Python" markdown="1">
N/A
</div>
</div>

局限性
-----------

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="Java/Scala" markdown="1">
**Java 表达式字符串**：Table API 中的 Java 表达式字符串，例如 `table.select("field.cast(STRING)")`，尚未被更新到新的类型系统中，使用[旧的 Planner 章节](#旧的-planner)中声明的字符串来表示。

**用户自定义函数**：用户自定义聚合函数尚不能声明数据类型，标量函数和表函数充分支持数据类型。

</div>
<div data-lang="Python" markdown="1">
本节仅适用于Java/Scala用户。
目前Python Table API的类型系统上还未发现类似的局限性。
</div>
</div>

数据类型列表
------------------

本节列出了所有预定义的数据类型。
<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="Java/Scala" markdown="1">
对于基于 JVM 的 Table API，这些类型也可以从 `org.apache.flink.table.api.DataTypes` 中找到。
</div>
<div data-lang="Python" markdown="1">
对于Python Table API, 这些类型可以从 `pyflink.table.types.DataTypes` 中找到。
</div>
</div>

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

**JVM 类型**

| Java 类型                               | 输入  | 输出   | 备注                 |
|:----------------------------------------|:-----:|:------:|:------------------------|
|`java.lang.String`                       | X     | X      | *缺省*               |
|`byte[]`                                 | X     | X      | 假设使用 UTF-8 编码。 |
|`org.apache.flink.table.data.StringData` | X     | X      | 内部数据结构。 |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
尚不支持
{% endhighlight %}
</div>
</div>

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="SQL/Java/Scala" markdown="1">
此类型用 `CHAR(n)` 声明，其中 `n` 表示字符数量。`n` 的值必须在 `1` 和 `2,147,483,647` 之间（含边界值）。如果未指定长度，`n` 等于 `1`。
</div>
<div data-lang="Python" markdown="1">
</div>
</div>

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

**JVM 类型**

| Java 类型                               | 输入  | 输出   | 备注                 |
|:----------------------------------------|:-----:|:------:|:------------------------|
|`java.lang.String`                       | X     | X      | *缺省*               |
|`byte[]`                                 | X     | X      | 假设使用 UTF-8 编码。 |
|`org.apache.flink.table.data.StringData` | X     | X      | 内部数据结构。 |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.VARCHAR(n)

DataTypes.STRING()
{% endhighlight %}

<span class="label label-danger">注意</span> 当前，声明`DataTypes.VARCHAR(n)`中的所指定的最大的字符数量 `n` 必须为 `2,147,483,647`。
</div>
</div>

此类型用 `VARCHAR(n)` 声明，其中 `n` 表示最大的字符数量。`n` 的值必须在 `1` 和 `2,147,483,647` 之间（含边界值）。如果未指定长度，`n` 等于 `1`。

`STRING` 等价于 `VARCHAR(2147483647)`.

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

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                 |
|:-------------------|:-----:|:------:|:------------------------|
|`byte[]`            | X     | X      | *缺省*               |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
尚不支持
{% endhighlight %}
</div>
</div>

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="SQL/Java/Scala" markdown="1">
此类型用 `BINARY(n)` 声明，其中 `n` 是字节数量。`n` 的值必须在 `1` 和 `2,147,483,647` 之间（含边界值）。如果未指定长度，`n` 等于 `1`。
</div>
<div data-lang="Python" markdown="1">
</div>
</div>

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

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                 |
|:-------------------|:-----:|:------:|:------------------------|
|`byte[]`            | X     | X      | *缺省*               |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.VARBINARY(n)

DataTypes.BYTES()
{% endhighlight %}

<span class="label label-danger">注意</span> 当前，声明`DataTypes.VARBINARY(n)`中的所指定的最大的字符数量 `n` 必须为 `2,147,483,647`。
</div>
</div>

此类型用 `VARBINARY(n)` 声明，其中 `n` 是最大的字节数量。`n` 的值必须在 `1` 和 `2,147,483,647` 之间（含边界值）。如果未指定长度，`n` 等于 `1`。

`BYTES` 等价于 `VARBINARY(2147483647)`。

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

**JVM 类型**

| Java 类型                                | 输入  | 输出   | 备注                 |
|:-----------------------------------------|:-----:|:------:|:------------------------|
|`java.math.BigDecimal`                    | X     | X      | *缺省*               |
|`org.apache.flink.table.data.DecimalData` | X     | X      | 内部数据结构。 |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.DECIMAL(p, s)
{% endhighlight %}

<span class="label label-danger">注意</span> 当前，声明`DataTypes.DECIMAL(p, s)`中的所指定的精度 `p` 必须为`38`，尾数 `n` 必须为 `18`。
</div>
</div>

此类型用 `DECIMAL(p, s)` 声明，其中 `p` 是数字的位数（*精度*），`s` 是数字中小数点右边的位数（*尾数*）。`p` 的值必须介于 `1` 和 `38` 之间（含边界值）。`s` 的值必须介于 `0` 和 `p` 之间（含边界值）。其中 `p` 的缺省值是 `10`，`s` 的缺省值是 `0`。

`NUMERIC(p, s)` 和 `DEC(p, s)` 都等价于这个类型。

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

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Byte`    | X     | X      | *缺省*                                    |
|`byte`              | X     | (X)    | 仅当类型不可为空时才输出。 |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.TINYINT()
{% endhighlight %}
</div>
</div>

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

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Short`   | X     | X      | *缺省*                                    |
|`short`             | X     | (X)    | 仅当类型不可为空时才输出。 |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.SMALLINT()
{% endhighlight %}
</div>
</div>

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

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Integer` | X     | X      | *缺省*                                    |
|`int`               | X     | (X)    | 仅当类型不可为空时才输出。 |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.INT()
{% endhighlight %}
</div>
</div>

`INTEGER` 等价于此类型。

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

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Long`    | X     | X      | *缺省*                                    |
|`long`              | X     | (X)    | 仅当类型不可为空时才输出。 |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.BIGINT()
{% endhighlight %}
</div>
</div>

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

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Float`   | X     | X      | *缺省*                                    |
|`float`             | X     | (X)    | 仅当类型不可为空时才输出。 |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.FLOAT()
{% endhighlight %}
</div>
</div>

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

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                                      |
|:-------------------|:-----:|:------:|:---------------------------------------------|
|`java.lang.Double`  | X     | X      | *缺省*                                    |
|`double`            | X     | (X)    | 仅当类型不可为空时才输出。 |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.DOUBLE()
{% endhighlight %}
</div>
</div>

`DOUBLE PRECISION` 等价于此类型。

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

**JVM 类型**

| Java 类型            | 输入 | 输出 | 备注                                      |
|:---------------------|:-----:|:------:|:---------------------------------------------|
|`java.time.LocalDate` | X     | X      | *缺省*                                    |
|`java.sql.Date`       | X     | X      |                                              |
|`java.lang.Integer`   | X     | X      | 描述从 Epoch 算起的天数。    |
|`int`                 | X     | (X)    | 描述从 Epoch 算起的天数。<br>仅当类型不可为空时才输出。 |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.DATE()
{% endhighlight %}
</div>
</div>

#### `TIME`

*不带*时区的时间数据类型，由 `hour:minute:second[.fractional]` 组成，精度达到纳秒，范围从 `00:00:00.000000000` 到 `23:59:59.999999999`。

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="SQL/Java/Scala" markdown="1">
与 SQL 标准相比，不支持闰秒（`23:59:60` 和 `23:59:61`），语义上更接近于 `java.time.LocalTime`。没有提供*带有*时区的时间。
</div>
<div data-lang="Python" markdown="1">
与 SQL 标准相比，不支持闰秒（`23:59:60` 和 `23:59:61`）。没有提供*带有*时区的时间。
</div>
</div>

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

**JVM 类型**

| Java 类型            |输入 |输出 |备注                                             |
|:---------------------|:-----:|:------:|:----------------------------------------------------|
|`java.time.LocalTime` | X     | X      | *缺省*                                           |
|`java.sql.Time`       | X     | X      |                                                     |
|`java.lang.Integer`   | X     | X      | 描述自当天以来的毫秒数。    |
|`int`                 | X     | (X)    | 描述自当天以来的毫秒数。<br>仅当类型不可为空时才输出。 |
|`java.lang.Long`      | X     | X      | 描述自当天以来的纳秒数。     |
|`long`                | X     | (X)    | 描述自当天以来的纳秒数。<br>仅当类型不可为空时才输出。 |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.TIME(p)
{% endhighlight %}

<span class="label label-danger">注意</span> 当前，声明`DataTypes.TIME(p)`中的所指定的精度 `p` 必须为`0`。
</div>
</div>

此类型用 `TIME(p)` 声明，其中 `p` 是秒的小数部分的位数（*精度*）。`p` 的值必须介于 `0` 和 `9` 之间（含边界值）。如果未指定精度，则 `p` 等于 `0`。

#### `TIMESTAMP`

*不带*时区的时间戳数据类型，由 `year-month-day hour:minute:second[.fractional]` 组成，精度达到纳秒，范围从 `0000-01-01 00:00:00.000000000` 到 `9999-12-31 23:59:59.999999999`。

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="SQL/Java/Scala" markdown="1">
与 SQL 标准相比，不支持闰秒（`23:59:60` 和 `23:59:61`），语义上更接近于 `java.time.LocalDateTime`。

不支持和 `BIGINT`（JVM `long` 类型）互相转换，因为这意味着有时区，然而此类型是无时区的。对于语义上更接近于 `java.time.Instant` 的需求请使用 `TIMESTAMP WITH LOCAL TIME ZONE`。
</div>
<div data-lang="Python" markdown="1">
与 SQL 标准相比，不支持闰秒（`23:59:60` 和 `23:59:61`）。

不支持和 `BIGINT`（JVM `long` 类型）互相转换，因为这意味着有时区，然而此类型是无时区的。有此需求请使用 `TIMESTAMP WITH LOCAL TIME ZONE`。
</div>
</div>

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

**JVM 类型**

| Java 类型                | 输入 | 输出 | 备注                                             |
|:-------------------------|:-----:|:------:|:----------------------------------------------------|
|`java.time.LocalDateTime`                   | X     | X      | *缺省*                            |
|`java.sql.Timestamp`                        | X     | X      |                                   |
|`org.apache.flink.table.data.TimestampData` | X     | X      | 内部数据结构。                      |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.TIMESTAMP(p)
{% endhighlight %}

<span class="label label-danger">注意</span> 当前，声明`DataTypes.TIMESTAMP(p)`中的所指定的精度 `p` 必须为`3`。
</div>
</div>

此类型用 `TIMESTAMP(p)` 声明，其中 `p` 是秒的小数部分的位数（*精度*）。`p` 的值必须介于 `0` 和 `9` 之间（含边界值）。如果未指定精度，则 `p` 等于 `6`。

`TIMESTAMP(p) WITHOUT TIME ZONE` 等价于此类型。

#### `TIMESTAMP WITH TIME ZONE`

*带有*时区的时间戳数据类型，由 `year-month-day hour:minute:second[.fractional] zone` 组成，精度达到纳秒，范围从 `0000-01-01 00:00:00.000000000 +14:59` 到
`9999-12-31 23:59:59.999999999 -14:59`。

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="SQL/Java/Scala" markdown="1">
与 SQL 标准相比，不支持闰秒（`23:59:60` 和 `23:59:61`），语义上更接近于 `java.time.OffsetDateTime`。
</div>
<div data-lang="Python" markdown="1">
与 SQL 标准相比，不支持闰秒（`23:59:60` 和 `23:59:61`）。
</div>
</div>

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

**JVM 类型**

| Java 类型                 | 输入 | 输出 | 备注              |
|:--------------------------|:-----:|:------:|:---------------------|
|`java.time.OffsetDateTime` | X     | X      | *缺省*            |
|`java.time.ZonedDateTime`  | X     |        | 忽略时区 ID。 |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
尚不支持
{% endhighlight %}
</div>
</div>

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="SQL/Java/Scala" markdown="1">
此类型用 `TIMESTAMP(p) WITH TIME ZONE` 声明，其中 `p` 是秒的小数部分的位数（*精度*）。`p` 的值必须介于 `0` 和 `9` 之间（含边界值）。如果未指定精度，则 `p` 等于 `6`。
</div>
<div data-lang="Python" markdown="1">
</div>
</div>

#### `TIMESTAMP WITH LOCAL TIME ZONE`

*带有本地*时区的时间戳数据类型，由 `year-month-day hour:minute:second[.fractional] zone` 组成，精度达到纳秒，范围从 `0000-01-01 00:00:00.000000000 +14:59` 到
`9999-12-31 23:59:59.999999999 -14:59`。

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="SQL/Java/Scala" markdown="1">
不支持闰秒（`23:59:60` 和 `23:59:61`），语义上更接近于 `java.time.OffsetDateTime`。

与 `TIMESTAMP WITH TIME ZONE` 相比，时区偏移信息并非物理存储在每个数据中。相反，此类型在 Table 编程环境的 UTC 时区中采用 `java.time.Instant` 语义。每个数据都在当前会话中配置的本地时区中进行解释，以便用于计算和可视化。
</div>
<div data-lang="Python" markdown="1">
不支持闰秒（`23:59:60` 和 `23:59:61`）。

与 `TIMESTAMP WITH TIME ZONE` 相比，时区偏移信息并非物理存储在每个数据中。每个数据都在当前会话中配置的本地时区中进行解释，以便用于计算和可视化。
</div>
</div>

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

**JVM 类型**

| Java 类型                                  |输入 |输出 |备注                                           |
|:-------------------------------------------|:-----:|:------:|:--------------------------------------------------|
|`java.time.Instant`                         | X     | X      | *缺省*                                         |
|`java.lang.Integer`                         | X     | X      | 描述从 Epoch 算起的秒数。      |
|`int`                                       | X     | (X)    | 描述从 Epoch 算起的秒数。<br>仅当类型不可为空时才输出。 |
|`java.lang.Long`                            | X     | X      | 描述从 Epoch 算起的毫秒数。                          |
|`long`                                      | X     | (X)    | 描述从 Epoch 算起的毫秒数。<br>仅当类型不可为空时才输出 |
|`org.apache.flink.table.data.TimestampData` | X     | X      | 内部数据结构。                                       |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(p)
{% endhighlight %}

<span class="label label-danger">注意</span> 当前，声明`DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(p)`中的所指定的精度 `p` 必须为3。
</div>
</div>

此类型用 `TIMESTAMP(p) WITH LOCAL TIME ZONE` 声明，其中 `p` 是秒的小数部分的位数（*精度*）。`p` 的值必须介于 `0` 和 `9` 之间（含边界值）。如果未指定精度，则 `p` 等于 `6`。

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

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                            |
|:-------------------|:-----:|:------:|:-----------------------------------|
|`java.time.Period`  | X     | X      | 忽略 `days` 部分。 *缺省* |
|`java.lang.Integer` | X     | X      | 描述月的数量。    |
|`int`               | X     | (X)    | 描述月的数量。<br>仅当类型不可为空时才输出。 |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.INTERVAL(DataTypes.YEAR())
DataTypes.INTERVAL(DataTypes.YEAR(p))
DataTypes.INTERVAL(DataTypes.YEAR(p), DataTypes.MONTH())
DataTypes.INTERVAL(DataTypes.MONTH())
{% endhighlight %}
</div>
</div>

可以使用以上组合来声明类型，其中 `p` 是年数（*年精度*）的位数。`p` 的值必须介于 `1` 和 `4` 之间（含边界值）。如果未指定年精度，`p` 则等于 `2`。

#### `INTERVAL DAY TO SECOND`

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

**JVM 类型**

| Java 类型           | 输入 | 输出 | 备注                               |
|:--------------------|:-----:|:------:|:--------------------------------------|
|`java.time.Duration` | X     | X      | *缺省*                             |
|`java.lang.Long`     | X     | X      | 描述毫秒数。 |
|`long`               | X     | (X)    | 描述毫秒数。<br>仅当类型不可为空时才输出。 |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
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

**JVM 类型**

| Java 类型                              | 输入  | 输出   | 备注                              |
|:---------------------------------------|:-----:|:------:|:----------------------------------|
|*t*`[]`                                 | (X)   | (X)    | 依赖于子类型。 *缺省*             |
| `java.util.List<t>`                    | X     | X      |                                   |
| `java.util.List<t>` 的*子类型*          | X     |        |                                   |
|`org.apache.flink.table.data.ArrayData` | X     | X      | 内部数据结构。                    |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.ARRAY(t)
{% endhighlight %}
</div>
</div>

此类型用 `ARRAY<t>` 声明，其中 `t` 是所包含元素的数据类型。

`t ARRAY` 接近等价于 SQL 标准。例如，`INT ARRAY` 等价于 `ARRAY<INT>`。

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

**JVM 类型**

| Java 类型                             | 输入  | 输出   | 备注           |
|:--------------------------------------|:-----:|:------:|:---------------|
| `java.util.Map<kt, vt>`               | X     | X      | *缺省*         |
| `java.util.Map<kt, vt>` 的*子类型*    | X     |        |                |
|`org.apache.flink.table.data.MapData`  | X     | X      | 内部数据结构。 |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.MAP(kt, vt)
{% endhighlight %}
</div>
</div>

此类型用 `MAP<kt, vt>` 声明，其中 `kt` 是键的数据类型，`vt` 是值的数据类型。

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

**JVM 类型**

| Java 类型                                        | 输入  | 输出   | 备注                                                  |
|:-------------------------------------------------|:-----:|:------:|:------------------------------------------------------|
|`java.util.Map<t, java.lang.Integer>`             | X     | X      | 将每个值可多重地分配给一个整数 *缺省*                 |
| `java.util.Map<t, java.lang.Integer>` 的*子类型* | X     |        |                                                       |
|`org.apache.flink.table.data.MapData`             | X     | X      | 内部数据结构。                                        |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.MULTISET(t)
{% endhighlight %}
</div>
</div>

此类型用 `MULTISET<t>` 声明，其中 `t` 是所包含元素的数据类型。

`t MULTISET` 接近等价于 SQL 标准。例如，`INT MULTISET` 等价于 `MULTISET<INT>`。

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

**JVM 类型**

| Java 类型                            | 输入  | 输出   | 备注                    |
|:-------------------------------------|:-----:|:------:|:------------------------|
|`org.apache.flink.types.Row`          | X     | X      | *缺省*                  |
|`org.apache.flink.table.data.RowData` | X     | X      | 内部数据结构。          |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.ROW([DataTypes.FIELD(n0, t0), DataTypes.FIELD(n1, t1), ...])
DataTypes.ROW([DataTypes.FIELD(n0, t0, d0), DataTypes.FIELD(n1, t1, d1), ...])
{% endhighlight %}
</div>
</div>

此类型用 `ROW<n0 t0 'd0', n1 t1 'd1', ...>` 声明，其中 `n` 是唯一的字段名称，`t` 是字段的逻辑类型，`d` 是字段的描述。

`ROW(...)` 接近等价于 SQL 标准。例如，`ROW(myField INT, myOtherField BOOLEAN)` 等价于 `ROW<myField INT, myOtherField BOOLEAN>`。

### 用户自定义数据类型

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="Java/Scala" markdown="1">
<span class="label label-danger">注意</span> 还未完全支持用户自定义数据类型，当前（从 Flink 1.11 开始）它们仅可作为函数参数和返回值的未注册的结构化类型。

结构化类型类似于面向对象编程语言中的对象，可包含零个、一个或多个属性，每个属性都包含一个名称和一个类型。

有两种结构化类型：

- 存储在 catalog 并由 _catatlog 标识符_ 标识的类型（例如 `cat.db.MyType`），等价于 SQL 标准定义里的结构化类型。

- 由 _实现类_ 标识，通常以反射方式匿名定义的未注册类型（例如 `com.myorg.model.MyType`）。当写代码定义表时，这些功能很有用。它们使你能够重用现有的JVM类，而无需重复手动定义数据类型。

#### 可注册的结构化类型

当前尚不支持，因此无法在 catalog 里保存或在 `CREATE TABLE` DDL 语句里引用它们。

#### 未注册的结构化类型

可以从常规 POJOs（Plain Old Java Objects）自动反射式提取出未注册的结构化类型。

结构化类型的实现类必须满足以下要求：
- 可被全局访问到，即必须声明为 `public`、`static`，不能用 `abstract`；
- 提供无参默认构造器，或可设置所有成员变量的构造器；
- 可访问类的所有成员变量，比如使用 `public` 声明成员变量，或遵循通用代码规范写 getter 比如 `getField()`、`isField()`、`field()`；
- 可设置类的所有成员变量，比如使用 `public` 声明成员变量，定义可设置所有成员变量的构造器，或遵循通用代码规范写 setter 比如 `setField(...)`、`field(...)`；
- 所有成员变量都要映射到某个数据类型，比如使用反射式提取进行隐式映射，或用 `@DataTypeHint` [注解](#data-type-annotations) 显式映射；
- 忽略 `static` 或 `transient` 修饰的成员变量；

只要字段不（递归地）指向自己，反射式提取支持字段的任意嵌套。

成员变量（比如 `public int age;`）的类型必须包含在本文为每种数据类型定义的受支持的 JVM 类型列表里（例如，`java.lang.Integer` 或 `int` 对应 `INT`）。

对于某些类，需要有注解才能将类映射到数据类型（例如， `@DataTypeHint("DECIMAL(10, 2)")` 为 `java.math.BigDecimal` 分配固定的精度和小数位）。
</div>
<div data-lang="Python" markdown="1">
</div>
</div>

**声明**

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

**JVM 类型**

| Java 类型                            | 输入  | 输出   | 备注                                                  |
|:-------------------------------------|:-----:|:------:|:------------------------------------------------------|
|*类型*                               | X     | X      | 原始类或子类（用于输入）或超类（用于输出）*缺省*      |
|`org.apache.flink.types.Row`          | X     | X      | 代表一行数据的结构化类型。                            |
|`org.apache.flink.table.data.RowData` | X     | X      | 内部数据结构。                                        |

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

**JVM 类型**

| Java 类型                            | 输入  | 输出   | 备注                                                  |
|:-------------------------------------|:-----:|:------:|:------------------------------------------------------|
|*类型*                               | X     | X      | 原始类或子类（用于输入）或超类（用于输出）*缺省*      |
|`org.apache.flink.types.Row`          | X     | X      | 代表一行数据的结构化类型。                            |
|`org.apache.flink.table.data.RowData` | X     | X      | 内部数据结构。                                        |

</div>
<div data-lang="Python" markdown="1">
{% highlight python %}
尚不支持
{% endhighlight %}
</div>
</div>

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

**JVM 类型**

| Java 类型          | 输入 | 输出 | 备注                              |
|:-------------------|:-----:|:------:|:-------------------------------------|
|`java.lang.Boolean` | X     | X      | *缺省*                            |
|`boolean`           | X     | (X)    | 仅当类型不可为空时才输出。 |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
DataTypes.BOOLEAN()
{% endhighlight %}
</div>
</div>

#### `RAW`

任意序列化类型的数据类型。此类型对于 Flink Table 来讲是一个黑盒子，仅在跟外部交互时被反序列化。

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

DataTypes.RAW(class)
{% endhighlight %}

**JVM 类型**

| Java 类型                                 | 输入  | 输出   | 备注                                                |
|:------------------------------------------|:-----:|:------:|:----------------------------------------------------|
|*类型*                                     | X     | X      | 原始类或子类（用于输入）或超类（用于输出）。 *缺省* |
|`byte[]`                                   |       | X      |                                                     |
|`org.apache.flink.table.data.RawValueData` | X     | X      | 内部数据结构。                                      |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
尚不支持
{% endhighlight %}
</div>
</div>

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="SQL/Java/Scala" markdown="1">
此类型用 `RAW('class', 'snapshot')` 声明，其中 `class` 是原始类，`snapshot` 是 Base64 编码的序列化的 `TypeSerializerSnapshot`。通常，类型字符串不是直接声明的，而是在持久化类型时生成的。

在 API 中，可以通过直接提供 `Class` + `TypeSerializer` 或通过传递 `TypeInformation` 并让框架从那里提取 `Class` + `TypeSerializer` 来声明 `RAW` 类型。
</div>
<div data-lang="Python" markdown="1">
</div>
</div>

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

**JVM 类型**

| Java 类型         | 输入 | 输出 | 备注                              |
|:------------------|:-----:|:------:|:-------------------------------------|
|`java.lang.Object` | X     | X      | *缺省*                            |
|*任何类型*        |       | (X)    | 任何非基本数据类型              |

</div>

<div data-lang="Python" markdown="1">
{% highlight python %}
尚不支持
{% endhighlight %}
</div>
</div>

数据类型注解
---------------------

<div class="codetabs" data-hide-tabs="1" markdown="1">
<div data-lang="Java/Scala" markdown="1">
Flink API 经常尝试使用反射自动从类信息中提取数据类型，以避免重复的手动定义模式工作。然而以反射方式提取数据类型并不总是成功的，因为可能会丢失逻辑信息。因此，可能有必要在类或字段声明附近添加额外信息以支持提取逻辑。

下表列出了可以隐式映射到数据类型而无需额外信息的类。

If you intend to implement classes in Scala, *it is recommended to use boxed types* (e.g. `java.lang.Integer`)
instead of Scala's primitives. Scala's primitives (e.g. `Int` or `Double`) are compiled to JVM primitives (e.g.
`int`/`double`) and result in `NOT NULL` semantics as shown in the table below. Furthermore, Scala primitives that
are used in generics (e.g. `java.lang.Map[Int, Double]`) are erased during compilation and lead to class
information similar to `java.lang.Map[java.lang.Object, java.lang.Object]`.

| 类                          | 数据类型                            |
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


本文提到的其他 JVM 桥接类都需要 `@DataTypeHint` 注解。

_数据类型提示_ 可以参数化或替换函数参数和返回值、结构化类或结构化类字段的默认提取逻辑，实现者可以通过声明 `@DataTypeHint` 注解来选择默认提取逻辑应修改的程度。

`@DataTypeHint` 注解提供了一组可选的提示参数，以下示例显示了其中一些参数，可以在注解类的文档中找到更多信息。
</div>
<div data-lang="Python" markdown="1">
</div>
</div>

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
<div data-lang="Python" markdown="1">
{% highlight python %}
尚不支持
{% endhighlight %}
</div>
</div>

{% top %}
