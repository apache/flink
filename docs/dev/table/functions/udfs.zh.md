---
title: "自定义函数"
nav-parent_id: table_functions
nav-pos: 50
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

自定义函数是一个非常重要的功能，因为它极大的扩展了查询的表达能力。

* This will be replaced by the TOC
{:toc}

注册自定义函数
-------------------------------
在大多数情况下，自定义函数在使用之前都需要注册。在 Scala Table API 中可以不用注册。

通过调用 `registerFunction()` 把函数注册到 `TableEnvironment`。当一个函数注册之后，它就在 `TableEnvironment` 的函数 catalog 里面了，这样 Table API 或者 SQL 解析器就可以识别并使用它。

关于如何注册和使用每种类型的自定义函数（标量函数、表值函数和聚合函数），更多示例可以看下面的部分。

{% top %}

标量函数
----------------

如果需要的标量函数没有被内置函数覆盖，就可以在自定义一个标量函数在 Table API 和 SQL 中使用。自定义标量函数可以把 0 到多个标量值映射成 1 个标量值。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
想要实现自定义标量函数，你需要扩展 `org.apache.flink.table.functions` 里面的 `ScalarFunction` 并且实现一个或者多个求值方法。标量函数的行为取决于你写的求值方法。求值方法并须是 `public` 的，而且名字必须是 `eval`。求值方法的参数类型以及返回值类型就决定了标量函数的参数类型和返回值类型。可以通过实现多个名为 `eval` 的方法对求值方法进行重载。求值方法也支持可变参数，例如 `eval(String... strs)`。

下面的示例展示了如何实现一个求哈希值的函数。先把它注册到 `TableEnvironment` 里，然后在查询的时候就可以直接使用了。需要注意的是，你可以在注册之前通过构造方法来配置你的标量函数：

{% highlight java %}
public class HashCode extends ScalarFunction {
  private int factor = 12;
  
  public HashCode(int factor) {
      this.factor = factor;
  }
  
  public int eval(String s) {
      return s.hashCode() * factor;
  }
}

BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

// 注册函数
tableEnv.registerFunction("hashCode", new HashCode(10));

// 在 Java Table API 中使用函数
myTable.select("string, string.hashCode(), hashCode(string)");

// 在 SQL API 中使用函数
tableEnv.sqlQuery("SELECT string, hashCode(string) FROM MyTable");
{% endhighlight %}

求值方法的返回值类型默认是由 Flink 的类型推导来决定的。类型推导可以推导出基本数据类型以及简单的 POJO，但是对于更复杂的、自定义的、或者组合类型，可能会推导出错误的结果。在这种情况下，可以通过覆盖 `ScalarFunction#getResultType()`，并且返回 `TypeInformation` 来定义复杂类型。

下面的示例展示了一个高级一点的自定义标量函数用法，它接收一个内部的时间戳参数，并且以 `long` 的形式返回该内部的时间戳。通过覆盖 `ScalarFunction#getResultType()`，我们定义了我们返回的 `long` 类型在代码生成时可以被解析为 `Types.TIMESTAMP` 类型。

{% highlight java %}
public static class TimestampModifier extends ScalarFunction {
  public long eval(long t) {
    return t % 1000;
  }

  public TypeInformation<?> getResultType(Class<?>[] signature) {
    return Types.SQL_TIMESTAMP;
  }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
想要实现自定义标量函数，你需要扩展 `org.apache.flink.table.functions` 里面的 `ScalarFunction` 并且实现一个或者多个求值方法。标量函数的行为取决于你写的求值方法。求值方法并须是 `public` 的，而且名字必须是 `eval`。求值方法的参数类型以及返回值类型就决定了标量函数的参数类型和返回值类型。可以通过实现多个名为 `eval` 的方法对求值方法进行重载。求值方法也支持可变参数，例如 `@varargs def eval(str: String*)`。

下面的示例展示了如何实现一个求哈希值的函数。先把它注册到 `TableEnvironment` 里，然后在查询的时候就可以直接使用了。需要注意的是，你可以在注册之前通过构造方法来配置你的标量函数：

{% highlight scala %}
// 必须定义在 static/object 上下文中
class HashCode(factor: Int) extends ScalarFunction {
  def eval(s: String): Int = {
    s.hashCode() * factor
  }
}

val tableEnv = BatchTableEnvironment.create(env)

// 在 Scala Table API 中使用函数
val hashCode = new HashCode(10)
myTable.select('string, hashCode('string))

// 在 SQL 中注册和使用函数
tableEnv.registerFunction("hashCode", new HashCode(10))
tableEnv.sqlQuery("SELECT string, hashCode(string) FROM MyTable")
{% endhighlight %}

求值方法的返回值类型默认是由 Flink 的类型推导来决定的。类型推导可以推导出基本数据类型以及简单的 POJO，但是对于更复杂的、自定义的、或者组合类型，可能会推导出错误的结果。在这种情况下，可以通过覆盖 `ScalarFunction#getResultType()`，并且返回 `TypeInformation` 来定义复杂类型。

下面的示例展示了一个高级一点的自定义标量函数用法，它接收一个内部的时间戳参数，并且以 `long` 的形式返回该内部的时间戳。通过覆盖 `ScalarFunction#getResultType()`，我们定义了我们返回的 `long` 类型在代码生成时可以被解析为 `Types.TIMESTAMP` 类型。

{% highlight scala %}
object TimestampModifier extends ScalarFunction {
  def eval(t: Long): Long = {
    t % 1000
  }

  override def getResultType(signature: Array[Class[_]]): TypeInformation[_] = {
    Types.TIMESTAMP
  }
}
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
要定义一个 Python 标量函数，你可以继承 `pyflink.table.udf` 下的 `ScalarFunction`，并且实现一个求值函数。Python 标量函数的行为取决于你实现的求值函数，它的名字必须是 `eval`。

下面的示例展示了如何自定义一个 Python 的求哈希值的函数，并且把它注册到 `TableEnvironment` 里，然后在查询中使用它。你可以在注册函数之前通过构造函数来配置你的标量函数。

{% highlight python %}
class HashCode(ScalarFunction):
  def __init__(self):
    self.factor = 12

  def eval(self, s):
    return hash(s) * self.factor

table_env = BatchTableEnvironment.create(env)

# 注册 Python 函数
table_env.register_function("hash_code", udf(HashCode(), DataTypes.BIGINT(), DataTypes.BIGINT()))

# 在 Python Table API 中使用函数
my_table.select("string, bigint, string.hash_code(), hash_code(string)")

# 在 SQL API 中使用函数
table_env.sql_query("SELECT string, bigint, hash_code(bigint) FROM MyTable")
{% endhighlight %}

除了继承 `ScalarFunction`，还有很多方法可以定义 Python 标量函数。
更多细节，可以参考 [Python 标量函数]({{ site.baseurl }}/zh/dev/table/python/python_udfs.html#scalar-functions) 文档。
</div>
</div>

{% top %}

表值函数
---------------

跟自定义标量函数一样，自定义表值函数的输入参数也可以是 0 到多个标量。但是跟标量函数只能返回一个值不同的是，它可以返回任意多行。返回的每一行可以包含 1 到多列。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

要定义一个表值函数，你需要扩展 `org.apache.flink.table.functions` 下的 `TableFunction`，并且实现（一个或者多个）求值方法。表值函数的行为取决于你实现的求值方法。求值方法必须被声明为 `public`，并且名字必须是 `eval`。可以通过实现多个名为 `eval` 的方法对求值方法进行重载。求值方法的参数类型决定了表值函数的参数类型。表值函数也可以支持变长参数，比如 `eval(String... strs)`。表值函数返回的表的类型取决于 `TableFunction` 的泛型参数。求值方法通过 `collect(T)` 方法来发送要输出的行。

在 Table API 中，表值函数是通过 `.joinLateral` 或者 `.leftOuterJoinLateral` 来使用的。`joinLateral` 算子会把外表（算子左侧的表）的每一行跟跟表值函数返回的所有行（位于算子右侧）进行 （cross）join。`leftOuterJoinLateral` 算子也是把外表（算子左侧的表）的每一行跟表值函数返回的所有行（位于算子右侧）进行（cross）join，并且如果表值函数返回 0 行也会保留外表的这一行。在 SQL 里面用 CORSS JOIN 或者 以 ON TRUE 为条件的 LEFT JOIN 来配合 `LATERAL TABLE（<TableFunction>)` 的使用。

下面的例子展示了如何定义一个表值函数，如何在 TableEnvironment 中注册表值函数，以及如何在查询中使用表值函数。你可以在注册之前通过构造函数来配置你的表值函数：

{% highlight java %}
// 泛型参数的类型 "Tuple2<String, Integer>" 决定了返回的表的 schema 是（String，Integer）。
public class Split extends TableFunction<Tuple2<String, Integer>> {
    private String separator = " ";
    
    public Split(String separator) {
        this.separator = separator;
    }
    
    public void eval(String str) {
        for (String s : str.split(separator)) {
            // 使用 collect(...) 来输出一行数据
            collect(new Tuple2<String, Integer>(s, s.length()));
        }
    }
}

BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);
Table myTable = ...         // table schema: [a: String]

// 注册表值函数。
tableEnv.registerFunction("split", new Split("#"));

// 在 Java Table API 中使用表值函数。"as" 指明了表的字段名字
myTable.joinLateral("split(a) as (word, length)")
    .select("a, word, length");
myTable.leftOuterJoinLateral("split(a) as (word, length)")
    .select("a, word, length");

// 在 SQL 中用 LATERAL 和 TABLE 关键字来使用表值函数
// CROSS JOIN a table function (等价于 Table API 中的 "join").
tableEnv.sqlQuery("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)");
// LEFT JOIN a table function (等价于 in Table API 中的 "leftOuterJoin").
tableEnv.sqlQuery("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE");
{% endhighlight %}

需要注意的是 POJO 类型没有确定的字段顺序。所以，你不可以用 `AS` 来重命名返回的 POJO 的字段。

`TableFunction` 的返回类型默认是用 Flink 自动类型推导来决定的。对于基础类型和简单的 POJO 类型推导是没有问题的，但是对于更复杂的、自定义的、以及组合的类型可能会推导错误。如果有这种情况，可以通过重写（override） `TableFunction#getResultType()` 并且返回 `TypeInformation` 来指定返回类型。

下面的例子展示了 `TableFunction` 返回了一个 `Row` 类型，需要显示指定返回类型。我们通过重写 `TableFunction#getResultType` 来指定 `RowTypeInfo(String, Integer)` 作为返回的表的类型。

{% highlight java %}
public class CustomTypeSplit extends TableFunction<Row> {
    public void eval(String str) {
        for (String s : str.split(" ")) {
            Row row = new Row(2);
            row.setField(0, s);
            row.setField(1, s.length());
            collect(row);
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING(), Types.INT());
    }
}
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">

要定义一个表值函数，你需要扩展 `org.apache.flink.table.functions` 下的 `TableFunction`，并且实现（一个或者多个）求值方法。表值函数的行为取决于你实现的求值方法。求值方法必须被声明为 `public`，并且名字必须是 `eval`。可以通过实现多个名为 `eval` 的方法对求值方法进行重载。求值方法的参数类型决定了表值函数的参数类型。表值函数也可以支持变长参数，比如 `eval(String... strs)`。表值函数返回的表的类型取决于 `TableFunction` 的泛型参数。求值方法通过 `collect(T)` 方法来发送要输出的行。

在 Table API 中，表值函数是通过 `.joinLateral` 或者 `.leftOuterJoinLateral` 来使用的。`joinLateral` 算子会把外表（算子左侧的表）的每一行跟跟表值函数返回的所有行（位于算子右侧）进行 （cross）join。`leftOuterJoinLateral` 算子也是把外表（算子左侧的表）的每一行跟表值函数返回的所有行（位于算子右侧）进行（cross）join，并且如果表值函数返回 0 行也会保留外表的这一行。在 SQL 里面用 CORSS JOIN 或者 以 ON TRUE 为条件的 LEFT JOIN 来配合 `LATERAL TABLE（<TableFunction>)` 的使用。

下面的例子展示了如何定义一个表值函数，如何在 TableEnvironment 中注册表值函数，以及如何在查询中使用表值函数。你可以在注册之前通过构造函数来配置你的表值函数：

{% highlight scala %}
// 泛型参数的类型 "(String, Int)" 决定了返回的表的 schema 是 (String, Integer)。
class Split(separator: String) extends TableFunction[(String, Int)] {
  def eval(str: String): Unit = {
    // 使用 collect(...) 来输出一行
    str.split(separator).foreach(x => collect((x, x.length)))
  }
}

val tableEnv = BatchTableEnvironment.create(env)
val myTable = ...         // table schema: [a: String]

// 在 Scala Table API 中使用表值函数（注意：在 Scala Table API 中不需要注册函数）
val split = new Split("#")
// "as" 指明了返回表的字段名字
myTable.joinLateral(split('a) as ('word, 'length)).select('a, 'word, 'length)
myTable.leftOuterJoinLateral(split('a) as ('word, 'length)).select('a, 'word, 'length)

// 注册表值函数，然后才能在 SQL 查询中使用
tableEnv.registerFunction("split", new Split("#"))

// 在 SQL 中使用 LATERAL 和 TABLE 关键字类使用表值函数
// CROSS JOIN a table function (equivalent to "join" in Table API)
tableEnv.sqlQuery("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)")
// LEFT JOIN a table function (equivalent to "leftOuterJoin" in Table API)
tableEnv.sqlQuery("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE")
{% endhighlight %}
**重要：**不要把表值函数实现成一个 Scala object。Scala object 是一个单例，会有并发的问题。

需要注意的是 POJO 类型没有确定的字段顺序。所以，你不可以用 `AS` 来重命名返回的 POJO 的字段。

`TableFunction` 的返回类型默认是用 Flink 自动类型推导来决定的。对于基础类型和简单的 POJO 类型推导是没有问题的，但是对于更复杂的、自定义的、以及组合的类型可能会推导错误。如果有这种情况，可以通过重写（override） `TableFunction#getResultType()` 并且返回 `TypeInformation` 来指定返回类型。

下面的例子展示了 `TableFunction` 返回了一个 `Row` 类型，需要显示指定返回类型。我们通过重写 `TableFunction#getResultType` 来返回 `RowTypeInfo` 作为返回类型。

{% highlight scala %}
class CustomTypeSplit extends TableFunction[Row] {
  def eval(str: String): Unit = {
    str.split(" ").foreach({ s =>
      val row = new Row(2)
      row.setField(0, s)
      row.setField(1, s.length)
      collect(row)
    })
  }

  override def getResultType: TypeInformation[Row] = {
    Types.ROW(Types.STRING, Types.INT)
  }
}
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
要实现一个 Python 表值函数，你可以扩展 `pyflink.table.udtf` 下的 `TableFunction`，并且实现一个求值方法。Python 表值函数的行为取决于你实现的求值方法，它的名字必须是 `eval`。

在 Python Table API 中，表值函数是通过 `.join_lateral` 或者 `.left_outer_join_lateral` 来使用的。`join_lateral` 算子会把外表（算子左侧的表）的每一行跟跟表值函数返回的所有行（位于算子右侧）进行 （cross）join。`left_outer_join_lateral` 算子也是把外表（算子左侧的表）的每一行跟表值函数返回的所有行（位于算子右侧）进行（cross）join，并且如果表值函数返回 0 行也会保留外表的这一行。在 SQL 里面用 CORSS JOIN 或者 以 ON TRUE 为条件的 LEFT JOIN 来配合 `LATERAL TABLE（<TableFunction>)` 的使用。

下面的例子展示了如何定义一个 Python 表值函数，如何在 TableEnvironment 中注册表值函数，以及如何在查询中使用表值函数。你可以在注册之前通过构造函数来配置你的表值函数：

{% highlight python %}
class Split(TableFunction):
    def eval(self, string):
        for s in string.split(" "):
            yield s, len(s)

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)
my_table = ...  # type: Table, table schema: [a: String]

# 注册 Python 表值函数
table_env.register_function("split", udtf(Split(), DataTypes.STRING(), [DataTypes.STRING(), DataTypes.INT()]))

# 在 Python Table API 中使用 Python 表值函数
my_table.join_lateral("split(a) as (word, length)")
my_table.left_outer_join_lateral("split(a) as (word, length)")

# 在 SQL API 中使用 Python 表值函数
table_env.sql_query("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)")
table_env.sql_query("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE")

{% endhighlight %}

除了继承 `TableFunction`，还有很多其它方法可以定义 Python 表值函数。
更多信息，参考 [Python 表值函数]({{ site.baseurl }}/zh/dev/table/python/python_udfs.html#table-functions)文档。

</div>
</div>

{% top %}


聚合函数
---------------------

自定义聚合函数（UDAGG）是把一个表（一行或者多行，每行可以有一列或者多列）聚合成一个标量值。

<center>
<img alt="UDAGG mechanism" src="{{ site.baseurl }}/fig/udagg-mechanism.png" width="80%">
</center>

上面的图片展示了一个聚合的例子。假设你有一个关于饮料的表。表里面有三个字段，分别是 `id`、`name`、`price`，表里有 5 行数据。假设你需要找到所有饮料里最贵的饮料的价格，即执行一个 `max()` 聚合。你需要遍历所有 5 行数据，而结果就只有一个数值。

自定义聚合函数是通过扩展 `AggregateFunction` 来实现的。`AggregateFunction` 的工作过程如下。首先，它需要一个 `accumulator`，它是一个数据结构，存储了聚合的中间结果。通过调用 `AggregateFunction` 的 `createAccumulator()` 方法创建一个空的 accumulator。接下来，对于每一行数据，会调用 `accumulate()` 方法来更新 accumulator。当所有的数据都处理完了之后，通过调用 `getValue` 方法来计算和返回最终的结果。

**下面几个方法是每个 `AggregateFunction` 必须要实现的：**

- `createAccumulator()`
- `accumulate()` 
- `getValue()`

Flink 的类型推导在遇到复杂类型的时候可能会推导出错误的结果，比如那些非基本类型和普通的 POJO 类型的复杂类型。所以跟 `ScalarFunction` 和 `TableFunction` 一样，`AggregateFunction` 也提供了 `AggregateFunction#getResultType()` 和 `AggregateFunction#getAccumulatorType()` 来分别指定返回值类型和 accumulator 的类型，两个函数的返回值类型也都是 `TypeInformation`。
 
除了上面的方法，还有几个方法可以选择实现。这些方法有些可以让查询更加高效，而有些是在某些特定场景下必须要实现的。例如，如果聚合函数用在会话窗口（当两个会话窗口合并的时候需要 merge 他们的 accumulator）的话，`merge()` 方法就是必须要实现的。

**`AggregateFunction` 的以下方法在某些场景下是必须实现的：**

- `retract()` 在 bounded `OVER` 窗口中是必须实现的。
- `merge()` 在许多批式聚合和会话窗口聚合中是必须实现的。
- `resetAccumulator()` 在许多批式聚合中是必须实现的。

`AggregateFunction` 的所有方法都必须是 `public` 的，不能是 `static` 的，而且名字必须跟上面写的一样。`createAccumulator`、`getValue`、`getResultType` 以及 `getAccumulatorType` 这几个函数是在抽象类 `AggregateFunction` 中定义的，而其他函数都是约定的方法。如果要定义一个聚合函数，你需要扩展 `org.apache.flink.table.functions.AggregateFunction`，并且实现一个（或者多个）`accumulate` 方法。`accumulate` 方法可以重载，每个方法的参数类型不同，并且支持变长参数。

`AggregateFunction` 的所有方法的详细文档如下。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
/**
  * Base class for user-defined aggregates and table aggregates.
  *
  * @param <T>   the type of the aggregation result.
  * @param <ACC> the type of the aggregation accumulator. The accumulator is used to keep the
  *             aggregated values which are needed to compute an aggregation result.
  */
public abstract class UserDefinedAggregateFunction<T, ACC> extends UserDefinedFunction {

  /**
    * Creates and init the Accumulator for this (table)aggregate function.
    *
    * @return the accumulator with the initial value
    */
  public ACC createAccumulator(); // MANDATORY

  /**
    * Returns the TypeInformation of the (table)aggregate function's result.
    *
    * @return The TypeInformation of the (table)aggregate function's result or null if the result
    *         type should be automatically inferred.
    */
  public TypeInformation<T> getResultType = null; // PRE-DEFINED

  /**
    * Returns the TypeInformation of the (table)aggregate function's accumulator.
    *
    * @return The TypeInformation of the (table)aggregate function's accumulator or null if the
    *         accumulator type should be automatically inferred.
    */
  public TypeInformation<ACC> getAccumulatorType = null; // PRE-DEFINED
}

/**
  * Base class for aggregation functions. 
  *
  * @param <T>   the type of the aggregation result
  * @param <ACC> the type of the aggregation accumulator. The accumulator is used to keep the
  *             aggregated values which are needed to compute an aggregation result.
  *             AggregateFunction represents its state using accumulator, thereby the state of the
  *             AggregateFunction must be put into the accumulator.
  */
public abstract class AggregateFunction<T, ACC> extends UserDefinedAggregateFunction<T, ACC> {

  /** Processes the input values and update the provided accumulator instance. The method
    * accumulate can be overloaded with different custom types and arguments. An AggregateFunction
    * requires at least one accumulate() method.
    *
    * @param accumulator           the accumulator which contains the current aggregated results
    * @param [user defined inputs] the input value (usually obtained from a new arrived data).
    */
  public void accumulate(ACC accumulator, [user defined inputs]); // MANDATORY

  /**
    * Retracts the input values from the accumulator instance. The current design assumes the
    * inputs are the values that have been previously accumulated. The method retract can be
    * overloaded with different custom types and arguments. This function must be implemented for
    * datastream bounded over aggregate.
    *
    * @param accumulator           the accumulator which contains the current aggregated results
    * @param [user defined inputs] the input value (usually obtained from a new arrived data).
    */
  public void retract(ACC accumulator, [user defined inputs]); // OPTIONAL

  /**
    * Merges a group of accumulator instances into one accumulator instance. This function must be
    * implemented for datastream session window grouping aggregate and dataset grouping aggregate.
    *
    * @param accumulator  the accumulator which will keep the merged aggregate results. It should
    *                     be noted that the accumulator may contain the previous aggregated
    *                     results. Therefore user should not replace or clean this instance in the
    *                     custom merge method.
    * @param its          an {@link java.lang.Iterable} pointed to a group of accumulators that will be
    *                     merged.
    */
  public void merge(ACC accumulator, java.lang.Iterable<ACC> its); // OPTIONAL

  /**
    * Called every time when an aggregation result should be materialized.
    * The returned value could be either an early and incomplete result
    * (periodically emitted as data arrive) or the final result of the
    * aggregation.
    *
    * @param accumulator the accumulator which contains the current
    *                    aggregated results
    * @return the aggregation result
    */
  public T getValue(ACC accumulator); // MANDATORY

  /**
    * Resets the accumulator for this [[AggregateFunction]]. This function must be implemented for
    * dataset grouping aggregate.
    *
    * @param accumulator  the accumulator which needs to be reset
    */
  public void resetAccumulator(ACC accumulator); // OPTIONAL

  /**
    * Returns true if this AggregateFunction can only be applied in an OVER window.
    *
    * @return true if the AggregateFunction requires an OVER window, false otherwise.
    */
  public Boolean requiresOver = false; // PRE-DEFINED
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
/**
  * Base class for user-defined aggregates and table aggregates.
  *
  * @tparam T   the type of the aggregation result.
  * @tparam ACC the type of the aggregation accumulator. The accumulator is used to keep the
  *             aggregated values which are needed to compute an aggregation result.
  */
abstract class UserDefinedAggregateFunction[T, ACC] extends UserDefinedFunction {

  /**
    * Creates and init the Accumulator for this (table)aggregate function.
    *
    * @return the accumulator with the initial value
    */
  def createAccumulator(): ACC // MANDATORY

  /**
    * Returns the TypeInformation of the (table)aggregate function's result.
    *
    * @return The TypeInformation of the (table)aggregate function's result or null if the result
    *         type should be automatically inferred.
    */
  def getResultType: TypeInformation[T] = null // PRE-DEFINED

  /**
    * Returns the TypeInformation of the (table)aggregate function's accumulator.
    *
    * @return The TypeInformation of the (table)aggregate function's accumulator or null if the
    *         accumulator type should be automatically inferred.
    */
  def getAccumulatorType: TypeInformation[ACC] = null // PRE-DEFINED
}

/**
  * Base class for aggregation functions. 
  *
  * @tparam T   the type of the aggregation result
  * @tparam ACC the type of the aggregation accumulator. The accumulator is used to keep the
  *             aggregated values which are needed to compute an aggregation result.
  *             AggregateFunction represents its state using accumulator, thereby the state of the
  *             AggregateFunction must be put into the accumulator.
  */
abstract class AggregateFunction[T, ACC] extends UserDefinedAggregateFunction[T, ACC] {

  /**
    * Processes the input values and update the provided accumulator instance. The method
    * accumulate can be overloaded with different custom types and arguments. An AggregateFunction
    * requires at least one accumulate() method.
    *
    * @param accumulator           the accumulator which contains the current aggregated results
    * @param [user defined inputs] the input value (usually obtained from a new arrived data).
    */
  def accumulate(accumulator: ACC, [user defined inputs]): Unit // MANDATORY

  /**
    * Retracts the input values from the accumulator instance. The current design assumes the
    * inputs are the values that have been previously accumulated. The method retract can be
    * overloaded with different custom types and arguments. This function must be implemented for
    * datastream bounded over aggregate.
    *
    * @param accumulator           the accumulator which contains the current aggregated results
    * @param [user defined inputs] the input value (usually obtained from a new arrived data).
    */
  def retract(accumulator: ACC, [user defined inputs]): Unit // OPTIONAL

  /**
    * Merges a group of accumulator instances into one accumulator instance. This function must be
    * implemented for datastream session window grouping aggregate and dataset grouping aggregate.
    *
    * @param accumulator  the accumulator which will keep the merged aggregate results. It should
    *                     be noted that the accumulator may contain the previous aggregated
    *                     results. Therefore user should not replace or clean this instance in the
    *                     custom merge method.
    * @param its          an [[java.lang.Iterable]] pointed to a group of accumulators that will be
    *                     merged.
    */
  def merge(accumulator: ACC, its: java.lang.Iterable[ACC]): Unit // OPTIONAL
  
  /**
    * Called every time when an aggregation result should be materialized.
    * The returned value could be either an early and incomplete result
    * (periodically emitted as data arrive) or the final result of the
    * aggregation.
    *
    * @param accumulator the accumulator which contains the current
    *                    aggregated results
    * @return the aggregation result
    */
  def getValue(accumulator: ACC): T // MANDATORY

  /**
    * Resets the accumulator for this [[AggregateFunction]]. This function must be implemented for
    * dataset grouping aggregate.
    *
    * @param accumulator  the accumulator which needs to be reset
    */
  def resetAccumulator(accumulator: ACC): Unit // OPTIONAL

  /**
    * Returns true if this AggregateFunction can only be applied in an OVER window.
    *
    * @return true if the AggregateFunction requires an OVER window, false otherwise.
    */
  def requiresOver: Boolean = false // PRE-DEFINED
}
{% endhighlight %}
</div>
</div>


下面的例子展示了如何：

- 定义一个聚合函数来计算某一列的加权平均，
- 在 `TableEnvironment` 中注册函数，
- 在查询中使用函数。

为了计算加权平均值，accumulator 需要存储加权总和以及数据的条数。在我们的例子里，我们定义了一个类 `WeightedAvgAccum` 来作为 accumulator。Flink 的 checkpoint 机制会自动保存 accumulator，在失败时进行恢复，以此来保证精确一次的语义。

我们的 `WeightedAvg`（聚合函数）的 `accumulate` 方法有三个输入参数。第一个是 `WeightedAvgAccum` accumulator，另外两个是用户自定义的输入：输入的值 `ivalue` 和 输入的权重 `iweight`。尽管 `retract()`、`merge()`、`resetAccumulator()` 这几个方法在大多数聚合类型中都不是必须实现的，我们也在样例中提供了他们的实现。请注意我们在 Scala 样例中也是用的是 Java 的基础类型，并且定义了 `getResultType()` 和 `getAccumulatorType()`，因为 Flink 的类型推导对于 Scala 的类型推导做的不是很好。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
/**
 * Accumulator for WeightedAvg.
 */
public static class WeightedAvgAccum {
    public long sum = 0;
    public int count = 0;
}

/**
 * Weighted Average user-defined aggregate function.
 */
public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccum> {

    @Override
    public WeightedAvgAccum createAccumulator() {
        return new WeightedAvgAccum();
    }

    @Override
    public Long getValue(WeightedAvgAccum acc) {
        if (acc.count == 0) {
            return null;
        } else {
            return acc.sum / acc.count;
        }
    }

    public void accumulate(WeightedAvgAccum acc, long iValue, int iWeight) {
        acc.sum += iValue * iWeight;
        acc.count += iWeight;
    }

    public void retract(WeightedAvgAccum acc, long iValue, int iWeight) {
        acc.sum -= iValue * iWeight;
        acc.count -= iWeight;
    }
    
    public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
        Iterator<WeightedAvgAccum> iter = it.iterator();
        while (iter.hasNext()) {
            WeightedAvgAccum a = iter.next();
            acc.count += a.count;
            acc.sum += a.sum;
        }
    }
    
    public void resetAccumulator(WeightedAvgAccum acc) {
        acc.count = 0;
        acc.sum = 0L;
    }
}

// 注册函数
StreamTableEnvironment tEnv = ...
tEnv.registerFunction("wAvg", new WeightedAvg());

// 使用函数
tEnv.sqlQuery("SELECT user, wAvg(points, level) AS avgPoints FROM userScores GROUP BY user");

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import java.lang.{Long => JLong, Integer => JInteger}
import org.apache.flink.api.java.tuple.{Tuple1 => JTuple1}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.AggregateFunction

/**
 * Accumulator for WeightedAvg.
 */
class WeightedAvgAccum extends JTuple1[JLong, JInteger] {
  sum = 0L
  count = 0
}

/**
 * Weighted Average user-defined aggregate function.
 */
class WeightedAvg extends AggregateFunction[JLong, CountAccumulator] {

  override def createAccumulator(): WeightedAvgAccum = {
    new WeightedAvgAccum
  }
  
  override def getValue(acc: WeightedAvgAccum): JLong = {
    if (acc.count == 0) {
        null
    } else {
        acc.sum / acc.count
    }
  }
  
  def accumulate(acc: WeightedAvgAccum, iValue: JLong, iWeight: JInteger): Unit = {
    acc.sum += iValue * iWeight
    acc.count += iWeight
  }

  def retract(acc: WeightedAvgAccum, iValue: JLong, iWeight: JInteger): Unit = {
    acc.sum -= iValue * iWeight
    acc.count -= iWeight
  }
    
  def merge(acc: WeightedAvgAccum, it: java.lang.Iterable[WeightedAvgAccum]): Unit = {
    val iter = it.iterator()
    while (iter.hasNext) {
      val a = iter.next()
      acc.count += a.count
      acc.sum += a.sum
    }
  }

  def resetAccumulator(acc: WeightedAvgAccum): Unit = {
    acc.count = 0
    acc.sum = 0L
  }

  override def getAccumulatorType: TypeInformation[WeightedAvgAccum] = {
    new TupleTypeInfo(classOf[WeightedAvgAccum], Types.LONG, Types.INT)
  }

  override def getResultType: TypeInformation[JLong] = Types.LONG
}

// 注册函数
val tEnv: StreamTableEnvironment = ???
tEnv.registerFunction("wAvg", new WeightedAvg())

// 使用函数
tEnv.sqlQuery("SELECT user, wAvg(points, level) AS avgPoints FROM userScores GROUP BY user")

{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
'''
Java code:

/**
 * Accumulator for WeightedAvg.
 */
public static class WeightedAvgAccum {
    public long sum = 0;
    public int count = 0;
}

// The java class must have a public no-argument constructor and can be founded in current java classloader.
// Java 类必须有一个 public 的无参构造函数，并且可以在当前类加载器中加载到。

/**
 * Weighted Average user-defined aggregate function.
 */
public static class WeightedAvg extends AggregateFunction<Long, WeightedAvgAccum> {

    @Override
    public WeightedAvgAccum createAccumulator() {
        return new WeightedAvgAccum();
    }

    @Override
    public Long getValue(WeightedAvgAccum acc) {
        if (acc.count == 0) {
            return null;
        } else {
            return acc.sum / acc.count;
        }
    }

    public void accumulate(WeightedAvgAccum acc, long iValue, int iWeight) {
        acc.sum += iValue * iWeight;
        acc.count += iWeight;
    }

    public void retract(WeightedAvgAccum acc, long iValue, int iWeight) {
        acc.sum -= iValue * iWeight;
        acc.count -= iWeight;
    }
    
    public void merge(WeightedAvgAccum acc, Iterable<WeightedAvgAccum> it) {
        Iterator<WeightedAvgAccum> iter = it.iterator();
        while (iter.hasNext()) {
            WeightedAvgAccum a = iter.next();
            acc.count += a.count;
            acc.sum += a.sum;
        }
    }
    
    public void resetAccumulator(WeightedAvgAccum acc) {
        acc.count = 0;
        acc.sum = 0L;
    }
}
'''

# 注册函数
t_env = ...  # type: StreamTableEnvironment
t_env.register_java_function("wAvg", "my.java.function.WeightedAvg")

# 使用函数
t_env.sql_query("SELECT user, wAvg(points, level) AS avgPoints FROM userScores GROUP BY user")

{% endhighlight %}
</div>
</div>


{% top %}

表值聚合函数
---------------------

自定义表值聚合函数（UDTAGG）可以把一个表（一行或者多行，每行有一列或者多列）聚合成另一张表，结果中可以有多行多列。

<center>
<img alt="UDAGG mechanism" src="{{ site.baseurl }}/fig/udtagg-mechanism.png" width="80%">
</center>

上图展示了一个表值聚合函数的例子。假设你有一个饮料的表，这个表有 3 列，分别是 `id`、`name` 和 `price`，一共有 5 行。假设你需要找到价格最高的两个饮料，类似于 `top2()` 表值聚合函数。你需要遍历所有 5 行数据，结果是有 2 行数据的一个表。

用户自定义表值聚合函数是通过扩展 `TableAggregateFunction` 类来实现的。一个 `TableAggregateFunction` 的工作过程如下。首先，它需要一个 `accumulator`，这个 `accumulator` 负责存储聚合的中间结果。 通过调用 `TableAggregateFunction` 的 `createAccumulator` 方法来构造一个空的 accumulator。接下来，对于每一行数据，会调用 `accumulate` 方法来更新 accumulator。当所有数据都处理完之后，调用 `emitValue` 方法来计算和返回最终的结果。

**下面几个 `TableAggregateFunction` 的方法是必须要实现的：**

- `createAccumulator()`
- `accumulate()` 

Flink 的类型推导在遇到复杂类型的时候可能会推导出错误的结果，比如那些非基本类型和普通的 POJO 类型的复杂类型。所以类似于 `ScalarFunction` 和 `TableFunction`，`TableAggregateFunction` 也提供了 `TableAggregateFunction#getResultType()` 和 `TableAggregateFunction#getAccumulatorType()` 方法来指定返回值类型和 accumulator 的类型，这两个方法都需要返回 `TypeInformation`。
 
除了上面的方法，还有几个其他的方法可以选择性的实现。有些方法可以让查询更加高效，而有些方法对于某些特定场景是必须要实现的。比如，在会话窗口（当两个会话窗口合并时会合并两个 accumulator）中使用聚合函数时，必须要实现`merge()` 方法。

**下面几个 `TableAggregateFunction` 的方法在某些特定场景下是必须要实现的：**

- `retract()` 在 bounded `OVER` 窗口中的聚合函数必须要实现。
- `merge()` 在许多批式聚合和会话窗口聚合中是必须要实现的。
- `resetAccumulator()` 在许多批式聚合中是必须要实现的。
- `emitValue()` 在批式聚合以及窗口聚合中是必须要实现的。

**下面的 `TableAggregateFunction` 的方法可以提升流式任务的效率：**

- `emitUpdateWithRetract()` 在 retract 模式下，该方法负责发送被更新的值。

`emitValue` 方法会发送所有 accumulator 给出的结果。拿 TopN 来说，`emitValue` 每次都会发送所有的最大的 n 个值。这在流式任务中可能会有一些性能问题。为了提升性能，用户可以实现 `emitUpdateWithRetract` 方法。这个方法在 retract 模式下会增量的输出结果，比如有数据更新了，我们必须要撤回老的数据，然后再发送新的数据。如果定义了 `emitUpdateWithRetract` 方法，那它会优先于 `emitValue` 方法被使用，因为一般认为 `emitUpdateWithRetract` 会更加高效，因为它的输出是增量的。

`TableAggregateFunction` 的所有方法都必须是 `public` 的、非 `static` 的，而且名字必须跟上面提到的一样。`createAccumulator`、`getResultType` 和 `getAccumulatorType` 这三个方法是在抽象父类 `TableAggregateFunction` 中定义的，而其他的方法都是约定的方法。要实现一个表值聚合函数，你必须扩展 `org.apache.flink.table.functions.TableAggregateFunction`，并且实现一个（或者多个）`accumulate` 方法。`accumulate` 方法可以有多个重载的方法，也可以支持变长参数。

`TableAggregateFunction` 的所有方法的详细文档如下。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

/**
  * Base class for user-defined aggregates and table aggregates.
  *
  * @param <T>   the type of the aggregation result.
  * @param <ACC> the type of the aggregation accumulator. The accumulator is used to keep the
  *             aggregated values which are needed to compute an aggregation result.
  */
public abstract class UserDefinedAggregateFunction<T, ACC> extends UserDefinedFunction {

  /**
    * Creates and init the Accumulator for this (table)aggregate function.
    *
    * @return the accumulator with the initial value
    */
  public ACC createAccumulator(); // MANDATORY

  /**
    * Returns the TypeInformation of the (table)aggregate function's result.
    *
    * @return The TypeInformation of the (table)aggregate function's result or null if the result
    *         type should be automatically inferred.
    */
  public TypeInformation<T> getResultType = null; // PRE-DEFINED

  /**
    * Returns the TypeInformation of the (table)aggregate function's accumulator.
    *
    * @return The TypeInformation of the (table)aggregate function's accumulator or null if the
    *         accumulator type should be automatically inferred.
    */
  public TypeInformation<ACC> getAccumulatorType = null; // PRE-DEFINED
}

/**
  * Base class for table aggregation functions. 
  *
  * @param <T>   the type of the aggregation result
  * @param <ACC> the type of the aggregation accumulator. The accumulator is used to keep the
  *             aggregated values which are needed to compute a table aggregation result.
  *             TableAggregateFunction represents its state using accumulator, thereby the state of
  *             the TableAggregateFunction must be put into the accumulator.
  */
public abstract class TableAggregateFunction<T, ACC> extends UserDefinedAggregateFunction<T, ACC> {

  /** Processes the input values and update the provided accumulator instance. The method
    * accumulate can be overloaded with different custom types and arguments. A TableAggregateFunction
    * requires at least one accumulate() method.
    *
    * @param accumulator           the accumulator which contains the current aggregated results
    * @param [user defined inputs] the input value (usually obtained from a new arrived data).
    */
  public void accumulate(ACC accumulator, [user defined inputs]); // MANDATORY

  /**
    * Retracts the input values from the accumulator instance. The current design assumes the
    * inputs are the values that have been previously accumulated. The method retract can be
    * overloaded with different custom types and arguments. This function must be implemented for
    * datastream bounded over aggregate.
    *
    * @param accumulator           the accumulator which contains the current aggregated results
    * @param [user defined inputs] the input value (usually obtained from a new arrived data).
    */
  public void retract(ACC accumulator, [user defined inputs]); // OPTIONAL

  /**
    * Merges a group of accumulator instances into one accumulator instance. This function must be
    * implemented for datastream session window grouping aggregate and dataset grouping aggregate.
    *
    * @param accumulator  the accumulator which will keep the merged aggregate results. It should
    *                     be noted that the accumulator may contain the previous aggregated
    *                     results. Therefore user should not replace or clean this instance in the
    *                     custom merge method.
    * @param its          an {@link java.lang.Iterable} pointed to a group of accumulators that will be
    *                     merged.
    */
  public void merge(ACC accumulator, java.lang.Iterable<ACC> its); // OPTIONAL

  /**
    * Called every time when an aggregation result should be materialized. The returned value
    * could be either an early and incomplete result  (periodically emitted as data arrive) or
    * the final result of the  aggregation.
    *
    * @param accumulator the accumulator which contains the current
    *                    aggregated results
    * @param out         the collector used to output data
    */
  public void emitValue(ACC accumulator, Collector<T> out); // OPTIONAL
  
  /**
    * Called every time when an aggregation result should be materialized. The returned value
    * could be either an early and incomplete result (periodically emitted as data arrive) or
    * the final result of the aggregation.
    *
    * Different from emitValue, emitUpdateWithRetract is used to emit values that have been updated.
    * This method outputs data incrementally in retract mode, i.e., once there is an update, we
    * have to retract old records before sending new updated ones. The emitUpdateWithRetract
    * method will be used in preference to the emitValue method if both methods are defined in the
    * table aggregate function, because the method is treated to be more efficient than emitValue
    * as it can outputvalues incrementally.
    *
    * @param accumulator the accumulator which contains the current
    *                    aggregated results
    * @param out         the retractable collector used to output data. Use collect method
    *                    to output(add) records and use retract method to retract(delete)
    *                    records.
    */
  public void emitUpdateWithRetract(ACC accumulator, RetractableCollector<T> out); // OPTIONAL
  
  /**
    * Collects a record and forwards it. The collector can output retract messages with the retract
    * method. Note: only use it in {@code emitRetractValueIncrementally}.
    */
  public interface RetractableCollector<T> extends Collector<T> {

      /**
        * Retract a record.
        *
        * @param record The record to retract.
        */
      void retract(T record);
  }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
/**
  * Base class for user-defined aggregates and table aggregates.
  *
  * @tparam T   the type of the aggregation result.
  * @tparam ACC the type of the aggregation accumulator. The accumulator is used to keep the
  *             aggregated values which are needed to compute an aggregation result.
  */
abstract class UserDefinedAggregateFunction[T, ACC] extends UserDefinedFunction {

  /**
    * Creates and init the Accumulator for this (table)aggregate function.
    *
    * @return the accumulator with the initial value
    */
  def createAccumulator(): ACC // MANDATORY

  /**
    * Returns the TypeInformation of the (table)aggregate function's result.
    *
    * @return The TypeInformation of the (table)aggregate function's result or null if the result
    *         type should be automatically inferred.
    */
  def getResultType: TypeInformation[T] = null // PRE-DEFINED

  /**
    * Returns the TypeInformation of the (table)aggregate function's accumulator.
    *
    * @return The TypeInformation of the (table)aggregate function's accumulator or null if the
    *         accumulator type should be automatically inferred.
    */
  def getAccumulatorType: TypeInformation[ACC] = null // PRE-DEFINED
}

/**
  * Base class for table aggregation functions. 
  *
  * @tparam T   the type of the aggregation result
  * @tparam ACC the type of the aggregation accumulator. The accumulator is used to keep the
  *             aggregated values which are needed to compute an aggregation result.
  *             TableAggregateFunction represents its state using accumulator, thereby the state of
  *             the TableAggregateFunction must be put into the accumulator.
  */
abstract class TableAggregateFunction[T, ACC] extends UserDefinedAggregateFunction[T, ACC] {

  /**
    * Processes the input values and update the provided accumulator instance. The method
    * accumulate can be overloaded with different custom types and arguments. A TableAggregateFunction
    * requires at least one accumulate() method.
    *
    * @param accumulator           the accumulator which contains the current aggregated results
    * @param [user defined inputs] the input value (usually obtained from a new arrived data).
    */
  def accumulate(accumulator: ACC, [user defined inputs]): Unit // MANDATORY

  /**
    * Retracts the input values from the accumulator instance. The current design assumes the
    * inputs are the values that have been previously accumulated. The method retract can be
    * overloaded with different custom types and arguments. This function must be implemented for
    * datastream bounded over aggregate.
    *
    * @param accumulator           the accumulator which contains the current aggregated results
    * @param [user defined inputs] the input value (usually obtained from a new arrived data).
    */
  def retract(accumulator: ACC, [user defined inputs]): Unit // OPTIONAL

  /**
    * Merges a group of accumulator instances into one accumulator instance. This function must be
    * implemented for datastream session window grouping aggregate and dataset grouping aggregate.
    *
    * @param accumulator  the accumulator which will keep the merged aggregate results. It should
    *                     be noted that the accumulator may contain the previous aggregated
    *                     results. Therefore user should not replace or clean this instance in the
    *                     custom merge method.
    * @param its          an [[java.lang.Iterable]] pointed to a group of accumulators that will be
    *                     merged.
    */
  def merge(accumulator: ACC, its: java.lang.Iterable[ACC]): Unit // OPTIONAL
  
  /**
    * Called every time when an aggregation result should be materialized. The returned value
    * could be either an early and incomplete result  (periodically emitted as data arrive) or
    * the final result of the  aggregation.
    *
    * @param accumulator the accumulator which contains the current
    *                    aggregated results
    * @param out         the collector used to output data
    */
  def emitValue(accumulator: ACC, out: Collector[T]): Unit // OPTIONAL

  /**
    * Called every time when an aggregation result should be materialized. The returned value
    * could be either an early and incomplete result (periodically emitted as data arrive) or
    * the final result of the aggregation.
    *
    * Different from emitValue, emitUpdateWithRetract is used to emit values that have been updated.
    * This method outputs data incrementally in retract mode, i.e., once there is an update, we
    * have to retract old records before sending new updated ones. The emitUpdateWithRetract
    * method will be used in preference to the emitValue method if both methods are defined in the
    * table aggregate function, because the method is treated to be more efficient than emitValue
    * as it can outputvalues incrementally.
    *
    * @param accumulator the accumulator which contains the current
    *                    aggregated results
    * @param out         the retractable collector used to output data. Use collect method
    *                    to output(add) records and use retract method to retract(delete)
    *                    records.
    */
  def emitUpdateWithRetract(accumulator: ACC, out: RetractableCollector[T]): Unit // OPTIONAL
 
  /**
    * Collects a record and forwards it. The collector can output retract messages with the retract
    * method. Note: only use it in `emitRetractValueIncrementally`.
    */
  trait RetractableCollector[T] extends Collector[T] {
    
    /**
      * Retract a record.
      *
      * @param record The record to retract.
      */
    def retract(record: T): Unit
  }
}
{% endhighlight %}
</div>
</div>


下面的例子展示了如何

- 定义一个 `TableAggregateFunction` 来计算给定列的最大的 2 个值，
- 在 `TableEnvironment` 中注册函数，
- 在 Table API 查询中使用函数（当前只在 Table API 中支持 TableAggregateFunction）。

为了计算最大的 2 个值，accumulator 需要保存当前看到的最大的 2 个值。在我们的例子中，我们定义了类 `Top2Accum` 来作为 accumulator。Flink 的 checkpoint 机制会自动保存 accumulator，并且在失败时进行恢复，来保证精确一次的语义。

我们的 `Top2` 表值聚合函数（`TableAggregateFunction`）的 `accumulate()` 方法有两个输入，第一个是 `Top2Accum` accumulator，另一个是用户定义的输入：输入的值 `v`。尽管 `merge()` 方法在大多数聚合类型中不是必须的，我们也在样例中提供了它的实现。请注意，我们在 Scala 样例中也使用的是 Java 的基础类型，并且定义了 `getResultType()` 和 `getAccumulatorType()` 方法，因为 Flink 的类型推导对于 Scala 的类型推导支持的不是很好。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
/**
 * Accumulator for Top2.
 */
public class Top2Accum {
    public Integer first;
    public Integer second;
}

/**
 * The top2 user-defined table aggregate function.
 */
public static class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accum> {

    @Override
    public Top2Accum createAccumulator() {
        Top2Accum acc = new Top2Accum();
        acc.first = Integer.MIN_VALUE;
        acc.second = Integer.MIN_VALUE;
        return acc;
    }


    public void accumulate(Top2Accum acc, Integer v) {
        if (v > acc.first) {
            acc.second = acc.first;
            acc.first = v;
        } else if (v > acc.second) {
            acc.second = v;
        }
    }

    public void merge(Top2Accum acc, java.lang.Iterable<Top2Accum> iterable) {
        for (Top2Accum otherAcc : iterable) {
            accumulate(acc, otherAcc.first);
            accumulate(acc, otherAcc.second);
        }
    }

    public void emitValue(Top2Accum acc, Collector<Tuple2<Integer, Integer>> out) {
        // emit the value and rank
        if (acc.first != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.first, 1));
        }
        if (acc.second != Integer.MIN_VALUE) {
            out.collect(Tuple2.of(acc.second, 2));
        }
    }
}

// 注册函数
StreamTableEnvironment tEnv = ...
tEnv.registerFunction("top2", new Top2());

// 初始化表
Table tab = ...;

// 使用函数
tab.groupBy("key")
    .flatAggregate("top2(a) as (v, rank)")
    .select("key, v, rank");

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import java.lang.{Integer => JInteger}
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.TableAggregateFunction

/**
 * Accumulator for top2.
 */
class Top2Accum {
  var first: JInteger = _
  var second: JInteger = _
}

/**
 * The top2 user-defined table aggregate function.
 */
class Top2 extends TableAggregateFunction[JTuple2[JInteger, JInteger], Top2Accum] {

  override def createAccumulator(): Top2Accum = {
    val acc = new Top2Accum
    acc.first = Int.MinValue
    acc.second = Int.MinValue
    acc
  }

  def accumulate(acc: Top2Accum, v: Int) {
    if (v > acc.first) {
      acc.second = acc.first
      acc.first = v
    } else if (v > acc.second) {
      acc.second = v
    }
  }

  def merge(acc: Top2Accum, its: JIterable[Top2Accum]): Unit = {
    val iter = its.iterator()
    while (iter.hasNext) {
      val top2 = iter.next()
      accumulate(acc, top2.first)
      accumulate(acc, top2.second)
    }
  }

  def emitValue(acc: Top2Accum, out: Collector[JTuple2[JInteger, JInteger]]): Unit = {
    // emit the value and rank
    if (acc.first != Int.MinValue) {
      out.collect(JTuple2.of(acc.first, 1))
    }
    if (acc.second != Int.MinValue) {
      out.collect(JTuple2.of(acc.second, 2))
    }
  }
}

// 初始化表
val tab = ...

// 使用函数
tab
  .groupBy('key)
  .flatAggregate(top2('a) as ('v, 'rank))
  .select('key, 'v, 'rank)

{% endhighlight %}
</div>
</div>


下面的例子展示了如何使用 `emitUpdateWithRetract` 方法来只发送更新的数据。为了只发送更新的结果，accumulator 保存了上一次的最大的2个值，也保存了当前最大的2个值。注意：如果 TopN 中的 n 非常大，这种既保存上次的结果，也保存当前的结果的方式不太高效。一种解决这种问题的方式是把输入数据直接存储到 `accumulator` 中，然后在调用 `emitUpdateWithRetract` 方法时再进行计算。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
/**
 * Accumulator for Top2.
 */
public class Top2Accum {
    public Integer first;
    public Integer second;
    public Integer oldFirst;
    public Integer oldSecond;
}

/**
 * The top2 user-defined table aggregate function.
 */
public static class Top2 extends TableAggregateFunction<Tuple2<Integer, Integer>, Top2Accum> {

    @Override
    public Top2Accum createAccumulator() {
        Top2Accum acc = new Top2Accum();
        acc.first = Integer.MIN_VALUE;
        acc.second = Integer.MIN_VALUE;
        acc.oldFirst = Integer.MIN_VALUE;
        acc.oldSecond = Integer.MIN_VALUE;
        return acc;
    }

    public void accumulate(Top2Accum acc, Integer v) {
        if (v > acc.first) {
            acc.second = acc.first;
            acc.first = v;
        } else if (v > acc.second) {
            acc.second = v;
        }
    }

    public void emitUpdateWithRetract(Top2Accum acc, RetractableCollector<Tuple2<Integer, Integer>> out) {
        if (!acc.first.equals(acc.oldFirst)) {
            // if there is an update, retract old value then emit new value.
            if (acc.oldFirst != Integer.MIN_VALUE) {
                out.retract(Tuple2.of(acc.oldFirst, 1));
            }
            out.collect(Tuple2.of(acc.first, 1));
            acc.oldFirst = acc.first;
        }

        if (!acc.second.equals(acc.oldSecond)) {
            // if there is an update, retract old value then emit new value.
            if (acc.oldSecond != Integer.MIN_VALUE) {
                out.retract(Tuple2.of(acc.oldSecond, 2));
            }
            out.collect(Tuple2.of(acc.second, 2));
            acc.oldSecond = acc.second;
        }
    }
}

// 注册函数
StreamTableEnvironment tEnv = ...
tEnv.registerFunction("top2", new Top2());

// 初始化表
Table tab = ...;

// 使用函数
tab.groupBy("key")
    .flatAggregate("top2(a) as (v, rank)")
    .select("key, v, rank");

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import java.lang.{Integer => JInteger}
import org.apache.flink.table.api.Types
import org.apache.flink.table.functions.TableAggregateFunction

/**
 * Accumulator for top2.
 */
class Top2Accum {
  var first: JInteger = _
  var second: JInteger = _
  var oldFirst: JInteger = _
  var oldSecond: JInteger = _
}

/**
 * The top2 user-defined table aggregate function.
 */
class Top2 extends TableAggregateFunction[JTuple2[JInteger, JInteger], Top2Accum] {

  override def createAccumulator(): Top2Accum = {
    val acc = new Top2Accum
    acc.first = Int.MinValue
    acc.second = Int.MinValue
    acc.oldFirst = Int.MinValue
    acc.oldSecond = Int.MinValue
    acc
  }

  def accumulate(acc: Top2Accum, v: Int) {
    if (v > acc.first) {
      acc.second = acc.first
      acc.first = v
    } else if (v > acc.second) {
      acc.second = v
    }
  }

  def emitUpdateWithRetract(
    acc: Top2Accum,
    out: RetractableCollector[JTuple2[JInteger, JInteger]])
  : Unit = {
    if (acc.first != acc.oldFirst) {
      // if there is an update, retract old value then emit new value.
      if (acc.oldFirst != Int.MinValue) {
        out.retract(JTuple2.of(acc.oldFirst, 1))
      }
      out.collect(JTuple2.of(acc.first, 1))
      acc.oldFirst = acc.first
    }
    if (acc.second != acc.oldSecond) {
      // if there is an update, retract old value then emit new value.
      if (acc.oldSecond != Int.MinValue) {
        out.retract(JTuple2.of(acc.oldSecond, 2))
      }
      out.collect(JTuple2.of(acc.second, 2))
      acc.oldSecond = acc.second
    }
  }
}

// 初始化表
val tab = ...

// 使用函数
tab
  .groupBy('key)
  .flatAggregate(top2('a) as ('v, 'rank))
  .select('key, 'v, 'rank)

{% endhighlight %}
</div>
</div>


{% top %}

实现自定义函数的最佳实践
------------------------------------

在 Table API 和 SQL 的内部，代码生成会尽量的使用基础类型。自定义函数的参数及返回值类型是对象，会有很多的对象创建、转换（cast）、以及自动拆装箱的开销。因此，强烈建议使用基础类型来作为参数以及返回值的类型。`Types.DATE` 和 `Types.TIME` 可以用 `int` 来表示。`Types.TIMESTAMP` 可以用 `long` 来表示。

我们建议自定义函数用 Java 来实现，而不是用 Scala 来实现，因为 Flink 的类型推导对 Scala 不是很友好。

{% top %}

自定义函数跟运行时集成
---------------------------------

有时候自定义函数需要获取一些全局信息，或者在真正被调用之前做一些配置（setup）/清理（clean-up）的工作。自定义函数也提供了 `open()` 和 `close()` 方法，你可以重写这两个方法做到类似于 DataSet 或者 DataStream API 中 `RichFunction` 的功能。

`open()` 方法在求值方法被调用之前先调用。`close()` 方法在求值方法调用完之后被调用。

`open()` 方法提供了一个 `FunctionContext`，它包含了一些自定义函数被执行时的上下文信息，比如 metric group、分布式文件缓存，或者是全局的作业参数等。

下面的信息可以通过调用 `FunctionContext` 的对应的方法来获得：

| 方法                                  | 描述                                                    |
| :------------------------------------ | :----------------------------------------------------- |
| `getMetricGroup()`                    | 执行该函数的 subtask 的 Metric Group。                   |
| `getCachedFile(name)`                 | 分布式文件缓存的本地临时文件副本。                         |
| `getJobParameter(name, defaultValue)` | 跟对应的 key 关联的全局参数值。                           |

下面的例子展示了如何在一个标量函数中通过 `FunctionContext` 来获取一个全局的任务参数：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class HashCode extends ScalarFunction {

    private int factor = 0;

    @Override
    public void open(FunctionContext context) throws Exception {
        // 获取参数 "hashcode_factor"
        // 如果不存在，则使用默认值 "12"
        factor = Integer.valueOf(context.getJobParameter("hashcode_factor", "12")); 
    }

    public int eval(String s) {
        return s.hashCode() * factor;
    }
}

ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

// 设置任务参数
Configuration conf = new Configuration();
conf.setString("hashcode_factor", "31");
env.getConfig().setGlobalJobParameters(conf);

// 注册函数
tableEnv.registerFunction("hashCode", new HashCode());

// 在 Java Table API 中使用函数
myTable.select("string, string.hashCode(), hashCode(string)");

// 在 SQL 中使用函数
tableEnv.sqlQuery("SELECT string, HASHCODE(string) FROM MyTable");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
object hashCode extends ScalarFunction {

  var hashcode_factor = 12

  override def open(context: FunctionContext): Unit = {
    // 获取参数 "hashcode_factor"
    // 如果不存在，则使用默认值 "12"
    hashcode_factor = context.getJobParameter("hashcode_factor", "12").toInt
  }

  def eval(s: String): Int = {
    s.hashCode() * hashcode_factor
  }
}

val tableEnv = BatchTableEnvironment.create(env)

// 在 Scala Table API 中使用函数
myTable.select('string, hashCode('string))

// 在 SQL 中注册和使用函数
tableEnv.registerFunction("hashCode", hashCode)
tableEnv.sqlQuery("SELECT string, HASHCODE(string) FROM MyTable")
{% endhighlight %}

</div>
</div>

{% top %}

