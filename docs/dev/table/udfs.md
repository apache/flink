---
title: "User-defined Functions"
nav-parent_id: tableapi
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

User-defined functions are an important feature, because they significantly extend the expressiveness of queries.

**TODO**

* This will be replaced by the TOC
{:toc}

Register User-Defined Functions
-------------------------------

**TODO**

{% top %}

Scalar Functions
----------------

If a required scalar function is not contained in the built-in functions, it is possible to define custom, user-defined scalar functions for both the Table API and SQL. A user-defined scalar functions maps zero, one, or multiple scalar values to a new scalar value.

In order to define a scalar function one has to extend the base class `ScalarFunction` in `org.apache.flink.table.functions` and implement (one or more) evaluation methods. The behavior of a scalar function is determined by the evaluation method. An evaluation method must be declared publicly and named `eval`. The parameter types and return type of the evaluation method also determine the parameter and return types of the scalar function. Evaluation methods can also be overloaded by implementing multiple methods named `eval`.

The following example shows how to define your own hash code function, register it in the TableEnvironment, and call it in a query. Note that you can configure your scalar function via a constructor before it is registered:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
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

BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// register the function
tableEnv.registerFunction("hashCode", new HashCode(10));

// use the function in Java Table API
myTable.select("string, string.hashCode(), hashCode(string)");

// use the function in SQL API
tableEnv.sql("SELECT string, HASHCODE(string) FROM MyTable");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// must be defined in static/object context
class HashCode(factor: Int) extends ScalarFunction {
  def eval(s: String): Int = {
    s.hashCode() * factor
  }
}

val tableEnv = TableEnvironment.getTableEnvironment(env)

// use the function in Scala Table API
val hashCode = new HashCode(10)
myTable.select('string, hashCode('string))

// register and use the function in SQL
tableEnv.registerFunction("hashCode", new HashCode(10))
tableEnv.sql("SELECT string, HASHCODE(string) FROM MyTable");
{% endhighlight %}
</div>
</div>

By default the result type of an evaluation method is determined by Flink's type extraction facilities. This is sufficient for basic types or simple POJOs but might be wrong for more complex, custom, or composite types. In these cases `TypeInformation` of the result type can be manually defined by overriding `ScalarFunction#getResultType()`.

Internally, the Table API and SQL code generation works with primitive values as much as possible. If a user-defined scalar function should not introduce much overhead through object creation/casting during runtime, it is recommended to declare parameters and result types as primitive types instead of their boxed classes. `Types.DATE` and `Types.TIME` can also be represented as `int`. `Types.TIMESTAMP` can be represented as `long`.

The following example shows an advanced example which takes the internal timestamp representation and also returns the internal timestamp representation as a long value. By overriding `ScalarFunction#getResultType()` we define that the returned long value should be interpreted as a `Types.TIMESTAMP` by the code generation.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public static class TimestampModifier extends ScalarFunction {
  public long eval(long t) {
    return t % 1000;
  }

  public TypeInformation<?> getResultType(signature: Class<?>[]) {
    return Types.TIMESTAMP;
  }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
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
</div>

{% top %}

Table Functions
---------------

Similar to a user-defined scalar function, a user-defined table function takes zero, one, or multiple scalar values as input parameters. However in contrast to a scalar function, it can return an arbitrary number of rows as output instead of a single value. The returned rows may consist of one or more columns. 

In order to define a table function one has to extend the base class `TableFunction` in `org.apache.flink.table.functions` and implement (one or more) evaluation methods. The behavior of a table function is determined by its evaluation methods. An evaluation method must be declared `public` and named `eval`. The `TableFunction` can be overloaded by implementing multiple methods named `eval`. The parameter types of the evaluation methods determine all valid parameters of the table function. The type of the returned table is determined by the generic type of `TableFunction`. Evaluation methods emit output rows using the protected `collect(T)` method.

In the Table API, a table function is used with `.join(Expression)` or `.leftOuterJoin(Expression)` for Scala users and `.join(String)` or `.leftOuterJoin(String)` for Java users. The `join` operator (cross) joins each row from the outer table (table on the left of the operator) with all rows produced by the table-valued function (which is on the right side of the operator). The `leftOuterJoin` operator joins each row from the outer table (table on the left of the operator) with all rows produced by the table-valued function (which is on the right side of the operator) and preserves outer rows for which the table function returns an empty table. In SQL use `LATERAL TABLE(<TableFunction>)` with CROSS JOIN and LEFT JOIN with an ON TRUE join condition (see examples below).

The following example shows how to define table-valued function, register it in the TableEnvironment, and call it in a query. Note that you can configure your table function via a constructor before it is registered: 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// The generic type "Tuple2<String, Integer>" determines the schema of the returned table as (String, Integer).
public class Split extends TableFunction<Tuple2<String, Integer>> {
    private String separator = " ";
    
    public Split(String separator) {
        this.separator = separator;
    }
    
    public void eval(String str) {
        for (String s : str.split(separator)) {
            // use collect(...) to emit a row
            collect(new Tuple2<String, Integer>(s, s.length()));
        }
    }
}

BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
Table myTable = ...         // table schema: [a: String]

// Register the function.
tableEnv.registerFunction("split", new Split("#"));

// Use the table function in the Java Table API. "as" specifies the field names of the table.
myTable.join("split(a) as (word, length)").select("a, word, length");
myTable.leftOuterJoin("split(a) as (word, length)").select("a, word, length");

// Use the table function in SQL with LATERAL and TABLE keywords.
// CROSS JOIN a table function (equivalent to "join" in Table API).
tableEnv.sql("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)");
// LEFT JOIN a table function (equivalent to "leftOuterJoin" in Table API).
tableEnv.sql("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// The generic type "(String, Int)" determines the schema of the returned table as (String, Integer).
class Split(separator: String) extends TableFunction[(String, Int)] {
  def eval(str: String): Unit = {
    // use collect(...) to emit a row.
    str.split(separator).foreach(x -> collect((x, x.length))
  }
}

val tableEnv = TableEnvironment.getTableEnvironment(env)
val myTable = ...         // table schema: [a: String]

// Use the table function in the Scala Table API (Note: No registration required in Scala Table API).
val split = new Split("#")
// "as" specifies the field names of the generated table.
myTable.join(split('a) as ('word, 'length)).select('a, 'word, 'length);
myTable.leftOuterJoin(split('a) as ('word, 'length)).select('a, 'word, 'length);

// Register the table function to use it in SQL queries.
tableEnv.registerFunction("split", new Split("#"))

// Use the table function in SQL with LATERAL and TABLE keywords.
// CROSS JOIN a table function (equivalent to "join" in Table API)
tableEnv.sql("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)");
// LEFT JOIN a table function (equivalent to "leftOuterJoin" in Table API)
tableEnv.sql("SELECT a, word, length FROM MyTable LEFT JOIN TABLE(split(a)) as T(word, length) ON TRUE");
{% endhighlight %}
**IMPORTANT:** Do not implement TableFunction as a Scala object. Scala object is a singleton and will cause concurrency issues.
</div>
</div>

Please note that POJO types do not have a deterministic field order. Therefore, you cannot rename the fields of POJO returned by a table function using `AS`.

By default the result type of a `TableFunction` is determined by Flink’s automatic type extraction facilities. This works well for basic types and simple POJOs but might be wrong for more complex, custom, or composite types. In such a case, the type of the result can be manually specified by overriding `TableFunction#getResultType()` which returns its `TypeInformation`.

The following example shows an example of a `TableFunction` that returns a `Row` type which requires explicit type information. We define that the returned table type should be `RowTypeInfo(String, Integer)` by overriding `TableFunction#getResultType()`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class CustomTypeSplit extends TableFunction<Row> {
    public void eval(String str) {
        for (String s : str.split(" ")) {
            Row row = new Row(2);
            row.setField(0, s);
            row.setField(1, s.length);
            collect(row);
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(new TypeInformation[]{
               			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO});
    }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
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
    new RowTypeInfo(Seq(BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.INT_TYPE_INFO))
  }
}
{% endhighlight %}
</div>
</div>

{% top %}

Aggregation Functions
---------------------

**TODO**

{% top %}

Integrating UDFs with the Runtime
---------------------------------

Sometimes it might be necessary for a user-defined function to get global runtime information or do some setup/clean-up work before the actual work. User-defined functions provide `open()` and `close()` methods that can be overriden and provide similar functionality as the methods in `RichFunction` of DataSet or DataStream API.

The `open()` method is called once before the evaluation method. The `close()` method after the last call to the evaluation method.

The `open()` method provides a `FunctionContext` that contains information about the context in which user-defined functions are executed, such as the metric group, the distributed cache files, or the global job parameters.

The following information can be obtained by calling the corresponding methods of `FunctionContext`:

| Method                                | Description                                            |
| :------------------------------------ | :----------------------------------------------------- |
| `getMetricGroup()`                    | Metric group for this parallel subtask.                |
| `getCachedFile(name)`                 | Local temporary file copy of a distributed cache file. |
| `getJobParameter(name, defaultValue)` | Global job parameter value associated with given key.  |

The following example snippet shows how to use `FunctionContext` in a scalar function for accessing a global job parameter:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class HashCode extends ScalarFunction {

    private int factor = 0;

    @Override
    public void open(FunctionContext context) throws Exception {
        // access "hashcode_factor" parameter
        // "12" would be the default value if parameter does not exist
        factor = Integer.valueOf(context.getJobParameter("hashcode_factor", "12")); 
    }

    public int eval(String s) {
        return s.hashCode() * factor;
    }
}

ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);

// set job parameter
Configuration conf = new Configuration();
conf.setString("hashcode_factor", "31");
env.getConfig().setGlobalJobParameters(conf);

// register the function
tableEnv.registerFunction("hashCode", new HashCode());

// use the function in Java Table API
myTable.select("string, string.hashCode(), hashCode(string)");

// use the function in SQL
tableEnv.sql("SELECT string, HASHCODE(string) FROM MyTable");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
object hashCode extends ScalarFunction {

  var hashcode_factor = 12;

  override def open(context: FunctionContext): Unit = {
    // access "hashcode_factor" parameter
    // "12" would be the default value if parameter does not exist
    hashcode_factor = context.getJobParameter("hashcode_factor", "12").toInt
  }

  def eval(s: String): Int = {
    s.hashCode() * hashcode_factor
  }
}

val tableEnv = TableEnvironment.getTableEnvironment(env)

// use the function in Scala Table API
myTable.select('string, hashCode('string))

// register and use the function in SQL
tableEnv.registerFunction("hashCode", hashCode)
tableEnv.sql("SELECT string, HASHCODE(string) FROM MyTable");
{% endhighlight %}

</div>
</div>

{% top %}

