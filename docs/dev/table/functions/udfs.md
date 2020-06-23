---
title: "User-defined Functions"
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

User-defined functions (UDFs) are extension points to call frequently used logic or custom logic that cannot be expressed otherwise in queries.

User-defined functions can be implemented in a JVM language (such as Java or Scala) or Python. An implementer can use arbitrary third party libraries within a UDF. This page will focus on JVM-based languages.

* This will be replaced by the TOC
{:toc}

Overview
--------

Currently, Flink distinguishes between the following kinds of functions:

- *Scalar functions* map scalar values to a new scalar value.
- *Table functions* map scalar values to new rows.
- *Aggregate functions* map scalar values of multiple rows to a new scalar value.
- *Table aggregate functions* map scalar values of multiple rows to new rows.
- *Async table functions* are special functions for table sources that perform a lookup.

<span class="label label-danger">Attention</span> Scalar and table functions have been updated to the new type system based on [data types]({% link dev/table/types.md %}). Aggregating functions still use the old type system based on `TypeInformation`.

The following example shows how to create a simple scalar function and how to call the function in both Table API and SQL.

For SQL queries, a function must always be registered under a name. For Table API, a function can be registered or directly used _inline_.

<div class="codetabs" markdown="1">

<div data-lang="Java" markdown="1">
{% highlight java %}
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.ScalarFunction;
import static org.apache.flink.table.api.Expressions.*;

// define function logic
public static class SubstringFunction extends ScalarFunction {
  public String eval(String s, Integer begin, Integer end) {
    return s.substring(begin, end);
  }
}

TableEnvironment env = TableEnvironment.create(...);

// call function "inline" without registration in Table API
env.from("MyTable").select(call(SubstringFunction.class, $("myField"), 5, 12));

// register function
env.createTemporarySystemFunction("SubstringFunction", SubstringFunction.class);

// call registered function in Table API
env.from("MyTable").select(call("SubstringFunction", $("myField"), 5, 12));

// call registered function in SQL
env.sqlQuery("SELECT SubstringFunction(myField, 5, 12) FROM MyTable");

{% endhighlight %}
</div>

<div data-lang="Scala" markdown="1">
{% highlight scala %}
import org.apache.flink.table.api._
import org.apache.flink.table.functions.ScalarFunction

// define function logic
class SubstringFunction extends ScalarFunction {
  def eval(s: String, begin: Integer, end: Integer): String = {
    s.substring(begin, end)
  }
}

val env = TableEnvironment.create(...)

// call function "inline" without registration in Table API
env.from("MyTable").select(call(classOf[SubstringFunction], $"myField", 5, 12))

// register function
env.createTemporarySystemFunction("SubstringFunction", classOf[SubstringFunction])

// call registered function in Table API
env.from("MyTable").select(call("SubstringFunction", $"myField", 5, 12))

// call registered function in SQL
env.sqlQuery("SELECT SubstringFunction(myField, 5, 12) FROM MyTable")

{% endhighlight %}
</div>

</div>

For interactive sessions, it is also possible to parameterize functions before using or
registering them. In this case, function _instances_ instead of function _classes_ can be
used as temporary functions.

It requires that the parameters are serializable for shipping
function instances to the cluster.

<div class="codetabs" markdown="1">

<div data-lang="Java" markdown="1">
{% highlight java %}
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.ScalarFunction;
import static org.apache.flink.table.api.Expressions.*;

// define parameterizable function logic
public static class SubstringFunction extends ScalarFunction {

  private boolean endInclusive;

  public SubstringFunction(boolean endInclusive) {
    this.endInclusive = endInclusive;
  }

  public String eval(String s, Integer begin, Integer end) {
    return s.substring(a, endInclusive ? end + 1 : end);
  }
}

TableEnvironment env = TableEnvironment.create(...);

// call function "inline" without registration in Table API
env.from("MyTable").select(call(new SubstringFunction(true), $("myField"), 5, 12));

// register function
env.createTemporarySystemFunction("SubstringFunction", new SubstringFunction(true));

{% endhighlight %}
</div>

<div data-lang="Scala" markdown="1">
{% highlight scala %}
import org.apache.flink.table.api._
import org.apache.flink.table.functions.ScalarFunction

// define parameterizable function logic
class SubstringFunction(val endInclusive) extends ScalarFunction {
  def eval(s: String, begin: Integer, end: Integer): String = {
    s.substring(endInclusive ? end + 1 : end)
  }
}

val env = TableEnvironment.create(...)

// call function "inline" without registration in Table API
env.from("MyTable").select(call(new SubstringFunction(true), $"myField", 5, 12))

// register function
env.createTemporarySystemFunction("SubstringFunction", new SubstringFunction(true))

{% endhighlight %}
</div>

</div>

{% top %}

Implementation Guide
--------------------

<span class="label label-danger">Attention</span> This section only applies to scalar and table functions for now; until aggregate functions have been updated to the new type system.

Independent of the kind of function, all user-defined functions follow some basic implementation principles.

### Function Class

An implementation class must extend from one of the available base classes (e.g. `org.apache.flink.table.functions.ScalarFunction`).

The class must be declared `public`, not `abstract`, and should be globally accessible. Thus, non-static inner or anonymous classes are not allowed.

For storing a user-defined function in a persistent catalog, the class must have a default constructor and must be instantiable during runtime.

### Evaluation Methods

The base class provides a set of methods that can be overridden such as `open()`, `close()`, or `isDeterministic()`.

However, in addition to those declared methods, the main runtime logic that is applied to every incoming record must be implemented through specialized _evaluation methods_.

Depending on the function kind, evaluation methods such as `eval()`, `accumulate()`, or `retract()` are called by code-generated operators during runtime.

The methods must be declared `public` and take a well-defined set of arguments.

Regular JVM method calling semantics apply. Therefore, it is possible to:
- implement overloaded methods such as `eval(Integer)` and `eval(LocalDateTime)`,
- use var-args such as `eval(Integer...)`,
- use object inheritance such as `eval(Object)` that takes both `LocalDateTime` and `Integer`,
- and combinations of the above such as `eval(Object...)` that takes all kinds of arguments.

The following snippets shows an example of an overloaded function:

<div class="codetabs" markdown="1">

<div data-lang="Java" markdown="1">
{% highlight java %}
import org.apache.flink.table.functions.ScalarFunction;

// function with overloaded evaluation methods
public static class SumFunction extends ScalarFunction {

  public Integer eval(Integer a, Integer b) {
    return a + b;
  }

  public Integer eval(String a, String b) {
    return Integer.valueOf(a) + Integer.valueOf();
  }

  public Integer eval(Double... d) {
    double result = 0;
    for (double value : d)
      result += value;
    return (int) result;
  }
}

{% endhighlight %}
</div>

<div data-lang="Scala" markdown="1">
{% highlight scala %}
import org.apache.flink.table.functions.ScalarFunction
import scala.annotation.varargs

// function with overloaded evaluation methods
class SumFunction extends ScalarFunction {

  def eval(a: Integer, b: Integer): Integer = {
    a + b
  }

  def eval(a: String, b: String): Integer = {
    Integer.valueOf(a) + Integer.valueOf(b)
  }

  @varargs // generate var-args like Java
  def eval(d: Double*): Integer = {
    d.sum.toInt
  }
}

{% endhighlight %}
</div>

</div>

### Type Inference

The table ecosystem (similar to the SQL standard) is a strongly typed API. Therefore, both function parameters and return types must be mapped to a [data type]({% link dev/table/types.md %}).

From a logical perspective, the planner needs information about expected types, precision, and scale. From a JVM perspective, the planner needs information about how internal data structures are represented as JVM objects when calling a user-defined function.

The logic for validating input arguments and deriving data types for both the parameters and the result of a function is summarized under the term _type inference_.

Flink's user-defined functions implement an automatic type inference extraction that derives data types from the function's class and its evaluation methods via reflection. If this implicit reflective extraction approach is not successful, the extraction process can be supported by annotating affected parameters, classes, or methods with `@DataTypeHint` and `@FunctionHint`. More examples on how to annotate functions are shown below.

If more advanced type inference logic is required, an implementer can explicitly override the `getTypeInference()` method in every user-defined function. However, the annotation approach is recommended because it keeps custom type inference logic close to the affected locations and falls back to the default behavior for the remaining implementation.

#### Automatic Type Inference

The automatic type inference inspects the function's class and evaluation methods to derive data types for the arguments and result of a function. `@DataTypeHint` and `@FunctionHint` annotations support the automatic extraction.

For a full list of classes that can be implicitly mapped to a data type, see the [data type section]({% link dev/table/types.md %}#data-type-annotations).

**`@DataTypeHint`**

In many scenarios, it is required to support the automatic extraction _inline_ for paramaters and return types of a function

The following example shows how to use data type hints. More information can be found in the documentation of the annotation class.

<div class="codetabs" markdown="1">

<div data-lang="Java" markdown="1">
{% highlight java %}
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

// function with overloaded evaluation methods
public static class OverloadedFunction extends ScalarFunction {

  // no hint required
  public Long eval(long a, long b) {
    return a + b;
  }

  // define the precision and scale of a decimal
  public @DataTypeHint("DECIMAL(12, 3)") BigDecimal eval(double a, double b) {
    return BigDecimal.valueOf(a + b);
  }

  // define a nested data type
  @DataTypeHint("ROW<s STRING, t TIMESTAMP(3) WITH LOCAL TIME ZONE>")
  public Row eval(int i) {
    return Row.of(String.valueOf(i), Instant.ofEpochSecond(i));
  }

  // allow wildcard input and customly serialized output
  @DataTypeHint(value = "RAW", bridgedTo = ByteBuffer.class)
  public ByteBuffer eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
    return MyUtils.serializeToByteBuffer(o);
  }
}

{% endhighlight %}
</div>

<div data-lang="Scala" markdown="1">
{% highlight scala %}
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.annotation.InputGroup
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import scala.annotation.varargs

// function with overloaded evaluation methods
class OverloadedFunction extends ScalarFunction {

  // no hint required
  def eval(a: Long, b: Long): Long = {
    a + b
  }

  // define the precision and scale of a decimal
  @DataTypeHint("DECIMAL(12, 3)")
  def eval(double a, double b): BigDecimal = {
    java.lang.BigDecimal.valueOf(a + b)
  }

  // define a nested data type
  @DataTypeHint("ROW<s STRING, t TIMESTAMP(3) WITH LOCAL TIME ZONE>")
  def eval(Int i): Row = {
    Row.of(java.lang.String.valueOf(i), java.time.Instant.ofEpochSecond(i))
  }

  // allow wildcard input and customly serialized output
  @DataTypeHint(value = "RAW", bridgedTo = classOf[java.nio.ByteBuffer])
  def eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o): java.nio.ByteBuffer = {
    MyUtils.serializeToByteBuffer(o)
  }
}

{% endhighlight %}
</div>

</div>

**`@FunctionHint`**

In some scenarios, it is desirable that one evaluation method handles multiple different data types at the same time. Furthermore, in some scenarios, overloaded evaluation methods have a common result type that should be declared only once.

The `@FunctionHint` annotation can provide a mapping from argument data types to a result data type. It enables annotating entire function classes or evaluation methods for input, accumulator, and result data types. One or more annotations can be declared on top of a class or individually for each evaluation method for overloading function signatures. All hint parameters are optional. If a parameter is not defined, the default reflection-based extraction is used. Hint parameters defined on top of a function class are inherited by all evaluation methods.

The following example shows how to use function hints. More information can be found in the documentation of the annotation class.

<div class="codetabs" markdown="1">

<div data-lang="Java" markdown="1">
{% highlight java %}
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

// function with overloaded evaluation methods
// but globally defined output type
@FunctionHint(output = @DataTypeHint("ROW<s STRING, i INT>"))
public static class OverloadedFunction extends TableFunction<Row> {

  public void eval(int a, int b) {
    collect(Row.of("Sum", a + b));
  }

  // overloading of arguments is still possible
  public void eval() {
    collect(Row.of("Empty args", -1));
  }
}

// decouples the type inference from evaluation methods,
// the type inference is entirely determined by the function hints
@FunctionHint(
  input = [@DataTypeHint("INT"), @DataTypeHint("INT")],
  output = @DataTypeHint("INT")
)
@FunctionHint(
  input = [@DataTypeHint("LONG"), @DataTypeHint("LONG")],
  output = @DataTypeHint("LONG")
)
@FunctionHint(
  input = [],
  output = @DataTypeHint("BOOLEAN")
)
public static class OverloadedFunction extends TableFunction<Object> {

  // an implementer just needs to make sure that a method exists
  // that can be called by the JVM
  public void eval(Object... o) {
    if (o.length == 0) {
      collect(false);
    }
    collect(o[0]);
  }
}

{% endhighlight %}
</div>

<div data-lang="Scala" markdown="1">
{% highlight scala %}

import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.annotation.FunctionHint
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

// function with overloaded evaluation methods
// but globally defined output type
@FunctionHint(output = @DataTypeHint("ROW<s STRING, i INT>"))
class OverloadedFunction extends TableFunction[Row] {

  def eval(a: Int, b: Int): Unit = {
    collect(Row.of("Sum", Int.box(a + b)))
  }

  // overloading of arguments is still possible
  def eval(): Unit = {
    collect(Row.of("Empty args", Int.box(-1)))
  }
}

// decouples the type inference from evaluation methods,
// the type inference is entirely determined by the function hints
@FunctionHint(
  input = Array(@DataTypeHint("INT"), @DataTypeHint("INT")),
  output = @DataTypeHint("INT")
)
@FunctionHint(
  input = Array(@DataTypeHint("LONG"), @DataTypeHint("LONG")),
  output = @DataTypeHint("LONG")
)
@FunctionHint(
  input = Array(),
  output = @DataTypeHint("BOOLEAN")
)
class OverloadedFunction extends TableFunction[AnyRef] {

  // an implementer just needs to make sure that a method exists
  // that can be called by the JVM
  @varargs
  def eval(o: AnyRef*) = {
    if (o.length == 0) {
      collect(Boolean.box(false))
    }
    collect(o(0))
  }
}

{% endhighlight %}
</div>

</div>

#### Custom Type Inference

For most scenarios, `@DataTypeHint` and `@FunctionHint` should be sufficient to model user-defined functions. However, by overriding the automatic type inference defined in `getTypeInference()`, implementers can create arbitrary functions that behave like built-in system functions.

The following example implemented in Java illustrates the potential of a custom type inference logic. It uses a string literal argument to determine the result type of a function. The function takes two string arguments: the first argument represents the string to be parsed, the second argument represents the target type.

<div class="codetabs" markdown="1">

<div data-lang="Java" markdown="1">
{% highlight java %}
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.types.Row;

public static class LiteralFunction extends ScalarFunction {
  public Object eval(String s, String type) {
    switch (type) {
      case "INT":
        return Integer.valueOf(s);
      case "DOUBLE":
        return Double.valueOf(s);
      case "STRING":
      default:
        return s;
    }
  }

  // the automatic, reflection-based type inference is disabled and
  // replaced by the following logic
  @Override
  public TypeInference getTypeInference(DataTypeFactory typeFactory) {
    return TypeInference.newBuilder()
      // specify typed arguments
      // parameters will be casted implicitly to those types if necessary
      .typedArguments(DataTypes.STRING(), DataTypes.STRING())
      // specify a strategy for the result data type of the function
      .outputTypeStrategy(callContext -> {
        if (!callContext.isArgumentLiteral(1) || callContext.isArgumentNull(1)) {
          throw callContext.newValidationError("Literal expected for second argument.");
        }
        // return a data type based on a literal
        final String literal = callContext.getArgumentValue(1, String.class).orElse("STRING");
        switch (literal) {
          case "INT":
            return Optional.of(DataTypes.INT().notNull());
          case "DOUBLE":
            return Optional.of(DataTypes.DOUBLE().notNull());
          case "STRING":
          default:
            return Optional.of(DataTypes.STRING());
        }
      })
      .build();
  }
}

{% endhighlight %}
</div>

</div>

### Runtime Integration

Sometimes it might be necessary for a user-defined function to get global runtime information or do some setup/clean-up work before the actual work. User-defined functions provide `open()` and `close()` methods that can be overridden and provide similar functionality as the methods in `RichFunction` of DataStream API.

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
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

public static class HashCodeFunction extends ScalarFunction {

    private int factor = 0;

    @Override
    public void open(FunctionContext context) throws Exception {
        // access the global "hashcode_factor" parameter
        // "12" would be the default value if the parameter does not exist
        factor = Integer.parseInt(context.getJobParameter("hashcode_factor", "12"));
    }

    public int eval(String s) {
        return s.hashCode() * factor;
    }
}

TableEnvironment env = TableEnvironment.create(...);

// add job parameter
env.getConfig().addJobParameter("hashcode_factor", "31");

// register the function
env.createTemporarySystemFunction("hashCode", HashCodeFunction.class);

// use the function
env.sqlQuery("SELECT myField, hashCode(myField) FROM MyTable");

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.table.api._
import org.apache.flink.table.functions.FunctionContext
import org.apache.flink.table.functions.ScalarFunction

class HashCodeFunction extends ScalarFunction {

  private var factor: Int = 0

  override def open(context: FunctionContext): Unit = {
    // access the global "hashcode_factor" parameter
    // "12" would be the default value if the parameter does not exist
    factor = context.getJobParameter("hashcode_factor", "12").toInt
  }

  def eval(s: String): Int = {
    s.hashCode * factor
  }
}

val env = TableEnvironment.create(...)

// add job parameter
env.getConfig.addJobParameter("hashcode_factor", "31")

// register the function
env.createTemporarySystemFunction("hashCode", classOf[HashCodeFunction])

// use the function
env.sqlQuery("SELECT myField, hashCode(myField) FROM MyTable")

{% endhighlight %}
</div>

</div>

{% top %}

Scalar Functions
----------------

A user-defined scalar function maps zero, one, or multiple scalar values to a new scalar value. Any data type listed in the [data types section]({% link dev/table/types.md %}) can be used as a parameter or return type of an evaluation method.

In order to define a scalar function, one has to extend the base class `ScalarFunction` in `org.apache.flink.table.functions` and implement one or more evaluation methods named `eval(...)`.

The following example shows how to define your own hash code function and call it in a query. See the [Implementation Guide](#implementation-guide) for more details.

<div class="codetabs" markdown="1">

<div data-lang="Java" markdown="1">
{% highlight java %}
import org.apache.flink.table.annotation.InputGroup;
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.ScalarFunction;
import static org.apache.flink.table.api.Expressions.*;

public static class HashFunction extends ScalarFunction {

  // take any data type and return INT
  public int eval(@DataTypeHint(inputGroup = InputGroup.ANY) Object o) {
    return o.hashCode();
  }
}

TableEnvironment env = TableEnvironment.create(...);

// call function "inline" without registration in Table API
env.from("MyTable").select(call(HashFunction.class, $("myField")));

// register function
env.createTemporarySystemFunction("HashFunction", HashFunction.class);

// call registered function in Table API
env.from("MyTable").select(call("HashFunction", $("myField")));

// call registered function in SQL
env.sqlQuery("SELECT HashFunction(myField) FROM MyTable");

{% endhighlight %}
</div>

<div data-lang="Scala" markdown="1">
{% highlight scala %}
import org.apache.flink.table.annotation.InputGroup
import org.apache.flink.table.api._
import org.apache.flink.table.functions.ScalarFunction

class HashFunction extends ScalarFunction {

  // take any data type and return INT
  def eval(@DataTypeHint(inputGroup = InputGroup.ANY) o: AnyRef): Int {
    return o.hashCode();
  }
}

val env = TableEnvironment.create(...)

// call function "inline" without registration in Table API
env.from("MyTable").select(call(classOf[HashFunction], $"myField"))

// register function
env.createTemporarySystemFunction("HashFunction", classOf[HashFunction])

// call registered function in Table API
env.from("MyTable").select(call("HashFunction", $"myField"))

// call registered function in SQL
env.sqlQuery("SELECT HashFunction(myField) FROM MyTable")

{% endhighlight %}
</div>

</div>

If you intend to implement or call functions in Python, please refer to the [Python Scalar Functions]({% link dev/table/python/python_udfs.md %}#scalar-functions) documentation for more details.

{% top %}

Table Functions
---------------

Similar to a user-defined scalar function, a user-defined table function takes zero, one, or multiple scalar values as input arguments. However, in contrast to a scalar function, it can return an arbitrary number of rows (or structured types) as output instead of a single value. The returned record may consist of one or more fields. If an output record consists of only one field, the structured record can be omitted and a scalar value can be emitted. It will be wrapped into an implicit row by the runtime.

In order to define a table function, one has to extend the base class `TableFunction` in `org.apache.flink.table.functions` and implement one or more evaluation methods named `eval(...)`. Similar to other functions, input and output data types are automatically extracted using reflection. This includes the generic argument `T` of the class for determining an output data type. In contrast to scalar functions, the evaluation method itself must not have a return type, instead, table functions provide a `collect(T)` method that can be called within every evaluation method for emitting zero, one, or more records.

In the Table API, a table function is used with `.joinLateral(...)` or `.leftOuterJoinLateral(...)`. The `joinLateral` operator (cross) joins each row from the outer table (table on the left of the operator) with all rows produced by the table-valued function (which is on the right side of the operator). The `leftOuterJoinLateral` operator joins each row from the outer table (table on the left of the operator) with all rows produced by the table-valued function (which is on the right side of the operator) and preserves outer rows for which the table function returns an empty table.

In SQL, use `LATERAL TABLE(<TableFunction>)` with `JOIN` or `LEFT JOIN` with an `ON TRUE` join condition.

The following example shows how to define your own split function and call it in a query. See the [Implementation Guide](#implementation-guide) for more details.

<div class="codetabs" markdown="1">

<div data-lang="Java" markdown="1">
{% highlight java %}
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.*;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import static org.apache.flink.table.api.Expressions.*;

@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
public static class SplitFunction extends TableFunction<Row> {

  public void eval(String str) {
    for (String s : str.split(" ")) {
      // use collect(...) to emit a row
      collect(Row.of(s, s.length()));
    }
  }
}

TableEnvironment env = TableEnvironment.create(...);

// call function "inline" without registration in Table API
env
  .from("MyTable")
  .joinLateral(call(SplitFunction.class, $("myField")))
  .select($("myField"), $("word"), $("length"));
env
  .from("MyTable")
  .leftOuterJoinLateral(call(SplitFunction.class, $("myField")))
  .select($("myField"), $("word"), $("length"));

// rename fields of the function in Table API
env
  .from("MyTable")
  .leftOuterJoinLateral(call(SplitFunction.class, $("myField")).as("newWord", "newLength"))
  .select($("myField"), $("newWord"), $("newLength"));

// register function
env.createTemporarySystemFunction("SplitFunction", SplitFunction.class);

// call registered function in Table API
env
  .from("MyTable")
  .joinLateral(call("SplitFunction", $("myField")))
  .select($("myField"), $("word"), $("length"));
env
  .from("MyTable")
  .leftOuterJoinLateral(call("SplitFunction", $("myField")))
  .select($("myField"), $("word"), $("length"));

// call registered function in SQL
env.sqlQuery(
  "SELECT myField, word, length " +
  "FROM MyTable, LATERAL TABLE(SplitFunction(myField))");
env.sqlQuery(
  "SELECT myField, word, length " +
  "FROM MyTable " +
  "LEFT JOIN LATERAL TABLE(SplitFunction(myField)) ON TRUE");

// rename fields of the function in SQL
env.sqlQuery(
  "SELECT myField, newWord, newLength " +
  "FROM MyTable " +
  "LEFT JOIN LATERAL TABLE(SplitFunction(myField)) AS T(newWord, newLength) ON TRUE");

{% endhighlight %}
</div>

<div data-lang="Scala" markdown="1">
{% highlight scala %}
import org.apache.flink.table.annotation.DataTypeHint
import org.apache.flink.table.annotation.FunctionHint
import org.apache.flink.table.api._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

@FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
class SplitFunction extends TableFunction[Row] {

  def eval(str: String): Unit = {
    // use collect(...) to emit a row
    str.split(" ").foreach(s => collect(Row.of(s, s.length)))
  }
}

val env = TableEnvironment.create(...)

// call function "inline" without registration in Table API
env
  .from("MyTable")
  .joinLateral(call(classOf[SplitFunction], $"myField")
  .select($"myField", $"word", $"length")
env
  .from("MyTable")
  .leftOuterJoinLateral(call(classOf[SplitFunction], $"myField"))
  .select($"myField", $"word", $"length")

// rename fields of the function in Table API
env
  .from("MyTable")
  .leftOuterJoinLateral(call(classOf[SplitFunction], $"myField").as("newWord", "newLength"))
  .select($"myField", $"newWord", $"newLength")

// register function
env.createTemporarySystemFunction("SplitFunction", classOf[SplitFunction])

// call registered function in Table API
env
  .from("MyTable")
  .joinLateral(call("SplitFunction", $"myField"))
  .select($"myField", $"word", $"length")
env
  .from("MyTable")
  .leftOuterJoinLateral(call("SplitFunction", $"myField"))
  .select($"myField", $"word", $"length")

// call registered function in SQL
env.sqlQuery(
  "SELECT myField, word, length " +
  "FROM MyTable, LATERAL TABLE(SplitFunction(myField))");
env.sqlQuery(
  "SELECT myField, word, length " +
  "FROM MyTable " +
  "LEFT JOIN LATERAL TABLE(SplitFunction(myField)) ON TRUE")

// rename fields of the function in SQL
env.sqlQuery(
  "SELECT myField, newWord, newLength " +
  "FROM MyTable " +
  "LEFT JOIN LATERAL TABLE(SplitFunction(myField)) AS T(newWord, newLength) ON TRUE")

{% endhighlight %}
</div>

</div>

If you intend to implement functions in Scala, do not implement a table function as a Scala `object`. Scala `object`s are singletons and will cause concurrency issues.

If you intend to implement or call functions in Python, please refer to the [Python Table Functions]({% link dev/table/python/python_udfs.md %}#table-functions) documentation for more details.

{% top %}

Aggregation Functions
---------------------

User-Defined Aggregate Functions (UDAGGs) aggregate a table (one or more rows with one or more attributes) to a scalar value. 

<center>
<img alt="UDAGG mechanism" src="{{ site.baseurl }}/fig/udagg-mechanism.png" width="80%">
</center>

The above figure shows an example of an aggregation. Assume you have a table that contains data about beverages. The table consists of three columns, `id`, `name` and `price` and 5 rows. Imagine you need to find the highest price of all beverages in the table, i.e., perform a `max()` aggregation. You would need to check each of the 5 rows and the result would be a single numeric value.

User-defined aggregation functions are implemented by extending the `AggregateFunction` class. An `AggregateFunction` works as follows. First, it needs an `accumulator`, which is the data structure that holds the intermediate result of the aggregation. An empty accumulator is created by calling the `createAccumulator()` method of the `AggregateFunction`. Subsequently, the `accumulate()` method of the function is called for each input row to update the accumulator. Once all rows have been processed, the `getValue()` method of the function is called to compute and return the final result. 

**The following methods are mandatory for each `AggregateFunction`:**

- `createAccumulator()`
- `accumulate()` 
- `getValue()`

Flink’s type extraction facilities can fail to identify complex data types, e.g., if they are not basic types or simple POJOs. So similar to `ScalarFunction` and `TableFunction`, `AggregateFunction` provides methods to specify the `TypeInformation` of the result type (through 
 `AggregateFunction#getResultType()`) and the type of the accumulator (through `AggregateFunction#getAccumulatorType()`).
 
Besides the above methods, there are a few contracted methods that can be 
optionally implemented. While some of these methods allow the system more efficient query execution, others are mandatory for certain use cases. For instance, the `merge()` method is mandatory if the aggregation function should be applied in the context of a session group window (the accumulators of two session windows need to be joined when a row is observed that "connects" them). 

**The following methods of `AggregateFunction` are required depending on the use case:**

- `retract()` is required for aggregations on bounded `OVER` windows.
- `merge()` is required for many batch aggregations and session window aggregations.
- `resetAccumulator()` is required for many batch aggregations.

All methods of `AggregateFunction` must be declared as `public`, not `static` and named exactly as the names mentioned above. The methods `createAccumulator`, `getValue`, `getResultType`, and `getAccumulatorType` are defined in the `AggregateFunction` abstract class, while others are contracted methods. In order to define a aggregate function, one has to extend the base class `org.apache.flink.table.functions.AggregateFunction` and implement one (or more) `accumulate` methods. The method `accumulate` can be overloaded with different parameter types and supports variable arguments.

Detailed documentation for all methods of `AggregateFunction` is given below. 

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


The following example shows how to

- define an `AggregateFunction` that calculates the weighted average on a given column, 
- register the function in the `TableEnvironment`, and 
- use the function in a query.  

To calculate an weighted average value, the accumulator needs to store the weighted sum and count of all the data that has been accumulated. In our example we define a class `WeightedAvgAccum` to be the accumulator. Accumulators are automatically backup-ed by Flink's checkpointing mechanism and restored in case of a failure to ensure exactly-once semantics.

The `accumulate()` method of our `WeightedAvg` `AggregateFunction` has three inputs. The first one is the `WeightedAvgAccum` accumulator, the other two are user-defined inputs: input value `ivalue` and weight of the input `iweight`. Although the `retract()`, `merge()`, and `resetAccumulator()` methods are not mandatory for most aggregation types, we provide them below as examples. Please note that we used Java primitive types and defined `getResultType()` and `getAccumulatorType()` methods in the Scala example because Flink type extraction does not work very well for Scala types.

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

// register function
StreamTableEnvironment tEnv = ...
tEnv.registerFunction("wAvg", new WeightedAvg());

// use function
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

// register function
val tEnv: StreamTableEnvironment = ???
tEnv.registerFunction("wAvg", new WeightedAvg())

// use function
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

# register function
t_env = ...  # type: StreamTableEnvironment
t_env.register_java_function("wAvg", "my.java.function.WeightedAvg")

# use function
t_env.sql_query("SELECT user, wAvg(points, level) AS avgPoints FROM userScores GROUP BY user")

{% endhighlight %}
</div>
</div>

{% top %}

Table Aggregation Functions
---------------------

User-Defined Table Aggregate Functions (UDTAGGs) aggregate a table (one or more rows with one or more attributes) to a result table with multi rows and columns. 

<center>
<img alt="UDAGG mechanism" src="{{ site.baseurl }}/fig/udtagg-mechanism.png" width="80%">
</center>

The above figure shows an example of a table aggregation. Assume you have a table that contains data about beverages. The table consists of three columns, `id`, `name` and `price` and 5 rows. Imagine you need to find the top 2 highest prices of all beverages in the table, i.e., perform a `top2()` table aggregation. You would need to check each of the 5 rows and the result would be a table with the top 2 values.

User-defined table aggregation functions are implemented by extending the `TableAggregateFunction` class. A `TableAggregateFunction` works as follows. First, it needs an `accumulator`, which is the data structure that holds the intermediate result of the aggregation. An empty accumulator is created by calling the `createAccumulator()` method of the `TableAggregateFunction`. Subsequently, the `accumulate()` method of the function is called for each input row to update the accumulator. Once all rows have been processed, the `emitValue()` method of the function is called to compute and return the final results. 

**The following methods are mandatory for each `TableAggregateFunction`:**

- `createAccumulator()`
- `accumulate()` 

Flink’s type extraction facilities can fail to identify complex data types, e.g., if they are not basic types or simple POJOs. So similar to `ScalarFunction` and `TableFunction`, `TableAggregateFunction` provides methods to specify the `TypeInformation` of the result type (through 
 `TableAggregateFunction#getResultType()`) and the type of the accumulator (through `TableAggregateFunction#getAccumulatorType()`).
 
Besides the above methods, there are a few contracted methods that can be 
optionally implemented. While some of these methods allow the system more efficient query execution, others are mandatory for certain use cases. For instance, the `merge()` method is mandatory if the aggregation function should be applied in the context of a session group window (the accumulators of two session windows need to be joined when a row is observed that "connects" them). 

**The following methods of `TableAggregateFunction` are required depending on the use case:**

- `retract()` is required for aggregations on bounded `OVER` windows.
- `merge()` is required for many batch aggregations and session window aggregations.
- `resetAccumulator()` is required for many batch aggregations.
- `emitValue()` is required for batch and window aggregations.

**The following methods of `TableAggregateFunction` are used to improve the performance of streaming jobs:**

- `emitUpdateWithRetract()` is used to emit values that have been updated under retract mode.

For `emitValue` method, it emits full data according to the accumulator. Take TopN as an example, `emitValue` emit all top n values each time. This may bring performance problems for streaming jobs. To improve the performance, a user can also implement `emitUpdateWithRetract` method to improve the performance. The method outputs data incrementally in retract mode, i.e., once there is an update, we have to retract old records before sending new updated ones. The method will be used in preference to the `emitValue` method if they are all defined in the table aggregate function, because `emitUpdateWithRetract` is treated to be more efficient than `emitValue` as it can output values incrementally.

All methods of `TableAggregateFunction` must be declared as `public`, not `static` and named exactly as the names mentioned above. The methods `createAccumulator`, `getResultType`, and `getAccumulatorType` are defined in the parent abstract class of `TableAggregateFunction`, while others are contracted methods. In order to define a table aggregate function, one has to extend the base class `org.apache.flink.table.functions.TableAggregateFunction` and implement one (or more) `accumulate` methods. The method `accumulate` can be overloaded with different parameter types and supports variable arguments.

Detailed documentation for all methods of `TableAggregateFunction` is given below. 

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


The following example shows how to

- define a `TableAggregateFunction` that calculates the top 2 values on a given column, 
- register the function in the `TableEnvironment`, and 
- use the function in a Table API query(TableAggregateFunction is only supported by Table API).  

To calculate the top 2 values, the accumulator needs to store the biggest 2 values of all the data that has been accumulated. In our example we define a class `Top2Accum` to be the accumulator. Accumulators are automatically backup-ed by Flink's checkpointing mechanism and restored in case of a failure to ensure exactly-once semantics.

The `accumulate()` method of our `Top2` `TableAggregateFunction` has two inputs. The first one is the `Top2Accum` accumulator, the other one is the user-defined input: input value `v`. Although the `merge()` method is not mandatory for most table aggregation types, we provide it below as examples. Please note that we used Java primitive types and defined `getResultType()` and `getAccumulatorType()` methods in the Scala example because Flink type extraction does not work very well for Scala types.

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

// register function
StreamTableEnvironment tEnv = ...
tEnv.registerFunction("top2", new Top2());

// init table
Table tab = ...;

// use function
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

// init table
val tab = ...

// use function
tab
  .groupBy('key)
  .flatAggregate(top2('a) as ('v, 'rank))
  .select('key, 'v, 'rank)

{% endhighlight %}
</div>
</div>


The following example shows how to use `emitUpdateWithRetract` method to emit only updates. To emit only updates, in our example, the accumulator keeps both old and new top 2 values. Note: if the N of topN is big, it may inefficient to keep both old and new values. One way to solve this case is to store the input record into the accumulator in `accumulate` method and then perform calculation in `emitUpdateWithRetract`.

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

// register function
StreamTableEnvironment tEnv = ...
tEnv.registerFunction("top2", new Top2());

// init table
Table tab = ...;

// use function
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

// init table
val tab = ...

// use function
tab
  .groupBy('key)
  .flatAggregate(top2('a) as ('v, 'rank))
  .select('key, 'v, 'rank)

{% endhighlight %}
</div>
</div>

{% top %}
