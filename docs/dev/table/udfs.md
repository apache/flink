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

It should be noted that the Table API and SQL code generation internally tries to work with 
primitive values as much as possible. A user-defined function can introduce much overhead through object 
creation/casting during runtime, therefore it is highly recommended to declare parameters and 
result types as 
primitive types instead of their boxed classes. `Types.DATE` and `Types.TIME` can also be 
represented as `int`. `Types.TIMESTAMP` can be represented as `long`. We also recommended that 
User-Defined Functions should be written by Java instead of Scala language, due to the same reason.

* This will be replaced by the TOC
{:toc}

Register User-Defined Functions
-------------------------------
In most cases, user-defined functions must be registered before use (function registration is not
needed for tableAPI if the query is written by Scala language). This registration is conducted 
through either `StreamTableEnvironment#registerFunction` or `BatchTableEnvironment#registerFunction`. 
It will register the functions into the functionCatalog, such that the query parser of table 
and SQL API will be able to recognize and properly translate the user-defined functions. Please 
find the detailed examples of how to register and use each user-defined function 
(`ScalarFunction`, `TableFunction`, and `AggregateFunction`) in the following sub-sessions.


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

User-Defined Aggregate Functions convert/aggregate a table (more than one row) to a scalar value. 

<center>
<img alt="UDAGG mechanism" src="{{ site.baseurl }}/fig/udagg-mechanism.png" width="80%">
</center>

The above figure shows an example of 
aggregation. Let's say you have a table with columns of name and price for different drinks. If 
there are 5 rows in the table and if you want to find the highest price among these drinks, 
then your query needs to check all 5 rows, but ultimately you get only one output which is the 
highest price. 

To achieve this, we introduce an `accumulator`, the instance which keeps the intermediate result 
during the aggregation. When the aggregation starts, it first initializes the accumulator via a 
`createAccumulator` method, then applies an `accumulate` method with each row onto the accumulator.
The system can evaluate the intermediate result at any time through a `getValue` method.  
 
{% highlight text %}
createAccumulator, accumulate, and getValue are the mandatory methods for an AggregateFunction.
{% endhighlight %}

Flink’s type extraction facilities do not work well for complex types if they are not basic types
 or simple POJOs. So similar as ScalarFunction and TableFunction, AggregateFunction also 
 provides methods for users to specify the TypeInformation of aggregate result type (through 
 `AggregateFunction#getResultType()`) and accumulator type (through `AggregateFunction#getAccumulatorType()`).
 
Besides the above methods, there are a few contracted methods that can be 
optional to have (these methods usually provide the opportunities for a better query optimization,
 but there are cases they are necessary as a must-to-have method. For instance, `merge` method is
 mandatory for the session window, as runtime execution may have to merge two states of two session 
 windows if they have been connected by a late arrival data).
 
{% highlight text %}
retract, merge, and resetAccumulator are usually the optional methods, which provide a better 
optimization.
{% endhighlight %} 
      
All these methods must be declared publicly, not static and 
named exactly as the names mentioned above. The methods `createAccumulator`, `getValue`, 
`getResultType`, and `getAccumulatorType` are 
defined in the `AggregateFunction` abstract class, while others are contracted methods. In order 
to define a table function, one has to extend the base class `AggregateFunction` 
in `org.apache.flink.table.functions`:

{% highlight java %}
import org.apache.flink.table.functions.AggregateFunction;
{% endhighlight %} 

and implement (one or more) `accumulate` methods. 

The detailed explanation for all methods provided by AggregateFunction is introduced below. 


<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
/**
  * Processes the input values and update the provided accumulator instance. The method
  * accumulate can be overloaded with different custom types and arguments. An AggregateFunction
  * requires at least one accumulate() method.
  *
  * @param accumulator           the accumulator which contains the current aggregated results
  * @param [user defined inputs] the input value (usually obtained from a new arrived data).
  *
  * public void accumulate(ACC accumulator, [user defined inputs]);
  *
  *
  * Retracts the input values from the accumulator instance. The current design assumes the
  * inputs are the values that have been previously accumulated. The method retract can be
  * overloaded with different custom types and arguments. This function must be implemented for
  * datastream bounded over aggregate.
  *
  * @param accumulator           the accumulator which contains the current aggregated results
  * @param [user defined inputs] the input value (usually obtained from a new arrived data).
  *
  * public void retract(ACC accumulator, [user defined inputs]);
  *
  *
  * Merges a group of accumulator instances into one accumulator instance. This function must be
  * implemented for datastream session window grouping aggregate and dataset grouping aggregate.
  *
  * @param accumulator  the accumulator which will keep the merged aggregate results. It should
  *                     be noted that the accumulator may contain the previous aggregated
  *                     results. Therefore user should not replace or clean this instance in the
  *                     custom merge method.
  * @param its          an [[java.lang.Iterable]] pointed to a group of accumulators that will be
  *                     merged.
  *
  * public void merge(ACC accumulator, java.lang.Iterable<ACC> its);
  *
  *
  * Resets the accumulator for this [[AggregateFunction]]. This function must be implemented for
  * dataset grouping aggregate.
  *
  * @param accumulator  the accumulator which needs to be reset
  *
  * public void resetAccumulator(ACC accumulator);
  *
  *
  * @param <T>   the type of the aggregation result
  * @param <ACC> the type of the aggregation accumulator. The accumulator is used to keep the
  *             aggregated values which are needed to compute an aggregation result.
  *             AggregateFunction represents its state using accumulator, thereby the state of the
  *             AggregateFunction must be put into the accumulator.
  */
public abstract class AggregateFunction<T, ACC> extends UserDefinedFunction {
  /**
    * Creates and init the Accumulator for this [[AggregateFunction]].
    *
    * @return the accumulator with the initial value
    */
  public ACC createAccumulator();

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
  public T getValue(ACC accumulator);

    /**
    * Returns true if this AggregateFunction can only be applied in an OVER window.
    *
    * @return true if the AggregateFunction requires an OVER window, false otherwise.
    */
  public Boolean requiresOver = false;

  /**
    * Returns the TypeInformation of the AggregateFunction's result.
    *
    * @return The TypeInformation of the AggregateFunction's result or null if the result type
    *         should be automatically inferred.
    */
  public TypeInformation<T> getResultType = null;

  /**
    * Returns the TypeInformation of the AggregateFunction's accumulator.
    *
    * @return The TypeInformation of the AggregateFunction's accumulator or null if the
    *         accumulator type should be automatically inferred.
    */
  public TypeInformation<T> getAccumulatorType = null;
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
/**
  * Processes the input values and update the provided accumulator instance. The method
  * accumulate can be overloaded with different custom types and arguments. An AggregateFunction
  * requires at least one accumulate() method.
  *
  * @param accumulator           the accumulator which contains the current aggregated results
  * @param [user defined inputs] the input value (usually obtained from a new arrived data).
  *
  * def accumulate(accumulator: ACC, [user defined inputs]): Unit
  *
  *
  * Retracts the input values from the accumulator instance. The current design assumes the
  * inputs are the values that have been previously accumulated. The method retract can be
  * overloaded with different custom types and arguments. This function must be implemented for
  * datastream bounded over aggregate.
  *
  * @param accumulator           the accumulator which contains the current aggregated results
  * @param [user defined inputs] the input value (usually obtained from a new arrived data).
  *
  * def retract(accumulator: ACC, [user defined inputs]): Unit
  *
  *
  * Merges a group of accumulator instances into one accumulator instance. This function must be
  * implemented for datastream session window grouping aggregate and dataset grouping aggregate.
  *
  * @param accumulator  the accumulator which will keep the merged aggregate results. It should
  *                     be noted that the accumulator may contain the previous aggregated
  *                     results. Therefore user should not replace or clean this instance in the
  *                     custom merge method.
  * @param its          an [[java.lang.Iterable]] pointed to a group of accumulators that will be
  *                     merged.
  *
  * def merge(accumulator: ACC, its: java.lang.Iterable[ACC]): Unit
  *
  *
  * Resets the accumulator for this [[AggregateFunction]]. This function must be implemented for
  * dataset grouping aggregate.
  *
  * @param accumulator  the accumulator which needs to be reset
  *
  * def resetAccumulator(accumulator: ACC): Unit
  *
  *
  * @tparam T   the type of the aggregation result
  * @tparam ACC the type of the aggregation accumulator. The accumulator is used to keep the
  *             aggregated values which are needed to compute an aggregation result.
  *             AggregateFunction represents its state using accumulator, thereby the state of the
  *             AggregateFunction must be put into the accumulator.
  */
abstract class AggregateFunction[T, ACC] extends UserDefinedFunction {
  /**
    * Creates and init the Accumulator for this [[AggregateFunction]].
    *
    * @return the accumulator with the initial value
    */
  def createAccumulator(): ACC

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
  def getValue(accumulator: ACC): T

  /**
    * Returns true if this AggregateFunction can only be applied in an OVER window.
    *
    * @return true if the AggregateFunction requires an OVER window, false otherwise.
    */
  def requiresOver: Boolean = false

  /**
    * Returns the TypeInformation of the AggregateFunction's result.
    *
    * @return The TypeInformation of the AggregateFunction's result or null if the result type
    *         should be automatically inferred.
    */
  def getResultType: TypeInformation[T] = null

  /**
    * Returns the TypeInformation of the AggregateFunction's accumulator.
    *
    * @return The TypeInformation of the AggregateFunction's accumulator or null if the
    *         accumulator type should be automatically inferred.
    */
  def getAccumulatorType: TypeInformation[ACC] = null
}
{% endhighlight %}
</div>
</div>


The following example shows how to define an AggregateFunction (the example we provide is 
programed to calculate the weighted average on a given column), register it in the 
TableEnvironment, and call it in a query.  To calculate an weighted average value at any given 
time, we have to save the historical records of the weighted sum and count for all the data that 
have been received. To ensure the exact/at-least once senmatics against the
node or program crashes, we put the sum and count into a WeightedAvgAccum (an accumulator which
is maintained and kept in the flink backend state). The accumulate method of weighted average 
AggregateFunction has three inputs. The first one is the WeightedAvgAccum accumulator. The 
other two are user defined inputs: input value (ivalue) and weight of the input (iweight). The 
retract, merge and resetAccumulator methods are not mandatory for most aggregation types. We 
provide them below as examples for the reader’s interests. It should also be noted that we have used java
primitive types and defined getResultType() and getAccumulatorType() methods in the scala example. 
This is because Flink type extraction does not work very well for scala types. Therefore we 
highly recommend user to implement user defined aggregate function by java but not scala. 


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
 * Base class for WeightedAvg.
 */
public static class WeightedAvg extends AggregateFunction<long, WeightedAvgAccum> {
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
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import java.lang.{Long => JLong, Integer => JInteger}
import org.apache.flink.api.java.tuple.{Tuple1 => JTuple1}
import org.apache.flink.api.common.typeinfo.{BasicTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.TupleTypeInfo
import org.apache.flink.table.functions.AggregateFunction

/**
 * Accumulator for WeightedAvg.
 */
class WeightedAvgAccum extends JTuple1[JLong, JInteger] {
  sum = 0L
  count = 0
}

/**
 * Base class for WeightedAvg.
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
    new TupleTypeInfo(classOf[WeightedAvgAccum], 
                      BasicTypeInfo.LONG_TYPE_INFO,
                      BasicTypeInfo.INT_TYPE_INFO)
  }

  override def getResultType: TypeInformation[JLong] =
    BasicTypeInfo.LONG_TYPE_INFO
}
{% endhighlight %}
</div>
</div>


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

