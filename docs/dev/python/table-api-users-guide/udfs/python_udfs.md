---
title: "General User-defined Functions"
nav-id: general_python_udf
nav-parent_id: python_udf
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

User-defined functions are important features, because they significantly extend the expressiveness of Python Table API programs.

**NOTE:** Python UDF execution requires Python version (3.5, 3.6, 3.7 or 3.8) with PyFlink installed. It's required on both the client side and the cluster side. 

* This will be replaced by the TOC
{:toc}

## Scalar Functions
It supports to use Python scalar functions in Python Table API programs. In order to define a Python scalar function,
one can extend the base class `ScalarFunction` in `pyflink.table.udf` and implement an evaluation method.
The behavior of a Python scalar function is defined by the evaluation method which is named `eval`.
The evaluation method can support variable arguments, such as `eval(*args)`.

The following example shows how to define your own Python hash code function, register it in the TableEnvironment, and call it in a query.
Note that you can configure your scalar function via a constructor before it is registered:

{% highlight python %}
from pyflink.table.expressions import call 

class HashCode(ScalarFunction):
  def __init__(self):
    self.factor = 12

  def eval(self, s):
    return hash(s) * self.factor

table_env = BatchTableEnvironment.create(env)

# configure the off-heap memory of current taskmanager to enable the python worker uses off-heap memory.
table_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '80m')

hash_code = udf(HashCode(), result_type=DataTypes.BIGINT())

# use the Python function in Python Table API
my_table.select(my_table.string, my_table.bigint, hash_code(my_table.bigint), call(hash_code, my_table.bigint))

# use the Python function in SQL API
table_env.create_temporary_function("hash_code", udf(HashCode(), result_type=DataTypes.BIGINT()))
table_env.sql_query("SELECT string, bigint, hash_code(bigint) FROM MyTable")
{% endhighlight %}

<span class="label label-info">Note</span> If not using RocksDB as state backend, you can also configure the python
worker to use the managed memory of taskmanager by setting **python.fn-execution.memory.managed** to be **true**.
Then there is no need to set the the configuration **taskmanager.memory.task.off-heap.size**.

It also supports to use Java/Scala scalar functions in Python Table API programs.

{% highlight python %}
'''
Java code:

// The Java class must have a public no-argument constructor and can be founded in current Java classloader.
public class HashCode extends ScalarFunction {
  private int factor = 12;

  public int eval(String s) {
      return s.hashCode() * factor;
  }
}
'''
from pyflink.table.expressions import call

table_env = BatchTableEnvironment.create(env)

# configure the off-heap memory of current taskmanager to enable the python worker uses off-heap memory.
table_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '80m')

# register the Java function
table_env.create_java_temporary_function("hash_code", "my.java.function.HashCode")

# use the Java function in Python Table API
my_table.select(call('hash_code', my_table.string))

# use the Java function in SQL API
table_env.sql_query("SELECT string, bigint, hash_code(string) FROM MyTable")
{% endhighlight %}

<span class="label label-info">Note</span> If not using RocksDB as state backend, you can also configure the python
worker to use the managed memory of taskmanager by setting **python.fn-execution.memory.managed** to be **true**.
Then there is no need to set the the configuration **taskmanager.memory.task.off-heap.size**.

There are many ways to define a Python scalar function besides extending the base class `ScalarFunction`.
The following examples show the different ways to define a Python scalar function which takes two columns of
bigint as the input parameters and returns the sum of them as the result.

{% highlight python %}
# option 1: extending the base class `ScalarFunction`
class Add(ScalarFunction):
  def eval(self, i, j):
    return i + j

add = udf(Add(), result_type=DataTypes.BIGINT())

# option 2: Python function
@udf(result_type=DataTypes.BIGINT())
def add(i, j):
  return i + j

# option 3: lambda function
add = udf(lambda i, j: i + j, result_type=DataTypes.BIGINT())

# option 4: callable function
class CallableAdd(object):
  def __call__(self, i, j):
    return i + j

add = udf(CallableAdd(), result_type=DataTypes.BIGINT())

# option 5: partial function
def partial_add(i, j, k):
  return i + j + k

add = udf(functools.partial(partial_add, k=1), result_type=DataTypes.BIGINT())

# register the Python function
table_env.create_temporary_function("add", add)
# use the function in Python Table API
my_table.select("add(a, b)")

# You can also use the Python function in Python Table API directly
my_table.select(add(my_table.a, my_table.b))
{% endhighlight %}

## Table Functions
Similar to a Python user-defined scalar function, a user-defined table function takes zero, one, or 
multiple scalar values as input parameters. However in contrast to a scalar function, it can return 
an arbitrary number of rows as output instead of a single value. The return type of a Python UDTF 
could be of types Iterable, Iterator or generator.

The following example shows how to define your own Python multi emit function, register it in the 
TableEnvironment, and call it in a query.

{% highlight python %}
class Split(TableFunction):
    def eval(self, string):
        for s in string.split(" "):
            yield s, len(s)

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)
my_table = ...  # type: Table, table schema: [a: String]

# configure the off-heap memory of current taskmanager to enable the python worker uses off-heap memory.
table_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '80m')

# register the Python Table Function
split = udtf(Split(), result_types=[DataTypes.STRING(), DataTypes.INT()])

# use the Python Table Function in Python Table API
my_table.join_lateral(split(my_table.a).alias("word, length"))
my_table.left_outer_join_lateral(split(my_table.a).alias("word, length"))

# use the Python Table function in SQL API
table_env.create_temporary_function("split", udtf(Split(), result_types=[DataTypes.STRING(), DataTypes.INT()]))
table_env.sql_query("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)")
table_env.sql_query("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE")

{% endhighlight %}

<span class="label label-info">Note</span> If not using RocksDB as state backend, you can also configure the python
worker to use the managed memory of taskmanager by setting **python.fn-execution.memory.managed** to be **true**.
Then there is no need to set the the configuration **taskmanager.memory.task.off-heap.size**.

It also supports to use Java/Scala table functions in Python Table API programs.
{% highlight python %}
'''
Java code:

// The generic type "Tuple2<String, Integer>" determines the schema of the returned table as (String, Integer).
// The java class must have a public no-argument constructor and can be founded in current java classloader.
public class Split extends TableFunction<Tuple2<String, Integer>> {
    private String separator = " ";
    
    public void eval(String str) {
        for (String s : str.split(separator)) {
            // use collect(...) to emit a row
            collect(new Tuple2<String, Integer>(s, s.length()));
        }
    }
}
'''
from pyflink.table.expressions import call

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)
my_table = ...  # type: Table, table schema: [a: String]

# configure the off-heap memory of current taskmanager to enable the python worker uses off-heap memory.
table_env.get_config().get_configuration().set_string("taskmanager.memory.task.off-heap.size", '80m')

# Register the java function.
table_env.create_java_temporary_function("split", "my.java.function.Split")

# Use the table function in the Python Table API. "alias" specifies the field names of the table.
my_table.join_lateral(call('split', my_table.a).alias("word, length")).select(my_table.a, col('word'), col('length'))
my_table.left_outer_join_lateral(call('split', my_table.a).alias("word, length")).select(my_table.a, col('word'), col('length'))

# Register the python function.

# Use the table function in SQL with LATERAL and TABLE keywords.
# CROSS JOIN a table function (equivalent to "join" in Table API).
table_env.sql_query("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)")
# LEFT JOIN a table function (equivalent to "left_outer_join" in Table API).
table_env.sql_query("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE")
{% endhighlight %}

<span class="label label-info">Note</span> If not using RocksDB as state backend, you can also configure the python
worker to use the managed memory of taskmanager by setting **python.fn-execution.memory.managed** to be **true**.
Then there is no need to set the the configuration **taskmanager.memory.task.off-heap.size**.

Like Python scalar functions, you can use the above five ways to define Python TableFunctions.

<span class="label label-info">Note</span> The only difference is that the return type of Python Table Functions needs to be an iterable, iterator or generator.

{% highlight python %}
# option 1: generator function
@udtf(result_types=DataTypes.BIGINT())
def generator_func(x):
      yield 1
      yield 2

# option 2: return iterator
@udtf(result_types=DataTypes.BIGINT())
def iterator_func(x):
      return range(5)

# option 3: return iterable
@udtf(result_types=DataTypes.BIGINT())
def iterable_func(x):
      result = [1, 2, 3]
      return result
{% endhighlight %}
