---
title: "自定义函数"
nav-parent_id: python_tableapi
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

**NOTE:** Python UDF execution requires Python version (3.5, 3.6 or 3.7) with PyFlink installed. It's required on both the client side and the cluster side.

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
class HashCode(ScalarFunction):
  def __init__(self):
    self.factor = 12

  def eval(self, s):
    return hash(s) * self.factor

table_env = BatchTableEnvironment.create(env)

# register the Python function
table_env.register_function("hash_code", udf(HashCode(), DataTypes.BIGINT(), DataTypes.BIGINT()))

# use the Python function in Python Table API
my_table.select("string, bigint, bigint.hash_code(), hash_code(bigint)")

# use the Python function in SQL API
table_env.sql_query("SELECT string, bigint, hash_code(bigint) FROM MyTable")
{% endhighlight %}

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

table_env = BatchTableEnvironment.create(env)

# register the Java function
table_env.register_java_function("hash_code", "my.java.function.HashCode")

# use the Java function in Python Table API
my_table.select("string.hash_code(), hash_code(string)")

# use the Java function in SQL API
table_env.sql_query("SELECT string, bigint, hash_code(string) FROM MyTable")
{% endhighlight %}

There are many ways to define a Python scalar function besides extending the base class `ScalarFunction`.
The following examples show the different ways to define a Python scalar function which takes two columns of
bigint as the input parameters and returns the sum of them as the result.

{% highlight python %}
# option 1: extending the base class `ScalarFunction`
class Add(ScalarFunction):
  def eval(self, i, j):
    return i + j

add = udf(Add(), [DataTypes.BIGINT(), DataTypes.BIGINT()], DataTypes.BIGINT())

# option 2: Python function
@udf(input_types=[DataTypes.BIGINT(), DataTypes.BIGINT()], result_type=DataTypes.BIGINT())
def add(i, j):
  return i + j

# option 3: lambda function
add = udf(lambda i, j: i + j, [DataTypes.BIGINT(), DataTypes.BIGINT()], DataTypes.BIGINT())

# option 4: callable function
class CallableAdd(object):
  def __call__(self, i, j):
    return i + j

add = udf(CallableAdd(), [DataTypes.BIGINT(), DataTypes.BIGINT()], DataTypes.BIGINT())

# option 5: partial function
def partial_add(i, j, k):
  return i + j + k

add = udf(functools.partial(partial_add, k=1), [DataTypes.BIGINT(), DataTypes.BIGINT()],
          DataTypes.BIGINT())

# register the Python function
table_env.register_function("add", add)
# use the function in Python Table API
my_table.select("add(a, b)")
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

# register the Python Table Function
table_env.register_function("split", udtf(Split(), DataTypes.STRING(), [DataTypes.STRING(), DataTypes.INT()]))

# use the Python Table Function in Python Table API
my_table.join_lateral("split(a) as (word, length)")
my_table.left_outer_join_lateral("split(a) as (word, length)")

# use the Python Table function in SQL API
table_env.sql_query("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)")
table_env.sql_query("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE")

{% endhighlight %}


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

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)
my_table = ...  # type: Table, table schema: [a: String]

# Register the java function.
table_env.register_java_function("split", "my.java.function.Split")

# Use the table function in the Python Table API. "as" specifies the field names of the table.
my_table.join_lateral("split(a) as (word, length)").select("a, word, length")
my_table.left_outer_join_lateral("split(a) as (word, length)").select("a, word, length")

# Register the python function.

# Use the table function in SQL with LATERAL and TABLE keywords.
# CROSS JOIN a table function (equivalent to "join" in Table API).
table_env.sql_query("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)")
# LEFT JOIN a table function (equivalent to "left_outer_join" in Table API).
table_env.sql_query("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE")
{% endhighlight %}

Like Python scalar functions, you can use the above five ways to define Python TableFunctions.

<span class="label label-info">Note</span> The only difference is that the return type of Python Table Functions needs to be an iterable, iterator or generator.

{% highlight python %}
# option 1: generator function
@udtf(input_types=DataTypes.BIGINT(), result_types=DataTypes.BIGINT())
def generator_func(x):
      yield 1
      yield 2

# option 2: return iterator
@udtf(input_types=DataTypes.BIGINT(), result_types=DataTypes.BIGINT())
def iterator_func(x):
      return range(5)

# option 3: return iterable
@udtf(input_types=DataTypes.BIGINT(), result_types=DataTypes.BIGINT())
def iterable_func(x):
      result = [1, 2, 3]
      return result

table_env.register_function("iterable_func", iterable_func)
table_env.register_function("iterator_func", iterator_func)
table_env.register_function("generator_func", generator_func)

{% endhighlight %}
