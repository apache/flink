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

**NOTE:** Python UDF execution requires Python3.5+ with PyFlink installed. It's required on both the client side and the cluster side.

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
