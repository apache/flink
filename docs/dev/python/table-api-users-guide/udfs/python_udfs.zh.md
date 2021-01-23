---
title: "普通自定义函数（UDF）"
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

用户自定义函数是重要的功能，因为它们极大地扩展了Python Table API程序的表达能力。

**注意:** 要执行Python用户自定义函数，客户端和集群端都需要安装Python版本(3.5、3.6、3.7 或 3.8)，并安装PyFlink。

* This will be replaced by the TOC
{:toc}

<a name="scalar-functions"></a>

## 标量函数（ScalarFunction）

PyFlink支持在Python Table API程序中使用Python标量函数。 如果要定义Python标量函数，
可以继承`pyflink.table.udf`中的基类ScalarFunction，并实现eval方法。
Python标量函数的行为由名为`eval`的方法定义，eval方法支持可变长参数，例如`eval(* args)`。

以下示例显示了如何定义自己的Python哈希函数、如何在TableEnvironment中注册它以及如何在作业中使用它。

{% highlight python %}
from pyflink.table.expressions import call 

class HashCode(ScalarFunction):
  def __init__(self):
    self.factor = 12

  def eval(self, s):
    return hash(s) * self.factor

table_env = BatchTableEnvironment.create(env)

hash_code = udf(HashCode(), result_type=DataTypes.BIGINT())

# 在Python Table API中使用Python自定义函数
my_table.select(my_table.string, my_table.bigint, hash_code(my_table.bigint), call(hash_code, my_table.bigint))

# 在SQL API中使用Python自定义函数
table_env.create_temporary_function("hash_code", udf(HashCode(), result_type=DataTypes.BIGINT()))
table_env.sql_query("SELECT string, bigint, hash_code(bigint) FROM MyTable")
{% endhighlight %}

除此之外，还支持在Python Table API程序中使用Java / Scala标量函数。

{% highlight python %}
'''
Java code:

// Java类必须具有公共的无参数构造函数，并且可以在当前的Java类加载器中可以加载到。
public class HashCode extends ScalarFunction {
  private int factor = 12;

  public int eval(String s) {
      return s.hashCode() * factor;
  }
}
'''
from pyflink.table.expressions import call

table_env = BatchTableEnvironment.create(env)

# 注册Java函数
table_env.create_java_temporary_function("hash_code", "my.java.function.HashCode")

# 在Python Table API中使用Java函数
my_table.select(call('hash_code', my_table.string))

# 在SQL API中使用Java函数
table_env.sql_query("SELECT string, bigint, hash_code(string) FROM MyTable")
{% endhighlight %}

除了扩展基类`ScalarFunction`之外，还支持多种方式来定义Python标量函数。
以下示例显示了多种定义Python标量函数的方式。该函数需要两个类型为bigint的参数作为输入参数，并返回它们的总和作为结果。

{% highlight python %}
# 方式一：扩展基类`ScalarFunction`
class Add(ScalarFunction):
  def eval(self, i, j):
    return i + j

add = udf(Add(), result_type=DataTypes.BIGINT())

# 方式二：普通Python函数
@udf(result_type=DataTypes.BIGINT())
def add(i, j):
  return i + j

# 方式三：lambda函数
add = udf(lambda i, j: i + j, result_type=DataTypes.BIGINT())

# 方式四：callable函数
class CallableAdd(object):
  def __call__(self, i, j):
    return i + j

add = udf(CallableAdd(), result_type=DataTypes.BIGINT())

# 方式五：partial函数
def partial_add(i, j, k):
  return i + j + k

add = udf(functools.partial(partial_add, k=1), result_type=DataTypes.BIGINT())

# 注册Python自定义函数
table_env.create_temporary_function("add", add)
# 在Python Table API中使用Python自定义函数
my_table.select("add(a, b)")

# 也可以在Python Table API中直接使用Python自定义函数
my_table.select(add(my_table.a, my_table.b))
{% endhighlight %}

<a name="table-functions"></a>

## 表值函数（TableFunction）
与Python用户自定义标量函数类似，Python用户自定义表值函数以零个，一个或者多个列作为输入参数。但是，与标量函数不同的是，表值函数可以返回
任意数量的行作为输出而不是单个值。Python用户自定义表值函数的返回类型可以是Iterable，Iterator或generator类型。

以下示例说明了如何定义自己的Python自定义表值函数，将其注册到TableEnvironment中，并在作业中使用它。

{% highlight python %}
class Split(TableFunction):
    def eval(self, string):
        for s in string.split(" "):
            yield s, len(s)

env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)
my_table = ...  # type: Table, table schema: [a: String]

# 注册Python表值函数
split = udtf(Split(), result_types=[DataTypes.STRING(), DataTypes.INT()])

# 在Python Table API中使用Python表值函数
my_table.join_lateral(split(my_table.a).alias("word, length"))
my_table.left_outer_join_lateral(split(my_table.a).alias("word, length"))

# 在SQL API中使用Python表值函数
table_env.create_temporary_function("split", udtf(Split(), result_types=[DataTypes.STRING(), DataTypes.INT()]))
table_env.sql_query("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)")
table_env.sql_query("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE")

{% endhighlight %}

除此之外，还支持在Python Table API程序中使用Java / Scala表值函数。
{% highlight python %}
'''
Java code:

// 类型"Tuple2 <String，Integer>"代表，表值函数的输出类型为（String，Integer）。
// Java类必须具有公共的无参数构造函数，并且可以在当前的Java类加载器中加载到。
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

# 注册java自定义函数。
table_env.create_java_temporary_function("split", "my.java.function.Split")

# 在Python Table API中使用表值函数。 "alias"指定表的字段名称。
my_table.join_lateral(call('split', my_table.a).alias("word, length")).select(my_table.a, col('word'), col('length'))
my_table.left_outer_join_lateral(call('split', my_table.a).alias("word, length")).select(my_table.a, col('word'), col('length'))

# 注册python函数。

# 在SQL中将table函数与LATERAL和TABLE关键字一起使用。
# CROSS JOIN表值函数（等效于Table API中的"join"）。
table_env.sql_query("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)")
# LEFT JOIN一个表值函数（等同于Table API中的"left_outer_join"）。
table_env.sql_query("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE")
{% endhighlight %}

像Python标量函数一样，您可以使用上述五种方式来定义Python表值函数。

<span class="label label-info">注意</span> 唯一的区别是，Python表值函数的返回类型必须是iterable（可迭代子类）, iterator（迭代器） or generator（生成器）。

{% highlight python %}
# 方式一：生成器函数
@udtf(result_types=DataTypes.BIGINT())
def generator_func(x):
      yield 1
      yield 2

# 方式二：返回迭代器
@udtf(result_types=DataTypes.BIGINT())
def iterator_func(x):
      return range(5)

# 方式三：返回可迭代子类
@udtf(result_types=DataTypes.BIGINT())
def iterable_func(x):
      result = [1, 2, 3]
      return result
{% endhighlight %}

<a name="aggregate-functions"></a>

## 聚合函数（AggregateFunction）

A user-defined aggregate function (_UDAGG_) maps scalar values of multiple rows to a new scalar value.

**NOTE:** Currently the general user-defined aggregate function is only supported in the GroupBy aggregation of the blink planner in streaming mode. For batch mode or windowed aggregation, it's currently not supported and it is recommended to use the [Vectorized Aggregate Functions]({% link dev/python/table-api-users-guide/udfs/vectorized_python_udfs.zh.md %}#vectorized-aggregate-functions).

The behavior of an aggregate function is centered around the concept of an accumulator. The _accumulator_
is an intermediate data structure that stores the aggregated values until a final aggregation result
is computed.

For each set of rows that need to be aggregated, the runtime will create an empty accumulator by calling
`create_accumulator()`. Subsequently, the `accumulate(...)` method of the aggregate function will be called for each input
row to update the accumulator. Currently after each row has been processed, the `get_value(...)` method of the
aggregate function will be called to compute the aggregated result.

The following example illustrates the aggregation process:

<center>
<img alt="UDAGG mechanism" src="{% link /fig/udagg-mechanism-python.png %}" width="80%">
</center>

In the above example, we assume a table that contains data about beverages. The table consists of three columns (`id`, `name`,
and `price`) and 5 rows. We would like to find the highest price of all beverages in the table, i.e., perform
a `max()` aggregation.

In order to define an aggregate function, one has to extend the base class `AggregateFunction` in
`pyflink.table` and implement the evaluation method named `accumulate(...)`. 
The result type and accumulator type of the aggregate function can be specified by one of the following two approaches:

- Implement the method named `get_result_type()` and `get_accumulator_type()`.
- Wrap the function instance with the decorator `udaf` in `pyflink.table.udf` and specify the parameters `result_type` and `accumulator_type`.

The following example shows how to define your own aggregate function and call it in a query.

{% highlight python %}
from pyflink.common import Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import AggregateFunction, DataTypes, StreamTableEnvironment
from pyflink.table.expressions import call
from pyflink.table.udf import udaf


class WeightedAvg(AggregateFunction):

    def create_accumulator(self):
        # Row(sum, count)
        return Row(0, 0)

    def get_value(self, accumulator):
        if accumulator[1] == 0:
            return None
        else:
            return accumulator[0] / accumulator[1]

    def accumulate(self, accumulator, value, weight):
        accumulator[0] += value * weight
        accumulator[1] += weight
    
    def retract(self, accumulator, value, weight):
        accumulator[0] -= value * weight
        accumulator[1] -= weight
        
    def get_result_type(self):
        return DataTypes.BIGINT()
        
    def get_accumulator_type(self):
        return DataTypes.ROW([
            DataTypes.FIELD("f0", DataTypes.BIGINT()), 
            DataTypes.FIELD("f1", DataTypes.BIGINT())])


env = StreamExecutionEnvironment.get_execution_environment()
table_env = StreamTableEnvironment.create(env)
# the result type and accumulator type can also be specified in the udaf decorator:
# weighted_avg = udaf(WeightedAvg(), result_type=DataTypes.BIGINT(), accumulator_type=...)
weighted_avg = udaf(WeightedAvg())
t = table_env.from_elements([(1, 2, "Lee"),
                             (3, 4, "Jay"),
                             (5, 6, "Jay"),
                             (7, 8, "Lee")]).alias("value", "count", "name")

# call function "inline" without registration in Table API
result = t.group_by(t.name).select(weighted_avg(t.value, t.count).alias("avg")).to_pandas()
print(result)

# register function
table_env.create_temporary_function("weighted_avg", WeightedAvg())

# call registered function in Table API
result = t.group_by(t.name).select(call("weighted_avg", t.value, t.count).alias("avg")).to_pandas()
print(result)

# register table
table_env.create_temporary_view("source", t)

# call registered function in SQL
result = table_env.sql_query(
    "SELECT weighted_avg(`value`, `count`) AS avg FROM source GROUP BY name").to_pandas()
print(result)
{% endhighlight %}

The `accumulate(...)` method of our `WeightedAvg` class takes three input arguments. The first one is the accumulator
and the other two are user-defined inputs. In order to calculate a weighted average value, the accumulator
needs to store the weighted sum and count of all the data that have already been accumulated. In our example, we
use a `Row` object as the accumulator. Accumulators will be managed
by Flink's checkpointing mechanism and are restored in case of failover to ensure exactly-once semantics.

### Mandatory and Optional Methods

**The following methods are mandatory for each `AggregateFunction`:**

- `create_accumulator()`
- `accumulate(...)` 
- `get_value(...)`

**The following methods of `AggregateFunction` are required depending on the use case:**

- `retract(...)` is required when there are operations that could generate retraction messages before the current aggregation operation, e.g. group aggregate, outer join. \
This method is optional, but it is strongly recommended to be implemented to ensure the UDAF can be used in any use case.
- `get_result_type()` and `get_accumulator_type()` is required if the result type and accumulator type would not be specified in the `udaf` decorator.

### ListView and MapView

If an accumulator needs to store large amounts of data, `pyflink.table.ListView` and `pyflink.table.MapView` 
could be used instead of list and dict. These two data structures provide the similar functionalities as list and dict, 
however usually having better performance by leveraging Flink's state backend to eliminate unnecessary state access. 
You can use them by declaring `DataTypes.LIST_VIEW(...)` and `DataTypes.MAP_VIEW(...)` in the accumulator type, e.g.:

{% highlight python %}
from pyflink.table import ListView

class ListViewConcatAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        # the ListView is iterable
        return accumulator[1].join(accumulator[0])

    def create_accumulator(self):
        return Row(ListView(), '')

    def accumulate(self, accumulator, *args):
        accumulator[1] = args[1]
        # the ListView support add, clear and iterate operations.
        accumulator[0].add(args[0])

    def get_accumulator_type(self):
        return DataTypes.ROW([
            # declare the first column of the accumulator as a string ListView.
            DataTypes.FIELD("f0", DataTypes.LIST_VIEW(DataTypes.STRING())),
            DataTypes.FIELD("f1", DataTypes.BIGINT())])

    def get_result_type(self):
        return DataTypes.STRING()
{% endhighlight %}

Currently there are 2 limitations to use the ListView and MapView:

1. The accumulator must be a `Row`.
2. The `ListView` and `MapView` must be the first level children of the `Row` accumulator.

Please refer to the [documentation of the corresponding classes]({{ site.pythondocs_baseurl }}/api/python/pyflink.table.html#pyflink.table.ListView) for more information about this advanced feature.

**NOTE:** For reducing the data transmission cost between Python UDF worker and Java process caused by accessing the data in Flink states(e.g. accumulators and data views), 
there is a cached layer between the raw state handler and the Python state backend. You can adjust the values of these configuration options to change the behavior of the cache layer for best performance:
`python.state.cache-size`, `python.map-state.read-cache-size`, `python.map-state.write-cache-size`, `python.map-state.iterate-response-batch-size`.
For more details please refer to the [Python Configuration Documentation]({% link dev/python/python_config.zh.md %}).
