---
title: "General User-defined Functions"
weight: 5
type: docs
aliases:
  - /dev/python/table-api-users-guide/udfs/python_udfs.html
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

# General User-defined Functions

User-defined functions are important features, because they significantly extend the expressiveness of Python Table API programs.

**NOTE:** Python UDF execution requires Python version (3.6, 3.7 or 3.8) with PyFlink installed. It's required on both the client side and the cluster side. 

## Scalar Functions

It supports to use Python scalar functions in Python Table API programs. In order to define a Python scalar function,
one can extend the base class `ScalarFunction` in `pyflink.table.udf` and implement an evaluation method.
The behavior of a Python scalar function is defined by the evaluation method which is named `eval`.
The evaluation method can support variable arguments, such as `eval(*args)`.

The following example shows how to define your own Python hash code function, register it in the TableEnvironment, and call it in a query.
Note that you can configure your scalar function via a constructor before it is registered:

```python
from pyflink.table.expressions import call 

class HashCode(ScalarFunction):
  def __init__(self):
    self.factor = 12

  def eval(self, s):
    return hash(s) * self.factor

settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
table_env = TableEnvironment.create(settings)

hash_code = udf(HashCode(), result_type=DataTypes.BIGINT())

# use the Python function in Python Table API
my_table.select(my_table.string, my_table.bigint, hash_code(my_table.bigint), call(hash_code, my_table.bigint))

# use the Python function in SQL API
table_env.create_temporary_function("hash_code", udf(HashCode(), result_type=DataTypes.BIGINT()))
table_env.sql_query("SELECT string, bigint, hash_code(bigint) FROM MyTable")
```

It also supports to use Java/Scala scalar functions in Python Table API programs.

```python
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

settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
table_env = TableEnvironment.create(settings)

# register the Java function
table_env.create_java_temporary_function("hash_code", "my.java.function.HashCode")

# use the Java function in Python Table API
my_table.select(call('hash_code', my_table.string))

# use the Java function in SQL API
table_env.sql_query("SELECT string, bigint, hash_code(string) FROM MyTable")
```

There are many ways to define a Python scalar function besides extending the base class `ScalarFunction`.
The following examples show the different ways to define a Python scalar function which takes two columns of
bigint as the input parameters and returns the sum of them as the result.

```python
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
```

## Table Functions
Similar to a Python user-defined scalar function, a user-defined table function takes zero, one, or 
multiple scalar values as input parameters. However in contrast to a scalar function, it can return 
an arbitrary number of rows as output instead of a single value. The return type of a Python UDTF 
could be of types Iterable, Iterator or generator.

The following example shows how to define your own Python multi emit function, register it in the 
TableEnvironment, and call it in a query.

```python
class Split(TableFunction):
    def eval(self, string):
        for s in string.split(" "):
            yield s, len(s)

env_settings = EnvironmentSettings.new_instance().use_blink_planner().is_streaming_mode().build()
table_env = TableEnvironment.create(env_settings)
my_table = ...  # type: Table, table schema: [a: String]

# register the Python Table Function
split = udtf(Split(), result_types=[DataTypes.STRING(), DataTypes.INT()])

# use the Python Table Function in Python Table API
my_table.join_lateral(split(my_table.a).alias("word, length"))
my_table.left_outer_join_lateral(split(my_table.a).alias("word, length"))

# use the Python Table function in SQL API
table_env.create_temporary_function("split", udtf(Split(), result_types=[DataTypes.STRING(), DataTypes.INT()]))
table_env.sql_query("SELECT a, word, length FROM MyTable, LATERAL TABLE(split(a)) as T(word, length)")
table_env.sql_query("SELECT a, word, length FROM MyTable LEFT JOIN LATERAL TABLE(split(a)) as T(word, length) ON TRUE")

```

It also supports to use Java/Scala table functions in Python Table API programs.
```python
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

env_settings = EnvironmentSettings.new_instance().use_blink_planner().is_streaming_mode().build()
table_env = TableEnvironment.create(env_settings)
my_table = ...  # type: Table, table schema: [a: String]

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
```

Like Python scalar functions, you can use the above five ways to define Python TableFunctions.

<span class="label label-info">Note</span> The only difference is that the return type of Python Table Functions needs to be an iterable, iterator or generator.

```python
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
```

## Aggregate Functions

A user-defined aggregate function (_UDAGG_) maps scalar values of multiple rows to a new scalar value.

**NOTE:** Currently the general user-defined aggregate function is only supported in the GroupBy aggregation and Group Window Aggregation of the blink planner in streaming mode. For batch mode, it's currently not supported and it is recommended to use the [Vectorized Aggregate Functions]({{< ref "docs/dev/python/table/udfs/vectorized_python_udfs" >}}#vectorized-aggregate-functions).

The behavior of an aggregate function is centered around the concept of an accumulator. The _accumulator_
is an intermediate data structure that stores the aggregated values until a final aggregation result
is computed.

For each set of rows that need to be aggregated, the runtime will create an empty accumulator by calling
`create_accumulator()`. Subsequently, the `accumulate(...)` method of the aggregate function will be called for each input
row to update the accumulator. Currently after each row has been processed, the `get_value(...)` method of the
aggregate function will be called to compute the aggregated result.

The following example illustrates the aggregation process:

{{< img alt="UDAGG mechanism" src="/fig/udagg-mechanism-python.png" width="80%" >}}


In the above example, we assume a table that contains data about beverages. The table consists of three columns (`id`, `name`,
and `price`) and 5 rows. We would like to find the highest price of all beverages in the table, i.e., perform
a `max()` aggregation.

In order to define an aggregate function, one has to extend the base class `AggregateFunction` in
`pyflink.table` and implement the evaluation method named `accumulate(...)`. 
The result type and accumulator type of the aggregate function can be specified by one of the following two approaches:

- Implement the method named `get_result_type()` and `get_accumulator_type()`.
- Wrap the function instance with the decorator `udaf` in `pyflink.table.udf` and specify the parameters `result_type` and `accumulator_type`.

The following example shows how to define your own aggregate function and call it in a query.

```python
from pyflink.common import Row
from pyflink.table import AggregateFunction, DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table.expressions import call
from pyflink.table.udf import udaf
from pyflink.table.expressions import col, lit
from pyflink.table.window import Tumble


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


env_settings = EnvironmentSettings.new_instance().use_blink_planner().is_streaming_mode().build()
table_env = TableEnvironment.create(env_settings)
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

# use the general Python aggregate function in GroupBy Window Aggregation
tumble_window = Tumble.over(lit(1).hours) \
            .on(col("rowtime")) \
            .alias("w")

result = t.window(tumble_window) \
        .group_by(col('w'), col('name')) \
        .select("w.start, w.end, weighted_avg(value, count)") \
        .to_pandas()
print(result)

```

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
- `merge(...)` is required for session window ang hop window aggregations.
- `get_result_type()` and `get_accumulator_type()` is required if the result type and accumulator type would not be specified in the `udaf` decorator.

### ListView and MapView

If an accumulator needs to store large amounts of data, `pyflink.table.ListView` and `pyflink.table.MapView` 
could be used instead of list and dict. These two data structures provide the similar functionalities as list and dict, 
however usually having better performance by leveraging Flink's state backend to eliminate unnecessary state access. 
You can use them by declaring `DataTypes.LIST_VIEW(...)` and `DataTypes.MAP_VIEW(...)` in the accumulator type, e.g.:

```python
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
```

Currently there are 2 limitations to use the ListView and MapView:

1. The accumulator must be a `Row`.
2. The `ListView` and `MapView` must be the first level children of the `Row` accumulator.

Please refer to the {{< pythondoc file="pyflink.table.html#pyflink.table.ListView" name="documentation of the corresponding classes">}} for more information about this advanced feature.

**NOTE:** For reducing the data transmission cost between Python UDF worker and Java process caused by accessing the data in Flink states(e.g. accumulators and data views), 
there is a cached layer between the raw state handler and the Python state backend. You can adjust the values of these configuration options to change the behavior of the cache layer for best performance:
`python.state.cache-size`, `python.map-state.read-cache-size`, `python.map-state.write-cache-size`, `python.map-state.iterate-response-batch-size`.
For more details please refer to the [Python Configuration Documentation]({{< ref "docs/dev/python/python_config" >}}).

## Table Aggregate Functions

A user-defined table aggregate function (_UDTAGG_) maps scalar values of multiple rows to zero, one, or multiple rows (or structured types). 
The returned record may consist of one or more fields. If an output record consists of only a single field,
the structured record can be omitted, and a scalar value can be emitted that will be implicitly wrapped into a row by the runtime.

**NOTE:** Currently the general user-defined table aggregate function is only supported in the GroupBy aggregation
of the blink planner in streaming mode.

Similar to an [aggregate function](#aggregate-functions), the behavior of a table aggregate is centered around the concept of an accumulator.
The accumulator is an intermediate data structure that stores the aggregated values until a final aggregation result is computed.

For each set of rows that needs to be aggregated, the runtime will create an empty accumulator by calling
`create_accumulator()`. Subsequently, the `accumulate(...)` method of the function is called for each
input row to update the accumulator. Once all rows have been processed, the `emit_value(...)` method of
the function is called to compute and return the final result.

The following example illustrates the aggregation process:

<img alt="UDTAGG mechanism" src="/fig/udtagg-mechanism-python.png" width="80%">

In the example, we assume a table that contains data about beverages. The table consists of three columns (`id`, `name`,
and `price`) and 5 rows. We would like to find the 2 highest prices of all beverages in the table, i.e.,
perform a `TOP2()` table aggregation. We need to consider each of the 5 rows. The result is a table
with the top 2 values.

In order to define a table aggregate function, one has to extend the base class `TableAggregateFunction` in
`pyflink.table` and implement one or more evaluation methods named `accumulate(...)`.

The result type and accumulator type of the aggregate function can be specified by one of the following two approaches:

- Implement the method named `get_result_type()` and `get_accumulator_type()`.
- Wrap the function instance with the decorator `udtaf` in `pyflink.table.udf` and specify the parameters `result_type` and `accumulator_type`. 

The following example shows how to define your own aggregate function and call it in a query.

```python
from pyflink.common import Row
from pyflink.table import DataTypes, TableEnvironment, EnvironmentSettings
from pyflink.table.udf import udtaf, TableAggregateFunction

class Top2(TableAggregateFunction):

    def emit_value(self, accumulator):
        yield Row(accumulator[0])
        yield Row(accumulator[1])

    def create_accumulator(self):
        return [None, None]

    def accumulate(self, accumulator, row):
        if row[0] is not None:
            if accumulator[0] is None or row[0] > accumulator[0]:
                accumulator[1] = accumulator[0]
                accumulator[0] = row[0]
            elif accumulator[1] is None or row[0] > accumulator[1]:
                accumulator[1] = row[0]

    def get_accumulator_type(self):
        return DataTypes.ARRAY(DataTypes.BIGINT())

    def get_result_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT())])


env_settings = EnvironmentSettings.new_instance().use_blink_planner().in_streaming_mode().build()
table_env = TableEnvironment.create(env_settings)
# the result type and accumulator type can also be specified in the udtaf decorator:
# top2 = udtaf(Top2(), result_type=DataTypes.ROW([DataTypes.FIELD("a", DataTypes.BIGINT())]), accumulator_type=DataTypes.ARRAY(DataTypes.BIGINT()))
top2 = udtaf(Top2())
t = table_env.from_elements([(1, 'Hi', 'Hello'),
                              (3, 'Hi', 'hi'),
                              (5, 'Hi2', 'hi'),
                              (7, 'Hi', 'Hello'),
                              (2, 'Hi', 'Hello')], ['a', 'b', 'c'])

# call function "inline" without registration in Table API
result = t.group_by(t.b).flat_aggregate(top2).select('*').to_pandas()

# the result is:
#      b    a
# 0  Hi2  5.0
# 1  Hi2  NaN
# 2   Hi  7.0
# 3   Hi  3.0

```

The `accumulate(...)` method of our `Top2` class takes two inputs. The first one is the accumulator
and the second one is the user-defined input. In order to calculate a result, the accumulator needs to
store the 2 highest values of all the data that has been accumulated. Accumulators are automatically managed
by Flink's checkpointing mechanism and are restored in case of a failure to ensure exactly-once semantics.
The result values are emitted together with a ranking index.

### Mandatory and Optional Methods

**The following methods are mandatory for each `TableAggregateFunction`:**

- `create_accumulator()`
- `accumulate(...)` 
- `emit_value(...)`

**The following methods of `TableAggregateFunction` are required depending on the use case:**

- `retract(...)` is required when there are operations that could generate retraction messages before the current aggregation operation, e.g. group aggregate, outer join. \
This method is optional, but it is strongly recommended to be implemented to ensure the UDTAF can be used in any use case.
- `get_result_type()` and `get_accumulator_type()` is required if the result type and accumulator type would not be specified in the `udtaf` decorator.

### ListView and MapView

Similar to [Aggregation function](#aggregate-functions), we can also use ListView and MapView in Table Aggregate Function.

```python
from pyflink.common import Row
from pyflink.table import ListView
from pyflink.table.types import DataTypes
from pyflink.table.udf import TableAggregateFunction

class ListViewConcatTableAggregateFunction(TableAggregateFunction):

    def emit_value(self, accumulator):
        result = accumulator[1].join(accumulator[0])
        yield Row(result)
        yield Row(result)

    def create_accumulator(self):
        return Row(ListView(), '')

    def accumulate(self, accumulator, *args):
        accumulator[1] = args[1]
        accumulator[0].add(args[0])

    def get_accumulator_type(self):
        return DataTypes.ROW([
            DataTypes.FIELD("f0", DataTypes.LIST_VIEW(DataTypes.STRING())),
            DataTypes.FIELD("f1", DataTypes.BIGINT())])

    def get_result_type(self):
        return DataTypes.ROW([DataTypes.FIELD("a", DataTypes.STRING())])
```
