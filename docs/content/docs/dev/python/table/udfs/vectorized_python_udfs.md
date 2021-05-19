---
title: "Vectorized User-defined Functions"
weight: 10
type: docs
aliases:
  - /dev/python/table-api-users-guide/udfs/vectorized_python_udfs.html
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

# Vectorized User-defined Functions

Vectorized Python user-defined functions are functions which are executed by transferring a batch of elements between JVM and Python VM in Arrow columnar format.
The performance of vectorized Python user-defined functions are usually much higher than non-vectorized Python user-defined functions as the serialization/deserialization
overhead and invocation overhead are much reduced. Besides, users could leverage the popular Python libraries such as Pandas, Numpy, etc for the vectorized Python user-defined functions implementation.
These Python libraries are highly optimized and provide high-performance data structures and functions. It shares the similar way as the
[non-vectorized user-defined functions]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}) on how to define vectorized user-defined functions.
Users only need to add an extra parameter `func_type="pandas"` in the decorator `udf` or `udaf` to mark it as a vectorized user-defined function.

**NOTE:** Python UDF execution requires Python version (3.6, 3.7 or 3.8) with PyFlink installed. It's required on both the client side and the cluster side. 

## Vectorized Scalar Functions

Vectorized Python scalar functions take `pandas.Series` as the inputs and return a `pandas.Series` of the same length as the output.
Internally, Flink will split the input elements into batches, convert a batch of input elements into `Pandas.Series`
and then call user-defined vectorized Python scalar functions for each batch of input elements. Please refer to the config option
[python.fn-execution.arrow.batch.size]({{< ref "docs/dev/python/python_config" >}}#python-fn-execution-arrow-batch-size) for more details
on how to configure the batch size.

Vectorized Python scalar function could be used in any places where non-vectorized Python scalar functions could be used.

The following example shows how to define your own vectorized Python scalar function which computes the sum of two columns,
and use it in a query:

```python
@udf(result_type=DataTypes.BIGINT(), func_type="pandas")
def add(i, j):
  return i + j

settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
table_env = TableEnvironment.create(settings)

# use the vectorized Python scalar function in Python Table API
my_table.select(add(my_table.bigint, my_table.bigint))

# use the vectorized Python scalar function in SQL API
table_env.create_temporary_function("add", add)
table_env.sql_query("SELECT add(bigint, bigint) FROM MyTable")
```

## Vectorized Aggregate Functions

Vectorized Python aggregate functions takes one or more `pandas.Series` as the inputs and return one scalar value as output.

<span class="label label-info">Note</span> The return type does not support `RowType` and `MapType` for the time being.

Vectorized Python aggregate function could be used in `GroupBy Aggregation`(Batch), `GroupBy Window Aggregation`(Batch and Stream) and 
`Over Window Aggregation`(Batch and Stream bounded over window). For more details on the usage of Aggregations, you can refer
to [the relevant documentation]({{< ref "docs/dev/table/tableApi" >}}?code_tab=python#aggregations).

<span class="label label-info">Note</span> Pandas UDAF does not support partial aggregation. Besides, all the data for a group or window will be loaded into memory at the same time during execution and so you must make sure that the data of a group or window could fit into the memory.

<span class="label label-info">Note</span> Pandas UDAF is only supported in Blink Planner.

The following example shows how to define your own vectorized Python aggregate function which computes mean,
and use it in `GroupBy Aggregation`, `GroupBy Window Aggregation` and `Over Window Aggregation`:

```python
@udaf(result_type=DataTypes.FLOAT(), func_type="pandas")
def mean_udaf(v):
    return v.mean()

settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
table_env = TableEnvironment.create(settings)

my_table = ...  # type: Table, table schema: [a: String, b: BigInt, c: BigInt]

# use the vectorized Python aggregate function in GroupBy Aggregation
my_table.group_by(my_table.a).select(my_table.a, mean_udaf(add(my_table.b)))


# use the vectorized Python aggregate function in GroupBy Window Aggregation
tumble_window = Tumble.over(expr.lit(1).hours) \
            .on(expr.col("rowtime")) \
            .alias("w")

my_table.window(tumble_window) \
    .group_by("w") \
    .select("w.start, w.end, mean_udaf(b)")


# use the vectorized Python aggregate function in Over Window Aggregation
table_env.create_temporary_function("mean_udaf", mean_udaf)
table_env.sql_query("""
    SELECT a,
        mean_udaf(b)
        over (PARTITION BY a ORDER BY rowtime
        ROWS BETWEEN UNBOUNDED preceding AND UNBOUNDED FOLLOWING)
    FROM MyTable""")

```

There are many ways to define a vectorized Python aggregate functions.
The following examples show the different ways to define a vectorized Python aggregate function
which takes two columns of bigint as the inputs and returns the sum of the maximum of them as the result.

```python

# option 1: extending the base class `AggregateFunction`
class MaxAdd(AggregateFunction):

    def open(self, function_context):
        mg = function_context.get_metric_group()
        self.counter = mg.add_group("key", "value").counter("my_counter")
        self.counter_sum = 0

    def get_value(self, accumulator):
        # counter
        self.counter.inc(10)
        self.counter_sum += 10
        return accumulator[0]

    def create_accumulator(self):
        return []

    def accumulate(self, accumulator, *args):
        result = 0
        for arg in args:
            result += arg.max()
        accumulator.append(result)

max_add = udaf(MaxAdd(), result_type=DataTypes.BIGINT(), func_type="pandas")

# option 2: Python function
@udaf(result_type=DataTypes.BIGINT(), func_type="pandas")
def max_add(i, j):
  return i.max() + j.max()

# option 3: lambda function
max_add = udaf(lambda i, j: i.max() + j.max(), result_type=DataTypes.BIGINT(), func_type="pandas")

# option 4: callable function
class CallableMaxAdd(object):
  def __call__(self, i, j):
    return i.max() + j.max()

max_add = udaf(CallableMaxAdd(), result_type=DataTypes.BIGINT(), func_type="pandas")

# option 5: partial function
def partial_max_add(i, j, k):
  return i.max() + j.max() + k
  
max_add = udaf(functools.partial(partial_max_add, k=1), result_type=DataTypes.BIGINT(), func_type="pandas")

```
