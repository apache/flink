---
title: "Row-based Operations"
weight: 31
type: docs
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

# Row-based Operations

This page describes how to use row-based operations in PyFlink Table API.

## Map

Performs a `map` operation with a python [general scalar function]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}#scalar-functions) or [vectorized scalar function]({{< ref "docs/dev/python/table/udfs/vectorized_python_udfs" >}}#vectorized-scalar-functions).
The output will be flattened if the output type is a composite type.

```python
from pyflink.common import Row
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table.udf import udf

env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])

@udf(result_type='ROW<id BIGINT, data STRING>')
def func1(id: int, data: str) -> Row:
    return Row(id, data * 2)

# the input columns are specified as the inputs
table.map(func1(col('id'), col('data'))).execute().print()
# result is 
#+----------------------+--------------------------------+
#|                   id |                           data |
#+----------------------+--------------------------------+
#|                    1 |                           HiHi |
#|                    2 |                     HelloHello |
#+----------------------+--------------------------------+
```

It also supports to take a Row object (containing all the columns of the input table) as input.

```python
@udf(result_type='ROW<id BIGINT, data STRING>')
def func2(data: Row) -> Row:
    return Row(data.id, data.data * 2)

# specify the function without the input columns
table.map(func2).execute().print()
# result is 
#+----------------------+--------------------------------+
#|                   id |                           data |
#+----------------------+--------------------------------+
#|                    1 |                           HiHi |
#|                    2 |                     HelloHello |
#+----------------------+--------------------------------+
```

<span class="label label-info">Note</span> The input columns should not be specified when using func2 in the map operation.

It also supports to use [vectorized scalar function]({{< ref "docs/dev/python/table/udfs/vectorized_python_udfs" >}}#vectorized-scalar-functions) in the `map` operation.
It should be noted that the input type and output type should be pandas.DataFrame instead of Row in this case.

```python
import pandas as pd
@udf(result_type='ROW<id BIGINT, data STRING>', func_type='pandas')
def func3(data: pd.DataFrame) -> pd.DataFrame:
    res = pd.concat([data.id, data.data * 2], axis=1)
    return res

table.map(func3).execute().print()
# result is 
#+----------------------+--------------------------------+
#|                   id |                           data |
#+----------------------+--------------------------------+
#|                    1 |                           HiHi |
#|                    2 |                     HelloHello |
#+----------------------+--------------------------------+
```

## FlatMap

Performs a `flat_map` operation with a python [table function]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}#table-functions).

```python
from pyflink.common import Row
from pyflink.table.udf import udtf
from pyflink.table import EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

table = table_env.from_elements([(1, 'Hi,Flink'), (2, 'Hello')], ['id', 'data'])

@udtf(result_types=['INT', 'STRING'])
def split(x: Row) -> Row:
    for s in x.data.split(","):
        yield x.id, s

# use split in `flat_map`
table.flat_map(split).execute().print()
# result is
#+-------------+--------------------------------+
#|          f0 |                             f1 |
#+-------------+--------------------------------+
#|           1 |                             Hi |
#|           1 |                          Flink |
#|           2 |                          Hello |
#+-------------+--------------------------------+
```

The python [table function]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}#table-functions) could also be used in `join_lateral` and `left_outer_join_lateral`.

```python
# use table function in `join_lateral` or `left_outer_join_lateral`
table.join_lateral(split.alias('a', 'b')).execute().print()
# result is 
#+----------------------+--------------------------------+-------------+--------------------------------+
#|                   id |                           data |           a |                              b |
#+----------------------+--------------------------------+-------------+--------------------------------+
#|                    1 |                       Hi,Flink |           1 |                             Hi |
#|                    1 |                       Hi,Flink |           1 |                          Flink |
#|                    2 |                          Hello |           2 |                          Hello |
#+----------------------+--------------------------------+-------------+--------------------------------+
```

## Aggregate

Performs an `aggregate` operation with a python [general aggregate function]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}#aggregate-functions) or [vectorized aggregate function]({{< ref "docs/dev/python/table/udfs/vectorized_python_udfs" >}}#vectorized-aggregate-functions).

```python
from pyflink.common import Row
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table.udf import AggregateFunction, udaf

class CountAndSumAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        return Row(accumulator[0], accumulator[1])

    def create_accumulator(self):
        return Row(0, 0)

    def accumulate(self, accumulator, row):
        accumulator[0] += 1
        accumulator[1] += row.b

    def retract(self, accumulator, row):
        accumulator[0] -= 1
        accumulator[1] -= row.b

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            accumulator[0] += other_acc[0]
            accumulator[1] += other_acc[1]

    def get_accumulator_type(self):
        return 'ROW<a BIGINT, b BIGINT>'

    def get_result_type(self):
        return 'ROW<a BIGINT, b BIGINT>'

function = CountAndSumAggregateFunction()
agg = udaf(function,
           result_type=function.get_result_type(),
           accumulator_type=function.get_accumulator_type(),
           name=str(function.__class__.__name__))

# aggregate with a python general aggregate function

env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)
t = table_env.from_elements([(1, 2), (2, 1), (1, 3)], ['a', 'b'])

result = t.group_by(col('a')) \
    .aggregate(agg.alias("c", "d")) \
    .select(col('a'), col('c'), col('d'))
result.execute().print()

# the result is
#+----+----------------------+----------------------+----------------------+
#| op |                    a |                    c |                    d |
#+----+----------------------+----------------------+----------------------+
#| +I |                    1 |                    2 |                    5 |
#| +I |                    2 |                    1 |                    1 |
#+----+----------------------+----------------------+----------------------+

# aggregate with a python vectorized aggregate function
env_settings = EnvironmentSettings.in_batch_mode()
table_env = TableEnvironment.create(env_settings)

t = table_env.from_elements([(1, 2), (2, 1), (1, 3)], ['a', 'b'])

pandas_udaf = udaf(lambda pd: (pd.b.mean(), pd.b.max()),
                   result_type='ROW<a FLOAT, b INT>',
                   func_type="pandas")
t.aggregate(pandas_udaf.alias("a", "b")) \
 .select(col('a'), col('b')).execute().print()

# the result is
#+--------------------------------+-------------+
#|                              a |           b |
#+--------------------------------+-------------+
#|                            2.0 |           3 |
#+--------------------------------+-------------+
```

<span class="label label-info">Note</span> Similar to `map` operation, if you specify the aggregate function without the input columns in `aggregate` operation, it will take Row or Pandas.DataFrame as input which contains all the columns of the input table including the grouping keys.
<span class="label label-info">Note</span> You have to close the "aggregate" with a select statement and it should not contain aggregate functions in the select statement.
Besides, the output of aggregate will be flattened if it is a composite type.

## FlatAggregate

Performs a `flat_aggregate` operation with a python general [Table Aggregate Function]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}#table-aggregate-functions)

Similar to `GroupBy Aggregation`, `FlatAggregate` groups the inputs on the grouping keys.
Different from `AggregateFunction`, `TableAggregateFunction` could return 0, 1, or more records for a grouping key.
Similar to `aggregate`, you have to close the `flat_aggregate` with a select statement and the select statement should not contain aggregate functions.

```python
from pyflink.common import Row
from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col
from pyflink.table.udf import udtaf, TableAggregateFunction

class Top2(TableAggregateFunction):

    def emit_value(self, accumulator):
        yield Row(accumulator[0])
        yield Row(accumulator[1])

    def create_accumulator(self):
        return [None, None]

    def accumulate(self, accumulator, row):
        if row.a is not None:
            if accumulator[0] is None or row.a > accumulator[0]:
                accumulator[1] = accumulator[0]
                accumulator[0] = row.a
            elif accumulator[1] is None or row.a > accumulator[1]:
                accumulator[1] = row.a

    def get_accumulator_type(self):
        return 'ARRAY<BIGINT>'

    def get_result_type(self):
        return 'ROW<a BIGINT>'


env_settings = EnvironmentSettings.in_streaming_mode()
table_env = TableEnvironment.create(env_settings)
# the result type and accumulator type can also be specified in the udtaf decorator:
# top2 = udtaf(Top2(), result_type=DataTypes.ROW([DataTypes.FIELD("a", DataTypes.BIGINT())]), accumulator_type=DataTypes.ARRAY(DataTypes.BIGINT()))
top2 = udtaf(Top2())
t = table_env.from_elements([(1, 'Hi', 'Hello'),
                             (3, 'Hi', 'hi'),
                             (5, 'Hi2', 'hi'),
                             (7, 'Hi', 'Hello'),
                             (2, 'Hi', 'Hello')],
                            ['a', 'b', 'c'])

# call function "inline" without registration in Table API
result = t.group_by(col('b')).flat_aggregate(top2).select(col('*')).execute().print()

# the result is:
#+----+--------------------------------+----------------------+
#| op |                              b |                    a |
#+----+--------------------------------+----------------------+
#| +I |                            Hi2 |                    5 |
#| +I |                            Hi2 |               <NULL> |
#| +I |                             Hi |                    7 |
#| +I |                             Hi |                    3 |
#+----+--------------------------------+----------------------+
```
