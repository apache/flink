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

This page describes how to use Row-based Operations in PyFlink Table API.

## Map

Performs a map operation with a python [general scalar function]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}#scalar-functions) or [vectorized scalar function]({{< ref "docs/dev/python/table/udfs/vectorized_python_udfs" >}}#vectorized-scalar-functions).
The output will be flattened if the output type is a composite type.

<span class="label label-info">Note</span> If you do not specify input args of your scalar function, all input args will be merged as a Row or Pandas.DataFrame.
```python
from pyflink.common import Row
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table.types import DataTypes

env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
table_env = TableEnvironment.create(env_settings)

table = table_env.from_elements([(1, 'Hi'), (2, 'Hello')], ['id', 'data'])

# 1. Specify columns
@udf(result_type=DataTypes.ROW([DataTypes.FIELD("id", DataTypes.BIGINT()),
                               DataTypes.FIELD("data", DataTypes.STRING())]))
def func1(id: int, data: str) -> Row:
    return Row(id, data * 2)

table.map(func1(col('id'), col('data'))).to_pandas()
# result is 
#    _c0         _c1
#  0    1        HiHi
#  1    2  HelloHello


# 2. Don't specify columns in general scalar function
@udf(result_type=DataTypes.ROW([DataTypes.FIELD("id", DataTypes.BIGINT()),
                               DataTypes.FIELD("data", DataTypes.STRING())]))
def func2(data: Row) -> Row:
    return Row(data[0], data[1] * 2)

table.map(func2).alias('id', 'data').to_pandas()
# result is 
#      id        data
#  0    1        HiHi
#  1    2  HelloHello

# 3. Don't specify columns in pandas scalar function
import pandas as pd
@udf(result_type=DataTypes.ROW([DataTypes.FIELD("id", DataTypes.BIGINT()),
                               DataTypes.FIELD("data", DataTypes.STRING())]),
                               func_type='pandas')
def func3(data: pd.DataFrame) -> pd.DataFrame:
    res = pd.concat([data.id, data.data * 2], axis=1)
    return res

table.map(func3).alias('id', 'data').to_pandas()
# result is 
#      id        data
#  0    1        HiHi
#  1    2  HelloHello
```

## FlatMap

Performs a `flat_map` operation with a python [table function]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}#table-functions).

```python
from pyflink.common import Row
from pyflink.table.udf import udtf
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env = TableEnvironment.create(env_settings)

table = table_env.from_elements([(1, 'Hi,Flink'), (2, 'Hello')], ['id', 'data'])

@udtf(result_types=[DataTypes.INT(), DataTypes.STRING()])
def split(x: Row) -> Row:
    for s in x[1].split(","):
        yield x[0], s

# use table function split in `flat_map`
table.flat_map(split).to_pandas()
# result is 
#    f0       f1
# 0   1       Hi
# 1   1    Flink
# 2   2    Hello

# use table function in `join_lateral` or `left_outer_join_lateral`
table.join_lateral(split.alias('a', 'b')).to_pandas()
# result is 
#    id      data  a      b
# 0   1  Hi,Flink  1     Hi
# 1   1  Hi,Flink  1  Flink
# 2   2     Hello  2  Hello
```

## Aggregate

Performs an aggregate operation with a python general aggregate function or vectorized aggregate function.
You have to close the "aggregate" with a select statement and the select statement does not support aggregate functions.
The output of aggregate will be flattened if the output type is a composite type.

<span class="label label-info">Note</span> If you do not specify input args of your aggregate function, all input args including group key will be merged as a Row or Pandas.DataFrame.

```python
from pyflink.common import Row
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment
from pyflink.table.expressions import col
from pyflink.table.udf import AggregateFunction, udaf

class CountAndSumAggregateFunction(AggregateFunction):

    def get_value(self, accumulator):
        return Row(accumulator[0], accumulator[1])

    def create_accumulator(self):
        return Row(0, 0)

    def accumulate(self, accumulator, *args):
        accumulator[0] += 1
        accumulator[1] += args[0][1]

    def retract(self, accumulator, *args):
        accumulator[0] -= 1
        accumulator[1] -= args[0][1]

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            accumulator[0] += other_acc[0]
            accumulator[1] += other_acc[1]

    def get_accumulator_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT()),
             DataTypes.FIELD("b", DataTypes.BIGINT())])

    def get_result_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT()),
             DataTypes.FIELD("b", DataTypes.BIGINT())])

function = CountAndSumAggregateFunction()
agg = udaf(function,
           result_type=function.get_result_type(),
           accumulator_type=function.get_accumulator_type(),
           name=str(function.__class__.__name__))

# aggregate with a python general aggregate function

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env = TableEnvironment.create(env_settings)
t = table_env.from_elements([(1, 2), (2, 1), (1, 3)], ['a', 'b'])

result = t.group_by(col('a')) \
    .aggregate(agg.alias("c", "d")) \
    .select(col('a'), col('c'), col('d'))
result.to_pandas()

# the result is
#    a  c  d
# 0  1  2  5
# 1  2  1  1


# aggregate with a python vectorized aggregate function
env_settings = EnvironmentSettings.new_instance().in_batch_mode().use_blink_planner().build()
table_env = TableEnvironment.create(env_settings)

t = table_env.from_elements([(1, 2), (2, 1), (1, 3)], ['a', 'b'])

pandas_udaf = udaf(lambda pd: (pd.b.mean(), pd.b.max()),
                   result_type=DataTypes.ROW(
                       [DataTypes.FIELD("a", DataTypes.FLOAT()),
                        DataTypes.FIELD("b", DataTypes.INT())]),
                   func_type="pandas")
t.aggregate(pandas_udaf.alias("a", "b")) \
    .select(col('a'), col('b')).to_pandas()

# the result is
#      a  b
# 0  2.0  3
```

## FlatAggregate

Performs a flat_aggregate operation with a python general [Table Aggregate Function]({{< ref "docs/dev/python/table/udfs/python_udfs" >}}#table-aggregate-functions)

Similar to a **GroupBy Aggregation**. Groups the rows on the grouping keys with the following running table aggregation operator to aggregate rows group-wise. The difference from an AggregateFunction is that TableAggregateFunction may return 0 or more records for a group. You have to close the "flat_aggregate" with a select statement. And the select statement does not support aggregate functions.

```python
from pyflink.common import Row
from pyflink.table.expressions import col
from pyflink.table.udf import TableAggregateFunction, udtaf
from pyflink.table import DataTypes, EnvironmentSettings, TableEnvironment

class Top2(TableAggregateFunction):

    def emit_value(self, accumulator):
        yield Row(accumulator[0])
        yield Row(accumulator[1])

    def create_accumulator(self):
        return [None, None]

    def accumulate(self, accumulator, *args):
        if args[0][0] is not None:
            if accumulator[0] is None or args[0][0] > accumulator[0]:
                accumulator[1] = accumulator[0]
                accumulator[0] = args[0][0]
            elif accumulator[1] is None or args[0][0] > accumulator[1]:
                accumulator[1] = args[0][0]

    def retract(self, accumulator, *args):
        accumulator[0] = accumulator[0] - 1

    def merge(self, accumulator, accumulators):
        for other_acc in accumulators:
            self.accumulate(accumulator, other_acc[0])
            self.accumulate(accumulator, other_acc[1])

    def get_accumulator_type(self):
        return DataTypes.ARRAY(DataTypes.BIGINT())

    def get_result_type(self):
        return DataTypes.ROW(
            [DataTypes.FIELD("a", DataTypes.BIGINT())])

mytop = udtaf(Top2())

env_settings = EnvironmentSettings.new_instance().in_streaming_mode().use_blink_planner().build()
table_env = TableEnvironment.create(env_settings)
t = table_env.from_elements([(1, 'Hi', 'Hello'),
                              (3, 'Hi', 'hi'),
                              (5, 'Hi2', 'hi'),
                              (7, 'Hi', 'Hello'),
                              (2, 'Hi', 'Hello')], ['a', 'b', 'c'])
result = t.select(col('a'), col('c')) \
    .group_by(col('c')) \
    .flat_aggregate(mytop) \
    .select(col('b')) \
    .flat_aggregate(mytop.alias("b")) \
    .select(col('b'))

result.to_pandas()
# the result is
#    b
# 0  7
# 1  5
```
