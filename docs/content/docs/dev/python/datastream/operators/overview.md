---
title: "Overview"
weight: 1
type: docs
aliases:
  - /dev/python/datastream-api-users-guide/operators.html
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

# Operators

Operators transform one or more DataStreams into a new DataStream. Programs can combine multiple transformations into 
sophisticated dataflow topologies.

## DataStream Transformations

DataStream programs in Flink are regular programs that implement transformations on data streams (e.g., mapping, 
filtering, reducing). Please see [operators]({{< ref "docs/dev/datastream/operators/overview" >}})
for an overview of the available transformations in Python DataStream API.

## Functions
Transformations accept user-defined functions as input to define the functionality of the transformations.
The following section describes different ways of defining Python user-defined functions in Python DataStream API.

### Implementing Function Interfaces
Different Function interfaces are provided for different transformations in the Python DataStream API. For example, 
`MapFunction` is provided for the `map` transformation, `FilterFunction` is provided for the `filter` transformation, etc.
Users can implement the corresponding Function interface according to the type of the transformation. Take MapFunction for
instance:

```python
# Implementing MapFunction
class MyMapFunction(MapFunction):
    
    def map(self, value):
        return value + 1
        
data_stream = env.from_collection([1, 2, 3, 4, 5], type_info=Types.INT())
mapped_stream = data_stream.map(MyMapFunction(), output_type=Types.INT())
```

### Lambda Function
As shown in the following example, the transformations can also accept a lambda function to define the functionality of the transformation:

```python
data_stream = env.from_collection([1, 2, 3, 4, 5], type_info=Types.INT())
mapped_stream = data_stream.map(lambda x: x + 1, output_type=Types.INT())
```

<span class="label label-info">Note</span> `ConnectedStream.map()` and `ConnectedStream.flat_map()` do not support
lambda function and must accept `CoMapFunction` and `CoFlatMapFunction` separately.

### Python Function
Users could also use Python function to define the functionality of the transformation:

```python
def my_map_func(value):
    return value + 1

data_stream = env.from_collection([1, 2, 3, 4, 5], type_info=Types.INT())
mapped_stream = data_stream.map(my_map_func, output_type=Types.INT())
```

## Output Type

Users could specify the output type information of the transformation explicitly in Python DataStream API. If not
specified, the output type will be `Types.PICKLED_BYTE_ARRAY` by default, and the result data will be serialized using pickle serializer.
For more details about the pickle serializer, please refer to [Pickle Serialization]({{< ref "docs/dev/python/datastream/data_types" >}}#pickle-serialization).

Generally, the output type needs to be specified in the following scenarios.

### Convert DataStream into Table

```python
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def data_stream_api_demo():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    t_env.execute_sql("""
            CREATE TABLE my_source (
              a INT,
              b VARCHAR
            ) WITH (
              'connector' = 'datagen',
              'number-of-rows' = '10'
            )
        """)

    ds = t_env.to_append_stream(
        t_env.from_path('my_source'),
        Types.ROW([Types.INT(), Types.STRING()]))

    def split(s):
        splits = s[1].split("|")
        for sp in splits:
            yield s[0], sp

    ds = ds.map(lambda i: (i[0] + 1, i[1])) \
           .flat_map(split, Types.TUPLE([Types.INT(), Types.STRING()])) \
           .key_by(lambda i: i[1]) \
           .reduce(lambda i, j: (i[0] + j[0], i[1]))

    t_env.execute_sql("""
            CREATE TABLE my_sink (
              a INT,
              b VARCHAR
            ) WITH (
              'connector' = 'print'
            )
        """)

    table = t_env.from_data_stream(ds)
    table_result = table.execute_insert("my_sink")

    # 1）wait for job finishes and only used in local execution, otherwise, it may happen that the script exits with the job is still running
    # 2）should be removed when submitting the job to a remote cluster such as YARN, standalone, K8s etc in detach mode
    table_result.wait()


if __name__ == '__main__':
    data_stream_api_demo()
```

The output type must be specified for the flat_map operation in the above example which will be used as
the output type of the reduce operation implicitly. The reason is that
`t_env.from_data_stream(ds)` requires the output type of `ds` must be a composite type.

### Write DataStream to Sink

```python
from pyflink.common.typeinfo import Types

def split(s):
    splits = s[1].split("|")
    for sp in splits:
        yield s[0], sp

ds.map(lambda i: (i[0] + 1, i[1]), Types.TUPLE([Types.INT(), Types.STRING()])) \
  .sink_to(...)
```

Generally, the output type needs to be specified for the map operation in the above example if the sink only accepts special kinds of data, e.g. Row, etc.

## Bundling Python Functions

To run Python functions in any non-local mode, it is strongly recommended
bundling your Python functions definitions using the config option [`python-files`]({{< ref "docs/dev/python/python_config" >}}#python-files),
if your Python functions live outside the file where the `main()` function is defined.
Otherwise, you may run into `ModuleNotFoundError: No module named 'my_function'`
if you define Python functions in a file called `my_function.py`.

## Loading resources in Python Functions

There are scenarios when you want to load some resources in Python functions first,
then running computation over and over again, without having to re-load the resources.
For example, you may want to load a large deep learning model only once,
then run batch prediction against the model multiple times.

Overriding the `open` method inherited from the base class `Function` is exactly what you need.

```python
class Predict(MapFunction):
    def open(self, runtime_context: RuntimeContext):
        import pickle

        with open("resources.zip/resources/model.pkl", "rb") as f:
            self.model = pickle.load(f)

    def eval(self, x):
        return self.model.predict(x)
```
