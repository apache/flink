---
title: "Intro to the Python DataStream API"
weight: 1
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

# Intro to the Python DataStream API

DataStream programs in Flink are regular programs that implement transformations on data streams
(e.g., filtering, updating state, defining windows, aggregating). The data streams are initially
created from various sources (e.g., message queues, socket streams, files). Results are returned via
sinks, which may for example write the data to files, or to standard output (for example the command
line terminal).

Python DataStream API is a Python version of DataStream API which allows Python users could write
Python DatStream API jobs.

Common Structure of Python DataStream API Programs
--------------------------------------------

The following code example shows the common structure of Python DataStream API programs.

```python
from pyflink.common import WatermarkStrategy, Row
from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FileSink, OutputFileConfig, NumberSequenceSource
from pyflink.datastream.functions import RuntimeContext, MapFunction
from pyflink.datastream.state import ValueStateDescriptor


class MyMapFunction(MapFunction):

    def open(self, runtime_context: RuntimeContext):
        state_desc = ValueStateDescriptor('cnt', Types.PICKLED_BYTE_ARRAY())
        self.cnt_state = runtime_context.get_state(state_desc)

    def map(self, value):
        cnt = self.cnt_state.value()
        if cnt is None or cnt < 2:
            self.cnt_state.update(1 if cnt is None else cnt + 1)
            return value[0], value[1] + 1
        else:
            return value[0], value[1]


def state_access_demo():
    # 1. create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()

    # 2. create source DataStream
    seq_num_source = NumberSequenceSource(1, 10000)
    ds = env.from_source(
        source=seq_num_source,
        watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
        source_name='seq_num_source',
        type_info=Types.LONG())

    # 3. define the execution logic
    ds = ds.map(lambda a: Row(a % 4, 1), output_type=Types.ROW([Types.LONG(), Types.LONG()])) \
           .key_by(lambda a: a[0]) \
           .map(MyMapFunction(), output_type=Types.ROW([Types.LONG(), Types.LONG()]))

    # 4. create sink and emit result to sink
    output_path = '/opt/output/'
    file_sink = FileSink \
        .for_row_format(output_path, Encoder.simple_string_encoder()) \
        .with_output_file_config(OutputFileConfig.builder().with_part_prefix('pre').with_part_suffix('suf').build()) \
        .build()
    ds.sink_to(file_sink)

    # 5. execute the job
    env.execute('state_access_demo')


if __name__ == '__main__':
    state_access_demo()
```

{{< top >}}

Create a StreamExecutionEnvironment
---------------------------

The `StreamExecutionEnvironment` is a central concept of the DataStream API program.
The following code example shows how to create a `StreamExecutionEnvironment`:

```python
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
```

{{< top >}}

Create a DataStream
---------------

The DataStream API gets its name from the special `DataStream` class that is
used to represent a collection of data in a Flink program. You can think of
them as immutable collections of data that can contain duplicates. This data
can either be finite or unbounded, the API that you use to work on them is the
same.

A `DataStream` is similar to a regular Python `Collection` in terms of usage but
is quite different in some key ways. They are immutable, meaning that once they
are created you cannot add or remove elements. You can also not simply inspect
the elements inside but only work on them using the `DataStream` API
operations, which are also called transformations.

You can create an initial `DataStream` by adding a source in a Flink program.
Then you can derive new streams from this and combine them by using API methods
such as `map`, `filter`, and so on.

### Create from a list object

You can create a `DataStream` from a list object:

```python
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
ds = env.from_collection(
    collection=[(1, 'aaa|bb'), (2, 'bb|a'), (3, 'aaa|a')],
    type_info=Types.ROW([Types.INT(), Types.STRING()]))
```

The parameter `type_info` is optional, if not specified, the output type of the returned `DataStream`
will be `Types.PICKLED_BYTE_ARRAY()`.

### Create using DataStream connectors

You can also create a `DataStream` using DataStream connectors with method `add_source` as following:

```python
from pyflink.common.serialization import JsonRowDeserializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer

env = StreamExecutionEnvironment.get_execution_environment()
# the sql connector for kafka is used here as it's a fat jar and could avoid dependency issues
env.add_jars("file:///path/to/flink-sql-connector-kafka.jar")

deserialization_schema = JsonRowDeserializationSchema.builder() \
    .type_info(type_info=Types.ROW([Types.INT(), Types.STRING()])).build()

kafka_consumer = FlinkKafkaConsumer(
    topics='test_source_topic',
    deserialization_schema=deserialization_schema,
    properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})

ds = env.add_source(kafka_consumer)
```

<span class="label label-info">Note</span> It currently only supports `FlinkKafkaConsumer` to be
used as DataStream source connectors with method `add_source`.

<span class="label label-info">Note</span> The `DataStream` created using `add_source` could only
be executed in `streaming` executing mode.

You could also call the `from_source` method to create a `DataStream` using unified DataStream
source connectors:

```python
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import NumberSequenceSource

env = StreamExecutionEnvironment.get_execution_environment()
seq_num_source = NumberSequenceSource(1, 1000)
ds = env.from_source(
    source=seq_num_source,
    watermark_strategy=WatermarkStrategy.for_monotonous_timestamps(),
    source_name='seq_num_source',
    type_info=Types.LONG())
```

<span class="label label-info">Note</span> Currently, it only supports `NumberSequenceSource` and
`FileSource` as unified DataStream source connectors.

<span class="label label-info">Note</span> The `DataStream` created using `from_source` could be
executed in both `batch` and `streaming` executing mode.

### Create using Table & SQL connectors

Table & SQL connectors could also be used to create a `DataStream`. You could firstly create a
`Table` using Table & SQL connectors and then convert it to a `DataStream`.

```python
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

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
```

<span class="label label-info">Note</span> The StreamExecutionEnvironment `env` should be specified
when creating the TableEnvironment `t_env`.

<span class="label label-info">Note</span> As all the Java Table & SQL connectors could be used in
PyFlink Table API, this means that all of them could also be used in PyFlink DataStream API.

{{< top >}}

DataStream Transformations
---------------

Operators transform one or more `DataStream` into a new `DataStream`. Programs can combine multiple
transformations into sophisticated dataflow topologies.

The following example shows a simple example about how to convert a `DataStream` into another
`DataStream` using `map` transformation:

```python
ds = ds.map(lambda a: a + 1)
```

Please see [operators]({{< ref "docs/dev/datastream/operators/overview" >}}) for an overview of the
available DataStream transformations.

Conversion between DataStream and Table
---------------

It also supports to convert a `DataStream` to a `Table` and vice verse.

```python
# convert a DataStream to a Table
table = t_env.from_data_stream(ds, 'a, b, c')

# convert a Table to a DataStream
ds = table.to_append_stream(table, Types.ROW([Types.INT(), Types.STRING()]))
# or
ds = table.to_retract_stream(table, Types.ROW([Types.INT(), Types.STRING()]))
```

{{< top >}}

Emit Results
----------------

### Print

You can call the `print` method to print the data of a `DataStream` to the standard output:

```python
ds.print()
```

### Collect results to client

You can call the `execute_and_collect` method to collect the data of a `DataStream` to client:

```python
with ds.execute_and_collect() as results:
    for result in results:
        print(result)
```

<span class="label label-info">Note</span> The method `execute_and_collect` will collect the data of
the `DataStream` to the memory of the client and so it's a good practice to limit the number of rows
collected.

### Emit results to a DataStream sink connector

You can call the `add_sink` method to emit the data of a `DataStream` to a DataStream sink connector:

```python
from pyflink.common.typeinfo import Types
from pyflink.datastream.connectors import FlinkKafkaProducer
from pyflink.common.serialization import JsonRowSerializationSchema

serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
    type_info=Types.ROW([Types.INT(), Types.STRING()])).build()

kafka_producer = FlinkKafkaProducer(
    topic='test_sink_topic',
    serialization_schema=serialization_schema,
    producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})

ds.add_sink(kafka_producer)
```

<span class="label label-info">Note</span> It currently only supports FlinkKafkaProducer,
JdbcSink and StreamingFileSink to be used as DataStream sink connectors with method `add_sink`.

<span class="label label-info">Note</span> The method `add_sink` could only be used in `streaming`
executing mode.

You could also call the `sink_to` method to emit the data of a `DataStream` to a unified DataStream
sink connector:

```python
from pyflink.datastream.connectors import FileSink, OutputFileConfig
from pyflink.common.serialization import Encoder

output_path = '/opt/output/'
file_sink = FileSink \
    .for_row_format(output_path, Encoder.simple_string_encoder()) \
    .with_output_file_config(OutputFileConfig.builder().with_part_prefix('pre').with_part_suffix('suf').build()) \
    .build()
ds.sink_to(file_sink)
```

<span class="label label-info">Note</span> It currently only supports `FileSink` as unified
DataStream sink connectors.

<span class="label label-info">Note</span> The method `sink_to` could be used in both `batch` and
`streaming` executing mode.

### Emit results to a Table & SQL sink connector

Table & SQL connectors could also be used to write out a `DataStream`. You need firstly convert a
`DataStream` to a `Table` and then write it to a Table & SQL sink connector.

```python
from pyflink.common import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(stream_execution_environment=env)

# option 1：the result type of ds is Types.ROW
def split(s):
    splits = s[1].split("|")
    for sp in splits:
        yield Row(s[0], sp)

ds = ds.map(lambda i: (i[0] + 1, i[1])) \
       .flat_map(split, Types.ROW([Types.INT(), Types.STRING()])) \
       .key_by(lambda i: i[1]) \
       .reduce(lambda i, j: Row(i[0] + j[0], i[1]))

# option 1：the result type of ds is Types.TUPLE
def split(s):
    splits = s[1].split("|")
    for sp in splits:
        yield s[0], sp

ds = ds.map(lambda i: (i[0] + 1, i[1])) \
       .flat_map(split, Types.TUPLE([Types.INT(), Types.STRING()])) \
       .key_by(lambda i: i[1]) \
       .reduce(lambda i, j: (i[0] + j[0], i[1]))

# emit ds to print sink
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
```

<span class="label label-info">Note</span> The output type of DataStream `ds` must be composite type.

Submit Job
----------------

Finally, you should call the `StreamExecutionEnvironment.execute` method to submit the DataStream
API job for execution:

```python
env.execute()
```

If you convert the `DataStream` to a `Table` and then write it to a Table API & SQL sink connector,
it may happen that you need to submit the job using `TableEnvironment.execute` method.

```python
t_env.execute()
```
