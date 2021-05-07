---
title: "Python DataStream API 简介"
nav-parent_id: python_datastream_api
nav-pos: 5
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

DataStream programs in Flink are regular programs that implement transformations on data streams
(e.g., filtering, updating state, defining windows, aggregating). The data streams are initially
created from various sources (e.g., message queues, socket streams, files). Results are returned via
sinks, which may for example write the data to files, or to standard output (for example the command
line terminal).

Python DataStream API is a Python version of DataStream API which allows Python users could write
Python DatStream API jobs.

* This will be replaced by the TOC
{:toc}

Common Structure of Python DataStream API Programs
--------------------------------------------

The following code example shows the common structure of Python DataStream API programs.

{% highlight python %}
from pyflink.common import Row
from pyflink.common.serialization import JsonRowDeserializationSchema, JsonRowSerializationSchema
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer


def datastream_api_demo():
    # 1. create a StreamExecutionEnvironment
    env = StreamExecutionEnvironment.get_execution_environment()
    # the sql connector for kafka is used here as it's a fat jar and could avoid dependency issues
    env.add_jars("file:///path/to/flink-sql-connector-kafka.jar")

    # 2. create source DataStream
    deserialization_schema = JsonRowDeserializationSchema.builder() \
        .type_info(type_info=Types.ROW([Types.LONG(), Types.LONG()])).build()

    kafka_source = FlinkKafkaConsumer(
        topics='test_source_topic',
        deserialization_schema=deserialization_schema,
        properties={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})

    ds = env.add_source(kafka_source)

    # 3. define the execution logic
    ds = ds.map(lambda a: Row(a % 4, 1), output_type=Types.ROW([Types.LONG(), Types.LONG()])) \
           .key_by(lambda a: a[0]) \
           .reduce(lambda a, b: Row(a[0], a[1] + b[1]))

    # 4. create sink and emit result to sink
    serialization_schema = JsonRowSerializationSchema.builder().with_type_info(
        type_info=Types.ROW([Types.LONG(), Types.LONG()])).build()
    kafka_sink = FlinkKafkaProducer(
        topic='test_sink_topic',
        serialization_schema=serialization_schema,
        producer_config={'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'})
    ds.add_sink(kafka_sink)

    # 5. execute the job
    env.execute('datastream_api_demo')


if __name__ == '__main__':
    datastream_api_demo()
{% endhighlight %}

{% top %}

Create a StreamExecutionEnvironment
---------------------------

The `StreamExecutionEnvironment` is a central concept of the DataStream API program.
The following code example shows how to create a `StreamExecutionEnvironment`:

{% highlight python %}
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
{% endhighlight %}

{% top %}

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

{% highlight python %}
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment

env = StreamExecutionEnvironment.get_execution_environment()
ds = env.from_collection(
    collection=[(1, 'aaa|bb'), (2, 'bb|a'), (3, 'aaa|a')],
    type_info=Types.ROW([Types.INT(), Types.STRING()]))
{% endhighlight %}

The parameter `type_info` is optional, if not specified, the output type of the returned `DataStream`
will be `Types.PICKLED_BYTE_ARRAY()`.

### Create using DataStream connectors

You can also create a `DataStream` using DataStream connectors with method `add_source` as following:

{% highlight python %}
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
{% endhighlight %}

<span class="label label-info">Note</span> It currently only supports `FlinkKafkaConsumer` to be
used as DataStream source connectors.

### Create using Table & SQL connectors

Table & SQL connectors could also be used to create a `DataStream`. You could firstly create a
`Table` using Table & SQL connectors and then convert it to a `DataStream`.

{% highlight python %}
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
{% endhighlight %}

<span class="label label-info">Note</span> The StreamExecutionEnvironment `env` should be specified
when creating the TableEnvironment `t_env`.

<span class="label label-info">Note</span> As all the Java Table & SQL connectors could be used in
PyFlink Table API, this means that all of them could also be used in PyFlink DataStream API.

{% top %}

DataStream Transformations
---------------

Operators transform one or more `DataStream` into a new `DataStream`. Programs can combine multiple
transformations into sophisticated dataflow topologies.

The following example shows a simple example about how to convert a `DataStream` into another
`DataStream` using `map` transformation:

{% highlight python %}
ds = ds.map(lambda a: a + 1)
{% endhighlight %}

Please see [operators]({% link dev/stream/operators/index.zh.md %}) for an overview of the
available DataStream transformations.

Conversion between DataStream and Table
---------------

It also supports to convert a `DataStream` to a `Table` and vice verse.

{% highlight python %}
# convert a DataStream to a Table
table = t_env.from_data_stream(ds, 'a, b, c')
# convert a Table to a DataStream
ds = table.to_append_stream(table, Types.ROW([Types.INT(), Types.STRING()]))
# or
ds = table.to_retract_stream(table, Types.ROW([Types.INT(), Types.STRING()]))
{% endhighlight %}

{% top %}

Emit Results
----------------

### Print

You can call the `print` method to print the data of a `DataStream` to the standard output:

{% highlight python %}
ds.print()
{% endhighlight %}

### Emit results to a DataStream sink connector

You can call the `add_sink` method to emit the data of a `DataStream` to a DataStream sink connector:

{% highlight python %}
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
{% endhighlight %}

<span class="label label-info">Note</span> It currently only supports FlinkKafkaProducer,
JdbcSink and StreamingFileSink to be used as DataStream sink connectors.

### Emit results to a Table & SQL sink connector

Table & SQL connectors could also be used to write out a `DataStream`. You need firstly convert a
`DataStream` to a `Table` and then write it to a Table & SQL sink connector.

{% highlight python %}
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
{% endhighlight %}

<span class="label label-info">Note</span> The output type of DataStream `ds` must be composite type.

Submit Job
----------------

Finally, you should call the `StreamExecutionEnvironment.execute` method to submit the DataStream
API job for execution:

{% highlight python %}
env.execute()
{% endhighlight %}

If you convert the `DataStream` to a `Table` and then write it to a Table API & SQL sink connector,
it may happen that you need to submit the job using `TableEnvironment.execute` method.

{% highlight python %}
t_env.execute()
{% endhighlight %}
