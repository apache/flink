---
title: "Data Types"
nav-parent_id: python_datastream_api
nav-pos: 10
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

In Apache Flink's Python DataStream API, a data type describes the type of a value in the DataStream ecosystem. 
It can be used to declare input and output types of operations and informs the system how to serailize elements. 

* This will be replaced by the TOC
{:toc}


## Pickle Serialization

If the type has not been declared, data would be serialized or deserialized using Pickle. 
For example, the program below specifies no data types.

{% highlight python %}
from pyflink.datastream import StreamExecutionEnvironment


def processing():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.from_collection(collection=[(1, 'aaa'), (2, 'bbb')]) \
        .map(lambda record: (record[0]+1, record[1].upper())) \
        .print()  # note: print to stdout on the worker machine

    env.execute()


if __name__ == '__main__':
    processing()
{% endhighlight %}

However, types need to be specified when:

- Passing Python records to Java operations.
- Improve serialization and deserialization performance.

### Passing Python records to Java operations

Since Java operators or functions can not identify Python data, types need to be provided to help to convert Python types to Java types for processing.
For example, types need to be provided if you want to output data using the StreamingFileSink which is implemented in Java.

{% highlight python %}
from pyflink.common.serialization import SimpleStringEncoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import StreamingFileSink


def streaming_file_sink():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.from_collection(collection=[(1, 'aaa'), (2, 'bbb')]) \
        .map(lambda record: (record[0]+1, record[1].upper()),
             result_type=Types.ROW([Types.INT(), Types.STRING()])) \
        .add_sink(StreamingFileSink
                  .for_row_format('/tmp/output', SimpleStringEncoder())
                  .build())

    env.execute()


if __name__ == '__main__':
    streaming_file_sink()

{% endhighlight %}

### Improve serialization and deserialization performance

Even though data can be serialized and deserialized through Pickle, performance will be better if types are provided.
Explicit types allow PyFlink to use efficient serializers when moving records through the pipeline.

## Supported Data Types

You can use `pyflink.common.typeinfo.Types` to specify types in Python DataStream API. 
The table below shows the type supported now and how to define them:

| PyFlink Type | Usage |  Corresponding Python Type |
|:-----------------|:-----------------------|:-----------------------|
| `BOOLEAN` | `Types.BOOLEAN()` | `bool` |
| `SHORT` | `Types.SHORT()` | `int` |
| `INT` | `Types.INT()` | `int` |
| `LONG` | `Types.LONG()` | `int` |
| `FLOAT` | `Types.FLOAT()` | `float` |
| `DOUBLE` | `Types.DOUBLE()` | `float` |
| `CHAR` | `Types.CHAR()` | `str` |
| `BIG_INT` | `Types.BIG_INT()` | `bytes` |
| `BIG_DEC` | `Types.BIG_DEC()` | `decimal.Decimal` |
| `STRING` | `Types.STRING()` | `str` |
| `BYTE` | `Types.BYTE()` | `int` |
| `TUPLE` | `Types.TUPLE()` | `tuple` |
| `PRIMITIVE_ARRAY` | `Types.PRIMITIVE_ARRAY()` | `list` |
| `ROW` | `Types.ROW()` | `dict` |
