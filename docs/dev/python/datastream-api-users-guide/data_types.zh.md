---
title: "数据类型"
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

在 Apache Flink 的 Python DataStream API 中，一种数据类型描述 DataStream 生态系统中数据的类型。
数据类型可用于声明算子输入和输出的类型，并告知系统如何对数据进行序列化。

* This will be replaced by the TOC
{:toc}


## Pickle 序列化

如果类型没有被定义，数据将使用 Pickle 进行序列化和反序列化。 
例如，以下程序没有指定数据类型。

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

但是，在下列情况下需要指定类型:

- 将 Python 数据发送给 Java。
- 提高序列化和反序列化的性能。

### 发送 Python 数据给 Java

由于 Java 算子或函数不能识别 Python 数据，因此需要提供数据类型来将 Python 类型转换为 Java 类型以进行处理。
例如，如果你想要使用 Java 实现的 StreamingFileSink 输出数据，则需要提供数据类型。

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

### 提高序列化和反序列化的性能

尽管可以通过 Pickle 序列化和反序列化数据，但是如果提供了确定的类型，性能会更好。
当在 pipeline 中传递数据时，显式类型允许 PyFlink 使用更高效的序列化器。

## 支持的数据类型

你可以使用 `pyflink.common.typeinfo.Types` 在 Python DataStream API 中指定类型. 
下面列出了现在支持的类型以及如何定义它们:

| PyFlink 类型 | 使用 |  对应 Python 类型 |
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
