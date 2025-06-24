---
title: "Data Types"
weight: 25
type: docs
aliases:
  - /dev/python/datastream-api-users-guide/data_types.html
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

# Data Types

In Apache Flink's Python DataStream API, a data type describes the type of a value in the DataStream ecosystem. 
It can be used to declare input and output types of operations and informs the system how to serialize elements. 

## Pickle Serialization

If the type has not been declared, data would be serialized or deserialized using Pickle. 
For example, the program below specifies no data types.

```python
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
```

However, types need to be specified when:

- Passing Python records to Java operations.
- Improve serialization and deserialization performance.

### Passing Python records to Java operations

Since Java operators or functions can not identify Python data, types need to be provided to help to convert Python types to Java types for processing.
For example, types need to be provided if you want to output data using the FileSink which is implemented in Java.

```python
from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import FileSink


def file_sink():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.from_collection(collection=[(1, 'aaa'), (2, 'bbb')]) \
        .map(lambda record: (record[0]+1, record[1].upper()),
             output_type=Types.ROW([Types.INT(), Types.STRING()])) \
        .add_sink(FileSink
                  .for_row_format('/tmp/output', Encoder.simple_string_encoder())
                  .build())

    env.execute()


if __name__ == '__main__':
    file_sink()

```

### Improve serialization and deserialization performance

Even though data can be serialized and deserialized through Pickle, performance will be better if types are provided.
Explicit types allow PyFlink to use efficient serializers when moving records through the pipeline.

## Supported Data Types

You can use `pyflink.common.typeinfo.Types` to define types in Python DataStream API. 
The table below shows the types supported now and how to define them:

| PyFlink Type |  Python Type |  Java Type |
|:-----------------|:-----------------------|:-----------------------|
|`Types.BOOLEAN()` | `bool` | `java.lang.Boolean` |
|`Types.BYTE()` | `int` | `java.lang.Byte` |
|`Types.SHORT()` | `int` | `java.lang.Short` |
|`Types.INT()` | `int` | `java.lang.Integer` |
|`Types.LONG()` | `int` | `java.lang.Long` |
|`Types.FLOAT()` | `float` | `java.lang.Float` |
|`Types.DOUBLE()` | `float` | `java.lang.Double` |
|`Types.CHAR()` | `str` | `java.lang.Character` |
|`Types.STRING()` | `str` | `java.lang.String` |
|`Types.BIG_INT()` | `int` | `java.math.BigInteger` |
|`Types.BIG_DEC()` | `decimal.Decimal` | `java.math.BigDecimal` |
|`Types.INSTANT()` | `pyflink.common.time.Instant` | `java.time.Instant` |
|`Types.TUPLE()` | `tuple` | `org.apache.flink.api.java.tuple.Tuple0` ~ `org.apache.flink.api.java.tuple.Tuple25` |
|`Types.ROW()` | `pyflink.common.Row` | `org.apache.flink.types.Row` |
|`Types.ROW_NAMED()` | `pyflink.common.Row` | `org.apache.flink.types.Row` |
|`Types.MAP()` | `dict` | `java.util.Map` |
|`Types.PICKLED_BYTE_ARRAY()` | `The actual unpickled Python object` | `byte[]` |
|`Types.SQL_DATE()` | `datetime.date` | `java.sql.Date` |
|`Types.SQL_TIME()` | `datetime.time` | `java.sql.Time` |
|`Types.SQL_TIMESTAMP()` | `datetime.datetime` | `java.sql.Timestamp` |
|`Types.LIST()` | `list of Python object` | `java.util.List` |

The table below shows the array types supported:

| PyFlink Array Type |  Python Type |  Java Type |
|:-----------------|:-----------------------|:-----------------------|
|`Types.PRIMITIVE_ARRAY(Types.BYTE())` | `bytes` | `byte[]` |
|`Types.PRIMITIVE_ARRAY(Types.BOOLEAN())` | `list of bool` | `boolean[]` |
|`Types.PRIMITIVE_ARRAY(Types.SHORT())` | `list of int` | `short[]` |
|`Types.PRIMITIVE_ARRAY(Types.INT())` | `list of int` | `int[]` |
|`Types.PRIMITIVE_ARRAY(Types.LONG())` | `list of int` | `long[]` |
|`Types.PRIMITIVE_ARRAY(Types.FLOAT())` | `list of float` | `float[]` |
|`Types.PRIMITIVE_ARRAY(Types.DOUBLE())` | `list of float` | `double[]` |
|`Types.PRIMITIVE_ARRAY(Types.CHAR())` | `list of str` | `char[]` |
|`Types.BASIC_ARRAY(Types.BYTE())` | `list of int` | `java.lang.Byte[]` |
|`Types.BASIC_ARRAY(Types.BOOLEAN())` | `list of bool` | `java.lang.Boolean[]` |
|`Types.BASIC_ARRAY(Types.SHORT())` | `list of int` | `java.lang.Short[]` |
|`Types.BASIC_ARRAY(Types.INT())` | `list of int` | `java.lang.Integer[]` |
|`Types.BASIC_ARRAY(Types.LONG())` | `list of int` | `java.lang.Long[]` |
|`Types.BASIC_ARRAY(Types.FLOAT())` | `list of float` | `java.lang.Float[]` |
|`Types.BASIC_ARRAY(Types.DOUBLE())` | `list of float` | `java.lang.Double[]` |
|`Types.BASIC_ARRAY(Types.CHAR())` | `list of str` | `java.lang.Character[]` |
|`Types.BASIC_ARRAY(Types.STRING())` | `list of str` | `java.lang.String[]` |
|`Types.OBJECT_ARRAY()` | `list of Python object` | `Array` |
