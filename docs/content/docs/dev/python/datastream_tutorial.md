---
title: "DataStream API Tutorial"
weight: 22
type: docs
aliases:
  - /dev/python/datastream_tutorial.html
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

# DataStream API Tutorial

Apache Flink offers a DataStream API for building robust, stateful streaming applications. It provides fine-grained control over state and time, which allows for the implementation of advanced event-driven systems. In this step-by-step guide, you’ll learn how to build a simple streaming application with PyFlink and the DataStream API.

## What Will You Be Building? 

In this tutorial, you will learn how to write a simple Python DataStream pipeline.
The pipeline will read data from a non-empty collection and write the results to the local file system.

## Prerequisites

This walkthrough assumes that you have some familiarity with Python, but you should be able to follow along even if you come from a different programming language.

## Help, I’m Stuck! 

If you get stuck, check out the [community support resources](https://flink.apache.org/community.html).
In particular, Apache Flink's [user mailing list](https://flink.apache.org/community.html#mailing-lists) consistently ranks as one of the most active of any Apache project and a great way to get help quickly. 

## How To Follow Along

If you want to follow along, you will require a computer with: 

* Java 8 or 11
* Python 3.6, 3.7 or 3.8

Using Python DataStream API requires installing PyFlink, which is available on [PyPI](https://pypi.org/project/apache-flink/) and can be easily installed using `pip`. 

```bash
$ python -m pip install apache-flink
```

Once PyFlink is installed, you can move on to write a Python DataStream job.

## Writing a Flink Python DataStream API Program

DataStream API applications begin by declaring an execution environment (`StreamExecutionEnvironment`), the context in which a streaming program is executed. This is what you will use to set the properties of your job (e.g. default parallelism, restart strategy), create your sources and finally trigger the execution of the job.

```python
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
```

Once a `StreamExecutionEnvironment` is created, you can use it to declare your _source_. Sources ingest data from external systems, such as Apache Kafka, Rabbit MQ, or Apache Pulsar, into Flink Jobs. 

To keep things simple, this walkthrough uses a source that is backed by a collection of elements.

```python
ds = env.from_collection(
    collection=[(1, 'aaa'), (2, 'bbb')],
    type_info=Types.ROW([Types.INT(), Types.STRING()]))
```

This creates a data stream from the given collection, with the same type as that of the elements in it (here, a `ROW` type with a INT field and a STRING field).

You can now perform transformations on this data stream, or just write the data to an external system using a _sink_. This walkthrough uses the `StreamingFileSink` sink connector to write the data into a file in the `/tmp/output` directory.

```python
ds.add_sink(StreamingFileSink
    .for_row_format('/tmp/output', Encoder.simple_string_encoder())
    .build())
```

The last step is to execute the actual PyFlink DataStream API job. PyFlink applications are built lazily and shipped to the cluster for execution only once fully formed. To execute an application, you simply call `env.execute(job_name)`.

```python
env.execute("tutorial_job")
```

The complete code so far:

```python
from pyflink.common.serialization import Encoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import StreamingFileSink


def tutorial():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    ds = env.from_collection(
        collection=[(1, 'aaa'), (2, 'bbb')],
        type_info=Types.ROW([Types.INT(), Types.STRING()]))
    ds.add_sink(StreamingFileSink
                .for_row_format('/tmp/output', Encoder.simple_string_encoder())
                .build())
    env.execute("tutorial_job")


if __name__ == '__main__':
    tutorial()
```

## Executing a Flink Python DataStream API Program

Now that you defined your PyFlink program, you can run it! First, make sure that the output directory doesn't exist:

```bash
rm -rf /tmp/output
```

Next, you can run the example you just created on the command line:

```bash
$ python datastream_tutorial.py
```

The command builds and runs your PyFlink program in a local mini cluster. You can alternatively submit it to a remote cluster using the instructions detailed in [Job Submission Examples]({{< ref "docs/deployment/cli" >}}#submitting-pyflink-jobs).

Finally, you can see the execution result on the command line:

```bash
$ find /tmp/output -type f -exec cat {} \;
1,aaa
2,bbb
```

This walkthrough gives you the foundations to get started writing your own PyFlink DataStream API programs. To learn more about the Python DataStream API, please refer to {{< pythondoc name="Flink Python API Docs">}} for more details.
