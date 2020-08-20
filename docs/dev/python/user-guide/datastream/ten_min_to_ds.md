---
title: "10 Minutes to Python DataStream API"
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

This is a short introduction to Python DataStream, geared mainly for new users. You can see more complex recipes under the [Python DataStream API Doc]({% link dev/python/user-guide/datastream/index.md %}).

* This will be replaced by the TOC
{:toc}


## What is Python DataStream API

The DataStream API gets its name from the special `DataStream` class that is used to represent a collection of data in a Flink program. 
You can think of them as immutable collections of data that can contain duplicates. 
This data can either be finite or unbounded, the API that you use to work on them is the same.

Python DataStream API is a Python version of DataStream API with which Python users could write Python DatStream API jobs. 

## How to write a Python DataStream API job

This section guides you how to write a Python DataStream API job from the installation of PyFlink to the submission of a job.

### Installation

Using Python DataStream API requires installing PyFlink, which is available on [PyPI](https://pypi.org/project/apache-flink/) and can be easily installed using `pip`. 
Before installing PyFlink, check the working version of Python running in your system using:

{% highlight bash %}
$ python --version
Python 3.7.6  # the version printed here must be 3.5, 3.6 or 3.7
$ python -m pip install apache-flink
{% endhighlight %}

Once PyFlink is installed, you can move on to write a Python DataStream job.

### Obtain a streaming execution environment.

The `StreamExecutionEnvironment` is the basis for all Flink programs. It is the context in which a streaming program is executed. 
You can create a `StreamExecutionEnvironment` with the `get_execution_environment()` method.

{% highlight python %}
env = StreamExecutionEnvironment.get_execution_environment()
{% endhighlight %}

The `StreamExecutionEnvironment` can be used for setting execution parameters such as restart strategy, default parallelism, etc., for the job.
The [Python Doc](https://ci.apache.org/projects/flink/flink-docs-master/api/python/pyflink.datastream.html#pyflink.datastream.StreamExecutionEnvironment) has listed all the methods that `StreamExecutionEnvironment` supports. 

{% highlight python %}
# set parallelism
env.set_parallelism(10)
# set checkpoint mode
env.enable_checkpointing(300000, CheckpointingMode.AT_LEAST_ONCE)
{% endhighlight %}

### Read data

The streaming execution environment has several methods to read data from external systems. You can just read data from a given non-empty collection,
or using any of the other provided sources. 

{% highlight python %}
# read data from a given non-empty collection
env.from_collection(collection=[(1, 'aaa'), (2, 'bbb')])

# read data from provided sources
props = {'bootstrap.servers': 'localhost:9092', 'group.id': 'test_group'}
env.add_source(FlinkKafkaConsumer('topic', SimpleStringSchema(), props))
{% endhighlight %}

If you want to connect to other external systems, take a look at the connectors documentation for more details.

### Perform transformations on DataStream

The source APIs on `StreamExecutionEnvironment` will give you a DataStream on which you can then apply transformations to create new derived DataStreams.
For example, you can perform a `map()` or a `flat_map()` transformation which looks like:

{% highlight python %}
# read data from a given non-empty collection
ds = env.from_collection(collection=[(1, 'aaa'), (2, 'bbb')])

# perform map
ds = ds.map(lambda record: (record[0]+1, record[1].upper()))
# perform flat_map
ds = ds.flat_map(lambda record: [record[1] for _ in range(record[0])])
{% endhighlight %}

Other operations can be found in the operation documentation page. 

### Sink results

Once you have a DataStream containing your final results, you can write it to an outside system with a sink.
One thing you have to notice is types need to be specified if you want to emit Python data with a sink.
This is because all sinks are implemented in Java. Providing the type helps to convert Python types to Java types for processing.

{% highlight python %}
ds = ds.flat_map(lambda record: [record[1] for _ in range(record[0])], result_type=Types.STRING)
ds.add_sink(StreamingFileSink.for_row_format('/tmp/output', SimpleStringEncoder()).build())
{% endhighlight %}

If you want to connect to other external systems, take a look at the connectors documentation for more details.

### Submit job

Once you specified the complete program you need to trigger the program execution by calling execute() on the StreamExecutionEnvironment. 
This is because all operations, such as creating sources, transformations and sinks are lazy and only when `StreamExecutionEnvironment.execute(job_name)` is called will submit the job.

The `execute()` method will wait for the job to finish and then return a `JobExecutionResult`, this contains execution times and accumulator results.

If you don’t want to wait for the job to finish, you can trigger asynchronous job execution by calling `execute_aysnc()` on the `StreamExecutionEnvironment`. It will return a `JobClient` with which you can communicate with the job you just submitted.

{% highlight python %}
# Triggers the program execution.
env.execute()

# Triggers the program asynchronously.
env.execute_async()
{% endhighlight %}


## Example Program

The following program is a complete, working example of Python DataStream job. You can copy & paste the code to run it locally.

{% highlight python %}
from pyflink.common.serialization import SimpleStringEncoder
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import StreamingFileSink


def python_datastream():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.from_collection(collection=[(1, 'aaa'), (2, 'bbb')]).\
        flat_map(lambda record: [record[1] for _ in range(record[0])], result_type=Types.STRING()).\
        add_sink(StreamingFileSink.for_row_format('/tmp/output', SimpleStringEncoder()).build())
    env.execute()


if __name__ == '__main__':
    python_datastream()

{% endhighlight %}

To run the example program, just save it in a Python file named `python_datastream.py` and run it with Python:

{% highlight bash %}
Python python_datastream.py
{% endhighlight %}

Finally, you can check the results with the following command:

{% highlight bash %}
$ find /tmp/output -type f -exec cat {} \;
aaa
bbb
bbb
{% endhighlight %}
