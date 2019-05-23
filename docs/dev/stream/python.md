---
title: "Python Programming Guide (Streaming)"
is_beta: true
nav-title: Python API
nav-parent_id: streaming
nav-pos: 63
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

<span class="label label-danger">Attention</span> *This API is based on Jython,
which is not a full Python replacement and may restrict the libraries you are able
to use with your application (see below for more information).*

Analysis streaming programs in Flink are regular programs that implement transformations on
streaming data sets (e.g., filtering, mapping, joining, grouping). The streaming data sets are initially
created from certain sources (e.g., by reading files, or from collections).
Results are returned via sinks, which may for example write the data to (distributed) files, or to
standard output (for example the command line terminal). Flink streaming programs run in a variety
of contexts, standalone, or embedded in other programs. The execution can happen in a local JVM, or
on clusters of many machines.

In order to create your own Flink streaming program, we encourage you to start with the
[program skeleton](#program-skeleton) and gradually add your own
[transformations](#transformations). The remaining sections act as references for additional
operations and advanced features.

* This will be replaced by the TOC
{:toc}

Jython Framework
---------------
Flink Python streaming API uses Jython framework (see <http://www.jython.org/archive/21/docs/whatis.html>)
to drive the execution of a given script. The Python streaming layer, is actually a thin wrapper layer for the
existing Java streaming APIs.

#### Constraints
There are two main constraints for using Jython:

* The latest Python supported version is 2.7
* It is not straightforward to use Python C extensions, which may prevent use of some libraries

(For more information please see <https://wiki.python.org/jython/JythonFaq/GeneralInfo>.)

One possible alternative for streaming that allows for native Python execution would be the [Apache Beam
portability framework](https://beam.apache.org/contribute/portability/) with the Flink runner.

Streaming Program Example
-------------------------
The following streaming program is a complete, working example of WordCount. You can copy &amp; paste the code
to run it locally (see notes later in this section). It counts the number of each word (case insensitive)
in a stream of sentences, on a window size of 50 milliseconds and prints the results into the standard output.

{% highlight python %}
from org.apache.flink.streaming.api.functions.source import SourceFunction
from org.apache.flink.api.common.functions import FlatMapFunction, ReduceFunction
from org.apache.flink.api.java.functions import KeySelector
from org.apache.flink.streaming.api.windowing.time.Time import milliseconds


class Generator(SourceFunction):
    def __init__(self, num_iters):
        self._running = True
        self._num_iters = num_iters

    def run(self, ctx):
        counter = 0
        while self._running and counter < self._num_iters:
            ctx.collect('Hello World')
            counter += 1

    def cancel(self):
        self._running = False


class Tokenizer(FlatMapFunction):
    def flatMap(self, value, collector):
        for word in value.lower().split():
            collector.collect((1, word))


class Selector(KeySelector):
    def getKey(self, input):
        return input[1]


class Sum(ReduceFunction):
    def reduce(self, input1, input2):
        count1, word1 = input1
        count2, word2 = input2
        return (count1 + count2, word1)

def main(factory):
    env = factory.get_execution_environment()
    env.create_python_source(Generator(num_iters=1000)) \
        .flat_map(Tokenizer()) \
        .key_by(Selector()) \
        .time_window(milliseconds(50)) \
        .reduce(Sum()) \
        .output()
    env.execute()

{% endhighlight %}

**Notes:**

- Execution on a multi-node cluster requires a shared medium storage, which needs to be configured (.e.g HDFS)
  upfront.
- The output from of the given script is directed to the standard output. Consequently, the output
  is written to the corresponding worker `.out` file. If the script is executed inside the IntelliJ IDE,
  then the output will be displayed in the console tab.

{% top %}

Program Skeleton
----------------
As we already saw in the example, Flink streaming programs look like regular Python programs.
Each program consists of the same basic parts:

1. A `main(factory)` function definition, with an environment factory argument - the program entry point,
2. Obtain an `Environment` from the factory,
3. Load/create the initial data,
4. Specify transformations on this data,
5. Specify where to put the results of your computations, and
5. Execute your program.

We will now give an overview of each of those steps but please refer to the respective sections for
more details.

The `main(factory)` function is a must and it is used by Flink execution layer to run the
given Python streaming program.

The `Environment` is the basis for all Flink programs. You can
obtain one using the factory methods provided by the factory:

{% highlight python %}
factory.get_execution_environment()
{% endhighlight %}

For specifying data sources the streaming execution environment has several methods.
To just read a text file as a sequence of lines, you can use:

{% highlight python %}
env = factory.get_execution_environment()
text = env.read_text_file("file:///path/to/file")
{% endhighlight %}

This will give you a DataStream on which you can then apply transformations. For
more information on data sources and input formats, please refer to
[Data Sources](#data-sources).

Once you have a DataStream you can apply transformations to create a new
DataStream which you can then write to a file, transform again, or
combine with other DataStreams. You apply transformations by calling
methods on DataStream with your own custom transformation function. For example,
a map transformation looks like this:

{% highlight python %}
class Doubler(MapFunction):
    def map(self, value):
        return value * 2

data.map(Doubler())
{% endhighlight %}

This will create a new DataStream by doubling every value in the original DataStream.
For more information and a list of all the transformations,
please refer to [Transformations](#transformations).

Once you have a DataStream that needs to be written to disk you can call one
of these methods on DataStream:

{% highlight python %}
data.write_as_text("<file-path>")
data.write_as_text("<file-path>", mode=WriteMode.OVERWRITE)
data.output()
{% endhighlight %}

The last method is only useful for developing/debugging on a local machine,
it will output the contents of the DataSet to standard output. (Note that in
a cluster, the result goes to the standard out stream of the cluster nodes and ends
up in the *.out* files of the workers).
The first two do as the name suggests.
Please refer to [Data Sinks](#data-sinks) for more information on writing to files.

Once you specified the complete program you need to call `execute` on
the `Environment`. This will either execute on your local machine or submit your program
for execution on a cluster, depending on how Flink was started.

{% top %}

Project setup
---------------

Apart from setting up Flink, no additional work is required. Using Jython to execute the Python
script, means that no external packages are needed and the program is executed as if it was a jar file.

The Python API was tested on Windows/Linux/OSX systems.

{% top %}

Lazy Evaluation
---------------

All Flink programs are executed lazily: When the program's main method is executed, the data loading
and transformations do not happen directly. Rather, each operation is created and added to the
program's plan. The operations are actually executed when one of the `execute()` methods is invoked
on the Environment object. Whether the program is executed locally or on a cluster depends
on the environment of the program.

The lazy evaluation lets you construct sophisticated programs that Flink executes as one
holistically planned unit.

{% top %}

Transformations
---------------

Data transformations transform one or more DataStreams into a new DataStream. Programs can combine
multiple transformations into sophisticated assemblies.

This section gives a brief overview of the available transformations. The [transformations
documentation](./operators/index.html) has a full description of all transformations with
examples.

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>

  <tbody>
    <tr>
      <td><strong>Map</strong><br>PythonDataStream &rarr; PythonDataStream</td>
      <td>
        <p>Takes one element and produces one element.</p>
{% highlight python %}
class Doubler(MapFunction):
    def map(self, value):
        return value * 2

data_stream.map(Doubler())
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>FlatMap</strong><br>PythonDataStream &rarr; PythonDataStream</td>
      <td>
        <p>Takes one element and produces zero, one, or more elements. </p>
{% highlight python %}
class Tokenizer(FlatMapFunction):
    def flatMap(self, word, collector):
        collector.collect((1, word))

data_stream.flat_map(Tokenizer())
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Filter</strong><br>PythonDataStream &rarr; PythonDataStream</td>
      <td>
        <p>Evaluates a boolean function for each element and retains those for which the function
        returns true.</p>
{% highlight python %}
class GreaterThen1000(FilterFunction):
    def filter(self, value):
        return value > 1000

data_stream.filter(GreaterThen1000())
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>KeyBy</strong><br>PythonDataStream &rarr; PythonKeyedStream</td>
      <td>
        <p>Logically partitions a stream into disjoint partitions, each partition containing elements of the same key.
        Internally, this is implemented with hash partitioning. See <a href="/dev/api_concepts#specifying-keys">keys</a> on how to specify keys.
        This transformation returns a PythonKeyedDataStream.</p>
    {% highlight python %}
class Selector(KeySelector):
    def getKey(self, input):
        return input[1]  # Key by the second element in a tuple

data_stream.key_by(Selector()) // Key by field "someKey"
    {% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Reduce</strong><br>PythonKeyedStream &rarr; PythonDataStream</td>
      <td>
        <p>A "rolling" reduce on a keyed data stream. Combines the current element with the last reduced value and
                        emits the new value.</p>
{% highlight python %}
class Sum(ReduceFunction):
    def reduce(self, input1, input2):
        count1, val1 = input1
        count2, val2 = input2
        return (count1 + count2, val1)

data.reduce(Sum())
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Window</strong><br>PythonKeyedStream &rarr; PythonWindowedStream</td>
      <td>
        <p>Windows can be defined on already partitioned KeyedStreams. Windows group the data in each
        key according to some characteristic (e.g., the data that arrived within the last 5 seconds).
        See <a href="operators/windows.html">windows</a> for a complete description of windows.
    {% highlight python %}
keyed_stream.count_window(10, 5)  # Last 10 elements, sliding (jumping) by 5 elements

keyed_stream.time_window(milliseconds(30))  # Last 30 milliseconds of data

keted_stream.time_window(milliseconds(100), milliseconds(20))  # Last 100 milliseconds of data, sliding (jumping) by 20 milliseconds
    {% endhighlight %}
        </p>
      </td>
    </tr>

    <tr>
      <td><strong>Window Apply</strong><br>PythonWindowedStream &rarr; PythonDataStream</td>
      <td>
        <p>Applies a general function to the window as a whole. Below is a function that manually sums
           the elements of a window.</p>
    {% highlight python %}
class WindowSum(WindowFunction):
    def apply(self, key, window, values, collector):
        sum = 0
        for value in values:
            sum += value[0]
        collector.collect((key, sum))

windowed_stream.apply(WindowSum())
    {% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Window Reduce</strong><br>PythonWindowedStream &rarr; PythonDataStream</td>
      <td>
        <p>Applies a functional reduce function to the window and returns the reduced value.</p>
    {% highlight python %}
class Sum(ReduceFunction):
    def reduce(self, input1, input2):
        count1, val1 = input1
        count2, val2 = input2
        return (count1 + count2, val1)

windowed_stream.reduce(Sum())
    {% endhighlight %}
      </td>
    </tr>

    <tr>
    <td><strong>Union</strong><br>PythonDataStream* &rarr; PythonDataStream</td>
      <td>
        <p>Union of two or more data streams creating a new stream containing all the elements from all
           the streams. Note: If you union a data stream with itself you will get each element twice
           in the resulting stream.</p>
    {% highlight python %}
data_stream.union(other_stream1, other_stream2, ...);
    {% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Split</strong><br>PythonDataStream &rarr; PythonSplitStream</td>
      <td>
        <p>Split the stream into two or more streams according to some criterion.
    {% highlight python %}
class StreamSelector(OutputSelector):
    def select(self, value):
        return ["even"] if value % 2 == 0 else ["odd"]

splited_stream = data_stream.split(StreamSelector())
    {% endhighlight %}
        </p>
      </td>
    </tr>

    <tr>
      <td><strong>Select</strong><br>SplitStream &rarr; DataStream</td>
      <td>
        <p> Select one or more streams from a split stream.
    {% highlight python %}
even_data_stream = splited_stream.select("even")
odd_data_stream = splited_stream.select("odd")
all_data_stream = splited_stream.select("even", "odd")
    {% endhighlight %}
        </p>
      </td>
    </tr>

    <tr>
      <td><strong>Iterate</strong><br>PythonDataStream &rarr; PythonIterativeStream &rarr; PythonDataStream</td>
      <td>
        <p> Creates a "feedback" loop in the flow, by redirecting the output of one operator
            to some previous operator. This is especially useful for defining algorithms that
            continuously update a model. The following code starts with a stream and applies
            the iteration body continuously. Elements that are greater than 0 are sent back
            to the feedback channel, and the rest of the elements are forwarded downstream.
            See <a href="#iterations">iterations</a> for a complete description.
        {% highlight java %}
class MinusOne(MapFunction):
    def map(self, value):
        return value - 1

class PositiveNumber(FilterFunction):
    def filter(self, value):
        return value > 0

class LessEquelToZero(FilterFunction):
    def filter(self, value):
        return value <= 0

iteration = initial_stream.iterate(5000)
iteration_body = iteration.map(MinusOne())
feedback = iteration_body.filter(PositiveNumber())
iteration.close_with(feedback)
output = iteration_body.filter(LessEquelToZero())
        {% endhighlight %}
        </p>
      </td>
    </tr>

  </tbody>
</table>

{% top %}

Passing Functions to Flink
--------------------------

Certain operations require user-defined functions as arguments. All the functions should be
defined as Python classes that derived from the relevant Flink function. User-defined functions
are serialized and sent over to the TaskManagers for execution.

{% highlight python %}
class Filter(FilterFunction):
    def filter(self, value):
        return value > 5

data_stream.filter(Filter())
{% endhighlight %}

Rich functions (.e.g `RichFilterFunction`) enable to define (override) the optional operations: `open` & `close`.
The user may use these functions for initialization and cleanups.

{% highlight python %}
class Tokenizer(RichMapFunction):
    def open(self, config):
        pass
    def close(self):
        pass
    def map(self, value):
        pass

data_stream.map(Tokenizer())
{% endhighlight %}

The `open` function is called by the worker before starting the streaming pipeline.
The `close` function is called by the worker after the streaming pipeline is stopped.

{% top %}

Data Types
----------

Flink's Python Streaming API offers support for primitive Python types (int, float, bool,
string), as well as byte arrays and user-defined classes.

{% highlight python %}
class Person:
    def __init__(self, name, age):
        self.name = name
        self.age = age

class Tokenizer(MapFunction):
    def map(self, value):
        return (1, Person(*value))

data_stream.map(Tokenizer())
{% endhighlight %}

#### Tuples/Lists

You can use the tuples (or lists) for composite types. Python tuples are mapped to Jython native corresponding
types, which are handled by the Python wrapper thin layer.

{% highlight python %}
word_counts = env.from_elements(("hello", 1), ("world",2))

class Tokenizer(MapFunction):
    def map(self, value):
        return value[1]

counts = word_counts.map(Tokenizer())
{% endhighlight %}

{% top %}

Data Sources
------------

Data sources create the initial data streams, such as from files or from collections.

File-based:

- `read_text_file(path)` - reads files line wise and returns them as a stream of Strings.

Collection-based:

- `from_elements(*args)` - Creates a data stream from all the elements.
- `generate_sequence(from, to)` - Generates the sequence of numbers in the given interval, in parallel.

**Examples**

{% highlight python %}
env  = factory.get_execution_environment()

\# read text file from local files system
localLiens = env.read_text("file:///path/to/my/textfile")

\# read text file from a HDFS running at nnHost:nnPort
hdfsLines = env.read_text("hdfs://nnHost:nnPort/path/to/my/textfile")

\# create a set from some given elements
values = env.from_elements("Foo", "bar", "foobar", "fubar")

\# generate a number sequence
numbers = env.generate_sequence(1, 10000000)
{% endhighlight %}

{% top %}

Data Sinks
----------

Data sinks consume DataStreams and are used to store or return them:

- `write_as_text()` - Writes elements line-wise as Strings. The Strings are
  obtained by calling the *str()* method of each element.
- `output()` - Prints the *str()* value of each element on the
  standard out.
- `write_to_socket()` - Writes the DataStream to a socket [host:port] as a byte array.

A DataStream can be input to multiple operations. Programs can write or print a data stream and at the
same time run additional transformations on them.

**Examples**

Standard data sink methods:

{% highlight scala %}
 write DataStream to a file on the local file system
textData.write_as_text("file:///my/result/on/localFS")

 write DataStream to a file on a HDFS with a namenode running at nnHost:nnPort
textData.write_as_text("hdfs://nnHost:nnPort/my/result/on/localFS")

 write DataStream to a file and overwrite the file if it exists
textData.write_as_text("file:///my/result/on/localFS", WriteMode.OVERWRITE)

 this writes tuples in the text formatting "(a, b, c)", rather than as CSV lines
values.write_as_text("file:///path/to/the/result/file")
{% endhighlight %}

{% top %}

Parallel Execution
------------------

This section describes how the parallel execution of programs can be configured in Flink. A Flink
program consists of multiple tasks (operators, data sources, and sinks). A task is split into
several parallel instances for execution and each parallel instance processes a subset of the task's
input data. The number of parallel instances of a task is called its *parallelism* or *degree of
parallelism (DOP)*.

The degree of parallelism of a task can be specified in Flink on different levels.

### Execution Environment Level

Flink programs are executed in the context of an [execution environment](#program-skeleton). An
execution environment defines a default parallelism for all operators, data sources, and data sinks
it executes. Execution environment parallelism can be overwritten by explicitly configuring the
parallelism of an operator.

The default parallelism of an execution environment can be specified by calling the
`set_parallelism()` method. To execute all operators, data sources, and data sinks of the
[WordCount](#example-program) example program with a parallelism of `3`, set the default parallelism of the
execution environment as follows:

{% highlight python %}
env = factory.get_execution_environment()
env.set_parallelism(3)

text.flat_map(Tokenizer()) \
    .key_by(Selector()) \
    .time_window(milliseconds(30)) \
    .reduce(Sum()) \
    .print()

env.execute()
{% endhighlight %}

### System Level

A system-wide default parallelism for all execution environments can be defined by setting the
`parallelism.default` property in `./conf/flink-conf.yaml`. See the
[Configuration]({{ site.baseurl }}/ops/config.html) documentation for details.

{% top %}

Executing Plans
---------------

To run the plan with Flink, go to your Flink distribution, and run the pyflink-stream.sh script from the /bin
folder. The script containing the plan has to be passed as the first argument, followed by a number of additional
Python packages, and finally, separated by `-`additional arguments that will be fed to the script.

{% highlight python %}
./bin/pyflink-stream.sh <Script>[ <pathToPackage1>[ <pathToPackageX]][ - <param1>[ <paramX>]]
{% endhighlight %}

{% top %}
