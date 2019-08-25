---
title: "Flink DataStream API Programming Guide"
nav-title: Streaming (DataStream API)
nav-id: streaming
nav-parent_id: dev
nav-show_overview: true
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

DataStream programs in Flink are regular programs that implement transformations on data streams
(e.g., filtering, updating state, defining windows, aggregating). The data streams are initially created from various
sources (e.g., message queues, socket streams, files). Results are returned via sinks, which may for
example write the data to files, or to standard output (for example the command line
terminal). Flink programs run in a variety of contexts, standalone, or embedded in other programs.
The execution can happen in a local JVM, or on clusters of many machines.

Please see [basic concepts]({{ site.baseurl }}/dev/api_concepts.html) for an introduction
to the basic concepts of the Flink API.

In order to create your own Flink DataStream program, we encourage you to start with
[anatomy of a Flink Program]({{ site.baseurl }}/dev/api_concepts.html#anatomy-of-a-flink-program)
and gradually add your own
[stream transformations]({{ site.baseurl }}/dev/stream/operators/index.html). The remaining sections act as references for additional
operations and advanced features.


* This will be replaced by the TOC
{:toc}


Example Program
---------------

The following program is a complete, working example of streaming window word count application, that counts the
words coming from a web socket in 5 second windows. You can copy &amp; paste the code to run it locally.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        dataStream.print();

        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowWordCount {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    counts.print()

    env.execute("Window Stream WordCount")
  }
}
{% endhighlight %}
</div>

</div>

To run the example program, start the input stream with netcat first from a terminal:

{% highlight bash %}
nc -lk 9999
{% endhighlight %}

Just type some words hitting return for a new word. These will be the input to the
word count program. If you want to see counts greater than 1, type the same word again and again within
5 seconds (increase the window size from 5 seconds if you cannot type that fast &#9786;).

{% top %}

Data Sources
------------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

Sources are where your program reads its input from. You can attach a source to your program by
using `StreamExecutionEnvironment.addSource(sourceFunction)`. Flink comes with a number of pre-implemented
source functions, but you can always write your own custom sources by implementing the `SourceFunction`
for non-parallel sources, or by implementing the `ParallelSourceFunction` interface or extending the
`RichParallelSourceFunction` for parallel sources.

There are several predefined stream sources accessible from the `StreamExecutionEnvironment`:

File-based:

- `readTextFile(path)` - Reads text files, i.e. files that respect the `TextInputFormat` specification, line-by-line and returns them as Strings.

- `readFile(fileInputFormat, path)` - Reads (once) files as dictated by the specified file input format.

- `readFile(fileInputFormat, path, watchType, interval, pathFilter, typeInfo)` -  This is the method called internally by the two previous ones. It reads files in the `path` based on the given `fileInputFormat`. Depending on the provided `watchType`, this source may periodically monitor (every `interval` ms) the path for new data (`FileProcessingMode.PROCESS_CONTINUOUSLY`), or process once the data currently in the path and exit (`FileProcessingMode.PROCESS_ONCE`). Using the `pathFilter`, the user can further exclude files from being processed.

    *IMPLEMENTATION:*

    Under the hood, Flink splits the file reading process into two sub-tasks, namely *directory monitoring* and *data reading*. Each of these sub-tasks is implemented by a separate entity. Monitoring is implemented by a single, **non-parallel** (parallelism = 1) task, while reading is performed by multiple tasks running in parallel. The parallelism of the latter is equal to the job parallelism. The role of the single monitoring task is to scan the directory (periodically or only once depending on the `watchType`), find the files to be processed, divide them in *splits*, and assign these splits to the downstream readers. The readers are the ones who will read the actual data. Each split is read by only one reader, while a reader can read multiple splits, one-by-one.

    *IMPORTANT NOTES:*

    1. If the `watchType` is set to `FileProcessingMode.PROCESS_CONTINUOUSLY`, when a file is modified, its contents are re-processed entirely. This can break the "exactly-once" semantics, as appending data at the end of a file will lead to **all** its contents being re-processed.

    2. If the `watchType` is set to `FileProcessingMode.PROCESS_ONCE`, the source scans the path **once** and exits, without waiting for the readers to finish reading the file contents. Of course the readers will continue reading until all file contents are read. Closing the source leads to no more checkpoints after that point. This may lead to slower recovery after a node failure, as the job will resume reading from the last checkpoint.

Socket-based:

- `socketTextStream` - Reads from a socket. Elements can be separated by a delimiter.

Collection-based:

- `fromCollection(Collection)` - Creates a data stream from the Java Java.util.Collection. All elements
  in the collection must be of the same type.

- `fromCollection(Iterator, Class)` - Creates a data stream from an iterator. The class specifies the
  data type of the elements returned by the iterator.

- `fromElements(T ...)` - Creates a data stream from the given sequence of objects. All objects must be
  of the same type.

- `fromParallelCollection(SplittableIterator, Class)` - Creates a data stream from an iterator, in
  parallel. The class specifies the data type of the elements returned by the iterator.

- `generateSequence(from, to)` - Generates the sequence of numbers in the given interval, in
  parallel.

Custom:

- `addSource` - Attach a new source function. For example, to read from Apache Kafka you can use
    `addSource(new FlinkKafkaConsumer08<>(...))`. See [connectors]({{ site.baseurl }}/dev/connectors/index.html) for more details.

</div>

<div data-lang="scala" markdown="1">

<br />

Sources are where your program reads its input from. You can attach a source to your program by
using `StreamExecutionEnvironment.addSource(sourceFunction)`. Flink comes with a number of pre-implemented
source functions, but you can always write your own custom sources by implementing the `SourceFunction`
for non-parallel sources, or by implementing the `ParallelSourceFunction` interface or extending the
`RichParallelSourceFunction` for parallel sources.

There are several predefined stream sources accessible from the `StreamExecutionEnvironment`:

File-based:

- `readTextFile(path)` - Reads text files, i.e. files that respect the `TextInputFormat` specification, line-by-line and returns them as Strings.

- `readFile(fileInputFormat, path)` - Reads (once) files as dictated by the specified file input format.

- `readFile(fileInputFormat, path, watchType, interval, pathFilter)` -  This is the method called internally by the two previous ones. It reads files in the `path` based on the given `fileInputFormat`. Depending on the provided `watchType`, this source may periodically monitor (every `interval` ms) the path for new data (`FileProcessingMode.PROCESS_CONTINUOUSLY`), or process once the data currently in the path and exit (`FileProcessingMode.PROCESS_ONCE`). Using the `pathFilter`, the user can further exclude files from being processed.

    *IMPLEMENTATION:*

    Under the hood, Flink splits the file reading process into two sub-tasks, namely *directory monitoring* and *data reading*. Each of these sub-tasks is implemented by a separate entity. Monitoring is implemented by a single, **non-parallel** (parallelism = 1) task, while reading is performed by multiple tasks running in parallel. The parallelism of the latter is equal to the job parallelism. The role of the single monitoring task is to scan the directory (periodically or only once depending on the `watchType`), find the files to be processed, divide them in *splits*, and assign these splits to the downstream readers. The readers are the ones who will read the actual data. Each split is read by only one reader, while a reader can read multiple splits, one-by-one.

    *IMPORTANT NOTES:*

    1. If the `watchType` is set to `FileProcessingMode.PROCESS_CONTINUOUSLY`, when a file is modified, its contents are re-processed entirely. This can break the "exactly-once" semantics, as appending data at the end of a file will lead to **all** its contents being re-processed.

    2. If the `watchType` is set to `FileProcessingMode.PROCESS_ONCE`, the source scans the path **once** and exits, without waiting for the readers to finish reading the file contents. Of course the readers will continue reading until all file contents are read. Closing the source leads to no more checkpoints after that point. This may lead to slower recovery after a node failure, as the job will resume reading from the last checkpoint.

Socket-based:

- `socketTextStream` - Reads from a socket. Elements can be separated by a delimiter.

Collection-based:

- `fromCollection(Seq)` - Creates a data stream from the Java Java.util.Collection. All elements
  in the collection must be of the same type.

- `fromCollection(Iterator)` - Creates a data stream from an iterator. The class specifies the
  data type of the elements returned by the iterator.

- `fromElements(elements: _*)` - Creates a data stream from the given sequence of objects. All objects must be
  of the same type.

- `fromParallelCollection(SplittableIterator)` - Creates a data stream from an iterator, in
  parallel. The class specifies the data type of the elements returned by the iterator.

- `generateSequence(from, to)` - Generates the sequence of numbers in the given interval, in
  parallel.

Custom:

- `addSource` - Attach a new source function. For example, to read from Apache Kafka you can use
    `addSource(new FlinkKafkaConsumer08<>(...))`. See [connectors]({{ site.baseurl }}/dev/connectors/) for more details.

</div>
</div>

{% top %}

DataStream Transformations
--------------------------

Please see [operators]({{ site.baseurl }}/dev/stream/operators/index.html) for an overview of the available stream transformations.

{% top %}

Data Sinks
----------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

Data sinks consume DataStreams and forward them to files, sockets, external systems, or print them.
Flink comes with a variety of built-in output formats that are encapsulated behind operations on the
DataStreams:

- `writeAsText()` / `TextOutputFormat` - Writes elements line-wise as Strings. The Strings are
  obtained by calling the *toString()* method of each element.

- `writeAsCsv(...)` / `CsvOutputFormat` - Writes tuples as comma-separated value files. Row and field
  delimiters are configurable. The value for each field comes from the *toString()* method of the objects.

- `print()` / `printToErr()`  - Prints the *toString()* value
of each element on the standard out / standard error stream. Optionally, a prefix (msg) can be provided which is
prepended to the output. This can help to distinguish between different calls to *print*. If the parallelism is
greater than 1, the output will also be prepended with the identifier of the task which produced the output.

- `writeUsingOutputFormat()` / `FileOutputFormat` - Method and base class for custom file outputs. Supports
  custom object-to-bytes conversion.

- `writeToSocket` - Writes elements to a socket according to a `SerializationSchema`

- `addSink` - Invokes a custom sink function. Flink comes bundled with connectors to other systems (such as
    Apache Kafka) that are implemented as sink functions.

</div>
<div data-lang="scala" markdown="1">

<br />

Data sinks consume DataStreams and forward them to files, sockets, external systems, or print them.
Flink comes with a variety of built-in output formats that are encapsulated behind operations on the
DataStreams:

- `writeAsText()` / `TextOutputFormat` - Writes elements line-wise as Strings. The Strings are
  obtained by calling the *toString()* method of each element.

- `writeAsCsv(...)` / `CsvOutputFormat` - Writes tuples as comma-separated value files. Row and field
  delimiters are configurable. The value for each field comes from the *toString()* method of the objects.

- `print()` / `printToErr()`  - Prints the *toString()* value
of each element on the standard out / standard error stream. Optionally, a prefix (msg) can be provided which is
prepended to the output. This can help to distinguish between different calls to *print*. If the parallelism is
greater than 1, the output will also be prepended with the identifier of the task which produced the output.

- `writeUsingOutputFormat()` / `FileOutputFormat` - Method and base class for custom file outputs. Supports
  custom object-to-bytes conversion.

- `writeToSocket` - Writes elements to a socket according to a `SerializationSchema`

- `addSink` - Invokes a custom sink function. Flink comes bundled with connectors to other systems (such as
    Apache Kafka) that are implemented as sink functions.

</div>
</div>

Note that the `write*()` methods on `DataStream` are mainly intended for debugging purposes.
They are not participating in Flink's checkpointing, this means these functions usually have
at-least-once semantics. The data flushing to the target system depends on the implementation of the
OutputFormat. This means that not all elements send to the OutputFormat are immediately showing up
in the target system. Also, in failure cases, those records might be lost.

For reliable, exactly-once delivery of a stream into a file system, use the `flink-connector-filesystem`.
Also, custom implementations through the `.addSink(...)` method can participate in Flink's checkpointing
for exactly-once semantics.

{% top %}

Iterations
----------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

Iterative streaming programs implement a step function and embed it into an `IterativeStream`. As a DataStream
program may never finish, there is no maximum number of iterations. Instead, you need to specify which part
of the stream is fed back to the iteration and which part is forwarded downstream using a `split` transformation
or a `filter`. Here, we show an example using filters. First, we define an `IterativeStream`

{% highlight java %}
IterativeStream<Integer> iteration = input.iterate();
{% endhighlight %}

Then, we specify the logic that will be executed inside the loop using a series of transformations (here
a simple `map` transformation)

{% highlight java %}
DataStream<Integer> iterationBody = iteration.map(/* this is executed many times */);
{% endhighlight %}

To close an iteration and define the iteration tail, call the `closeWith(feedbackStream)` method of the `IterativeStream`.
The DataStream given to the `closeWith` function will be fed back to the iteration head.
A common pattern is to use a filter to separate the part of the stream that is fed back,
and the part of the stream which is propagated forward. These filters can, e.g., define
the "termination" logic, where an element is allowed to propagate downstream rather
than being fed back.

{% highlight java %}
iteration.closeWith(iterationBody.filter(/* one part of the stream */));
DataStream<Integer> output = iterationBody.filter(/* some other part of the stream */);
{% endhighlight %}

For example, here is program that continuously subtracts 1 from a series of integers until they reach zero:

{% highlight java %}
DataStream<Long> someIntegers = env.generateSequence(0, 1000);

IterativeStream<Long> iteration = someIntegers.iterate();

DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
  @Override
  public Long map(Long value) throws Exception {
    return value - 1 ;
  }
});

DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
  @Override
  public boolean filter(Long value) throws Exception {
    return (value > 0);
  }
});

iteration.closeWith(stillGreaterThanZero);

DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
  @Override
  public boolean filter(Long value) throws Exception {
    return (value <= 0);
  }
});
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">

<br />

Iterative streaming programs implement a step function and embed it into an `IterativeStream`. As a DataStream
program may never finish, there is no maximum number of iterations. Instead, you need to specify which part
of the stream is fed back to the iteration and which part is forwarded downstream using a `split` transformation
or a `filter`. Here, we show an example iteration where the body (the part of the computation that is repeated)
is a simple map transformation, and the elements that are fed back are distinguished by the elements that
are forwarded downstream using filters.

{% highlight scala %}
val iteratedStream = someDataStream.iterate(
  iteration => {
    val iterationBody = iteration.map(/* this is executed many times */)
    (iterationBody.filter(/* one part of the stream */), iterationBody.filter(/* some other part of the stream */))
})
{% endhighlight %}

For example, here is program that continuously subtracts 1 from a series of integers until they reach zero:

{% highlight scala %}
val someIntegers: DataStream[Long] = env.generateSequence(0, 1000)

val iteratedStream = someIntegers.iterate(
  iteration => {
    val minusOne = iteration.map( v => v - 1)
    val stillGreaterThanZero = minusOne.filter (_ > 0)
    val lessThanZero = minusOne.filter(_ <= 0)
    (stillGreaterThanZero, lessThanZero)
  }
)
{% endhighlight %}

</div>
</div>

{% top %}

Execution Parameters
--------------------

The `StreamExecutionEnvironment` contains the `ExecutionConfig` which allows to set job specific configuration values for the runtime.

Please refer to [execution configuration]({{ site.baseurl }}/dev/execution_configuration.html)
for an explanation of most parameters. These parameters pertain specifically to the DataStream API:

- `setAutoWatermarkInterval(long milliseconds)`: Set the interval for automatic watermark emission. You can
    get the current value with `long getAutoWatermarkInterval()`

{% top %}

### Fault Tolerance

[State & Checkpointing]({{ site.baseurl }}/dev/stream/state/checkpointing.html) describes how to enable and configure Flink's checkpointing mechanism.

### Controlling Latency

By default, elements are not transferred on the network one-by-one (which would cause unnecessary network traffic)
but are buffered. The size of the buffers (which are actually transferred between machines) can be set in the Flink config files.
While this method is good for optimizing throughput, it can cause latency issues when the incoming stream is not fast enough.
To control throughput and latency, you can use `env.setBufferTimeout(timeoutMillis)` on the execution environment
(or on individual operators) to set a maximum wait time for the buffers to fill up. After this time, the
buffers are sent automatically even if they are not full. The default value for this timeout is 100 ms.

Usage:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
env.setBufferTimeout(timeoutMillis);

env.generateSequence(1,10).map(new MyMapper()).setBufferTimeout(timeoutMillis);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env: LocalStreamEnvironment = StreamExecutionEnvironment.createLocalEnvironment
env.setBufferTimeout(timeoutMillis)

env.generateSequence(1,10).map(myMap).setBufferTimeout(timeoutMillis)
{% endhighlight %}
</div>
</div>

To maximize throughput, set `setBufferTimeout(-1)` which will remove the timeout and buffers will only be
flushed when they are full. To minimize latency, set the timeout to a value close to 0 (for example 5 or 10 ms).
A buffer timeout of 0 should be avoided, because it can cause severe performance degradation.

{% top %}

Debugging
---------

Before running a streaming program in a distributed cluster, it is a good
idea to make sure that the implemented algorithm works as desired. Hence, implementing data analysis
programs is usually an incremental process of checking results, debugging, and improving.

Flink provides features to significantly ease the development process of data analysis
programs by supporting local debugging from within an IDE, injection of test data, and collection of
result data. This section give some hints how to ease the development of Flink programs.

### Local Execution Environment

A `LocalStreamEnvironment` starts a Flink system within the same JVM process it was created in. If you
start the LocalEnvironment from an IDE, you can set breakpoints in your code and easily debug your
program.

A LocalEnvironment is created and used as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

DataStream<String> lines = env.addSource(/* some source */);
// build your program

env.execute();
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">

{% highlight scala %}
val env = StreamExecutionEnvironment.createLocalEnvironment()

val lines = env.addSource(/* some source */)
// build your program

env.execute()
{% endhighlight %}
</div>
</div>

### Collection Data Sources

Flink provides special data sources which are backed
by Java collections to ease testing. Once a program has been tested, the sources and sinks can be
easily replaced by sources and sinks that read from / write to external systems.

Collection data sources can be used as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

// Create a DataStream from a list of elements
DataStream<Integer> myInts = env.fromElements(1, 2, 3, 4, 5);

// Create a DataStream from any Java collection
List<Tuple2<String, Integer>> data = ...
DataStream<Tuple2<String, Integer>> myTuples = env.fromCollection(data);

// Create a DataStream from an Iterator
Iterator<Long> longIt = ...
DataStream<Long> myLongs = env.fromCollection(longIt, Long.class);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.createLocalEnvironment()

// Create a DataStream from a list of elements
val myInts = env.fromElements(1, 2, 3, 4, 5)

// Create a DataStream from any Collection
val data: Seq[(String, Int)] = ...
val myTuples = env.fromCollection(data)

// Create a DataStream from an Iterator
val longIt: Iterator[Long] = ...
val myLongs = env.fromCollection(longIt)
{% endhighlight %}
</div>
</div>

**Note:** Currently, the collection data source requires that data types and iterators implement
`Serializable`. Furthermore, collection data sources can not be executed in parallel (
parallelism = 1).

### Iterator Data Sink

Flink also provides a sink to collect DataStream results for testing and debugging purposes. It can be used as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.streaming.experimental.DataStreamUtils

DataStream<Tuple2<String, Integer>> myResult = ...
Iterator<Tuple2<String, Integer>> myOutput = DataStreamUtils.collect(myResult)
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">

{% highlight scala %}
import org.apache.flink.streaming.experimental.DataStreamUtils
import scala.collection.JavaConverters.asScalaIteratorConverter

val myResult: DataStream[(String, Int)] = ...
val myOutput: Iterator[(String, Int)] = DataStreamUtils.collect(myResult.javaStream).asScala
{% endhighlight %}
</div>
</div>

{% top %}

**Note:** `flink-streaming-contrib` module is removed from Flink 1.5.0.
Its classes have been moved into `flink-streaming-java` and `flink-streaming-scala`.

Where to go next?
-----------------

* [Operators]({{ site.baseurl }}/dev/stream/operators/index.html): Specification of available streaming operators.
* [Event Time]({{ site.baseurl }}/dev/event_time.html): Introduction to Flink's notion of time.
* [State & Fault Tolerance]({{ site.baseurl }}/dev/stream/state/index.html): Explanation of how to develop stateful applications.
* [Connectors]({{ site.baseurl }}/dev/connectors/index.html): Description of available input and output connectors.

{% top %}
