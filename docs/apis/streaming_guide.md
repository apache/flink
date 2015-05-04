---
title: "Flink Stream Processing API"
is_beta: true
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

<a href="#top"></a>

Flink Streaming is a system for high-throughput, low-latency data stream processing. The system can connect to and process data streams from different data sources like file sources, web sockets, message queues(Apache Kafka, RabbitMQ, Apache Flume, Twitter…) and also from any user defined data source using a very simple interface. Data streams can be transformed and modified to create new data streams using high-level functions similar to the ones provided by the batch processing API. Flink Streaming natively supports flexible, data-driven windowing semantics and iterative stream processing. The processed data can be pushed to different output types.

* This will be replaced by the TOC
{:toc}

Flink Streaming API
-----------

The Streaming API is currently part of the *flink-staging* Maven project. All relevant classes are located in the *org.apache.flink.streaming* package.

Add the following dependency to your `pom.xml` to use the Flink Streaming.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-core</artifactId>
  <version>{{site.version }}</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala</artifactId>
  <version>{{site.version }}</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}
</div>
</div>

In order to create your own Flink Streaming program we encourage you to start with the [skeleton](#program-skeleton) and gradually add your own [transformations](#transformations). The remaining sections act as references for additional transformations and advanced features.


Example Program
---------------

The following program is a complete, working example of streaming WordCount that incrementally counts the words coming from a web socket. You can copy &amp; paste the code to run it locally.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
public class StreamingWordCount {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .groupBy(0)
                .sum(1);
        
        dataStream.print();
        
        env.execute("Socket Stream WordCount");
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

object WordCount {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.print

    env.execute("Scala Socket Stream WordCount")
  }
}
{% endhighlight %}
</div>

</div>

To run the example program start the input stream with netcat first from a terminal:

~~~bash
nc -lk 9999
~~~

The lines typed to this terminal will be the source data stream for your streaming job.

[Back to top](#top)

Program Skeleton
----------------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

As presented in the [example](#example-program) a Flink Streaming program looks almost identical to a regular Flink program. Each stream processing program consists of the following parts:

1. Obtaining a `StreamExecutionEnvironment`,
2. Connecting to data stream sources,
3. Specifying transformations on the data streams,
4. Specifying output for the processed data,
5. Executing the program.

As these steps are basically the same as in the batch API we will only note the important differences.
For stream processing jobs, the user needs to obtain a `StreamExecutionEnvironment` in contrast with the [batch API](programming_guide.html#program-skeleton) where one would need an `ExecutionEnvironment`. The process otherwise is essentially the same:

{% highlight java %}
StreamExecutionEnvironment.getExecutionEnvironment();
StreamExecutionEnvironment.createLocalEnvironment(parallelism);
StreamExecutionEnvironment.createRemoteEnvironment(String host, int port, int parallelism, String... jarFiles);
{% endhighlight %}

For connecting to data streams the `StreamExecutionEnvironment` has many different methods, from basic file sources to completely general user defined data sources. We will go into details in the [basics](#basics) section.

For example:

{% highlight java %}
env.socketTextStream(host, port);
env.fromElements(elements…);
env.addSource(sourceFunction)
{% endhighlight %}

After defining the data stream sources the user can specify transformations on the data streams to create a new data stream. Different data streams can be also combined together for joint transformations which are being showcased in the [transformations](#transformations) section.

For example:

{% highlight java %}
dataStream.map(mapFunction).reduce(reduceFunction);
{% endhighlight %}

The processed data can be pushed to different outputs called sinks. The user can define their own sinks or use any predefined filesystem, message queue or database sink.

For example:

{% highlight java %}
dataStream.writeAsCsv(path);
dataStream.print();
dataStream.addSink(sinkFunction)
{% endhighlight %}

Once the complete program is specified `execute(programName)` is to be called on the `StreamExecutionEnvironment`. This will either execute on the local machine or submit the program for execution on a cluster, depending on the chosen execution environment.

{% highlight java %}
env.execute(programName);
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">

As presented in the [example](#example-program) a Flink Streaming program looks almost identical to a regular Flink program. Each stream processing program consists of the following parts:

1. Obtaining a `StreamExecutionEnvironment`,
2. Connecting to data stream sources,
3. Specifying transformations on the data streams,
4. Specifying output for the processed data,
5. Executing the program.

As these steps are basically the same as in the batch API we will only note the important differences.
For stream processing jobs, the user needs to obtain a `StreamExecutionEnvironment` in contrast with the [batch API](programming_guide.html#program-skeleton) where one would need an `ExecutionEnvironment`. The process otherwise is essentially the same:

{% highlight scala %}
StreamExecutionEnvironment.getExecutionEnvironment
StreamExecutionEnvironment.createLocalEnvironment(parallelism)
StreamExecutionEnvironment.createRemoteEnvironment(host: String, port: String, parallelism: Int, jarFiles: String*)
{% endhighlight %}

For connecting to data streams the `StreamExecutionEnvironment` has many different methods, from basic file sources to completely general user defined data sources. We will go into details in the [basics](#basics) section.

For example:

{% highlight scala %}
env.socketTextStream(host, port)
env.fromElements(elements…)
env.addSource(sourceFunction)
{% endhighlight %}

After defining the data stream sources the user can specify transformations on the data streams to create a new data stream. Different data streams can be also combined together for joint transformations which are being showcased in the [transformations](#transformations) section.

For example:

{% highlight scala %}
dataStream.map(mapFunction).reduce(reduceFunction)
{% endhighlight %}

The processed data can be pushed to different outputs called sinks. The user can define their own sinks or use any predefined filesystem, message queue or database sink.

For example:

{% highlight scala %}
dataStream.writeAsCsv(path)
dataStream.print
dataStream.addSink(sinkFunction)
{% endhighlight %}

Once the complete program is specified `execute(programName)` is to be called on the `StreamExecutionEnvironment`. This will either execute on the local machine or submit the program for execution on a cluster, depending on the chosen execution environment.

{% highlight scala %}
env.execute(programName)
{% endhighlight %}

</div>

</div>

[Back to top](#top)

Basics
----------------

### DataStream

The `DataStream` is the basic data abstraction provided by the Flink Streaming. It represents a continuous, parallel, immutable stream of data of a certain type. By applying transformations the user can create new data streams or output the results of the computations. For instance the map transformation creates a new `DataStream` by applying a user defined function on each element of a given `DataStream`

The transformations may return different data stream types allowing more elaborate transformations, for example the `groupBy(…)` method returns a `GroupedDataStream` which can be used for grouped transformations such as aggregating by key. We will discover more elaborate data stream types in the upcoming sections.

### Partitioning

Partitioning controls how individual data points of a stream are distributed among the parallel instances of the transformation operators. This also controls the ordering of the records in the `DataStream`. There is partial ordering guarantee for the outputs with respect to the partitioning scheme (outputs produced from each partition are guaranteed to arrive in the order they were produced).

There are several partitioning types supported in Flink Streaming:

 * *Forward(default)*: Forward partitioning directs the output data to the next operator on the same machine (if possible) avoiding expensive network I/O. If there are more processing nodes than inputs or vice verse the load is distributed among the extra nodes in a round-robin fashion. This is the default partitioner.
Usage: `dataStream.forward()`
 * *Shuffle*: Shuffle partitioning randomly partitions the output data stream to the next operator using uniform distribution. Use this only when it is important that the partitioning is randomised. If you only care about an even load use *Distribute*
Usage: `dataStream.shuffle()`
 * *Distribute*: Distribute partitioning directs the output data stream to the next operator in a round-robin fashion, achieving a balanced distribution.
Usage: `dataStream.distribute()`
 * *Field/Key*: Field/Key partitioning partitions the output data stream based on the hash code of a selected key of the tuples. Data points with the same key are directed to the same operator instance. 
Usage: `dataStream.groupBy(fields…)`
 * *Broadcast*: Broadcast partitioning sends the output data stream to all parallel instances of the next operator.
Usage: `dataStream.broadcast()`
 * *Global*: All data points are directed to the first instance of the operator. 
Usage: `dataStream.global()`

By default *Forward* partitioning is used. 

Partitioning does not remain in effect after a transformation, so it needs to be set again for subsequent operations.

### Connecting to the outside world

The user is expected to connect to the outside world through the source and the sink interfaces. 

#### Sources

Sources can by created by using `StreamExecutionEnvironment.addSource(sourceFunction)`. 
Either use one of the source functions that come with Flink or write a custom source
by implementing the `SourceFunction` interface. By default, sources run with
parallelism of 1. To create parallel sources the users source function needs to implement
`ParallelSourceFunction` or extend `RichParallelSourceFunction` in which cases the source will have
the parallelism of the environment. The parallelism for ParallelSourceFunctions can be changed
after creation by using `source.setParallelism(parallelism)`.

The `SourceFunction` interface has two methods: `reachedEnd()` and `next()`. The former is used
by the system to determine whether more input data is available. This method can block if there
is no data available right now but there might come more data in the future. The `next()` method
is called to get next data element. This method will only be called if `reachedEnd()` returns 
false. This method can also block if no data is currently available but more will arrive in the
future. 

The methods must react to thread interrupt calls and break out of blocking calls with
`InterruptedException`. The method may ignore interrupt calls and/or swallow InterruptedExceptions,
if it is guaranteed that the method returns quasi immediately irrespectively of the input.
This is true for example for file streams, where the call is guaranteed to return after a very
short I/O delay in the order of milliseconds.

In addition to the bounded data sources (with similar method signatures as the
[batch API](programming_guide.html#data-sources)) there are several predefined stream sources
accessible from the `StreamExecutionEnvironment`:

* *Socket text stream*: Creates a new `DataStream` that contains the strings received
from the given socket. Strings are decoded by the system's default character set. The user
can optionally set the delimiters or the number of connection retries in case of errors.
Usage: `env.socketTextStream(hostname, port,…)`

* *Text file stream*: Creates a new `DataStream` that contains the lines of the files created
(or modified) in a given directory. The system continuously monitors the given path, and processes
any new files or modifications based on the settings. The file will be read with the system's
default character set.
Usage: `env.readFileStream(String path, long checkFrequencyMillis, WatchType watchType)`

* *Message queue connectors*: There are pre-implemented connectors for a number of popular message
queue services, please refer to the section on [connectors](#stream-connectors) for more details.

* *Custom source*: Creates a new `DataStream` by using a user defined `SourceFunction` implementation.
Usage: `env.addSource(sourceFunction)`

#### Sinks

`DataStreamSink` represents the different outputs of Flink Streaming programs. The user can either define his own `SinkFunction` implementation or chose one of the available implementations (methods of `DataStream`).

For example:

 * `dataStream.print()` – Writes the `DataStream` to the standard output, practical for testing purposes
 * `dataStream.writeAsText(parameters)` – Writes the `DataStream` to a text file
 * `dataStream.writeAsCsv(parameters)` – Writes the `DataStream` to CSV format
 * `dataStream.addSink(sinkFunction)` – Custom sink implementation

There are pre-implemented connectors for a number of the most popular message queue services, please refer to the section on [connectors](#stream-connectors) for more detail.

[Back to top](#top)

Transformations
----------------

Transformations, also called operators, represent the users' business logic on the data stream. Operators consume data streams and produce new data streams. The user can chain and combine multiple operators on the data stream to produce the desired processing steps. Most of the operators work very similar to the batch Flink API allowing developers to reason about `DataStream` the same way as they would about `DataSet`. At the same time there are operators that exploit the streaming nature of the data to allow advanced functionality.

### Basic transformations

Basic transformations can be seen as functions that operate on records of the data stream.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

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
      <td><strong>Map</strong></td>
      <td>
        <p>Takes one element and produces one element. A map that doubles the values of the input stream:</p>
{% highlight java %}
dataStream.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return 2 * value;
            }
        });
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>FlatMap</strong></td>
      <td>
        <p>Takes one element and produces zero, one, or more elements. A flatmap that splits sentences to words:</p>
{% highlight java %}
dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) 
                throws Exception {
                for(String word: value.split(" ")){
                    out.collect(word);
                }
            }
        });
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Filter</strong></td>
      <td>
        <p>Evaluates a boolean function for each element and retains those for which the function returns true.
	<br/>
	<br/>
        A filter that filters out zero values:
        </p>
{% highlight java %}
dataStream.filter(new FilterFunction<Integer>() { 
            @Override
            public boolean filter(Integer value) throws Exception {
                return value != 0;
            }
        });
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Reduce</strong></td>
      <td>
        <p>Combines a stream of elements into another stream by repeatedly combining two elements
        into one and emits the current state after every reduction. Reduce may be applied on a full, windowed or grouped data stream.
        <br/>
        
        <strong>IMPORTANT:</strong> The streaming and the batch reduce functions have different semantics. A streaming reduce on a data stream emits the current reduced value for every new element on a data stream. On a windowed data stream it works as a batch reduce: it produces at most one value per window.
        <br/>
	<br/>
         A reducer that sums up the incoming stream, the result is a stream of intermediate sums:</p>
{% highlight java %}
dataStream.reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer value1, Integer value2) 
            throws Exception {
                return value1 + value2;
            }
        });
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Merge</strong></td>
      <td>
        <p>Merges two or more data streams creating a new stream containing all the elements from all the streams.</p>
{% highlight java %}
dataStream.merge(otherStream1, otherStream2, …)
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>

----------

The following transformations are available on data streams of Tuples:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>Project</strong></td>
      <td>
        <p>Selects a subset of fields from the tuples</p>
{% highlight java %}
DataStream<Tuple3<Integer, Double, String>> in = // [...]
DataStream<Tuple2<String, Integer>> out = in.project(2,0);
{% endhighlight %}
      </td>
    </tr>
  </tbody>
</table>

</div>

<div data-lang="scala" markdown="1">

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
      <td><strong>Map</strong></td>
      <td>
        <p>Takes one element and produces one element. A map that doubles the values of the input stream:</p>
{% highlight scala %}
dataStream.map{ x => x * 2 }
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>FlatMap</strong></td>
      <td>
        <p>Takes one element and produces zero, one, or more elements. A flatmap that splits sentences to words:</p>
{% highlight scala %}
data.flatMap { str => str.split(" ") }
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Filter</strong></td>
      <td>
        <p>Evaluates a boolean function for each element and retains those for which the function returns true.
       	<br/>
	<br/>
        A filter that filters out zero values:
        </p>
{% highlight scala %}
dataStream.filter{ _ != 0 }
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Reduce</strong></td>
        <td>
        <p>Combines a group of elements into a single element by repeatedly combining two elements
        into one and emits the current state after every reduction. Reduce may be applied on a full, windowed or grouped data stream.
        <br/>
        
        <strong>IMPORTANT:</strong> The streaming and the batch reduce functions have different semantics. A streaming reduce on a data stream emits the current reduced value for every new element on a data stream. On a windowed data stream it works as a batch reduce: it produces at most one value per window.
        <br/>
	<br/>
         A reducer that sums up the incoming stream, the result is a stream of intermediate sums:</p>
{% highlight scala %}
dataStream.reduce{ _ + _ }
{% endhighlight %}
      </td>
    </tr>

    <tr>
      <td><strong>Merge</strong></td>
      <td>
        <p>Merges two or more data streams creating a new stream containing all the elements from all the streams.</p>
{% highlight scala %}
dataStream.merge(otherStream1, otherStream2, …)
{% endhighlight %}
      </td>
    </tr>

  </tbody>


</table>

</div>

</div>

### Grouped operators

Some transformations require that the elements of a `DataStream` are grouped on some key. The user can create a `GroupedDataStream` by calling the `groupBy(key)` method of a non-grouped `DataStream`. 
Keys can be of three types: fields positions (applicable for tuple/array types), field expressions (applicable for pojo types), KeySelector instances. 

Aggregation or reduce operators called on `GroupedDataStream`s produce elements on a per group basis.

### Aggregations

The Flink Streaming API supports different types of pre-defined aggregations `DataStreams`. The common property of these operators, just like reduce on streams, they produce the stream of intermediate aggregate values.

Types of aggregations: `sum(field)`, `min(field)`, `max(field)`, `minBy(field, first)`, `maxBy(field, first)`.

With `sum`, `min`, and `max` for every incoming tuple the selected field is replaced with the current aggregated value. Fields can be selected using either field positions or field expressions (similarly to grouping).

With `minBy` and `maxBy` the output of the operator is the element with the current minimal or maximal value at the given field. If more components share the minimum or maximum value, the user can decide if the operator should return the first or last element. This can be set by the `first` boolean parameter.

There is also an option to apply user defined aggregations with the usage of the `aggregate(…)` function of the data stream.

### Window operators

Flink streaming provides very flexible data-driven windowing semantics to create arbitrary windows (also referred to as discretizations or slices) of the data streams and apply reduce, map or aggregation transformations on the windows acquired. Windowing can be used for instance to create rolling aggregations of the most recent N elements, where N could be defined by Time, Count or any arbitrary user defined measure. 

The user can control the size (eviction) of the windows and the frequency of transformation or aggregation calls (trigger) on them in an intuitive API. We will describe the exact semantics of these operators in the [policy based windowing](#policy-based-windowing) section.

Some examples:

 * `dataStream.window(eviction).every(trigger).reduceWindow(…)`
 * `dataStream.window(…).every(…).mapWindow(…).flatten()`
 * `dataStream.window(…).every(…).groupBy(…).aggregate(…).getDiscretizedStream()`

The core abstraction of the Windowing semantics is the `WindowedDataStream` and the `StreamWindow`. The `WindowedDataStream` is created when we first call the `window(…)` method of the DataStream and represents the windowed discretisation of the underlying stream. The user can think about it simply as a `DataStream<StreamWindow<T>>` where additional API functions are supplied to provide efficient transformations of individual windows. 

Please note at this point that the `.every(…)` call belongs together with the preceding `.window(…)` call and does not define a new transformation in itself.

The result of a window transformation is again a `WindowedDataStream` which can also be used to further apply other windowed computations. In this sense, window transformations define mapping from stream windows to stream windows.

The user has different ways of using the a result of a window operation:

 * `windowedDataStream.flatten()` - streams the results element wise and returns a `DataStream<T>` where T is the type of the underlying windowed stream
 * `windowedDataStream.getDiscretizedStream()` - returns a `DataStream<StreamWindow<T>>` for applying some advanced logic on the stream windows itself, be careful here as at this point we need to materialise the full windows
 * Calling any window transformation further transforms the windows, while preserving the windowing logic

The next example would create windows that hold elements of the last 5 seconds, and the user defined transformation would be executed on the windows every second (sliding the window by 1 second):

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
dataStream.window(Time.of(5, TimeUnit.SECONDS)).every(Time.of(1, TimeUnit.SECONDS));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
dataStream.window(Time.of(5, TimeUnit.SECONDS)).every(Time.of(1, TimeUnit.SECONDS))
{% endhighlight %}
</div>
</div>

This approach is often referred to as policy based windowing. Different policies (count, time, etc.) can be mixed as well, for example to downsample our stream, a window that takes the latest 100 elements of the stream every minute is created as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
dataStream.window(Count.of(100)).every(Time.of(1, TimeUnit.MINUTES));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
dataStream.window(Count.of(100)).every(Time.of(1, TimeUnit.MINUTES))
{% endhighlight %}
</div>
</div>

The user can also omit the `every(…)` call which results in a tumbling window emptying the window after every transformation call.

Several predefined policies are provided in the API, including delta-based, count-based and time-based policies. These can be accessed through the static methods provided by the `PolicyHelper` classes:

 * `Time.of(…)`
 * `Count.of(…)`
 * `Delta.of(…)`
 * `FullStream.window()`

For detailed description of these policies please refer to the [Javadocs](http://flink.apache.org/docs/latest/api/java/).

#### Policy based windowing
The policy based windowing is a highly flexible way to specify stream discretisation also called windowing semantics. Two types of policies are used for such a specification:

 * `TriggerPolicy` defines when to trigger the reduce or transformation UDF on the current window and emit the result. In the API it completes a window statement such as: `window(…).every(…)`, while the triggering policy is passed within `every`. In case the user wants to use tumbling eviction policy (the window is emptied after the transformation) he can omit the `.every(…)` call and pass the trigger policy directly to the `.window(…)` call.

 * `EvictionPolicy` defines the length of a window as a means of a predicate for evicting tuples when they are no longer needed. In the API this can be defined by the `window(…)` operation on a stream. There are mostly the same predefined policy types provided as for trigger policies.

Trigger and eviction policies work totally independently of each other. The eviction policy continuously maintains a window, into which it adds new elements and based on the eviction logic removes older elements in the order of arrival. The trigger policy on the other hand only decided at each new incoming element, whether it should trigger computation (and output results) on the currently maintained window.

Several predefined policies are provided in the API, including delta-based, punctuation based, count-based and time-based policies. Policies are in general UDFs and can implement any custom behaviour.

In addition to the `dataStream.window(…).every(…)` style users can specifically pass the trigger and eviction policies during the window call:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
dataStream.window(triggerPolicy, evictionPolicy);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
dataStream.window(triggerPolicy, evictionPolicy)
{% endhighlight %}
</div>

</div>

By default triggers can only trigger when a new element arrives. This might not be suitable for all the use-cases with low data rates . To also provide triggering between elements so called active policies (the two interfaces controlling this special behaviour is `ActiveTriggerPolicy` and `CentralActiveTrigger`) can be used. The predefined time-based policies are already implemented in such a way and can hold as an example for user defined active policy implementations. 

Time-based trigger and eviction policies can work with user defined `TimeStamp` implementations, these policies already cover most use cases.
 
#### Reduce on windowed data streams
The `WindowedDataStream<T>.reduceWindow(ReduceFunction<T>)` transformation calls the user-defined `ReduceFunction` at every trigger on the records currently in the window. The user can also use the different pre-implemented streaming aggregations such as `sum, min, max, minBy` and `maxBy`.

The following is an example for a window reduce that sums the elements in the last minute with 10 seconds slide interval:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
dataStream.window(Time.of(1, TimeUnit.MINUTES)).every(Time.of(10,TimeUnit.SECONDS)).sum(field);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
dataStream.window(Time.of(1, TimeUnit.MINUTES)).every(Time.of(10,TimeUnit.SECONDS)).sum(field)
{% endhighlight %}
</div>

</div>


#### Map on windowed data streams
The `WindowedDataStream<T>.mapWindow(WindowMapFunction<T,O>)` transformation calls  `mapWindow(…)` for each `StreamWindow` in the discretised stream providing access to all elements in the window through the iterable interface. At each function call the output `StreamWindow<O>` will consist of all the elements collected to the collector. This allows a straightforward way of mapping one stream window to another.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
windowedDataStream.mapWindow(windowMapFunction);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
windowedDataStream.mapWindow(windowMapFunction)
{% endhighlight %}
</div>

</div>

#### Grouped transformations on windowed data streams
Calling the `groupBy(…)` method on a windowed stream groups the elements by the given fields inside the stream windows. The window sizes (evictions) and slide sizes (triggers) will be calculated on the whole stream (in a global fashion), but the user defined functions will be applied on a per group basis inside the window. This means that for a call `windowedStream.groupBy(…).reduceWindow(…)` will transform each window into another window consisting of as many elements as keys in the original window, with the reduced values per key. Similarly the `mapWindow` transformation is applied per group as well.

The user can also create discretisation on a per group basis calling `window(…).every(…)` on an already grouped data stream. This will apply the discretisation logic independently for each key.

To highlight the differences let us look at two examples.

To get the maximal value for each key on the last 100 elements (global) we use the first approach:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
dataStream.window(Count.of(100)).every(…).groupBy(groupingField).max(field);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
dataStream.window(Count.of(100)).every(…).groupBy(groupingField).max(field)
{% endhighlight %}
</div>

</div>

Using this approach we took the last 100 elements, divided it into groups by key then applied the aggregation. To create fixed size windows for every key we need to reverse the order of the groupBy call. So to take the max for the last 100 elements in Each group:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
dataStream.groupBy(groupingField).window(Count.of(100)).every(…).max(field);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
dataStream.groupBy(groupingField).window(Count.of(100)).every(…).max(field)
{% endhighlight %}
</div>

</div>

This will create separate windows for different keys and apply the trigger and eviction policies on a per group basis.

#### Applying multiple transformations on a window
Using the `WindowedDataStream` abstraction we can apply several transformations one after another on the discretised streams without having to re-discretise it:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
dataStream.window(Count.of(1000)).groupBy(firstKey).mapWindow(…)
    .groupBy(secondKey).reduceWindow(…).flatten();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
dataStream.window(Count.of(1000)).groupBy(firstKey).mapWindow(…)
    .groupBy(secondKey).reduceWindow(…).flatten()
{% endhighlight %}
</div>
</div>

The above call would create global windows of 1000 elements group it by the first key and then apply a mapWindow transformation. The resulting windowed stream will then be grouped by the second key and further reduced. The results of the reduce transformation are then flattened.

Notice that here we only defined the window size once at the beginning of the transformation. This means that anything that happens afterwards (`groupBy(firstKey).mapWindow(…).groupBy(secondKey).reduceWindow(…)`) happens inside the 1000 element windows. Of course the mapWindow might reduce the number of elements but the key idea is that each transformation still corresponds to the same 1000 elements in the original stream.

#### Periodic aggregations on the full stream history
Sometimes it is necessary to aggregate over all the previously seen data in the stream. For this purpose either use the `dataStream.window(FullStream.window()).every(trigger)` or equivalently `dataStream.every(trigger)`. 

#### Global vs local discretisation
By default all window discretisation calls (`dataStream.window(…)`) define global windows meaning that a global window of count 100 will contain the last 100 elements arrived at the discretisation operator in order. In most cases (except for Time) this means that the operator doing the actual discretisation needs to have a parallelism of 1 to be able to correctly execute the discretisation logic.

Sometimes it is sufficient to create local discretisations, which allows the discretiser to run in parallel and apply the given discretisation logic at every discretiser instance. To allow local discretisation use the `local()` method of the windowed data stream.

For example `dataStream.window(Count.of(100)).maxBy(field)` would create global windows of 100 elements (Count discretises with parallelism of 1) and return the record with the max value by the selected field, alternatively the `dataStream.window(Count.of(100)).local().maxBy(field)` would create several count discretisers (as defined by the environment parallelism) and compute the max values accordingly.


### Temporal database style operators

While database style operators like joins (on key) and crosses are hard to define properly on data streams, a straightforward interpretation is to apply these operators on windows of the data streams. 

Currently join and cross operators are supported only on time windows. We are working on alleviating this limitations in the next release.

Temporal operators take the current windows of both streams and apply the join/cross logic on these window pairs.

The Join transformation produces a new Tuple DataStream with two fields. Each tuple holds a joined element of the first input DataStream in the first tuple field and a matching element of the second input DataStream in the second field for the current window. The user can also supply a custom join function to control the produced elements.

The following code shows a default Join transformation using field position keys:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
dataStream1.join(dataStream2)
    .onWindow(windowing_params)
    .where(key_in_first)
    .equalTo(key_in_second);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
dataStream1.join(dataStream2)
    .onWindow(windowing_params)
    .where(key_in_first)
    .equalTo(key_in_second)
{% endhighlight %}
</div>
</div>

The Cross transformation combines two `DataStream`s into one `DataStream`. It builds all pairwise combinations of the elements of both input DataStreams in the current window, i.e., it builds a temporal Cartesian product. The user can also supply a custom cross function to control the produced elements

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
dataStream1.cross(dataStream2).onWindow(windowing_params);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
dataStream1 cross dataStream2 onWindow (windowing_params)
{% endhighlight %}
</div>
</div>


### Co operators

Co operators allow the users to jointly transform two `DataStream`s of different types providing a simple way to jointly manipulate streams with a shared state. It is designed to support joint stream transformations where merging is not appropriate due to different data types or in case the user needs explicit tracking of the origin of individual elements.
Co operators can be applied to `ConnectedDataStream`s which represent two `DataStream`s of possibly different types. A `ConnectedDataStream` can be created by calling the `connect(otherDataStream)` method of a `DataStream`. Please note that the two connected `DataStream`s can also be merged data streams.

#### Map on ConnectedDataStream
Applies a CoMap transformation on two separate DataStreams, mapping them to a common output type. The transformation calls a `CoMapFunction.map1()` for each element of the first input and `CoMapFunction.map2()` for each element of the second input. Each CoMapFunction call returns exactly one element.
A CoMap operator that outputs true if an Integer value is received and false if a String value is received:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Integer> dataStream1 = ...
DataStream<String> dataStream2 = ...
        
dataStream1.connect(dataStream2)
    .map(new CoMapFunction<Integer, String, Boolean>() {
            
            @Override
            public Boolean map1(Integer value) {
                return true;
            }
            
            @Override
            public Boolean map2(String value) {
                return false;
            }
        })
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val dataStream1 : DataStream[Int] = ...
val dataStream2 : DataStream[String] = ...

(dataStream1 connect dataStream2)
  .map(
    (_ : Int) => true,
    (_ : String) => false
  )
{% endhighlight %}
</div>
</div>

#### FlatMap on ConnectedDataStream
The FlatMap operator for the `ConnectedDataStream` works similarly to CoMap, but instead of returning exactly one element after each map call the user can output arbitrarily many values using the Collector interface. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Integer> dataStream1 = ...
DataStream<String> dataStream2 = ...
        
dataStream1.connect(dataStream2)
    .flatMap(new CoFlatMapFunction<Integer, String, String>() {

            @Override
            public void flatMap1(Integer value, Collector<String> out) {
                out.collect(value.toString());
            }

            @Override
            public void flatMap2(String value, Collector<String> out) {
                for (String word: value.split(" ")) {
                  out.collect(word);
                }
            }
        })
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val dataStream1 : DataStream[Int] = ...
val dataStream2 : DataStream[String] = ...

(dataStream1 connect dataStream2)
  .flatMap(
    (num : Int) => List(num.toString),
    (str : String) => str.split(" ")
  )
{% endhighlight %}
</div>
</div>

#### WindowReduce on ConnectedDataStream
The windowReduce operator applies a user defined `CoWindowFunction` to time aligned windows of the two data streams and return zero or more elements of an arbitrary type. The user can define the window and slide intervals and can also implement custom timestamps to be used for calculating windows.

#### Reduce on ConnectedDataStream
The Reduce operator for the `ConnectedDataStream` applies a simple reduce transformation on the joined data streams and then maps the reduced elements to a common output type.

### Output splitting
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

Most data stream operators support directed outputs (output splitting), meaning that different output elements are sent only to specific outputs. The outputs are referenced by their name given at the point of receiving:

{% highlight java %}
SplitDataStream<Integer> split = someDataStream.split(outputSelector);
DataStream<Integer> even = split.select("even");
DataStream<Integer> odd = split.select("odd");
{% endhighlight %}
In the above example the data stream named “even” will only contain elements that are directed to the output named “even”. The user can of course further transform these new stream by for example squaring only the even elements.

Data streams only receive the elements directed to selected output names. The user can also select multiple output names by `splitStream.select(“output1”, “output2”, …)`. It is common that a stream listens to all the outputs, so `split.selectAll()` provides this functionality without having to select all names.

The outputs of an operator are directed by implementing a selector function (implementing the `OutputSelector` interface):

{% highlight java %}
Iterable<String> select(OUT value);
{% endhighlight %}

The data is sent to all the outputs returned in the iterable (referenced by their name). This way the direction of the outputs can be determined by the value of the data sent. 

For example to split even and odd numbers:

{% highlight java %}
@Override
Iterable<String> select(Integer value) {

    List<String> outputs = new ArrayList<String>();

    if (value % 2 == 0) {
        outputs.add("even");
    } else {
        outputs.add("odd");
    }

    return outputs;
}
{% endhighlight %}

Every output will be emitted to the selected outputs exactly once, even if you add the same output names more than once.

The functionality provided by output splitting can also be achieved efficiently (due to operator chaining) by multiple filter operators.
</div>
<div data-lang="scala" markdown="1">

Most data stream operators support directed outputs (output splitting), meaning that different output elements are sent only to specific outputs. The outputs are referenced by their name given at the point of receiving:

{% highlight scala %}
val split = someDataStream.split(
  (num: Int) =>
    (num % 2) match {
      case 0 => List("even")
      case 1 => List("odd")
    }
)

val even = split select "even" 
val odd = split select "odd"
{% endhighlight %}

In the above example the data stream named “even” will only contain elements that are directed to the output named “even”. The user can of course further transform these new stream by for example squaring only the even elements.

Data streams only receive the elements directed to selected output names. The user can also select multiple output names by `splitStream.select(“output1”, “output2”, …)`. It is common that a stream listens to all the outputs, so `split.selectAll` provides this functionality without having to select all names.

The outputs of an operator are directed by implementing a function that returns the output names for the value. The data is sent to all the outputs returned by the function (referenced by their name). This way the direction of the outputs can be determined by the value of the data sent.

Every output will be emitted to the selected outputs exactly once, even if you add the same output names more than once.

The functionality provided by output splitting can also be achieved efficiently (due to operator chaining) by multiple filter operators.
</div>

</div>

### Iterations
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
The Flink Streaming API supports implementing iterative stream processing dataflows similarly to the batch Flink API. Iterative streaming programs also implement a step function and embed it into an `IterativeDataStream`.
Unlike in the batch API the user does not define the maximum number of iterations, but at the tail of each iteration part of the output is streamed forward to the next operator and part is streamed back to the iteration head. The user controls the output of the iteration tail using [output splitting](#output-splitting) or [filters](#filter).
To start an iterative part of the program the user defines the iteration starting point:

{% highlight java %}
IterativeDataStream<Integer> iteration = source.iterate(maxWaitTimeMillis);
{% endhighlight %}

The operator applied on the iteration starting point is the head of the iteration, where data is fed back from the iteration tail.

{% highlight java %}
DataStream<Integer> head = iteration.map(new IterationHead());
{% endhighlight %}

To close an iteration and define the iteration tail, the user calls `closeWith(iterationTail)` method of the `IterativeDataStream`. This iteration tail (the DataStream given to the `closeWith` function) will be fed back to the iteration head. A common pattern is to use [filters](#filter) to separate the output of the iteration from the feedback-stream.

{% highlight java %}
DataStream<Integer> tail = head.map(new IterationTail());

iteration.closeWith(tail.filter(isFeedback));

DataStream<Integer> output = tail.filter(isOutput);

output.map(…).project(…);
{% endhighlight %}

In this case all values passing the `isFeedback` filter will be fed back to the iteration head, and the values passing the `isOutput` filter will produce the output of the iteration that can be transformed further (here with a `map` and a `projection`) outside the iteration.

Because iterative streaming programs do not have a set number of iterations for each data element, the streaming program has no information on the end of its input. As a consequence iterative streaming programs run until the user manually stops the program. While this is acceptable under normal circumstances a method is provided to allow iterative programs to shut down automatically if no input received by the iteration head for a predefined number of milliseconds.
To use this functionality the user needs to add the maxWaitTimeMillis parameter to the `dataStream.iterate(…)` call to control the max wait time. 
</div>
<div data-lang="scala" markdown="1">
The Flink Streaming API supports implementing iterative stream processing dataflows similarly to the batch Flink API. Iterative streaming programs also implement a step function and embed it into an `IterativeDataStream`.
Unlike in the batch API the user does not define the maximum number of iterations, but at the tail of each iteration part of the output is streamed forward to the next operator and part is streamed back to the iteration head. The user controls the output of the iteration tail by defining a step function that return two DataStreams: a feedback and an output. The first one is the output that will be fed back to the start of the iteration and the second is the output stream of the iterative part.

A common pattern is to use [filters](#filter) to separate the output from the feedback-stream. In this case all values passing the `isFeedback` filter will be fed back to the iteration head, and the values passing the `isOutput` filter will produce the output of the iteration that can be transformed further (here with a `map` and a `projection`) outside the iteration.

{% highlight scala %}
val iteratedStream = someDataStream.iterate(maxWaitTime) {
  iteration => {
    val head = iteration.map(iterationHead)
    val tail = head.map(iterationTail)
    (tail.filter(isFeedback), tail.filter(isOutput))
  }
}.map(…).project(…)
{% endhighlight %}

Because iterative streaming programs do not have a set number of iterations for each data element, the streaming program has no information on the end of its input. As a consequence iterative streaming programs run until the user manually stops the program. While this is acceptable under normal circumstances a method is provided to allow iterative programs to shut down automatically if no input received by the iteration head for a predefined number of milliseconds.
To use this functionality the user needs to add the maxWaitTimeMillis parameter to the `dataStream.iterate(…)` call to control the max wait time. 
</div>

</div>

### Rich functions
The [usage](programming_guide.html#rich-functions) of rich functions are essentially the same as in the batch Flink API. All transformations that take as argument a user-defined function can instead take a rich function as argument:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
dataStream.map(new RichMapFunction<Integer, String>() {
    @Override
    public void open(Configuration parameters) throws Exception {
        /* initialization of function */
    }

    @Override
    public String map(Integer value) { return value.toString(); }
});
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
dataStream map
  new RichMapFunction[Int, String] {
    override def open(config: Configuration) = {
      /* initialization of function */
    }
    override def map(value: Int): String = value.toString
  }
{% endhighlight %}
</div>
</div>

Rich functions provide, in addition to the user-defined function (`map()`, `reduce()`, etc), the `open()` and `close()` methods for initialization and finalization.

[Back to top](#top)

Lambda expressions with Java 8
------------

For a more consise code one can rely on one of the main feature of Java 8, lambda expressions. The following program has similar functionality to the one provided in the [example](#example-program) section, while showcasing the usage of lambda expressions.

<div class="codetabs" markdown="1">
<div data-lang="java8" markdown="1">
{% highlight java %}
public class StreamingWordCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.fromElements(
                "Who's there?",
                "I think I hear them. Stand, ho! Who's there?");

            DataStream<Tuple2<String, Integer>> counts = 
        // normalize and split each line
        text.map(line -> line.toLowerCase().split("\\W+"))
        // convert splitted line in pairs (2-tuples) containing: (word,1)
        .flatMap((String[] tokens, Collector<Tuple2<String, Integer>> out) -> {
        // emit the pairs with non-zero-length words
            Arrays.stream(tokens)
                .filter(t -> t.length() > 0)
                .forEach(t -> out.collect(new Tuple2<>(t, 1)));
        })
        // group by the tuple field "0" and sum up tuple field "1"
        .groupBy(0)
        .sum(1);

        counts.print();

        env.execute("Streaming WordCount");
    }
}
{% endhighlight %}
</div>
</div>

For a detailed Java 8 Guide please refer to the [Java 8 Programming Guide](java8_programming_guide.html). Operators specific to streaming, such as Operator splitting also support this usage. [Output splitting](#output-splitting) can be rewritten as follows:

<div class="codetabs" markdown="1">
<div data-lang="java8" markdown="1">
{% highlight java %}
SplitDataStream<Integer> split = someDataStream
    .split(x -> Arrays.asList(String.valueOf(x % 2)));
{% endhighlight %}
</div>
</div>

Operator Settings
----------------

### Parallelism

Setting parallelism for operators works exactly the same way as in the batch Flink API. The user can control the number of parallel instances created for each operator by calling the `operator.setParallelism(parallelism)` method.

### Buffer timeout

By default data points are not transferred on the network one-by-one, which would cause unnecessary network traffic, but are buffered in the output buffers. The size of the output buffers can be set in the Flink config files. While this method is good for optimizing throughput, it can cause latency issues when the incoming stream is not fast enough.
To tackle this issue the user can call `env.setBufferTimeout(timeoutMillis)` on the execution environment (or on individual operators) to set a maximum wait time for the buffers to fill up. After this time the buffers are flushed automatically even if they are not full. The default value for this timeout is 100 ms which should be appropriate for most use-cases. 

Usage:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
env.setBufferTimeout(timeoutMillis);

env.genereateSequence(1,10).map(new MyMapper()).setBufferTimeout(timeoutMillis);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment
env.setBufferTimeout(timeoutMillis)

env.genereateSequence(1,10).map(myMap).setBufferTimeout(timeoutMillis)
{% endhighlight %}
</div>
</div>

To maximise the throughput the user can call `setBufferTimeout(-1)` which will remove the timeout and buffers will only be flushed when they are full.
To minimise latency, set the timeout to a value close to 0 (for example 5 or 10 ms). Theoretically a buffer timeout of 0 will cause all outputs to be flushed when produced, but this setting should be avoided because it can cause severe performance degradation.


[Back to top](#top)
    
Stream connectors
----------------

Connectors provide an interface for accessing data from various third party sources (message queues). Currently four connectors are natively supported, namely [Apache Kafka](https://kafka.apache.org/),  [RabbitMQ](http://www.rabbitmq.com/), [Apache Flume](https://flume.apache.org/index.html) and [Twitter Streaming API](https://dev.twitter.com/docs/streaming-apis).

Typically the connector packages consist of a source and sink class (with the exception of Twitter where only a source is provided). To use these sources the user needs to pass Serialization/Deserialization schemas for the connectors for the desired types. (Or use some predefined ones)

To run an application using one of these connectors usually additional third party components are required to be installed and launched, e.g. the servers for the message queues. Further instructions for these can be found in the corresponding subsections. [Docker containers](#docker-containers-for-connectors) are also provided encapsulating these services to aid users getting started with connectors.

### Apache Kafka

This connector provides access to data streams from [Apache Kafka](https://kafka.apache.org/).

#### Installing Apache Kafka
* Follow the instructions from [Kafka's quickstart](https://kafka.apache.org/documentation.html#quickstart) to download the code and launch a server (launching a Zookeeper and a Kafka server is required every time before starting the application).
* On 32 bit computers [this](http://stackoverflow.com/questions/22325364/unrecognized-vm-option-usecompressedoops-when-running-kafka-from-my-ubuntu-in) problem may occur. 
* If the Kafka and Zookeeper servers are running on a remote machine, then the `advertised.host.name` setting in the `config/server.properties` file the  must be set to the machine's IP address.

#### Kafka Source
The standard `KafkaSource` is a Kafka consumer providing an access to one topic.

The following parameters have to be provided for the `KafkaSource(...)` constructor:

1. Zookeeper hostname
2. The topic name
3. Deserialization schema

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> stream = env
	.addSource(new KafkaSource<String>("localhost:2181", "test", new SimpleStringSchema()))
	.print();
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
stream = env
    .addSource(new KafkaSource[String]("localhost:2181", "test", new SimpleStringSchema)
    .print
{% endhighlight %}
</div>
</div>

#### Persistent Kafka Source
As Kafka persists all the data, a fault tolerant Kafka source can be provided.

The PersistentKafkaSource can read a topic, and if the job fails for some reason, the source will
continue on reading from where it left off after a restart.
For example if there are 3 partitions in the topic with offsets 31, 122, 110 read at the time of job
failure, then at the time of restart it will continue on reading from those offsets, no matter whether these partitions have new messages.

To use fault tolerant Kafka Sources, monitoring of the topology needs to be enabled at the execution environment:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableMonitoring(5000);
{% endhighlight %}
</div>
</div>

Also note that Flink can only restart the topology if enough processing slots are available to restart the topology.
So if the topology fails due to loss of a TaskManager, there must be still enough slots available afterwards.
Flink on YARN supports automatic restart of lost YARN containers.

The following arguments have to be provided for the `PersistentKafkaSource(...)` constructor:

1. Zookeeper hostname
2. The topic name
3. Deserialization schema

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
stream.addSource(new PersistentKafkaSource<String>("localhost:2181", "test", new SimpleStringSchema()));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
stream.addSource(new PersistentKafkaSource[String]("localhost:2181", "test", new SimpleStringSchema))
{% endhighlight %}
</div>
</div>

#### Kafka Sink
A class providing an interface for sending data to Kafka. 

The followings have to be provided for the `KafkaSink(…)` constructor in order:

1. Zookeeper hostname
2. The topic name
3. Serialization schema

Example: 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
stream.addSink(new KafkaSink<String>("localhost:2181", "test", new SimpleStringSchema()));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
stream.addSink(new KafkaSink[String]("localhost:2181", "test", new SimpleStringSchema))
{% endhighlight %}
</div>
</div>

The user can also define custom Kafka producer configuration for the KafkaSink with the constructor:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public KafkaSink(String zookeeperAddress, String topicId, Properties producerConfig,
      SerializationSchema<IN, byte[]> serializationSchema)
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
public KafkaSink(String zookeeperAddress, String topicId, Properties producerConfig,
      SerializationSchema serializationSchema)
{% endhighlight %}
</div>
</div>

If this constructor is used, the user needs to make sure to set the broker with the "metadata.broker.list" property. Also the serializer configuration should be left default, the serialization should be set via SerializationSchema.

More about Kafka can be found [here](https://kafka.apache.org/documentation.html).

[Back to top](#top)

### Apache Flume

This connector provides access to data streams from [Apache Flume](http://flume.apache.org/).

#### Installing Apache Flume
[Download](http://flume.apache.org/download.html) Apache Flume. A configuration file is required for starting agents in Flume. A configuration file for running the example can be found [here](#config_file).

#### Flume Source
A class providing an interface for receiving data from Flume.

The followings have to be provided for the `FlumeSource(…)` constructor in order:

1. The hostname
2. The port number
3. Deserialization schema

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> stream = env
	.addSource(new FlumeSource<String>("localhost", 41414, new SimpleStringSchema()))
	.print();
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
stream = env
    .addSource(new FlumeSource[String]("localhost", 41414, new SimpleStringSchema))
    .print
{% endhighlight %}
</div>
</div>

#### Flume Sink
A class providing an interface for sending data to Flume. 

The followings have to be provided for the `FlumeSink(…)` constructor in order:

1. The hostname
2. The port number
3. Serialisation schema

Example: 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
stream.addSink(new FlumeSink<String>("localhost", 42424, new StringToByteSerializer()));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
stream.addSink(new FlumeSink[String]("localhost", 42424, new StringToByteSerializer))
{% endhighlight %}
</div>
</div>

##### Configuration file<a name="config_file"></a>
An example of a configuration file:

~~~
a1.channels = c1
a1.sources = r1
a1.sinks = k1

a1.channels.c1.type = memory

a1.sources.r1.channels = c1
a1.sources.r1.type = avro
a1.sources.r1.bind = localhost
a1.sources.r1.port = 42424

a1.sinks.k1.channel = c1
a1.sinks.k1.type = avro
a1.sinks.k1.hostname = localhost
a1.sinks.k1.port = 41414
~~~

To run the `FlumeTopology` example the previous configuration file must located in the Flume directory and named example.conf and the agent can be started with the following command:

~~~
bin/flume-ng agent --conf conf --conf-file example.conf --name a1 -Dflume.root.logger=INFO,console
~~~

If the agent is not started before the application starts a `FlumeSink` then the sink will retry to build the connection for 90 seconds, if unsuccessful it throws a `RuntimeException`.

More on Flume can be found [here](http://flume.apache.org).

[Back to top](#top)

### RabbitMQ

This connector provides access to data streams from [RabbitMQ](http://www.rabbitmq.com/).

##### Installing RabbitMQ
Follow the instructions from the [RabbitMQ download page](http://www.rabbitmq.com/download.html). After the installation the server automatically starts and the application connecting to RabbitMQ can be launched.

#### RabbitMQ Source

A class providing an interface for receiving data from RabbitMQ.

The followings have to be provided for the `RMQSource(…)` constructor in order:

1. The hostname
2. The queue name
3. Deserialization schema

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> stream = env
	.addSource(new RMQSource<String>("localhost", "hello", new SimpleStringSchema))
	.print
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
stream = env
    .addSource(new RMQSource[String]("localhost", "hello", new SimpleStringSchema))
    .print
{% endhighlight %}
</div>
</div>

#### RabbitMQ Sink
A class providing an interface for sending data to RabbitMQ. 

The followings have to be provided for the `RMQSink(…)` constructor in order:

1. The hostname
2. The queue name
3. Serialisation schema

Example: 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
stream.addSink(new RMQSink<String>("localhost", "hello", new StringToByteSerializer()));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
stream.addSink(new RMQSink[String]("localhost", "hello", new StringToByteSerializer))
{% endhighlight %}
</div>
</div>

More about RabbitMQ can be found [here](http://www.rabbitmq.com/).

[Back to top](#top)

### Twitter Streaming API

Twitter Streaming API provides opportunity to connect to the stream of tweets made available by Twitter. Flink Streaming comes with a built-in `TwitterSource` class for establishing a connection to this stream.

#### Authentication
In order to connect to Twitter stream the user has to register their program and acquire the necessary information for the authentication. The process is described below.

#### Acquiring the authentication information
First of all, a Twitter account is needed. Sign up for free at [twitter.com/signup](https://twitter.com/signup) or sign in at Twitter's [Application Management](https://apps.twitter.com/) and register the application by clicking on the "Create New App" button. Fill out a form about your program and accept the Terms and Conditions. 
After selecting the application you the API key and API secret (called `consumerKey` and `sonsumerSecret` in `TwitterSource` respectively) is located on the "API Keys" tab. The necessary access token data (`token` and `secret`) can be acquired here. 
Remember to keep these pieces of information a secret and do not push them to public repositories.

#### Accessing the authentication information
Create a properties file and pass its path in the constructor of `TwitterSource`. The content of the file should be similar to this:

~~~bash
#properties file for my app
secret=***
consumerSecret=***
token=***-***
consumerKey=***
~~~

#### Constructors
The `TwitterSource` class has two constructors.

1. `public TwitterSource(String authPath, int numberOfTweets);` 
to emit finite number of tweets
2. `public TwitterSource(String authPath);` 
for streaming

Both constructors expect a `String authPath` argument determining the location of the properties file containing the authentication information. In the first case, `numberOfTweets` determine how many tweet the source emits. 

#### Usage
In constract to other connecters the `TwitterSource` depends on no additional services. For example the following code should run gracefully:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> streamSource = env.addSource(new TwitterSource("/PATH/TO/myFile.properties"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
streamSource = env.addSource(new TwitterSource("/PATH/TO/myFile.properties"))
{% endhighlight %}
</div>
</div>

The `TwitterSource` emits strings containing a JSON code. 
To retrieve information from the JSON code you can add a FlatMap or a Map function handling JSON code. For example use an implementation `JSONParseFlatMap` abstract class among the examples. `JSONParseFlatMap` is an extension of the `FlatMapFunction` and has a

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
String getField(String jsonText, String field);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
getField(jsonText : String, field : String) : String
{% endhighlight %}
</div>
</div>

function which can be use to acquire the value of a given field. 

There are two basic types of tweets. The usual tweets contain information such as date and time of creation, id, user, language and many more details. The other type is the delete information.

#### Example
`TwitterLocal` is an example how to use `TwitterSource`. It implements a language frequency counter program. 

[Back to top](#top)

### Docker containers for connectors

A Docker container is provided with all the required configurations for test running the connectors of Apache Flink. The servers for the message queues will be running on the docker container while the example topology can be run on the user's computer. The only exception is Flume, more can be read about this issue in the [Flume section](#flume). 

#### Installing Docker
The official Docker installation guide can be found [here](https://docs.docker.com/installation/).
After installing Docker an image can be pulled for each connector. Containers can be started from these images where all the required configurations are set.

#### Creating a jar with all the dependencies
For the easiest set up create a jar with all the dependencies of the *flink-streaming-connectors* project.

~~~bash
cd /PATH/TO/GIT/flink/flink-staging/flink-streaming-connectors
mvn assembly:assembly
~~~bash

This creates an assembly jar under *flink-streaming-connectors/target*. 

#### RabbitMQ
Pull the docker image:

~~~bash
sudo docker pull flinkstreaming/flink-connectors-rabbitmq 
~~~

To run the container type:

~~~bash
sudo docker run -p 127.0.0.1:5672:5672 -t -i flinkstreaming/flink-connectors-rabbitmq
~~~

Now a terminal started running from the image with all the necessary configurations to test run the RabbitMQ connector. The -p flag binds the localhost's and the Docker container's ports so RabbitMQ can communicate with the application through these.

To start the RabbitMQ server:

~~~bash
sudo /etc/init.d/rabbitmq-server start
~~~

To launch the example on the host computer execute:

~~~bash
java -cp /PATH/TO/JAR-WITH-DEPENDENCIES org.apache.flink.streaming.connectors.rabbitmq.RMQTopology \
> log.txt 2> errorlog.txt
~~~

In the example there are two connectors. One that sends messages to RabbitMQ and one that receives messages from the same queue. In the logger messages the arriving messages can be observed in the following format:

~~~
<DATE> INFO rabbitmq.RMQTopology: String: <one> arrived from RMQ 
<DATE> INFO rabbitmq.RMQTopology: String: <two> arrived from RMQ
<DATE> INFO rabbitmq.RMQTopology: String: <three> arrived from RMQ
<DATE> INFO rabbitmq.RMQTopology: String: <four> arrived from RMQ
<DATE> INFO rabbitmq.RMQTopology: String: <five> arrived from RMQ
~~~

#### Apache Kafka

Pull the image:

~~~bash
sudo docker pull flinkstreaming/flink-connectors-kafka 
~~~

To run the container type:

~~~bash
sudo docker run -p 127.0.0.1:2181:2181 -p 127.0.0.1:9092:9092 -t -i \
flinkstreaming/flink-connectors-kafka
~~~

Now a terminal started running from the image with all the necessary configurations to test run the Kafka connector. The -p flag binds the localhost's and the Docker container's ports so Kafka can communicate with the application through these.
First start a zookeeper in the background:

~~~bash
/kafka_2.9.2-0.8.1.1/bin/zookeeper-server-start.sh /kafka_2.9.2-0.8.1.1/config/zookeeper.properties \
> zookeeperlog.txt &
~~~

Then start the kafka server in the background:

~~~bash
/kafka_2.9.2-0.8.1.1/bin/kafka-server-start.sh /kafka_2.9.2-0.8.1.1/config/server.properties \
 > serverlog.txt 2> servererr.txt &
~~~

To launch the example on the host computer execute:

~~~bash
java -cp /PATH/TO/JAR-WITH-DEPENDENCIES org.apache.flink.streaming.connectors.kafka.KafkaTopology \
> log.txt 2> errorlog.txt
~~~


In the example there are two connectors. One that sends messages to Kafka and one that receives messages from the same queue. In the logger messages the arriving messages can be observed in the following format:

~~~
<DATE> INFO kafka.KafkaTopology: String: (0) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (1) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (2) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (3) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (4) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (5) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (6) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (7) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (8) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (9) arrived from Kafka
~~~

#### Apache Flume

At the moment remote access for Flume connectors does not work. This example is only runnable on the same machine where the Flume server is. In this case both will be in the Docker container.

Pull the image:

~~~bash
sudo docker pull flinkstreaming/flink-connectors-flume
~~~

To run the container type:

~~~bash
sudo docker run -t -i flinkstreaming/flink-connectors-flume
~~~

Now a terminal started running from the image with all the necessary configurations to test run the Flume connector. The -p flag binds the localhost's and the Docker container's ports so flume can communicate with the application through these.

To have the latest version of Flink type:
~~~bash
cd /git/flink/
git pull
~~~

Then build the code with:

~~~bash
cd /git/flink/flink-staging/flink-streaming/flink-streaming-connectors/
mvn install -DskipTests
~~~

First start the server in the background:

~~~bash
/apache-flume-1.5.0-bin/bin/flume-ng agent \
--conf conf --conf-file /apache-flume-1.5.0-bin/example.conf --name a1 \
-Dflume.root.logger=INFO,console > /flumelog.txt 2> /flumeerr.txt &
~~~

Then press enter and launch the example with:

~~~bash
java -cp /PATH/TO/JAR-WITH-DEPENDENCIES org.apache.flink.streaming.connectors.flume.FlumeTopology
~~~

In the example there are to connectors. One that sends messages to Flume and one that receives messages from the same queue. In the logger messages the arriving messages can be observed in the following format:

~~~
<DATE> INFO flume.FlumeTopology: String: <one> arrived from Flume
<DATE> INFO flume.FlumeTopology: String: <two> arrived from Flume
<DATE> INFO flume.FlumeTopology: String: <three> arrived from Flume
<DATE> INFO flume.FlumeTopology: String: <four> arrived from Flume
<DATE> INFO flume.FlumeTopology: String: <five> arrived from Flume
~~~

[Back to top](#top)
