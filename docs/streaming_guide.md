---
title: "Flink Stream Processing API"
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

* This will be replaced by the TOC
{:toc}

<a href="#top"></a>

Introduction
------------


Flink Streaming is an extension of the core Flink API for high-throughput, low-latency data stream processing. The system can connect to and process data streams from many data sources like RabbitMQ, Flume, Twitter, ZeroMQ and also from any user defined data source. Data streams can be transformed and modified using high-level functions similar to the ones provided by the batch processing API. Flink Streaming provides native support for iterative stream processing. The processed data can be pushed to different output types.

Flink Streaming API
-----------

The Streaming API is currently part of the *addons* Maven project. All relevant classes are located in the *org.apache.flink.streaming* package.

Add the following dependency to your `pom.xml` to use the Flink Streaming.

~~~xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-core</artifactId>
    <version>{{site.FLINK_VERSION_STABLE}}</version>
</dependency>
~~~

Create a data stream flow with our Java API as described below. In order to create your own Flink Streaming program, we encourage you to start with the [skeleton](#program-skeleton) and gradually add your own [operations](#operations). The remaining sections act as references for additional operations and advanced features.


Example Program
---------------

The following program is a complete, working example of streaming WordCount. You can copy &amp; paste the code to run it locally.

~~~java
public class StreamingWordCount {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        
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
~~~

To run the example program start the input stream with netcat first from a terminal:

~~~batch
nc -lk 9999
~~~

The lines typed to this terminal are submitted as a source for your streaming job.

[Back to top](#top)

Program Skeleton
----------------

As presented in the [example](#example-program), a Flink Streaming program looks almost identical to a regular Flink program. Each stream processing program consists of the following parts:

1. Creating a `StreamExecutionEnvironment`,
2. Connecting to data stream sources,
3. Specifying transformations on the data streams,
4. Specifying output for the processed data,
5. Executing the program.

As these steps are basically the same as in the core API we will only note the important differences.
For stream processing jobs, the user needs to obtain a `StreamExecutionEnvironment` in contrast with the batch API where one would need an `ExecutionEnvironment`. The process otherwise is essentially the same:

~~~java 
StreamExecutionEnvironment.getExecutionEnvironment()
StreamExecutionEnvironment.createLocalEnvironment(parallelism)
StreamExecutionEnvironment.createRemoteEnvironment(…)
~~~

For connecting to data streams the `StreamExecutionEnvironment` has many different methods, from basic file sources to completely general user defined data sources. We will go into details in the [basics](#basics) section.

~~~java
env.socketTextStream(host, port)
env.fromElements(elements…)
~~~

After defining the data stream sources, the user can specify transformations on the data streams to create a new data stream. Different data streams can be also combined together for joint transformations which are being showcased in the [operations](#operations) section.

~~~java
dataStream.map(new Mapper()).reduce(new Reducer())
~~~

The processed data can be pushed to different outputs called sinks. The user can define their own sinks or use any predefined filesystem or database sink.

~~~java
dataStream.writeAsCsv(path)
dataStream.print()
~~~

Once the complete program is specified `execute(programName)` is to be called on the `StreamExecutionEnvironment`. This will either execute on the local machine or submit the program for execution on a cluster, depending on the chosen execution environment.

~~~java
env.execute(programName)
~~~

[Back to top](#top)

Basics
----------------

### DataStream

The `DataStream` is the basic abstraction provided by the Flink Streaming API. It represents a continuous stream of data of a certain type from either a data source or a transformed data stream. Operations will be applied on individual data points or windows of the `DataStream` based on the type of the operation. For example the map operator transforms each data point individually while window operations work on an interval of data points at the same time.
 
The operations may return different `DataStream` types allowing more elaborate transformations, for example the `groupBy(…)` method returns a `GroupedDataStream` which can be used for grouped operations such as aggregating by key.

### Partitioning

Partitioning controls how individual data points are distributed among the parallel instances of the transformation operators. By default *Forward* partitioning is used. There are several partitioning types supported in Flink Streaming:

 * *Forward*: Forward partitioning directs the output data to the next operator on the same machine (if possible) avoiding expensive network I/O. If there are more processing nodes than inputs or vice verse the load is distributed among the extra nodes in a round-robin fashion. This is the default partitioner.
Usage: `dataStream.forward()`
 * *Shuffle*: Shuffle partitioning randomly partitions the output data stream to the next operator using uniform distribution. Use this only when it is important that the partitioning is randomised. If you only care about an even load use *Distribute*
Usage: `dataStream.shuffle()`
 * *Distribute*: Distribute partitioning directs the output data stream to the next operator in a round-robin fashion, achieving a balanced distribution.
Usage: `dataStream.distribute()`
 * *Field/Key*: Field/Key partitioning partitions the output data stream based on the hash code of a selected key of the tuples. Data points with the same key are directed to the same operator instance. The user can define keys by field positions (for tuple and array types), field expressions (for Pojo types) and custom keys using the `KeySelector` interface. 
Usage: `dataStream.partitionBy(keys)`
 * *Broadcast*: Broadcast partitioning sends the output data stream to all parallel instances of the next operator.
Usage: `dataStream.broadcast()`
 * *Global*: All data points end up at the same operator instance. To achieve this use the parallelism setting of the corresponding operator.
Usage: `operator.setParallelism(1)`

### Sources

The user can connect to data streams by the different implementations of `SourceFunction` using `StreamExecutionEnvironment.addSource(SourceFunction)`. In contrast with other operators, DataStreamSources have a default operator parallelism of 1.

To create parallel sources the users source function needs to implement `ParallelSourceFunction` or extend `RichParallelSourceFunction` in which cases the source will have the parallelism of the environment. The degree of parallelism for ParallelSourceFunctions can be changed afterwards using `source.setParallelism(int dop)`.

There are several predefined ones similar to the ones of the batch API and some streaming specific ones like:

 * `socketTextStream(hostname, port)`
 * `readTextStream(filepath)`
 * `generateSequence(from, to)`
 * `fromElements(elements…)`
 * `fromCollection(collection)`
 * `readTextFile(filepath)`

These can be used to easily test and debug streaming programs.
There are pre-implemented connectors for a number of the most popular message queue services, please refer to the section on [connectors](#stream-connectors) for more detail.

### Sinks

`DataStreamSink` represents the different outputs of a Flink Streaming program. There are several pre-defined implementations available right away:

 * `dataStream.print()` – Writes the DataStream to the standard output, practical for testing purposes
 * `dataStream.writeAsText(parameters)` – Writes the DataStream to a text file
 * `dataStream.writeAsCsv(parameters)` – Writes the DataStream to CSV format

The user can also implement arbitrary sink functionality by implementing the `SinkFunction` interface and using it with `dataStream.addSink(sinkFunction)`.

[Back to top](#top)

Operations
----------------

Operations represent transformations on the `DataStream`. The user can chain and combine multiple operators on the data stream to produce the desired processing steps. Most of the operators work very similar to the core Flink API allowing developers to reason about `DataStream` the same way as they would about `DataSet`. At the same time there are operators that exploit the streaming nature of the data to allow advanced functionality.

### Basic operators

Basic operators can be seen as functions that transform each data element in the data stream.
 
#### Map
The Map transformation applies a user-defined `MapFunction` on each element of a `DataStream`. It implements a one-to-one mapping, that is, exactly one element must be returned by the function.
A map operator that doubles the values of the input stream:

~~~java
dataStream.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return 2 * value;
            }
        })
~~~

#### FlatMap
The FlatMap transformation applies a user-defined `FlatMapFunction` on each element of a `DataStream`. This variant of a map function can return arbitrary many result elements (including none) for each input element.
A flatmap operator that splits sentences to words:

~~~java
dataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for(String word: value.split(" ")){
                    out.collect(word);
                }
            }
        })
~~~

#### Filter
The Filter transformation applies a user-defined `FilterFunction` on each element of a `DataStream` and retains only those elements for which the function returns true.
A filter that filters out zero values:

~~~java
dataStream.filter(new FilterFunction<Integer>() { 
            @Override
            public boolean filter(Integer value) throws Exception {
                return value != 0;
            }
        })
~~~

#### Reduce
The Reduce transformation applies a user-defined `ReduceFunction` to all elements of a `DataStream`. The `ReduceFunction` subsequently combines pairs of elements into one element and outputs the current reduced value as a `DataStream`.
A reducer that sums up the incoming stream:

~~~java
dataStream.reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer value1, Integer value2) throws Exception {
                return value1+value2;
            }
        })
~~~

#### Merge
Merges two or more `DataStream` outputs, creating a new DataStream containing all the elements from all the streams.

~~~java
dataStream.merge(otherStream1, otherStream2…)
~~~

### Grouped operators

Some transformations require that the elements of a `DataStream` are grouped on some key. The user can create a `GroupedDataStream` by calling the `groupBy(key)` method of a non-grouped `DataStream`. 
Keys can be of three types: fields positions (applicable for tuple/array types), field expressions (applicable for pojo types), KeySelector instances. 

The user can apply different reduce transformations on the obtained `GroupedDataStream`:

#### Reduce on GroupedDataStream
When the reduce operator is applied on a grouped data stream, the user-defined `ReduceFunction` will combine subsequent pairs of elements having the same key value. The combined results are sent to the output stream.

### Aggregations

The Flink Streaming API supports different types of pre-defined aggregation operators similarly to the core API.

Types of aggregations: `sum(field)`, `min(field)`, `max(field)`, `minBy(field, first)`, `maxBy(field, first)`

With `sum`, `min`, and `max` for every incoming tuple the selected field is replaced with the current aggregated value. Fields can be selected using either field positions or field expressions (similarly to grouping).  

With `minBy` and `maxBy` the output of the operator is the element with the current minimal or maximal value at the given field. If more components share the minimum or maximum value, the user can decide if the operator should return the first or last element. This can be set by the `first` boolean parameter.

There is also an option to apply user defined aggregations with the usage of the `aggregate(…)` function of the data stream.

### Window operators

Flink streaming provides very flexible windowing semantics to create arbitrary windows (also referred to as discretizations or slices) of the data streams and apply reduction or aggregation operations on the windows acquired. Windowing can be used for instance to create rolling aggregations of the most recent N elements, where N could be defined by Time, Count or any arbitrary user defined measure.

The user can control the size (eviction) of the windows and the frequency of reduction or aggregation calls (triggers) on them in an intuitive API:


 * `dataStream.window(…).every(…).reduce(…)`
 * `dataStream.window(…).every(…).reduceGroup(…)`
 * `dataStream.window(…).every(…).aggregate(…)`

The next example would create windows that hold elements of the last 5 seconds, and the user defined aggregation/reduce is executed on the windows every second (sliding the window by 1 second):

~~~java
dataStream.window(Time.of(5, TimeUnit.SECONDS)).every(Time.of(1, TimeUnit.SECONDS))
~~~

This approach is often referred to as policy based windowing. Different policies (count, time, etc.) can be mixed as well; for example to downsample our stream, a window that takes the latest 100 elements of the stream every minute is created as follows:

~~~java
dataStream.window(Count.of(100)).every(Time.of(1, TimeUnit.MINUTES))
~~~

The user can also omit the `.every(…)` call which results in a tumbling window emptying the window after every aggregation call.

Several predefined policies are provided in the API, including delta-based, count-based and time-based policies. These can be accessed through the static methods provided by the `PolicyHelper` classes:

 * `Time.of(…)`
 * `Count.of(…)`
 * `Delta.of(…)`

For detailed description of these policies please refer to the javadocs.

#### Policy based windowing
The policy based windowing is a highly flexible way to specify stream discretisation also called windowing semantics. Two types of policies are used for such a specification:

 * `TriggerPolicy` defines when to trigger the reduce UDF on the current window and emit the result. In the API it completes a window statement such as: `.window(…).every(…)`, while the triggering policy is passed within `every`. 

When multiple triggers are used, the reduction or aggregation is executed at every trigger.

Several predefined policies are provided in the API, including delta-based, punctuation based, count-based and time-based policies. Policies are in general UDFs and can implement any custom behaviour.

 * `EvictionPolicy` defines the length of a window as a means of a predicate for evicting tuples when they are no longer needed. In the API this can be defined by the `.window(…)` operation on a stream. There are mostly the same predefined policy types provided as for trigger policies.

When multiple evictions are used the strictest one controls the elements in the window. For instance in the call `dataStream.window(Count.of(5), Time.of(1,TimeUnit.SECONDS)).every(…)` produces a window of maximum 5 elements which have arrived in the last second.

In addition to the `dataStream.window(…).every(…)` style users can specifically pass the list of trigger and eviction policies during the window call:

~~~java
dataStream.window(ListOfTriggerPolicies, ListOfEvictionPolicies)
~~~

By default most triggers can only trigger when a new element arrives. This might not be suitable for all the use-cases, especially when time based windowing is applied. To also provide trigering between elements so called active policies can be used. The predefined time-based policies are already implemented in such a way and can hold as an example for user defined active policy implementations. 

Time-based trigger and eviction policies can work with user defined `TimeStamp` implementations, these policies already cover most use cases.
 
#### Reduce on windowed data streams
The transformation calls a user-defined `ReduceFunction` at every trigger on the records currently in the window. The user can also use the different streaming aggregations.

A window reduce that sums the elements in the last minute with 10 seconds slide interval:

~~~java
dataStream.window(Time.of(1, TimeUnit.MINUTES)).every(Time.of(10,TimeUnit.SECONDS)).sum(field);
~~~

#### ReduceGroup on windowed data streams
The transformation calls a `GroupReduceFunction` for each data batch or data window similarly as a reduce, but providing access to all elements in the window.

~~~java
dataStream.window(…).every(…).reduceGroup(reducer);
~~~

#### Grouped operations on windowed data streams
Calling the `.groupBy(fields)` method on a windowed stream groups the elements by the given fields inside the windows. The window sizes (evictions) and slide sizes (triggers) will be calculated on the whole stream (in a central fashion), but the user defined functions will be applied on a per group basis.

The user can also create windows and triggers on a per group basis calling `.window(…).every(…)` on an already grouped data stream. To highlight the differences let us look at to examples.

To get the maximal value by key on the last 100 elements we use the first approach:

~~~java
dataStream.window(Count.of(100)).every(…).groupBy(groupingField).max(field);
~~~

Using this approach we took the last 100 elements, divided it into groups by key then applied the aggregation.

To create fixed size windows for every key we need to reverse the order of the groupBy call. So to take the max for the last 100 elements in Each group:

~~~java
dataStream.groupBy(groupingField).window(Count.of(100)).every(…).max(field);
~~~

This will create separate windows for different keys and apply the trigger and eviction policies on a per group basis.

### Temporal database style operators

While database style operators like joins (on key) and crosses are hard to define properly on data streams, a straight forward implementation is to apply these operators on windows of the data streams.

Currently join and cross operators are supported on time windows.

The Join transformation produces a new Tuple DataStream with two fields. Each tuple holds a joined element of the first input DataStream in the first tuple field and a matching element of the second input DataStream in the second field for the current window.

The following code shows a default Join transformation using field position keys:

~~~java
dataStream1.join(dataStream2)
		.onWindow(windowing_params)
		.where(key_in_first)
		.equalTo(key_in_second);
~~~

The Cross transformation combines two DataStreams into one DataStreams. It builds all pairwise combinations of the elements of both input DataStreams in the current window, i.e., it builds a temporal Cartesian product.

~~~java
dataStream1.cross(dataStream2).onWindow(windowing_params);
~~~


### Co operators

Co operators allow the users to jointly transform two `DataStreams` of different types providing a simple way to jointly manipulate a shared state. It is designed to support joint stream transformations where merging is not appropriate due to different data types or in case the user needs explicit tracking of the joined stream origin.
Co operators can be applied to `ConnectedDataStreams` which represent two `DataStreams` of possibly different types. A `ConnectedDataStream` can be created by calling the `connect(otherDataStream)` method of a `DataStream`. Please note that the two connected `DataStreams` can also be merged data streams.

#### Map on ConnectedDataStream
Applies a CoMap transformation on two separate DataStreams, mapping them to a common output type. The transformation calls a `CoMapFunction.map1()` for each element of the first input and `CoMapFunction.map2()` for each element of the second input. Each CoMapFunction call returns exactly one element.
A CoMap operator that outputs true if an Integer value is received and false if a String value is received:

~~~java
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
~~~

#### FlatMap on ConnectedDataStream
The FlatMap operator for the `ConnectedDataStream` works similarly to CoMap, but instead of returning exactly one element after each map call the user can output arbitrarily many values using the Collector interface. 

~~~java
DataStream<Integer> dataStream1 = ...
DataStream<String> dataStream2 = ...
        
dataStream1.connect(dataStream2)
    .flatMap(new CoFlatMapFunction<Integer, String, Boolean>() {

            @Override
            public void flatMap1(Integer value, Collector<Boolean> out) {
                out.collect(true);
            }

            @Override
            public void flatMap2(String value, Collector<Boolean> out) {
                out.collect(false);
            }
        })
~~~

#### WindowReduce on ConnectedDataStream
The windowReduce operator applies a user defined `CoWindowFunction` to time aligned windows of the two data streams and return zero or more elements of an arbitrary type. The user can define the window and slide intervals and can also implement custom timestamps to be used for calculating windows.

#### Reduce on ConnectedDataStream
The Reduce operator for the `ConnectedDataStream` applies a simple reduce transformation on the joined data streams and then maps the reduced elements to a common output type.

### Output splitting

Most data stream operators support directed outputs (output splitting), meaning that different output elements are sent only to specific outputs. The outputs are referenced by their name given at the point of receiving:

~~~java
SplitDataStream<Integer> split = someDataStream.split(outputSelector);
DataStream<Integer> even = split.select("even”);
DataStream<Integer> odd = split.select("odd");
~~~

In the above example the data stream named ‘even’ will only contain elements that are directed to the output named “even”. The user can of course further transform these new stream by for example squaring only the even elements.

Data streams only receive the elements directed to selected output names. The user can also select multiple output names by `splitStream.select(“output1”, “output2”…)`. It is common that a stream listens to all the outputs, so `split.selectAll()` provides this functionality without having to select all names.

The outputs of an operator are directed by implementing a selector function (implementing the `OutputSelector` interface):

~~~java
Iterable<String> select(OUT value);
~~~

The data is sent to all the outputs returned in the iterable (referenced by their name). This way the direction of the outputs can be determined by the value of the data sent. 

For example to split even and odd numbers:

~~~java
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
~~~

Every output will be emitted to the selected outputs exactly once, even if you add the same output names more than once.

### Iterations
The Flink Streaming API supports implementing iterative stream processing dataflows similarly to the core Flink API. Iterative streaming programs also implement a step function and embed it into an `IterativeDataStream`.
Unlike in the core API the user does not define the maximum number of iterations, but at the tail of each iteration part of the output is streamed forward to the next operator and part is streamed back to the iteration head. The user controls the output of the iteration tail using [output splitting](#output-splitting).
To start an iterative part of the program the user defines the iteration starting point:

~~~java
IterativeDataStream<Integer> iteration = source.iterate(maxWaitTimeMillis);
~~~
The operator applied on the iteration starting point is the head of the iteration, where data is fed back from the iteration tail.

~~~java
DataStream<Integer> head = iteration.map(new IterationHead());
~~~

To close an iteration and define the iteration tail, the user calls `.closeWith(iterationTail)` method of the `IterativeDataStream`.

A common pattern is to use output splitting:

~~~java
SplitDataStream<..> tailOperator = head.map(new IterationTail()).split(outputSelector);
iteration.closeWith(tailOperator.select("iterate"));
~~~ 

In these case all output directed to the “iterate” edge would be fed back to the iteration head.

Because iterative streaming programs do not have a set number of iterations for each data element, the streaming program has no information on the end of its input. From this it follows that iterative streaming programs run until the user manually stops the program. While this is acceptable under normal circumstances a method is provided to allow iterative programs to shut down automatically if no input received by the iteration head for a predefined number of milliseconds.
To use this functionality the user needs to add the maxWaitTimeMillis parameter to the `dataStream.iterate(…)` call to control the max wait time. 

### Rich functions
The usage of rich functions are essentially the same as in the core Flink API. All transformations that take as argument a user-defined function can instead take a rich function as argument:

~~~java
dataStream.map(new RichMapFunction<Integer, String>() {
  public String map(Integer value) { return value.toString(); }
});
~~~

Rich functions provide, in addition to the user-defined function (`map()`, `reduce()`, etc), the `open()` and `close()` methods for initialization and finalization. (In contrast to the core API, the streaming API currently does not support the  `getRuntimeContext()` and `setRuntimeContext()` methods.)

[Back to top](#top)

### Lambda expressions with Java 8

For a more consice code one can rely on one of the main feautere of Java 8, lambda expressions. The following program has similar functionality to the one provided in the [example](#example-program) section, while showcasing the usage of lambda expressions.

~~~java
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
~~~

For a detailed Java 8 Guide please refer to the [Java 8 Programming Guide](java8_programming_guide.html). Operators specific to streaming, such as Operator splitting also support this usage. [Output splitting](#output-splitting) can be rewritten as follows:

~~~java
SplitDataStream<Integer> split = someDataStream
					.split(x -> Arrays.asList(String.valueOf(x % 2)));
~~~

Operator Settings
----------------

### Parallelism

Setting parallelism for operators works exactly the same way as in the core Flink API. The user can control the number of parallel instances created for each operator by calling the `operator.setParallelism(dop)` method.

### Buffer timeout

By default data points are not transferred on the network one-by-one, which would cause unnecessary network traffic, but are buffered in the output buffers. The size of the output buffers can be set in the Flink config files. While this method is good for optimizing throughput, it can cause latency issues when the incoming stream is not fast enough.
To tackle this issue the user can call `env.setBufferTimeout(timeoutMillis)` on the execution environment (or on individual operators) to set a maximum wait time for the buffers to fill up. After this time the buffers are flushed automatically even if they are not full. The default value for this timeout is 100ms which should be appropriate for most use-cases. 

Usage:

~~~java
LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
env.setBufferTimeout(timeoutMillis);

env.genereateSequence(1,10).map(new MyMapper()).setBufferTimeout(timeoutMillis);
~~~

To maximise the throughput the user can call `.setBufferTimeout(-1)` which will remove the timeout and buffers will only be flushed when they are full.
To minimise latency, set the timeout to a value close to 0 (fro example 5 or 10 ms). Theoretically a buffer timeout of 0 will cause all outputs to be flushed when produced, but this setting should be avoided because it can cause severe performance degradation.


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
* If the Kafka zookeeper and server are running on a remote machine then in the config/server.properties file the advertised.host.name must be set to the machine's IP address.

#### Kafka Source
A class providing an interface for receiving data from Kafka.

The followings have to be provided for the `KafkaSource(..)` constructor in order:

1. The hostname
2. The group name
3. The topic name
4. The parallelism
5. Deserialisation schema


Example:

~~~java
DataStream<String> stream = env
	.addSource(new KafkaSource<String>("localhost:2181", "group", "test",new SimpleStringSchema()))
	.print();
~~~

#### Kafka Sink
A class providing an interface for sending data to Kafka. 

The followings have to be provided for the `KafkaSink()` constructor in order:

1. The topic name
2. The hostname
3. Serialisation schema

Example: 

~~~java
stream.addSink(new KafkaSink<String, String>("test", "localhost:9092", new SimpleStringSchema()));
~~~


More about Kafka can be found [here](https://kafka.apache.org/documentation.html).

[Back to top](#top)

### Apache Flume

This connector provides access to datastreams from [Apache Flume](http://flume.apache.org/).

#### Installing Apache Flume
[Download](http://flume.apache.org/download.html) Apache Flume. A configuration file is required for starting agents in Flume. A configuration file for running the example can be found [here](#config_file).

#### Flume Source
A class providing an interface for receiving data from Flume.

The followings have to be provided for the `FlumeSource(…)` constructor in order:

1. The hostname
2. The port number
3. Deserialisation schema

Example:

~~~java
DataStream<String> stream = env
	.addSource(new FlumeSource<String>("localhost", 41414, new SimpleStringSchema()))
	.print();
~~~

#### Flume Sink
A class providing an interface for sending data to Flume. 

The followings have to be provided for the `FlumeSink(…)` constructor in order:

1. The hostname
2. The port number
3. Serialisation schema

Example: 

~~~java
stream.addSink(new FlumeSink<String>("localhost", 42424, new StringToByteSerializer()));
~~~

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

This connector provides access to datastreams from [RabbitMQ](http://www.rabbitmq.com/).

##### Installing RabbitMQ
Follow the instructions from the [RabbitMQ download page](http://www.rabbitmq.com/download.html). After the installation the server automatically starts and the application connecting to RabbitMQ can be launched.

#### RabbitMQ Source

A class providing an interface for receiving data from RabbitMQ.

The followings have to be provided for the `RMQSource(…)` constructor in order:

1. The hostname
2. The queue name
3. Deserialisation schema

Example:

~~~java
DataStream<String> stream = env
	.addSource(new RMQSource<String>("localhost", "hello", new SimpleStringSchema()))
	.print();
~~~

#### RabbitMQ Sink
A class providing an interface for sending data to RabbitMQ. 

The followings have to be provided for the `RMQSink(…)` constructor in order:

1. The hostname
2. The queue name
3. Serialisation schema

Example: 

~~~java
stream.addSink(new RMQSink<String>("localhost", "hello", new StringToByteSerializer()));
~~~


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

~~~batch
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

~~~java
DataStream<String> streamSource = env.AddSource(new TwitterSource("/PATH/TO/myFile.properties"));
~~~

The `TwitterSource` emits strings containing a JSON code. 
To retrieve information from the JSON code you can add a FlatMap or a Map function handling JSON code. For example use an implementation `JSONParseFlatMap` abstract class among the examples. `JSONParseFlatMap` is an extension of the `FlatMapFunction` and has a

~~~java
String getField(String jsonText, String field);
~~~

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

~~~batch
cd /PATH/TO/GIT/incubator-flink/flink-addons/flink-streaming-connectors
mvn assembly:assembly
~~~batch

This creates an assembly jar under *flink-streaming-connectors/target*. 

#### RabbitMQ
Pull the image:

~~~batch
sudo docker pull flinkstreaming/flink-connectors-rabbitmq 
~~~

To run the container type:

~~~batch
sudo docker run -p 127.0.0.1:5672:5672 -t -i flinkstreaming/flink-connectors-rabbitmq
~~~

Now a terminal started running from the image with all the necessary configurations to test run the RabbitMQ connector. The -p flag binds the localhost's and the Docker container's ports so RabbitMQ can communicate with the application through these.

To start the RabbitMQ server:

~~~batch
sudo /etc/init.d/rabbitmq-server start
~~~

To launch the example on the host computer execute:

~~~batch
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

~~~batch
sudo docker pull flinkstreaming/flink-connectors-kafka 
~~~

To run the container type:

~~~batch
sudo docker run -p 127.0.0.1:2181:2181 -p 127.0.0.1:9092:9092 -t -i \
flinkstreaming/flink-connectors-kafka
~~~

Now a terminal started running from the image with all the necessary configurations to test run the Kafka connector. The -p flag binds the localhost's and the Docker container's ports so Kafka can communicate with the application through these.
First start a zookeeper in the background:

~~~batch
/kafka_2.9.2-0.8.1.1/bin/zookeeper-server-start.sh /kafka_2.9.2-0.8.1.1/config/zookeeper.properties \
> zookeeperlog.txt &
~~~

Then start the kafka server in the background:

~~~batch
/kafka_2.9.2-0.8.1.1/bin/kafka-server-start.sh /kafka_2.9.2-0.8.1.1/config/server.properties \
 > serverlog.txt 2> servererr.txt &
~~~

To launch the example on the host computer execute:

~~~batch
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

~~~batch
sudo docker pull flinkstreaming/flink-connectors-flume
~~~

To run the container type:

~~~batch
sudo docker run -t -i flinkstreaming/flink-connectors-flume
~~~

Now a terminal started running from the image with all the necessary configurations to test run the Flume connector. The -p flag binds the localhost's and the Docker container's ports so flume can communicate with the application through these.

To have the latest version of Flink type:
~~~batch
cd /git/incubator-flink/
git pull
~~~

Then build the code with:

~~~batch
cd /git/incubator-flink/flink-addons/flink-streaming/flink-streaming-connectors/
mvn install -DskipTests
~~~

First start the server in the background:

~~~batch
/apache-flume-1.5.0-bin/bin/flume-ng agent \
--conf conf --conf-file /apache-flume-1.5.0-bin/example.conf --name a1 \
-Dflume.root.logger=INFO,console > /flumelog.txt 2> /flumeerr.txt &
~~~

Then press enter and launch the example with:

~~~batch
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
