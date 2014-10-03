---
title: "Flink Stream Processing API"
---

* This will be replaced by the TOC
{:toc}

<a href="#top"></a>

Introduction
------------


Flink Streaming is an extension of the core Flink API for high-throughput, low-latency data stream processing. The system can connect to and process data streams from many data sources like Flume, Twitter, ZeroMQ and also from any user defined data source. Data streams can be transformed and modified using high-level functions similar to the ones provided by the batch processing API. Flink Streaming provides native support for iterative stream processing. The processed data can be pushed to different output types.

Flink Streaming API
-----------

The Streaming API is part of the *addons* Maven project. All relevant classes are located in the *org.apache.flink.streaming* package.

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
                .fromElements("Who's there?",
            "I think I hear them. Stand, ho! Who's there?")
                .flatMap(new Splitter())
                .groupBy(0)
                .sum(1);
        
        dataStream.print();
        
        env.execute();
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
StreamExecutionEnvironment.createLocalEnvironment(params…)
StreamExecutionEnvironment.createRemoteEnvironment(params…)
~~~

For connecting to data streams the `StreamExecutionEnvironment` has many different methods, from basic file sources to completely general user defined data sources. We will go into details in the [basics](#basics) section.

~~~java
env.readTextFile(filePath)
~~~

After defining the data stream sources, the user can specify transformations on the data streams to create a new data stream. Different data streams can be also combined together for joint transformations which are being showcased in the [operations](#operations) section.

~~~java
dataStream.map(new Mapper()).reduce(new Reducer())
~~~

The processed data can be pushed to different outputs called sinks. The user can define their own sinks or use any predefined filesystem or database sink.

~~~java
dataStream.writeAsCsv(path)
~~~

Once the complete program is specified `execute()` needs to be called on the `StreamExecutionEnvironment`. This will either execute on the local machine or submit the program for execution on a cluster, depending on the chosen execution environment.

~~~java
env.execute()
~~~

[Back to top](#top)

Basics
----------------

### DataStream

The `DataStream` is the basic abstraction provided by the Flink Streaming API. It represents a continuous stream of data of a certain type from either a data source or a transformed data stream. Operations will be applied on individual data points or windows of the `DataStream` based on the type of the operation. For example the map operator transforms each data point individually while window or batch aggregations work on an interval of data points at the same time.
 
The operations may return different `DataStream` types allowing more elaborate transformations, for example the `groupBy()` method returns a `GroupedDataStream` which can be used for group operations.

### Partitioning

Partitioning controls how individual data points are distributed among the parallel instances of the transformation operators. By default *Forward* partitioning is used. There are several partitioning types supported in Flink Streaming:

 * *Forward*: Forward partitioning directs the output data to the next operator on the same machine (if possible) avoiding expensive network I/O. This is the default partitioner.
Usage: `dataStream.forward()`
 * *Shuffle*: Shuffle partitioning randomly partitions the output data stream to the next operator using uniform distribution.
Usage: `dataStream.shuffle()`
 * *Distribute*: Distribute partitioning directs the output data stream to the next operator in a round-robin fashion, achieving a balanced distribution.
Usage: `dataStream.distribute()`
 * *Field*: Field partitioning partitions the output data stream based on the hash code of a selected key field. Data points with the same key are directed to the same operator instance.
Usage: `dataStream.partitionBy(keyposition)`
 * *Broadcast*: Broadcast partitioning sends the output data stream to all parallel instances of the next operator.
Usage: `dataStream.broadcast()`
 * *Global*: All data points end up at the same operator instance. To achieve this use the parallelism setting of the corresponding operator.
Usage: `operator.setParallelism(1)`

### Sources

The user can connect to data streams by the different implemenations of `DataStreamSource` using methods provided in `StreamExecutionEnvironment`. There are several predefined ones similar to the ones provided by the batch API like:

 * `env.genereateSequence(from, to)`
 * `env.fromElements(elements…)`
 * `env.fromCollection(collection)`
 * `env.readTextFile(filepath)`

These can be used to easily test and debug streaming programs. There are also some streaming specific sources for example `env.readTextStream(filepath)` which iterates over the same file infinitely providing yet another nice testing tool.
There are implemented connectors for a number of the most popular message queue services, please refer to the section on [connectors](#stream-connectors) for more detail.
Besides the pre-defined solutions the user can implement their own source by implementing the `SourceFunction` interface and using the `env.addSource(sourceFunction)` method of the `StreamExecutionEnvironment`.

### Sinks

`DataStreamSink` represents the different outputs of a Flink Streaming program. There are several pre-defined implementations `DataStreamSink` available right away:

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
Merges two or more `DataStream` instances creating a new DataStream containing all the elements from all the streams.

~~~java
dataStream.merge(otherStream1, otherStream2…)
~~~

### Grouped operators

Some transformations require that the `DataStream` is grouped on some key value. The user can create a `GroupedDataStream` by calling the `groupBy(keyPosition)` method of a non-grouped `DataStream`. The user can apply different reduce transformations on the obtained `GroupedDataStream`:

#### Reduce on GroupedDataStream
When the reduce operator is applied on a grouped data stream, the user-defined `ReduceFunction` will combine subsequent pairs of elements having the same key value. The combined results are sent to the output stream.

### Aggregations

The Flink Streaming API supports different types of aggregation operators similarly to the core API. For grouped data streams the aggregations work in a grouped fashion.

Types of aggregations: `sum(fieldPosition)`, `min(fieldPosition)`, `max(fieldPosition)`, `minBy(fieldPosition, first)`, `maxBy(fieldPosition, first)`

With `sum`, `min`, and `max` for every incoming tuple the selected field is replaced with the current aggregated value. If the aggregations are used without defining field position, position `0` is used as default. 

With `minBy` and `maxBy` the output of the operator is the element with the current minimal or maximal value at the given fieldposition. If more components share the minimum or maximum value, the user can decide if the operator should return the first or last element. This can be set by the `first` boolean parameter.

### Window/Batch operators

Window and batch operators allow the user to execute function on slices or windows of the DataStream in a sliding fashion. If the stepsize for the slide is not defined then the window/batchsize is used as stepsize by default. The user can also use user defined timestamps for calculating time windows.

When applied to grouped data streams the data stream is batched/windowed for different key values separately. 

For example a `dataStream.groupBy(0).batch(100, 10)` produces batches of the last 100 elements for each key value with 10 record step size.
 
#### Reduce on windowed/batched data streams
The transformation calls a user-defined `ReduceFunction` on records received in the batch or during the predefined time window. The window is shifted after each reduce call. The user can also use the different streaming aggregations.

A window reduce that sums the elements in the last minute with 10 seconds slide interval:

~~~java
dataStream.window(60000, 10000).sum();
~~~

#### ReduceGroup on windowed/batched data streams
The transformation calls a `GroupReduceFunction` for each data batch or data window. The batch/window slides by the predefined number of elements/time after each call.

~~~java
dataStream.batch(1000, 100).reduceGroup(reducer);
~~~

### Co operators

Co operators allow the users to jointly transform two `DataStreams` of different types providing a simple way to jointly manipulate a shared state. It is designed to support joint stream transformations where merging is not appropriate due to different data types or the in cases when user needs explicit track of the datas origin.
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

#### winddowReduceGroup on ConnectedDataStream
The windowReduceGroup operator applies a user defined `CoGroupFunction` to time aligned windows of the two data streams and return zero or more elements of an arbitrary type. The user can define the window and slide intervals and can also implement custom timestamps to be used for calculating windows.

~~~java
DataStream<Integer> dataStream1 = ...
DataStream<String> dataStream2 = ...

dataStream1.connect(dataStream2)
    .windowReduceGroup(new CoGroupFunction<Integer, String, String>() {

        @Override
        public void coGroup(Iterable<Integer> first, Iterable<String> second,
            Collector<String> out) throws Exception {

            //Do something here

        }
    }, 10000, 5000);
~~~


#### Reduce on ConnectedDataStream
The Reduce operator for the `ConnectedDataStream` applies a simple reduce transformation on the joined data streams and then maps the reduced elements to a common output type.

### Output splitting

Most data stream operators support directed outputs, meaning that different data elements are received by only given outputs. The outputs are referenced by their name given at the point of receiving:

~~~java
SplitDataStream<Integer> split = someDataStream.split(outputSelector);
DataStream<Integer> even = split.select("even");
DataStream<Integer> odd = split.select("odd");
~~~

Data streams only receive the elements directed to selected output names. These outputs are directed by implementing a selector function (extending `OutputSelector`):

~~~java
void select(OUT value, Collection<String> outputs);
~~~

The data is sent to all the outputs added to the collection outputs (referenced by their name). This way the direction of the outputs can be determined by the value of the data sent. For example:

~~~java
@Override
void select(Integer value, Collection<String> outputs) {
    if (value % 2 == 0) {
        outputs.add("even");
    } else {
        outputs.add("odd");
    }
}
~~~

This output selection allows data streams to listen to multiple outputs, and data points to be sent to multiple outputs. A value is sent to all the outputs specified in the `OutputSelector` and a data stream will receive a value if it has selected any of the outputs the value is sent to. The stream will receive the data at most once.
It is common that a stream listens to all the outputs, so `split.selectAll()` is provided as an alias for explicitly selecting all output names.


### Iterations
The Flink Streaming API supports implementing iterative stream processing dataflows similarly to the core Flink API. Iterative streaming programs also implement a step function and embed it into an `IterativeDataStream`.
Unlike in the core API the user does not define the maximum number of iterations, but at the tail of each iteration the output is both streamed forward to the next operator and also streamed back to the iteration head. The user controls the output of the iteration tail using [output splitting](#output-splitting).
To start an iterative part of the program the user defines the iteration starting point:

~~~java
IterativeDataStream<Integer> iteration = source.iterate();
~~~
The operator applied on the iteration starting point is the head of the iteration, where data is fed back from the iteration tail.

~~~java
DataStream<Integer> head = iteration.map(new IterationHead());
~~~

To close an iteration and define the iteration tail, the user calls `.closeWith(tail)` method of the `IterativeDataStream`:

~~~java
DataStream<Integer> tail = head.map(new IterationTail());
iteration.closeWith(tail);
~~~
Or to use with output splitting:
~~~java
SplitDataStream<Integer> tail = head.map(new IterationTail()).split(outputSelector);
iteration.closeWith(tail.select("iterate"));
~~~ 

Because iterative streaming programs do not have a set number of iteratons for each data element, the streaming program has no information on the end of its input. From this it follows that iterative streaming programs run until the user manually stops the program. While this is acceptable under normal circumstances a method is provided to allow iterative programs to shut down automatically if no input received by the iteration head for a predefined number of milliseconds.
To use this function the user needs to call, the `iteration.setMaxWaitTime(millis)` to control the max wait time. 

### Rich functions
The usage of rich functions are essentially the same as in the core Flink API. All transformations that take as argument a user-defined function can instead take a rich function as argument:

~~~java
dataStream.map(new RichMapFunction<Integer, String>() {
  public String map(Integer value) { return value.toString(); }
});
~~~

Rich functions provide, in addition to the user-defined function (`map()`, `reduce()`, etc), the `open()` and `close()` methods for initialization and finalization. (In contrast to the core API, the streaming API currently does not support the  `getRuntimeContext()` and `setRuntimeContext()` methods.)

[Back to top](#top)


Operator Settings
----------------

### Parallelism

Setting parallelism for operators works exactly the same way as in the core Flink API. The user can control the number of parallel instances created for each operator by calling the `operator.setParallelism(dop)` method.

### Buffer timeout

By default data points are not transferred on the network one-by-one, which would cause unnecessary network traffic, but are buffered in the output buffers. The size of the output buffers can be set in the Flink config files. While this method is good for optimizing throughput, it can cause latency issues when the incoming stream is not fast enough.
To tackle this issue the user can call `env.setBufferTimeout(timeoutMillis)` on the execution environment (or on individual operators) to set a maximum wait time for the buffers to fill up. After this time the buffers are flushed automatically even if they are not full. Usage:

~~~java
LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
env.setBufferTimeout(timeoutMillis);

env.genereateSequence(1,10).map(new MyMapper()).setBufferTimeout(timeoutMillis);
~~~

### Mutability

Most operators allow setting mutability for reading input data. If the operator is set mutable then the variable used to store input data for operators will be reused in a mutable fashion to avoid excessive object creation. By default, all operators are set to immutable.
Usage:
~~~java
operator.setMutability(isMutable)
~~~

[Back to top](#top)
    
Stream connectors
----------------

Connectors provide an interface for accessing data from various third party sources (message queues). Currently four connectors are natively supported, namely [Apache Kafka](https://kafka.apache.org/),  [RabbitMQ](http://www.rabbitmq.com/), [Apache Flume](https://flume.apache.org/index.html) and [Twitter Streaming API](https://dev.twitter.com/docs/streaming-apis).

Typically the connector packages consist of an abstract source and sink (with the exception of Twitter where only a source is provided). The burden of the user is to implement a subclass of these abstract classes specifying a serializer and a deserializer function. 

To run an application using one of these connectors usually additional third party components are required to be installed and launched, e.g. the servers for the message queues. Further instructions for these can be found in the corresponding subsections. [Docker containers](#docker_connectors) are also provided encapsulating these services to aid users getting started with connectors.

### Apache Kafka

This connector provides access to data streams from [Apache Kafka](https://kafka.apache.org/).

#### Installing Apache Kafka
* Follow the instructions from [Kafka's quickstart](https://kafka.apache.org/documentation.html#quickstart) to download the code and launch a server (launching a Zookeeper and a Kafka server is required every time before starting the application).
* On 32 bit computers [this](http://stackoverflow.com/questions/22325364/unrecognized-vm-option-usecompressedoops-when-running-kafka-from-my-ubuntu-in) problem may occur. 
* If the Kafka zookeeper and server are running on a remote machine then in the config/server.properties file the advertised.host.name must be set to the machine's IP address.

#### Kafka Source
An abstract class providing an interface for receiving data from Kafka. By implementing the user must:

 * Write a constructor calling the constructor of the abstract class,
 * Write a deserializer function which processes the data coming from Kafka,
 * Stop the source manually when necessary with one of the close functions.

The implemented class must extend `KafkaSource`, for example: `KafkaSource<String>`.

##### Constructor
An example of an implementation of a constructor:

~~~java
public MyKafkaSource(String zkQuorum, String groupId, String topicId, int numThreads) {
    super(zkQuorum, groupId, topicId, numThreads);
}
~~~

##### Deserializer
An example of an implementation of a deserializer:

~~~java
@Override
public String deserialize(byte[] msg) {
    String s = new String(msg);
    if(s.equals("q")){
        closeWithoutSend();
    }
    return new String(s);
}
~~~

The source closes when it receives the String `"q"`.

###### Close<a name="kafka_source_close"></a>
Two types of close functions are available, namely `closeWithoutSend()` and `sendAndClose()`. The former closes the connection immediately and no further data will be sent, while the latter closes the connection only when the next message is sent after this call.

In the example provided `closeWithoutSend()` is used because here the String `"q"` is meta-message indicating the end of the stream and there is no need to forward it. 

#### Kafka Sink
An abstract class providing an interface for sending data to Kafka. By implementing the user must:

 * Write a constructor calling the constructor of the abstract class,
 * Write a serializer function to send data in the desired form to Kafka,
 * Stop the sink manually when necessary with one of the close functions.

The implemented class must extend `KafkaSink`, for example `KafkaSink<String, String>`.

##### Constructor
An example of an implementation of a constructor:

~~~java
public MyKafkaSink(String topicId, String brokerAddr) {
    super(topicId, brokerAddr);
}
~~~

##### Serializer
An example of an implementation of a serializer:

~~~java
@Override
public String serialize(String tuple) {
    if(tuple.equals("q")){
        sendAndClose();
    }
    return tuple;
}
~~~

##### Close
The API provided is the [same](#kafka_source_close) as the one for `KafkaSource`.

#### Building A Topology
To use a Kafka connector as a source in Flink call the `addSource()` function with a new instance of the class which extends `KafkaSource` as parameter:

~~~java
DataStream<String> stream1 = env.
    addSource(new MyKafkaSource("localhost:2181", "group", "test", 1), SOURCE_PARALELISM)
    .print();
~~~

The followings have to be provided for the `MyKafkaSource()` constructor in order:

1. The hostname
2. The group name
3. The topic name
4. The parallelism

Similarly to use a Kafka connector as a sink in Flink call the `addSink()` function with a new instance of the class which extends `KafkaSink`:

~~~java
DataStream<String> stream2 = env
    .addSource(new MySource())
    .addSink(new MyKafkaSink("test", "localhost:9092"));
~~~

The followings have to be provided for the `MyKafkaSink()` constructor in order:

1. The topic name
2. The hostname

More about Kafka can be found [here](https://kafka.apache.org/documentation.html).

[Back to top](#top)

### Apache Flume

This connector provides access to datastreams from [Apache Flume](http://flume.apache.org/).

#### Installing Apache Flume
[Download](http://flume.apache.org/download.html) Apache Flume. A configuration file is required for starting agents in Flume. A configuration file for running the example can be found [here](#config_file). 

#### Flume Source
An abstract class providing an interface for receiving data from Flume. By implementing the user must:

 * Write a constructor calling the constructor of the abstract class,
 * Write a deserializer function which processes the data coming from Flume,
 * Stop the source manually when necessary with one of the close functions.

The implemented class must extend `FlumeSource` for example: `FlumeSource<String>`

##### Constructor
An example of an implementation of a constructor:

~~~java
MyFlumeSource(String host, int port) {
    super(host, port);
}
~~~

##### Deserializer
An example of an implementation of a deserializer:

~~~java
@Override
public String deserialize(byte[] msg) {
    String s = (String) SerializationUtils.deserialize(msg);
    String out = s;
    if (s.equals("q")) {
        closeWithoutSend();
    }
    return out;
}
~~~

The source closes when it receives the String `"q"`.

##### Close<a name="flume_source_close"></a>
Two types of close functions are available, namely `closeWithoutSend()` and `sendAndClose()`.The former closes the connection immediately and no further data will be sent, while the latter closes the connection only when the next message is sent after this call.

In the example `closeWithoutSend()` is used because here the String `"q"` is meta-message indicating the end of the stream and there is no need to forward it. 

#### Flume Sink
An abstract class providing an interface for sending data to Flume. By implementing the user must:

* Write a constructor calling the constructor of the abstract class,
* Write a serializer function to send data in the desired form to Flume,
* Stop the sink manually when necessary with one of the close functions.

The implemented class must extend `FlumeSink`, for example `FlumeSink<String, String>`.

##### Constructor
An example of an implementation of a constructor:

~~~java
public MyFlumeSink(String host, int port) {
    super(host, port);
}
~~~

##### Serializer
An example of an implementation of a serializer.

~~~java
@Override
public byte[] serialize(String tuple) {
    if (tuple.equals("q")) {
        try {
            sendAndClose();
        } catch (Exception e) {
            new RuntimeException("Error while closing Flume connection with " + port + " at "
                + host, e);
        }
    }
    return SerializationUtils.serialize(tuple);
}
~~~

##### Close
The API provided is the [same](#flume_source_close) as the one for `FlumeSource`.

#### Building A Topology
To use a Flume connector as a source in Flink call the `addSource()` function with a new instance of the class which extends `FlumeSource` as parameter:

~~~java
DataStream<String> dataStream1 = env
    .addSource(new MyFlumeSource("localhost", 41414))
    .print();
~~~

The followings have to be provided for the `MyFlumeSource()` constructor in order:

1. The hostname
2. The port number

Similarly to use a Flume connector as a sink in Flink call the `addSink()` function with a new instance of the class which extends `FlumeSink`

~~~java
DataStream<String> dataStream2 = env
    .fromElements("one", "two", "three", "four", "five", "q")
    .addSink(new MyFlumeSink("localhost", 42424));
~~~

The followings have to be provided for the `MyFlumeSink()` constructor in order:

1. The hostname
2. The port number

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
An abstract class providing an interface for receiving data from RabbitMQ. By implementing the user must:

* Write a constructor calling the constructor of the abstract class,
* Write a deserializer function which processes the data coming from RabbitMQ,
* Stop the source manually when necessary with one of the close functions.

The implemented class must extend `RabbitMQSource` for example: `RabbitMQSource<String>`

##### Constructor
An example of an implementation of a constructor:

~~~java
public MyRMQSource(String HOST_NAME, String QUEUE_NAME) {
    super(HOST_NAME, QUEUE_NAME);
}
~~~

##### Deserializer
An example of an implemetation of a deserializer:

~~~java
@Override
public String deserialize(byte[] t) {
    String s = (String) SerializationUtils.deserialize(t);
    String out = s;
    if (s.equals("q")) {
        closeWithoutSend();
    }
    return out;
}
~~~

The source closes when it receives the String `"q"`.

##### Close<a name="rmq_source_close"></a>
Two types of close functions are available, namely `closeWithoutSend()` and `sendAndClose()`. The former closes the connection immediately and no further data will be sent, while the latter closes the connection only when the next message is sent after this call.

Closes the connection only when the next message is sent after this call.

In the example `closeWithoutSend()` is used because here the String `"q"` is meta-message indicating the end of the stream and there is no need to forward it. 

#### RabbitMQ Sink
An abstract class providing an interface for sending data to RabbitMQ. By implementing the user must:

* Write a constructor calling the constructor of the abstract class
* Write a serializer function to send data in the desired form to RabbitMQ
* Stop the sink manually when necessary with one of the close functions

The implemented class must extend `RabbitMQSink` for example: `RabbitMQSink<String, String>`

##### Constructor
An example of an implementation of a constructor:

~~~java
public MyRMQSink(String HOST_NAME, String QUEUE_NAME) {
    super(HOST_NAME, QUEUE_NAME);
}
~~~

##### Serializer
An example of an implementation of a serializer.

~~~java
@Override
public byte[] serialize(Tuple tuple) {
    if (t.getField(0).equals("q")) {
        sendAndClose();
    }
    return SerializationUtils.serialize(tuple.f0);
}
~~~

##### Close
The API provided is the [same](#rmq_source_close) as the one for `RabbitMQSource`.

#### Building A Topology
To use a RabbitMQ connector as a source in Flink call the `addSource()` function with a new instance of the class which extends `RabbitMQSource` as parameter:

~~~java
DataStream<String> dataStream1 = env
    .addSource(new MyRMQSource("localhost", "hello"))
    .print();
~~~

The followings have to be provided for the `MyRabbitMQSource()` constructor in order:

1. The hostname
2. The queue name

Similarly to use a RabbitMQ connector as a sink in Flink call the `addSink()` function with a new instance of the class which extends `RabbitMQSink`

~~~java
DataStream<String> dataStream2 = env
    .fromElements("one", "two", "three", "four", "five", "q")
    .addSink(new MyRMQSink("localhost", "hello"));
~~~

The followings have to be provided for the `MyRabbitMQSink()` constructor in order:

1. The hostname
1. The queue name

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

### Docker containers for connectors<a name="docker_connectors"></a>

A Docker container is provided with all the required configurations for test running the connectors of Apache Flink. The servers for the message queues will be running on the docker container while the example topology can be run on the user's computer. The only exception is Flume, more can be read about this issue at the [Flume section](#flume). 

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
