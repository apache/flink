---
title: "Overview"
weight: 1
type: docs
aliases:
  - /dev/stream/operators/
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

# Operators

Operators transform one or more DataStreams into a new DataStream. Programs can combine
multiple transformations into sophisticated dataflow topologies.

This section gives a description of the basic transformations, the effective physical
partitioning after applying those as well as insights into Flink's operator chaining.

## DataStream Transformations

### Map
#### DataStream &rarr; DataStream

Takes one element and produces one element. A map function that doubles the values of the input stream:

{{< tabs mapfunc >}}
{{< tab "Java">}}
```java
DataStream<Integer> dataStream = //...
dataStream.map(new MapFunction<Integer, Integer>() {
    @Override
    public Integer map(Integer value) throws Exception {
        return 2 * value;
    }
});
```
{{< /tab >}}
{{< tab "Scala">}}
```scala
dataStream.map { x => x * 2 }
```
{{< /tab >}}
{{< tab "Python" >}}
```python
data_stream = env.from_collection(collection=[1, 2, 3, 4, 5])
data_stream.map(lambda x: 2 * x, output_type=Types.INT())
```
{{< /tab >}}
{{< /tabs>}}

### FlatMap
#### DataStream &rarr; DataStream

Takes one element and produces zero, one, or more elements. A flatmap function that splits sentences to words:

{{< tabs flatmapfunc >}}
{{< tab "Java">}}
```java
dataStream.flatMap(new FlatMapFunction<String, String>() {
    @Override
    public void flatMap(String value, Collector<String> out)
        throws Exception {
        for(String word: value.split(" ")){
            out.collect(word);
        }
    }
});
```
{{< /tab >}}
{{< tab "Scala">}}
```scala
dataStream.flatMap { str => str.split(" ") }
```
{{< /tab >}}
{{< tab "Python" >}}
```python
data_stream = env.from_collection(collection=['hello apache flink', 'streaming compute'])
data_stream.flat_map(lambda x: x.split(' '), output_type=Types.STRING())
```
{{< /tab >}}
{{< /tabs>}}

### Filter
#### DataStream &rarr; DataStream

Evaluates a boolean function for each element and retains those for which the function returns true. A filter that filters out zero values: 

{{< tabs filterfunc >}}
{{< tab "Java">}}
```java
dataStream.filter(new FilterFunction<Integer>() {
    @Override
    public boolean filter(Integer value) throws Exception {
        return value != 0;
    }
});
```
{{< /tab >}}
{{< tab "Scala">}}
```scala
dataStream.filter { _ != 0 }
```
{{< /tab >}}
{{< tab "Python" >}}
```python
data_stream = env.from_collection(collection=[0, 1, 2, 3, 4, 5])
data_stream.filter(lambda x: x != 0)
```
{{< /tab >}}
{{< /tabs>}}

### KeyBy
#### DataStream &rarr; KeyedStream

Logically partitions a stream into disjoint partitions. All records with the same key are assigned to the same partition. Internally, _keyBy()_ is implemented with hash partitioning. There are different ways to [specify keys]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#keyed-datastream).

{{< tabs keybyfunc >}}
{{< tab "Java">}}
```java
dataStream.keyBy(value -> value.getSomeKey());
dataStream.keyBy(value -> value.f0);
```
{{< /tab >}}
{{< tab "Scala">}}
```scala
dataStream.keyBy(_.someKey)
dataStream.keyBy(_._1)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
data_stream = env.from_collection(collection=[(1, 'a'), (2, 'a'), (3, 'b')])
data_stream.key_by(lambda x: x[1], key_type=Types.STRING()) // Key by the result of KeySelector
```
{{< /tab >}}
{{< /tabs>}}

{{< hint warning >}}
A type **cannot be a key if**:

1. it is a POJO type but does not override the `hashCode()` method and relies on the `Object.hashCode()` implementation.
2. it is an array of any type.
{{< /hint >}}

### Reduce
#### KeyedStream &rarr; DataStream

A "rolling" reduce on a keyed data stream. Combines the current element with the last reduced value and emits the new value.

A reduce function that creates a stream of partial sums:

{{< tabs globalreduce >}}
{{< tab "Java">}}
```java
keyedStream.reduce(new ReduceFunction<Integer>() {
    @Override
    public Integer reduce(Integer value1, Integer value2)
    throws Exception {
        return value1 + value2;
    }
});
```
{{< /tab >}}
{{< tab "Scala">}}
```scala
keyedStream.reduce { _ + _ }
```
{{< /tab >}}
{{< tab "Python" >}}
```python
data_stream = env.from_collection(collection=[(1, 'a'), (2, 'a'), (3, 'a'), (4, 'b')], type_info=Types.TUPLE([Types.INT(), Types.STRING()]))
data_stream.key_by(lambda x: x[1]).reduce(lambda a, b: (a[0] + b[0], b[1]))
```
{{< /tab >}}
{{< /tabs>}}

### Window
#### KeyedStream &rarr; WindowedStream

Windows can be defined on already partitioned KeyedStreams. Windows group the data in each key according to some characteristic (e.g., the data that arrived within the last 5 seconds).
See [windows]({{< ref "docs/dev/datastream/operators/windows" >}}) for a complete description of windows.

{{< tabs window >}}
{{< tab "Java">}}
```java
dataStream
  .keyBy(value -> value.f0)
  .window(TumblingEventTimeWindows.of(Time.seconds(5))); 
```
{{< /tab >}}
{{< tab "Scala">}}
```scala
dataStream
  .keyBy(_._1)
  .window(TumblingEventTimeWindows.of(Time.seconds(5))) 
```
{{< /tab >}}
{{< tab "Python" >}}
```python
data_stream.key_by(lambda x: x[1]).window(TumblingEventTimeWindows.of(Time.seconds(5)))
```
{{< /tab >}}
{{< /tabs>}}

### WindowAll
#### DataStream &rarr; AllWindowedStream

Windows can be defined on regular DataStreams. Windows group all the stream events according to some characteristic (e.g., the data that arrived within the last 5 seconds). See [windows]({{< ref "docs/dev/datastream/operators/windows" >}}) for a complete description of windows.

{{< hint warning >}}
This is in many cases a non-parallel transformation. All records will be gathered in one task for the windowAll operator.
{{< /hint >}}

{{< tabs windowAll >}}
{{< tab "Java">}}
```java
dataStream
  .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));
```
{{< /tab >}}
{{< tab "Scala">}}
```scala
dataStream
  .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
data_stream.window_all(TumblingEventTimeWindows.of(Time.seconds(5)))
```
{{< /tab >}}
{{< /tabs>}}

### Window Apply 
#### WindowedStream &rarr; DataStream
#### AllWindowedStream &rarr; DataStream

Applies a general function to the window as a whole. Below is a function that manually sums the elements of a window.

{{< hint info >}}
If you are using a windowAll transformation, you need to use an `AllWindowFunction` instead.
{{< /hint >}}

{{< tabs windowapply >}}
{{< tab "Java">}}
```java
windowedStream.apply(new WindowFunction<Tuple2<String,Integer>, Integer, Tuple, Window>() {
    public void apply (Tuple tuple,
            Window window,
            Iterable<Tuple2<String, Integer>> values,
            Collector<Integer> out) throws Exception {
        int sum = 0;
        for (value t: values) {
            sum += t.f1;
        }
        out.collect (new Integer(sum));
    }
});

// applying an AllWindowFunction on non-keyed window stream
allWindowedStream.apply (new AllWindowFunction<Tuple2<String,Integer>, Integer, Window>() {
    public void apply (Window window,
            Iterable<Tuple2<String, Integer>> values,
            Collector<Integer> out) throws Exception {
        int sum = 0;
        for (value t: values) {
            sum += t.f1;
        }
        out.collect (new Integer(sum));
    }
});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
windowedStream.apply { WindowFunction }

// applying an AllWindowFunction on non-keyed window stream
allWindowedStream.apply { AllWindowFunction }
```
{{< /tab >}}
{{< tab "Python" >}}
```python
class MyWindowFunction(WindowFunction[tuple, int, int, TimeWindow]):

    def apply(self, key: int, window: TimeWindow, inputs: Iterable[tuple]) -> Iterable[int]:
        sum = 0
        for input in inputs:
            sum += input[1]
        yield sum


class MyAllWindowFunction(AllWindowFunction[tuple, int, TimeWindow]):

    def apply(self, window: TimeWindow, inputs: Iterable[tuple]) -> Iterable[int]:
        sum = 0
        for input in inputs:
            sum += input[1]
        yield sum


windowed_stream.apply(MyWindowFunction())

# applying an AllWindowFunction on non-keyed window stream
all_windowed_stream.apply(MyAllWindowFunction())
```
{{< /tab >}}
{{< /tabs>}}

### WindowReduce
#### WindowedStream &rarr; DataStream

Applies a functional reduce function to the window and returns the reduced value.

{{< tabs windowreduce >}}
{{< tab "Java" >}}
```java
windowedStream.reduce (new ReduceFunction<Tuple2<String,Integer>>() {
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
        return new Tuple2<String,Integer>(value1.f0, value1.f1 + value2.f1);
    }
});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
windowedStream.reduce { _ + _ }
```
{{< /tab >}}
{{< tab "Python" >}}
```python
class MyReduceFunction(ReduceFunction):

    def reduce(self, value1, value2):
        return value1[0], value1[1] + value2[1]


windowed_stream.reduce(MyReduceFunction())
```
{{< /tab >}}
{{< /tabs>}}

### Union
#### DataStream\* &rarr; DataStream

Union of two or more data streams creating a new stream containing all the elements from all the streams. Note: If you union a data stream with itself you will get each element twice in the resulting stream.

{{< tabs union >}}
{{< tab "Java" >}}
```java
dataStream.union(otherStream1, otherStream2, ...);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
dataStream.union(otherStream1, otherStream2, ...)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
data_stream.union(otherStream1, otherStream2, ...)
```
{{< /tab >}}
{{< /tabs>}}

### Window Join
#### DataStream,DataStream &rarr; DataStream

Join two data streams on a given key and a common window.

{{< tabs windowjoin >}}
{{< tab "Java" >}}
```java
dataStream.join(otherStream)
    .where(<key selector>).equalTo(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(3)))
    .apply (new JoinFunction () {...});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
dataStream.join(otherStream)
    .where(<key selector>).equalTo(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(3)))
    .apply { ... }
```
{{< /tab >}}
{{< tab "Python" >}}
This feature is not yet supported in Python
{{< /tab >}}
{{< /tabs>}}

### Interval Join
#### KeyedStream,KeyedStream &rarr; DataStream

Join two elements e1 and e2 of two keyed streams with a common key over a given time interval, so that `e1.timestamp + lowerBound <= e2.timestamp <= e1.timestamp + upperBound`.

{{< tabs intervaljoin >}}
{{< tab "Java" >}}
```java
// this will join the two streams so that
// key1 == key2 && leftTs - 2 < rightTs < leftTs + 2
keyedStream.intervalJoin(otherKeyedStream)
    .between(Time.milliseconds(-2), Time.milliseconds(2)) // lower and upper bound
    .upperBoundExclusive(true) // optional
    .lowerBoundExclusive(true) // optional
    .process(new IntervalJoinFunction() {...});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// this will join the two streams so that
// key1 == key2 && leftTs - 2 < rightTs < leftTs + 2
keyedStream.intervalJoin(otherKeyedStream)
    .between(Time.milliseconds(-2), Time.milliseconds(2)) 
    // lower and upper bound
    .upperBoundExclusive(true) // optional
    .lowerBoundExclusive(true) // optional
    .process(new IntervalJoinFunction() {...})
```
{{< /tab >}}
{{< tab "Python" >}}
This feature is not yet supported in Python
{{< /tab >}}
{{< /tabs>}}

### Window CoGroup
#### DataStream,DataStream &rarr; DataStream

Cogroups two data streams on a given key and a common window.

{{< tabs windowcogroup >}}
{{< tab "Java" >}}
```java
dataStream.coGroup(otherStream)
    .where(0).equalTo(1)
    .window(TumblingEventTimeWindows.of(Time.seconds(3)))
    .apply (new CoGroupFunction () {...});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
dataStream.coGroup(otherStream)
    .where(0).equalTo(1)
    .window(TumblingEventTimeWindows.of(Time.seconds(3)))
    .apply {}
```
{{< /tab >}}
{{< tab "Python" >}}
This feature is not yet supported in Python
{{< /tab >}}
{{< /tabs>}}

### Connect
#### DataStream,DataStream &rarr; ConnectedStream

"Connects" two data streams retaining their types. Connect allowing for shared state between the two streams.

{{< tabs connect >}}
{{< tab "Java" >}}
```java
DataStream<Integer> someStream = //...
DataStream<String> otherStream = //...

ConnectedStreams<Integer, String> connectedStreams = someStream.connect(otherStream);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
someStream : DataStream[Int] = ...
otherStream : DataStream[String] = ...

val connectedStreams = someStream.connect(otherStream)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
stream_1 = ...
stream_2 = ...
connected_streams = stream_1.connect(stream_2)
```
{{< /tab >}}
{{< /tabs>}}

### CoMap, CoFlatMap
#### ConnectedStream &rarr; DataStream

Similar to map and flatMap on a connected data stream

{{< tabs comap >}}
{{< tab "Java" >}}
```java
connectedStreams.map(new CoMapFunction<Integer, String, Boolean>() {
    @Override
    public Boolean map1(Integer value) {
        return true;
    }

    @Override
    public Boolean map2(String value) {
        return false;
    }
});
connectedStreams.flatMap(new CoFlatMapFunction<Integer, String, String>() {

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
});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
connectedStreams.map(
    (_ : Int) => true,
    (_ : String) => false
)
connectedStreams.flatMap(
    (_ : Int) => true,
    (_ : String) => false
)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
class MyCoMapFunction(CoMapFunction):
    
    def map1(self, value):
        return value[0] + 1, value[1]
       
    def map2(self, value):
        return value[0], value[1] + 'flink'
        
class MyCoFlatMapFunction(CoFlatMapFunction):
    
    def flat_map1(self, value)
        for i in range(value[0]):
            yield i
    
    def flat_map2(self, value):
        yield value[0] + 1
        
connectedStreams.map(MyCoMapFunction())
connectedStreams.flat_map(MyCoFlatMapFunction())
```
{{< /tab >}}
{{< /tabs>}}

### Cache
#### DataStream &rarr; CachedDataStream

Cache the intermediate result of the transformation. Currently, only jobs that run with batch 
execution mode are supported. The cache intermediate result is generated lazily at the first time 
the intermediate result is computed so that the result can be reused by later jobs. If the cache is 
lost, it will be recomputed using the original transformations.

{{< tabs cache >}}
{{< tab "Java" >}}
```java
DataStream<Integer> dataStream = //...
CachedDataStream<Integer> cachedDataStream = dataStream.cache();
cachedDataStream.print(); // Do anything with the cachedDataStream
...
env.execute(); // Execute and create cache.
        
cachedDataStream.print(); // Consume cached result.
env.execute();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val dataStream : DataStream[Int] = //...
val cachedDataStream = dataStream.cache()
cachedDataStream.print() // Do anything with the cachedDataStream
...
env.execute() // Execute and create cache.

cachedDataStream.print() // Consume cached result.
env.execute()
```
{{< /tab >}}
{{< tab "Python" >}}
```python
data_stream = ... # DataStream
cached_data_stream = data_stream.cache()
cached_data_stream.print()
# ...
env.execute() # Execute and create cache.

cached_data_stream.print() # Consume cached result.
env.execute()
```
{{< /tab >}}
{{< /tabs>}}

## Physical Partitioning

Flink also gives low-level control (if desired) on the exact stream partitioning after a transformation, via the following functions.

### Custom Partitioning
#### DataStream &rarr; DataStream

Uses a user-defined Partitioner to select the target task for each element. 

{{< tabs custompartitioning >}}
{{< tab "Java" >}}
```java
dataStream.partitionCustom(partitioner, "someKey");
dataStream.partitionCustom(partitioner, 0);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
dataStream.partitionCustom(partitioner, "someKey")
dataStream.partitionCustom(partitioner, 0)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
data_stream = env.from_collection(collection=[(2, 'a'), (2, 'a'), (3, 'b')])
data_stream.partition_custom(lambda key, num_partition: key % partition, lambda x: x[0])
```
{{< /tab >}}
{{< /tabs>}}

### Random Partitioning
#### DataStream &rarr; DataStream

Partitions elements randomly according to a uniform distribution. 

{{< tabs shuffle >}}
{{< tab "Java" >}}
```java
dataStream.shuffle();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
dataStream.shuffle()
```
{{< /tab >}}
{{< tab "Python" >}}
```python
data_stream.shuffle()
```
{{< /tab >}}
{{< /tabs>}}


### Rescaling
#### DataStream &rarr; DataStream

Partitions elements, round-robin, to a subset of downstream operations. This is useful if you want to have pipelines where you, for example, fan out from each parallel instance of a source to a subset of several mappers to distribute load but don't want the full rebalance that rebalance() would incur. This would require only local data transfers instead of transferring data over network, depending on other configuration values such as the number of slots of TaskManagers.

The subset of downstream operations to which the upstream operation sends elements depends on the degree of parallelism of both the upstream and downstream operation. For example, if the upstream operation has parallelism 2 and the downstream operation has parallelism 6, then one upstream operation would distribute elements to three downstream operations while the other upstream operation would distribute to the other three downstream operations. If, on the other hand, the downstream operation has parallelism 2 while the upstream operation has parallelism 6 then three upstream operations would distribute to one downstream operation while the other three upstream operations would distribute to the other downstream operation.

In cases where the different parallelisms are not multiples of each other one or several downstream operations will have a differing number of inputs from upstream operations.

Please see this figure for a visualization of the connection pattern in the above example:

{{< img src="/fig/rescale.svg" alt="Checkpoint barriers in data streams" >}}

{{< tabs rescale >}}
{{< tab "Java" >}}
```java
dataStream.rescale();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
dataStream.rescale()
```
{{< /tab >}}
{{< tab "Python" >}}
```python
data_stream.rescale()
```
{{< /tab >}}
{{< /tabs>}}

### Broadcasting
#### DataStream &rarr; DataStream

Broadcasts elements to every partition. 

{{< tabs broadcast >}}
{{< tab "Java" >}}
```java
dataStream.broadcast();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
dataStream.broadcast()
```
{{< /tab >}}
{{< tab "Python" >}}
```python
data_stream.broadcast()
```
{{< /tab >}}
{{< /tabs>}}

## Task Chaining and Resource Groups

Chaining two subsequent transformations means co-locating them within the same thread for better performance. Flink by default chains operators if this is possible (e.g., two subsequent map transformations). The API gives fine-grained control over chaining if desired:

Use `StreamExecutionEnvironment.disableOperatorChaining()` if you want to disable chaining in the whole job. For more fine grained control, the following functions are available. Note that these functions can only be used right after a DataStream transformation as they refer to the previous transformation. For example, you can use `someStream.map(...).startNewChain()`, but you cannot use `someStream.startNewChain()`.

A resource group is a slot in Flink, see slots. You can manually isolate operators in separate slots if desired.

### Start New Chain

Begin a new chain, starting with this operator.
The two mappers will be chained, and filter will not be chained to the first mapper. 

{{< tabs startnewchain >}}
{{< tab "Java" >}}
```java
someStream.filter(...).map(...).startNewChain().map(...);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
someStream.filter(...).map(...).startNewChain().map(...)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
some_stream.filter(...).map(...).start_new_chain().map(...)
```
{{< /tab >}}
{{< /tabs>}}

### Disable Chaining

Do not chain the map operator.

{{< tabs disablechaining >}}
{{< tab "Java" >}}
```java
someStream.map(...).disableChaining();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
someStream.map(...).disableChaining()
```
{{< /tab >}}
{{< tab "Python" >}}
```python
some_stream.map(...).disable_chaining()
```
{{< /tab >}}
{{< /tabs>}}

### Set Slot Sharing Group

Set the slot sharing group of an operation. Flink will put operations with the same slot sharing group into the same slot while keeping operations that don't have the slot sharing group in other slots. This can be used to isolate slots. The slot sharing group is inherited from input operations if all input operations are in the same slot sharing group. The name of the default slot sharing group is "default", operations can explicitly be put into this group by calling slotSharingGroup("default"). 

{{< tabs slotsharing >}}
{{< tab "Java" >}}
```java
someStream.filter(...).slotSharingGroup("name");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
someStream.filter(...).slotSharingGroup("name")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
some_stream.filter(...).slot_sharing_group("name")
```
{{< /tab >}}
{{< /tabs>}}

## Name And Description
Operators and job vertices in flink have a name and a description. 
Both name and description are introduction about what an operator or a job vertex is doing, but they are used differently.

The name of operator and job vertex will be used in web ui, thread name, logging, metrics, etc.
The name of a job vertex is constructed based on the name of operators in it.
The name needs to be as concise as possible to avoid high pressure on external systems.

The description will be used in the execution plan and displayed as the details of a job vertex in web UI.
The description of a job vertex is constructed based on the description of operators in it.
The description can contain detail information about operators to facilitate debugging at runtime.

{{< tabs namedescription >}}
{{< tab "Java" >}}
```java
someStream.filter(...).name("filter").setDescription("x in (1, 2, 3, 4) and y > 1");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
someStream.filter(...).name("filter").setDescription("x in (1, 2, 3, 4) and y > 1")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
some_stream.filter(...).name("filter").set_description("x in (1, 2, 3, 4) and y > 1")
```
{{< /tab >}}
{{< /tabs>}}

The format of description of a job vertex is a tree format string by default. 
Users can set `pipeline.vertex-description-mode` to `CASCADING`, if they want to set description to be the cascading format as in former versions.

Operators generated by Flink SQL will have a name consisted by type of operator and id, and a detailed description, by default.
Users can set `table.exec.simplify-operator-name-enabled` to `false`, if they want to set name to be the detailed description as in former versions.

When the topology of the pipeline is complex, users can add a topological index in the name of vertex by set `pipeline.vertex-name-include-index-prefix` to `true`,
so that we can easily find the vertex in the graph according to logs or metrics tags.

