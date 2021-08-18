---
title: 概览
weight: 1
type: docs
aliases:
  - /zh/dev/stream/operators/
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

<a name="operators"></a>

# 算子

用户通过算子能将一个或多个 DataStream 转换成新的 DataStream，在应用程序中可以将多个数据转换算子合并成一个复杂的数据流拓扑。

这部分内容将描述 Flink DataStream API 中基本的数据转换 API，数据转换后各种数据分区方式，以及算子的链接策略。

<a name="datastream-transformations"></a>

## 数据流转换

<a name="map"></a>

### Map

#### DataStream &rarr; DataStream

输入一个数据同时输出一个数据。下面是将输入流中数值加倍的 map function：

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

<a name="flatmap"></a>

### FlatMap

#### DataStream &rarr; DataStream

输入一个数据同时产生零个、一个或多个数据。下面是将句子拆分为单词的 flatmap function：

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

<a name="filter"></a>

### Filter

#### DataStream &rarr; DataStream

为每个数据执行一个布尔 function，并保留那些 function 输出值为 true 的元素。下面是过滤掉零值的 filter：

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

<a name="keyby"></a>

### KeyBy
#### DataStream &rarr; KeyedStream

在逻辑上将流划分为不相交的分区。具有相同键的记录都分配到同一个分区。在内部， _keyBy()_  是通过哈希分区实现的。有多种[指定键]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#keyed-datastream)的方式。

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

以下情况，一个类**不能作为键**：

1. 它是一种 POJO 类，但没有重写 hashCode() 方法而是依赖于 Object.hashCode() 实现。
2. 它是任意类的数组。

{{< /hint >}}

<a name="reduce"></a>

### Reduce
#### KeyedStream &rarr; DataStream

在相同键的数据流上“滚动”执行 reduce。将当前元素与最后一次 reduce 得到的值组合然后输出新值。

下面是创建局部求和的流的 reduce function：

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

<a name="window"></a>

### Window
#### KeyedStream &rarr; WindowedStream

可以在已经分区的 KeyedStreams 上定义 Windows。Window 根据某些特征（例如，最近 5 秒内到达的数据）对每个键中的数据进行分组。请参阅 [windows]({{< ref "docs/dev/datastream/operators/windows" >}})获取有关 window 的完整说明。

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
This feature is not yet supported in Python
{{< /tab >}}
{{< /tabs>}}

<a name="windowall"></a>

### WindowAll
#### DataStream &rarr; AllWindowedStream

可以在普通 DataStreams 上定义 Window。 Window 根据某些特征（例如，最近 5 秒内到达的数据）对所有流事件进行分组。请参阅[windows]({{< ref "docs/dev/datastream/operators/windows" >}})获取有关 window 的完整说明。

{{< hint warning >}}

在许多情况下，这是一种非并行转换。所有记录都将收集到 windowAll 算子对应的一个任务中。
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
This feature is not yet supported in Python
{{< /tab >}}
{{< /tabs>}}

<a name="window-apply"></a>

### Window Apply 
#### WindowedStream &rarr; DataStream
#### AllWindowedStream &rarr; DataStream

将通用 function 应用于整个窗口。下面是一个手动对窗口内元素求和的 function。

{{< hint info >}}
如果你使用 windowAll 转换，则需要改用 `AllWindowFunction`。
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

// 在 non-keyed 窗口流上应用 AllWindowFunction
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

// 在 non-keyed 窗口流上应用 AllWindowFunction
allWindowedStream.apply { AllWindowFunction }
```
{{< /tab >}}
{{< tab "Python" >}}
Python 尚不支持此功能
{{< /tab >}}
{{< /tabs>}}

<a name="windowreduce"></a>

### WindowReduce
#### WindowedStream &rarr; DataStream

对窗口应用 reduce function 并返回 reduce 后的值。

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
This feature is not yet supported in Python
{{< /tab >}}
{{< /tabs>}}

<a name="union"></a>

### Union
#### DataStream\* &rarr; DataStream

将两个或多个数据流联合来创建一个包含所有流中数据的新流。注意：如果一个数据流和自身进行联合，这个流中的每个数据将在合并后的流中出现两次。

{{< tabs union >}}
{{< tab "Java" >}}
```java
dataStream.union(otherStream1, otherStream2, ...);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
dataStream.union(otherStream1, otherStream2, ...);
```
{{< /tab >}}
{{< tab "Python" >}}
```python
data_stream.union(otherStream1, otherStream2, ...)
```
{{< /tab >}}
{{< /tabs>}}

<a name="window-join"></a>

### Window Join
#### DataStream,DataStream &rarr; DataStream

根据指定的 key 和窗口 join 两个数据流。

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

<a name="interval-join"></a>

### Interval Join
#### KeyedStream,KeyedStream &rarr; DataStream

将分别属于两个 keyed stream 的元素 e1 和 e2 根据一个共同的 key 和指定的时间范围 Join 在一起，同时满足 `e1.timestamp + lowerBound <= e2.timestamp <= e1.timestamp + upperBound`。

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

<a name="window-cogroup"></a>

### Window CoGroup
#### DataStream,DataStream &rarr; DataStream

根据指定的 key 和窗口将两个数据流组合在一起。

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

<a name="connect"></a>

### Connect
#### DataStream,DataStream &rarr; ConnectedStream

“连接” 两个数据流并保留各自的类型。connect 允许在两个流之间共享状态。

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

<a name="comap-coflatmap"></a>

### CoMap, CoFlatMap
#### ConnectedStream &rarr; DataStream

类似于在连接的数据流上进行 map 和 flatMap。

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

<a name="iterate"></a>

### Iterate
#### DataStream &rarr; IterativeStream &rarr; ConnectedStream

通过将一个算子的输出重定向到某个之前的算子来在流中创建“反馈”循环。这对于定义持续更新模型的算法特别有用。下面的代码从一个流开始，并不断地应用迭代自身。大于 0 的元素被发送回反馈通道，其余元素被转发到下游。

{{< tabs iterate >}}
{{< tab "Java" >}}
```java
IterativeStream<Long> iteration = initialStream.iterate();
DataStream<Long> iterationBody = iteration.map (/*do something*/);
DataStream<Long> feedback = iterationBody.filter(new FilterFunction<Long>(){
    @Override
    public boolean filter(Long value) throws Exception {
        return value > 0;
    }
});
iteration.closeWith(feedback);
DataStream<Long> output = iterationBody.filter(new FilterFunction<Long>(){
    @Override
    public boolean filter(Long value) throws Exception {
        return value <= 0;
    }
});
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
initialStream.iterate {
  iteration => {
    val iterationBody = iteration.map {/*do something*/}
    (iterationBody.filter(_ > 0), iterationBody.filter(_ <= 0))
  }
}
```
{{< /tab >}}
{{< tab "Python" >}}
This feature is not yet supported in Python
{{< /tab >}}
{{< /tabs>}}

<a name="physical-partitioning"></a>

## 物理分区

Flink 也提供以下方法让用户根据需要在数据转换完成后对数据分区进行更细粒度的配置。

<a name="custom-partitioning"></a>

### 自定义分区
#### DataStream &rarr; DataStream

使用用户定义的 Partitioner 为每个元素选择目标任务。

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

<a name="random-partitioning"></a>

### 随机分区
#### DataStream &rarr; DataStream

将元素随机地均匀划分到分区。

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

<a name="rescalling"></a>


### 重新缩放
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

<a name="broadcasting"></a>

### 广播
#### DataStream &rarr; DataStream

将元素广播至所有每个分区 。

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

<a name="broadcasting"></a>

## 算子链和资源组

将两个算子链接在一起能使得它们在同一个线程中执行，从而提升性能。Flink 默认会将能链接的算子尽可能地进行链接(例如， 两个 map 转换操作)。此外， Flink 还提供了对链接更细粒度控制的 API 以满足更多需求：

如果想对整个作业禁用算子链，可以调用 `StreamExecutionEnvironment.disableOperatorChaining()`。下列方法还提供了更细粒度的控制。需要注 意的是， 这些方法只能在 `DataStream` 转换操作后才能被调用，因为它们只对前一次数据转换生效。例如，可以 `someStream.map(...).startNewChain()` 这样调用，而不能 someStream.startNewChain()这样。

一个资源组对应着 Flink 中的一个 slot 槽，更多细节请看slots 槽。 你可以根据需要手动地将各个算子隔离到不同的 slot 中。 

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
