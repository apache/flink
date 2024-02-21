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

# 算子

用户通过算子能将一个或多个 DataStream 转换成新的 DataStream，在应用程序中可以将多个数据转换算子合并成一个复杂的数据流拓扑。

这部分内容将描述 Flink DataStream API 中基本的数据转换 API，数据转换后各种数据分区方式，以及算子的链接策略。

## 数据流转换

### Map
#### DataStream &rarr; DataStream

输入一个元素同时输出一个元素。下面是将输入流中元素数值加倍的 map function：

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

输入一个元素同时产生零个、一个或多个元素。下面是将句子拆分为单词的 flatmap function：

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

为每个元素执行一个布尔 function，并保留那些 function 输出值为 true 的元素。下面是过滤掉零值的 filter：

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

在逻辑上将流划分为不相交的分区。具有相同 key 的记录都分配到同一个分区。在内部， _keyBy()_  是通过哈希分区实现的。有多种[指定 key ]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#keyed-datastream)的方式。

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
以下情况，一个类**不能作为 key**：

1. 它是一种 POJO 类，但没有重写 hashCode() 方法而是依赖于 Object.hashCode() 实现。
2. 它是任意类的数组。
{{< /hint >}}

### Reduce
#### KeyedStream &rarr; DataStream

在相同 key 的数据流上“滚动”执行 reduce。将当前元素与最后一次 reduce 得到的值组合然后输出新值。

下面是创建局部求和流的 reduce function：

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

可以在已经分区的 KeyedStreams 上定义 Window。Window 根据某些特征（例如，最近 5 秒内到达的数据）对每个 key Stream 中的数据进行分组。请参阅 [windows]({{< ref "docs/dev/datastream/operators/windows" >}}) 获取有关 window 的完整说明。

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

可以在普通 DataStream 上定义 Window。 Window 根据某些特征（例如，最近 5 秒内到达的数据）对所有流事件进行分组。请参阅[windows]({{< ref "docs/dev/datastream/operators/windows" >}})获取有关 window 的完整说明。

{{< hint warning >}}
这适用于非并行转换的大多数场景。所有记录都将收集到 windowAll 算子对应的一个任务中。
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

# 在 non-keyed 窗口流上应用 AllWindowFunction
all_windowed_stream.apply(MyAllWindowFunction())
```
{{< /tab >}}
{{< /tabs>}}

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

将两个或多个数据流联合来创建一个包含所有流中数据的新流。注意：如果一个数据流和自身进行联合，这个流中的每个数据将在合并后的流中出现两次。

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
Python 中尚不支持此特性。
{{< /tab >}}
{{< /tabs>}}

### Interval Join
#### KeyedStream,KeyedStream &rarr; DataStream

根据 key 相等并且满足指定的时间范围内（`e1.timestamp + lowerBound <= e2.timestamp <= e1.timestamp + upperBound`）的条件将分别属于两个 keyed stream 的元素 e1 和 e2  Join 在一起。

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
Python 中尚不支持此特性。
{{< /tab >}}
{{< /tabs>}}

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
Python 中尚不支持此特性。
{{< /tab >}}
{{< /tabs>}}

### Connect
#### DataStream,DataStream &rarr; ConnectedStream

“连接” 两个数据流并保留各自的类型。connect 允许在两个流的处理逻辑之间共享状态。

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

### Cache
#### DataStream &rarr; CachedDataStream

把算子的结果缓存起来。目前只支持批执行模式下运行的作业。算子的结果在算子第一次执行的时候会被缓存起来，之后的
作业中会复用该算子缓存的结果。如果算子的结果丢失了，它会被原来的算子重新计算并缓存。

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

## 物理分区

Flink 也提供以下方法让用户根据需要在数据转换完成后对数据分区进行更细粒度的配置。

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


### Rescaling
#### DataStream &rarr; DataStream

将元素以 Round-robin 轮询的方式分发到下游算子。如果你想实现数据管道，这将很有用，例如，想将数据源多个并发实例的数据分发到多个下游 map 来实现负载分配，但又不想像 rebalance() 那样引起完全重新平衡。该算子将只会到本地数据传输而不是网络数据传输，这取决于其它配置值，例如 TaskManager 的 slot 数量。

上游算子将元素发往哪些下游的算子实例集合同时取决于上游和下游算子的并行度。例如，如果上游算子并行度为 2，下游算子的并发度为 6， 那么上游算子的其中一个并行实例将数据分发到下游算子的三个并行实例， 另外一个上游算子的并行实例则将数据分发到下游算子的另外三个并行实例中。再如，当下游算子的并行度为2，而上游算子的并行度为 6 的时候，那么上游算子中的三个并行实例将会分发数据至下游算子的其中一个并行实例，而另外三个上游算子的并行实例则将数据分发至另下游算子的另外一个并行实例。

当算子的并行度不是彼此的倍数时，一个或多个下游算子将从上游算子获取到不同数量的输入。

请参阅下图来可视化地感知上述示例中的连接模式：

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

### 广播
#### DataStream &rarr; DataStream

将元素广播到每个分区 。

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

<a name="task-chaining-and-resource-groups"></a>

## 算子链和资源组

将两个算子链接在一起能使得它们在同一个线程中执行，从而提升性能。Flink 默认会将能链接的算子尽可能地进行链接(例如， 两个 map 转换操作)。此外， Flink 还提供了对链接更细粒度控制的 API 以满足更多需求：

如果想对整个作业禁用算子链，可以调用 `StreamExecutionEnvironment.disableOperatorChaining()`。下列方法还提供了更细粒度的控制。需要注意的是，这些方法只能在 `DataStream` 转换操作后才能被调用，因为它们只对前一次数据转换生效。例如，可以 `someStream.map(...).startNewChain()` 这样调用，而不能 `someStream.startNewChain()` 这样。

一个资源组对应着 Flink 中的一个 slot 槽，更多细节请看 slots 。 你可以根据需要手动地将各个算子隔离到不同的 slot 中。 


### 创建新链

基于当前算子创建一个新的算子链。  
后面两个 map 将被链接起来，而 filter 和第一个 map 不会链接在一起。

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

### 禁止链接

禁止和 map 算子链接在一起。

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

### 配置 Slot 共享组

为某个算子设置 slot 共享组。Flink 会将同一个 slot 共享组的算子放在同一个 slot 中，而将不在同一 slot 共享组的算子保留在其它 slot 中。这可用于隔离 slot 。如果所有输入算子都属于同一个 slot 共享组，那么 slot 共享组从将继承输入算子所在的 slot。slot 共享组的默认名称是 “default”，可以调用 slotSharingGroup(“default”) 来显式地将算子放入该组。

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

## 名字和描述

Flink里的算子和作业节点会有一个名字和一个描述。名字和描述。名字和描述都是用来介绍一个算子或者节点是在做什么操作，但是他们会被用在不同地方。

名字会用在用户界面、线程名、日志、指标等场景。节点的名字会根据节点中算子的名字来构建。
名字需要尽可能的简洁，避免对外部系统产生大的压力。

描述主要用在执行计划展示，以及用户界面展示。节点的描述同样是根据节点中算子的描述来构建。
描述可以包括详细的算子行为的信息，以便我们在运行时进行debug分析。

{{< tabs namedescription>}}
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

节点的描述默认是按照一个多行的树形结构来构建的，用户可以通过把`pipeline.vertex-description-mode`设为`CASCADING`, 实现将描述改为老版本的单行递归模式。

Flink SQL框架生成的算子默认会有一个由算子的类型以及id构成的名字，以及一个带有详细信息的描述。
用户可以通过将`table.exec.simplify-operator-name-enabled`设为`false`，将名字改为和以前的版本一样的详细描述。

当一个作业的拓扑很复杂时，用户可以把`pipeline.vertex-name-include-index-prefix`设为`true`，在节点的名字前增加一个拓扑序的前缀，这样就可以很容易根据指标以及日志的信息快速找到拓扑图中对应节点。

