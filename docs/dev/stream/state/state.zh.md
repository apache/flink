---
title: "Working with State"
nav-parent_id: streaming_state
nav-pos: 1
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

本文档主要介绍如何在 Flink 作业中使用状态
* 目录
{:toc}

## Keyed State 与 Operator State

Flink 中有两种基本的状态：`Keyed State` 和 `Operator State`。

### Keyed State

*Keyed State* 通常和 key 相关，仅可使用在 `KeyedStream` 的方法和算子中。

你可以把 Keyed State 看作分区或者共享的 Operator State, 而且每个 key 仅出现在一个分区内。
逻辑上每个 keyed-state 和唯一元组 <算子并发实例, key> 绑定，由于每个 key 仅"属于"
算子的一个并发，因此简化为 <算子, key>。

Keyed State 会按照 *Key Group* 进行管理。Key Group 是 Flink 分发 Keyed State 的最小单元；
Key Group 的数目等于作业的最大并发数。在执行过程中，每个 keyed operator 会对应到一个或多个 Key Group

### Operator State

对于 *Operator State* (或者 *non-keyed state*) 来说，每个 operator state 和一个并发实例进行绑定。
[Kafka Connector]({{ site.baseurl }}/zh/dev/connectors/kafka.html) 是 Flink 中使用 operator state 的一个很好的示例。
每个 Kafka 消费者的并发在 Operator State 中维护一个 topic partition 到 offset 的映射关系。

Operator State 在 Flink 作业的并发改变后，会重新分发状态，分发的策略和 Keyed State 不一样。

## Raw State 与 Managed State

*Keyed State* 和 *Operator State* 分别有两种存在形式：*managed* and *raw*.

*Managed State* 由 Flink 运行时控制的数据结构表示，比如内部的 hash table 或者 RocksDB。
比如 "ValueState", "ListState" 等。Flink runtime 会对这些状态进行编码并写入 checkpoint。

*Raw State* 则保存在算子自己的数据结构中。checkpoint 的时候，Flink 并不知晓具体的内容，仅仅写入一串字节序列到 checkpoint。

所有 datastream 的 function 都可以使用 managed state, 但是 raw state 则只能在实现算子的时候使用。
由于 Flink 可以在修改并发时更好的分发状态数据，并且能够更好的管理内存，因此建议使用 managed state（而不是 raw state）。

<span class="label label-danger">注意</span> 如果你的 managed state 需要定制化的序列化逻辑，
为了后续的兼容性请参考 [相应指南](custom_serialization.html)，Flink 的默认序列化器不需要用户做特殊的处理。

## 使用 Managed Keyed State

managed keyed state 接口提供不同类型状态的访问接口，这些状态都作用于当前输入数据的 key 下。换句话说，这些状态仅可在 `KeyedStream`
上使用，可以通过 `stream.keyBy(...)` 得到 `KeyedStream`.

接下来，我们会介绍不同类型的状态，然后介绍如何使用他们。所有支持的状态类型如下所示：

* `ValueState<T>`: 保存一个可以更新和检索的值（如上所述，每个值都对应到当前的输入数据的 key，因此算子接收到的每个 key 都可能对应一个值）。
这个值可以通过 `update(T)` 进行更新，通过 `T value()` 进行检索。


* `ListState<T>`: 保存一个元素的列表。可以往这个列表中追加数据，并在当前的列表上进行检索。可以通过
 `add(T)` 或者 `addAll(List<T>)` 进行添加元素，通过 `Iterable<T> get()` 获得整个列表。还可以通过 `update(List<T>)` 覆盖当前的列表。

* `ReducingState<T>`: 保存一个单值，表示添加到状态的所有值的聚合。接口与 `ListState` 类似，但使用 `add(T)` 增加元素，会使用提供的 `ReduceFunction` 进行聚合。

* `AggregatingState<IN, OUT>`: 保留一个单值，表示添加到状态的所有值的聚合。和 `ReducingState` 相反的是, 聚合类型可能与 添加到状态的元素的类型不同。
接口与 `ListState` 类似，但使用 `add(IN)` 添加的元素会用指定的 `AggregateFunction` 进行聚合。

* `FoldingState<T, ACC>`: 保留一个单值，表示添加到状态的所有值的聚合。 与 `ReducingState` 相反，聚合类型可能与添加到状态的元素类型不同。 
接口与 `ListState` 类似，但使用`add（T）`添加的元素会用指定的 `FoldFunction` 折叠成聚合值。

* `MapState<UK, UV>`: 维护了一个映射列表。 你可以添加键值对到状态中，也可以获得反映当前所有映射的迭代器。使用 `put(UK，UV)` 或者 `putAll(Map<UK，UV>)` 添加映射。
 使用 `get(UK)` 检索特定 key。 使用 `entries()`，`keys()` 和 `values()` 分别检索映射、键和值的可迭代视图。

所有类型的状态还有一个`clear()` 方法，清除当前 key 下的状态数据，也就是当前输入元素的 key。

<span class="label label-danger">注意</span> `FoldingState` 和 `FoldingStateDescriptor` 从 Flink 1.4 开始就已经被启用，将会在未来被删除。
作为替代请使用 `AggregatingState` 和 `AggregatingStateDescriptor`。

请牢记，这些状态对象仅用于与状态交互。状态本身不一定存储在内存中，还可能在磁盘或其他位置。
另外需要牢记的是从状态中获取的值取决于输入元素所代表的 key。 因此，在不同 key 上调用同一个接口，可能得到不同的值。

你必须创建一个 `StateDescriptor`，才能得到对应的状态句柄。 这保存了状态名称（正如我们稍后将看到的，你可以创建多个状态，并且它们必须具有唯一的名称以便可以引用它们），
状态所持有值的类型，并且可能包含用户指定的函数，例如`ReduceFunction`。 根据不同的状态类型，可以创建`ValueStateDescriptor`，`ListStateDescriptor`，
`ReducingStateDescriptor`，`FoldingStateDescriptor` 或 `MapStateDescriptor`。

状态通过 `RuntimeContext` 进行访问，因此只能在 *rich functions* 中使用。请参阅[这里]({{site.baseurl}}/zh/dev/api_concepts.html#rich-functions)获取相关信息，
但是我们很快也会看到一个例子。`RichFunction` 中 `RuntimeContext` 提供如下方法：

* `ValueState<T> getState(ValueStateDescriptor<T>)`
* `ReducingState<T> getReducingState(ReducingStateDescriptor<T>)`
* `ListState<T> getListState(ListStateDescriptor<T>)`
* `AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT>)`
* `FoldingState<T, ACC> getFoldingState(FoldingStateDescriptor<T, ACC>)`
* `MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV>)`

下面是一个 `FlatMapFunction` 的例子，展示了如何将这些部分组合起来：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    /**
     * The ValueState handle. The first field is the count, the second field a running sum.
     */
    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

        // access the state value
        Tuple2<Long, Long> currentSum = sum.value();

        // update the count
        currentSum.f0 += 1;

        // add the second field of the input value
        currentSum.f1 += input.f1;

        // update the state
        sum.update(currentSum);

        // if the count reaches 2, emit the average and clear the state
        if (currentSum.f0 >= 2) {
            out.collect(new Tuple2<>(input.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
                        Tuple2.of(0L, 0L)); // default value of the state, if nothing was set
        sum = getRuntimeContext().getState(descriptor);
    }
}

// this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
        .keyBy(0)
        .flatMap(new CountWindowAverage())
        .print();

// the printed output will be (1,4) and (1,5)
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
class CountWindowAverage extends RichFlatMapFunction[(Long, Long), (Long, Long)] {

  private var sum: ValueState[(Long, Long)] = _

  override def flatMap(input: (Long, Long), out: Collector[(Long, Long)]): Unit = {

    // access the state value
    val tmpCurrentSum = sum.value

    // If it hasn't been used before, it will be null
    val currentSum = if (tmpCurrentSum != null) {
      tmpCurrentSum
    } else {
      (0L, 0L)
    }

    // update the count
    val newSum = (currentSum._1 + 1, currentSum._2 + input._2)

    // update the state
    sum.update(newSum)

    // if the count reaches 2, emit the average and clear the state
    if (newSum._1 >= 2) {
      out.collect((input._1, newSum._2 / newSum._1))
      sum.clear()
    }
  }

  override def open(parameters: Configuration): Unit = {
    sum = getRuntimeContext.getState(
      new ValueStateDescriptor[(Long, Long)]("average", createTypeInformation[(Long, Long)])
    )
  }
}


object ExampleCountWindowAverage extends App {
  val env = StreamExecutionEnvironment.getExecutionEnvironment

  env.fromCollection(List(
    (1L, 3L),
    (1L, 5L),
    (1L, 7L),
    (1L, 4L),
    (1L, 2L)
  )).keyBy(_._1)
    .flatMap(new CountWindowAverage())
    .print()
  // the printed output will be (1,4) and (1,5)

  env.execute("ExampleManagedState")
}
{% endhighlight %}
</div>
</div>

这个例子实现了一个简单的计数窗口。 我们把元组的第一个元素当作 key（在示例中都 key 都是 "1"）。 该函数将出现的次数以及总和存储在 "ValueState" 中。 
一旦出现次数达到 2，则将平均值发送到下游，并清除状态重新开始。 请注意，我们会为每个不同的 key（元组中第一个元素）保存一个单独的值。

### 状态有效期 (TTL)

任何类型的 keyed state 都可以有 *有效期* (TTL)。如果配置了 TTL 且状态值已过期，则会尽最大可能清除对应的值，这会在后面详述。

所有状态类型都支持单元素的 TTL。 这意味着列表元素和映射元素将独立到期。

在使用状态 TTL 前，需要先构建一个`StateTtlConfig` 配置对象。 然后把配置传递到 state descriptor 中启用 TTL 功能：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;

StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build();
    
ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("text state", String.class);
stateDescriptor.enableTimeToLive(ttlConfig);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.common.time.Time

val ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build
    
val stateDescriptor = new ValueStateDescriptor[String]("text state", classOf[String])
stateDescriptor.enableTimeToLive(ttlConfig)
{% endhighlight %}
</div>
</div>

TTL 配置有以下几个选项：
`newBuilder` 的第一个参数表示数据的有效期，是必选项。

TTL 的更新策略（默认是 `OnCreateAndWrite`）：

 - `StateTtlConfig.UpdateType.OnCreateAndWrite` - 仅在创建和写入时更新
 - `StateTtlConfig.UpdateType.OnReadAndWrite` - 读取时也更新
 
数据在过期但还未被清理时的可见性配置如下（默认为 `NeverReturnExpired`):

 - `StateTtlConfig.StateVisibility.NeverReturnExpired` - 不返回过期数据
 - `StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp` - 会返回过期但未清理的数据
 
`NeverReturnExpired` 情况下，过期数据就像不存在一样，不管是否被物理删除。这对于不能访问过期数据的场景下非常有用，比如敏感数据。
`ReturnExpiredIfNotCleanedUp` 在数据被物理删除前都会返回。

**注意:** 

- 状态上次的修改时间会和数据一起保存在 state backend 中，因此开启该特性会增加状态数据的存储。
Heap state backend 会额外存储一个包括用户状态以及时间戳的 Java 对象，RocksDB state backend 会在每个状态值（list 或者 map 的每个元素）序列化后增加 8 个字节。 

- 暂时只支持基于 *processing time* 的 TTL。

- 尝试从 checkpoint/savepoint 进行恢复时，TTL 的状态（是否开启）必须和之前保持一致，否则会遇到 "StateMigrationException"。

- TTL 的配置并不会保存在 checkpoint/savepoint 中，仅对当前 Job 有效。

- 当前开启 TTL 的 map state 仅在用户值序列化器支持 null 的情况下，才支持用户值为 null。如果用户值序列化器不支持 null，
可以用 `NullableSerializer` 包装一层。

#### 过期数据的清理

默认情况下，仅在用户读取过期数据时才会删除过对应的状态，例如调用 `ValueState.value()`。

<span class="label label-danger">注意</span> 默认情况下，如果不显示读取过期数据，则不会进行删除，可能导致状态持续增加。这种行为在未来可能会进行改变。

##### 全量快照时清理

此外，你可以在进行全量快照时进行清理，这将减少快照的大小。 当前实现中不会清除本地状态，但是在从先前快照恢复时则会移除过期数据。可以通过 `StateTtlConfig` 配置：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .cleanupFullSnapshot()
    .build();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.time.Time

val ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .cleanupFullSnapshot
    .build
{% endhighlight %}
</div>
</div>

这个配置对 RocksDB 增量 checkpoint 无效。

**注意:**
- 这种清理方式可以在任何时候通过  `StateTtlConfig` 启用或者关闭，比如在从 savepoint 恢复时。

#### Cleanup in background

Besides cleanup in full snapshot, you can also activate the cleanup in background. The following option
will activate a default background cleanup in StateTtlConfig if it is supported for the used backend:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.api.common.state.StateTtlConfig;
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .cleanupInBackground()
    .build();
{% endhighlight %}
</div>
 <div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.common.state.StateTtlConfig
val ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .cleanupInBackground
    .build
{% endhighlight %}
</div>
</div>

For more fine-grained control over some special cleanup in background, you can configure it separately as described below.
Currently, heap state backend relies on incremental cleanup and RocksDB backend uses compaction filter for background cleanup.

##### 增量数据清理

另外可以选择增量式清理状态数据，在状态访问或/和处理时进行。如果某个状态开启了该清理策略，则会在存储后端保留一个所有状态的惰性全局迭代器。
每次触发增量清理时，从迭代器中选择已经过期的数进行清理。

该特性可以通过 `StateTtlConfig` 进行启用：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.api.common.state.StateTtlConfig;
 StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .cleanupIncrementally(10, true)
    .build();
{% endhighlight %}
</div>
 <div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.common.state.StateTtlConfig
val ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .cleanupIncrementally(10, true)
    .build
{% endhighlight %}
</div>
</div>

该策略有两个参数。 第一个是每次清理时检查状态的条目数。如果启用，则始终按每个状态访问触发。第二个参数表示是否在处理每条记录时触发清理。
If you enable the default background cleanup then this strategy will be activated for heap backend with 5 checked entries and without cleanup per record processing.

**注意:**
- 如果没有 state 访问，也没有处理数据，则不会清理过期数据。
- 增量清理会增加数据处理的耗时。
- 现在仅 Heap state backend 支持增量清除机制。在 RocksDB state backend 上启用该特性无效。
- 如果 Heap state backend 使用同步快照方式，则会保存一份所有 key 的拷贝，从而防止并发修改问题，因此会增加内存的使用。但异步快照则没有这个问题。
- 对已有的作业，这个清理方式可以在任何时候通过 `StateTtlConfig` 启用或禁用该特性，比如从 savepoint 重启后。

##### 在 RocksDB 压缩时清理

如果使用 RocksDB state backend，还支持 Flink 为 RocksDB 定制的压缩过滤器。RocksDB 会周期性的对数据进行合并压缩从而减少存储空间。
Flink 压缩过滤器会在压缩时过滤掉已经过期的状态数据。

该特性默认是关闭的，可以通过 Flink 的配置项 `state.backend.rocksdb.ttl.compaction.filter.enabled` 或者调用 `RocksDBStateBackend::enableTtlCompactionFilter`
启用该特性。然后通过如下方式让任何具有 TTL 配置的状态使用过滤器：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.api.common.state.StateTtlConfig;

StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .cleanupInRocksdbCompactFilter(1000)
    .build();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.common.state.StateTtlConfig

val ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .cleanupInRocksdbCompactFilter(1000)
    .build
{% endhighlight %}
</div>
</div>

RocksDB compaction filter will query current timestamp, used to check expiration, from Flink every time
after processing certain number of state entries.
You can change it and pass a custom value to
`StateTtlConfig.newBuilder(...).cleanupInRocksdbCompactFilter(long queryTimeAfterNumEntries)` method.
Updating the timestamp more often can improve cleanup speed
but it decreases compaction performance because it uses JNI call from native code.
If you enable the default background cleanup then this strategy will be activated for RocksDB backend and the current timestamp will be queried each time 1000 entries have been processed.

你还可以通过配置开启 RocksDB 过滤器的 debug 日志：
`log4j.logger.org.rocksdb.FlinkCompactionFilter=DEBUG`

**注意:**
- 压缩时调用 TTL 过滤器会降低速度。TTL 过滤器需要解析上次访问的时间戳，并对每个将参与压缩的状态进行是否过期检查。
对于集合型状态类型（比如 list 和 map），会对集合中每个元素进行检查。
- 对于元素序列化后长度不固定的列表状态，TTL 过滤器需要在每次 JNI 调用过程中，额外调用 Flink 的 java 序列化器，
从而确定下一个未过期数据的位置。
- 对已有的作业，这个清理方式可以在任何时候通过 `StateTtlConfig` 启用或禁用该特性，比如从 savepoint 重启后。

### DataStream 状态相关的 Scala API 

除了上面描述的接口之外，Scala API 还在 `KeyedStream` 上对 `map()` 和 `flatMap()` 访问 `ValueState` 提供了一个更便捷的接口。 
用户函数能够通过 `Option` 获取当前 `ValueState` 的值，并且返回即将保存到状态的值。

{% highlight scala %}
val stream: DataStream[(String, Int)] = ...

val counts: DataStream[(String, Int)] = stream
  .keyBy(_._1)
  .mapWithState((in: (String, Int), count: Option[Int]) =>
    count match {
      case Some(c) => ( (in._1, c), Some(c + in._2) )
      case None => ( (in._1, 0), Some(in._2) )
    })
{% endhighlight %}

## 使用 Managed Operator State

用户可以通过实现 `CheckpointedFunction` 或 `ListCheckpointed<T extends Serializable>` 接口来使用 managed operator state。

#### CheckpointedFunction

`CheckpointedFunction` 接口提供了访问 non-keyed state 的方法，需要实现如下两个方法：

{% highlight java %}
void snapshotState(FunctionSnapshotContext context) throws Exception;

void initializeState(FunctionInitializationContext context) throws Exception;
{% endhighlight %}

进行 checkpoint 时会调用 `snapshotState()`。 用户自定义函数初始化时会调用 `initializeState()`，初始化包括第一次自定义函数初始化和从之前的 checkpoint 恢复。
因此 `initializeState()` 不仅是定义不同状态类型初始化的地方，也需要包括状态恢复的逻辑。

当前，managed operator state 以 list 的形式存在。这些状态是一个 *可序列化* 对象的集合 `List`，彼此独立，方便在改变并发后进行状态的重新分派。
换句话说，这些对象是重新分配 non-keyed state 的最细粒度。根据状态的不同访问方式，有如下几种重新分配的模式：

  - **Even-split redistribution:** 每个算子都保存一个列表形式的状态集合，整个状态由所有的列表拼接而成。当作业恢复或重新分配的时候，整个状态会按照算子的并发度进行均匀分配。
    比如说，算子 A 的并发读为 1，包含两个元素 `element1` 和 `element2`，当并发读增加为 2 时，`element1` 会被分到并发 0 上，`element2` 则会被分到并发 1 上。

  - **Union redistribution:** 每个算子保存一个列表形式的状态集合。整个状态由所有的列表拼接而成。当作业恢复或重新分配时，每个算子都将获得所有的状态数据。

下面的例子中的 `SinkFunction` 在 `CheckpointedFunction` 中进行数据缓存，然后统一发送到下游，这个例子演示了列表状态数据的 event-split redistribution。 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class BufferingSink
        implements SinkFunction<Tuple2<String, Integer>>,
                   CheckpointedFunction {

    private final int threshold;

    private transient ListState<Tuple2<String, Integer>> checkpointedState;

    private List<Tuple2<String, Integer>> bufferedElements;

    public BufferingSink(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context contex) throws Exception {
        bufferedElements.add(value);
        if (bufferedElements.size() == threshold) {
            for (Tuple2<String, Integer> element: bufferedElements) {
                // send it to the sink
            }
            bufferedElements.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.clear();
        for (Tuple2<String, Integer> element : bufferedElements) {
            checkpointedState.add(element);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> descriptor =
            new ListStateDescriptor<>(
                "buffered-elements",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            for (Tuple2<String, Integer> element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
class BufferingSink(threshold: Int = 0)
  extends SinkFunction[(String, Int)]
    with CheckpointedFunction {

  @transient
  private var checkpointedState: ListState[(String, Int)] = _

  private val bufferedElements = ListBuffer[(String, Int)]()

  override def invoke(value: (String, Int), context: Context): Unit = {
    bufferedElements += value
    if (bufferedElements.size == threshold) {
      for (element <- bufferedElements) {
        // send it to the sink
      }
      bufferedElements.clear()
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    checkpointedState.clear()
    for (element <- bufferedElements) {
      checkpointedState.add(element)
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    val descriptor = new ListStateDescriptor[(String, Int)](
      "buffered-elements",
      TypeInformation.of(new TypeHint[(String, Int)]() {})
    )

    checkpointedState = context.getOperatorStateStore.getListState(descriptor)

    if(context.isRestored) {
      for(element <- checkpointedState.get()) {
        bufferedElements += element
      }
    }
  }

}
{% endhighlight %}
</div>
</div>

`initializeState` 方法接收一个 `FunctionInitializationContext` 参数，会用来初始化 non-keyed state 的 "容器"。这些容器是一个 `ListState`
用于在 checkpoint 时保存 non-keyed state 对象。

注意这些状态是如何初始化的，和 keyed state 类系，`StateDescriptor` 会包括状态名字、以及状态类型相关信息。


<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ListStateDescriptor<Tuple2<String, Integer>> descriptor =
    new ListStateDescriptor<>(
        "buffered-elements",
        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));

checkpointedState = context.getOperatorStateStore().getListState(descriptor);
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}

val descriptor = new ListStateDescriptor[(String, Long)](
    "buffered-elements",
    TypeInformation.of(new TypeHint[(String, Long)]() {})
)

checkpointedState = context.getOperatorStateStore.getListState(descriptor)

{% endhighlight %}
</div>
</div>

调用不同的获取状态对象的接口，会使用不同的状态分配算法。比如 `getUnionListState(descriptor)` 会使用 union redistribution 算法，
而 `getListState(descriptor)` 则简单的使用 even-split redistribution 算法。

当初始化好状态对象后，我们通过 `isRestored()` 方法判断是否从之前的故障中恢复回来，如果该方法返回 `true` 则表示从故障中进行恢复，会执行接下来的恢复逻辑。

正如代码所示，`BufferingSink` 中初始化时，恢复回来的 `ListState` 的所有元素会添加到一个局部变量中，供下次 `snapshotState()` 时使用。
然后清空 `ListState`，再把当前局部变量中的所有元素写入到 checkpoint 中。

另外，我们同样可以在 `initializeState()` 方法中使用 `FunctionInitializationContext` 初始化 keyed state。

#### ListCheckpointed

`ListCheckpointed` 接口是 `CheckpointedFunction` 的精简版，仅支持 even-split redistributuion 的 list state。同样需要实现两个方法：

{% highlight java %}
List<T> snapshotState(long checkpointId, long timestamp) throws Exception;

void restoreState(List<T> state) throws Exception;
{% endhighlight %}

`snapshotState()` 需要返回一个将写入到 checkpoint 的对象列表，`restoreState` 则需要处理恢复回来的对象列表。如果状态不可切分，
则可以在 `snapshotState()` 中返回 `Collections.singletonList(MY_STATE)`。

### 带状态的 Source Function

带状态的数据源比其他的算子需要注意更多东西。为了保证更新状态以及输出的原子性（用于支持 exactly-once 语义），用户需要在发送数据前获取数据源的全局锁。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public static class CounterSource
        extends RichParallelSourceFunction<Long>
        implements ListCheckpointed<Long> {

    /**  current offset for exactly once semantics */
    private Long offset = 0L;

    /** flag for job cancellation */
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) {
        final Object lock = ctx.getCheckpointLock();

        while (isRunning) {
            // output and state update are atomic
            synchronized (lock) {
                ctx.collect(offset);
                offset += 1;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public List<Long> snapshotState(long checkpointId, long checkpointTimestamp) {
        return Collections.singletonList(offset);
    }

    @Override
    public void restoreState(List<Long> state) {
        for (Long s : state)
            offset = s;
    }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
class CounterSource
       extends RichParallelSourceFunction[Long]
       with ListCheckpointed[Long] {

  @volatile
  private var isRunning = true

  private var offset = 0L

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    val lock = ctx.getCheckpointLock

    while (isRunning) {
      // output and state update are atomic
      lock.synchronized({
        ctx.collect(offset)

        offset += 1
      })
    }
  }

  override def cancel(): Unit = isRunning = false

  override def restoreState(state: util.List[Long]): Unit =
    for (s <- state) {
      offset = s
    }

  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Long] =
    Collections.singletonList(offset)

}
{% endhighlight %}
</div>
</div>

希望订阅 checkpoint 成功消息的算子，可以参考 `org.apache.flink.runtime.state.CheckpointListener` 接口。

{% top %}
