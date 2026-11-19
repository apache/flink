---
title: "Working with State V2"
weight: 2
type: docs
aliases:
  - /dev/stream/state/state_v2.html
  - /apis/streaming/state_v2.html
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

# 使用 State V2（新的 API）

本章节你将了解 Flink 用于编写有状态程序的新 API。要了解有状态流处理背后的概念，请参阅 [Stateful Stream
Processing]({{< ref "docs/concepts/stateful-stream-processing" >}})。

新的 state API 比以前的 API 更灵活，用户可以通过新的 API 使用异步状态操作，是一套更强大、更高效的 API。
异步状态访问对 state backend 来说是必要的，它使 state backend 能够处理大状态，并在必要时将数据溢出到远程文件系统。
这被称为存算分离（disaggregated state management）。更多关于存算分离的信息，请参阅
[Disaggregated State Management]({{< ref "docs/ops/state/disaggregated_state" >}})。

## Keyed DataStream

如果你希望使用 keyed state，首先需要为 `DataStream` 指定 key（主键）。这个主键用于状态分区（也会给数据流中的记录本身分区）。
你可以使用 `DataStream` 中 Java API 的 `keyBy(KeySelector)` 来指定 key。它将生成 `KeyedStream`，接下来允许使用 keyed state 操作。
你需要在 `KeyedStream` 上调用 `enableAsyncState()` 来开启异步状态操作。

Key selector 函数接收单条记录作为输入，返回这条记录的 key。该 key 可以为任何类型，但是它的计算产生方式**必须**是具备确定性的。

Flink 的数据模型不基于 key-value 对，因此实际上将数据集在物理上封装成 key 和 value 是没有必要的。
Key 是“虚拟”的。它们定义为基于实际数据的函数，用以操纵分组算子。

下面的例子展示了 key selector 函数。它仅返回了对象当中的字段。

{{< tabs "54e3bde7-659a-4683-81fa-312a2c81036b" >}}
{{< tab "Java" >}}
```java
// some ordinary POJO
public class WordCount {
  public String word;
  public int count;

  public String getWord() { return word; }
}
DataStream<WordCount> words = // [...]
KeyedStream<WordCount> keyed = words
  .keyBy(WordCount::getWord).enableAsyncState();
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

<a name="using-keyed-state-v2"></a>

## 使用 Keyed State V2

和之前的 state API 不同，新的 state API 是为异步状态访问而设计的。每种类型的状态都提供了两种版本的 API：同步和异步。
同步 API 是阻塞式的 API，会等待状态访问完成。异步 API 是非阻塞的，它会返回一个 `StateFuture`，这个 `StateFuture` 会在状态访问完成时完成。
在 `StateFuture` 完成时，用户的回调或后续逻辑（如果有）会被执行。异步 API 比同步 API 更加高效，我们更推荐使用异步 API。
需要注意，**不建议**在同一个用户函数中混合使用同步和异步状态访问。

Keyed state 接口提供了对不同类型状态的访问，这些状态都作用于当前输入元素的 key 下。这意味着这类状态只能在 `KeyedStream` 上使用，
在 Java 中可以通过 `stream.keyBy(…)` 创建 `KeyedStream`。最重要的是，需要在 `KeyedStream` 上调用 `enableAsyncState()` 来开启异步状态访问。
新的 API 只有在调用了 `enableAsyncState()` 的 `KeyedStream` 上才可用。

接下来，我们会介绍不同类型的状态，然后介绍如何在程序中使用它们。由于同步 API 与之前的 API 是一样的，这里只关注异步 API。

<a name="the-return-values"></a>

### 返回值

首先，我们需要熟悉异步状态访问方法的返回值。

`StateFuture<T>` 是一个 future，会在状态访问完成时以状态访问的结果完成，结果类型是 T。它提供了多个方法来处理结果，包括：
* `StateFuture<Void> thenAccept(Consumer<T>)`：该方法接收一个 `Consumer` 作为参数，`Consumer` 会在状态访问完成时以结果作为参数被调用。
  它返回一个 `StateFuture<Void>`，在 `Consumer` 执行结束时完成。
* `StateFuture<R> thenApply(Function<T, R>)`：该方法接收一个 `Function` 作为参数，`Function` 会在状态访问完成时以结果作为参数被调用。
  `Function` 的返回值会作为后续 `StateFuture` 的结果，该 `StateFuture` 在 `Function` 执行结束时完成。
* `StateFuture<R> thenCompose(Function<T, StateFuture<R>>)`：该方法接收一个 `Function` 作为参数，`Function` 会在状态访问完成时以结果作为参数被调用。
  `Function` 的返回值应该是一个 `StateFuture<R>`，它会作为 `thenCompose` 的返回值提供给调用者。
  该 `StateFuture<R>` 会在 `Function` 内部的 `StateFuture<R>` 完成时完成。
* `StateFuture<R> thenCombine(StateFuture<U>, BiFunction<T, U, R>)`：该方法接收另一个 `StateFuture` 和一个 `BiFunction` 作为参数，
  `BiFunction` 会在两个 `StateFuture` 都完成时以两者的结果作为参数被调用。`BiFunction` 的返回值会作为后续 `StateFuture` 的结果，
  该 `StateFuture` 在 `BiFunction` 执行结束时完成。

以上这些方法类似于 `CompletableFuture` 中的对应方法。除此之外，需要注意 `StateFuture` 不提供 `get()` 方法来阻塞当前线程直到状态访问完成，
这是因为阻塞当前线程可能会导致递归阻塞。`StateFuture<T>` 还提供了 `thenAccept`、`thenApply`、`thenCompose` 和 `thenCombine` 的条件版本，
用于状态访问完成后，后续逻辑需要根据状态访问的结果分成两个分支的情况。这些方法的条件版本是 `thenConditionallyAccept`、
`thenConditionallyApply`、`thenConditionallyCompose` 和 `thenConditionallyCombine`。

`StateIterator<T>` 是一个迭代器，可以用来迭代状态中的元素。它提供了以下方法：
* `boolean isEmpty()`：一个同步方法，如果迭代器中没有元素则返回 `true`，否则返回 `false`。
* `StateFuture<Void> onNext(Consumer<T>)`：该方法接收一个 `Consumer` 作为参数，`Consumer` 会在状态访问完成时以下一个元素作为参数被调用。
  它返回一个 `StateFuture<Void>`，在 `Consumer` 执行结束时完成。
  `onNext` 也提供了函数版本，即 `StateFuture<Collection<R>> onNext(Function<T, R>)`。该方法接收一个 `Function` 作为参数，
  `Function` 会在状态访问完成时以下一个元素作为参数被调用。`Function` 的返回值会被收集起来，作为后续 `StateFuture` 结果中的集合返回，
  该 `StateFuture` 在 `Function` 执行结束时完成。

我们还提供了 `StateFutureUtils` 类，其中包含一些用于处理 `StateFuture` 的工具方法：
* `StateFuture<T> completedFuture(T)`：该方法返回一个以给定值完成的 `StateFuture`。当你希望在 `thenCompose` 方法中返回一个常量值用于后续处理时，
  这个方法非常有用。
* `StateFuture<Void> completedVoidFuture()`：该方法返回一个以 `null` 值完成的 `StateFuture`，是 `completedFuture` 的 void 值版本。
* `StateFuture<Collection<T>> combineAll(Collection<StateFuture<T>>)`：该方法接收一个 `StateFuture` 的集合作为参数，
  返回一个 `StateFuture`，该 `StateFuture` 在所有输入的 `StateFuture` 都完成后完成，其结果是所有输入 `StateFuture` 结果的集合。
  当你希望组合多个 `StateFuture` 的结果时，这个方法非常有用。
* `StateFuture<Iterable<T>> toIterable(StateFuture<StateIterator<T>>)`：该方法接收一个结果为 `StateIterator` 的 `StateFuture` 作为参数，
  返回一个结果为 `Iterable` 的 `StateFuture`，该 `Iterable` 包含了 `StateIterator` 中的所有元素。
  当你希望把 `StateIterator` 转换为 `Iterable` 时，这个方法非常有用。
  但这可能会禁用延迟加载的功能，如果没有充分理由这样做，建议不这样使用。本方法只有当下一步的计算依赖于迭代器的全部数据时才有用。

<a name="state-primitives"></a>

### 状态原语

所有支持的状态类型如下所示：

* `ValueState<T>`：保存一个可以更新和检索的值（如上所述，每个值都对应到当前输入数据的 key，因此算子接收到的每个 key 都可能对应一个值）。
这个值可以通过 `asyncUpdate(T)` 进行更新，通过 `StateFuture<T> asyncValue()` 进行检索。

* `ListState<T>`：保存一个元素的列表。可以往这个列表中追加数据，并获取当前存储的所有元素的 `StateIterator`。
可以通过 `asyncAdd(T)` 或者 `asyncAddAll(List<T>)` 添加元素，通过 `StateFuture<StateIterator<T>> asyncGet()` 获得整个列表。
还可以通过 `asyncUpdate(List<T>)` 覆盖当前的列表。

* `ReducingState<T>`：保存一个单值，表示添加到状态的所有值的聚合。接口与 `ListState` 类似，但使用 `asyncAdd(T)` 增加的元素，
会使用指定的 `ReduceFunction` 进行聚合。

* `AggregatingState<IN, OUT>`：保存一个单值，表示添加到状态的所有值的聚合。和 `ReducingState` 不同的是，聚合类型可能与添加到状态的元素的类型不同。
接口与 `ListState` 相同，但使用 `asyncAdd(IN)` 添加的元素会用指定的 `AggregateFunction` 进行聚合。

* `MapState<UK, UV>`：维护了一个映射列表。你可以添加键值对到状态中，也可以获取当前存储的所有映射的 `StateIterator`。
使用 `asyncPut(UK, UV)` 或者 `asyncPutAll(Map<UK, UV>)` 添加映射，使用 `asyncGet(UK)` 检索特定 key 对应的值。
使用 `asyncEntries()`、`asyncKeys()` 和 `asyncValues()` 分别检索映射、键和值的可迭代视图。你还可以通过 `asyncIsEmpty()` 来判断是否包含任何键值对。

所有类型的状态还有一个 `asyncClear()` 方法，清除当前 key 下的状态数据，也就是当前输入元素的 key。

请牢记，这些状态对象仅用于与状态交互。状态本身不一定存储在内存中，还可能在磁盘或其他位置。
另外需要牢记的是从状态中获取的值取决于输入元素所代表的 key。因此，在不同 key 上调用同一个接口，可能得到不同的值。

你必须创建一个 `StateDescriptor`，才能得到对应的状态句柄。它保存了状态名称（正如我们稍后将看到的，你可以创建多个状态，并且它们必须具有唯一的名称以便可以引用它们）、
状态所持有值的类型，并且可能包含用户指定的函数，例如 `ReduceFunction`。根据不同的状态类型，可以创建 `ValueStateDescriptor`、`ListStateDescriptor`、
`AggregatingStateDescriptor`、`ReducingStateDescriptor` 或 `MapStateDescriptor`。
为了和之前的 state API 区分，你应该使用 `org.apache.flink.api.common.state.v2` 包（注意是 **v2**）下的 `StateDescriptor`。

状态通过 `RuntimeContext` 进行访问，因此只能在 *rich functions* 中使用。请参阅[这里]({{< ref "docs/dev/datastream/user_defined_functions" >}}#rich-functions)获取相关信息，
我们很快也会看到一个例子。`RichFunction` 中的 `RuntimeContext` 提供了如下方法来访问状态：

* `ValueState<T> getState(ValueStateDescriptor<T>)`
* `ReducingState<T> getReducingState(ReducingStateDescriptor<T>)`
* `ListState<T> getListState(ListStateDescriptor<T>)`
* `AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT>)`
* `MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV>)`

下面是一个 `FlatMapFunction` 的例子，展示了如何将这些部分组合起来：

{{< tabs "348fd48c-fa36-4d27-9e53-9bffb74d8a87" >}}
{{< tab "Java" >}}
```java
public class CountWindowAverage extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    /**
     * The ValueState handle. The first field is the count, the second field a running sum.
     */
    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

        // access the state value
        sum.asyncValue().thenApply(currentSum -> {
            // if it hasn't been used before, it will be null
            Tuple2<Long, Long> current = currentSum == null ? Tuple2.of(0L, 0L) : currentSum;

            // update the count
            current.f0 += 1;

            // add the second field of the input value
            current.f1 += input.f1;

            return current;
        }).thenAccept(r -> {
            // if the count reaches 2, emit the average and clear the state
            if (r.f0 >= 2) {
                out.collect(Tuple2.of(input.f0, r.f1 / r.f0));
                sum.asyncClear();
            } else {
                sum.asyncUpdate(r);
            }
        });
    }

    @Override
    public void open(OpenContext ctx) {
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<>(
                        "average", // the state name
                        TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {})); // type information
        sum = getRuntimeContext().getState(descriptor);
    }
}

// this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
env.fromData(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
        .keyBy(value -> value.f0)
        .enableAsyncState()
        .flatMap(new CountWindowAverage())
        .print();

// the printed output will be (1,4) and (1,5)
```
{{< /tab >}}
{{< /tabs >}}

这个例子实现了一个简单的计数窗口。我们把元组的第一个元素当作 key（在示例中所有 key 都是 `1`）。该函数将出现的次数以及总和存储在 `ValueState` 中。
一旦出现次数达到 2，则将平均值发送到下游，并清除状态，从 `0` 重新开始。请注意，如果元组第一个元素的值各不相同，我们会为每个不同的 key 保存一个单独的状态值。

<a name="execution-order"></a>

### 执行顺序

状态访问方法是异步执行的，这意味着状态访问方法不会阻塞当前线程。在同步 API 下，状态访问方法会按照它们被调用的顺序执行；但在异步 API 下，
状态访问方法可能会乱序执行，尤其是对于不同输入元素的调用。在上面的例子中，如果 `flatMap` 函数接收两个不同的输入元素 A 和 B，
元素 A 和 B 的状态访问方法都会被执行。首先，元素 A 的 `asyncGet` 被执行，然后元素 B 的 `asyncGet` 也被允许执行，
但两个 `asyncGet` 的完成顺序不能保证。因此我们无法保证两个 `StateFuture` 后续步骤的执行顺序，也就无法确定元素 A 和元素 B 的
`asyncClear` 和 `asyncUpdate` 的调用顺序。

尽管状态访问方法可能会乱序执行，这并不意味着所有用户代码都会并行执行。在 `processElement`、`flatMap` 或者 `thenXXxx`
方法中的用户代码都会在单个线程（即主任务线程）中执行。因此不会出现用户代码的并发问题。

通常，你无需担心状态访问方法的执行顺序问题，但 Flink 仍会确保以下规则：
* 对于相同 key 的元素，用户代码入口 `flatMap` 会严格按照元素到达的顺序被调用。
* 传递给 `thenXXxx` 方法的函数会按照调用链的顺序执行。如果没有形成调用链，或者同时有多个调用链，则无法保证执行顺序。

<a name="best-practice-of-asynchronous-apis"></a>

### 异步 API 的最佳实践

异步 API 的设计比同步 API 更加高效和强大。使用异步 API 时，有一些最佳实践应该遵循：
* **避免混合使用同步和异步状态访问**。
* 通过连续的 `thenXXxx` 方法进行链式调用来处理状态访问的结果，并发起下一次状态访问或返回结果。将逻辑分割成由 `thenXXxx` 方法划分的多个步骤。
* 避免在用户函数（例如 `RichFlatMapFunction`）中访问可变（mutable）成员。因为状态访问方法是乱序执行的，
  可变成员可能会以无法预测的顺序被访问。相反，应该使用状态访问的结果在不同步骤之间传递数据。`StateFutureUtils.completedFuture`
  和 `thenApply` 方法可以被用来传递数据。或者使用一个每次在 `flatMap` 调用时初始化的容器（`AtomicReference`），以便在不同的 lambda 之间共享数据。

<a name="state-time-to-live-ttl"></a>

### 状态有效期（TTL）

任何类型的 keyed state 都可以有*有效期*（time-to-live，TTL）。如果配置了 TTL 且状态值已过期，则会尽最大可能清除对应的值，这会在后面详述。

所有集合类型的状态都支持单元素的 TTL。这意味着列表元素和映射元素将独立到期。

在使用状态 TTL 前，需要先构建一个 `StateTtlConfig` 配置对象。然后把配置传递到 state descriptor 中启用 TTL 功能：

{{< tabs "43221f0b-b56c-42e5-833e-ed416e5f9b4e" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import java.time.Duration;

StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Duration.ofSeconds(1))
    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
    .build();
    
ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("text state", String.class);
stateDescriptor.enableTimeToLive(ttlConfig);
```
{{< /tab >}}
{{< /tabs >}}

TTL 配置有以下几个选项：

`newBuilder` 方法的第一个参数表示数据的有效期，是必选项。

TTL 的更新策略（默认是 `OnCreateAndWrite`）：

 - `StateTtlConfig.UpdateType.OnCreateAndWrite` - 仅在创建和写入时更新
 - `StateTtlConfig.UpdateType.OnReadAndWrite` - 读取时也更新

    （**注意：** 如果你同时将状态的可见性配置为 `StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp`，
    那么在 PyFlink 作业中，状态的读缓存将会失效，这将导致一部分的性能损失）

数据在过期但还未被清理时的可见性配置如下（默认为 `NeverReturnExpired`）：

 - `StateTtlConfig.StateVisibility.NeverReturnExpired` - 不返回过期数据

    （**注意：** 在 PyFlink 作业中，状态的读写缓存都将失效，这将导致一部分的性能损失）

 - `StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp` - 会返回过期但未清理的数据

`NeverReturnExpired` 情况下，过期数据就像不存在一样，不管是否被物理删除。这对于在 TTL 之后必须严格禁止访问数据的场景非常有用，比如处理隐私敏感数据的应用。

另一个选项 `ReturnExpiredIfNotCleanedUp` 允许在数据被清理前返回过期的数据。

**注意：**

- 状态上次的修改时间会和数据一起保存在 state backend 中，因此开启该特性会增加状态数据的存储。
Heap state backend 会额外存储一个包括用户状态对象引用以及一个 long 类型原始值的 Java 对象，RocksDB/ForSt state backend 会在每个存储的值、列表元素或映射元素上增加 8 个字节。

- 暂时只支持基于 *processing time* 的 TTL。

- 使用开启了 TTL 的 descriptor 恢复之前未配置 TTL 的状态（或者反过来），将会导致兼容性失败并抛出 `StateMigrationException`。

- TTL 的配置并不会保存在 checkpoint 或 savepoint 中，仅对当前运行的作业有效。

- 不建议在从 checkpoint 恢复时将 TTL 从短调长，这可能会产生潜在的数据错误。

- 当前开启 TTL 的 map state 仅在用户值序列化器支持 null 的情况下，才支持用户值为 null。如果用户值序列化器不支持 null，
可以用 `NullableSerializer` 包装一层，代价是序列化后的数据会多出一个字节。

- 启用 TTL 配置后，`StateDescriptor` 中的 `defaultValue`（已被标记为 deprecated）将会失效。这个设计的目的是为了确保语义更加清晰，在此基础上，用户需要手动管理那些实际值为 null 或已过期的状态默认值。

<a name="cleanup-of-expired-state"></a>

#### 过期数据的清理

默认情况下，过期数据会在读取的时候被删除，例如 `ValueState#value`，同时会有后台线程定期清理（如果 state backend 支持的话）。可以通过 `StateTtlConfig` 配置关闭后台清理：

{{< tabs "df774bf4-84ec-49a9-8372-c63b59122608" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.state.StateTtlConfig;
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Duration.ofSeconds(1))
    .disableCleanupInBackground()
    .build();
```
{{< /tab >}}
{{< /tabs >}}

{{< hint info >}}
目前 ForSt State Backend 只会在压缩（compaction）过程中清理状态。如果你使用的是 ForSt 以外的 state backend，并且想了解其他的清理策略，
请参阅 [State V1 文档]({{< ref "docs/dev/datastream/fault-tolerance/state#cleanup-of-expired-state" >}})中关于过期数据清理的内容。
{{< /hint >}}

<a name="cleanup-during-compaction"></a>

##### 在压缩时清理

如果使用 ForSt state backend，则会调用 Flink 定制的压缩过滤器进行后台清理。ForSt 会周期性地进行异步压缩，以合并状态更新并减少存储空间。
Flink 的压缩过滤器会检查带有 TTL 的状态条目的过期时间戳，并过滤掉已经过期的值。

该特性可以通过 `StateTtlConfig` 进行配置：

{{< tabs "4db505be-2c9c-6ef7-aa01-74763968b7a9" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.state.StateTtlConfig;

StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Duration.ofSeconds(1))
    .cleanupInRocksdbCompactFilter(1000, Duration.ofHours(1))
    .build();
```
{{< /tab >}}
{{< /tabs >}}

Flink 每处理一定条数的状态数据后，ForSt 压缩过滤器就会从 Flink 查询一次当前时间戳，用于检测状态是否已经过期。
你可以通过 `StateTtlConfig.newBuilder(...).cleanupInRocksdbCompactFilter(long queryTimeAfterNumEntries)` 方法指定处理状态的条数。
时间戳更新得越频繁，状态的清理越及时，但由于压缩会有从 native 代码调用 JNI 的开销，因此会影响整体的压缩性能。
ForSt backend 的默认后台清理策略会每处理 1000 条数据查询一次当前时间戳。

定期压缩可以加速过期状态条目的清理，特别是对于很少访问的状态条目。
比这个值早的文件将被选取进行压缩，并重新写入与之前相同的 Level 中。
该功能可以确保文件定期通过压缩过滤器压缩。
你可以通过 `StateTtlConfig.newBuilder(...).cleanupInRocksdbCompactFilter(long queryTimeAfterNumEntries, Duration periodicCompactionTime)`
方法设定定期压缩的时间。
定期压缩的时间的默认值是 30 天。
你可以将其设置为 0 以关闭定期压缩，或设置一个较小的值以加速过期状态条目的清理，但它将会触发更多压缩。

你还可以通过为 `FlinkCompactionFilter` 开启 debug 级别，来开启 ForSt 过滤器 native 代码的 debug 日志：

`log4j.logger.org.forstdb.FlinkCompactionFilter=DEBUG`

**注意：**
- 压缩时调用 TTL 过滤器会降低速度。TTL 过滤器需要解析上次访问的时间戳，并对每个将参与压缩的状态进行是否过期检查。
  对于集合型状态类型（比如 list 和 map），会对集合中每个元素进行检查。
- 对于元素序列化后长度不固定的列表状态，如果至少第一个元素已经过期，TTL 过滤器需要在每次 JNI 调用过程中，额外调用 Flink 的 Java 序列化器，
  从而确定下一个未过期数据的位置。
- 对已有的作业，这个清理方式可以在任何时候通过 `StateTtlConfig` 启用或禁用，比如从 savepoint 重启后。
- 定期压缩功能只在 TTL 启用时生效。

<a name="operator-state"></a>

## 算子状态（Operator State）

*算子状态*（或者*非 keyed 状态*）是绑定到一个并行算子实例的状态。[Kafka Connector]({{< ref "docs/connectors/datastream/kafka" >}}) 是 Flink 中使用算子状态一个很具有启发性的例子。
Kafka consumer 每个并行实例维护了 topic partitions 和偏移量的 map 作为它的算子状态。

当并行度改变的时候，算子状态支持将状态重新分发给各并行算子实例。处理重分发过程有多种不同的方案。

在典型的有状态 Flink 应用中你无需使用算子状态。它大都作为一种特殊类型的状态使用，用于实现 source/sink，以及没有可用于对状态进行分区的 key 的场景中。

<a name="broadcast-state"></a>

## 广播状态（Broadcast State）

*广播状态*是一种特殊的*算子状态*。引入它的目的在于支持一个流中的元素需要广播到所有下游任务的使用情形。在这些任务中广播状态用于保持所有子任务状态相同。
该状态接下来可在第二个处理记录的数据流中访问。可以设想包含了一系列用于处理其他流中元素规则的低吞吐量数据流，这个例子自然而然地运用了广播状态。
考虑到上述这类使用情形，广播状态和其他算子状态的不同之处在于：

 1. 它具有 map 格式，
 2. 它仅在一些特殊的算子中可用。这些算子的输入为一个*广播*数据流和*非广播*数据流，
 3. 这类算子可以拥有不同命名的*多个广播状态*。

{{< top >}}

<a name="using-operator-state"></a>

## 使用 Operator State

用户可以通过实现 `CheckpointedFunction` 接口来使用 operator state。


#### CheckpointedFunction

`CheckpointedFunction` 接口提供了访问 non-keyed state 的方法，支持不同的重新分配模式，需要实现如下两个方法：

```java
void snapshotState(FunctionSnapshotContext context) throws Exception;

void initializeState(FunctionInitializationContext context) throws Exception;
```

进行 checkpoint 时会调用 `snapshotState()`。用户自定义函数初始化时会调用 `initializeState()`，初始化包括第一次自定义函数初始化和从之前的 checkpoint 恢复。
因此 `initializeState()` 不仅是定义不同状态类型初始化的地方，也需要包括状态恢复的逻辑。

当前支持 list 形式的 operator state。这些状态是一个*可序列化*对象的集合 `List`，彼此独立，方便在改变并发后进行状态的重新分派。
换句话说，这些对象是重新分配 non-keyed state 的最细粒度。根据状态的不同访问方式，有如下几种重新分配的模式：

  - **Even-split redistribution：** 每个算子都保存一个列表形式的状态集合，整个状态由所有的列表拼接而成。当作业恢复或重新分配的时候，整个状态会按照算子的并发度进行均匀分配。
    每个算子会得到一个子列表，子列表可以为空，也可以包含一个或多个元素。
    比如说，算子 A 的并发度为 1，其 checkpoint 状态包含两个元素 `element1` 和 `element2`，当并发度增加为 2 时，`element1` 可能会被分到算子实例 0 上，`element2` 则会被分到算子实例 1 上。

  - **Union redistribution：** 每个算子保存一个列表形式的状态集合。整个状态由所有的列表拼接而成。当作业恢复或重新分配时，每个算子都将获得所有的状态数据。
    在列表基数很大时不要使用此功能，因为 checkpoint metadata 会为每个列表条目存储一个偏移量，可能会超过 RPC framesize 限制或导致 out-of-memory 错误。

下面的例子中的 `MapFunction` 通过 `CheckpointedFunction` 统计每个并行实例处理过的元素数量，演示了列表状态基本的 even-split redistribution：

{{< tabs "c27e5b18-94af-4d03-8e6c-2f1a7b9d0e54" >}}
{{< tab "Java" >}}
```java
public class CountingMap
        implements MapFunction<String, Tuple2<String, Long>>, CheckpointedFunction {

    private transient ListState<Long> checkpointedCount;

    private long count;

    @Override
    public Tuple2<String, Long> map(String value) {
        count++;
        return Tuple2.of(value, count);
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedCount.update(Collections.singletonList(count));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Long> descriptor =
            new ListStateDescriptor<>("count", Long.class);

        checkpointedCount = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            for (Long partialCount : checkpointedCount.get()) {
                count += partialCount;
            }
        }
    }
}
```
{{< /tab >}}
{{< /tabs >}}

当作业以不同的并行度恢复时，一个实例可能会收到之前多个实例的计数，因此 `initializeState()` 会将它们累加起来。

Sink 不实现 `CheckpointedFunction`，而是将算子状态保存在它的 writer 中：sink 实现 `SupportsWriterState` 接口，而 writer（一个 `StatefulSinkWriter`）
通过 `snapshotState()` 以列表形式返回其状态。Flink 使用 even-split redistribution 存储该列表，并将恢复的元素传递给 `restoreWriter()`。
下面是一个 sink 的例子，它会先缓存元素，再统一发送到外部系统：

{{< tabs "5b99663c-1801-486c-a085-ce66882a2k49" >}}
{{< tab "Java" >}}
```java
public class BufferingSink
        implements Sink<Tuple2<String, Integer>>,
                   SupportsWriterState<Tuple2<String, Integer>, Tuple2<String, Integer>> {

    private final int threshold;

    public BufferingSink(int threshold) {
        this.threshold = threshold;
    }

    @Override
    public BufferingWriter createWriter(WriterInitContext context) {
        return new BufferingWriter(threshold, Collections.emptyList());
    }

    @Override
    public BufferingWriter restoreWriter(
            WriterInitContext context, Collection<Tuple2<String, Integer>> recoveredState) {
        return new BufferingWriter(threshold, recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<Tuple2<String, Integer>> getWriterStateSerializer() {
        return new BufferedElementSerializer();
    }
}

public class BufferingWriter
        implements StatefulSinkWriter<Tuple2<String, Integer>, Tuple2<String, Integer>> {

    private final int threshold;

    private final List<Tuple2<String, Integer>> bufferedElements;

    public BufferingWriter(int threshold, Collection<Tuple2<String, Integer>> recoveredState) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>(recoveredState);
    }

    @Override
    public void write(Tuple2<String, Integer> value, Context context) {
        bufferedElements.add(value);
        if (bufferedElements.size() >= threshold) {
            send();
        }
    }

    @Override
    public void flush(boolean endOfInput) {
        // on a checkpoint, the buffered elements go into the writer state instead
        if (endOfInput) {
            send();
        }
    }

    private void send() {
        for (Tuple2<String, Integer> element: bufferedElements) {
            // send it to the sink
        }
        bufferedElements.clear();
    }

    @Override
    public List<Tuple2<String, Integer>> snapshotState(long checkpointId) {
        return new ArrayList<>(bufferedElements);
    }

    @Override
    public void close() {}
}

public class BufferedElementSerializer
        implements SimpleVersionedSerializer<Tuple2<String, Integer>> {

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(Tuple2<String, Integer> element) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(32);
        out.writeUTF(element.f0);
        out.writeInt(element.f1);
        return out.getCopyOfBuffer();
    }

    @Override
    public Tuple2<String, Integer> deserialize(int version, byte[] serialized) throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        return Tuple2.of(in.readUTF(), in.readInt());
    }
}
```
{{< /tab >}}
{{< /tabs >}}

在实现了 `CheckpointedFunction` 的函数中，`initializeState` 方法接收一个 `FunctionInitializationContext` 参数，会用来初始化 non-keyed state 的“容器”。这些容器是一个 `ListState`，
用于在 checkpoint 时保存 non-keyed state 对象。

注意这些状态是如何初始化的，和 keyed state 类似，`StateDescriptor` 会包括状态名字、以及状态所持有值的类型相关信息：


{{< tabs "e75410d8-4ea2-48e7-a7ef-249ece2a9997" >}}
{{< tab "Java" >}}
```java
private transient ListState<Long> checkpointedCount;

@Override
public void initializeState(FunctionInitializationContext context) throws Exception {
    ListStateDescriptor<Long> descriptor =
        new ListStateDescriptor<>("count", Long.class);

    checkpointedCount = context.getOperatorStateStore().getListState(descriptor);
}
```

{{< /tab >}}
{{< /tabs >}}
调用不同的获取状态对象的接口，会使用不同的状态分配算法。比如 `getUnionListState(descriptor)` 会使用 union redistribution 算法，
而如果方法名中不包含重新分配模式，例如 `getListState(descriptor)`，则表示使用基本的 even-split redistribution 算法。

当初始化好状态对象后，我们通过 `isRestored()` 方法判断是否从之前的故障中恢复回来，如果该方法返回 `true` 则表示从故障中进行恢复，会执行接下来的恢复逻辑。

将 `ListState` 保存在一个字段中，供之后在 `snapshotState()` 中使用。在 `snapshotState()` 中，用需要 checkpoint 的对象更新它，
这会替换上一次 checkpoint 包含的对象。

另外，我们同样可以在 `initializeState()` 方法中使用 `FunctionInitializationContext` 初始化 keyed state。

{{< top >}}

<a name="migrate-from-the-old-state-api"></a>

## 从旧的状态 API 迁移

从旧的状态 API 迁移到新的状态 API 非常方便，请按以下步骤操作：

1. 在 `KeyedStream` 上调用 `enableAsyncState()` 来启用新的状态 API。
2. 将 `StateDescriptor` 替换为 `v2` 包下新的 `StateDescriptor`，同时将旧的状态句柄替换为 `v2` 包下新的状态句柄。
3. 用新的异步方法重写旧的状态访问方法。
4. 使用新的状态 API 时，推荐使用 ForSt State Backend，它支持异步状态访问。其他 state backend 只支持同步执行状态访问，
尽管它们也可以和新的状态 API 一起使用。
