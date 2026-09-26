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

# 使用 State V2 (新的 API)

本章节您将了解 Flink 用于编写有状态程序的新 API，要了解有状态流处理背后的概念，请参阅[Stateful Stream
Processing]({{< ref "docs/concepts/stateful-stream-processing" >}})。

新的状态 API 比以前的 API 更灵活，用户可以通过新的状态 API 异步访问状态，是一套更强大有效的 API。
异步状态访问对存算分离来说是必要的，即令状态后端支持大状态时将文件溢出到远程文件系统的能力。更多关于
存算分离的具体信息，请参阅[Disaggregated State Management]({{< ref "docs/ops/state/disaggregated_state" >}})。

## Keyed DataStream

如果你希望使用 keyed state，首先需要为`DataStream`指定 key（主键）。这个主键用于状态分区（也会给数据流中的记录本身分区）。
你可以使用 `DataStream` 中 Java API 的 `keyBy(KeySelector)` 。  它将生成 `KeyedStream`，接下来允许使用 keyed state 操作。
您可以通过 `KeyedStream` 的 `enableAsyncState()` 方法来启动异步状态操作。

Key selector 函数接收单条记录作为输入，返回这条记录的 key。该 key 可以为任何类型，但是它的计算产生方式**必须**是具备确定性的。

The data model of Flink is not based on key-value pairs. Therefore, you do not
need to physically pack the data set types into keys and values. Keys are
"virtual": they are defined as functions over the actual data to guide the
grouping operator.

The following example shows a key selector function that simply returns the
field of an object:

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

## 使用 Keyed State V2

和之前的 state API 不同，新的 state API 允许用户异步访问状态。每种类型的状态都提供了两种版本的 API：同步和异步。
同步 API 是一个阻塞等待状态访问完成的 API。异步 API 是非阻塞的，它会返回一个 `StateFuture`，这个 `StateFuture` 会在状态访问完成时完成。
在 `StateFuture` 完成时，用户的回调会被执行。异步 API 比同步 API 更加高效，我们更推荐使用异步 API。
需要注意，**不建议**在同一个用户函数中混合使用同步和异步状态访问。

Key state 接口提供了对不同类型状态的访问，它们都是以 key 为作用域。这意味着这个类型的状态只能在 `KeyedStream` 上使用，`keyedStream`
可以通过 `stream.keyBy(…)` 在 Java 中创建。最重要的是，`keyedStream` 需要通过调用 `enableAsyncState()` 来开启异步状态访问。
新的 API 集合只有在 `KeyedStream` 上开启异步状态访问时才可用。

接下来，我们将看到不同类型的状态，并看到它们如何在程序中使用。由于同步 API 与之前的 API 是等价的，这里只关注异步 API。

### 返回值

首先，我们应该熟悉异步状态访问方法的返回值。

`StateFuture<T>` 完成时会返回状态访问的结果。返回类型是 T。它提供了多个方法来处理结果，包括：
* `StateFuture<Void> thenAccept(Consumer<T>)`: 接受一个在状态访问完成时会被调用的 `Consumer` 作为参数。
   它返回一个 `StateFuture<Void>`，在 `Consumer` 执行结束时完成。
* `StateFuture<R> thenApply(Function<T, R>)`: 接受一个在状态访问完成时会被调用的 `Function` 作为参数。
   返回值 `StateFuture<R>` 会使用 `Function` 的返回值会作为内部结果，在 `Function` 执行结束时完成。
* `StateFuture<R> thenCompose(Function<T, StateFuture<R>>)`: 接受一个在状态访问完成时会被调用的
   `Function` 作为参数。 函数的返回值是 `StateFuture<R>`， 被传给 `thenCompose` 的调用者作为最终的返回值。返回值
   `StateFuture<R>` 会在 `Function` 内部的 `StateFuture<R>` 完成时完成。
* `StateFuture<R> thenCombine(StateFuture<U>, BiFunction<T, U, R>)`: 接受另一个 `StateFuture<U>`
  和一个 `BiFunction` 作为参数，`BiFunction` 在两个 `StateFuture` 都完成时被调用。 返回值 `StateFuture`使用
  `BiFunction` 的返回值作为内部结果， 返回值的 `StateFuture<R>` 会在两个 `StateFuture` 都完成时完成。

以上这些方法类似于 `CompletableFuture` 的相关方法。除此之外，注意，`StateFuture` 不提供 `get()` 方法来阻塞当前线程，
直到状态访问完成。 这是因为阻塞当前线程可能会导致递归阻塞。 `StateFuture<T>` 还提供了条件版本的 `thenAccept`,
`thenApply`, `thenCompose` 和 `thenCombine`，用于后续逻辑依赖状态访问返回值来选择不同分支的情景。这些方法的条件版本是：
`thenConditionallyAccept`, `thenConditionallyApply`, `thenConditionallyCompose` 和 `thenConditionallyCombine`。

`StateIterator<T>` 是一个迭代器，可用来迭代状态中的元素。它提供了以下方法：
* `boolean isEmpty()` : 当迭代器没有元素时返回 `true`，否则返回 `false`，是一个同步方法。
* `StateFuture<Void> onNext(Consumer<T>)` : 该方法接受一个 `Consumer`作为参数，`Consumer` 会在状态访问完成时与下一个
   元素一起被调用。它返回一个 `StateFuture<Void>`，在 `Consumer` 执行完时完成。同样，`onNext` 也提供了 `Function`
   作为参数的版本：`StateFuture<Collection<R>> onNext(Function<T, R>)`。该方法接受一个 `Function` 作为参数，
   会在状态访问完成时与下一个元素一起被调用。`Function` 的返回值会被收集到一个集合中作为返回值 `StateFuture` 的内部
   结果，返回值 `StateFuture` 在 `Function` 执行结束时完成。

我们也提供了 `StateFutureUtils` 类，其中包含一些用于处理 `StateFuture` 的实用方法：
* `StateFuture<T> completedFuture(T)`: 该方法返回一个已经完成的 `StateFuture`，该 `StateFuture` 的返回值为给定的值。
* `StateFuture<Void> completedVoidFuture()`: 该方法返回一个已经完成的 `StateFuture`，该 `StateFuture` 的返回值为 `null`，
   是 `completedFuture` 的 void 返回值版本。
* `StateFuture<Collection<T>> combineAll(Collection<StateFuture<T>>)` : 该方法接受一个 `StateFuture` 集合
   作为参数，返回一个 `StateFuture`，返回值 `StateFuture` 在所有的 `StateFuture` 都完成后完成。返回值 `StateFuture`
   的内部结果是输入的 `StateFuture` 的结果的集合。该方法在想组合多个 `StateFuture` 时的结果时非常有用。
* `StateFuture<Iterable<T>> toIterable(StateFuture<StateIterator<T>>)` : 该方法接受一个结果为 `StateIterator`
   的 `StateFuture` 作为参数，返回一个 `Iterable` 作为结果的 `StateFuture`。 返回值 `StateFuture` 的内部结果
   `Iterable` 包含了 `StateIterator` 的所有元素。该函数在想把 `StateIterator` 转换为 `Iterable` 时非常有用。
   但这可能会禁用延迟加载的功能，如果没有充分理由这样做，建议不这样使用。本方法只有当下一步的计算依赖于迭代器的全部数据时才有用。

### 状态原语

所有支持的状态类型如下所示：

* `ValueState<T>`: 保存一个可以更新和检索的值（如上所述，每个值都对应到当前的输入数据的 key，因此算子接收到的每个 key
  都可能对应一个值）。 这个值可以通过 `asyncUpdate(T)` 进行更新，通过 `StateFuture<T> asyncValue()` 进行检索。

* `ListState<T>`: 保存一个元素的列表。可以往这个列表中追加数据，并在当前的列表上进行检索。可以通过
  `asyncAdd(T)` 或者 `asyncAddAll(List<T>)` 进行添加元素，通过 `StateFuture<StateIterator<T>> asyncGet()`
   获得整个列表。还可以通过 `asyncUpdate(List<T>)` 覆盖当前的列表。

* `ReducingState<T>`: 保存一个单值，表示添加到状态的所有值的聚合。接口与 `ListState` 类似，但使用 `asyncAdd(T)`
   增加元素，会使用提供的 `ReduceFunction` 进行聚合。

* `AggregatingState<IN, OUT>`: 保留一个单值，表示添加到状态的所有值的聚合。和 `ReducingState` 相反的是,
  聚合类型可能与添加到状态的元素的类型不同。 接口与 `ListState` 类似，但使用 `asyncAdd(IN)` 添加的元素会用指定的
  `AggregateFunction` 进行聚合。

* `MapState<UK, UV>`: 维护了一个映射列表。 你可以添加键值对到状态中，也可以获得反映当前所有映射的迭代器。使用 `asyncPut(UK，UV)`
   或者 `asyncPutAll(Map<UK，UV>)` 添加映射。 使用 `asyncGet(UK)` 检索特定 key。 使用 `asyncEntries()`，`asyncKeys()` 和 `asyncValues()`
   分别检索映射、键和值的可迭代视图。你还可以通过 `asyncIsEmpty()` 来判断是否包含任何键值对。

所有类型的状态还有一个`asyncClear()` 方法，清除当前 key 下的状态数据，也就是当前输入元素的 key。

请注意，这些状态对象仅用于与状态交互。状态本身不一定存储在内存中，还可能在磁盘或其他位置。
另外需要注意的是从状态中获取的值取决于输入元素所代表的 key。 因此，在不同 key 上调用同一个接口，可能得到不同的值。

你必须创建一个 `StateDescriptor`，才能得到对应的状态句柄。 这保存了状态名称（正如我们稍后将看到的，你可以创建多个状态，
并且它们必须具有唯一的名称以便可以引用它们）， 状态所持有值的类型，并且可能包含用户指定的函数，例如`ReduceFunction`。
根据不同的状态类型，可以创建`ValueStateDescriptor`，`ListStateDescriptor`，`AggregatingStateDescriptor`，
`ReducingStateDescriptor` 或 `MapStateDescriptor`。为了和之前的 state APIs 区分，应该使用
`org.apache.flink.api.common.state.v2` （注意是 **v2**）包下的 `StateDescriptor`。

状态通过 `RuntimeContext` 进行访问，因此只能在 *rich functions* 中使用。请参阅
[这里]({{< ref "docs/dev/datastream/user_defined_functions" >}}#rich-functions)获取相关信息，
但是我们很快也会看到一个例子。`RichFunction` 中 `RuntimeContext` 提供如下方法：

* `ValueState<T> getState(ValueStateDescriptor<T>)`
* `ReducingState<T> getReducingState(ReducingStateDescriptor<T>)`
* `ListState<T> getListState(ListStateDescriptor<T>)`
* `AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT>)`
* `MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV>)`

This is an example `FlatMapFunction` that shows how all of the parts fit together:

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
env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(1L, 4L), Tuple2.of(1L, 2L))
        .keyBy(value -> value.f0)
        .enableAsyncState()
        .flatMap(new CountWindowAverage())
        .print();

// the printed output will be (1,4) and (1,5)
```
{{< /tab >}}
{{< /tabs >}}

This example implements a poor man's counting window. We key the tuples by the first field
(in the example all have the same key `1`). The function stores the count and a running sum in
a `ValueState`. Once the count reaches 2 it will emit the average and clear the state so that
we start over from `0`. Note that this would keep a different state value for each different input
key if we had tuples with different values in the first field.

### 执行顺序

状态访问是异步的，这意味着状态访问方法不会阻塞当前线程。在同步 API 下，状态访问方法会按照它们被调用的顺序执行，但是异步 API 下，
尤其是对于不同输入元素的状态访问方法，状态访问方法可能会乱序执行。在上面的例子中，如果 `flatMap` 函数接收两个不同的
输入元素 A 和 B，元素 A 和 B 的状态访问方法会被执行。首先，元素 A 的 `asyncGet` 方法被执行，然后元素 B 的 `asyncGet` 方法接着被执行，
但两个 `asyncGet` 的完成顺序不能保证。因此我们无法保证两个 `StateFuture` 后续步骤的执行顺序，也就无法确定元素 A 和 元素 B 的
`asyncClear` 和 `asyncUpdate` 的调用顺序。

尽管状态访问方法可能会乱序执行，这并不意味着所有用户代码都会并行执行。在 `processElement`, `flatMap` 或者 `thenXXxx`
方法中的用户代码都会在单个线程（即主任务线程）中执行。因此不会出现用户代码的并发问题。

通常，您无需担心状态访问方法的执行顺序问题，但 Flink 仍确保一些规则:
* 同 key 的元素的执行顺序严格按照元素到达 `flatMap` 的顺序执行的。
* 传递给 `thenXXxx` 方法的函数会按照调用链的顺序执行。如果同时有多个调用链，则无法保证执行顺序。

### 异步 APIs 的最佳实践

异步 APIs 的设计比同步 APIs 更加高效和强大。当使用异步 APIs 时，有一些最佳实践应该遵循：
* **不要混合使用同步和异步状态访问**
* 将逻辑分割成多个步骤，通过连续的 `thenXXxx` 来进行链式调用，前一个 `thenXXxx` 的结果传递给后一个 `thenXXxx`。
* 避免在用户函数（例如 `RichFlatMapFunction`） 中使用可变（mutable）成员。因为状态访问方法是乱序执行的，
  可变成员可能会以无法预测的顺序执行。相反，建议使用 `thenXXxx` 的结果在不同步骤之间传递数据。`StateFutureUtils.completedFuture`
  和 `thenApply` 方法可以被用来传递数据。或者使用一个每次在 `flatMap` 调用时初始化的容器 （`AtomicReference`），以便在不同回调之间共享数据。


### State Time-To-Live (TTL)

A *time-to-live* (TTL) can be assigned to the keyed state of any type. If a TTL is configured and a
state value has expired, the stored value will be cleaned up on a best effort basis which is
discussed in more detail below.

All state collection types support per-entry TTLs. This means that list elements and map entries
expire independently.

In order to use state TTL one must first build a `StateTtlConfig` configuration object. The TTL 
functionality can then be enabled in any state descriptor by passing the configuration:

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

The configuration has several options to consider:

The first parameter of the `newBuilder` method is mandatory, it is the time-to-live value.

The update type configures when the state TTL is refreshed (by default `OnCreateAndWrite`):

 - `StateTtlConfig.UpdateType.OnCreateAndWrite` - only on creation and write access
 - `StateTtlConfig.UpdateType.OnReadAndWrite` - also on read access

    (**Notes:** If you set the state visibility to `StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp`
    at the same time, the state read cache will be disabled, which will cause some performance loss in PyFlink)
 
The state visibility configures whether the expired value is returned on read access 
if it is not cleaned up yet (by default `NeverReturnExpired`):

 - `StateTtlConfig.StateVisibility.NeverReturnExpired` - expired value is never returned 

    (**Notes:** The state read/write cache will be disabled, which will cause some performance loss in PyFlink)

 - `StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp` - returned if still available
 
In case of `NeverReturnExpired`, the expired state behaves as if it does not exist anymore, 
even if it still has to be removed. The option can be useful for use cases 
where data has to become unavailable for read access strictly after TTL, 
e.g. application working with privacy sensitive data.
 
Another option `ReturnExpiredIfNotCleanedUp` allows to return the expired state before its cleanup.

**Notes:** 

- The state backends store the timestamp of the last modification along with the user value, 
which means that enabling this feature increases consumption of state storage. 
Heap state backend stores an additional Java object with a reference to the user state object 
and a primitive long value in memory. The RocksDB/ForSt state backend adds 8 bytes per stored value,
list entry or map entry.

- Only TTLs in reference to *processing time* are currently supported.

- Trying to restore state, which was previously configured without TTL, using TTL enabled descriptor or vice versa
will lead to compatibility failure and `StateMigrationException`.

- The TTL configuration is not part of checkpoints or savepoints but rather a way of how Flink treats it in the currently running job.

- It is not recommended to restore checkpoint state with adjusting the ttl from a short value to a long value,
which may cause potential data errors.

- The map state with TTL currently supports null user values only if the user value serializer can handle null values. 
If the serializer does not support null values, it can be wrapped with `NullableSerializer` at the cost of an extra byte in the serialized form.

- With TTL enabled configuration, the `defaultValue` in `StateDescriptor`, which is actually already deprecated, will no longer take an effect. This aims to make the semantics more clear and let user manually manage the default value if the contents of the state is null or expired.

#### Cleanup of Expired State

By default, expired values are explicitly removed on read, such as `ValueState#value`, and periodically garbage collected
in the background if supported by the configured state backend. Background cleanup can be disabled in the `StateTtlConfig`:

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
Currently the ForSt State Backend only cleanup the state in compaction process. If you are using
a state backend other than ForSt and want to read about other cleanup strategies, please refer to the
[State V1 documentation]({{< ref "docs/dev/datastream/fault-tolerance/state#cleanup-of-expired-state" >}})
about the expired state cleanup.
{{< /hint >}}

##### Cleanup during compaction

If the ForSt state backend is used, a Flink specific compaction filter will be called for the background cleanup.
ForSt periodically runs asynchronous compactions to merge state updates and reduce storage.
Flink compaction filter checks expiration timestamp of state entries with TTL
and excludes expired values.

This feature can be configured in `StateTtlConfig`:

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

ForSt compaction filter will query current timestamp, used to check expiration, from Flink every time
after processing certain number of state entries.
You can change it and pass a custom value to
`StateTtlConfig.newBuilder(...).cleanupInRocksdbCompactFilter(long queryTimeAfterNumEntries)` method.
Updating the timestamp more often can improve cleanup speed
but it decreases compaction performance because it uses JNI call from native code.
The default background cleanup for ForSt backend queries the current timestamp each time 1000 entries have been processed.

Periodic compaction could speed up expired state entries cleanup, especially for state entries rarely accessed.
Files older than this value will be picked up for compaction, and re-written to the same level as they were before.
It makes sure a file goes through compaction filters periodically.
You can change it and pass a custom value to
`StateTtlConfig.newBuilder(...).cleanupInRocksdbCompactFilter(long queryTimeAfterNumEntries, Duration periodicCompactionTime)` method.
The default value of Periodic compaction seconds is 30 days.
You could set it to 0 to turn off periodic compaction or set a small value to speed up expired state entries cleanup, but it
would trigger more compactions.

You can activate debug logs from the native code of ForSt filter
by activating debug level for `FlinkCompactionFilter`:

`log4j.logger.org.forstdb.FlinkCompactionFilter=DEBUG`

**Notes:**
- Calling of TTL filter during compaction slows it down.
  The TTL filter has to parse timestamp of last access and check its expiration
  for every stored state entry per key which is being compacted.
  In case of collection state type (list or map) the check is also invoked per stored element.
- If this feature is used with a list state which has elements with non-fixed byte length,
  the native TTL filter has to call additionally a Flink java type serializer of the element over JNI per each state entry
  where at least the first element has expired to determine the offset of the next unexpired element.
- For existing jobs, this cleanup strategy can be activated or deactivated anytime in `StateTtlConfig`,
  e.g. after restart from savepoint.
- Periodic compaction could only work when TTL is enabled.
## 算子状态 (Operator State)

*算子状态*（或者*非 keyed 状态*）是绑定到一个并行算子实例的状态。[Kafka Connector]({{< ref "docs/connectors/datastream/kafka" >}}) 是 Flink 中使用算子状态一个很具有启发性的例子。
Kafka consumer 每个并行实例维护了 topic partitions 和偏移量的 map 作为它的算子状态。

当并行度改变的时候，算子状态支持将状态重新分发给各并行算子实例。处理重分发过程有多种不同的方案。

在典型的有状态 Flink 应用中你无需使用算子状态。它大都作为一种特殊类型的状态使用。用于实现 source/sink，以及无法对 state 进行分区而没有主键的这类场景中。

**注意：** Python DataStream API 仍无法支持算子状态。

## 广播状态 (Broadcast State)

*广播状态*是一种特殊的*算子状态*。引入它的目的在于支持一个流中的元素需要广播到所有下游任务的使用情形。在这些任务中广播状态用于保持所有子任务状态相同。
该状态接下来可在第二个处理记录的数据流中访问。可以设想包含了一系列用于处理其他流中元素规则的低吞吐量数据流，这个例子自然而然地运用了广播状态。
考虑到上述这类使用情形，广播状态和其他算子状态的不同之处在于：

 1. 它具有 map 格式，
 2. 它仅在一些特殊的算子中可用。这些算子的输入为一个*广播*数据流和*非广播*数据流，
 3. 这类算子可以拥有不同命名的*多个广播状态* 。

{{< top >}}

## Using Operator State

To use operator state, a stateful function can implement the `CheckpointedFunction`
interface.


#### CheckpointedFunction

The `CheckpointedFunction` interface provides access to non-keyed state with different
redistribution schemes. It requires the implementation of two methods:

```java
void snapshotState(FunctionSnapshotContext context) throws Exception;

void initializeState(FunctionInitializationContext context) throws Exception;
```

Whenever a checkpoint has to be performed, `snapshotState()` is called. The counterpart, `initializeState()`,
is called every time the user-defined function is initialized, be that when the function is first initialized
or be that when the function is actually recovering from an earlier checkpoint. Given this, `initializeState()` is not
only the place where different types of state are initialized, but also where state recovery logic is included.

Currently, list-style operator state is supported. The state
is expected to be a `List` of *serializable* objects, independent from each other,
thus eligible for redistribution upon rescaling. In other words, these objects are the finest granularity at which
non-keyed state can be redistributed. Depending on the state accessing method,
the following redistribution schemes are defined:

  - **Even-split redistribution:** Each operator returns a List of state elements. The whole state is logically a concatenation of
    all lists. On restore/redistribution, the list is evenly divided into as many sublists as there are parallel operators.
    Each operator gets a sublist, which can be empty, or contain one or more elements.
    As an example, if with parallelism 1 the checkpointed state of an operator
    contains elements `element1` and `element2`, when increasing the parallelism to 2, `element1` may end up in operator instance 0,
    while `element2` will go to operator instance 1.

  - **Union redistribution:** Each operator returns a List of state elements. The whole state is logically a concatenation of
    all lists. On restore/redistribution, each operator gets the complete list of state elements. Do not use this feature if
    your list may have high cardinality. Checkpoint metadata will store an offset to each list entry, which could lead to RPC
    framesize or out-of-memory errors.

Below is an example of a stateful `SinkFunction` that uses `CheckpointedFunction`
to buffer elements before sending them to the outside world. It demonstrates
the basic even-split redistribution list state:

{{< tabs "5b99663c-1801-486c-a085-ce66882a2k49" >}}
{{< tab "Java" >}}
```java
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
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        bufferedElements.add(value);
        if (bufferedElements.size() >= threshold) {
            for (Tuple2<String, Integer> element: bufferedElements) {
                // send it to the sink
            }
            bufferedElements.clear();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointedState.update(bufferedElements);
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
```
{{< /tab >}}
{{< /tabs >}}

The `initializeState` method takes as argument a `FunctionInitializationContext`. This is used to initialize
the non-keyed state "containers". These are a container of type `ListState` where the non-keyed state objects
are going to be stored upon checkpointing.

Note how the state is initialized, similar to keyed state,
with a `StateDescriptor` that contains the state name and information
about the type of the value that the state holds:


{{< tabs "e75410d8-4ea2-48e7-a7ef-249ece2a9997" >}}
{{< tab "Java" >}}
```java
ListStateDescriptor<Tuple2<String, Integer>> descriptor =
    new ListStateDescriptor<>(
        "buffered-elements",
        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

checkpointedState = context.getOperatorStateStore().getListState(descriptor);
```

{{< /tab >}}
{{< /tabs >}}
The naming convention of the state access methods contain its redistribution
pattern followed by its state structure. For example, to use list state with the
union redistribution scheme on restore, access the state by using `getUnionListState(descriptor)`.
If the method name does not contain the redistribution pattern, *e.g.* `getListState(descriptor)`,
it simply implies that the basic even-split redistribution scheme will be used.

After initializing the container, we use the `isRestored()` method of the context to check if we are
recovering after a failure. If this is `true`, *i.e.* we are recovering, the restore logic is applied.

As shown in the code of the modified `BufferingSink`, this `ListState` recovered during state
initialization is kept in a class variable for future use in `snapshotState()`. There the `ListState` is cleared
of all objects included by the previous checkpoint, and is then filled with the new ones we want to checkpoint.

As a side note, the keyed state can also be initialized in the `initializeState()` method. This can be done
using the provided `FunctionInitializationContext`.

{{< top >}}

## 从老的状态 API 迁移

从老的状态 API 迁移到新的状态 API 非常方便，请采用以下步骤：

1. 在  `KeyedStream` 上调用 `enableAsyncState()` 来启动新的状态 API.
2. 替换 `StateDescriptor` 为 `v2` 包下的 `StateDescriptor`。 同时， 替换旧的 state handle 为新的 state handle。
3. 用新的异步 API 重写旧的状态访问方法。
4. 使用新的状态 API 时，推荐使用 ForSt 状态后端，它支持异步状态访问。其他状态只支持同步的状态访问，尽管它们也可以和新的状态 API 一起使用。
