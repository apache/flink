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

# Working with State V2 (New APIs)

In this section you will learn about the new APIs that Flink provides for writing
stateful programs. Please take a look at [Stateful Stream
Processing]({{< ref "docs/concepts/stateful-stream-processing" >}})
to learn about the concepts behind stateful stream processing.

The new state API is designed to be more flexible than the previous API. User can perform
asynchronous state operations, thus making it more powerful and more efficient.
The asynchronous state access is essential for the state backend to be able to handle
large state sizes and to be able to spill to remote file systems when necessary.
This is called the 'disaggregated state management'. For more information about this, 
please see [Disaggregated State Management]({{< ref "docs/ops/state/disaggregated_state" >}}).

## Keyed DataStream

If you want to use keyed state, you first need to specify a key on a
`DataStream` that should be used to partition the state (and also the records
in the stream themselves). You can specify a key using `keyBy(KeySelector)`
in Java API on a `DataStream`. This will yield a `KeyedStream`, which then allows operations
that use keyed state. You should perform `enableAsyncState()` on the `KeyedStream` to enable
asynchronous state operations.

A key selector function takes a single record as input and returns the key for
that record. The key can be of any type and **must** be derived from
deterministic computations.

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

## Using Keyed State V2

Unlike the previous state API, the new state API is designed for asynchronous state access.
Each type of state gives two versions of the API: synchronous and asynchronous. The synchronous
API is a blocking API that waits for the state access to complete. The asynchronous API is
non-blocking and returns a `StateFuture` that will be completed when the state access
is done. After that a callback or following logic will be invoked (if any).
The asynchronous API is more efficient and should be used whenever possible.
It is highly **not** recommended to mixed synchronous and asynchronous state access in the same
user function.

The keyed state interfaces provide access to different types of state that are all scoped to
the key of the current input element. This means that this type of state can only be used
on a `KeyedStream`, which can be created via `stream.keyBy(…)` in Java. And then most importantly,
the keyed stream needs to be enabled for asynchronous state access by calling `enableAsyncState()`.
The new API set are only available on `KeyedStream` with `enableAsyncState()` invoked.

Now, we will look at the different types of state available, and then we will see
how they can be used in a program. Since the synchronous APIs are identical with the original APIs,
We only focus on the asynchronous ones here. 

### The Return Values

First of all, we should get familiar with the return value of those asynchronous state access methods.

`StateFuture<T>` is a future that will be completed with the result of the state access.
The return bype is T. It provides multiple methods to handle the result, listed as:
* `StateFuture<Void> thenAccept(Consumer<T>)`: This method takes a `Consumer` that will be called with the result
  when the state access is done. It returns a `StateFuture<Void>`, which will be finished when the
  `Consumer` is done.
* `StateFuture<R> thenApply(Function<T, R>)`: This method takes a `Function` that will be called with the result
  when the state access is done. The return value of the function will be the result of the following
  `StateFuture`, which will be finished when the `Function` is done.
* `StateFuture<R> thenCompose(Function<T, StateFuture<R>>)`: This method takes a `Function` that will
  be called with the result when the state access is done. The return value of the function should be
  a `StateFuture<R>`, which is exposed to the invoker of `thenCompose` as a return value.
  The `StateFuture<R>` will be finished when the inner `StateFuture<R>` of `Function` is finished.
* `StateFuture<R> thenCombine(StateFuture<U>, BiFunction<T, U, R>)`: This method takes another `StateFuture` and a
  `BiFunction` that will be called with the results of both `StateFuture`s when they are done. The return
  value of the `BiFunction` will be the result of the following `StateFuture`, which will be finished when
  the `BiFunction` is done.

Those methods are similar to the corresponding ones of the `CompletableFuture`. Besides these methods,
keep in mind that `StateFuture` does not provide a `get()` method to block the current thread until the
state access is done. This is because blocking the current thread may cause recursive blocking. The 
`StateFuture<T>` also provides conditional version of `thenAccept`, `thenApply`, `thenCompose` and `thenCombine`,
which is for the case that the state access is done and the following logic will be split into two branches
based on the result of the state access. The conditional version of those methods are `thenConditionallyAccept`,
`thenConditionallyApply`, `thenConditionallyCompose` and `thenConditionallyCombine`.

`StateIterator<T>` is an iterator that can be used to iterate over the elements of a state. It provides
the following methods:
* `boolean isEmpty()` : A synchronous method returns a `true` if the iterator has no elements, and `false` otherwise.
* `StateFuture<Void> onNext(Consumer<T>)` : This method takes a `Consumer` that will be called with the next element
  when the state access is done. It returns a `StateFuture<Void>`, which will be finished when the `Consumer` is done.
  Also, a function version of `onNext` is provided, which is `StateFuture<Collection<R>> onNext(Function<T, R>)`. This method
  takes a `Function` that will be called with the next element when the state access is done. The return value of the function 
  will be collected and returned as a collection of the following `StateFuture`, which will be
  finished when the `Function` is done.

We also provide a `StateFutureUtils` class that contains some utility methods to handle `StateFuture`s.
These methods are:
* `StateFuture<T> completedFuture(T)`: This method returns a completed `StateFuture` with the given value. This is
useful when you want to return a constant value in a `thenCompose` method for further processing.
* `StateFuture<Void> completedVoidFuture()`: This method returns a completed `StateFuture` with `null` value. 
A void value version of `completedFuture`.
* `StateFuture<Collection<T>> combineAll(Collection<StateFuture<T>>)` : This method takes a collection of `StateFuture`s
and returns a `StateFuture` that will be completed when all the input `StateFuture`s are completed. The result of the
returned `StateFuture` is a collection of the results of the input `StateFuture`s. This is useful when you want to
combine the results of multiple `StateFuture`s.
* `StateFuture<Iterable<T>> toIterable(StateFuture<StateIterator<T>>)` : This method takes a `StateFuture` of `StateIterator`
and returns a `StateFuture` of `Iterable`. The result of the returned `StateFuture` is an `Iterable` that contains all
the elements of the `StateIterator`. This is useful when you want to convert a `StateIterator` to an `Iterable`.
There is no good reason to do so, since this may disable the capability of lazy loading. Only useful when the further 
calculation depends on the whole data from the iterator.


### State Primitives

The available state primitives are:

* `ValueState<T>`: This keeps a value that can be updated and
retrieved (scoped to key of the input element as mentioned above, so there will possibly be one value
for each key that the operation sees). The value can be set using `asyncUpdate(T)` and retrieved using
`StateFuture<T> asyncValue()`.

* `ListState<T>`: This keeps a list of elements. You can append elements and retrieve an `StateIterator`
over all currently stored elements. Elements are added using `asyncAdd(T)` or `asyncAddAll(List<T>)`,
the Iterable can be retrieved using `StateFuture<StateIterator<T>> asyncGet()`.
You can also override the existing list with `asyncUpdate(List<T>)`

* `ReducingState<T>`: This keeps a single value that represents the aggregation of all values
added to the state. The interface is similar to `ListState` but elements added using
`asyncAdd(T)` are reduced to an aggregate using a specified `ReduceFunction`.

* `AggregatingState<IN, OUT>`: This keeps a single value that represents the aggregation of all values
added to the state. Contrary to `ReducingState`, the aggregate type may be different from the type
of elements that are added to the state. The interface is the same as for `ListState` but elements
added using `asyncAdd(IN)` are aggregated using a specified `AggregateFunction`.

* `MapState<UK, UV>`: This keeps a list of mappings. You can put key-value pairs into the state and
retrieve an `StateIterator` over all currently stored mappings. Mappings are added using `asyncPut(UK, UV)` or
`asyncPutAll(Map<UK, UV>)`. The value associated with a user key can be retrieved using `asyncGet(UK)`.
The iterable views for mappings, keys and values can be retrieved using `asyncEntries()`,
`asyncKeys()` and `asyncValues()` respectively. You can also use `asyncIsEmpty()` to check whether
this map contains any key-value mappings.

All types of state also have a method `asyncClear()` that clears the state for the currently
active key, i.e. the key of the input element.

It is important to keep in mind that these state objects are only used for interfacing
with state. The state is not necessarily stored inside but might reside on disk or somewhere else.
The second thing to keep in mind is that the value you get from the state
depends on the key of the input element. So the value you get in one invocation of your
user function can differ from the value in another invocation if the keys involved are different.

To get a state handle, you have to create a `StateDescriptor`. This holds the name of the state
(as we will see later, you can create several states, and they have to have unique names so
that you can reference them), the type of the values that the state holds, and possibly
a user-specified function, such as a `ReduceFunction`. Depending on what type of state you
want to retrieve, you create either a `ValueStateDescriptor`, a `ListStateDescriptor`, 
an `AggregatingStateDescriptor`, a `ReducingStateDescriptor`, or a `MapStateDescriptor`.
To differentiate between the previous state APIs, you should use the `StateDescriptor`s under the
`org.apache.flink.api.common.state.v2` package (note the **v2**).

State is accessed using the `RuntimeContext`, so it is only possible in *rich functions*.
Please see [here]({{< ref "docs/dev/datastream/user_defined_functions" >}}#rich-functions) for
information about that, but we will also see an example shortly. The `RuntimeContext` that
is available in a `RichFunction` has these methods for accessing state:

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

### Execution Order

The state access methods are executed asynchronously. This means that the state access methods
will not block the current thread. With synchronous APIs, the state access methods will be executed in
the order they are called. However, with asynchronous APIs, the state access methods will be executed
out of order, especially for those invokes for different incoming elements. For the above example,
if the `flatMap` function is invoked for two different incoming elements A and B, the state access
methods for A and B will be executed. Firstly, `asyncGet` is executed for A, then `asyncGet` is
allowed to execute for B. The finish order of the two `asyncGet` is not guaranteed. Thus the order
of continuation of the two `StateFuture`s is not guaranteed. Thus invokes of `asyncClear` or
`asyncUpdate`for A and B are not determined.

Although the state access methods are executed out of order, this not mean that all the user code
are run in parallel. The user code in the `processElement`, `flatMap` or `thenXXxx` methods
following the state access methods will be executed in a single thread (the task thread). So there
is no concurrency issue for the user code.

Typically, you don't need to worry about the execution order of the state access methods, but there
is still some rules the Flink will ensure:
* The execution order of user code entry `flatMap` for same-key elements are invoked strictly in
order of element arrival.
* The consumers or functions passed to the `thenXXxx` methods are executed in the order they are
chained. If they are not chained, or there are multiple chains, the order is not guaranteed.

### Best practice of asynchronous APIs

The asynchronous APIs are designed to be more efficient and more powerful than the synchronous APIs.
There are some best practices that you should follow when using the asynchronous APIs:
* **Avoid mixing synchronous and asynchronous state access**
* Use chaining of `thenXXxx` methods to handle the result of the state access and then gives another
    state access or result. Divide the logic into multiple steps split by `thenXXxx` methods.
* Avoid accessing mutable members of the user function (`RichFlatMapFunction`). Since the state access
    methods are executed out of order, the mutable members may be accessed in unpredictable order.
    Instead, use the result of the state access to pass the data between different steps. The 
    `StateFutureUtils.completedFuture` or `thenApply` method can be used to pass the data. Or use
    a captured container (`AtomicReference`) which is initialized for each invoke of the `flatMap`
    to share between lambdas.


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

## Operator State

*Operator State* (or *non-keyed state*) is state that is bound to one
parallel operator instance. The [Kafka Connector]({{< ref "docs/connectors/datastream/kafka" >}}) is a good motivating example for the use of
Operator State in Flink. Each parallel instance of the Kafka consumer maintains
a map of topic partitions and offsets as its Operator State.

The Operator State interfaces support redistributing state among parallel
operator instances when the parallelism is changed. There are different schemes
for doing this redistribution.

In a typical stateful Flink Application you don't need operators state. It is
mostly a special type of state that is used in source/sink implementations and
scenarios where you don't have a key by which state can be partitioned.

## Broadcast State

*Broadcast State* is a special type of *Operator State*.  It was introduced to
support use cases where records of one stream need to be broadcasted to all
downstream tasks, where they are used to maintain the same state among all
subtasks. This state can then be accessed while processing records of a second
stream. As an example where broadcast state can emerge as a natural fit, one
can imagine a low-throughput stream containing a set of rules which we want to
evaluate against all elements coming from another stream. Having the above type
of use cases in mind, broadcast state differs from the rest of operator states
in that:

 1. it has a map format,
 2. it is only available to specific operators that have as inputs a
    *broadcasted* stream and a *non-broadcasted* one, and
 3. such an operator can have *multiple broadcast states* with different names.

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

## Migrate from the Old State API

It is very easy to migrate from the old state API to the new one. Please take the following steps:

1. Invoke `enableAsyncState()` on the `KeyedStream` to enable the new state API.
2. Replace the `StateDescriptor`s with the new ones under `v2` package. Also, replace the old state
handles with the new ones under `v2` package.
3. Rewrite the old state access methods with the new asynchronous ones.
4. It is recommended to use ForSt State Backend for the new state API, which could perform 
async state access. Other state backends only support synchronous execution of state access,
although it can be used with the new state API.
