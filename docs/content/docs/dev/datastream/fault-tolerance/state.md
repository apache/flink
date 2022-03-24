---
title: "Working with State"
weight: 2
type: docs
aliases:
  - /dev/stream/state/state.html
  - /apis/streaming/state.html
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

# Working with State

In this section you will learn about the APIs that Flink provides for writing
stateful programs. Please take a look at [Stateful Stream
Processing]({{< ref "docs/concepts/stateful-stream-processing" >}})
to learn about the concepts behind stateful stream processing.

## Keyed DataStream

If you want to use keyed state, you first need to specify a key on a
`DataStream` that should be used to partition the state (and also the records
in the stream themselves). You can specify a key using `keyBy(KeySelector)`
in Java/Scala API or `key_by(KeySelector)` in Python API on a `DataStream`.
This will yield a `KeyedStream`, which then allows operations that use keyed state.

A key selector function takes a single record as input and returns the key for
that record. The key can be of any type and **must** be derived from
deterministic computations.

The data model of Flink is not based on key-value pairs. Therefore, you do not
need to physically pack the data set types into keys and values. Keys are
"virtual": they are defined as functions over the actual data to guide the
grouping operator.

The following example shows a key selector function that simply returns the
field of an object:

{{< tabs "54e3bde7-659a-4683-81fa-112a2c81036b" >}}
{{< tab "Java" >}}
```java
// some ordinary POJO
public class WC {
  public String word;
  public int count;

  public String getWord() { return word; }
}
DataStream<WC> words = // [...]
KeyedStream<WC> keyed = words
  .keyBy(WC::getWord);
```

{{< /tab >}}
{{< tab "Scala" >}}
```scala
// some ordinary case class
case class WC(word: String, count: Int)
val words: DataStream[WC] = // [...]
val keyed = words.keyBy( _.word )
```
{{< /tab >}}

{{< tab "Python" >}}
```python
words = # type: DataStream[Row]
keyed = words.key_by(lambda row: row[0])
```
{{< /tab >}}
{{< /tabs >}}

#### Tuple Keys and Expression Keys

Flink also has two alternative ways of defining keys: tuple keys and expression
keys in the Java/Scala API(still not supported in the Python API). With this you can
specify keys using tuple field indices or expressions
for selecting fields of objects. We don't recommend using these today but you
can refer to the Javadoc of DataStream to learn about them. Using a KeySelector
function is strictly superior: with Java lambdas they are easy to use and they
have potentially less overhead at runtime.

{{< top >}}

## Using Keyed State

The keyed state interfaces provides access to different types of state that are all scoped to
the key of the current input element. This means that this type of state can only be used
on a `KeyedStream`, which can be created via `stream.keyBy(…)` in Java/Scala API or `stream.key_by(…)` in Python API.

Now, we will first look at the different types of state available and then we will see
how they can be used in a program. The available state primitives are:

* `ValueState<T>`: This keeps a value that can be updated and
retrieved (scoped to key of the input element as mentioned above, so there will possibly be one value
for each key that the operation sees). The value can be set using `update(T)` and retrieved using
`T value()`.

* `ListState<T>`: This keeps a list of elements. You can append elements and retrieve an `Iterable`
over all currently stored elements. Elements are added using `add(T)` or `addAll(List<T>)`, the Iterable can
be retrieved using `Iterable<T> get()`. You can also override the existing list with `update(List<T>)`

* `ReducingState<T>`: This keeps a single value that represents the aggregation of all values
added to the state. The interface is similar to `ListState` but elements added using
`add(T)` are reduced to an aggregate using a specified `ReduceFunction`.

* `AggregatingState<IN, OUT>`: This keeps a single value that represents the aggregation of all values
added to the state. Contrary to `ReducingState`, the aggregate type may be different from the type
of elements that are added to the state. The interface is the same as for `ListState` but elements
added using `add(IN)` are aggregated using a specified `AggregateFunction`.

* `MapState<UK, UV>`: This keeps a list of mappings. You can put key-value pairs into the state and
retrieve an `Iterable` over all currently stored mappings. Mappings are added using `put(UK, UV)` or
`putAll(Map<UK, UV>)`. The value associated with a user key can be retrieved using `get(UK)`. The iterable
views for mappings, keys and values can be retrieved using `entries()`, `keys()` and `values()` respectively.
You can also use `isEmpty()` to check whether this map contains any key-value mappings.

All types of state also have a method `clear()` that clears the state for the currently
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

{{< tabs "348fd48c-fa36-4d27-9e53-9bffb74d8a86" >}}
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
        .keyBy(value -> value.f0)
        .flatMap(new CountWindowAverage())
        .print();

// the printed output will be (1,4) and (1,5)
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
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

  env.execute("ExampleKeyedState")
}
```
{{< /tab >}}

{{< tab "Python" >}}
```python
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment, FlatMapFunction, RuntimeContext
from pyflink.datastream.state import ValueStateDescriptor

class CountWindowAverage(FlatMapFunction):

    def __init__(self):
        self.sum = None

    def open(self, runtime_context: RuntimeContext):
        descriptor = ValueStateDescriptor(
            "average",  # the state name
            Types.PICKLED_BYTE_ARRAY()  # type information
        )
        self.sum = runtime_context.get_state(descriptor)

    def flat_map(self, value):
        # access the state value
        current_sum = self.sum.value()
        if current_sum is None:
            current_sum = (0, 0)

        # update the count
        current_sum = (current_sum[0] + 1, current_sum[1] + value[1])

        # update the state
        self.sum.update(current_sum)

        # if the count reaches 2, emit the average and clear the state
        if current_sum[0] >= 2:
            self.sum.clear()
            yield value[0], int(current_sum[1] / current_sum[0])


env = StreamExecutionEnvironment.get_execution_environment()
env.from_collection([(1, 3), (1, 5), (1, 7), (1, 4), (1, 2)]) \
    .key_by(lambda row: row[0]) \
    .flat_map(CountWindowAverage()) \
    .print()

env.execute()

# the printed output will be (1,4) and (1,5)
```
{{< /tab >}}
{{< /tabs >}}

This example implements a poor man's counting window. We key the tuples by the first field
(in the example all have the same key `1`). The function stores the count and a running sum in
a `ValueState`. Once the count reaches 2 it will emit the average and clear the state so that
we start over from `0`. Note that this would keep a different state value for each different input
key if we had tuples with different values in the first field.

### State Time-To-Live (TTL)

A *time-to-live* (TTL) can be assigned to the keyed state of any type. If a TTL is configured and a
state value has expired, the stored value will be cleaned up on a best effort basis which is
discussed in more detail below.

All state collection types support per-entry TTLs. This means that list elements and map entries
expire independently.

In order to use state TTL one must first build a `StateTtlConfig` configuration object. The TTL 
functionality can then be enabled in any state descriptor by passing the configuration:

{{< tabs "43221f5b-b56c-42e5-833e-ed416e9d9b4e" >}}
{{< tab "Java" >}}
```java
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
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
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
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.common.time import Time
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import ValueStateDescriptor, StateTtlConfig

ttl_config = StateTtlConfig \
  .new_builder(Time.seconds(1)) \
  .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
  .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired) \
  .build()

state_descriptor = ValueStateDescriptor("text state", Types.STRING())
state_descriptor.enable_time_to_live(ttl_config)
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
and a primitive long value in memory. The RocksDB state backend adds 8 bytes per stored value, list entry or map entry.

- Only TTLs in reference to *processing time* are currently supported.

- Trying to restore state, which was previously configured without TTL, using TTL enabled descriptor or vice versa
will lead to compatibility failure and `StateMigrationException`.

- The TTL configuration is not part of check- or savepoints but rather a way of how Flink treats it in the currently running job.

- The map state with TTL currently supports null user values only if the user value serializer can handle null values. 
If the serializer does not support null values, it can be wrapped with `NullableSerializer` at the cost of an extra byte in the serialized form.

- With TTL enabled configuration, the `defaultValue` in `StateDescriptor`, which is atucally already deprecated, will no longer take an effect. This aims to make the semantics more clear and let user manually manage the default value if the contents of the state is null or expired.

#### Cleanup of Expired State

By default, expired values are explicitly removed on read, such as `ValueState#value`, and periodically garbage collected
in the background if supported by the configured state backend. Background cleanup can be disabled in the `StateTtlConfig`:

{{< tabs "dfad4bf4-84ec-49a9-8372-c63b59122603" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.state.StateTtlConfig;
StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .disableCleanupInBackground()
    .build();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.common.state.StateTtlConfig
val ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .disableCleanupInBackground
    .build
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.common.time import Time
from pyflink.datastream.state import StateTtlConfig

ttl_config = StateTtlConfig \
  .new_builder(Time.seconds(1)) \
  .disable_cleanup_in_background() \
  .build()
```
{{< /tab >}}
{{< /tabs >}}

For more fine-grained control over some special cleanup in background, you can configure it separately as described below.
Currently, heap state backend relies on incremental cleanup and RocksDB backend uses compaction filter for background cleanup.

##### Cleanup in full snapshot

Additionally, you can activate the cleanup at the moment of taking the full state snapshot which 
will reduce its size. The local state is not cleaned up under the current implementation 
but it will not include the removed expired state in case of restoration from the previous snapshot.
It can be configured in `StateTtlConfig`:

{{< tabs "ad8a4c67-222c-4beb-9547-ca5f8085695c" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .cleanupFullSnapshot()
    .build();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.common.state.StateTtlConfig
import org.apache.flink.api.common.time.Time

val ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .cleanupFullSnapshot
    .build
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.common.time import Time
from pyflink.datastream.state import StateTtlConfig

ttl_config = StateTtlConfig \
  .new_builder(Time.seconds(1)) \
  .cleanup_full_snapshot() \
  .build()
```
{{< /tab >}}
{{< /tabs >}}

This option is not applicable for the incremental checkpointing in the RocksDB state backend.

{{< hint info >}}
For existing jobs, this cleanup strategy can be activated or deactivated anytime in `StateTtlConfig`, 
e.g. after restart from savepoint.
{{< /hint >}}

##### Incremental cleanup

Another option is to trigger cleanup of some state entries incrementally.
The trigger can be a callback from each state access or/and each record processing.
If this cleanup strategy is active for certain state,
The storage backend keeps a lazy global iterator for this state over all its entries.
Every time incremental cleanup is triggered, the iterator is advanced.
The traversed state entries are checked and expired ones are cleaned up.

This feature can be configured in `StateTtlConfig`:

{{< tabs "e8c96bb9-6485-4f86-8fdb-5e538e538827" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.state.StateTtlConfig;
 StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .cleanupIncrementally(10, true)
    .build();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.common.state.StateTtlConfig
val ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .cleanupIncrementally(10, true)
    .build
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.common.time import Time
from pyflink.datastream.state import StateTtlConfig

ttl_config = StateTtlConfig \
  .new_builder(Time.seconds(1)) \
  .cleanup_incrementally(10, True) \
  .build()
```
{{< /tab >}}
{{< /tabs >}}

This strategy has two parameters. The first one is number of checked state entries per each cleanup triggering.
It is always triggered per each state access.
The second parameter defines whether to trigger cleanup additionally per each record processing.
The default background cleanup for heap backend checks 5 entries without cleanup per record processing.

**Notes:**
- If no access happens to the state or no records are processed, expired state will persist.
- Time spent for the incremental cleanup increases record processing latency.
- At the moment incremental cleanup is implemented only for Heap state backend. Setting it for RocksDB will have no effect.
- If heap state backend is used with synchronous snapshotting, the global iterator keeps a copy of all keys 
while iterating because of its specific implementation which does not support concurrent modifications. 
Enabling of this feature will increase memory consumption then. Asynchronous snapshotting does not have this problem.
- For existing jobs, this cleanup strategy can be activated or deactivated anytime in `StateTtlConfig`, 
e.g. after restart from savepoint.

##### Cleanup during RocksDB compaction

If the RocksDB state backend is used, a Flink specific compaction filter will be called for the background cleanup.
RocksDB periodically runs asynchronous compactions to merge state updates and reduce storage.
Flink compaction filter checks expiration timestamp of state entries with TTL
and excludes expired values. 

This feature can be configured in `StateTtlConfig`:

{{< tabs "4db505be-2c9c-4ef7-aa01-74763968b7a1" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.state.StateTtlConfig;

StateTtlConfig ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .cleanupInRocksdbCompactFilter(1000)
    .build();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.common.state.StateTtlConfig

val ttlConfig = StateTtlConfig
    .newBuilder(Time.seconds(1))
    .cleanupInRocksdbCompactFilter(1000)
    .build
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.common.time import Time
from pyflink.datastream.state import StateTtlConfig

ttl_config = StateTtlConfig \
  .new_builder(Time.seconds(1)) \
  .cleanup_in_rocksdb_compact_filter(1000) \
  .build()
```
{{< /tab >}}
{{< /tabs >}}

RocksDB compaction filter will query current timestamp, used to check expiration, from Flink every time 
after processing certain number of state entries.
You can change it and pass a custom value to 
`StateTtlConfig.newBuilder(...).cleanupInRocksdbCompactFilter(long queryTimeAfterNumEntries)` method. 
Updating the timestamp more often can improve cleanup speed 
but it decreases compaction performance because it uses JNI call from native code.
The default background cleanup for RocksDB backend queries the current timestamp each time 1000 entries have been processed.

You can activate debug logs from the native code of RocksDB filter 
by activating debug level for `FlinkCompactionFilter`:

`log4j.logger.org.rocksdb.FlinkCompactionFilter=DEBUG`

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

### State in the Scala DataStream API

In addition to the interface described above, the Scala API has shortcuts for stateful
`map()` or `flatMap()` functions with a single `ValueState` on `KeyedStream`. The user function
gets the current value of the `ValueState` in an `Option` and must return an updated value that
will be used to update the state.

```scala
val stream: DataStream[(String, Int)] = ...

val counts: DataStream[(String, Int)] = stream
  .keyBy(_._1)
  .mapWithState((in: (String, Int), count: Option[Int]) =>
    count match {
      case Some(c) => ( (in._1, c), Some(c + in._2) )
      case None => ( (in._1, 0), Some(in._2) )
    })
```

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

**Notes:** Operator state is still not supported in Python DataStream API.

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

**Notes:** Broadcast state is still not supported in Python DataStream API.

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

{{< tabs "5b99313c-1801-486c-a085-ce66882a2e49" >}}
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
    public void invoke(Tuple2<String, Integer> value, Context contex) throws Exception {
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
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class BufferingSink(threshold: Int = 0)
  extends SinkFunction[(String, Int)]
    with CheckpointedFunction {

  @transient
  private var checkpointedState: ListState[(String, Int)] = _

  private val bufferedElements = ListBuffer[(String, Int)]()

  override def invoke(value: (String, Int), context: Context): Unit = {
    bufferedElements += value
    if (bufferedElements.size >= threshold) {
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
      for(element <- checkpointedState.get().asScala) {
        bufferedElements += element
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
{{< tab "Scala" >}}
```scala

val descriptor = new ListStateDescriptor[(String, Long)](
    "buffered-elements",
    TypeInformation.of(new TypeHint[(String, Long)]() {})
)

checkpointedState = context.getOperatorStateStore.getListState(descriptor)

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

### Stateful Source Functions

Stateful sources require a bit more care as opposed to other operators.
In order to make the updates to the state and output collection atomic (required for exactly-once semantics
on failure/recovery), the user is required to get a lock from the source's context.

{{< tabs "aa821e89-7cc6-4c1e-be5b-90bfa0df6036" >}}
{{< tab "Java" >}}
```java
public static class CounterSource
        extends RichParallelSourceFunction<Long>
        implements CheckpointedFunction {

    /**  current offset for exactly once semantics */
    private Long offset = 0L;

    /** flag for job cancellation */
    private volatile boolean isRunning = true;
    
    /** Our state object. */
    private ListState<Long> state;

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
    public void initializeState(FunctionInitializationContext context) throws Exception {
        state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
                "state",
                LongSerializer.INSTANCE));
                
        // restore any state that we might already have to our fields, initialize state
        // is also called in case of restore.
        for (Long l : state.get()) {
            offset = l;
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        state.clear();
        state.add(offset);
    }
}
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
class CounterSource
       extends RichParallelSourceFunction[Long]
       with CheckpointedFunction {

  @volatile
  private var isRunning = true

  private var offset = 0L
  private var state: ListState[Long] = _

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
  
  override def initializeState(context: FunctionInitializationContext): Unit = {
    state = context.getOperatorStateStore.getListState(
      new ListStateDescriptor[Long]("state", classOf[Long]))

    for (l <- state.get().asScala) {
      offset = l
    }
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    state.clear()
    state.add(offset)
  }
}
```
{{< /tab >}}
{{< /tabs >}}

Some operators might need the information when a checkpoint is fully acknowledged by Flink to communicate that with the outside world. In this case see the `org.apache.flink.api.common.state.CheckpointListener` interface.

{{< top >}}
