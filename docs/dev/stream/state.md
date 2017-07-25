---
title: "Working with State"
nav-parent_id: streaming
nav-pos: 40
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

* ToC
{:toc}

Stateful functions and operators store data across the processing of individual elements/events, making state a critical building block for
any type of more elaborate operation. For example:

  - When an application searches for certain event patterns, the state will store the sequence of events encountered so far.
  - When aggregating events per minute, the state holds the pending aggregates.
  - When training a machine learning model over a stream of data points, the state holds the current version of the model parameters.

In order to make state fault tolerant, Flink needs to be aware of the state and [checkpoint](checkpointing.html) it.
In many cases, Flink can also *manage* the state for the application, meaning Flink deals with the memory management (possibly spilling to disk
if necessary) to allow applications to hold very large state.

This document explains how to use Flink's state abstractions when developing an application.


## Keyed State and Operator State

There are two basic kinds of state in Flink: `Keyed State` and `Operator State`.

### Keyed State

*Keyed State* is always relative to keys and can only be used in functions and operators on a `KeyedStream`.

You can think of Keyed State as Operator State that has been partitioned,
or sharded, with exactly one state-partition per key.
Each keyed-state is logically bound to a unique
composite of <parallel-operator-instance, key>, and since each key
"belongs" to exactly one parallel instance of a keyed operator, we can
think of this simply as <operator, key>.

Keyed State is further organized into so-called *Key Groups*. Key Groups are the
atomic unit by which Flink can redistribute Keyed State;
there are exactly as many Key Groups as the defined maximum parallelism.
During execution each parallel instance of a keyed operator works with the keys
for one or more Key Groups.

### Operator State

With *Operator State* (or *non-keyed state*), each operator state is
bound to one parallel operator instance.
The [Kafka Connector](../connectors/kafka.html) is a good motivating example for the use of Operator State
in Flink. Each parallel instance of the Kafka consumer maintains a map
of topic partitions and offsets as its Operator State.

The Operator State interfaces support redistributing state among
parallel operator instances when the parallelism is changed. There can be different schemes for doing this redistribution.

## Raw and Managed State

*Keyed State* and *Operator State* exist in two forms: *managed* and *raw*.

*Managed State* is represented in data structures controlled by the Flink runtime, such as internal hash tables, or RocksDB.
Examples are "ValueState", "ListState", etc. Flink's runtime encodes
the states and writes them into the checkpoints.

*Raw State* is state that operators keep in their own data structures. When checkpointed, they only write a sequence of bytes into
the checkpoint. Flink knows nothing about the state's data structures and sees only the raw bytes.

All datastream functions can use managed state, but the raw state interfaces can only be used when implementing operators.
Using managed state (rather than raw state) is recommended, since with
managed state Flink is able to automatically redistribute state when the parallelism is
changed, and also do better memory management.

## Using Managed Keyed State

The managed keyed state interface provides access to different types of state that are all scoped to
the key of the current input element. This means that this type of state can only be used
on a `KeyedStream`, which can be created via `stream.keyBy(…)`.

Now, we will first look at the different types of state available and then we will see
how they can be used in a program. The available state primitives are:

* `ValueState<T>`: This keeps a value that can be updated and
retrieved (scoped to key of the input element as mentioned above, so there will possibly be one value
for each key that the operation sees). The value can be set using `update(T)` and retrieved using
`T value()`.

* `ListState<T>`: This keeps a list of elements. You can append elements and retrieve an `Iterable`
over all currently stored elements. Elements are added using `add(T)`, the Iterable can
be retrieved using `Iterable<T> get()`.

* `ReducingState<T>`: This keeps a single value that represents the aggregation of all values
added to the state. The interface is the same as for `ListState` but elements added using
`add(T)` are reduced to an aggregate using a specified `ReduceFunction`.

* `FoldingState<T, ACC>`: This keeps a single value that represents the aggregation of all values
added to the state. Contrary to `ReducingState`, the aggregate type may be different from the type
of elements that are added to the state. The interface is the same as for `ListState` but elements
added using `add(T)` are folded into an aggregate using a specified `FoldFunction`.

* `MapState<UK, UV>`: This keeps a list of mappings. You can put key-value pairs into the state and
retrieve an `Iterable` over all currently stored mappings. Mappings are added using `put(UK, UV)` or
`putAll(Map<UK, UV>)`. The value associated with a user key can be retrieved using `get(UK)`. The iterable
views for mappings, keys and values can be retrieved using `entries()`, `keys()` and `values()` respectively.

All types of state also have a method `clear()` that clears the state for the currently
active key, i.e. the key of the input element.

<span class="label label-danger">Attention</span> `FoldingState` will be deprecated in one of
the next versions of Flink and will be completely removed in the future. A more general
alternative will be provided.

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
a `ReducingStateDescriptor`, a `FoldingStateDescriptor` or a `MapStateDescriptor`.

State is accessed using the `RuntimeContext`, so it is only possible in *rich functions*.
Please see [here]({{ site.baseurl }}/dev/api_concepts.html#rich-functions) for
information about that, but we will also see an example shortly. The `RuntimeContext` that
is available in a `RichFunction` has these methods for accessing state:

* `ValueState<T> getState(ValueStateDescriptor<T>)`
* `ReducingState<T> getReducingState(ReducingStateDescriptor<T>)`
* `ListState<T> getListState(ListStateDescriptor<T>)`
* `FoldingState<T, ACC> getFoldingState(FoldingStateDescriptor<T, ACC>)`
* `MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV>)`

This is an example `FlatMapFunction` that shows how all of the parts fit together:

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

This example implements a poor man's counting window. We key the tuples by the first field
(in the example all have the same key `1`). The function stores the count and a running sum in
a `ValueState`. Once the count reaches 2 it will emit the average and clear the state so that
we start over from `0`. Note that this would keep a different state value for each different input
key if we had tuples with different values in the first field.

### State in the Scala DataStream API

In addition to the interface described above, the Scala API has shortcuts for stateful
`map()` or `flatMap()` functions with a single `ValueState` on `KeyedStream`. The user function
gets the current value of the `ValueState` in an `Option` and must return an updated value that
will be used to update the state.

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

## Using Managed Operator State

To use managed operator state, a stateful function can implement either the more general `CheckpointedFunction`
interface, or the `ListCheckpointed<T extends Serializable>` interface.

#### CheckpointedFunction

The `CheckpointedFunction` interface provides access to non-keyed state with different
redistribution schemes. It requires the implementation of two methods:

{% highlight java %}
void snapshotState(FunctionSnapshotContext context) throws Exception;

void initializeState(FunctionInitializationContext context) throws Exception;
{% endhighlight %}

Whenever a checkpoint has to be performed, `snapshotState()` is called. The counterpart, `initializeState()`,
is called every time the user-defined function is initialized, be that when the function is first initialized
or be that when the function is actually recovering from an earlier checkpoint. Given this, `initializeState()` is not
only the place where different types of state are initialized, but also where state recovery logic is included.

Currently, list-style managed operator state is supported. The state
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
    all lists. On restore/redistribution, each operator gets the complete list of state elements.

Below is an example of a stateful `SinkFunction` that uses `CheckpointedFunction`
to buffer elements before sending them to the outside world. It demonstrates
the basic even-split redistribution list state:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class BufferingSink
        implements SinkFunction<Tuple2<String, Integer>>,
                   CheckpointedFunction,
                   CheckpointedRestoring<ArrayList<Tuple2<String, Integer>>> {

    private final int threshold;

    private transient ListState<Tuple2<String, Integer>> checkpointedState;

    private List<Tuple2<String, Integer>> bufferedElements;

    public BufferingSink(int threshold) {
        this.threshold = threshold;
        this.bufferedElements = new ArrayList<>();
    }

    @Override
    public void invoke(Tuple2<String, Integer> value) throws Exception {
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
                TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));

        checkpointedState = context.getOperatorStateStore().getListState(descriptor);

        if (context.isRestored()) {
            for (Tuple2<String, Integer> element : checkpointedState.get()) {
                bufferedElements.add(element);
            }
        }
    }

    @Override
    public void restoreState(ArrayList<Tuple2<String, Integer>> state) throws Exception {
        // this is from the CheckpointedRestoring interface.
        this.bufferedElements.addAll(state);
    }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
class BufferingSink(threshold: Int = 0)
  extends SinkFunction[(String, Int)]
    with CheckpointedFunction
    with CheckpointedRestoring[List[(String, Int)]] {

  @transient
  private var checkpointedState: ListState[(String, Int)] = null

  private val bufferedElements = ListBuffer[(String, Int)]()

  override def invoke(value: (String, Int)): Unit = {
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

  override def restoreState(state: List[(String, Int)]): Unit = {
    bufferedElements ++= state
  }
}
{% endhighlight %}
</div>
</div>

The `initializeState` method takes as argument a `FunctionInitializationContext`. This is used to initialize
the non-keyed state "containers". These are a container of type `ListState` where the non-keyed state objects
are going to be stored upon checkpointing.

Note how the state is initialized, similar to keyed state,
with a `StateDescriptor` that contains the state name and information
about the type of the value that the state holds:


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

#### ListCheckpointed

The `ListCheckpointed` interface is a more limited variant of `CheckpointedFunction`,
which only supports list-style state with even-split redistribution scheme on restore.
It also requires the implementation of two methods:

{% highlight java %}
List<T> snapshotState(long checkpointId, long timestamp) throws Exception;

void restoreState(List<T> state) throws Exception;
{% endhighlight %}

On `snapshotState()` the operator should return a list of objects to checkpoint and
`restoreState` has to handle such a list upon recovery. If the state is not re-partitionable, you can always
return a `Collections.singletonList(MY_STATE)` in the `snapshotState()`.

### Stateful Source Functions

Stateful sources require a bit more care as opposed to other operators.
In order to make the updates to the state and output collection atomic (required for exactly-once semantics
on failure/recovery), the user is required to get a lock from the source's context.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public static class CounterSource
        extends RichParallelSourceFunction<Long>
        implements ListCheckpointed<Long> {

    /**  current offset for exactly once semantics */
    private Long offset;

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

Some operators might need the information when a checkpoint is fully acknowledged by Flink to communicate that with the outside world. In this case see the `org.apache.flink.runtime.state.CheckpointListener` interface.

## Custom Serialization for Managed State

This section is targeted as a guideline for users who require the use of custom serialization for their state, covering how
to provide a custom serializer and how to handle upgrades to the serializer for compatibility. If you're simply using
Flink's own serializers, this section is irrelevant and can be skipped.

### Using custom serializers

As demonstrated in the above examples, when registering a managed operator or keyed state, a `StateDescriptor` is required
to specify the state's name, as well as information about the type of the state. The type information is used by Flink's
[type serialization framework](../types_serialization.html) to create appropriate serializers for the state.

It is also possible to completely bypass this and let Flink use your own custom serializer to serialize managed states,
simply by directly instantiating the `StateDescriptor` with your own `TypeSerializer` implementation:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
public class CustomTypeSerializer extends TypeSerializer<Tuple2<String, Integer>> {...};

ListStateDescriptor<Tuple2<String, Integer>> descriptor =
    new ListStateDescriptor<>(
        "state-name",
        new CustomTypeSerializer());

checkpointedState = getRuntimeContext().getListState(descriptor);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
class CustomTypeSerializer extends TypeSerializer[(String, Integer)] {...}

val descriptor = new ListStateDescriptor[(String, Integer)](
    "state-name",
    new CustomTypeSerializer)
)

checkpointedState = getRuntimeContext.getListState(descriptor);
{% endhighlight %}
</div>
</div>

Note that Flink writes state serializers along with the state as metadata. In certain cases on restore (see following
subsections), the written serializer needs to be deserialized and used. Therefore, it is recommended to avoid using
anonymous classes as your state serializers. Anonymous classes do not have a guarantee on the generated classname,
varying across compilers and depends on the order that they are instantiated within the enclosing class, which can 
easily cause the previously written serializer to be unreadable (since the original class can no longer be found in the
classpath).

### Handling serializer upgrades and compatibility

Flink allows changing the serializers used to read and write managed state, so that users are not locked in to any
specific serialization. When state is restored, the new serializer registered for the state (i.e., the serializer
that comes with the `StateDescriptor` used to access the state in the restored job) will be checked for compatibility,
and is replaced as the new serializer for the state.

A compatible serializer would mean that the serializer is capable of reading previous serialized bytes of the state,
and the written binary format of the state also remains identical. The means to check the new serializer's compatibility
is provided through the following two methods of the `TypeSerializer` interface:

{% highlight java %}
public abstract TypeSerializerConfigSnapshot snapshotConfiguration();
public abstract CompatibilityResult ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot);
{% endhighlight %}

Briefly speaking, every time a checkpoint is performed, the `snapshotConfiguration` method is called to create a
point-in-time view of the state serializer's configuration. The returned configuration snapshot is stored along with the
checkpoint as the state's metadata. When the checkpoint is used to restore a job, that serializer configuration snapshot
will be provided to the _new_ serializer of the same state via the counterpart method, `ensureCompatibility`, to verify
compatibility of the new serializer. This method serves as a check for whether or not the new serializer is compatible,
as well as a hook to possibly reconfigure the new serializer in the case that it is incompatible.

Note that Flink's own serializers are implemented such that they are at least compatible with themselves, i.e. when the
same serializer is used for the state in the restored job, the serializer's will reconfigure themselves to be compatible
with their previous configuration.

The following subsections illustrate guidelines to implement these two methods when using custom serializers.

#### Implementing the `snapshotConfiguration` method

The serializer's configuration snapshot should capture enough information such that on restore, the information
carried over to the new serializer for the state is sufficient for it to determine whether or not it is compatible.
This could typically contain information about the serializer's parameters or binary format of the serialized data;
generally, anything that allows the new serializer to decide whether or not it can be used to read previous serialized
bytes, and that it writes in the same binary format.

How the serializer's configuration snapshot is written to and read from checkpoints is fully customizable. The below
is the base class for all serializer configuration snapshot implementations, the `TypeSerializerConfigSnapshot`.

{% highlight java %}
public abstract TypeSerializerConfigSnapshot extends VersionedIOReadableWritable {
  public abstract int getVersion();
  public void read(DataInputView in) {...}
  public void write(DataOutputView out) {...}
}
{% endhighlight %}

The `read` and `write` methods define how the configuration is read from and written to the checkpoint. The base
implementations contain logic to read and write the version of the configuration snapshot, so it should be extended and
not completely overridden.

The version of the configuration snapshot is determined through the `getVersion` method. Versioning for the serializer
configuration snapshot is the means to maintain compatible configurations, as information included in the configuration
may change over time. By default, configuration snapshots are only compatible with the current version (as returned by
`getVersion`). To indicate that the configuration is compatible with other versions, override the `getCompatibleVersions`
method to return more version values. When reading from the checkpoint, you can use the `getReadVersion` method to
determine the version of the written configuration and adapt the read logic to the specific version.

<span class="label label-danger">Attention</span> The version of the serializer's configuration snapshot is **not**
related to upgrading the serializer. The exact same serializer can have different implementations of its
configuration snapshot, for example when more information is added to the configuration to allow more comprehensive
compatibility checks in the future.

One limitation of implementing a `TypeSerializerConfigSnapshot` is that an empty constructor must be present. The empty
constructor is required when reading the configuration snapshot from checkpoints.

#### Implementing the `ensureCompatibility` method

The `ensureCompatibility` method should contain logic that performs checks against the information about the previous
serializer carried over via the provided `TypeSerializerConfigSnapshot`, basically doing one of the following:

  * Check whether the serializer is compatible, while possibly reconfiguring itself (if required) so that it may be
    compatible. Afterwards, acknowledge with Flink that the serializer is compatible.

  * Acknowledge that the serializer is incompatible and that state migration is required before Flink can proceed with
    using the new serializer.

The above cases can be translated to code by returning one of the following from the `ensureCompatibility` method:

  * **`CompatibilityResult.compatible()`**: This acknowledges that the new serializer is compatible, or has been reconfigured to
    be compatible, and Flink can proceed with the job with the serializer as is.

  * **`CompatibilityResult.requiresMigration()`**: This acknowledges that the serializer is incompatible, or cannot be
    reconfigured to be compatible, and requires a state migration before the new serializer can be used. State migration
    is performed by using the previous serializer to read the restored state bytes to objects, and then serialized again
    using the new serializer.

  * **`CompatibilityResult.requiresMigration(TypeDeserializer deserializer)`**: This acknowledgement has equivalent semantics
    to `CompatibilityResult.requiresMigration()`, but in the case that the previous serializer cannot be found or loaded
    to read the restored state bytes for the migration, a provided `TypeDeserializer` can be used as a fallback resort.

<span class="label label-danger">Attention</span> Currently, as of Flink 1.3, if the result of the compatibility check
acknowledges that state migration needs to be performed, the job simply fails to restore from the checkpoint as state
migration is currently not available. The ability to migrate state will be introduced in future releases.

### Managing `TypeSerializer` and `TypeSerializerConfigSnapshot` classes in user code

Since `TypeSerializer`s and `TypeSerializerConfigSnapshot`s are written as part of checkpoints along with the state
values, the availability of the classes within the classpath may affect restore behaviour.

`TypeSerializer`s are directly written into checkpoints using Java Object Serialization. In the case that the new
serializer acknowledges that it is incompatible and requires state migration, it will be required to be present to be
able to read the restored state bytes. Therefore, if the original serializer class no longer exists or has been modified
(resulting in a different `serialVersionUID`) as a result of a serializer upgrade for the state, the restore would
not be able to proceed. The alternative to this requirement is to provide a fallback `TypeDeserializer` when
acknowledging that state migration is required, using `CompatibilityResult.requiresMigration(TypeDeserializer deserializer)`.

The class of `TypeSerializerConfigSnapshot`s in the restored checkpoint must exist in the classpath, as they are
fundamental components to compatibility checks on upgraded serializers and would not be able to be restored if the class
is not present. Since configuration snapshots are written to checkpoints using custom serialization, the implementation
of the class is free to be changed, as long as compatibility of the configuration change is handled using the versioning
mechanisms in `TypeSerializerConfigSnapshot`.
