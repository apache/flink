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


## Operator State and Keyed State

There are two basic kinds of state in Flink: `Operator State` and `Keyed State`.

### Operator State

With *Operator State* (or *non-keyed state*), each operator state is
bound to one parallel operator instance.
The Kafka source connector is a good motivating example for the use of Operator State
in Flink. Each parallel instance of this Kafka consumer maintains a map
of topic partitions and offsets as its Operator State.

New interfaces in Flink 1.2 subsume the `Checkpointed` interface in Flink 1.0 and
1.1, which has been deprecated. 
These new Operator State interfaces support redistributing state among
parallel operator instances when the parallelism is changed. 

There can be different schemes for doing this redistribution; the following are currently defined:

  - **List-style redistribution:** Each operator returns a List of state elements. The whole state is logically a concatenation of
    all lists. On restore/redistribution, the list is evenly divided into as many sublists as there are parallel operators.
    Each operator gets a sublist, which can be empty, or contain one or more elements.


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


## Using Managed Operator State

A stateful function can implement either the more general `CheckpointedFunction` 
interface, or the `ListCheckpointed<T extends Serializable>` interface (which is semantically closer to the old 
`Checkpointed` one).

[The Flink Function Migration documentation](../migration.html) has
more information on this topic, and examples of how to use both
`CheckpointedFunction` and `ListCheckpointed`.

### Stateful Source Functions

Stateful sources require a bit more care as opposed to other operators.
In order to make the updates to the state and output collection atomic (required for exactly-once semantics
on failure/recovery), the user is required to get a lock from the source's context.

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

Some operators might need the information when a checkpoint is fully acknowledged by Flink to communicate that with the outside world. In this case see the `org.apache.flink.runtime.state.CheckpointListener` interface.


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
want to retrieve, you create either a `ValueStateDescriptor`, a `ListStateDescriptor` or
a `ReducingStateDescriptor`.

State is accessed using the `RuntimeContext`, so it is only possible in *rich functions*.
Please see [here]({{ site.baseurl }}/dev/api_concepts#rich-functions) for
information about that, but we will also see an example shortly. The `RuntimeContext` that
is available in a `RichFunction` has these methods for accessing state:

* `ValueState<T> getState(ValueStateDescriptor<T>)`
* `ReducingState<T> getReducingState(ReducingStateDescriptor<T>)`
* `ListState<T> getListState(ListStateDescriptor<T>)`

This is an example `FlatMapFunction` that shows how all of the parts fit together:

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

