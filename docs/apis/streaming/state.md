---
title: "Working with State"

sub-nav-parent: fault_tolerance
sub-nav-group: streaming
sub-nav-pos: 1
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

All transformations in Flink may look like functions (in the functional processing terminology), but
are in fact stateful operators. You can make *every* transformation (`map`, `filter`, etc) stateful
by using Flink's state interface or checkpointing instance fields of your function. You can register
any instance field
as ***managed*** state by implementing an interface. In this case, and also in the case of using
Flink's native state interface, Flink will automatically take consistent snapshots of your state
periodically, and restore its value in the case of a failure.

The end effect is that updates to any form of state are the same under failure-free execution and
execution under failures.

First, we look at how to make instance fields consistent under failures, and then we look at
Flink's state interface.

By default state checkpoints will be stored in-memory at the JobManager. For proper persistence of large
state, Flink supports storing the checkpoints on file systems (HDFS, S3, or any mounted POSIX file system),
which can be configured in the `flink-conf.yaml` or via `StreamExecutionEnvironment.setStateBackend(…)`.
See [state backends]({{ site.baseurl }}/apis/streaming/state_backends.html) for information
about the available state backends and how to configure them.

* ToC
{:toc}

## Using the Key/Value State Interface

The Key/Value state interface provides access to different types of state that are all scoped to
the key of the current input element. This means that this type of state can only be used
on a `KeyedStream`, which can be created via `stream.keyBy(…)`.

Now, we will first look at the different types of state available and then we will see
how they can be used in a program. The available state primitives are:

* `ValueState<T>`: This keeps a value that can be updated and
retrieved (scoped to key of the input element, mentioned above, so there will possibly be one value
for each key that the operation sees). The value can be set using `update(T)` and retrieved using
`T value()`.

* `ListState<T>`: This keeps a list of elements. You can append elements and retrieve an `Iterable`
over all currently stored elements. Elements are added using `add(T)`, the Iterable can
be retrieved using `Iterable<T> get()`.

* `ReducingState<T>`: This keeps a single value that represents the aggregation of all values
added to the state. The interface is the same as for `ListState` but elements added using
`add(T)` are reduced to an aggregate using a specified `ReduceFunction`.

All types of state also have a method `clear()` that clears the state for the currently
active key (i.e. the key of the input element).

It is important to keep in mind that these state objects are only used for interfacing
with state. The state is not necessarily stored inside but might reside on disk or somewhere else.
The second thing to keep in mind is that the value you get from the state
depend on the key of the input element. So the value you get in one invocation of your
user function can be different from the one you get in another invocation if the key of
the element is different.

To get a state handle you have to create a `StateDescriptor` this holds the name of the state
(as we will later see you can create several states, and they have to have unique names so
that you can reference them), the type of the values that the state holds and possibly
a user-specified function, such as a `ReduceFunction`. Depending on what type of state you
want to retrieve you create one of `ValueStateDescriptor`, `ListStateDescriptor` or
`ReducingStateDescriptor`.

State is accessed using the `RuntimeContext`, so it is only possible in *rich functions*.
Please see [here]({{ site.baseurl }}/apis/common/#specifying-transformation-functions) for
information about that but we will also see an example shortly. The `RuntimeContext` that
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
a `ValueState`, once the count reaches 2 it will emit the average and clear the state so that
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

## Checkpointing Instance Fields

Instance fields can be checkpointed by using the `Checkpointed` interface.

When the user-defined function implements the `Checkpointed` interface, the `snapshotState(…)` and `restoreState(…)`
methods will be executed to draw and restore function state.

In addition to that, user functions can also implement the `CheckpointNotifier` interface to receive notifications on
completed checkpoints via the `notifyCheckpointComplete(long checkpointId)` method.
Note that there is no guarantee for the user function to receive a notification if a failure happens between
checkpoint completion and notification. The notifications should hence be treated in a way that notifications from
later checkpoints can subsume missing notifications.

The above example for `ValueState` can be implemented using instance fields like this:

{% highlight java %}

public class CountWindowAverage
        extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>
        implements Checkpointed<Tuple2<Long, Long>> {

    private Tuple2<Long, Long> sum = null;

    @Override
    public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<Long, Long>> out) throws Exception {

        // update the count
        sum.f0 += 1;

        // add the second field of the input value
        sum.f1 += input.f1;


        // if the count reaches 2, emit the average and clear the state
        if (sum.f0 >= 2) {
            out.collect(new Tuple2<>(input.f0, sum.f1 / sum.f0));
            sum = Tuple2.of(0L, 0L);
        }
    }

    @Override
    public void open(Configuration config) {
        if (sum == null) {
            // only recreate if null
            // restoreState will be called before open()
            // so this will already set the sum to the restored value
            sum = Tuple2.of(0L, 0L);
        }
    }

    // regularly persists state during normal operation
    @Override
    public Serializable snapshotState(long checkpointId, long checkpointTimestamp) {
        return sum;
    }

    // restores state on recovery from failure
    @Override
    public void restoreState(Tuple2<Long, Long> state) {
        sum = state;
    }
}
{% endhighlight %}

## Stateful Source Functions

Stateful sources require a bit more care as opposed to other operators.
In order to make the updates to the state and output collection atomic (required for exactly-once semantics
on failure/recovery), the user is required to get a lock from the source's context.

{% highlight java %}
public static class CounterSource
        extends RichParallelSourceFunction<Long>
        implements Checkpointed<Long> {

    /**  current offset for exactly once semantics */
    private long offset;

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
    public Long snapshotState(long checkpointId, long checkpointTimestamp) {
        return offset;

    }

    @Override
	public void restoreState(Long state) {
        offset = state;
    }
}
{% endhighlight %}

Some operators might need the information when a checkpoint is fully acknowledged by Flink to communicate that with the outside world. In this case see the `flink.streaming.api.checkpoint.CheckpointNotifier` interface.

## State Checkpoints in Iterative Jobs

Flink currently only provides processing guarantees for jobs without iterations. Enabling checkpointing on an iterative job causes an exception. In order to force checkpointing on an iterative program the user needs to set a special flag when enabling checkpointing: `env.enableCheckpointing(interval, force = true)`.

Please note that records in flight in the loop edges (and the state changes associated with them) will be lost during failure.

{% top %}