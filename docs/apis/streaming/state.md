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
by declaring local variables or using Flink's state interface. You can register any local variable
as ***managed*** state by implementing an interface. In this case, and also in the case of using
Flink's native state interface, Flink will automatically take consistent snapshots of your state
periodically, and restore its value in the case of a failure.

The end effect is that updates to any form of state are the same under failure-free execution and
execution under failures.

First, we look at how to make local variables consistent under failures, and then we look at
Flink's state interface.

By default state checkpoints will be stored in-memory at the JobManager. For proper persistence of large
state, Flink supports storing the checkpoints on file systems (HDFS, S3, or any mounted POSIX file system),
which can be configured in the `flink-conf.yaml` or via `StreamExecutionEnvironment.setStateBackend(…)`.

* ToC
{:toc}


## Checkpointing Local Variables

Local variables can be checkpointed by using the `Checkpointed` interface.

When the user-defined function implements the `Checkpointed` interface, the `snapshotState(…)` and `restoreState(…)`
methods will be executed to draw and restore function state.

In addition to that, user functions can also implement the `CheckpointNotifier` interface to receive notifications on
completed checkpoints via the `notifyCheckpointComplete(long checkpointId)` method.
Note that there is no guarantee for the user function to receive a notification if a failure happens between
checkpoint completion and notification. The notifications should hence be treated in a way that notifications from
later checkpoints can subsume missing notifications.

For example the same counting, reduce function shown for `OperatorState`s by using the `Checkpointed` interface instead:

{% highlight java %}
public class CounterSum extends ReduceFunction<Long>, Checkpointed<Long> {

    // persistent counter
    private long counter = 0;

    @Override
    public Long reduce(Long value1, Long value2) {
        counter++;
        return value1 + value2;
    }

    // regularly persists state during normal operation
    @Override
    public Serializable snapshotState(long checkpointId, long checkpointTimestamp) {
        return counter;
    }

    // restores state on recovery from failure
    @Override
    public void restoreState(Long state) {
        counter = state;
    }
}
{% endhighlight %}

## Using the Key/Value State Interface

The state interface gives access to key/value states, which are a collection of key/value pairs.
Because the state is partitioned by the keys (distributed accross workers), it can only be used
on the `KeyedStream`, created via `stream.keyBy(…)` (which means also that it is usable in all
types of functions on keyed windows).

The handle to the state can be obtained from the function's `RuntimeContext`. The state handle will
then give access to the value mapped under the key of the current record or window - each key consequently
has its own value.

The following code sample shows how to use the key/value state inside a reduce function.
When creating the state handle, one needs to supply a name for that state (a function can have multiple states
of different types), the type of the state (used to create efficient serializers), and the default value (returned
as a value for keys that do not yet have a value associated).

{% highlight java %}
public class CounterSum extends RichReduceFunction<Long> {

    /** The state handle */
    private OperatorState<Long> counter;

    @Override
    public Long reduce(Long value1, Long value2) {
        counter.update(counter.value() + 1);
        return value1 + value2;
    }

    @Override
    public void open(Configuration config) {
        counter = getRuntimeContext().getKeyValueState("myCounter", Long.class, 0L);
    }
}
{% endhighlight %}

State updated by this is usually kept locally inside the flink process (unless one configures explicitly
an external state backend). This means that lookups and updates are process local and this very fast.

The important implication of having the keys set implicitly is that it forces programs to group the stream
by key (via the `keyBy()` function), making the key partitioning transparent to Flink. That allows the system
to efficiently restore and redistribute keys and state.

The Scala API has shortcuts that for stateful `map()` or `flatMap()` functions on `KeyedStream`, which give the
state of the current key as an option directly into the function, and return the result with a state update:

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

## Stateful Source Functions

Stateful sources require a bit more care as opposed to other operators.
In order to make the updates to the state and output collection atomic (required for exactly-once semantics
on failure/recovery), the user is required to get a lock from the source's context.

{% highlight java %}
public static class CounterSource extends RichParallelSourceFunction<Long>, Checkpointed<Long> {

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