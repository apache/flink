---
title: Context and State Processing
weight: 4
type: docs
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

{{< hint warning >}}
**Note:** DataStream API V2 is a new set of APIs, to gradually replace the original DataStream API. It is currently in the experimental stage and is not fully available for production.
{{< /hint >}}

# Context and State Processing

## Context

Unlike attributes like the name of process operation, some information(such as current key) can only be obtained when the process function is executed.
In order to build a bridge between process functions and the execution engine, DataStream API provide a unified entrypoint called Runtime Context.

We divide all contextual into multiple parts according to their functions:

- JobInfo: Hold all job related information, such as job name, execution mode.

- TaskInfo: Hold all task related information, such as parallelism.

- MetricGroup: Manage context related to metrics, such as registering a metric.

- State Manager: Manage context related to state, such as accessing a specific state.

- Watermark Manager: Manage context related to watermark, such as triggering a watermark.

- ProcessingTime Manager: Manage context related to processing timer, such as getting the current processing time.

Those context information can be classified into two categories:

* NonPartitionedContext: Include JobInfo, TaskInfo, MetricGroup, WatermarkManager.
* PartitionedContext: Include StateManager, ProcessingTimeManager.

Typically, `PartitionedContext` is provided to a `ProcessFunction` when processing of data within 
a specific partition is expected. E.g., upon receiving of new records or triggering of timers. 
On the other hand, `NonPartitionedContext` is provided when no specific partition is relevant. 
E.g., initiating or cleaning-up of the `ProcessFunction`.

The following code snippet shows how to get the parallelism and execution mode of the process function:

```java
new OneInputStreamProcessFunction<String, String>(){
    private transient int parallelism;
    
    private transient ExecutionMode executionMode;
    @Override
    public void open(NonPartitionedContext<String> ctx) throws Exception {
        parallelism = ctx.getTaskInfo().getParallelism();
        executionMode = ctx.getJobInfo().getExecutionMode();
    }
}
```

## State Processing

State is the foundation of stateful computation. DataStream API supports declaring state in Process Function and
accessing and updating state during data processing.

In general, you should follow the principle of "declare first, use later.". In general,
writing a stateful process function is divided into three steps:

1. Defining the state in the form of `StateDeclaration`.
2. Declaring the state in `ProcessFunction#usesStates`.
3. Getting and updating state via `StateManager`.

Before we go any further, let's look at what a stateful process function looks like:

```java
private static class StatefulFunction implements OneInputStreamProcessFunction<Long, Long> {
    // Step1: Defining the state in the form of `StateDeclaration`.
    static final StateDeclaration.ListStateDeclaration<Long> LIST_STATE_DECLARATION =
            StateDeclarations.listStateBuilder("example-list-state", TypeDescriptors.LONG).build();
     
    // Step2: Declaring the state in `ProcessFunction#usesStates`
    @Override
    public Set<StateDeclaration> usesStates() {
        return Collections.singleton(LIST_STATE_DECLARATION);
    }
    
    @Override
    public void processRecord(Long record, Collector<Long> output, RuntimeContext ctx)
            throws Exception {
        // Step3: Getting and updating state via `StateManager`.
        ListState<Long> state =
                ctx.getStateManager().getState(LIST_STATE_DECLARATION);
        // do something with this state. For example, update the state by this record.
        state.update(Collections.singletonList(record));
    }
}
```

### Define State

StateDeclaration is used to define a specific state, two types of information need to be provided for a state declaration:

- Name: Used as the unique identifier of the state.

- RedistributionMode: Defines how the state is redistributed between different partitions. For the keyed partition stream,
since the state is bounded within the partition, no redistribution is required. But for the non-keyed partition stream,
the partition will change with the parallelism, so the state must define how to redistribute.

There are three RedistributionMode to choose:
- NONE: Not supports redistribution.
- REDISTRIBUTABLE: This state can be safely redistributed between different partitions, and the 
specific redistribution strategy is determined by the state itself.
- IDENTICAL: States are guaranteed to be identical in different partitions, thus redistribution is 
not a problem.

There are currently five type of `StateDeclaration`: `ValueStateDeclaration`, `ListStateDeclaration`, `MapStateDeclaration`, `ReducingStateDeclaration`, `AggregatingStateDeclaration` and `BroadcastStateDeclaration`
that describe the `ValueState`, `ListState`, `MapState`, `ReducingState`, `AggregatingState` and `BroadcastState` respectively.

* `ValueState<T>`: This keeps a value that can be updated and
  retrieved. The value can be set using `update(T)` and retrieved using `T value()`.

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

* `BroadcastState<K, V>`: This can be created to store the state of a `BroadcastStream`. This state
assumes that the same elements are sent to all instances of a process function. You can put key-value pairs into the state and
retrieve an `Iterable` over all currently stored mappings.

For ease of use, DataStream API provide an auxiliary class called `StateDeclarations`, which encapsulates a series of methods for creating various StateDeclaration instances.

For example:

```java
// create a value state declaration
ValueStateDeclaration<Integer> valueStateDeclaration = StateDeclarations.valueState("example-value-state", TypeDescriptors.LONG);

// create a map state declaration
MapStateDeclaration<Long, String> mapStateDeclaration = StateDeclarations.mapState(
        "example-map-state", TypeDescriptors.LONG, TypeDescriptors.STRING);

// create a reducing state declaration with a `sum` reduce function
ReducingStateDeclaration<Long> reducingStateDeclaration =
        StateDeclarations.reducingState("example-reducing-state", TypeDescriptors.LONG, Long::sum);
```

To provide the type information of state, in the above example, the `TypeDescriptors` class provides a set of pre-defined type descriptors for common types, such as `INT`, `LONG`, `BOOLEAN`, `STRING`, `LIST`, `MAP`, etc.
If your type is not in the pre-defined list, you can provide a class implements `TypeDescriptor` interface to define your own type descriptor.

### Declare State

As we said earlier, when using state in the DataStream API, you must follow the principle of "declare first, use later.". 
`ProcessFunction` has a method called `useStates` to explicitly declares states upfront. 

```java
default Set<StateDeclaration> usesStates() {
    return Collections.emptySet();
}
```

For stateful functions, this method must be overridden and each specific state must be declared in this method before it can be used. This gives
flink more opportunities to optimize the execution of functions.

### Get and Update State

The `StateManager` is used to manage the state for the process function. It provides methods for getting state, as well as method for retrieving current key.

```java
public interface StateManager {
    /**
     * Get the key of current record.
     *
     * @return The key of current processed record.
     * @throws UnsupportedOperationException if the key can not be extracted for this function, for
     *     instance, get the key from a non-keyed partition stream.
     */
    <K> K getCurrentKey() throws UnsupportedOperationException;

    /**
     * Get the optional of the specific list state.
     *
     * @param stateDeclaration of this state.
     * @return the list state corresponds to the state declaration, this may be empty.
     */
    <T> Optional<ListState<T>> getStateOptional(ListStateDeclaration<T> stateDeclaration)
            throws Exception;

    /**
     * Get the specific list state.
     *
     * @param stateDeclaration of this state.
     * @return the list state corresponds to the state declaration
     * @throws RuntimeException if the state is not available.
     */
    <T> ListState<T> getState(ListStateDeclaration<T> stateDeclaration) throws Exception;

    /**
     * Get the optional of the specific value state.
     *
     * @param stateDeclaration of this state.
     * @return the value state corresponds to the state declaration, this may be empty.
     */
    <T> Optional<ValueState<T>> getStateOptional(ValueStateDeclaration<T> stateDeclaration)
            throws Exception;

    /**
     * Get the specific value state.
     *
     * @param stateDeclaration of this state.
     * @return the value state corresponds to the state declaration.
     * @throws RuntimeException if the state is not available.
     */
    <T> ValueState<T> getState(ValueStateDeclaration<T> stateDeclaration) throws Exception;

    /**
     * Get the optional of the specific map state.
     *
     * @param stateDeclaration of this state.
     * @return the map state corresponds to the state declaration, this may be empty.
     */
    <K, V> Optional<MapState<K, V>> getStateOptional(MapStateDeclaration<K, V> stateDeclaration)
            throws Exception;

    /**
     * Get the specific map state.
     *
     * @param stateDeclaration of this state.
     * @return the map state corresponds to the state declaration.
     * @throws RuntimeException if the state is not available.
     */
    <K, V> MapState<K, V> getState(MapStateDeclaration<K, V> stateDeclaration) throws Exception;

    /**
     * Get the optional of the specific reducing state.
     *
     * @param stateDeclaration of this state.
     * @return the reducing state corresponds to the state declaration, this may be empty.
     */
    <T> Optional<ReducingState<T>> getStateOptional(ReducingStateDeclaration<T> stateDeclaration)
            throws Exception;

    /**
     * Get the specific reducing state.
     *
     * @param stateDeclaration of this state.
     * @return the reducing state corresponds to the state declaration.
     * @throws RuntimeException if the state is not available.
     */
    <T> ReducingState<T> getState(ReducingStateDeclaration<T> stateDeclaration) throws Exception;

    /**
     * Get the optional of the specific aggregating state.
     *
     * @param stateDeclaration of this state.
     * @return the aggregating state corresponds to the state declaration, this may be empty.
     */
    <IN, ACC, OUT> Optional<AggregatingState<IN, OUT>> getStateOptional(
            AggregatingStateDeclaration<IN, ACC, OUT> stateDeclaration) throws Exception;

    /**
     * Get the specific aggregating state.
     *
     * @param stateDeclaration of this state.
     * @return the aggregating state corresponds to the state declaration.
     * @throws RuntimeException if the state is not available.
     */
    <IN, ACC, OUT> AggregatingState<IN, OUT> getState(
            AggregatingStateDeclaration<IN, ACC, OUT> stateDeclaration) throws Exception;

    /**
     * Get the optional of the specific broadcast state.
     *
     * @param stateDeclaration of this state.
     * @return the broadcast state corresponds to the state declaration, this may be empty.
     */
    <K, V> Optional<BroadcastState<K, V>> getStateOptional(
            BroadcastStateDeclaration<K, V> stateDeclaration) throws Exception;

    /**
     * Get the specific broadcast state.
     *
     * @param stateDeclaration of this state.
     * @return the broadcast state corresponds to the state declaration.
     * @throws RuntimeException if the state is not available.
     */
    <K, V> BroadcastState<K, V> getState(BroadcastStateDeclaration<K, V> stateDeclaration)
            throws Exception;
}
```

You can get the `StateManager` from anywhere you have access to the `PartitionedContext` using `context#getStateManager()`. 
Next, using the `getState` or `getStateOptional` method to obtain the state you declared, and then read and updated it.


### The Legitimacy of State Declaration and Access

In `ProcessFunction`, not all state declarations and accesses are legal, depending on the specific type of input streams.

For `OneInputStreamProcessFunction` and `TwoOutputStreamProcessFunction`, the legality of state declaration and access are listed in the following table:

{{< img src="/fig/datastream/one-input-state-access.png" alt="one-input-state-access" width="40%" >}}

For the two inputs case, things get a little more complicated. Since different inputs may come from different type of streams, the legality of state access is also different for the data processing functions of each input edge.
The specific rules are shown in the following table:

{{< img src="/fig/datastream/two-input-state-access.png" alt="two-input-state-access" width="70%" >}}
(K, NK, G, B denote the shorthand for Keyed, Non-Keyed, Global, Broadcast Stream, respectively)

flink will check the legality of state declaration in advance. If an illegal state is declared in `usesStates` method, exception will be thrown at job compile time.

### State Access Methods

Note that the datastream v2 is using the state v2 APIs. For more detail about the state access
methods, please refer to the
[Using Keyed State V2]({{< ref "docs/dev/datastream/fault-tolerance/state_v2#using-keyed-state-v2" >}}).


{{< top >}}
