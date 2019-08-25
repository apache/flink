---
title: "Broadcast State 模式"
nav-parent_id: streaming_state
nav-pos: 2
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

[Working with State](state.html) describes operator state which upon restore is either evenly distributed among the 
parallel tasks of an operator, or unioned, with the whole state being used to initialize the restored parallel tasks.

A third type of supported *operator state* is the *Broadcast State*. Broadcast state was introduced to support use cases
where some data coming from one stream is required to be broadcasted to all downstream tasks, where it is stored locally
and is used to process all incoming elements on the other stream. As an example where broadcast state can emerge as a 
natural fit, one can imagine a low-throughput stream containing a set of rules which we want to evaluate against all 
elements coming from another stream. Having the above type of use cases in mind, broadcast state differs from the rest 
of operator states in that:
 1. it has a map format,
 2. it is only available to specific operators that have as inputs a *broadcasted* stream and a *non-broadcasted* one, and
 3. such an operator can have *multiple broadcast states* with different names.

## Provided APIs

To show the provided APIs, we will start with an example before presenting their full functionality. As our running 
example, we will use the case where we have a stream of objects of different colors and shapes and we want to find pairs
of objects of the same color that follow a certain pattern, *e.g.* a rectangle followed by a triangle. We assume that
the set of interesting patterns evolves over time. 

In this example, the first stream will contain elements of type `Item` with a `Color` and a `Shape` property. The other
stream will contain the `Rules`.

Starting from the stream of `Items`, we just need to *key it* by `Color`, as we want pairs of the same color. This will
make sure that elements of the same color end up on the same physical machine.

{% highlight java %}
// key the shapes by color
KeyedStream<Item, Color> colorPartitionedStream = shapeStream
                        .keyBy(new KeySelector<Shape, Color>(){...});
{% endhighlight %}

Moving on to the `Rules`, the stream containing them should be broadcasted to all downstream tasks, and these tasks 
should store them locally so that they can evaluate them against all incoming `Items`. The snippet below will i) broadcast 
the stream of rules and ii) using the provided `MapStateDescriptor`, it will create the broadcast state where the rules
will be stored.

{% highlight java %}

// a map descriptor to store the name of the rule (string) and the rule itself.
MapStateDescriptor<String, Rule> ruleStateDescriptor = new MapStateDescriptor<>(
			"RulesBroadcastState",
			BasicTypeInfo.STRING_TYPE_INFO,
			TypeInformation.of(new TypeHint<Rule>() {}));
		
// broadcast the rules and create the broadcast state
BroadcastStream<Rule> ruleBroadcastStream = ruleStream
                        .broadcast(ruleStateDescriptor);
{% endhighlight %}

Finally, in order to evaluate the `Rules` against the incoming elements from the `Item` stream, we need to:
 1. connect the two streams, and
 2. specify our match detecting logic.

Connecting a stream (keyed or non-keyed) with a `BroadcastStream` can be done by calling `connect()` on the 
non-broadcasted stream, with the `BroadcastStream` as an argument. This will return a `BroadcastConnectedStream`, on 
which we can call `process()` with a special type of `CoProcessFunction`. The function will contain our matching logic. 
The exact type of the function depends on the type of the non-broadcasted stream: 
 - if that is **keyed**, then the function is a `KeyedBroadcastProcessFunction`. 
 - if it is **non-keyed**, the function is a `BroadcastProcessFunction`. 
 
 Given that our non-broadcasted stream is keyed, the following snippet includes the above calls:

<div class="alert alert-info">
  <strong>Attention:</strong> The connect should be called on the non-broadcasted stream, with the BroadcastStream
   as an argument.
</div>

{% highlight java %}
DataStream<String> output = colorPartitionedStream
                 .connect(ruleBroadcastStream)
                 .process(
                     
                     // type arguments in our KeyedBroadcastProcessFunction represent: 
                     //   1. the key of the keyed stream
                     //   2. the type of elements in the non-broadcast side
                     //   3. the type of elements in the broadcast side
                     //   4. the type of the result, here a string
                     
                     new KeyedBroadcastProcessFunction<Color, Item, Rule, String>() {
                         // my matching logic
                     }
                 );
{% endhighlight %}

### BroadcastProcessFunction and KeyedBroadcastProcessFunction

As in the case of a `CoProcessFunction`, these functions have two process methods to implement; the `processBroadcastElement()`
which is responsible for processing incoming elements in the broadcasted stream and the `processElement()` which is used 
for the non-broadcasted one. The full signatures of the methods are presented below:

{% highlight java %}
public abstract class BroadcastProcessFunction<IN1, IN2, OUT> extends BaseBroadcastProcessFunction {

    public abstract void processElement(IN1 value, ReadOnlyContext ctx, Collector<OUT> out) throws Exception;

    public abstract void processBroadcastElement(IN2 value, Context ctx, Collector<OUT> out) throws Exception;
}
{% endhighlight %}

{% highlight java %}
public abstract class KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT> {

    public abstract void processElement(IN1 value, ReadOnlyContext ctx, Collector<OUT> out) throws Exception;

    public abstract void processBroadcastElement(IN2 value, Context ctx, Collector<OUT> out) throws Exception;

    public void onTimer(long timestamp, OnTimerContext ctx, Collector<OUT> out) throws Exception;
}
{% endhighlight %}

The first thing to notice is that both functions require the implementation of the `processBroadcastElement()` method 
for processing elements in the broadcast side and the `processElement()` for elements in the non-broadcasted side. 

The two methods differ in the context they are provided. The non-broadcast side has a `ReadOnlyContext`, while the 
broadcasted side has a `Context`. 

Both of these contexts (`ctx` in the following enumeration):
 1. give access to the broadcast state: `ctx.getBroadcastState(MapStateDescriptor<K, V> stateDescriptor)`
 2. allow to query the timestamp of the element: `ctx.timestamp()`, 
 3. get the current watermark: `ctx.currentWatermark()`
 4. get the current processing time: `ctx.currentProcessingTime()`, and 
 5. emit elements to side-outputs: `ctx.output(OutputTag<X> outputTag, X value)`. 

The `stateDescriptor` in the `getBroadcastState()` should be identical to the one in the `.broadcast(ruleStateDescriptor)` 
above.

The difference lies in the type of access each one gives to the broadcast state. The broadcasted side has 
**read-write access** to it, while the non-broadcast side has **read-only access** (thus the names). The reason for this
is that in Flink there is no cross-task communication. So, to guarantee that the contents in the Broadcast State are the
same across all parallel instances of our operator, we give read-write access only to the broadcast side, which sees the
same elements across all tasks, and we require the computation on each incoming element on that side to be identical 
across all tasks. Ignoring this rule would break the consistency guarantees of the state, leading to inconsistent and 
often difficult to debug results.

<div class="alert alert-info">
  <strong>Attention:</strong> The logic implemented in `processBroadcast()` must have the same deterministic behavior
  across all parallel instances!
</div>

Finally, due to the fact that the `KeyedBroadcastProcessFunction` is operating on a keyed stream, it 
exposes some functionality which is not available to the `BroadcastProcessFunction`. That is:
 1. the `ReadOnlyContext` in the `processElement()` method gives access to Flink's underlying timer service, which allows
  to register event and/or processing time timers. When a timer fires, the `onTimer()` (shown above) is invoked with an 
  `OnTimerContext` which exposes the same functionality as the `ReadOnlyContext` plus 
   - the ability to ask if the timer that fired was an event or processing time one and 
   - to query the key associated with the timer.
 2. the `Context` in the `processBroadcastElement()` method contains the method 
 `applyToKeyedState(StateDescriptor<S, VS> stateDescriptor, KeyedStateFunction<KS, S> function)`. This allows to 
  register a `KeyedStateFunction` to be **applied to all states of all keys** associated with the provided `stateDescriptor`. 

<div class="alert alert-info">
  <strong>Attention:</strong> Registering timers is only possible at `processElement()` of the `KeyedBroadcastProcessFunction`
  and only there. It is not possible in the `processBroadcastElement()` method, as there is no key associated to the 
  broadcasted elements.
</div>
  
Coming back to our original example, our `KeyedBroadcastProcessFunction` could look like the following:

{% highlight java %}
new KeyedBroadcastProcessFunction<Color, Item, Rule, String>() {

    // store partial matches, i.e. first elements of the pair waiting for their second element
    // we keep a list as we may have many first elements waiting
    private final MapStateDescriptor<String, List<Item>> mapStateDesc =
        new MapStateDescriptor<>(
            "items",
            BasicTypeInfo.STRING_TYPE_INFO,
            new ListTypeInfo<>(Item.class));

    // identical to our ruleStateDescriptor above
    private final MapStateDescriptor<String, Rule> ruleStateDescriptor = 
        new MapStateDescriptor<>(
            "RulesBroadcastState",
            BasicTypeInfo.STRING_TYPE_INFO,
            TypeInformation.of(new TypeHint<Rule>() {}));

    @Override
    public void processBroadcastElement(Rule value,
                                        Context ctx,
                                        Collector<String> out) throws Exception {
        ctx.getBroadcastState(ruleStateDescriptor).put(value.name, value);
    }

    @Override
    public void processElement(Item value,
                               ReadOnlyContext ctx,
                               Collector<String> out) throws Exception {

        final MapState<String, List<Item>> state = getRuntimeContext().getMapState(mapStateDesc);
        final Shape shape = value.getShape();
    
        for (Map.Entry<String, Rule> entry :
                ctx.getBroadcastState(ruleStateDescriptor).immutableEntries()) {
            final String ruleName = entry.getKey();
            final Rule rule = entry.getValue();
    
            List<Item> stored = state.get(ruleName);
            if (stored == null) {
                stored = new ArrayList<>();
            }
    
            if (shape == rule.second && !stored.isEmpty()) {
                for (Item i : stored) {
                    out.collect("MATCH: " + i + " - " + value);
                }
                stored.clear();
            }
    
            // there is no else{} to cover if rule.first == rule.second
            if (shape.equals(rule.first)) {
                stored.add(value);
            }
    
            if (stored.isEmpty()) {
                state.remove(ruleName);
            } else {
                state.put(ruleName, stored);
            }
        }
    }
}
{% endhighlight %}

## Important Considerations

After describing the offered APIs, this section focuses on the important things to keep in mind when using broadcast 
state. These are:

  - **There is no cross-task communication:** As stated earlier, this is the reason why only the broadcast side of a 
`(Keyed)-BroadcastProcessFunction` can modify the contents of the broadcast state. In addition, the user has to make 
sure that all tasks modify the contents of the broadcast state in the same way for each incoming element. Otherwise,
different tasks might have different contents, leading to inconsistent results.

  - **Order of events in Broadcast State may differ across tasks:** Although broadcasting the elements of a stream 
guarantees that all elements will (eventually) go to all downstream tasks, elements may arrive in a different order 
to each task. So the state updates for each incoming element *MUST NOT depend on the ordering* of the incoming
events.

  - **All tasks checkpoint their broadcast state:** Although all tasks have the same elements in their broadcast state
when a checkpoint takes place (checkpoint barriers do not overpass elements), all tasks checkpoint their broadcast state, 
and not just one of them. This is a design decision to avoid having all tasks read from the same file during a restore 
(thus avoiding hotspots), although it comes at the expense of increasing the size of the checkpointed state by a factor 
of p (= parallelism). Flink guarantees that upon restoring/rescaling there will be **no duplicates** and **no missing data**. 
In case of recovery with the same or smaller parallelism, each task reads its checkpointed state. Upon scaling up, each
task reads its own state, and the remaining tasks (`p_new`-`p_old`) read checkpoints of previous tasks in a round-robin
manner.

  - **No RocksDB state backend:** Broadcast state is kept in-memory at runtime and memory provisioning should be done 
accordingly. This holds for all operator states.

{% top %}
