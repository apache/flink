---
title: "FlinkCEP - Complex event processing for Flink"
nav-title: Event Processing (CEP)
nav-parent_id: libs
nav-pos: 1
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

FlinkCEP is the complex event processing library for Flink.
It allows you to easily detect complex event patterns in a stream of endless data.
Complex events can then be constructed from matching sequences.
This gives you the opportunity to quickly get hold of what's really important in your data.

<span class="label label-danger">Attention</span> The events in the `DataStream` to which
you want to apply pattern matching have to implement proper `equals()` and `hashCode()` methods
because these are used for comparing and matching events.

* This will be replaced by the TOC
{:toc}

## Getting Started

If you want to jump right in, you have to [set up a Flink program]({{ site.baseurl }}/dev/linking_with_flink.html).
Next, you have to add the FlinkCEP dependency to the `pom.xml` of your project.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-cep{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-cep-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}
</div>
</div>

Note that FlinkCEP is currently not part of the binary distribution.
See linking with it for cluster execution [here]({{site.baseurl}}/dev/linking.html).

Now you can start writing your first CEP program using the pattern API.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Event> input = ...

Pattern<Event, ?> pattern = Pattern.begin("start").where(evt -> evt.getId() == 42)
    .next("middle").subtype(SubEvent.class).where(subEvt -> subEvt.getVolume() >= 10.0)
    .followedBy("end").where(evt -> evt.getName().equals("end"));

PatternStream<Event> patternStream = CEP.pattern(input, pattern);

DataStream<Alert> result = patternStream.select(pattern -> {
    return createAlertFrom(pattern);
});
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[Event] = ...

val pattern = Pattern.begin("start").where(_.getId == 42)
  .next("middle").subtype(classOf[SubEvent]).where(_.getVolume >= 10.0)
  .followedBy("end").where(_.getName == "end")

val patternStream = CEP.pattern(input, pattern)

val result: DataStream[Alert] = patternStream.select(createAlert(_))
{% endhighlight %}
</div>
</div>

Note that we use Java 8 lambdas in our Java code examples to make them more succinct.

## The Pattern API

The pattern API allows you to quickly define complex event patterns.

Each pattern consists of multiple stages or what we call states.
In order to go from one state to the next, the user can specify conditions.
These conditions can be the contiguity of events or a filter condition on an event.

Each pattern has to start with an initial state:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Pattern<Event, ?> start = Pattern.<Event>begin("start");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val start : Pattern[Event, _] = Pattern.begin("start")
{% endhighlight %}
</div>
</div>

Each state must have a unique name to identify the matched events later on.
Additionally, we can specify a filter condition for the event to be accepted as the start event via the `where` method.
These filtering conditions can be either an `IterativeCondition` or a `SimpleCondition`. 

**Iterative Conditions:** This type of conditions can iterate over the previously accepted elements in the pattern and 
decide to accept a new element or not, based on some statistic over those elements. 

Below is the code for an iterative condition that accepts elements whose name start with "foo" and for which, the sum 
of the prices of the previously accepted elements for a state named "middle", plus the price of the current event, do 
not exceed the value of 5.0. Iterative condition can be very powerful, especially in combination with quantifiers, e.g.
`oneToMany` or `zeroToMany`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
start.where(new IterativeCondition<SubEvent>() {
    @Override
    public boolean filter(SubEvent value, Context<SubEvent> ctx) throws Exception {
        if (!value.getName().startsWith("foo")) {
            return false;
        }
        
        double sum = value.getPrice();
        for (Event event : ctx.getEventsForPattern("middle")) {
            sum += event.getPrice();
        }
        return Double.compare(sum, 5.0) < 0;
    }
});
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
start.where(
    (value, ctx) => {
        lazy val sum = ctx.getEventsForPattern("middle").asScala.map(_.getPrice).sum
        value.getName.startsWith("foo") && sum + value.getPrice < 5.0
    }
)
{% endhighlight %}
</div>
</div>

<span class="label label-danger">Attention</span> The call to `Context.getEventsForPattern(...)` has to find the 
elements that belong to the pattern. The cost of this operation can vary, so when implementing your condition, try 
to minimize the times the method is called.

**Simple Conditions:** This type of conditions extend the aforementioned `IterativeCondition` class. They are simple 
filtering conditions that decide to accept an element or not, based only on properties of the element itself.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
start.where(new SimpleCondition<Event>() {
    @Override
    public boolean filter(Event value) {
        return ... // some condition
    }
});
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
start.where(event => ... /* some condition */)
{% endhighlight %}
</div>
</div>

We can also restrict the type of the accepted event to some subtype of the initial event type (here `Event`) via the `subtype` method.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
start.subtype(SubEvent.class).where(new SimpleCondition<SubEvent>() {
    @Override
    public boolean filter(SubEvent value) {
        return ... // some condition
    }
});
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
start.subtype(classOf[SubEvent]).where(subEvent => ... /* some condition */)
{% endhighlight %}
</div>
</div>

As it can be seen here, the subtype condition can also be combined with an additional filter condition on the subtype.
In fact, you can always provide multiple conditions by calling `where` and `subtype` multiple times.
These conditions will then be combined using the logical AND operator.

In order to construct or conditions, one has to call the `or` method with a respective filter function.
Any existing filter function is then ORed with the given one.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
pattern.where(new SimpleCondition<Event>() {
    @Override
    public boolean filter(Event value) {
        return ... // some condition
    }
}).or(new SimpleCondition<Event>() {
    @Override
    public boolean filter(Event value) {
        return ... // or condition
    }
});
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
pattern.where(event => ... /* some condition */).or(event => ... /* or condition */)
{% endhighlight %}
</div>
</div>

Next, we can append further states to detect complex patterns.
We can control the contiguity of two succeeding events to be accepted by the pattern.

Strict contiguity means that two matching events have to be directly the one after the other.
This means that no other events can occur in between. 
A strict contiguity pattern state can be created via the `next` method.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Pattern<Event, ?> strictNext = start.next("middle");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val strictNext: Pattern[Event, _] = start.next("middle")
{% endhighlight %}
</div>
</div>

Non-strict contiguity means that other events are allowed to occur in-between two matching events.
A non-strict contiguity pattern state can be created via the `followedBy` method.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Pattern<Event, ?> nonStrictNext = start.followedBy("middle");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val nonStrictNext : Pattern[Event, _] = start.followedBy("middle")
{% endhighlight %}
</div>
</div>
It is also possible to define a temporal constraint for the pattern to be valid.
For example, one can define that a pattern should occur within 10 seconds via the `within` method. 
Temporal patterns are supported for both [processing and event time]({{site.baseurl}}/dev/event_time.html).

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
next.within(Time.seconds(10));
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
next.within(Time.seconds(10))
{% endhighlight %}
</div>
</div>

<br />

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">Pattern Operation</th>
            <th class="text-center">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><strong>Begin</strong></td>
            <td>
            <p>Defines a starting pattern state:</p>
{% highlight java %}
Pattern<Event, ?> start = Pattern.<Event>begin("start");
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>Next</strong></td>
            <td>
                <p>Appends a new pattern state. A matching event has to directly succeed the previous matching event:</p>
{% highlight java %}
Pattern<Event, ?> next = start.next("next");
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>FollowedBy</strong></td>
            <td>
                <p>Appends a new pattern state. Other events can occur between a matching event and the previous matching event:</p>
{% highlight java %}
Pattern<Event, ?> followedBy = start.followedBy("next");
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>Where</strong></td>
            <td>
                <p>Defines a condition for the current pattern state. Only if an event satisifes the condition, it can match the state:</p>
{% highlight java %}
patternState.where(new IterativeCondition<Event>() {
    @Override
    public boolean filter(Event value, Context ctx) throws Exception {
        return ... // some condition
    }
});
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>Or</strong></td>
            <td>
                <p>Adds a new filter condition which is ORed with an existing filter condition. Only if an event passes the filter condition, it can match the state:</p>
{% highlight java %}
patternState.where(new IterativeCondition<Event>() {
    @Override
    public boolean filter(Event value, Context ctx) throws Exception {
        return ... // some condition
    }
}).or(new IterativeCondition<Event>() {
    @Override
    public boolean filter(Event value, Context ctx) throws Exception {
        return ... // alternative condition
    }
});
{% endhighlight %}
                    </td>
                </tr>
       <tr>
           <td><strong>Subtype</strong></td>
           <td>
               <p>Defines a subtype condition for the current pattern state. Only if an event is of this subtype, it can match the state:</p>
{% highlight java %}
patternState.subtype(SubEvent.class);
{% endhighlight %}
           </td>
       </tr>
       <tr>
          <td><strong>Within</strong></td>
          <td>
              <p>Defines the maximum time interval for an event sequence to match the pattern. If a non-completed event sequence exceeds this time, it is discarded:</p>
{% highlight java %}
patternState.within(Time.seconds(10));
{% endhighlight %}
          </td>
       </tr>
       <tr>
          <td><strong>ZeroOrMore</strong></td>
          <td>
              <p>Specifies that this pattern can occur zero or more times(kleene star). This means any number of events can be matched in this state.</p>
              <p>If eagerness is enabled(by default) for a pattern A*B and sequence A1 A2 B will generate patterns: B, A1 B and A1 A2 B. If disabled B, A1 B, A2 B and A1 A2 B.</p>
              <p>By default a relaxed internal continuity (between subsequent events of a loop) is used. For more info on the internal continuity see <a href="#consecutive_java">consecutive</a></p>
      {% highlight java %}
      patternState.zeroOrMore();
      {% endhighlight %}
          </td>
       </tr>
       <tr>
          <td><strong>OneOrMore</strong></td>
          <td>
              <p>Specifies that this pattern can occur one or more times(kleene star). This means at least one and at most infinite number of events can be matched in this state.</p>
              <p>If eagerness is enabled (by default) for a pattern A*B and sequence A1 A2 B will generate patterns: A1 B and A1 A2 B. If disabled A1 B, A2 B and A1 A2 B.</p>
              <p>By default a relaxed internal continuity (between subsequent events of a loop) is used. For more info on the internal continuity see <a href="#consecutive_java">consecutive</a></p>
      {% highlight java %}
      patternState.oneOrMore();
      {% endhighlight %}
          </td>
       </tr>
       <tr>
          <td><strong>Optional</strong></td>
          <td>
              <p>Specifies that this pattern can occur zero or once.</p>
      {% highlight java %}
      patternState.optional();
      {% endhighlight %}
          </td>
       </tr>
       <tr>
          <td><strong>Times</strong></td>
          <td>
              <p>Specifies exact number of times that this pattern should be matched.</p>
              <p>By default a relaxed internal continuity (between subsequent events of a loop) is used. For more info on the internal continuity see <a href="#consecutive_java">consecutive</a></p>
      {% highlight java %}
      patternState.times(2);
      {% endhighlight %}
          </td>
       </tr>
       <tr>
          <td><strong>Consecutive</strong><a name="consecutive_java"></a></td>
          <td>
              <p>Works in conjunction with zeroOrMore, oneOrMore or times. Specifies that any not matching element breaks the loop.</p>
              
              <p>If not applied a relaxed continuity (as in followedBy) is used.</p>

          <p>E.g. a pattern like:</p>
      {% highlight java %}
      Pattern.<Event>begin("start").where(new SimpleCondition<Event>() {
           @Override
           public boolean filter(Event value) throws Exception {
               return value.getName().equals("c");
           }
      })
      .followedBy("middle").where(new SimpleCondition<Event>() {
           @Override
           public boolean filter(Event value) throws Exception {
               return value.getName().equals("a");
           }
      })
      .oneOrMore(true).consecutive()
      .followedBy("end1").where(new SimpleCondition<Event>() {
           @Override
           public boolean filter(Event value) throws Exception {
               return value.getName().equals("b");
           }
      });
      {% endhighlight %}

             <p>Will generate the following matches for a sequence: C D A1 A2 A3 D A4 B</p>

             <p>with consecutive applied: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}</p>
             <p>without consecutive applied: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}, {C A1 A2 A3 A4 B}</p>

             <p><b>NOTICE:</b> This option can be applied only to zeroOrMore(), oneOrMore() and times()!</p>
          </td>
       </tr>
  </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">
<table class="table table-bordered">
    <thead>
        <tr>
            <th class="text-left" style="width: 25%">Pattern Operation</th>
            <th class="text-center">Description</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td><strong>Begin</strong></td>
            <td>
            <p>Defines a starting pattern state:</p>
{% highlight scala %}
val start = Pattern.begin[Event]("start")
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>Next</strong></td>
            <td>
                <p>Appends a new pattern state. A matching event has to directly succeed the previous matching event:</p>
{% highlight scala %}
val next = start.next("middle")
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>FollowedBy</strong></td>
            <td>
                <p>Appends a new pattern state. Other events can occur between a matching event and the previous matching event:</p>
{% highlight scala %}
val followedBy = start.followedBy("middle")
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>Where</strong></td>
            <td>
                <p>Defines a filter condition for the current pattern state. Only if an event passes the filter, it can match the state:</p>
{% highlight scala %}
patternState.where(event => ... /* some condition */)
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>Or</strong></td>
            <td>
                <p>Adds a new filter condition which is ORed with an existing filter condition. Only if an event passes the filter condition, it can match the state:</p>
{% highlight scala %}
patternState.where(event => ... /* some condition */)
    .or(event => ... /* alternative condition */)
{% endhighlight %}
                    </td>
                </tr>
       <tr>
           <td><strong>Subtype</strong></td>
           <td>
               <p>Defines a subtype condition for the current pattern state. Only if an event is of this subtype, it can match the state:</p>
{% highlight scala %}
patternState.subtype(classOf[SubEvent])
{% endhighlight %}
           </td>
       </tr>
       <tr>
          <td><strong>Within</strong></td>
          <td>
              <p>Defines the maximum time interval for an event sequence to match the pattern. If a non-completed event sequence exceeds this time, it is discarded:</p>
{% highlight scala %}
patternState.within(Time.seconds(10))
{% endhighlight %}
          </td>
      </tr>
       <tr>
          <td><strong>ZeroOrMore</strong></td>
          <td>
              <p>Specifies that this pattern can occur zero or more times(kleene star). This means any number of events can be matched in this state.</p>
              <p>If eagerness is enabled(by default) for a pattern A*B and sequence A1 A2 B will generate patterns: B, A1 B and A1 A2 B. If disabled B, A1 B, A2 B and A1 A2 B.</p>
              <p>By default a relaxed internal continuity (between subsequent events of a loop) is used. For more info on the internal continuity see <a href="#consecutive_scala">consecutive</a></p>
      {% highlight scala %}
      patternState.zeroOrMore()
      {% endhighlight %}
          </td>
       </tr>
       <tr>
          <td><strong>OneOrMore</strong></td>
          <td>
              <p>Specifies that this pattern can occur one or more times(kleene star). This means at least one and at most infinite number of events can be matched in this state.</p>
              <p>If eagerness is enabled (by default) for a pattern A*B and sequence A1 A2 B will generate patterns: A1 B and A1 A2 B. If disabled A1 B, A2 B and A1 A2 B.</p>
              <p>By default a relaxed internal continuity (between subsequent events of a loop) is used. For more info on the internal continuity see <a href="#consecutive_scala">consecutive</a></p>
      {% highlight scala %}
      patternState.oneOrMore()
      {% endhighlight %}
          </td>
       </tr>
       <tr>
          <td><strong>Optional</strong></td>
          <td>
              <p>Specifies that this pattern can occur zero or once.</p>
      {% highlight scala %}
      patternState.optional()
      {% endhighlight %}
          </td>
       </tr>
       <tr>
          <td><strong>Times</strong></td>
          <td>
              <p>Specifies exact number of times that this pattern should be matched.</p>
              <p>By default a relaxed internal continuity (between subsequent events of a loop) is used. For more info on the internal continuity see <a href="#consecutive_scala">consecutive</a></p>
      {% highlight scala %}
      patternState.times(2)
      {% endhighlight %}
          </td>
       </tr>
       <tr>
          <td><strong>Consecutive</strong><a name="consecutive_scala"></a></td>
          <td>
            <p>Works in conjunction with zeroOrMore, oneOrMore or times. Specifies that any not matching element breaks the loop.</p>
            
            <p>If not applied a relaxed continuity (as in followedBy) is used.</p>
            
      {% highlight scala %}
      Pattern.begin("start").where(_.getName().equals("c"))
       .followedBy("middle").where(_.getName().equals("a"))
                            .oneOrMore(true).consecutive()
       .followedBy("end1").where(_.getName().equals("b"));
      {% endhighlight %}

            <p>Will generate the following matches for a sequence: C D A1 A2 A3 D A4 B</p>

            <p>with consecutive applied: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}</p>
            <p>without consecutive applied: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}, {C A1 A2 A3 A4 B}</p>

            <p><b>NOTICE:</b> This option can be applied only to zeroOrMore(), oneOrMore() and times()!</p>
          </td>
       </tr>
  </tbody>
</table>
</div>

</div>

### Detecting Patterns

In order to run a stream of events against your pattern, you have to create a `PatternStream`.
Given an input stream `input` and a pattern `pattern`, you create the `PatternStream` by calling

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Event> input = ...
Pattern<Event, ?> pattern = ...

PatternStream<Event> patternStream = CEP.pattern(input, pattern);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val input : DataStream[Event] = ...
val pattern : Pattern[Event, _] = ...

val patternStream: PatternStream[Event] = CEP.pattern(input, pattern)
{% endhighlight %}
</div>
</div>

### Selecting from Patterns
Once you have obtained a `PatternStream` you can select from detected event sequences via the `select` or `flatSelect` methods.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
The `select` method requires a `PatternSelectFunction` implementation.
A `PatternSelectFunction` has a `select` method which is called for each matching event sequence.
It receives a map of string/event pairs of the matched events.
The string is defined by the name of the state to which the event has been matched.
The `select` method can return exactly one result.

{% highlight java %}
class MyPatternSelectFunction<IN, OUT> implements PatternSelectFunction<IN, OUT> {
    @Override
    public OUT select(Map<String, IN> pattern) {
        IN startEvent = pattern.get("start");
        IN endEvent = pattern.get("end");
        return new OUT(startEvent, endEvent);
    }
}
{% endhighlight %}

A `PatternFlatSelectFunction` is similar to the `PatternSelectFunction`, with the only distinction that it can return an arbitrary number of results.
In order to do this, the `select` method has an additional `Collector` parameter which is used for the element output.

{% highlight java %}
class MyPatternFlatSelectFunction<IN, OUT> implements PatternFlatSelectFunction<IN, OUT> {
    @Override
    public void select(Map<String, IN> pattern, Collector<OUT> collector) {
        IN startEvent = pattern.get("start");
        IN endEvent = pattern.get("end");

        for (int i = 0; i < startEvent.getValue(); i++ ) {
            collector.collect(new OUT(startEvent, endEvent));
        }
    }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
The `select` method takes a selection function as argument, which is called for each matching event sequence.
It receives a map of string/event pairs of the matched events.
The string is defined by the name of the state to which the event has been matched.
The selection function returns exactly one result per call.

{% highlight scala %}
def selectFn(pattern : mutable.Map[String, IN]): OUT = {
    val startEvent = pattern.get("start").get
    val endEvent = pattern.get("end").get
    OUT(startEvent, endEvent)
}
{% endhighlight %}

The `flatSelect` method is similar to the `select` method. Their only difference is that the function passed to the `flatSelect` method can return an arbitrary number of results per call.
In order to do this, the function for `flatSelect` has an additional `Collector` parameter which is used for the element output.

{% highlight scala %}
def flatSelectFn(pattern : mutable.Map[String, IN], collector : Collector[OUT]) = {
    val startEvent = pattern.get("start").get
    val endEvent = pattern.get("end").get
    for (i <- 0 to startEvent.getValue) {
        collector.collect(OUT(startEvent, endEvent))
    }
}
{% endhighlight %}
</div>
</div>

### Handling Timed Out Partial Patterns

Whenever a pattern has a window length associated via the `within` keyword, it is possible that partial event patterns will be discarded because they exceed the window length.
In order to react to these timeout events the `select` and `flatSelect` API calls allow a timeout handler to be specified.
This timeout handler is called for each partial event pattern which has timed out.
The timeout handler receives all the events that have been matched so far by the pattern, and the timestamp when the timeout was detected.


<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
In order to treat partial patterns, the `select` and `flatSelect` API calls offer an overloaded version which takes as the first parameter a `PatternTimeoutFunction`/`PatternFlatTimeoutFunction` and as second parameter the known `PatternSelectFunction`/`PatternFlatSelectFunction`.
The return type of the timeout function can be different from the select function.
The timeout event and the select event are wrapped in `Either.Left` and `Either.Right` respectively so that the resulting data stream is of type `org.apache.flink.types.Either`.

{% highlight java %}
PatternStream<Event> patternStream = CEP.pattern(input, pattern);

DataStream<Either<TimeoutEvent, ComplexEvent>> result = patternStream.select(
    new PatternTimeoutFunction<Event, TimeoutEvent>() {...},
    new PatternSelectFunction<Event, ComplexEvent>() {...}
);

DataStream<Either<TimeoutEvent, ComplexEvent>> flatResult = patternStream.flatSelect(
    new PatternFlatTimeoutFunction<Event, TimeoutEvent>() {...},
    new PatternFlatSelectFunction<Event, ComplexEvent>() {...}
);
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">
In order to treat partial patterns, the `select` API call offers an overloaded version which takes as the first parameter a timeout function and as second parameter a selection function.
The timeout function is called with a map of string-event pairs of the partial match which has timed out and a long indicating when the timeout occurred.
The string is defined by the name of the state to which the event has been matched.
The timeout function returns exactly one result per call.
The return type of the timeout function can be different from the select function.
The timeout event and the select event are wrapped in `Left` and `Right` respectively so that the resulting data stream is of type `Either`.

{% highlight scala %}
val patternStream: PatternStream[Event] = CEP.pattern(input, pattern)

DataStream[Either[TimeoutEvent, ComplexEvent]] result = patternStream.select{
    (pattern: mutable.Map[String, Event], timestamp: Long) => TimeoutEvent()
} {
    pattern: mutable.Map[String, Event] => ComplexEvent()
}
{% endhighlight %}

The `flatSelect` API call offers the same overloaded version which takes as the first parameter a timeout function and as second parameter a selection function.
In contrast to the `select` functions, the `flatSelect` functions are called with an `Collector`.
The collector can be used to emit an arbitrary number of events.

{% highlight scala %}
val patternStream: PatternStream[Event] = CEP.pattern(input, pattern)

DataStream[Either[TimeoutEvent, ComplexEvent]] result = patternStream.flatSelect{
    (pattern: mutable.Map[String, Event], timestamp: Long, out: Collector[TimeoutEvent]) =>
        out.collect(TimeoutEvent())
} {
    (pattern: mutable.Map[String, Event], out: Collector[ComplexEvent]) =>
        out.collect(ComplexEvent())
}
{% endhighlight %}

</div>
</div>

## Examples

The following example detects the pattern `start, middle(name = "error") -> end(name = "critical")` on a keyed data stream of `Events`.
The events are keyed by their ids and a valid pattern has to occur within 10 seconds.
The whole processing is done with event time.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = ...
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

DataStream<Event> input = ...

DataStream<Event> partitionedInput = input.keyBy(new KeySelector<Event, Integer>() {
	@Override
	public Integer getKey(Event value) throws Exception {
		return value.getId();
	}
});

Pattern<Event, ?> pattern = Pattern.<Event>begin("start")
	.next("middle").where(new SimpleCondition<Event>() {
		@Override
		public boolean filter(Event value) throws Exception {
			return value.getName().equals("error");
		}
	}).followedBy("end").where(new SimpleCondition<Event>() {
		@Override
		public boolean filter(Event value) throws Exception {
			return value.getName().equals("critical");
		}
	}).within(Time.seconds(10));

PatternStream<Event> patternStream = CEP.pattern(partitionedInput, pattern);

DataStream<Alert> alerts = patternStream.select(new PatternSelectFunction<Event, Alert>() {
	@Override
	public Alert select(Map<String, Event> pattern) throws Exception {
		return createAlert(pattern);
	}
});
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env : StreamExecutionEnvironment = ...
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

val input : DataStream[Event] = ...

val partitionedInput = input.keyBy(event => event.getId)

val pattern = Pattern.begin("start")
  .next("middle").where(_.getName == "error")
  .followedBy("end").where(_.getName == "critical")
  .within(Time.seconds(10))

val patternStream = CEP.pattern(partitionedInput, pattern)

val alerts = patternStream.select(createAlert(_)))
{% endhighlight %}
</div>
</div>
