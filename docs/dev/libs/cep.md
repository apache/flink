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

FlinkCEP is the Complex Event Processing (CEP) library implemented on top of Flink.
It allows you to easily detect event patterns in an endless stream of events, thus
giving you the opportunity to quickly get hold of what's really important in your
data.

This page describes the API calls available in Flink CEP. We start by presenting the [Pattern API](#the-pattern-api),
which allows you to specify the patterns that you want to detect in your stream, before presenting how you can
[detect and act upon matching event sequences](#detecting-patterns). At the end, we present the assumptions the CEP
library makes when [dealing with lateness](#handling-lateness-in-event-time) in event time and how you can
[migrate your job](#migrating-from-an-older-flink-version) from an older Flink version to Flink-1.3.

* This will be replaced by the TOC
{:toc}

## Getting Started

If you want to jump right in, you have to [set up a Flink program]({{ site.baseurl }}/dev/linking_with_flink.html) and
add the FlinkCEP dependency to the `pom.xml` of your project.

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

Now you can start writing your first CEP program using the Pattern API.

{% warn Attention %} The events in the `DataStream` to which
you want to apply pattern matching must implement proper `equals()` and `hashCode()` methods
because these are used for comparing and matching events.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Event> input = ...

Pattern<Event, ?> pattern = Pattern.<Event>begin("start").where(
        new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) {
                return event.getId() == 42;
            }
        }
    ).next("middle").subtype(SubEvent.class).where(
        new SimpleCondition<Event>() {
            @Override
            public boolean filter(SubEvent subEvent) {
                return subEvent.getVolume() >= 10.0;
            }
        }
    ).followedBy("end").where(
         new SimpleCondition<Event>() {
            @Override
            public boolean filter(Event event) {
                return event.getName().equals("end");
            }
         }
    );

PatternStream<Event> patternStream = CEP.pattern(input, pattern);

DataStream<Alert> result = patternStream.select(
    new PatternSelectFunction<Event, Alert> {
        @Override
        public Alert select(Map<String, List<Event>> pattern) throws Exception {
            return createAlertFrom(pattern);
        }
    }
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

## The Pattern API

The pattern API allows you to quickly define complex pattern sequences that you want to extract
from your input stream.

Each such complex pattern sequence consists of multiple simple patterns, i.e. patterns looking for
individual events with the same properties. From now on, these simple patterns will be called **patterns**, and
the final complex pattern sequence we are searching for in the stream, the **pattern sequence**. A pattern sequence
can be seen as a graph of such patterns, where transitions from one pattern to the next occur based on user-specified
*conditions*, e.g. `event.getName().equals("start")`. A **match** is a sequence of input events which visits all
patterns of the complex pattern graph, through a sequence of valid pattern transitions.

{% warn Attention %} Each pattern must have a unique name, which is used to later
identify the matched events.

{% warn Attention %} Pattern names **CANNOT** contain the character `":"`.

In the remainder of this section we will first describe how to define [Individual Patterns](#individual-patterns), and then cover how you can
combine individual patterns into [Complex Patterns](#combining-patterns).

### Individual Patterns

A **Pattern** can be either a *singleton* pattern, or a *looping* one. Singleton patterns accept a single
event, while looping ones can accept more than one. In pattern matching symbols, in the pattern `"a b+ c? d"` (or `"a"`,
followed by *one or more* `"b"`'s, optionally followed by a `"c"`, followed by a `"d"`), `a`, `c?`, and `d` are
singleton patterns, while `b+` is a looping one. By default, a pattern is a singleton pattern and you can transform
it to a looping one by using [Quantifiers](#quantifiers). In addition, each pattern can have one or more
[Conditions](#conditions) based on which it accepts events.

#### Quantifiers

In FlinkCEP, looping patterns can be specified using these methods: `pattern.oneOrMore()`, for patterns that expect one or
more occurrences of a given event (e.g. the `b+` mentioned previously); and `pattern.times(#ofTimes)`, for patterns that
expect a specific number of occurrences of a given type of event, e.g. 4 `a`'s; and `pattern.times(#fromTimes, #toTimes)`,
for patterns that expect a specific minimum number of occurrences and maximum number of occurrences of a given type of event,
e.g. 2-4 `a`s. All patterns, looping or not, can be made optional using the `pattern.optional()` method. For a pattern
named `start`, the following are valid quantifiers:

 <div class="codetabs" markdown="1">
 <div data-lang="java" markdown="1">
 {% highlight java %}
 // expecting 4 occurrences
 start.times(4);

 // expecting 0 or 4 occurrences
 start.times(4).optional();

 // expecting 2, 3 or 4 occurrences
 start.times(2, 4);

 // expecting 0, 2, 3 or 4 occurrences
 start.times(2, 4).optional();

 // expecting 1 or more occurrences
 start.oneOrMore();

 // expecting 0 or more occurrences
 start.oneOrMore().optional();

 // expecting 2 or more occurrences
 start.timesOrMore(2);

 // expecting 0, 2 or more occurrences
 start.timesOrMore(2).optional();

 {% endhighlight %}
 </div>

 <div data-lang="scala" markdown="1">
 {% highlight scala %}
 // expecting 4 occurrences
 start.times(4)

 // expecting 0 or 4 occurrences
 start.times(4).optional()

 // expecting 2, 3 or 4 occurrences
 start.times(2, 4);

 // expecting 0, 2, 3 or 4 occurrences
 start.times(2, 4).optional();

 // expecting 1 or more occurrences
 start.oneOrMore()

 // expecting 0 or more occurrences
 start.oneOrMore().optional()

 // expecting 2 or more occurrences
 start.timesOrMore(2);

 // expecting 0, 2 or more occurrences
 start.timesOrMore(2).optional();
 {% endhighlight %}
 </div>
 </div>

#### Conditions

At every pattern, and in order to go from one pattern to the next, you can specify additional **conditions**.
These conditions can be related to:

 1. a [property of the incoming event](#conditions-on-properties), e.g. its value should be larger than 5,
 or larger than the average value of the previously accepted events.

 2. the [contiguity of the matching events](#conditions-on-contiguity), e.g. detect pattern `a,b,c` without
 non-matching events between any matching ones.

The latter refers to "looping" patterns, *i.e.* patterns that can accept more than one event, e.g. the `b+` in `a b+ c`,
which searches for one or more `b`'s.

##### Conditions on Properties

Conditions on the event properties can be specified via the `pattern.where()`, `pattern.or()` or the `pattern.until()` method. These can
be either `IterativeCondition`s or `SimpleCondition`s.

**Iterative Conditions:** This is the most general type of conditions. This is how you can specify a condition that
accepts subsequent events based on properties of the previously accepted events or some statistic over a subset of them.

Below is the code for an iterative condition that accepts the next event for a pattern named "middle" if its name starts
with "foo", and if the sum of the prices of the previously accepted events for that pattern plus the price of the current
event do not exceed the value of 5.0. Iterative conditions can be very powerful, especially in combination with looping
patterns, e.g. `oneOrMore()`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
middle.oneOrMore().where(new IterativeCondition<SubEvent>() {
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
middle.oneOrMore().where(
    (value, ctx) => {
        lazy val sum = ctx.getEventsForPattern("middle").asScala.map(_.getPrice).sum
        value.getName.startsWith("foo") && sum + value.getPrice < 5.0
    }
)
{% endhighlight %}
</div>
</div>

{% warn Attention %} The call to `context.getEventsForPattern(...)` finds all the
previously accepted events for a given potential match. The cost of this operation can vary, so when implementing
your condition, try to minimize its use.

**Simple Conditions:** This type of condition extends the aforementioned `IterativeCondition` class and decides
whether to accept an event or not, based *only* on properties of the event itself.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
start.where(new SimpleCondition<Event>() {
    @Override
    public boolean filter(Event value) {
        return value.getName().startsWith("foo");
    }
});
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
start.where(event => event.getName.startsWith("foo"))
{% endhighlight %}
</div>
</div>

Finally, we can also restrict the type of the accepted event to some subtype of the initial event type (here `Event`)
via the `pattern.subtype(subClass)` method.

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

**Combining Conditions:** As shown, the `subtype` condition can be combined with additional conditions.
In fact, this holds for every condition. You can arbitrarily combine conditions by sequentially calling
`where()`. The final result will be the logical **AND** of the results of the individual conditions. In
order to combine conditions using **OR**, you can use the `or()` method, as shown below.

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


**Stop condition:** In case of looping patterns (`oneOrMore()` and `oneOrMore().optional()`) you can
also specify a stop condition, e.g. accept events with value larger than 5 until the sum of values is smaller than 50.

To better understand it, have a look at the following example. Given

* pattern like `"(a+ until b)"` (one or more `"a"` until `"b"`)

* a sequence of incoming events `"a1" "c" "a2" "b" "a3"`

* the library will output results: `{a1 a2} {a1} {a2} {a3}`.

As you can see `{a1 a2 a3}` or `{a2 a3}` are not returned due to the stop condition.

##### Conditions on Contiguity

FlinkCEP supports the following forms of contiguity between events:

 1. Strict Contiguity: which expects all matching events to appear strictly the one after the other,
 without any non-matching events in-between.

 2. Relaxed Contiguity: which simply ignores non-matching events appearing in-between the matching ones.

 3. Non-Deterministic Relaxed Contiguity: which further relaxes contiguity, allowing additional matches
 that ignore some matching events.

To illustrate the above with an example, a pattern sequence `"a+ b"` (one or more `"a"`'s followed by a `"b"`) with
input `"a1", "c", "a2", "b"` will have the following results:

 1. Strict Contiguity: `{a2 b}` -- the `"c"` after `"a1"` causes `"a1"` to be discarded.

 2. Relaxed Contiguity: `{a1 b}` and `{a1 a2 b}` -- `c` is simply ignored.

 3. Non-Deterministic Relaxed Contiguity: `{a1 b}`, `{a2 b}`, and `{a1 a2 b}`.

For looping patterns (e.g. `oneOrMore()` and `times()`) the default is *relaxed contiguity*. If you want
strict contiguity, you have to explicitly specify it by using the `consecutive()` call, and if you want
*non-deterministic relaxed contiguity* you can use the `allowCombinations()` call.

{% warn Attention %}
In this section we are talking about contiguity *within* a single looping pattern, and the
`consecutive()` and `allowCombinations()` calls need to be understood in that context. Later when looking at
[Combining Patterns](#combining-patterns) we'll discuss other calls, such as `next()` and `followedBy()`,
that are used to specify contiguity conditions *between* patterns.

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
            <td><strong>where(condition)</strong></td>
            <td>
                <p>Defines a condition for the current pattern. To match the pattern, an event must satisfy the condition.
                 Multiple consecutive where() clauses lead to their conditions being ANDed:</p>
{% highlight java %}
pattern.where(new IterativeCondition<Event>() {
    @Override
    public boolean filter(Event value, Context ctx) throws Exception {
        return ... // some condition
    }
});
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>or(condition)</strong></td>
            <td>
                <p>Adds a new condition which is ORed with an existing one. An event can match the pattern only if it
                passes at least one of the conditions:</p>
{% highlight java %}
pattern.where(new IterativeCondition<Event>() {
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
                 <td><strong>until(condition)</strong></td>
                 <td>
                     <p>Specifies a stop condition for looping pattern. Meaning if event matching the given condition occurs, no more
                     events will be accepted into the pattern.</p>
                     <p>Applicable only in conjunction with <code>oneOrMore()</code></p>
                     <p><b>NOTE:</b> It allows for cleaning state for corresponding pattern on event-based condition.</p>
{% highlight java %}
pattern.oneOrMore().until(new IterativeCondition<Event>() {
    @Override
    public boolean filter(Event value, Context ctx) throws Exception {
        return ... // alternative condition
    }
});
{% endhighlight %}
                 </td>
              </tr>
       <tr>
           <td><strong>subtype(subClass)</strong></td>
           <td>
               <p>Defines a subtype condition for the current pattern. An event can only match the pattern if it is
                of this subtype:</p>
{% highlight java %}
pattern.subtype(SubEvent.class);
{% endhighlight %}
           </td>
       </tr>
       <tr>
          <td><strong>oneOrMore()</strong></td>
          <td>
              <p>Specifies that this pattern expects at least one occurrence of a matching event.</p>
              <p>By default a relaxed internal contiguity (between subsequent events) is used. For more info on
              internal contiguity see <a href="#consecutive_java">consecutive</a>.</p>
              <p><b>NOTE:</b> It is advised to use either <code>until()</code> or <code>within()</code> to enable state clearing</p>
{% highlight java %}
pattern.oneOrMore();
{% endhighlight %}
          </td>
       </tr>
           <tr>
              <td><strong>timesOrMore(#times)</strong></td>
              <td>
                  <p>Specifies that this pattern expects at least <strong>#times</strong> occurrences
                  of a matching event.</p>
                  <p>By default a relaxed internal contiguity (between subsequent events) is used. For more info on
                  internal contiguity see <a href="#consecutive_java">consecutive</a>.</p>
{% highlight java %}
pattern.timesOrMore(2);
{% endhighlight %}
           </td>
       </tr>
       <tr>
          <td><strong>times(#ofTimes)</strong></td>
          <td>
              <p>Specifies that this pattern expects an exact number of occurrences of a matching event.</p>
              <p>By default a relaxed internal contiguity (between subsequent events) is used. For more info on
              internal contiguity see <a href="#consecutive_java">consecutive</a>.</p>
{% highlight java %}
pattern.times(2);
{% endhighlight %}
          </td>
       </tr>
       <tr>
          <td><strong>times(#fromTimes, #toTimes)</strong></td>
          <td>
              <p>Specifies that this pattern expects occurrences between <strong>#fromTimes</strong>
              and <strong>#toTimes</strong> of a matching event.</p>
              <p>By default a relaxed internal contiguity (between subsequent events) is used. For more info on
              internal contiguity see <a href="#consecutive_java">consecutive</a>.</p>
{% highlight java %}
pattern.times(2, 4);
{% endhighlight %}
          </td>
       </tr>
       <tr>
          <td><strong>optional()</strong></td>
          <td>
              <p>Specifies that this pattern is optional, i.e. it may not occur at all. This is applicable to all
              aforementioned quantifiers.</p>
{% highlight java %}
pattern.oneOrMore().optional();
{% endhighlight %}
          </td>
       </tr>
       <tr>
          <td><strong>consecutive()</strong><a name="consecutive_java"></a></td>
          <td>
              <p>Works in conjunction with <code>oneOrMore()</code> and <code>times()</code> and imposes strict contiguity between the matching
              events, i.e. any non-matching element breaks the match (as in <code>next()</code>).</p>
              <p>If not applied a relaxed contiguity (as in <code>followedBy()</code>) is used.</p>

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
}).oneOrMore().consecutive()
.followedBy("end1").where(new SimpleCondition<Event>() {
  @Override
  public boolean filter(Event value) throws Exception {
    return value.getName().equals("b");
  }
});
{% endhighlight %}
              <p>Will generate the following matches for an input sequence: C D A1 A2 A3 D A4 B</p>

              <p>with consecutive applied: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}</p>
              <p>without consecutive applied: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}, {C A1 A2 A3 A4 B}</p>
          </td>
       </tr>
       <tr>
       <td><strong>allowCombinations()</strong><a name="allow_comb_java"></a></td>
       <td>
              <p>Works in conjunction with <code>oneOrMore()</code> and <code>times()</code> and imposes non-deterministic relaxed contiguity
              between the matching events (as in <code>followedByAny()</code>).</p>
              <p>If not applied a relaxed contiguity (as in <code>followedBy()</code>) is used.</p>

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
}).oneOrMore().allowCombinations()
.followedBy("end1").where(new SimpleCondition<Event>() {
  @Override
  public boolean filter(Event value) throws Exception {
    return value.getName().equals("b");
  }
});
{% endhighlight %}
               <p>Will generate the following matches for an input sequence: C D A1 A2 A3 D A4 B</p>

               <p>with combinations enabled: {C A1 B}, {C A1 A2 B}, {C A1 A3 B}, {C A1 A4 B}, {C A1 A2 A3 B}, {C A1 A2 A4 B}, {C A1 A3 A4 B}, {C A1 A2 A3 A4 B}</p>
               <p>without combinations enabled: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}, {C A1 A2 A3 A4 B}</p>
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
            <td><strong>where(condition)</strong></td>
            <td>
              <p>Defines a condition for the current pattern. To match the pattern, an event must satisfy the condition.
                                  Multiple consecutive where() clauses lead to their conditions being ANDed:</p>
{% highlight scala %}
pattern.where(event => ... /* some condition */)
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>or(condition)</strong></td>
            <td>
                <p>Adds a new condition which is ORed with an existing one. An event can match the pattern only if it
                passes at least one of the conditions:</p>
{% highlight scala %}
pattern.where(event => ... /* some condition */)
    .or(event => ... /* alternative condition */)
{% endhighlight %}
                    </td>
                </tr>
<tr>
          <td><strong>until(condition)</strong></td>
          <td>
              <p>Specifies a stop condition for looping pattern. Meaning if event matching the given condition occurs, no more
              events will be accepted into the pattern.</p>
              <p>Applicable only in conjunction with <code>oneOrMore()</code></p>
              <p><b>NOTE:</b> It allows for cleaning state for corresponding pattern on event-based condition.</p>
{% highlight scala %}
pattern.oneOrMore().until(event => ... /* some condition */)
{% endhighlight %}
          </td>
       </tr>
       <tr>
           <td><strong>subtype(subClass)</strong></td>
           <td>
               <p>Defines a subtype condition for the current pattern. An event can only match the pattern if it is
               of this subtype:</p>
{% highlight scala %}
pattern.subtype(classOf[SubEvent])
{% endhighlight %}
           </td>
       </tr>
       <tr>
          <td><strong>oneOrMore()</strong></td>
          <td>
               <p>Specifies that this pattern expects at least one occurrence of a matching event.</p>
                            <p>By default a relaxed internal contiguity (between subsequent events) is used. For more info on
                            internal contiguity see <a href="#consecutive_scala">consecutive</a>.</p>
                            <p><b>NOTE:</b> It is advised to use either <code>until()</code> or <code>within()</code> to enable state clearing</p>
{% highlight scala %}
pattern.oneOrMore()
{% endhighlight %}
          </td>
       </tr>
       <tr>
          <td><strong>timesOrMore(#times)</strong></td>
          <td>
              <p>Specifies that this pattern expects at least <strong>#times</strong> occurrences
              of a matching event.</p>
              <p>By default a relaxed internal contiguity (between subsequent events) is used. For more info on
              internal contiguity see <a href="#consecutive_scala">consecutive</a>.</p>
{% highlight scala %}
pattern.timesOrMore(2)
{% endhighlight %}
           </td>
       </tr>
       <tr>
                 <td><strong>times(#ofTimes)</strong></td>
                 <td>
                     <p>Specifies that this pattern expects an exact number of occurrences of a matching event.</p>
                                   <p>By default a relaxed internal contiguity (between subsequent events) is used.
                                   For more info on internal contiguity see <a href="#consecutive_scala">consecutive</a>.</p>
{% highlight scala %}
pattern.times(2)
{% endhighlight %}
                 </td>
       </tr>
       <tr>
         <td><strong>times(#fromTimes, #toTimes)</strong></td>
         <td>
             <p>Specifies that this pattern expects occurrences between <strong>#fromTimes</strong>
             and <strong>#toTimes</strong> of a matching event.</p>
             <p>By default a relaxed internal contiguity (between subsequent events) is used. For more info on
             internal contiguity see <a href="#consecutive_java">consecutive</a>.</p>
{% highlight scala %}
pattern.times(2, 4);
{% endhighlight %}
         </td>
       </tr>
       <tr>
          <td><strong>optional()</strong></td>
          <td>
             <p>Specifies that this pattern is optional, i.e. it may not occur at all. This is applicable to all
                           aforementioned quantifiers.</p>
{% highlight scala %}
pattern.oneOrMore().optional()
{% endhighlight %}
          </td>
       </tr>
       <tr>
          <td><strong>consecutive()</strong><a name="consecutive_scala"></a></td>
          <td>
            <p>Works in conjunction with <code>oneOrMore()</code> and <code>times()</code> and imposes strict contiguity between the matching
                          events, i.e. any non-matching element breaks the match (as in <code>next()</code>).</p>
                          <p>If not applied a relaxed contiguity (as in <code>followedBy()</code>) is used.</p>

      <p>E.g. a pattern like:</p>
{% highlight scala %}
Pattern.begin("start").where(_.getName().equals("c"))
  .followedBy("middle").where(_.getName().equals("a"))
                       .oneOrMore().consecutive()
  .followedBy("end1").where(_.getName().equals("b"));
{% endhighlight %}

            <p>Will generate the following matches for an input sequence: C D A1 A2 A3 D A4 B</p>

                          <p>with consecutive applied: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}</p>
                          <p>without consecutive applied: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}, {C A1 A2 A3 A4 B}</p>
          </td>
       </tr>
       <tr>
              <td><strong>allowCombinations()</strong><a name="allow_comb_java"></a></td>
              <td>
                <p>Works in conjunction with <code>oneOrMore()</code> and <code>times()</code> and imposes non-deterministic relaxed contiguity
                     between the matching events (as in <code>followedByAny()</code>).</p>
                     <p>If not applied a relaxed contiguity (as in <code>followedBy()</code>) is used.</p>

      <p>E.g. a pattern like:</p>
{% highlight scala %}
Pattern.begin("start").where(_.getName().equals("c"))
  .followedBy("middle").where(_.getName().equals("a"))
                       .oneOrMore().allowCombinations()
  .followedBy("end1").where(_.getName().equals("b"));
{% endhighlight %}

                      <p>Will generate the following matches for an input sequence: C D A1 A2 A3 D A4 B</p>

                      <p>with combinations enabled: {C A1 B}, {C A1 A2 B}, {C A1 A3 B}, {C A1 A4 B}, {C A1 A2 A3 B}, {C A1 A2 A4 B}, {C A1 A3 A4 B}, {C A1 A2 A3 A4 B}</p>
                      <p>without combinations enabled: {C A1 B}, {C A1 A2 B}, {C A1 A2 A3 B}, {C A1 A2 A3 A4 B}</p>
              </td>
              </tr>
  </tbody>
</table>
</div>

</div>

### Combining Patterns

Now that we have seen what an individual pattern can look like, it is time to see how to combine them
into a full pattern sequence.

A pattern sequence has to start with an initial pattern, as shown below:

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

Next, you can append more patterns to your pattern sequence by specifying the desired *contiguity conditions* between
them. In the [previous section](#conditions-on-contiguity) we described the different contiguity modes supported by
Flink, namely *strict*, *relaxed*, and *non-deterministic relaxed*, and how to apply them in looping patterns. To apply
them between consecutive patterns, you can use:

1. `next()`, for *strict*,
2. `followedBy()`, for *relaxed*, and
3. `followedByAny()`, for *non-deterministic relaxed* contiguity.

or

1. `notNext()`, if you do not want an event type to directly follow another
2. `notFollowedBy()`, if you do not want an event type to be anywhere between two other event types

{% warn Attention %} A pattern sequence cannot end in `notFollowedBy()`.

{% warn Attention %} A `NOT` pattern cannot be preceded by an optional one.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

// strict contiguity
Pattern<Event, ?> strict = start.next("middle").where(...);

// relaxed contiguity
Pattern<Event, ?> relaxed = start.followedBy("middle").where(...);

// non-deterministic relaxed contiguity
Pattern<Event, ?> nonDetermin = start.followedByAny("middle").where(...);

// NOT pattern with strict contiguity
Pattern<Event, ?> strictNot = start.notNext("not").where(...);

// NOT pattern with relaxed contiguity
Pattern<Event, ?> relaxedNot = start.notFollowedBy("not").where(...);

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

// strict contiguity
val strict: Pattern[Event, _] = start.next("middle").where(...)

// relaxed contiguity
val relaxed: Pattern[Event, _] = start.followedBy("middle").where(...)

// non-deterministic relaxed contiguity
val nonDetermin: Pattern[Event, _] = start.followedByAny("middle").where(...)

// NOT pattern with strict contiguity
val strictNot: Pattern[Event, _] = start.notNext("not").where(...)

// NOT pattern with relaxed contiguity
val relaxedNot: Pattern[Event, _] = start.notFollowedBy("not").where(...)

{% endhighlight %}
</div>
</div>

Bear in mind that relaxed contiguity means that only the first succeeding matching event will be matched, while
with non-deterministic relaxed contiguity, multiple matches will be emitted for the same beginning. As an example,
a pattern `a b`, given the event sequence `"a", "c", "b1", "b2"`, will give the following results:

1. Strict Contiguity between `a` and `b`: `{}` (no match) -- the `"c"` after `"a"` causes `"a"` to be discarded.

2. Relaxed Contiguity between `a` and `b`: `{a b1}` -- as relaxed continuity is viewed as "skip non-matching events
till the next matching one".

3. Non-Deterministic Relaxed Contiguity between `a` and `b`: `{a b1}`, `{a b2}` -- as this is the most general form.

Finally, it is also possible to define a temporal constraint for the pattern to be valid.
For example, you can define that a pattern should occur within 10 seconds via the `pattern.within()` method.
Temporal patterns are supported for both [processing and event time]({{site.baseurl}}/dev/event_time.html).

{% warn Attention %} A pattern sequence can only have one temporal constraint. If
multiple such constraints are defined on different individual patterns, then the smallest one is applied.

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

It is also possible to define a pattern sequence as the condition for `begin`, `followedBy`, `followedByAny` and
`next`. The pattern sequence will be considered as the matching condition logically and a `GroupPattern` will be
returned and it is possible to apply `oneOrMore()`, `times(#ofTimes)`, `times(#fromTimes, #toTimes)`, `optional()`,
`consecutive()`, `allowCombinations()` to the `GroupPattern`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

Pattern<Event, ?> start = Pattern.begin(
    Pattern.<Event>begin("start").where(...).followedBy("start_middle").where(...)
);

// strict contiguity
Pattern<Event, ?> strict = start.next(
    Pattern.<Event>begin("next_start").where(...).followedBy("next_middle").where(...)
).times(3);

// relaxed contiguity
Pattern<Event, ?> relaxed = start.followedBy(
    Pattern.<Event>begin("followedby_start").where(...).followedBy("followedby_middle").where(...)
).oneOrMore();

// non-deterministic relaxed contiguity
Pattern<Event, ?> nonDetermin = start.followedByAny(
    Pattern.<Event>begin("followedbyany_start").where(...).followedBy("followedbyany_middle").where(...)
).optional();

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

val start: Pattern[Event, _] = Pattern.begin(
    Pattern.begin[Event, _]("start").where(...).followedBy("start_middle").where(...)
)

// strict contiguity
val strict: Pattern[Event, _] = start.next(
    Pattern.begin[Event, _]("next_start").where(...).followedBy("next_middle").where(...)
).times(3)

// relaxed contiguity
val relaxed: Pattern[Event, _] = start.followedBy(
    Pattern.begin[Event, _]("followedby_start").where(...).followedBy("followedby_middle").where(...)
).oneOrMore()

// non-deterministic relaxed contiguity
val nonDetermin: Pattern[Event, _] = start.followedByAny(
    Pattern.begin[Event, _]("followedbyany_start").where(...).followedBy("followedbyany_middle").where(...)
).optional()

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
            <td><strong>begin(#name)</strong></td>
            <td>
            <p>Defines a starting pattern:</p>
{% highlight java %}
Pattern<Event, ?> start = Pattern.<Event>begin("start");
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>begin(#pattern_sequence)</strong></td>
            <td>
            <p>Defines a starting pattern:</p>
{% highlight java %}
Pattern<Event, ?> start = Pattern.<Event>begin(
    Pattern.<Event>begin("start").where(...).followedBy("middle").where(...)
);
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>next(#name)</strong></td>
            <td>
                <p>Appends a new pattern. A matching event has to directly succeed the previous matching event
                (strict contiguity):</p>
{% highlight java %}
Pattern<Event, ?> next = start.next("middle");
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>next(#pattern_sequence)</strong></td>
            <td>
                <p>Appends a new pattern. A sequence of matching events have to directly succeed the previous matching event
                (strict contiguity):</p>
{% highlight java %}
Pattern<Event, ?> next = start.next(
    Pattern.<Event>begin("start").where(...).followedBy("middle").where(...)
);
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>followedBy(#name)</strong></td>
            <td>
                <p>Appends a new pattern. Other events can occur between a matching event and the previous
                matching event (relaxed contiguity):</p>
{% highlight java %}
Pattern<Event, ?> followedBy = start.followedBy("middle");
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>followedBy(#pattern_sequence)</strong></td>
            <td>
                 <p>Appends a new pattern. Other events can occur between a sequence of matching events and the previous
                 matching event (relaxed contiguity):</p>
{% highlight java %}
Pattern<Event, ?> followedBy = start.followedBy(
    Pattern.<Event>begin("start").where(...).followedBy("middle").where(...)
);
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>followedByAny(#name)</strong></td>
            <td>
                <p>Appends a new pattern. Other events can occur between a matching event and the previous
                matching event, and alternative matches will be presented for every alternative matching event
                (non-deterministic relaxed contiguity):</p>
{% highlight java %}
Pattern<Event, ?> followedByAny = start.followedByAny("middle");
{% endhighlight %}
             </td>
        </tr>
        <tr>
             <td><strong>followedByAny(#pattern_sequence)</strong></td>
             <td>
                 <p>Appends a new pattern. Other events can occur between a sequence of matching events and the previous
                 matching event, and alternative matches will be presented for every alternative sequence of matching events
                 (non-deterministic relaxed contiguity):</p>
{% highlight java %}
Pattern<Event, ?> followedByAny = start.followedByAny(
    Pattern.<Event>begin("start").where(...).followedBy("middle").where(...)
);
{% endhighlight %}
             </td>
        </tr>
        <tr>
                    <td><strong>notNext()</strong></td>
                    <td>
                        <p>Appends a new negative pattern. A matching (negative) event has to directly succeed the
                        previous matching event (strict contiguity) for the partial match to be discarded:</p>
{% highlight java %}
Pattern<Event, ?> notNext = start.notNext("not");
{% endhighlight %}
                    </td>
                </tr>
                <tr>
                    <td><strong>notFollowedBy()</strong></td>
                    <td>
                        <p>Appends a new negative pattern. A partial matching event sequence will be discarded even
                        if other events occur between the matching (negative) event and the previous matching event
                        (relaxed contiguity):</p>
{% highlight java %}
Pattern<Event, ?> notFollowedBy = start.notFllowedBy("not");
{% endhighlight %}
                    </td>
                </tr>
       <tr>
          <td><strong>within(time)</strong></td>
          <td>
              <p>Defines the maximum time interval for an event sequence to match the pattern. If a non-completed event
              sequence exceeds this time, it is discarded:</p>
{% highlight java %}
pattern.within(Time.seconds(10));
{% endhighlight %}
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
            <td><strong>begin()</strong></td>
            <td>
            <p>Defines a starting pattern:</p>
{% highlight scala %}
val start = Pattern.begin[Event]("start")
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>next(#name)</strong></td>
            <td>
                <p>Appends a new pattern. A matching event has to directly succeed the previous matching event
                (strict contiguity):</p>
{% highlight scala %}
val next = start.next("middle")
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>next(#pattern_sequence)</strong></td>
            <td>
                <p>Appends a new pattern. A sequence of matching events have to directly succeed the previous matching event
                (strict contiguity):</p>
{% highlight scala %}
val next = start.next(
    Pattern.begin[Event]("start").where(...).followedBy("middle").where(...)
)
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>followedBy(#name)</strong></td>
            <td>
                <p>Appends a new pattern. Other events can occur between a matching event and the previous
                matching event (relaxed contiguity) :</p>
{% highlight scala %}
val followedBy = start.followedBy("middle")
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>followedBy(#pattern_sequence)</strong></td>
            <td>
                <p>Appends a new pattern. Other events can occur between a sequence of matching events and the previous
                matching event (relaxed contiguity) :</p>
{% highlight scala %}
val followedBy = start.followedBy(
    Pattern.begin[Event]("start").where(...).followedBy("middle").where(...)
)
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>followedByAny(#name)</strong></td>
            <td>
                <p>Appends a new pattern. Other events can occur between a matching event and the previous
                matching event, and alternative matches will be presented for every alternative matching event
                (non-deterministic relaxed contiguity):</p>
{% highlight scala %}
val followedByAny = start.followedByAny("middle")
{% endhighlight %}
            </td>
         </tr>
         <tr>
             <td><strong>followedByAny(#pattern_sequence)</strong></td>
             <td>
                 <p>Appends a new pattern. Other events can occur between a sequence of matching events and the previous
                 matching event, and alternative matches will be presented for every alternative sequence of matching events
                 (non-deterministic relaxed contiguity):</p>
{% highlight scala %}
val followedByAny = start.followedByAny(
    Pattern.begin[Event]("start").where(...).followedBy("middle").where(...)
)
{% endhighlight %}
             </td>
         </tr>

                <tr>
                                    <td><strong>notNext()</strong></td>
                                    <td>
                                        <p>Appends a new negative pattern. A matching (negative) event has to directly succeed the
                                        previous matching event (strict contiguity) for the partial match to be discarded:</p>
{% highlight scala %}
val notNext = start.notNext("not")
{% endhighlight %}
                                    </td>
                                </tr>
                                <tr>
                                    <td><strong>notFollowedBy()</strong></td>
                                    <td>
                                        <p>Appends a new negative pattern. A partial matching event sequence will be discarded even
                                        if other events occur between the matching (negative) event and the previous matching event
                                        (relaxed contiguity):</p>
{% highlight scala %}
val notFollowedBy = start.notFllowedBy("not")
{% endhighlight %}
                                    </td>
                                </tr>

       <tr>
          <td><strong>within(time)</strong></td>
          <td>
              <p>Defines the maximum time interval for an event sequence to match the pattern. If a non-completed event
              sequence exceeds this time, it is discarded:</p>
{% highlight scala %}
pattern.within(Time.seconds(10))
{% endhighlight %}
          </td>
      </tr>
  </tbody>
</table>
</div>

</div>

## Detecting Patterns

After specifying the pattern sequence you are looking for, it is time to apply it to your input stream to detect
potential matches. In order to run a stream of events against your pattern sequence, you have to create a `PatternStream`.
Given an input stream `input`, a pattern `pattern` and an optional comparator `comparator` which is used to
sort events with the same timestamp in case of EventTime or that arrived at the same moment, you create the
`PatternStream` by calling:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Event> input = ...
Pattern<Event, ?> pattern = ...
EventComparator<Event> comparator = ... // optional

PatternStream<Event> patternStream = CEP.pattern(input, pattern, comparator);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val input : DataStream[Event] = ...
val pattern : Pattern[Event, _] = ...
var comparator : EventComparator[Event] = ... // optional

val patternStream: PatternStream[Event] = CEP.pattern(input, pattern, comparator)
{% endhighlight %}
</div>
</div>

The input stream can be *keyed* or *non-keyed* depending on your use-case.

{% warn Attention %} Applying your pattern on a non-keyed stream will result in a job with
parallelism equal to 1.

### Selecting from Patterns

Once you have obtained a `PatternStream` you can select from detected event sequences via the `select` or `flatSelect` methods.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
The `select()` method requires a `PatternSelectFunction` implementation.
A `PatternSelectFunction` has a `select` method which is called for each matching event sequence.
It receives a match in the form of `Map<String, List<IN>>` where the key is the name of each pattern in your pattern
sequence and the value is a list of all accepted events for that pattern (`IN` is the type of your input elements).
The events for a given pattern are ordered by timestamp. The reason for returning a list of accepted events for each
pattern is that when using looping patterns (e.g. `oneToMany()` and `times()`), more than one event may be accepted for a
given pattern. The selection function returns exactly one result.

{% highlight java %}
class MyPatternSelectFunction<IN, OUT> implements PatternSelectFunction<IN, OUT> {
    @Override
    public OUT select(Map<String, List<IN>> pattern) {
        IN startEvent = pattern.get("start").get(0);
        IN endEvent = pattern.get("end").get(0);
        return new OUT(startEvent, endEvent);
    }
}
{% endhighlight %}

A `PatternFlatSelectFunction` is similar to the `PatternSelectFunction`, with the only distinction that it can return an
arbitrary number of results. In order to do this, the `select` method has an additional `Collector` parameter which is
used to forward your output elements downstream.

{% highlight java %}
class MyPatternFlatSelectFunction<IN, OUT> implements PatternFlatSelectFunction<IN, OUT> {
    @Override
    public void select(Map<String, List<IN>> pattern, Collector<OUT> collector) {
        IN startEvent = pattern.get("start").get(0);
        IN endEvent = pattern.get("end").get(0);

        for (int i = 0; i < startEvent.getValue(); i++ ) {
            collector.collect(new OUT(startEvent, endEvent));
        }
    }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
The `select()` method takes a selection function as argument, which is called for each matching event sequence.
It receives a match in the form of `Map[String, Iterable[IN]]` where the key is the name of each pattern in your pattern
sequence and the value is an Iterable over all accepted events for that pattern (`IN` is the type of your input elements).
The events for a given pattern are ordered by timestamp. The reason for returning an iterable of accepted events for each
pattern is that when using looping patterns (e.g. `oneToMany()` and `times()`), more than one event may be accepted for a
given pattern. The selection function returns exactly one result per call.

{% highlight scala %}
def selectFn(pattern : Map[String, Iterable[IN]]): OUT = {
    val startEvent = pattern.get("start").get.next
    val endEvent = pattern.get("end").get.next
    OUT(startEvent, endEvent)
}
{% endhighlight %}

The `flatSelect` method is similar to the `select` method. Their only difference is that the function passed to the
`flatSelect` method can return an arbitrary number of results per call. In order to do this, the function for
`flatSelect` has an additional `Collector` parameter which is used to forward your output elements downstream.

{% highlight scala %}
def flatSelectFn(pattern : Map[String, Iterable[IN]], collector : Collector[OUT]) = {
    val startEvent = pattern.get("start").get.next
    val endEvent = pattern.get("end").get.next
    for (i <- 0 to startEvent.getValue) {
        collector.collect(OUT(startEvent, endEvent))
    }
}
{% endhighlight %}
</div>
</div>

### Handling Timed Out Partial Patterns

Whenever a pattern has a window length attached via the `within` keyword, it is possible that partial event sequences
are discarded because they exceed the window length. In order to react to these timed out partial matches the `select`
and `flatSelect` API calls allow a timeout handler to be specified. This timeout handler is called for each timed out
partial event sequence. The timeout handler receives all the events that have been matched so far by the pattern, and
the timestamp when the timeout was detected.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
In order to treat partial patterns, the `select` and `flatSelect` API calls offer an overloaded version which takes as
the first parameter a `PatternTimeoutFunction`/`PatternFlatTimeoutFunction` and as second parameter the known
`PatternSelectFunction`/`PatternFlatSelectFunction`. The return type of the timeout function can be different from the
select function. The timeout event and the select event are wrapped in `Either.Left` and `Either.Right` respectively
so that the resulting data stream is of type `org.apache.flink.types.Either`.

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
The string is defined by the name of the pattern to which the event has been matched.
The timeout function returns exactly one result per call.
The return type of the timeout function can be different from the select function.
The timeout event and the select event are wrapped in `Left` and `Right` respectively so that the resulting data stream is of type `Either`.

{% highlight scala %}
val patternStream: PatternStream[Event] = CEP.pattern(input, pattern)

DataStream[Either[TimeoutEvent, ComplexEvent]] result = patternStream.select{
    (pattern: Map[String, Iterable[Event]], timestamp: Long) => TimeoutEvent()
} {
    pattern: Map[String, Iterable[Event]] => ComplexEvent()
}
{% endhighlight %}

The `flatSelect` API call offers the same overloaded version which takes as the first parameter a timeout function and as second parameter a selection function.
In contrast to the `select` functions, the `flatSelect` functions are called with a `Collector`.
The collector can be used to emit an arbitrary number of events.

{% highlight scala %}
val patternStream: PatternStream[Event] = CEP.pattern(input, pattern)

DataStream[Either[TimeoutEvent, ComplexEvent]] result = patternStream.flatSelect{
    (pattern: Map[String, Iterable[Event]], timestamp: Long, out: Collector[TimeoutEvent]) =>
        out.collect(TimeoutEvent())
} {
    (pattern: mutable.Map[String, Iterable[Event]], out: Collector[ComplexEvent]) =>
        out.collect(ComplexEvent())
}
{% endhighlight %}

</div>
</div>

## Handling Lateness in Event Time

In `CEP` the order in which elements are processed matters. To guarantee that elements are processed in the correct order
when working in event time, an incoming element is initially put in a buffer where elements are *sorted in ascending
order based on their timestamp*, and when a watermark arrives, all the elements in this buffer with timestamps smaller
than that of the watermark are processed. This implies that elements between watermarks are processed in event-time order.

{% warn Attention %} The library assumes correctness of the watermark when working in event time.

To also guarantee that elements across watermarks are processed in event-time order, Flink's CEP library assumes
*correctness of the watermark*, and considers as *late* elements whose timestamp is smaller than that of the last
seen watermark. Late elements are not further processed.

## Examples

The following example detects the pattern `start, middle(name = "error") -> end(name = "critical")` on a keyed data
stream of `Events`. The events are keyed by their ids and a valid pattern has to occur within 10 seconds.
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
	public Alert select(Map<String, List<Event>> pattern) throws Exception {
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

## Migrating from an older Flink version

The CEP library in Flink-1.3 ships with a number of new features which have led to some changes in the API. Here we
describe the changes that you need to make to your old CEP jobs, in order to be able to run them with Flink-1.3. After
making these changes and recompiling your job, you will be able to resume its execution from a savepoint taken with the
old version of your job, *i.e.* without having to re-process your past data.

The changes required are:

1. Change your conditions (the ones in the `where(...)` clause) to extend the `SimpleCondition` class instead of
implementing the `FilterFunction` interface.

2. Change your functions provided as arguments to the `select(...)` and `flatSelect(...)` methods to expect a list of
events associated with each pattern (`List` in `Java`, `Iterable` in `Scala`). This is because with the addition of
the looping patterns, multiple input events can match a single (looping) pattern.

3. The `followedBy()` in Flink 1.1 and 1.2 implied `non-deterministic relaxed contiguity` (see
[here](#conditions-on-contiguity)). In Flink 1.3 this has changed and `followedBy()` implies `relaxed contiguity`,
while `followedByAny()` should be used if `non-deterministic relaxed contiguity` is required.
