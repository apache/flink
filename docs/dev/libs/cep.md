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
It allows you to detect event patterns in an endless stream of events, giving you the opportunity to get hold of what's important in your
data.

This page describes the API calls available in Flink CEP. We start by presenting the [Pattern API](#the-pattern-api),
which allows you to specify the patterns that you want to detect in your stream, before presenting how you can
[detect and act upon matching event sequences](#detecting-patterns). We then present the assumptions the CEP
library makes when [dealing with lateness](#handling-lateness-in-event-time) in event time and how you can
[migrate your job](#migrating-from-an-older-flink-versionpre-13) from an older Flink version to Flink-1.3.

* This will be replaced by the TOC
{:toc}

## Getting Started

If you want to jump right in, [set up a Flink program]({{ site.baseurl }}/dev/linking_with_flink.html) and
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

{% info %} FlinkCEP is not part of the binary distribution. See how to link with it for cluster execution [here]({{site.baseurl}}/dev/linking.html).

Now you can start writing your first CEP program using the Pattern API.

{% warn Attention %} The events in the `DataStream` to which
you want to apply pattern matching must implement proper `equals()` and `hashCode()` methods
because FlinkCEP uses them for comparing and matching events.

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
        new SimpleCondition<SubEvent>() {
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
    new PatternSelectFunction<Event, Alert>() {
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

val pattern = Pattern.begin[Event]("start").where(_.getId == 42)
  .next("middle").subtype(classOf[SubEvent]).where(_.getVolume >= 10.0)
  .followedBy("end").where(_.getName == "end")

val patternStream = CEP.pattern(input, pattern)

val result: DataStream[Alert] = patternStream.select(createAlert(_))
{% endhighlight %}
</div>
</div>

## The Pattern API

The pattern API allows you to define complex pattern sequences that you want to extract from your input stream.

Each complex pattern sequence consists of multiple simple patterns, i.e. patterns looking for individual events with the same properties. From now on, we will call these simple patterns **patterns**, and the final complex pattern sequence we are searching for in the stream, the **pattern sequence**. You can see a pattern sequence as a graph of such patterns, where transitions from one pattern to the next occur based on user-specified
*conditions*, e.g. `event.getName().equals("end")`. A **match** is a sequence of input events which visits all
patterns of the complex pattern graph, through a sequence of valid pattern transitions.

{% warn Attention %} Each pattern must have a unique name, which you use later to identify the matched events.

{% warn Attention %} Pattern names **CANNOT** contain the character `":"`.

In the rest of this section we will first describe how to define [Individual Patterns](#individual-patterns), and then how you can combine individual patterns into [Complex Patterns](#combining-patterns).

### Individual Patterns

A **Pattern** can be either a *singleton* or a *looping* pattern. Singleton patterns accept a single
event, while looping patterns can accept more than one. In pattern matching symbols, the pattern `"a b+ c? d"` (or `"a"`, followed by *one or more* `"b"`'s, optionally followed by a `"c"`, followed by a `"d"`), `a`, `c?`, and `d` are
singleton patterns, while `b+` is a looping one. By default, a pattern is a singleton pattern and you can transform
it to a looping one by using [Quantifiers](#quantifiers). Each pattern can have one or more
[Conditions](#conditions) based on which it accepts events.

#### Quantifiers

In FlinkCEP, you can specify looping patterns using these methods: `pattern.oneOrMore()`, for patterns that expect one or more occurrences of a given event (e.g. the `b+` mentioned before); and `pattern.times(#ofTimes)`, for patterns that
expect a specific number of occurrences of a given type of event, e.g. 4 `a`'s; and `pattern.times(#fromTimes, #toTimes)`, for patterns that expect a specific minimum number of occurrences and a maximum number of occurrences of a given type of event, e.g. 2-4 `a`s.

You can make looping patterns greedy using the `pattern.greedy()` method, but you cannot yet make group patterns greedy. You can make all patterns, looping or not, optional using the `pattern.optional()` method.

For a pattern named `start`, the following are valid quantifiers:

 <div class="codetabs" markdown="1">
 <div data-lang="java" markdown="1">
 {% highlight java %}
 // expecting 4 occurrences
 start.times(4);

 // expecting 0 or 4 occurrences
 start.times(4).optional();

 // expecting 2, 3 or 4 occurrences
 start.times(2, 4);

 // expecting 2, 3 or 4 occurrences and repeating as many as possible
 start.times(2, 4).greedy();

 // expecting 0, 2, 3 or 4 occurrences
 start.times(2, 4).optional();

 // expecting 0, 2, 3 or 4 occurrences and repeating as many as possible
 start.times(2, 4).optional().greedy();

 // expecting 1 or more occurrences
 start.oneOrMore();

 // expecting 1 or more occurrences and repeating as many as possible
 start.oneOrMore().greedy();

 // expecting 0 or more occurrences
 start.oneOrMore().optional();

 // expecting 0 or more occurrences and repeating as many as possible
 start.oneOrMore().optional().greedy();

 // expecting 2 or more occurrences
 start.timesOrMore(2);

 // expecting 2 or more occurrences and repeating as many as possible
 start.timesOrMore(2).greedy();

 // expecting 0, 2 or more occurrences and repeating as many as possible
 start.timesOrMore(2).optional().greedy();
 {% endhighlight %}
 </div>

 <div data-lang="scala" markdown="1">
 {% highlight scala %}
 // expecting 4 occurrences
 start.times(4)

 // expecting 0 or 4 occurrences
 start.times(4).optional()

 // expecting 2, 3 or 4 occurrences
 start.times(2, 4)

 // expecting 2, 3 or 4 occurrences and repeating as many as possible
 start.times(2, 4).greedy()

 // expecting 0, 2, 3 or 4 occurrences
 start.times(2, 4).optional()

 // expecting 0, 2, 3 or 4 occurrences and repeating as many as possible
 start.times(2, 4).optional().greedy()

 // expecting 1 or more occurrences
 start.oneOrMore()

 // expecting 1 or more occurrences and repeating as many as possible
 start.oneOrMore().greedy()

 // expecting 0 or more occurrences
 start.oneOrMore().optional()

 // expecting 0 or more occurrences and repeating as many as possible
 start.oneOrMore().optional().greedy()

 // expecting 2 or more occurrences
 start.timesOrMore(2)

 // expecting 2 or more occurrences and repeating as many as possible
 start.timesOrMore(2).greedy()

 // expecting 0, 2 or more occurrences
 start.timesOrMore(2).optional()

 // expecting 0, 2 or more occurrences and repeating as many as possible
 start.timesOrMore(2).optional().greedy()
 {% endhighlight %}
 </div>
 </div>

#### Conditions

For every pattern you can specify a condition that an incoming event has to meet in order to be "accepted" into the pattern e.g. its value should be larger than 5,
or larger than the average value of the previously accepted events.
You can specify conditions on the event properties via the `pattern.where()`, `pattern.or()` or `pattern.until()` methods.
These can be either `IterativeCondition`s or `SimpleCondition`s.

**Iterative Conditions:** This is the most general type of condition. This is how you can specify a condition that
accepts subsequent events based on properties of the previously accepted events or a statistic over a subset of them.

Below is the code for an iterative condition that accepts the next event for a pattern named "middle" if its name starts
with "foo", and if the sum of the prices of the previously accepted events for that pattern plus the price of the current event do not exceed the value of 5.0. Iterative conditions can be powerful, especially in combination with looping patterns, e.g. `oneOrMore()`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
middle.oneOrMore()
    .subtype(SubEvent.class)
    .where(new IterativeCondition<SubEvent>() {
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
middle.oneOrMore()
    .subtype(classOf[SubEvent])
    .where(
        (value, ctx) => {
            lazy val sum = ctx.getEventsForPattern("middle").map(_.getPrice).sum
            value.getName.startsWith("foo") && sum + value.getPrice < 5.0
        }
    )
{% endhighlight %}
</div>
</div>

{% warn Attention %} The call to `ctx.getEventsForPattern(...)` finds all the
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

Finally, you can also restrict the type of the accepted event to a subtype of the initial event type (here `Event`)
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

**Combining Conditions:** As shown above, you can combine the `subtype` condition with additional conditions. This holds for every condition. You can arbitrarily combine conditions by sequentially calling `where()`. The final result will be the logical **AND** of the results of the individual conditions. To combine conditions using **OR**, you can use the `or()` method, as shown below.

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
                     <p>Specifies a stop condition for a looping pattern. Meaning if event matching the given condition occurs, no more
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
          <td><strong>greedy()</strong></td>
          <td>
              <p>Specifies that this pattern is greedy, i.e. it will repeat as many as possible. This is only applicable
              to quantifiers and it does not support group pattern currently.</p>
{% highlight java %}
pattern.oneOrMore().greedy();
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
pattern.times(2, 4)
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
          <td><strong>greedy()</strong></td>
          <td>
             <p>Specifies that this pattern is greedy, i.e. it will repeat as many as possible. This is only applicable
             to quantifiers and it does not support group pattern currently.</p>
{% highlight scala %}
pattern.oneOrMore().greedy()
{% endhighlight %}
          </td>
       </tr>
  </tbody>
</table>
</div>
</div>

### Combining Patterns

Now that you've seen what an individual pattern can look like, it is time to see how to combine them
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
them. FlinkCEP supports the following forms of contiguity between events:

 1. **Strict Contiguity**: Expects all matching events to appear strictly one after the other, without any non-matching events in-between.

 2. **Relaxed Contiguity**: Ignores non-matching events appearing in-between the matching ones.

 3. **Non-Deterministic Relaxed Contiguity**: Further relaxes contiguity, allowing additional matches
 that ignore some matching events. 
 
To apply them between consecutive patterns, you can use:

1. `next()`, for *strict*,
2. `followedBy()`, for *relaxed*, and
3. `followedByAny()`, for *non-deterministic relaxed* contiguity.

or

1. `notNext()`, if you do not want an event type to directly follow another
2. `notFollowedBy()`, if you do not want an event type to be anywhere between two other event types.

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

Relaxed contiguity means that only the first succeeding matching event will be matched, while
with non-deterministic relaxed contiguity, multiple matches will be emitted for the same beginning. As an example,
a pattern `"a b"`, given the event sequence `"a", "c", "b1", "b2"`, will give the following results:

1. Strict Contiguity between `"a"` and `"b"`: `{}` (no match), the `"c"` after `"a"` causes `"a"` to be discarded.

2. Relaxed Contiguity between `"a"` and `"b"`: `{a b1}`, as relaxed continuity is viewed as "skip non-matching events
till the next matching one".

3. Non-Deterministic Relaxed Contiguity between `"a"` and `"b"`: `{a b1}`, `{a b2}`, as this is the most general form.

It's also possible to define a temporal constraint for the pattern to be valid.
For example, you can define that a pattern should occur within 10 seconds via the `pattern.within()` method.
Temporal patterns are supported for both [processing and event time]({{site.baseurl}}/dev/event_time.html).

{% warn Attention %} A pattern sequence can only have one temporal constraint. If multiple such constraints are defined on different individual patterns, then the smallest is applied.

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

#### Contiguity within looping patterns

You can apply the same contiguity condition as discussed in the previous [section](#combining-patterns) within a looping pattern.
The contiguity will be applied between elements accepted into such a pattern.
To illustrate the above with an example, a pattern sequence `"a b+ c"` (`"a"` followed by any(non-deterministic relaxed) sequence of one or more `"b"`'s followed by a `"c"`) with
input `"a", "b1", "d1", "b2", "d2", "b3" "c"` will have the following results:

 1. **Strict Contiguity**: `{a b3 c}` -- the `"d1"` after `"b1"` causes `"b1"` to be discarded, the same happens for `"b2"` because of `"d2"`.

 2. **Relaxed Contiguity**: `{a b1 c}`, `{a b1 b2 c}`, `{a b1 b2 b3 c}`, `{a b2 c}`, `{a b2 b3 c}`, `{a b3 c}` - `"d"`'s are ignored.

 3. **Non-Deterministic Relaxed Contiguity**: `{a b1 c}`, `{a b1 b2 c}`, `{a b1 b3 c}`, `{a b1 b2 b3 c}`, `{a b2 c}`, `{a b2 b3 c}`, `{a b3 c}` -
    notice the `{a b1 b3 c}`, which is the result of relaxing contiguity between `"b"`'s.

For looping patterns (e.g. `oneOrMore()` and `times()`) the default is *relaxed contiguity*. If you want
strict contiguity, you have to explicitly specify it by using the `consecutive()` call, and if you want
*non-deterministic relaxed contiguity* you can use the `allowCombinations()` call.

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
  .followedBy("end1").where(_.getName().equals("b"))
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
  .followedBy("end1").where(_.getName().equals("b"))
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

### Groups of patterns

It's also possible to define a pattern sequence as the condition for `begin`, `followedBy`, `followedByAny` and
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
    Pattern.begin[Event]("start").where(...).followedBy("start_middle").where(...)
)

// strict contiguity
val strict: Pattern[Event, _] = start.next(
    Pattern.begin[Event]("next_start").where(...).followedBy("next_middle").where(...)
).times(3)

// relaxed contiguity
val relaxed: Pattern[Event, _] = start.followedBy(
    Pattern.begin[Event]("followedby_start").where(...).followedBy("followedby_middle").where(...)
).oneOrMore()

// non-deterministic relaxed contiguity
val nonDetermin: Pattern[Event, _] = start.followedByAny(
    Pattern.begin[Event]("followedbyany_start").where(...).followedBy("followedbyany_middle").where(...)
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
Pattern<Event, ?> notFollowedBy = start.notFollowedBy("not");
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
            <td><strong>begin(#name)</strong></td>
            <td>
            <p>Defines a starting pattern:</p>
{% highlight scala %}
val start = Pattern.begin[Event]("start")
{% endhighlight %}
            </td>
        </tr>
       <tr>
            <td><strong>begin(#pattern_sequence)</strong></td>
            <td>
            <p>Defines a starting pattern:</p>
{% highlight scala %}
val start = Pattern.begin(
    Pattern.begin[Event]("start").where(...).followedBy("middle").where(...)
)
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
val notFollowedBy = start.notFollowedBy("not")
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

### After Match Skip Strategy

For a given pattern, the same event may be assigned to multiple successful matches. To control to how many matches an event will be assigned, you need to specify the skip strategy called `AfterMatchSkipStrategy`. There are four types of skip strategies, listed as follows:

* <strong>*NO_SKIP*</strong>: Every possible match will be emitted.
* <strong>*SKIP_PAST_LAST_EVENT*</strong>: Discards every partial match that started after the match started but before it ended.
* <strong>*SKIP_TO_FIRST*</strong>: Discards every partial match that started after the match started but before the first event of *PatternName* occurred.
* <strong>*SKIP_TO_LAST*</strong>: Discards every partial match that started after the match started but before the last event of *PatternName* occurred.

Notice that when using *SKIP_TO_FIRST* and *SKIP_TO_LAST* skip strategy, a valid *PatternName* should also be specified.

For example, for a given pattern `b+ c` and a data stream `b1 b2 b3 c`, the differences between these four skip strategies are as follows:

<table class="table table-bordered">
    <tr>
        <th class="text-left" style="width: 25%">Skip Strategy</th>
        <th class="text-center" style="width: 25%">Result</th>
        <th class="text-center"> Description</th>
    </tr>
    <tr>
        <td><strong>NO_SKIP</strong></td>
        <td>
            <code>b1 b2 b3 c</code><br>
            <code>b2 b3 c</code><br>
            <code>b3 c</code><br>
        </td>
        <td>After found matching <code>b1 b2 b3 c</code>, the match process will not discard any result.</td>
    </tr>
    <tr>
        <td><strong>SKIP_TO_NEXT</strong></td>
        <td>
            <code>b1 b2 b3 c</code><br>
            <code>b2 b3 c</code><br>
            <code>b3 c</code><br>
        </td>
        <td>After found matching <code>b1 b2 b3 c</code>, the match process will not discard any result, because no other match could start at b1.</td>
    </tr>
    <tr>
        <td><strong>SKIP_PAST_LAST_EVENT</strong></td>
        <td>
            <code>b1 b2 b3 c</code><br>
        </td>
        <td>After found matching <code>b1 b2 b3 c</code>, the match process will discard all started partial matches.</td>
    </tr>
    <tr>
        <td><strong>SKIP_TO_FIRST</strong>[<code>b</code>]</td>
        <td>
            <code>b1 b2 b3 c</code><br>
            <code>b2 b3 c</code><br>
            <code>b3 c</code><br>
        </td>
        <td>After found matching <code>b1 b2 b3 c</code>, the match process will try to discard all partial matches started before <code>b1</code>, but there are no such matches. Therefore nothing will be discarded.</td>
    </tr>
    <tr>
        <td><strong>SKIP_TO_LAST</strong>[<code>b</code>]</td>
        <td>
            <code>b1 b2 b3 c</code><br>
            <code>b3 c</code><br>
        </td>
        <td>After found matching <code>b1 b2 b3 c</code>, the match process will try to discard all partial matches started before <code>b3</code>. There is one such match <code>b2 b3 c</code></td>
    </tr>
</table>

Have a look also at another example to better see the difference between NO_SKIP and SKIP_TO_FIRST:
Pattern: `(a | c) (b | c) c+.greedy d` and sequence: `a b c1 c2 c3 d` Then the results will be:


<table class="table table-bordered">
    <tr>
        <th class="text-left" style="width: 25%">Skip Strategy</th>
        <th class="text-center" style="width: 25%">Result</th>
        <th class="text-center"> Description</th>
    </tr>
    <tr>
        <td><strong>NO_SKIP</strong></td>
        <td>
            <code>a b c1 c2 c3 d</code><br>
            <code>b c1 c2 c3 d</code><br>
            <code>c1 c2 c3 d</code><br>
            <code>c2 c3 d</code><br>
        </td>
        <td>After found matching <code>a b c1 c2 c3 d</code>, the match process will not discard any result.</td>
    </tr>
    <tr>
        <td><strong>SKIP_TO_FIRST</strong>[<code>b*</code>]</td>
        <td>
            <code>a b c1 c2 c3 d</code><br>
            <code>c1 c2 c3 d</code><br>
        </td>
        <td>After found matching <code>a b c1 c2 c3 d</code>, the match process will discard all partial matches started before <code>c1</code>. There is one such match <code>b c1 c2 c3 d</code>.</td>
    </tr>
</table>

To better understand the difference between NO_SKIP and SKIP_TO_NEXT take a look at following example:
Pattern: `a b+` and sequence: `a b1 b2 b3` Then the results will be:


<table class="table table-bordered">
    <tr>
        <th class="text-left" style="width: 25%">Skip Strategy</th>
        <th class="text-center" style="width: 25%">Result</th>
        <th class="text-center"> Description</th>
    </tr>
    <tr>
        <td><strong>NO_SKIP</strong></td>
        <td>
            <code>a b1</code><br>
            <code>a b1 b2</code><br>
            <code>a b1 b2 b3</code><br>
        </td>
        <td>After found matching <code>a b1</code>, the match process will not discard any result.</td>
    </tr>
    <tr>
        <td><strong>SKIP_TO_NEXT</strong>[<code>b*</code>]</td>
        <td>
            <code>a b1</code><br>
        </td>
        <td>After found matching <code>a b1</code>, the match process will discard all partial matches started at <code>a</code>. This means neither <code>a b1 b2</code> nor <code>a b1 b2 b3</code> could be generated.</td>
    </tr>
</table>

To specify which skip strategy to use, just create an `AfterMatchSkipStrategy` by calling:
<table class="table table-bordered">
    <tr>
        <th class="text-left" width="25%">Function</th>
        <th class="text-center">Description</th>
    </tr>
    <tr>
        <td><code>AfterMatchSkipStrategy.noSkip()</code></td>
        <td>Create a <strong>NO_SKIP</strong> skip strategy </td>
    </tr>
    <tr>
        <td><code>AfterMatchSkipStrategy.skipToNext()</code></td>
        <td>Create a <strong>SKIP_TO_NEXT</strong> skip strategy </td>
    </tr>
    <tr>
        <td><code>AfterMatchSkipStrategy.skipPastLastEvent()</code></td>
        <td>Create a <strong>SKIP_PAST_LAST_EVENT</strong> skip strategy </td>
    </tr>
    <tr>
        <td><code>AfterMatchSkipStrategy.skipToFirst(patternName)</code></td>
        <td>Create a <strong>SKIP_TO_FIRST</strong> skip strategy with the referenced pattern name <i>patternName</i></td>
    </tr>
    <tr>
        <td><code>AfterMatchSkipStrategy.skipToLast(patternName)</code></td>
        <td>Create a <strong>SKIP_TO_LAST</strong> skip strategy with the referenced pattern name <i>patternName</i></td>
    </tr>
</table>

Then apply the skip strategy to a pattern by calling:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
AfterMatchSkipStrategy skipStrategy = ...
Pattern.begin("patternName", skipStrategy);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val skipStrategy = ...
Pattern.begin("patternName", skipStrategy)
{% endhighlight %}
</div>
</div>

{% warn Attention %} For SKIP_TO_FIRST/LAST there are two options how to handle cases when there are no elements mapped to
the specified variable. By default a NO_SKIP strategy will be used in this case. The other option is to throw exception in such situation.
One can enable this option by:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
AfterMatchSkipStrategy.skipToFirst(patternName).throwExceptionOnMiss()
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
AfterMatchSkipStrategy.skipToFirst(patternName).throwExceptionOnMiss()
{% endhighlight %}
</div>
</div>

## Detecting Patterns

After specifying the pattern sequence you are looking for, it is time to apply it to your input stream to detect
potential matches. To run a stream of events against your pattern sequence, you have to create a `PatternStream`.
Given an input stream `input`, a pattern `pattern` and an optional comparator `comparator` used to sort events with the same timestamp in case of EventTime or that arrived at the same moment, you create the `PatternStream` by calling:

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

{% warn Attention %} Applying your pattern on a non-keyed stream will result in a job with parallelism equal to 1.

### Selecting from Patterns

Once you have obtained a `PatternStream` you can select from detected event sequences via the `select` or `flatSelect` methods.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
The `select()` method requires a `PatternSelectFunction` implementation.
A `PatternSelectFunction` has a `select` method which is called for each matching event sequence.
It receives a match in the form of `Map<String, List<IN>>` where the key is the name of each pattern in your pattern
sequence and the value is a list of all accepted events for that pattern (`IN` is the type of your input elements).
The events for a given pattern are ordered by timestamp. The reason for returning a list of accepted events for each
pattern is that when using looping patterns (e.g. `oneToMany()` and `times()`), more than one event may be accepted for a given pattern. The selection function returns exactly one result.

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
arbitrary number of results. To do this, the `select` method has an additional `Collector` parameter which is
used to forward your output elements downstream.

{% highlight java %}
class MyPatternFlatSelectFunction<IN, OUT> implements PatternFlatSelectFunction<IN, OUT> {
    @Override
    public void flatSelect(Map<String, List<IN>> pattern, Collector<OUT> collector) {
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

The events for a given pattern are ordered by timestamp. The reason for returning an iterable of accepted events for each pattern is that when using looping patterns (e.g. `oneToMany()` and `times()`), more than one event may be accepted for a given pattern. The selection function returns exactly one result per call.

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
are discarded because they exceed the window length. To react to these timed out partial matches the `select`
and `flatSelect` API calls allow you to specify a timeout handler. This timeout handler is called for each timed out
partial event sequence. The timeout handler receives all the events that have been matched so far by the pattern, and
the timestamp when the timeout was detected.

To treat partial patterns, the `select` and `flatSelect` API calls offer an overloaded version which takes as
parameters

 * `PatternTimeoutFunction`/`PatternFlatTimeoutFunction`
 * [OutputTag]({{ site.baseurl }}/dev/stream/side_output.html) for the side output in which the timed out matches will be returned
 * and the known `PatternSelectFunction`/`PatternFlatSelectFunction`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
PatternStream<Event> patternStream = CEP.pattern(input, pattern);

OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

SingleOutputStreamOperator<ComplexEvent> result = patternStream.select(
    outputTag,
    new PatternTimeoutFunction<Event, TimeoutEvent>() {...},
    new PatternSelectFunction<Event, ComplexEvent>() {...}
);

DataStream<TimeoutEvent> timeoutResult = result.getSideOutput(outputTag);

SingleOutputStreamOperator<ComplexEvent> flatResult = patternStream.flatSelect(
    outputTag,
    new PatternFlatTimeoutFunction<Event, TimeoutEvent>() {...},
    new PatternFlatSelectFunction<Event, ComplexEvent>() {...}
);

DataStream<TimeoutEvent> timeoutFlatResult = flatResult.getSideOutput(outputTag);
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">

{% highlight scala %}
val patternStream: PatternStream[Event] = CEP.pattern(input, pattern)

val outputTag = OutputTag[String]("side-output")

val result: SingleOutputStreamOperator[ComplexEvent] = patternStream.select(outputTag){
    (pattern: Map[String, Iterable[Event]], timestamp: Long) => TimeoutEvent()
} {
    pattern: Map[String, Iterable[Event]] => ComplexEvent()
}

val timeoutResult: DataStream<TimeoutEvent> = result.getSideOutput(outputTag)
{% endhighlight %}

The `flatSelect` API call offers the same overloaded version which takes as the first parameter a timeout function and as second parameter a selection function.
In contrast to the `select` functions, the `flatSelect` functions are called with a `Collector`. You can use the collector to emit an arbitrary number of events.

{% highlight scala %}
val patternStream: PatternStream[Event] = CEP.pattern(input, pattern)

val outputTag = OutputTag[String]("side-output")

val result: SingleOutputStreamOperator[ComplexEvent] = patternStream.flatSelect(outputTag){
    (pattern: Map[String, Iterable[Event]], timestamp: Long, out: Collector[TimeoutEvent]) =>
        out.collect(TimeoutEvent())
} {
    (pattern: mutable.Map[String, Iterable[Event]], out: Collector[ComplexEvent]) =>
        out.collect(ComplexEvent())
}

val timeoutResult: DataStream<TimeoutEvent> = result.getSideOutput(outputTag)
{% endhighlight %}

</div>
</div>

## Handling Lateness in Event Time

In `CEP` the order in which elements are processed matters. To guarantee that elements are processed in the correct order when working in event time, an incoming element is initially put in a buffer where elements are *sorted in ascending order based on their timestamp*, and when a watermark arrives, all the elements in this buffer with timestamps smaller than that of the watermark are processed. This implies that elements between watermarks are processed in event-time order.

{% warn Attention %} The library assumes correctness of the watermark when working in event time.

To guarantee that elements across watermarks are processed in event-time order, Flink's CEP library assumes
*correctness of the watermark*, and considers as *late* elements whose timestamp is smaller than that of the last
seen watermark. Late elements are not further processed. Also, you can specify a sideOutput tag to collect the late elements come after the last seen watermark, you can use it like this.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
PatternStream<Event> patternStream = CEP.pattern(input, pattern);

OutputTag<String> lateDataOutputTag = new OutputTag<String>("late-data"){};

SingleOutputStreamOperator<ComplexEvent> result = patternStream
    .sideOutputLateData(lateDataOutputTag)
    .select(
        new PatternSelectFunction<Event, ComplexEvent>() {...}
    );

DataStream<String> lateData = result.getSideOutput(lateDataOutputTag);


{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">

{% highlight scala %}

val patternStream: PatternStream[Event] = CEP.pattern(input, pattern)

val lateDataOutputTag = OutputTag[String]("late-data")

val result: SingleOutputStreamOperator[ComplexEvent] = patternStream
      .sideOutputLateData(lateDataOutputTag)
      .select{
          pattern: Map[String, Iterable[ComplexEvent]] => ComplexEvent()
      }

val lateData: DataStream<String> = result.getSideOutput(lateDataOutputTag)

{% endhighlight %}

</div>
</div>

## Examples

The following example detects the pattern `start, middle(name = "error") -> end(name = "critical")` on a keyed data
stream of `Events`. The events are keyed by their `id`s and a valid pattern has to occur within 10 seconds.
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

val pattern = Pattern.begin[Event]("start")
  .next("middle").where(_.getName == "error")
  .followedBy("end").where(_.getName == "critical")
  .within(Time.seconds(10))

val patternStream = CEP.pattern(partitionedInput, pattern)

val alerts = patternStream.select(createAlert(_)))
{% endhighlight %}
</div>
</div>

## Migrating from an older Flink version(pre 1.3)

### Migrating to 1.4+

In Flink-1.4 the backward compatibility of CEP library with <= Flink 1.2 was dropped. Unfortunately 
it is not possible to restore a CEP job that was once run with 1.2.x

### Migrating to 1.3.x

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

{% top %}
