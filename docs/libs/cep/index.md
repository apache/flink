---
title: "FlinkCEP - Complex event processing for Flink"
# Top navigation
top-nav-group: libs
top-nav-pos: 2
top-nav-title: CEP
# Sub navigation
sub-nav-group: batch
sub-nav-id: flinkcep
sub-nav-pos: 2
sub-nav-parent: libs
sub-nav-title: CEP
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

## Getting Started

If you want to jump right in, you have to [set up a Flink program]({{ site.baseurl }}/apis/batch/index.html#linking-with-flink).
Next, you have to add the FlinkCEP dependency to the `pom.xml` of your project.

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-cep{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that FlinkCEP is currently not part of the binary distribution.
See linking with it for cluster execution [here]({{site.baseurl}}/apis/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

Now you can start writing your first CEP program using the pattern API.

{% highlight java %}
DataStream<Event> input = ...

Pattern<Event, ?> pattern = Pattern.begin("start").where(evt -> evt.getId() == 42)
    .next("middle").subtype(SubEvent.class).where(subEvt -> subEvt.getVolume() >= 10.0)
    .followedBy("end").where(evt -> evt.getName().equals("end"));
    
PatternStream<Event> patternStream = CEP.from(input, pattern);

DataStream<Alert> result = patternStream.select(pattern -> {
    return createAlertFrom(pattern);
});
{% endhighlight %}

Note that we have used Java 8 lambdas here to make the example more succinct.

## The Pattern API

The pattern API allows you to quickly define complex event patterns.

Each pattern consists of multiple stages or what we call states.
In order to go from one state to the next, the user can specify conditions.
These conditions can be the contiguity of events or a filter condition on an event.

Each pattern has to start with an initial state:

{% highlight java %}
Pattern<Event, ?> start = Pattern.<Event>begin("start");
{% endhighlight %}

Each state must have an unique name to identify the matched events later on.
Additionally, we can specify a filter condition for the event to be accepted as the start event via the `where` method.
  
{% highlight java %}
start.where(new FilterFunction<Event>() {
    @Override
    public boolean filter(Event value) {
        return ... // some condition
    }
});
{% endhighlight %}

We can also restrict the type of the accepted event to some subtype of the initial event type (here `Event`) via the `subtype` method.

{% highlight java %}
start.subtype(SubEvent.class).where(new FilterFunction<SubEvent>() {
    @Override
    public boolean filter(SubEvent value) {
        return ... // some condition
    }
});
{% endhighlight %}

As it can be seen here, the subtype condition can also be combined with an additional filter condition on the subtype.
In fact you can always provide multiple conditions by calling `where` and `subtype` multiple times.
These conditions will then be combined using the logical AND operator.

Next, we can append further states to detect complex patterns.
We can control the contiguity of two succeeding events to be accepted by the pattern.

Strict contiguity means that two matching events have to succeed directly.
This means that no other events can occur in between.
A strict contiguity pattern state can be created via the `next` method.

{% highlight java %}
Pattern<Event, ?> strictNext = start.next("middle");
{% endhighlight %}

Non-strict contiguity means that other events are allowed to occur in-between two matching events.
A non-strict contiguity pattern state can be created via the `followedBy` method.

It is also possible to define a temporal constraint for the pattern to be valid.
For example, one can define that a pattern should occur within 10 seconds via the `within` method.

{% highlight java %}
next.within(Time.seconds(10));
{% endhighlight %}

{% highlight java %}
Pattern<Event, ?> nonStrictNext = start.followedBy("middle");
{% endhighlight %}

<br />

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
Pattern<Event, ?> next = start.followedBy("next");
{% endhighlight %}
            </td>
        </tr>
        <tr>
            <td><strong>Where</strong></td>
            <td>
                <p>Defines a filter condition for the current pattern state. Only if an event passes the filter, it can match the state:</p>
{% highlight java %}
patternState.where(new FilterFunction<Event>() {
    @Override
    public boolean filter(Event value) throws Exception {
        return ... // some condition
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
  </tbody>
</table>

### Detecting Patterns

In order to run a stream of events against your pattern, you have to create a `PatternStream`.
Given an input stream `input` and a pattern `pattern`, you create the `PatternStream` by calling

{% highlight java %}
DataStream<Event> input = ...
Pattern<Event, ?> pattern = ...

PatternStream<Event> patternStream = CEP.from(input, pattern);
{% endhighlight %}

### Selecting from Patterns

Once you have obtained a `PatternStream` you can select from detected event sequences via the `select` or `flatSelect` methods.
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

## Examples

The following example detects the pattern `start, middle(name = "error") -> end(name = "critical")` on a keyed data stream of `Events`.
The events are keyed by their ids and a valid pattern has to occur within 10 seconds.
The whole processing is done with event time.

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
	.next("middle").where(new FilterFunction<Event>() {
		@Override
		public boolean filter(Event value) throws Exception {
			return value.getName().equals("name");
		}
	}).followedBy("end").where(new FilterFunction<Event>() {
		@Override
		public boolean filter(Event value) throws Exception {
			return value.getName().equals("critical");
		}
	}).within(Time.seconds(10));

PatternStream<Event> patternStream = CEP.from(partitionedInput, pattern);

DataStream<Alert> alerts = patternStream.select(new PatternSelectFunction<Event, Alert>() {
	@Override
	public Alert select(Map<String, Event> pattern) throws Exception {
		return new Alert(pattern.get("start"), pattern.get("end"))
	}
});
{% endhighlight %}
