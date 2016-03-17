---
title: "Windows"

sub-nav-id: windows
sub-nav-group: streaming
sub-nav-pos: 3
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

* This will be replaced by the TOC
{:toc}

## Windows on Keyed Data Streams

Flink offers a variety of methods for defining windows on a `KeyedStream`. All of these group elements *per key*,
i.e., each window will contain elements with the same key value.

### Basic Window Constructs

Flink offers a general window mechanism that provides flexibility, as well as a number of pre-defined windows
for common use cases. See first if your use case can be served by the pre-defined windows below before moving
to defining your own windows.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
      <tr>
        <td><strong>Tumbling time window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 5 seconds, that "tumbles". This means that elements are
          grouped according to their timestamp in groups of 5 second duration, and every element belongs to exactly one window.
	  The notion of time is specified by the selected TimeCharacteristic (see <a href="{{ site.baseurl }}/apis/streaming/event_time.html">time</a>).
    {% highlight java %}
keyedStream.timeWindow(Time.seconds(5));
    {% endhighlight %}
          </p>
        </td>
      </tr>
      <tr>
          <td><strong>Sliding time window</strong><br>KeyedStream &rarr; WindowedStream</td>
          <td>
            <p>
             Defines a window of 5 seconds, that "slides" by 1 seconds. This means that elements are
             grouped according to their timestamp in groups of 5 second duration, and elements can belong to more than
             one window (since windows overlap by at most 4 seconds)
             The notion of time is specified by the selected TimeCharacteristic (see <a href="{{ site.baseurl }}/apis/streaming/event_time.html">time</a>).
      {% highlight java %}
keyedStream.timeWindow(Time.seconds(5), Time.seconds(1));
      {% endhighlight %}
            </p>
          </td>
        </tr>
      <tr>
        <td><strong>Tumbling count window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 1000 elements, that "tumbles". This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element belongs to exactly one window.
    {% highlight java %}
keyedStream.countWindow(1000);
    {% endhighlight %}
        </p>
        </td>
      </tr>
      <tr>
      <td><strong>Sliding count window</strong><br>KeyedStream &rarr; WindowedStream</td>
      <td>
        <p>
          Defines a window of 1000 elements, that "slides" every 100 elements. This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element can belong to more than one window (as windows overlap by at most 900 elements).
  {% highlight java %}
keyedStream.countWindow(1000, 100)
  {% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>

<div data-lang="scala" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
      <tr>
        <td><strong>Tumbling time window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 5 seconds, that "tumbles". This means that elements are
          grouped according to their timestamp in groups of 5 second duration, and every element belongs to exactly one window.
          The notion of time is specified by the selected TimeCharacteristic (see <a href="{{ site.baseurl }}/apis/streaming/event_time.html">time</a>).
    {% highlight scala %}
keyedStream.timeWindow(Time.seconds(5))
    {% endhighlight %}
          </p>
        </td>
      </tr>
      <tr>
          <td><strong>Sliding time window</strong><br>KeyedStream &rarr; WindowedStream</td>
          <td>
            <p>
             Defines a window of 5 seconds, that "slides" by 1 seconds. This means that elements are
             grouped according to their timestamp in groups of 5 second duration, and elements can belong to more than
             one window (since windows overlap by at most 4 seconds)
             The notion of time is specified by the selected TimeCharacteristic (see <a href="{{ site.baseurl }}/apis/streaming/event_time.html">time</a>).
      {% highlight scala %}
keyedStream.timeWindow(Time.seconds(5), Time.seconds(1))
      {% endhighlight %}
            </p>
          </td>
        </tr>
      <tr>
        <td><strong>Tumbling count window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 1000 elements, that "tumbles". This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element belongs to exactly one window.
    {% highlight scala %}
keyedStream.countWindow(1000)
    {% endhighlight %}
        </p>
        </td>
      </tr>
      <tr>
      <td><strong>Sliding count window</strong><br>KeyedStream &rarr; WindowedStream</td>
      <td>
        <p>
          Defines a window of 1000 elements, that "slides" every 100 elements. This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element can belong to more than one window (as windows overlap by at most 900 elements).
  {% highlight scala %}
keyedStream.countWindow(1000, 100)
  {% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>

### Advanced Window Constructs

The general mechanism can define more powerful windows at the cost of more verbose syntax. For example,
below is a window definition where windows hold elements of the last 5 seconds and slides every 1 second,
but the execution of the window function is triggered when 100 elements have been added to the
window, and every time execution is triggered, 10 elements are retained in the window:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
keyedStream
    .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1))
    .trigger(CountTrigger.of(100))
    .evictor(CountEvictor.of(10));
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
keyedStream
    .window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1))
    .trigger(CountTrigger.of(100))
    .evictor(CountEvictor.of(10))
{% endhighlight %}
</div>
</div>

The general recipe for building a custom window is to specify (1) a `WindowAssigner`, (2) a `Trigger` (optionally),
and (3) an `Evictor` (optionally).

The `WindowAssigner` defines how incoming elements are assigned to windows. A window is a logical group of elements
that has a begin-value, and an end-value corresponding to a begin-time and end-time. Elements with timestamp (according
to some notion of time described above within these values are part of the window).

For example, the `SlidingEventTimeWindows`
assigner in the code above defines a window of size 5 seconds, and a slide of 1 second. Assume that
time starts from 0 and is measured in milliseconds. Then, we have 6 windows
that overlap: [0,5000], [1000,6000], [2000,7000], [3000, 8000], [4000, 9000], and [5000, 10000]. Each incoming
element is assigned to the windows according to its timestamp. For example, an element with timestamp 2000 will be
assigned to the first three windows. Flink comes bundled with window assigners that cover the most common use cases. You can write your
own window types by extending the `WindowAssigner` class.

<div class="codetabs" markdown="1">

<div data-lang="java" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
      <tr>
        <td><strong>Global window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
	    All incoming elements of a given key are assigned to the same window.
	    The window does not contain a default trigger, hence it will never be triggered
	    if a trigger is not explicitly specified.
          </p>
    {% highlight java %}
stream.window(GlobalWindows.create());
    {% endhighlight %}
        </td>
      </tr>
      <tr>
        <td><strong>Tumbling time windows</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
            Incoming elements are assigned to a window of a certain size (1 second below) based on
            their timestamp. Windows do not overlap, i.e., each element is assigned to exactly one window.
            This assigner comes with a default trigger that fires for a window when a
            watermark with value higher than its end-value is received.
          </p>
      {% highlight java %}
stream.window(TumblingEventTimeWindows.of(Time.seconds(1)));
      {% endhighlight %}
        </td>
      </tr>
      <tr>
        <td><strong>Sliding time windows</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
            Incoming elements are assigned to a window of a certain size (5 seconds below) based on
            their timestamp. Windows "slide" by the provided value (1 second in the example), and hence
            overlap. This assigner comes with a default trigger that fires for a window when a
	          watermark with value higher than its end-value is received.
          </p>
    {% highlight java %}
stream.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)));
    {% endhighlight %}
        </td>
      </tr>
      <tr>
          <td><strong>Tumbling processing time windows</strong><br>KeyedStream &rarr; WindowedStream</td>
          <td>
            <p>
              Incoming elements are assigned to a window of a certain size (1 second below) based on
              the current processing time. Windows do not overlap, i.e., each element is assigned to exactly one window.
              This assigner comes with a default trigger that fires for a window a window when the current
              processing time exceeds its end-value.
            </p>
      {% highlight java %}
stream.window(TumblingProcessingTimeWindows.of(Time.seconds(1)));
      {% endhighlight %}
          </td>
        </tr>
      <tr>
        <td><strong>Sliding processing time windows</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
            Incoming elements are assigned to a window of a certain size (5 seconds below) based on
            their timestamp. Windows "slide" by the provided value (1 second in the example), and hence
            overlap. This assigner comes with a default trigger that fires for a window a window when the current
            processing time exceeds its end-value.
          </p>
    {% highlight java %}
stream.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)));
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
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
      <tr>
        <td><strong>Global window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
            All incoming elements of a given key are assigned to the same window.
	    The window does not contain a default trigger, hence it will never be triggered
	    if a trigger is not explicitly specified.
          </p>
    {% highlight scala %}
stream.window(GlobalWindows.create)
    {% endhighlight %}
        </td>
      </tr>
      <tr>
          <td><strong>Tumbling time windows</strong><br>KeyedStream &rarr; WindowedStream</td>
          <td>
            <p>
             Incoming elements are assigned to a window of a certain size (1 second below) based on
            their timestamp. Windows do not overlap, i.e., each element is assigned to exactly one window.
            This assigner comes with a default trigger that fires for a window when a
            watermark with value higher than its end-value is received.
            </p>
      {% highlight scala %}
stream.window(TumblingEventTimeWindows.of(Time.seconds(1)))
      {% endhighlight %}
          </td>
        </tr>
      <tr>
        <td><strong>Sliding time windows</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
            Incoming elements are assigned to a window of a certain size (5 seconds below) based on
            their timestamp. Windows "slide" by the provided value (1 second in the example), and hence
            overlap. This assigner comes with a default trigger that fires for a window when a
            watermark with value higher than its end-value is received.
          </p>
    {% highlight scala %}
stream.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
    {% endhighlight %}
        </td>
      </tr>
      <tr>
          <td><strong>Tumbling processing time windows</strong><br>KeyedStream &rarr; WindowedStream</td>
          <td>
            <p>
              Incoming elements are assigned to a window of a certain size (1 second below) based on
              the current processing time. Windows do not overlap, i.e., each element is assigned to exactly one window.
              This assigner comes with a default trigger that fires for a window a window when the current
              processing time exceeds its end-value.

            </p>
      {% highlight scala %}
stream.window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
      {% endhighlight %}
          </td>
        </tr>
      <tr>
        <td><strong>Sliding processing time windows</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
            Incoming elements are assigned to a window of a certain size (5 seconds below) based on
            their timestamp. Windows "slide" by the provided value (1 second in the example), and hence
            overlap. This assigner comes with a default trigger that fires for a window a window when the current
            processing time exceeds its end-value.
          </p>
    {% highlight scala %}
stream.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
    {% endhighlight %}
        </td>
      </tr>
  </tbody>
</table>
</div>

</div>

The `Trigger` specifies when the function that comes after the window clause (e.g., `sum`, `count`) is evaluated ("fires")
for each window. If a trigger is not specified, a default trigger for each window type is used (that is part of the
definition of the `WindowAssigner`). Flink comes bundled with a set of triggers if the ones that windows use by
default do not fit the application. You can write your own trigger by implementing the `Trigger` interface. Note that
specifying a trigger will override the default trigger of the window assigner.

<div class="codetabs" markdown="1">

<div data-lang="java" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  <tr>
    <td><strong>Processing time trigger</strong></td>
    <td>
      <p>
        A window is fired when the current processing time exceeds its end-value.
        The elements on the triggered window are henceforth discarded.
      </p>
{% highlight java %}
windowedStream.trigger(ProcessingTimeTrigger.create());
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Watermark trigger</strong></td>
    <td>
      <p>
        A window is fired when a watermark with value that exceeds the window's end-value has been received.
        The elements on the triggered window are henceforth discarded.
      </p>
{% highlight java %}
windowedStream.trigger(EventTimeTrigger.create());
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Continuous processing time trigger</strong></td>
    <td>
      <p>
        A window is periodically considered for being fired (every 5 seconds in the example).
        The window is actually fired only when the current processing time exceeds its end-value.
        The elements on the triggered window are retained.
      </p>
{% highlight java %}
windowedStream.trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Continuous watermark time trigger</strong></td>
    <td>
      <p>
        A window is periodically considered for being fired (every 5 seconds in the example).
        A window is actually fired when a watermark with value that exceeds the window's end-value has been received.
        The elements on the triggered window are retained.
      </p>
{% highlight java %}
windowedStream.trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Count trigger</strong></td>
    <td>
      <p>
        A window is fired when it has more than a certain number of elements (1000 below).
        The elements of the triggered window are retained.
      </p>
{% highlight java %}
windowedStream.trigger(CountTrigger.of(1000));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Purging trigger</strong></td>
    <td>
      <p>
        Takes any trigger as an argument and forces the triggered window elements to be
        "purged" (discarded) after triggering.
      </p>
{% highlight java %}
windowedStream.trigger(PurgingTrigger.of(CountTrigger.of(1000)));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Delta trigger</strong></td>
    <td>
      <p>
        A window is periodically considered for being fired (every 5000 milliseconds in the example).
        A window is actually fired when the value of the last added element exceeds the value of
        the first element inserted in the window according to a `DeltaFunction`.
      </p>
{% highlight java %}
windowedStream.trigger(new DeltaTrigger.of(5000.0, new DeltaFunction<Double>() {
    @Override
    public double getDelta (Double old, Double new) {
        return (new - old > 0.01);
    }
}));
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
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  <tr>
    <td><strong>Processing time trigger</strong></td>
    <td>
      <p>
        A window is fired when the current processing time exceeds its end-value.
        The elements on the triggered window are henceforth discarded.
      </p>
{% highlight scala %}
windowedStream.trigger(ProcessingTimeTrigger.create);
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Watermark trigger</strong></td>
    <td>
      <p>
        A window is fired when a watermark with value that exceeds the window's end-value has been received.
        The elements on the triggered window are henceforth discarded.
      </p>
{% highlight scala %}
windowedStream.trigger(EventTimeTrigger.create);
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Continuous processing time trigger</strong></td>
    <td>
      <p>
        A window is periodically considered for being fired (every 5 seconds in the example).
        The window is actually fired only when the current processing time exceeds its end-value.
        The elements on the triggered window are retained.
      </p>
{% highlight scala %}
windowedStream.trigger(ContinuousProcessingTimeTrigger.of(Time.seconds(5)));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Continuous watermark time trigger</strong></td>
    <td>
      <p>
        A window is periodically considered for being fired (every 5 seconds in the example).
        A window is actually fired when a watermark with value that exceeds the window's end-value has been received.
        The elements on the triggered window are retained.
      </p>
{% highlight scala %}
windowedStream.trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Count trigger</strong></td>
    <td>
      <p>
        A window is fired when it has more than a certain number of elements (1000 below).
        The elements of the triggered window are retained.
      </p>
{% highlight scala %}
windowedStream.trigger(CountTrigger.of(1000));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Purging trigger</strong></td>
    <td>
      <p>
        Takes any trigger as an argument and forces the triggered window elements to be
        "purged" (discarded) after triggering.
      </p>
{% highlight scala %}
windowedStream.trigger(PurgingTrigger.of(CountTrigger.of(1000)));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Delta trigger</strong></td>
    <td>
      <p>
        A window is periodically considered for being fired (every 5000 milliseconds in the example).
        A window is actually fired when the value of the last added element exceeds the value of
        the first element inserted in the window according to a `DeltaFunction`.
      </p>
{% highlight scala %}
windowedStream.trigger(DeltaTrigger.of(5000.0, { (old,new) => new - old > 0.01 }))
{% endhighlight %}
    </td>
  </tr>
 </tbody>
</table>
</div>

</div>

After the trigger fires, and before the function (e.g., `sum`, `count`) is applied to the window contents, an
optional `Evictor` removes some elements from the beginning of the window before the remaining elements
are passed on to the function. Flink comes bundled with a set of evictors You can write your own evictor by
implementing the `Evictor` interface.

<div class="codetabs" markdown="1">

<div data-lang="java" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  <tr>
      <td><strong>Time evictor</strong></td>
      <td>
        <p>
         Evict all elements from the beginning of the window, so that elements from end-value - 1 second
         until end-value are retained (the resulting window size is 1 second).
        </p>
  {% highlight java %}
triggeredStream.evictor(TimeEvictor.of(Time.seconds(1)));
  {% endhighlight %}
      </td>
    </tr>
   <tr>
       <td><strong>Count evictor</strong></td>
       <td>
         <p>
          Retain 1000 elements from the end of the window backwards, evicting all others.
         </p>
   {% highlight java %}
triggeredStream.evictor(CountEvictor.of(1000));
   {% endhighlight %}
       </td>
     </tr>
    <tr>
        <td><strong>Delta evictor</strong></td>
        <td>
          <p>
            Starting from the beginning of the window, evict elements until an element with
            value lower than the value of the last element is found (by a threshold and a
            DeltaFunction).
          </p>
    {% highlight java %}
triggeredStream.evictor(DeltaEvictor.of(5000, new DeltaFunction<Double>() {
  public double (Double oldValue, Double newValue) {
      return newValue - oldValue;
  }
}));
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
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  <tr>
      <td><strong>Time evictor</strong></td>
      <td>
        <p>
         Evict all elements from the beginning of the window, so that elements from end-value - 1 second
         until end-value are retained (the resulting window size is 1 second).
        </p>
  {% highlight scala %}
triggeredStream.evictor(TimeEvictor.of(Time.seconds(1)));
  {% endhighlight %}
      </td>
    </tr>
   <tr>
       <td><strong>Count evictor</strong></td>
       <td>
         <p>
          Retain 1000 elements from the end of the window backwards, evicting all others.
         </p>
   {% highlight scala %}
triggeredStream.evictor(CountEvictor.of(1000));
   {% endhighlight %}
       </td>
     </tr>
    <tr>
        <td><strong>Delta evictor</strong></td>
        <td>
          <p>
            Starting from the beginning of the window, evict elements until an element with
            value lower than the value of the last element is found (by a threshold and a
            DeltaFunction).
          </p>
    {% highlight scala %}
windowedStream.evictor(DeltaEvictor.of(5000.0, { (old,new) => new - old > 0.01 }))
    {% endhighlight %}
        </td>
      </tr>
 </tbody>
</table>
</div>

</div>

### Recipes for Building Windows

The mechanism of window assigner, trigger, and evictor is very powerful, and it allows you to define
many different kinds of windows. Flink's basic window constructs are, in fact, syntactic
sugar on top of the general mechanism. Below is how some common types of windows can be
constructed using the general mechanism

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 35%">Window type</th>
      <th class="text-center">Definition</th>
    </tr>
  </thead>
  <tbody>
      <tr>
        <td>
	  <strong>Tumbling count window</strong><br>
    {% highlight java %}
stream.countWindow(1000)
    {% endhighlight %}
	</td>
        <td>
    {% highlight java %}
stream.window(GlobalWindows.create())
  .trigger(PurgingTrigger.of(CountTrigger.of(size)))
    {% endhighlight %}
        </td>
      </tr>
      <tr>
        <td>
	  <strong>Sliding count window</strong><br>
    {% highlight java %}
stream.countWindow(1000, 100)
    {% endhighlight %}
	</td>
        <td>
    {% highlight java %}
stream.window(GlobalWindows.create())
  .evictor(CountEvictor.of(1000))
  .trigger(CountTrigger.of(100))
    {% endhighlight %}
        </td>
      </tr>
      <tr>
        <td>
	  <strong>Tumbling event time window</strong><br>
    {% highlight java %}
stream.timeWindow(Time.seconds(5))
    {% endhighlight %}
	</td>
        <td>
    {% highlight java %}
stream.window(TumblingEventTimeWindows.of((Time.seconds(5)))
  .trigger(EventTimeTrigger.create())
    {% endhighlight %}
        </td>
      </tr>
      <tr>
        <td>
	  <strong>Sliding event time window</strong><br>
    {% highlight java %}
stream.timeWindow(Time.seconds(5), Time.seconds(1))
    {% endhighlight %}
	</td>
        <td>
    {% highlight java %}
stream.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1)))
  .trigger(EventTimeTrigger.create())
    {% endhighlight %}
        </td>
      </tr>
      <tr>
        <td>
	  <strong>Tumbling processing time window</strong><br>
    {% highlight java %}
stream.timeWindow(Time.seconds(5))
    {% endhighlight %}
	</td>
        <td>
    {% highlight java %}
stream.window(TumblingProcessingTimeWindows.of((Time.seconds(5)))
  .trigger(ProcessingTimeTrigger.create())
    {% endhighlight %}
        </td>
      </tr>
      <tr>
        <td>
	  <strong>Sliding processing time window</strong><br>
    {% highlight java %}
stream.timeWindow(Time.seconds(5), Time.seconds(1))
    {% endhighlight %}
	</td>
        <td>
    {% highlight java %}
stream.window(SlidingProcessingTimeWindows.of(Time.seconds(5), Time.seconds(1)))
  .trigger(ProcessingTimeTrigger.create())
    {% endhighlight %}
        </td>
      </tr>
  </tbody>
</table>


## Windows on Unkeyed Data Streams

You can also define windows on regular (non-keyed) data streams using the `windowAll` transformation. These
windowed data streams have all the capabilities of keyed windowed data streams, but are evaluated at a single
task (and hence at a single computing node). The syntax for defining triggers and evictors is exactly the
same:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
nonKeyedStream
    .windowAll(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1))
    .trigger(CountTrigger.of(100))
    .evictor(CountEvictor.of(10));
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
nonKeyedStream
    .windowAll(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(1))
    .trigger(CountTrigger.of(100))
    .evictor(CountEvictor.of(10))
{% endhighlight %}
</div>
</div>

Basic window definitions are also available for windows on non-keyed streams:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
      <tr>
        <td><strong>Tumbling time window all</strong><br>DataStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 5 seconds, that "tumbles". This means that elements are
          grouped according to their timestamp in groups of 5 second duration, and every element belongs to exactly one window.
          The notion of time used is controlled by the StreamExecutionEnvironment.
    {% highlight java %}
nonKeyedStream.timeWindowAll(Time.seconds(5));
    {% endhighlight %}
          </p>
        </td>
      </tr>
      <tr>
          <td><strong>Sliding time window all</strong><br>DataStream &rarr; WindowedStream</td>
          <td>
            <p>
             Defines a window of 5 seconds, that "slides" by 1 seconds. This means that elements are
             grouped according to their timestamp in groups of 5 second duration, and elements can belong to more than
             one window (since windows overlap by at least 4 seconds)
             The notion of time used is controlled by the StreamExecutionEnvironment.
      {% highlight java %}
nonKeyedStream.timeWindowAll(Time.seconds(5), Time.seconds(1));
      {% endhighlight %}
            </p>
          </td>
        </tr>
      <tr>
        <td><strong>Tumbling count window all</strong><br>DataStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 1000 elements, that "tumbles". This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element belongs to exactly one window.
    {% highlight java %}
nonKeyedStream.countWindowAll(1000)
    {% endhighlight %}
        </p>
        </td>
      </tr>
      <tr>
      <td><strong>Sliding count window all</strong><br>DataStream &rarr; WindowedStream</td>
      <td>
        <p>
          Defines a window of 1000 elements, that "slides" every 100 elements. This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element can belong to more than one window (as windows overlap by at least 900 elements).
  {% highlight java %}
nonKeyedStream.countWindowAll(1000, 100)
  {% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>

<div data-lang="scala" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
      <tr>
        <td><strong>Tumbling time window all</strong><br>DataStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 5 seconds, that "tumbles". This means that elements are
          grouped according to their timestamp in groups of 5 second duration, and every element belongs to exactly one window.
          The notion of time used is controlled by the StreamExecutionEnvironment.
    {% highlight scala %}
nonKeyedStream.timeWindowAll(Time.seconds(5));
    {% endhighlight %}
          </p>
        </td>
      </tr>
      <tr>
          <td><strong>Sliding time window all</strong><br>DataStream &rarr; WindowedStream</td>
          <td>
            <p>
             Defines a window of 5 seconds, that "slides" by 1 seconds. This means that elements are
             grouped according to their timestamp in groups of 5 second duration, and elements can belong to more than
             one window (since windows overlap by at least 4 seconds)
             The notion of time used is controlled by the StreamExecutionEnvironment.
      {% highlight scala %}
nonKeyedStream.timeWindowAll(Time.seconds(5), Time.seconds(1));
      {% endhighlight %}
            </p>
          </td>
        </tr>
      <tr>
        <td><strong>Tumbling count window all</strong><br>DataStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 1000 elements, that "tumbles". This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element belongs to exactly one window.
    {% highlight scala %}
nonKeyedStream.countWindowAll(1000)
    {% endhighlight %}
        </p>
        </td>
      </tr>
      <tr>
      <td><strong>Sliding count window all</strong><br>DataStream &rarr; WindowedStream</td>
      <td>
        <p>
          Defines a window of 1000 elements, that "slides" every 100 elements. This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element can belong to more than one window (as windows overlap by at least 900 elements).
  {% highlight scala %}
nonKeyedStream.countWindowAll(1000, 100)
  {% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>
