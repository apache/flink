---
title: "Pre-defined Timestamp Extractors / Watermark Emitters"
nav-parent_id: event_time
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

* toc
{:toc}

As described in [timestamps and watermark handling]({{ site.baseurl }}/dev/event_timestamps_watermarks.html),
Flink provides abstractions that allow the programmer to assign their own timestamps and emit their own watermarks. More specifically,
one can do so by implementing one of the `AssignerWithPeriodicWatermarks` and `AssignerWithPunctuatedWatermarks` interfaces, depending
on the use case. In a nutshell, the first will emit watermarks periodically, while the second does so based on some property of
the incoming records, e.g. whenever a special element is encountered in the stream.

In order to further ease the programming effort for such tasks, Flink comes with some pre-implemented timestamp assigners.
This section provides a list of them. Apart from their out-of-the-box functionality, their implementation can serve as an example
for custom implementations.

### **Assigners with ascending timestamps**

The simplest special case for *periodic* watermark generation is the case where timestamps seen by a given source task
occur in ascending order. In that case, the current timestamp can always act as a watermark, because no earlier timestamps will
arrive.

Note that it is only necessary that timestamps are ascending *per parallel data source task*. For example, if
in a specific setup one Kafka partition is read by one parallel data source instance, then it is only necessary that
timestamps are ascending within each Kafka partition. Flink's watermark merging mechanism will generate correct
watermarks whenever parallel streams are shuffled, unioned, connected, or merged.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<MyEvent> stream = ...

DataStream<MyEvent> withTimestampsAndWatermarks =
    stream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<MyEvent>() {

        @Override
        public long extractAscendingTimestamp(MyEvent element) {
            return element.getCreationTime();
        }
});
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val stream: DataStream[MyEvent] = ...

val withTimestampsAndWatermarks = stream.assignAscendingTimestamps( _.getCreationTime )
{% endhighlight %}
</div>
</div>

### **Assigners allowing a fixed amount of lateness**

Another example of periodic watermark generation is when the watermark lags behind the maximum (event-time) timestamp
seen in the stream by a fixed amount of time. This case covers scenarios where the maximum lateness that can be encountered in a
stream is known in advance, e.g. when creating a custom source containing elements with timestamps spread within a fixed period of
time for testing. For these cases, Flink provides the `BoundedOutOfOrdernessTimestampExtractor` which takes as an argument
the `maxOutOfOrderness`, i.e. the maximum amount of time an element is allowed to be late before being ignored when computing the
final result for the given window. Lateness corresponds to the result of `t - t_w`, where `t` is the (event-time) timestamp of an
element, and `t_w` that of the previous watermark. If `lateness > 0` then the element is considered late and is, by default, ignored when computing
the result of the job for its corresponding window. See the documentation about [allowed lateness]({{ site.baseurl }}/dev/stream/operators/windows.html#allowed-lateness)
for more information about working with late elements.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<MyEvent> stream = ...

DataStream<MyEvent> withTimestampsAndWatermarks =
    stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<MyEvent>(Time.seconds(10)) {

        @Override
        public long extractTimestamp(MyEvent element) {
            return element.getCreationTime();
        }
});
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val stream: DataStream[MyEvent] = ...

val withTimestampsAndWatermarks = stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[MyEvent](Time.seconds(10))( _.getCreationTime ))
{% endhighlight %}
</div>
</div>

{% top %}
