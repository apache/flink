---
title: "实验功能"
nav-id: experimental_features
nav-show_overview: true
nav-parent_id: streaming
nav-pos: 100
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

This section describes experimental features in the DataStream API. Experimental features are still evolving and can be either unstable,
incomplete, or subject to heavy change in future versions.

Reinterpreting a pre-partitioned data stream as keyed stream
------------------------------------------------------------

We can re-interpret a pre-partitioned data stream as a keyed stream to avoid shuffling.

**WARNING**: The re-interpreted data stream **MUST** already be pre-partitioned in **EXACTLY** the same way Flink's keyBy would partition
the data in a shuffle w.r.t. key-group assignment.

One use-case for this could be a materialized shuffle between two jobs: the first job performs a keyBy shuffle and materializes
each output into a partition. A second job has sources that, for each parallel instance, reads from the corresponding partitions
created by the first job. Those sources can now be re-interpreted as keyed streams, e.g. to apply windowing. Notice that this trick
makes the second job embarrassingly parallel, which can be helpful for a fine-grained recovery scheme.

This re-interpretation functionality is exposed through `DataStreamUtils`:

{% highlight java %}
static <T, K> KeyedStream<T, K> reinterpretAsKeyedStream(
    DataStream<T> stream,
    KeySelector<T, K> keySelector,
    TypeInformation<K> typeInfo)
{% endhighlight %}

Given a base stream, a key selector, and type information,
the method creates a keyed stream from the base stream.

Code example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStreamSource<Integer> source = ...
DataStreamUtils.reinterpretAsKeyedStream(source, (in) -> in, TypeInformation.of(Integer.class))
    .window(TumblingEventTimeWindows.of(Time.seconds(1)))
    .reduce((a, b) -> a + b)
    .addSink(new DiscardingSink<>());
env.execute();
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setParallelism(1)
val source = ...
new DataStreamUtils(source).reinterpretAsKeyedStream((in) => in)
  .window(TumblingEventTimeWindows.of(Time.seconds(1)))
  .reduce((a, b) => a + b)
  .addSink(new DiscardingSink[Int])
env.execute()
{% endhighlight %}

{% top %}
