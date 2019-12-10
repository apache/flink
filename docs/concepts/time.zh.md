---
title: Event Time
nav-pos: 2
nav-title: "Event Time"
nav-parent_id: concepts
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

Time is a unique attribute in data processing because, unlike other characteristics, it always moves forward.
However, that does not mean records are processed in order by perfectly increasing timestamps.
Systems go down, network connections lag, and data consumption often occurs out of order.
Event time is the tool by which Flink can reorder events and generate deterministic results from messy, out of order data streams.

* This will be replaced by the TOC
{:toc}

## Different Notions of Time

Very rarely will a system want to process data based on the current time of the running job - [Processing Time]({{ site.baseurl }}/concepts/glossary.html#processing-time) - but instead, the time at which the event was generated - [Event Time]({{ site.baseurl }}/concepts/glossary.html#event-time).
If a transaction occurs at 1 o'clock on Sunday, then that is when it should be counted, even if the processing does not occur until several days later. 

Most records contain embedded timestamps that can be used to determine when event generation occurred at the edge.
Flink can use these timestamps to calculate correct results concerning time.
For example, the event timestamp of a record can be used to bucket elements into the right [Window]({{ site.baseurl }}/dev/stream/operators/windows.html) or detect patterns based on properly ordered data in a [MATCH_RECOGNIZE]({{ site.baseurl }}/dev/table/streaming/match_recognize.html) query. 

<img src="{{ site.baseurl }}/fig/event_ingestion_processing_time.svg" alt="Event Time, Ingestion Time, and Processing Time" class="offset" width="80%" />

## Making Progress

A stream processor that supports event time needs a way to measure the progress of event time.
A window that computes hourly aggregations needs to know when no more records are expected for a particular hour, so it can close the window and output its results.

Event time can progress independently of processing time, measured by wall clocks.
In one program, the current event time of an operator may trail slightly behind the processing time, accounting for a delay in receiving the events, while both proceed at the same speed.
On the other hand, another streaming program might progress through weeks of event time with only a few seconds of processing, by fast-forwarding through some historical data already buffered in a Kafka topic or another message queue.

The mechanism in Flink to measure progress in event time is [watermarks]({{ site.baseurl }}/concepts/glossary.html#watermark).
Watermarks flow as part of the data stream and carry a timestamp t.
A *Watermark(t)* declares that event time has reached time t, meaning that no more elements are expected to arrive with a timestamp *t' <= t*.
The figure below shows a stream of events with logical timestamps and watermarks flowing inline.
In this example, the events are in order by their timestamps, meaning the watermarks are periodic markers in the stream.

<img src="{{ site.baseurl }}/fig/stream_watermark_in_order.svg" alt="A data stream with events (in order) and watermarks" class="center" width="65%" />

Watermarks are crucial for *out-of-order* streams, as illustrated below, where the events are not correctly ordered by time.
A watermark is a declaration that all events up to a specific timestamp should have arrived.
Once a watermark reaches an operator, the operator can advance its internal *event time clock* to the value of the watermark.

<img src="{{ site.baseurl }}/fig/stream_watermark_out_of_order.svg" alt="A data stream with events (out of order) and watermarks" class="center" width="65%" />

Note that event time is inherited by a freshly created stream element, or elements, from either the event that produced them or from watermark that triggered the creation of those elements.

### Propagation of Watermarks

Watermarks are generated at, or directly after, source functions.
Each parallel instance of a source function usually generates its watermarks independently.
These watermarks define the event time at that particular parallel source.

As the watermarks flow through the streaming program, they advance the event time at the operators where they arrive.
Whenever an operator advances its event time, it generates a new watermark downstream for its successor operators.

Some operators consume multiple input streams; a union, for example, or operators following a *keyBy(...)* function.
For such an operator, current event time is the minimum of its input streams' event times.
As its input streams update their event times, so does the operator.

The figure below shows an example of events and watermarks flowing through parallel streams, and operators tracking event time.

<img src="{{ site.baseurl }}/fig/parallel_streams_watermarks.svg" alt="Parallel data streams and operators with events and watermarks" class="center" width="80%" />

## Late Elements

Certain elements may violate the watermark condition, meaning that even after the *Watermark(t)* has occurred,
more elements with timestamp *t' <= t* will arrive.
In fact, in many real-world setups, individual elements can be arbitrarily delayed, making it impossible to specify a time by which all events less than a specific event timestamp have been processed.
Even if the lateness can be bounded, delaying the watermarks by too much is often undesirable.
It will cause too much delay in the evaluation of event time windows.

For this reason, streaming programs may explicitly expect some *late* elements.
Late elements are elements that arrive after the system's event time clock, as signaled by watermarks, has already passed the time of the new element's timestamp.
See [allowed lateness]({{ site.baseurl }}/dev/stream/operators/windows.html#allowed-lateness) for more information on how to work with late elements in event time windows.

## Further Resources

Flink implements many techniques from the Dataflow Model.
For more information about event time and watermarks, have a look at the articles below.

  - [Streaming 101](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-101) by Tyler Akidau
  - The [Dataflow Model paper](https://research.google.com/pubs/archive/43864.pdf)

