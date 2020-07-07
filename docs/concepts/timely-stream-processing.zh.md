---
title: 及时流处理
nav-id: timely-stream-processing
nav-pos: 3
nav-title: 及时流处理
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

* This will be replaced by the TOC
{:toc}

## Introduction

Timely steam processing is an extension of [stateful stream processing]({% link
concepts/stateful-stream-processing.zh.md %}) in which time plays some role in the
computation. Among other things, this is the case when you do time series
analysis, when doing aggregations based on certain time periods (typically
called windows), or when you do event processing where the time when an event
occured is important.

In the following sections we will highlight some of the topics that you should
consider when working with timely Flink Applications.

{% top %}

## Notions of Time: Event Time and Processing Time

When referring to time in a streaming program (for example to define windows),
one can refer to different notions of *time*:

- **Processing time:** Processing time refers to the system time of the machine
  that is executing the respective operation.

  When a streaming program runs on processing time, all time-based operations
  (like time windows) will use the system clock of the machines that run the
  respective operator. An hourly processing time window will include all
  records that arrived at a specific operator between the times when the system
  clock indicated the full hour. For example, if an application begins running
  at 9:15am, the first hourly processing time window will include events
  processed between 9:15am and 10:00am, the next window will include events
  processed between 10:00am and 11:00am, and so on.

  Processing time is the simplest notion of time and requires no coordination
  between streams and machines.  It provides the best performance and the
  lowest latency. However, in distributed and asynchronous environments
  processing time does not provide determinism, because it is susceptible to
  the speed at which records arrive in the system (for example from the message
  queue), to the speed at which the records flow between operators inside the
  system, and to outages (scheduled, or otherwise).

- **Event time:** Event time is the time that each individual event occurred on
  its producing device.  This time is typically embedded within the records
  before they enter Flink, and that *event timestamp* can be extracted from
  each record. In event time, the progress of time depends on the data, not on
  any wall clocks. Event time programs must specify how to generate *Event Time
  Watermarks*, which is the mechanism that signals progress in event time. This
  watermarking mechanism is described in a later section,
  [below](#event-time-and-watermarks).

  In a perfect world, event time processing would yield completely consistent
  and deterministic results, regardless of when events arrive, or their
  ordering.  However, unless the events are known to arrive in-order (by
  timestamp), event time processing incurs some latency while waiting for
  out-of-order events. As it is only possible to wait for a finite period of
  time, this places a limit on how deterministic event time applications can
  be.

  Assuming all of the data has arrived, event time operations will behave as
  expected, and produce correct and consistent results even when working with
  out-of-order or late events, or when reprocessing historic data. For example,
  an hourly event time window will contain all records that carry an event
  timestamp that falls into that hour, regardless of the order in which they
  arrive, or when they are processed. (See the section on [late
  events](#late-elements) for more information.)

  Note that sometimes when event time programs are processing live data in
  real-time, they will use some *processing time* operations in order to
  guarantee that they are progressing in a timely fashion.

<img src="{{ site.baseurl }}/fig/event_processing_time.svg" alt="Event Time and Processing Time" class="offset" width="80%" />

{% top %}

## Event Time and Watermarks

*Note: Flink implements many techniques from the Dataflow Model. For a good
introduction to event time and watermarks, have a look at the articles below.*

  - [Streaming
    101](https://www.oreilly.com/ideas/the-world-beyond-batch-streaming-101) by
    Tyler Akidau
  - The [Dataflow Model
    paper](https://research.google.com/pubs/archive/43864.pdf)


A stream processor that supports *event time* needs a way to measure the
progress of event time.  For example, a window operator that builds hourly
windows needs to be notified when event time has passed beyond the end of an
hour, so that the operator can close the window in progress.

*Event time* can progress independently of *processing time* (measured by wall
clocks).  For example, in one program the current *event time* of an operator
may trail slightly behind the *processing time* (accounting for a delay in
receiving the events), while both proceed at the same speed.  On the other
hand, another streaming program might progress through weeks of event time with
only a few seconds of processing, by fast-forwarding through some historic data
already buffered in a Kafka topic (or another message queue).

------

The mechanism in Flink to measure progress in event time is **watermarks**.
Watermarks flow as part of the data stream and carry a timestamp *t*. A
*Watermark(t)* declares that event time has reached time *t* in that stream,
meaning that there should be no more elements from the stream with a timestamp
*t' <= t* (i.e. events with timestamps older or equal to the watermark).

The figure below shows a stream of events with (logical) timestamps, and
watermarks flowing inline. In this example the events are in order (with
respect to their timestamps), meaning that the watermarks are simply periodic
markers in the stream.

<img src="{{ site.baseurl }}/fig/stream_watermark_in_order.svg" alt="A data stream with events (in order) and watermarks" class="center" width="65%" />

Watermarks are crucial for *out-of-order* streams, as illustrated below, where
the events are not ordered by their timestamps.  In general a watermark is a
declaration that by that point in the stream, all events up to a certain
timestamp should have arrived.  Once a watermark reaches an operator, the
operator can advance its internal *event time clock* to the value of the
watermark.

<img src="{{ site.baseurl }}/fig/stream_watermark_out_of_order.svg" alt="A data stream with events (out of order) and watermarks" class="center" width="65%" />

Note that event time is inherited by a freshly created stream element (or
elements) from either the event that produced them or from watermark that
triggered creation of those elements.

### Watermarks in Parallel Streams

Watermarks are generated at, or directly after, source functions. Each parallel
subtask of a source function usually generates its watermarks independently.
These watermarks define the event time at that particular parallel source.

As the watermarks flow through the streaming program, they advance the event
time at the operators where they arrive. Whenever an operator advances its
event time, it generates a new watermark downstream for its successor
operators.

Some operators consume multiple input streams; a union, for example, or
operators following a *keyBy(...)* or *partition(...)* function.  Such an
operator's current event time is the minimum of its input streams' event times.
As its input streams update their event times, so does the operator.

The figure below shows an example of events and watermarks flowing through
parallel streams, and operators tracking event time.

<img src="{{ site.baseurl }}/fig/parallel_streams_watermarks.svg" alt="Parallel data streams and operators with events and watermarks" class="center" width="80%" />

## Lateness

It is possible that certain elements will violate the watermark condition,
meaning that even after the *Watermark(t)* has occurred, more elements with
timestamp *t' <= t* will occur. In fact, in many real world setups, certain
elements can be arbitrarily delayed, making it impossible to specify a time by
which all elements of a certain event timestamp will have occurred.
Furthermore, even if the lateness can be bounded, delaying the watermarks by
too much is often not desirable, because it causes too much delay in the
evaluation of event time windows.

For this reason, streaming programs may explicitly expect some *late* elements.
Late elements are elements that arrive after the system's event time clock (as
signaled by the watermarks) has already passed the time of the late element's
timestamp. See [Allowed Lateness]({% link
dev/stream/operators/windows.zh.md %}#allowed-lateness) for more information on
how to work with late elements in event time windows.

## Windowing

Aggregating events (e.g., counts, sums) works differently on streams than in
batch processing.  For example, it is impossible to count all elements in a
stream, because streams are in general infinite (unbounded). Instead,
aggregates on streams (counts, sums, etc), are scoped by **windows**, such as
*"count over the last 5 minutes"*, or *"sum of the last 100 elements"*.

Windows can be *time driven* (example: every 30 seconds) or *data driven*
(example: every 100 elements).  One typically distinguishes different types of
windows, such as *tumbling windows* (no overlap), *sliding windows* (with
overlap), and *session windows* (punctuated by a gap of inactivity).

<img src="{{ site.baseurl }}/fig/windows.svg" alt="Time- and Count Windows" class="offset" width="80%" />

Please check out this [blog
post](https://flink.apache.org/news/2015/12/04/Introducing-windows.html) for
additional examples of windows or take a look a [window documentation]({% link
dev/stream/operators/windows.zh.md %}) of the DataStream API.

{% top %}
