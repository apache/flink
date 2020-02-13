---
title: Timely Stream Processing
nav-id: timely-stream-processing
nav-pos: 3
nav-title: Timely Stream Processing
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

## Time

When referring to time in a streaming program (for example to define windows), one can refer to different notions
of time:

  - **Event Time** is the time when an event was created. It is usually described by a timestamp in the events,
    for example attached by the producing sensor, or the producing service. Flink accesses event timestamps
    via [timestamp assigners]({{ site.baseurl }}/dev/event_timestamps_watermarks.html).

  - **Ingestion time** is the time when an event enters the Flink dataflow at the source operator.

  - **Processing Time** is the local time at each operator that performs a time-based operation.

<img src="{{ site.baseurl }}/fig/event_ingestion_processing_time.svg" alt="Event Time, Ingestion Time, and Processing Time" class="offset" width="80%" />

More details on how to handle time are in the [event time docs]({{ site.baseurl }}/dev/event_time.html).

{% top %}

## Windows

Aggregating events (e.g., counts, sums) works differently on streams than in batch processing.
For example, it is impossible to count all elements in a stream,
because streams are in general infinite (unbounded). Instead, aggregates on streams (counts, sums, etc),
are scoped by **windows**, such as *"count over the last 5 minutes"*, or *"sum of the last 100 elements"*.

Windows can be *time driven* (example: every 30 seconds) or *data driven* (example: every 100 elements).
One typically distinguishes different types of windows, such as *tumbling windows* (no overlap),
*sliding windows* (with overlap), and *session windows* (punctuated by a gap of inactivity).

<img src="{{ site.baseurl }}/fig/windows.svg" alt="Time- and Count Windows" class="offset" width="80%" />

More window examples can be found in this [blog post](https://flink.apache.org/news/2015/12/04/Introducing-windows.html).
More details are in the [window docs](../dev/stream/operators/windows.html).

{% top %}

## Latency & Completeness

### Latency vs. Completeness in Batch & Stream Processing

{% top %}

## Notions of Time

### Event Time

`Different Sources of Time (Event, Ingestion, Storage)`

### Processing Time

{% top %}

## Making Progress: Watermarks, Processing Time, Lateness

### Propagation of watermarks

{% top %}

## Windowing

{% top %}

