---
title: Dataflow Programming Model
nav-id: programming-model
nav-pos: 101
nav-title: Programming Model (outdated)
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

## Levels of Abstraction

Flink offers different levels of abstraction to develop streaming/batch applications.

<img src="{{ site.baseurl }}/fig/levels_of_abstraction.svg" alt="Programming levels of abstraction" class="offset" width="80%" />

  - The lowest level abstraction simply offers **stateful streaming**. It is embedded into the [DataStream API](../dev/datastream_api.html)
    via the [Process Function](../dev/stream/operators/process_function.html). It allows users freely process events from one or more streams,
    and use consistent fault tolerant *state*. In addition, users can register event time and processing time callbacks,
    allowing programs to realize sophisticated computations.

  - In practice, most applications would not need the above described low level abstraction, but would instead program against the
    **Core APIs** like the [DataStream API](../dev/datastream_api.html) (bounded/unbounded streams) and the [DataSet API](../dev/batch/index.html)
    (bounded data sets). These fluent APIs offer the common building blocks for data processing, like various forms of user-specified
    transformations, joins, aggregations, windows, state, etc. Data types processed in these APIs are represented as classes
    in the respective programming languages.

    The low level *Process Function* integrates with the *DataStream API*, making it possible to go the lower level abstraction 
    for certain operations only. The *DataSet API* offers additional primitives on bounded data sets, like loops/iterations.

  - The **Table API** is a declarative DSL centered around *tables*, which may be dynamically changing tables (when representing streams).
    The [Table API](../dev/table/index.html) follows the (extended) relational model: Tables have a schema attached (similar to tables in relational databases)
    and the API offers comparable operations, such as select, project, join, group-by, aggregate, etc.
    Table API programs declaratively define *what logical operation should be done* rather than specifying exactly
   *how the code for the operation looks*. Though the Table API is extensible by various types of user-defined
    functions, it is less expressive than the *Core APIs*, but more concise to use (less code to write).
    In addition, Table API programs also go through an optimizer that applies optimization rules before execution.

    One can seamlessly convert between tables and *DataStream*/*DataSet*, allowing programs to mix *Table API* and with the *DataStream*
    and *DataSet* APIs.

  - The highest level abstraction offered by Flink is **SQL**. This abstraction is similar to the *Table API* both in semantics and
    expressiveness, but represents programs as SQL query expressions.
    The [SQL](../dev/table/index.html#sql) abstraction closely interacts with the Table API, and SQL queries can be executed over tables defined in the *Table API*.


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

## Next Steps

Continue with the basic concepts in Flink's [Distributed Runtime](runtime.html).
