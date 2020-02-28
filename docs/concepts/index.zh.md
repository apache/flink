---
title: 概念
nav-id: concepts
nav-pos: 2
nav-title: '<i class="fa fa-map-o title appetizer" aria-hidden="true"></i> 概念'
nav-parent_id: root
nav-show_overview: true
permalink: /concepts/index.html
always-expand: true
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

Flink offers different levels of abstraction for developing streaming/batch applications.

<img src="{{ site.baseurl }}/fig/levels_of_abstraction.svg" alt="Programming levels of abstraction" class="offset" width="80%" />

  - The lowest level abstraction simply offers **stateful streaming**. It is
    embedded into the [DataStream API]({{ site.baseurl}}{% link
    dev/datastream_api.md %}) via the [Process Function]({{ site.baseurl }}{%
    link dev/stream/operators/process_function.md %}). It allows users freely
    process events from one or more streams, and use consistent fault tolerant
    *state*. In addition, users can register event time and processing time
    callbacks, allowing programs to realize sophisticated computations.

  - In practice, most applications would not need the above described low level
    abstraction, but would instead program against the **Core APIs** like the
    [DataStream API]({{ site.baseurl }}{% link dev/datastream_api.md %})
    (bounded/unbounded streams) and the [DataSet API]({{ site.baseurl }}{% link
    dev/batch/index.md %}) (bounded data sets). These fluent APIs offer the
    common building blocks for data processing, like various forms of
    user-specified transformations, joins, aggregations, windows, state, etc.
    Data types processed in these APIs are represented as classes in the
    respective programming languages.

    The low level *Process Function* integrates with the *DataStream API*,
    making it possible to go the lower level abstraction for certain operations
    only. The *DataSet API* offers additional primitives on bounded data sets,
    like loops/iterations.

  - The **Table API** is a declarative DSL centered around *tables*, which may
    be dynamically changing tables (when representing streams).  The [Table
    API]({{ site.baseurl }}{% link dev/table/index.md %}) follows the
    (extended) relational model: Tables have a schema attached (similar to
    tables in relational databases) and the API offers comparable operations,
    such as select, project, join, group-by, aggregate, etc.  Table API
    programs declaratively define *what logical operation should be done*
    rather than specifying exactly *how the code for the operation looks*.
    Though the Table API is extensible by various types of user-defined
    functions, it is less expressive than the *Core APIs*, but more concise to
    use (less code to write).  In addition, Table API programs also go through
    an optimizer that applies optimization rules before execution.

    One can seamlessly convert between tables and *DataStream*/*DataSet*,
    allowing programs to mix *Table API* and with the *DataStream* and
    *DataSet* APIs.

  - The highest level abstraction offered by Flink is **SQL**. This abstraction
    is similar to the *Table API* both in semantics and expressiveness, but
    represents programs as SQL query expressions.  The [SQL]({{ site.baseurl
    }}{% link dev/table/index.md %}#sql) abstraction closely interacts with the
    Table API, and SQL queries can be executed over tables defined in the
    *Table API*.

This _concepts_ section explains the basic concepts behind the different APIs,
that is the concepts behind Flink as a stateful and timely stream processing
system.
