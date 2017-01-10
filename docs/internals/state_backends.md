---
title:  "State and State Backends"
nav-title: State Backends
nav-parent_id: internals
nav-pos: 4
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

**NOTE** This document is only a sketch of some bullet points, to be fleshed out.

**NOTE** The structure of State Backends changed heavily between version 1.1 and 1.2. This documentation is only applicable
to Apache Flink version 1.2 and later.


## Keyed State and Operator state

There are two basic state backends: `Keyed State` and `Operator State`.

#### Keyed State

*Keyed State* is always relative to keys and can only be used in functions and operators on a `KeyedStream`.
Examples of keyed state are the `ValueState` or `ListState` that one can create in a function on a `KeyedStream`, as
well as the state of a keyed window operator.

Keyed State is organized in so called *Key Groups*. Key Groups are the unit by which keyed state can be redistributed and
there are as many key groups as the defined maximum parallelism.
During execution each parallel instance of an operator gets one or more key groups.

#### Operator State

*Operator State* is state per parallel subtask. It subsumes the `Checkpointed` interface in Flink 1.0 and Flink 1.1.
The new `CheckpointedFunction` interface is basically a shortcut (syntactic sugar) for the Operator State.

Operator State needs special re-distribution schemes when parallelism is changed. There can be different variations of such
schemes; the following are currently defined:

  - **List-style redistribution:** Each operator returns a List of state elements. The whole state is logically a concatenation of
    all lists. On restore/redistribution, the list is evenly divided into as many sublists as there are parallel operators.
    Each operator gets a sublist, which can be empty, or contain one or more elements.


## Raw and Managed State

*Keyed State* and *Operator State* exist in two forms: *managed* and *raw*.

*Managed State* is represented in data structures controlled by the Flink runtime, such as internal hash tables, or RocksDB.
Examples are "ValueState", "ListState", etc. Flink's runtime encodes the states and writes them into the checkpoints.

*Raw State* is state that users and operators keep in their own data structures. When checkpointed, they only write a sequence of bytes into
the checkpoint. Flink knows nothing about the state's data structures and sees only the raw bytes.

