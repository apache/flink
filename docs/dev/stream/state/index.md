---
title: "State & Fault Tolerance"
nav-id: streaming_state
nav-title: "State & Fault Tolerance"
nav-parent_id: streaming
nav-pos: 3
nav-show_overview: true
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

In this section you will learn about the APIs that Flink provides for writing
stateful programs. Please take a look at [Stateful Stream
Processing]({% link concepts/stateful-stream-processing.md %})
to learn about the concepts behind stateful stream processing.

{% top %}

Where to go next?
-----------------

* [Working with State]({% link dev/stream/state/state.md %}): Shows how to use state in a Flink application and explains the different kinds of state.
* [The Broadcast State Pattern]({% link dev/stream/state/broadcast_state.md %}): Explains how to connect a broadcast stream with a non-broadcast stream and use state to exchange information between them. 
* [Checkpointing]({% link dev/stream/state/checkpointing.md %}): Describes how to enable and configure checkpointing for fault tolerance.
* [Queryable State]({% link dev/stream/state/queryable_state.md %}): Explains how to access state from outside of Flink during runtime.
* [State Schema Evolution]({% link dev/stream/state/schema_evolution.md %}): Shows how schema of state types can be evolved.
* [Custom Serialization for Managed State]({% link dev/stream/state/custom_serialization.md %}): Discusses how to implement custom serializers, especially for schema evolution.

{% top %}
