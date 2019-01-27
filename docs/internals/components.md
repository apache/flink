---
title:  "Component Stack"
nav-parent_id: internals
nav-pos: 1
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

As a software stack, Flink is a layered system. The different layers of the stack build on
top of each other and raise the abstraction level of the program representations they accept:

- The **runtime** layer receives a program in the form of a *JobGraph*. A JobGraph is a generic parallel
data flow with arbitrary tasks that consume and produce data streams.

- Both the **DataStream API** and the **DataSet API** generate JobGraphs through separate compilation
processes. The DataSet API uses an optimizer to determine the optimal plan for the program, while
the DataStream API uses a stream builder.

- The JobGraph is executed according to a variety of deployment options available in Flink (e.g., local,
remote, YARN, etc)

- Libraries and APIs that are bundled with Flink generate DataSet or DataStream API programs. These are
Table for queries on logical tables, FlinkML for Machine Learning, and Gelly for graph processing.

{% top %}
