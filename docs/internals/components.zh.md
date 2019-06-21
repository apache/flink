---
title:  "组件堆栈"
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
Table for queries on logical tables, complex event processing, and Gelly for graph processing.

You can click on the components in the figure to learn more.

<center>
  <img src="{{ site.baseurl }}/fig/stack.png" width="700px" alt="Apache Flink: Stack" usemap="#overview-stack">
</center>

<map name="overview-stack">
<area id="lib-datastream-cep" title="CEP: Complex Event Processing" href="{{ site.baseurl }}/dev/libs/cep.html" shape="rect" coords="63,0,143,177" />
<area id="lib-datastream-table" title="Table: Relational DataStreams" href="{{ site.baseurl }}/dev/table/index.html" shape="rect" coords="143,0,223,177" />
<area id="lib-dataset-gelly" title="Gelly: Graph Processing" href="{{ site.baseurl }}/dev/libs/gelly/index.html" shape="rect" coords="461,0,541,177" />
<area id="lib-dataset-table" title="Table API and SQL" href="{{ site.baseurl }}/dev/table/index.html" shape="rect" coords="544,0,624,177" />
<area id="datastream" title="DataStream API" href="{{ site.baseurl }}/dev/datastream_api.html" shape="rect" coords="64,177,379,255" />
<area id="dataset" title="DataSet API" href="{{ site.baseurl }}/dev/batch/index.html" shape="rect" coords="382,177,697,255" />
<area id="runtime" title="Runtime" href="{{ site.baseurl }}/concepts/runtime.html" shape="rect" coords="63,257,700,335" />
<area id="local" title="Local" href="{{ site.baseurl }}/getting-started/tutorials/local_setup.html" shape="rect" coords="62,337,275,414" />
<area id="cluster" title="Cluster" href="{{ site.baseurl }}/ops/deployment/cluster_setup.html" shape="rect" coords="273,336,486,413" />
<area id="cloud" title="Cloud" href="{{ site.baseurl }}/ops/deployment/gce_setup.html" shape="rect" coords="485,336,700,414" />
</map>

{% top %}
