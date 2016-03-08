---
title: "Overview"
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

This documentation is for Apache Flink version {{ site.version }}.

Apache Flink is an open source platform for distributed stream and batch data processing. Flink’s core is a streaming dataflow engine that provides data distribution, communication, and fault tolerance for distributed computations over data streams. Flink also builds batch processing on top of the streaming engine, overlaying native iteration support, managed memory, and program optimization.

## First Steps

- **Quickstarts**: [Run an example program](quickstart/setup_quickstart.html) on your local machine or [write a simple program](quickstart/run_example_quickstart.html) working on live Wikipedia edits.

- **Setup:** The [local]({{ site.baseurl }}/setup/local_setup.html), [cluster](setup/cluster_setup.html), and [cloud](setup/gce_setup.html) setup guides show you how to deploy Flink.

- **Programming Guides**: You can check out our guides about [basic concepts](apis/common/index.html) and the [DataStream API](apis/streaming/index.html) or [DataSet API](apis/batch/index.html) to learn how to write your first Flink programs.

- **Migration Guide**: Check out the [0.10 to 1.0 migration guide](https://cwiki.apache.org/confluence/display/FLINK/Migration+Guide%3A+0.10.x+to+1.0.x) if you are upgrading from Flink 0.10.x.

## Stack

This is an overview of Flink's stack. Click on any component to go to the respective documentation page.

<center>
  <img src="fig/stack.png" width="700px" alt="Apache Flink: Stack" usemap="#overview-stack">
</center>

<map name="overview-stack">
<area id="lib-datastream-cep" title="CEP: Complex Event Processing" href="{{ site.baseurl }}/apis/streaming/libs/cep.html" shape="rect" coords="63,0,143,177" />
<area id="lib-datastream-table" title="Table: Relational DataStreams" href="{{ site.baseurl }}/apis/batch/libs/table.html" shape="rect" coords="143,0,223,177" />
<area id="lib-dataset-ml" title="FlinkML: Machine Learning" href="{{ site.baseurl }}/apis/batch/libs/ml/index.html" shape="rect" coords="382,2,462,176" />
<area id="lib-dataset-gelly" title="Gelly: Graph Processing" href="{{ site.baseurl }}/apis/batch/libs/gelly.html" shape="rect" coords="461,0,541,177" />
<area id="lib-dataset-table" title="Table: Relational DataSets" href="{{ site.baseurl }}/apis/batch/libs/table.html" shape="rect" coords="544,0,624,177" />
<area id="datastream" title="DataStream API" href="{{ site.baseurl }}/apis/streaming/index.html" shape="rect" coords="64,177,379,255" />
<area id="dataset" title="DataSet API" href="{{ site.baseurl }}/apis/batch/index.html" shape="rect" coords="382,177,697,255" />
<area id="runtime" title="Runtime" href="{{ site.baseurl }}/internals/general_arch.html" shape="rect" coords="63,257,700,335" />
<area id="local" title="Local" href="{{ site.baseurl }}/setup/local_setup.html" shape="rect" coords="62,337,275,414" />
<area id="cluster" title="Cluster" href="{{ site.baseurl }}/setup/cluster_setup.html" shape="rect" coords="273,336,486,413" />
<area id="cloud" title="Cloud" href="{{ site.baseurl }}/setup/gce_setup.html" shape="rect" coords="485,336,700,414" />
</map>
