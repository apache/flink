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

Apache Flink is an open source platform for distributed stream and batch data processing. Flink’s core is
a streaming dataflow engine that provides data distribution, communication, and fault tolerance for distributed
computations over data streams. Flink also builds batch processing on top of the streaming engine, overlaying
native iteration support, managed memory, and program optimization.

If you want to write your first program, look at one of the available quickstarts, and refer to the
[DataSet API guide](apis/programming_guide.html) or the [DataStream API guide](apis/streaming_guide.html).

## Stack

This is an overview of Flink's stack. Click on any component to go to the respective documentation page.

<img src="fig/overview-stack-0.9.png" width="893" height="450" alt="Stack" usemap="#overview-stack">

<map name="overview-stack">
  <area shape="rect" coords="188,0,263,200" alt="Graph API: Gelly" href="libs/gelly_guide.html">
  <area shape="rect" coords="268,0,343,200" alt="Flink ML" href="libs/ml/">
  <area shape="rect" coords="348,0,423,200" alt="Table" href="libs/table.html">

  <area shape="rect" coords="188,205,538,260" alt="DataSet API (Java/Scala)" href="apis/programming_guide.html">
  <area shape="rect" coords="543,205,893,260" alt="DataStream API (Java/Scala)" href="apis/streaming_guide.html">

  <!-- <area shape="rect" coords="188,275,538,330" alt="Optimizer" href="optimizer.html"> -->
  <!-- <area shape="rect" coords="543,275,893,330" alt="Stream Builder" href="streambuilder.html"> -->

  <area shape="rect" coords="188,335,893,385" alt="Flink Runtime" href="internals/general_arch.html">

  <area shape="rect" coords="188,405,328,455" alt="Local" href="apis/local_execution.html">
  <area shape="rect" coords="333,405,473,455" alt="Remote" href="apis/cluster_execution.html">
  <area shape="rect" coords="478,405,638,455" alt="Embedded" href="apis/local_execution.html">
  <area shape="rect" coords="643,405,765,455" alt="YARN" href="setup/yarn_setup.html">
  <area shape="rect" coords="770,405,893,455" alt="Tez" href="setup/flink_on_tez.html">
</map>


## Download

This documentation is for Apache Flink version {{ site.version }}, which is the current development version of the next upcoming major release of Apache Flink.

You can download the latest pre-built snapshot version from the [downloads]({{ site.download_url }}#latest) page of the [project website]({{ site.website_url }}).

<!--The Scala API uses Scala {{ site.scala_version }}. Please make sure to use a compatible version.

The Scala API uses Scala 2.10, but you can use the API with Scala 2.11. To use Flink with
Scala 2.11, please check [build guide](/setup/building.html#build-flink-for-scala-211)
and [programming guide](/apis/programming_guide.html#scala-dependency-versions).-->


