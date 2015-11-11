---
title:  "General Architecture and Process Model"
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

## The Processes

When the Flink system is started, it bring up the *JobManager* and one or more *TaskManagers*. The JobManager
is the coordinator of the Flink system, while the TaskManagers are the workers that execute parts of the
parallel programs. When starting the system in *local* mode, a single JobManager and TaskManager are brought
up within the same JVM.

When a program is submitted, a client is created that performs the pre-processing and turns the program
into the parallel data flow form that is executed by the JobManager and TaskManagers. The figure below
illustrates the different actors in the system and their interactions.

<div style="text-align: center;">
<img src="../fig/process_model.svg" width="100%" alt="Flink Process Model">
</div>

## Component Stack

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

You can click on the components in the figure to learn more.

<img src="../fig/overview-stack-0.9.png" width="893" height="450" alt="Stack" usemap="#overview-stack">

<map name="overview-stack">
  <area shape="rect" coords="188,0,263,200" alt="Graph API: Gelly" href="../libs/gelly_guide.html">
  <area shape="rect" coords="268,0,343,200" alt="Flink ML" href="../libs/ml/">
  <area shape="rect" coords="348,0,423,200" alt="Table" href="../libs/table.html">

  <area shape="rect" coords="188,205,538,260" alt="DataSet API (Java/Scala)" href="../apis/programming_guide.html">
  <area shape="rect" coords="543,205,893,260" alt="DataStream API (Java/Scala)" href="../apis/streaming_guide.html">

  <!-- <area shape="rect" coords="188,275,538,330" alt="Optimizer" href="optimizer.html"> -->
  <!-- <area shape="rect" coords="543,275,893,330" alt="Stream Builder" href="streambuilder.html"> -->

  <area shape="rect" coords="188,335,893,385" alt="Flink Runtime" href="general_arch.html">

  <area shape="rect" coords="188,405,328,455" alt="Local" href="../apis/local_execution.html">
  <area shape="rect" coords="333,405,473,455" alt="Remote" href="../apis/cluster_execution.html">
  <area shape="rect" coords="478,405,638,455" alt="Embedded" href="../apis/local_execution.html">
  <area shape="rect" coords="643,405,765,455" alt="YARN" href="../setup/yarn_setup.html">
  <area shape="rect" coords="770,405,893,455" alt="Tez" href="../setup/flink_on_tez.html">
</map>

## Projects and Dependencies

The Flink system code is divided into multiple sub-projects. The goal is to reduce the number of
dependencies that a project implementing a Flink progam needs, as well as to faciltate easier testing
of smaller sub-modules.

The individual projects and their dependencies are shown in the figure below.

<div style="text-align: center;">
<img src="fig/projects_dependencies.svg" alt="The Flink sub-projects and their dependencies" height="600px" style="text-align: center;"/>
</div>

In addition to the projects listed in the figure above, Flink currently contains the following sub-projects:

- `flink-dist`: The *distribution* project. It defines how to assemble the compiled code, scripts, and other resources
into the final folder structure that is ready to use.

- `flink-staging`: A series of projects that are in an early version. Currently contains
among other things projects for YARN support, JDBC data sources and sinks, hadoop compatibility,
graph specific operators, and HBase connectors.

- `flink-quickstart`: Scripts, maven archetypes, and example programs for the quickstarts and tutorials.

- `flink-contrib`: Useful tools contributed by users. The code is maintained mainly by external contributors. The requirements for code being accepted into `flink-contrib` are lower compared to the rest of the code.





