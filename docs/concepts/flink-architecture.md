---
title: Flink Architecture
nav-id: flink-architecture
nav-pos: 4
nav-title: Flink Architecture
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

## Flink Applications and Flink Sessions

`TODO: expand this section`

{% top %}

## Anatomy of a Flink Cluster

`TODO: expand this section, especially about components of the Flink Master and
container environments`

The Flink runtime consists of two types of processes:

  - The *Flink Master* coordinates the distributed execution. It schedules
    tasks, coordinates checkpoints, coordinates recovery on failures, etc.

    There is always at least one *Flink Master*. A high-availability setup will
    have multiple *Flink Masters*, one of which one is always the *leader*, and
    the others are *standby*.

  - The *TaskManagers* (also called *workers*) execute the *tasks* (or more
    specifically, the subtasks) of a dataflow, and buffer and exchange the data
    *streams*.

    There must always be at least one TaskManager.

The Flink Master and TaskManagers can be started in various ways: directly on
the machines as a [standalone cluster]({{ site.baseurl }}{% link
ops/deployment/cluster_setup.md %}), in containers, or managed by resource
frameworks like [YARN]({{ site.baseurl }}{% link ops/deployment/yarn_setup.md
%}) or [Mesos]({{ site.baseurl }}{% link ops/deployment/mesos.md %}).
TaskManagers connect to Flink Masters, announcing themselves as available, and
are assigned work.

The *client* is not part of the runtime and program execution, but is used to
prepare and send a dataflow to the Flink Master.  After that, the client can
disconnect, or stay connected to receive progress reports. The client runs
either as part of the Java/Scala program that triggers the execution, or in the
command line process `./bin/flink run ...`.

<img src="{{ site.baseurl }}/fig/processes.svg" alt="The processes involved in executing a Flink dataflow" class="offset" width="80%" />

{% top %}
