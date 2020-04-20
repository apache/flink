---
title: Stream Processing
nav-id: stream-processing
nav-pos: 1
nav-title: Stream Processing
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

`TODO: Add introduction`
* This will be replaced by the TOC
{:toc}

## A Unified System for Batch & Stream Processing

`TODO`

{% top %}

## Programs and Dataflows

The basic building blocks of Flink programs are **streams** and
**transformations**. Conceptually a *stream* is a (potentially never-ending)
flow of data records, and a *transformation* is an operation that takes one or
more streams as input, and produces one or more output streams as a result.

When executed, Flink programs are mapped to **streaming dataflows**, consisting
of **streams** and transformation **operators**. Each dataflow starts with one
or more **sources** and ends in one or more **sinks**. The dataflows resemble
arbitrary **directed acyclic graphs** *(DAGs)*. Although special forms of
cycles are permitted via *iteration* constructs, for the most part we will
gloss over this for simplicity.

<img src="{{ site.baseurl }}/fig/program_dataflow.svg" alt="A DataStream program, and its dataflow." class="offset" width="80%" />

Often there is a one-to-one correspondence between the transformations in the
programs and the operators in the dataflow. Sometimes, however, one
transformation may consist of multiple operators.

{% top %}

## Parallel Dataflows

Programs in Flink are inherently parallel and distributed. During execution, a
*stream* has one or more **stream partitions**, and each *operator* has one or
more **operator subtasks**. The operator subtasks are independent of one
another, and execute in different threads and possibly on different machines or
containers.

The number of operator subtasks is the **parallelism** of that particular
operator. The parallelism of a stream is always that of its producing operator.
Different operators of the same program may have different levels of
parallelism.

<img src="{{ site.baseurl }}/fig/parallel_dataflow.svg" alt="A parallel dataflow" class="offset" width="80%" />

Streams can transport data between two operators in a *one-to-one* (or
*forwarding*) pattern, or in a *redistributing* pattern:

  - **One-to-one** streams (for example between the *Source* and the *map()*
    operators in the figure above) preserve the partitioning and ordering of
    the elements. That means that subtask[1] of the *map()* operator will see
    the same elements in the same order as they were produced by subtask[1] of
    the *Source* operator.

  - **Redistributing** streams (as between *map()* and *keyBy/window* above, as
    well as between *keyBy/window* and *Sink*) change the partitioning of
    streams. Each *operator subtask* sends data to different target subtasks,
    depending on the selected transformation. Examples are *keyBy()* (which
    re-partitions by hashing the key), *broadcast()*, or *rebalance()* (which
    re-partitions randomly). In a *redistributing* exchange the ordering among
    the elements is only preserved within each pair of sending and receiving
    subtasks (for example, subtask[1] of *map()* and subtask[2] of
    *keyBy/window*). So in this example, the ordering within each key is
    preserved, but the parallelism does introduce non-determinism regarding the
    order in which the aggregated results for different keys arrive at the
    sink.

{% top %}
