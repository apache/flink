---
title:  "Jobs and Scheduling"
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

This document briefly describes how Flink schedules jobs and
how it represents and tracks job status on the JobManager.

* This will be replaced by the TOC
{:toc}


## Scheduling

Execution resources in Flink are defined through _Task Slots_. Each TaskManager will have one or more task slots,
each of which can run one pipeline of parallel tasks. A pipeline consists of multiple successive tasks, such as the
*n-th* parallel instance of a MapFunction together with the *n-th* parallel instance of a ReduceFunction.
Note that Flink often executes successive tasks concurrently: For Streaming programs, that happens in any case,
but also for batch programs, it happens frequently.

The figure below illustrates that. Consider a program with a data source, a *MapFunction*, and a *ReduceFunction*.
The source and MapFunction are executed with a parallelism of 4, while the ReduceFunction is executed with a
parallelism of 3. A pipeline consists of the sequence Source - Map - Reduce. On a cluster with 2 TaskManagers with
3 slots each, the program will be executed as described below.

<div style="text-align: center;">
<img src="{{ site.baseurl }}/fig/slots.svg" alt="Assigning Pipelines of Tasks to Slots" height="250px" style="text-align: center;"/>
</div>

Internally, Flink defines through {% gh_link /flink-runtime/src/main/java/org/apache/flink/runtime/jobmanager/scheduler/SlotSharingGroup.java "SlotSharingGroup" %}
and {% gh_link /flink-runtime/src/main/java/org/apache/flink/runtime/jobmanager/scheduler/CoLocationGroup.java "CoLocationGroup" %}
which tasks may share a slot (permissive), respectively which tasks must be strictly placed into the same slot.


## JobManager Data Structures

During job execution, the JobManager keeps track of distributed tasks, decides when to schedule the next task (or set of tasks),
and reacts to finished tasks or execution failures.

The JobManager receives the {% gh_link /flink-runtime/src/main/java/org/apache/flink/runtime/jobgraph/ "JobGraph" %},
which is a representation of the data flow consisting of operators ({% gh_link /flink-runtime/src/main/java/org/apache/flink/runtime/jobgraph/JobVertex.java "JobVertex" %})
and intermediate results ({% gh_link /flink-runtime/src/main/java/org/apache/flink/runtime/jobgraph/IntermediateDataSet.java "IntermediateDataSet" %}).
Each operator has properties, like the parallelism and the code that it executes.
In addition, the JobGraph has a set of attached libraries, that are necessary to execute the code of the operators.

The JobManager transforms the JobGraph into an {% gh_link /flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/ "ExecutionGraph" %}.
The ExecutionGraph is a parallel version of the JobGraph: For each JobVertex, it contains an {% gh_link /flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/ExecutionVertex.java "ExecutionVertex" %} per parallel subtask. An operator with a parallelism of 100 will have one JobVertex and 100 ExecutionVertices.
The ExecutionVertex tracks the state of execution of a particular subtask. All ExecutionVertices from one JobVertex are held in an
{% gh_link /flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/ExecutionJobVertex.java "ExecutionJobVertex" %},
which tracks the status of the operator as a whole.
Besides the vertices, the ExecutionGraph also contains the {% gh_link /flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/IntermediateResult.java "IntermediateResult" %} and the {% gh_link /flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/IntermediateResultPartition.java "IntermediateResultPartition" %}. The former tracks the state of the *IntermediateDataSet*, the latter the state of each of its partitions.

<div style="text-align: center;">
<img src="{{ site.baseurl }}/fig/job_and_execution_graph.svg" alt="JobGraph and ExecutionGraph" height="400px" style="text-align: center;"/>
</div>

Each ExecutionGraph has a job status associated with it.
This job status indicates the current state of the job execution.

A Flink job is first in the *created* state, then switches to *running* and upon completion of all work it switches to *finished*.
In case of failures, a job switches first to *failing* where it cancels all running tasks.
If all job vertices have reached a final state and the job is not restartable, then the job transitions to *failed*.
If the job can be restarted, then it will enter the *restarting* state.
Once the job has been completely restarted, it will reach the *created* state.

In case that the user cancels the job, it will go into the *cancelling* state.
This also entails the cancellation of all currently running tasks.
Once all running tasks have reached a final state, the job transitions to the state *cancelled*.

Unlike the states *finished*, *canceled* and *failed* which denote a globally terminal state and, thus, trigger the clean up of the job, the *suspended* state is only locally terminal.
Locally terminal means that the execution of the job has been terminated on the respective JobManager but another JobManager of the Flink cluster can retrieve the job from the persistent HA store and restart it.
Consequently, a job which reaches the *suspended* state won't be completely cleaned up.

<div style="text-align: center;">
<img src="{{ site.baseurl }}/fig/job_status.svg" alt="States and Transitions of Flink job" height="500px" style="text-align: center;"/>
</div>

During the execution of the ExecutionGraph, each parallel task goes through multiple stages, from *created* to *finished* or *failed*. The diagram below illustrates the
states and possible transitions between them. A task may be executed multiple times (for example in the course of failure recovery).
For that reason, the execution of an ExecutionVertex is tracked in an {% gh_link /flink-runtime/src/main/java/org/apache/flink/runtime/executiongraph/Execution.java "Execution" %}. Each ExecutionVertex has a current Execution, and prior Executions.

<div style="text-align: center;">
<img src="{{ site.baseurl }}/fig/state_machine.svg" alt="States and Transitions of Task Executions" height="300px" style="text-align: center;"/>
</div>

{% top %}
