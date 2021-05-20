---
title: Flink Architecture
weight: 4
type: docs
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

# Flink Architecture

Flink is a distributed system and requires effective allocation and management
of compute resources in order to execute streaming applications. It integrates
with all common cluster resource managers such as [Hadoop
YARN](https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html),
[Apache Mesos](https://mesos.apache.org/) and
[Kubernetes](https://kubernetes.io/), but can also be set up to run as a
standalone cluster or even as a library.

This section contains an overview of Flink’s architecture and describes how its
main components interact to execute applications and recover from failures.

## Anatomy of a Flink Cluster

The Flink runtime consists of two types of processes: a _JobManager_ and one or more _TaskManagers_.

{{< img src="/fig/processes.svg" alt="The processes involved in executing a Flink dataflow" class="offset" width="70%" >}}

The *Client* is not part of the runtime and program execution, but is used to
prepare and send a dataflow to the JobManager.  After that, the client can
disconnect (_detached mode_), or stay connected to receive progress reports
(_attached mode_). The client runs either as part of the Java/Scala program
that triggers the execution, or in the command line process `./bin/flink run
...`.

The JobManager and TaskManagers can be started in various ways: directly on
the machines as a [standalone cluster]({{< ref "docs/deployment/resource-providers/standalone/overview" >}}), in containers, or managed by resource
frameworks like [YARN]({{< ref "docs/deployment/resource-providers/yarn" >}})
or [Mesos]({{< ref "docs/deployment/resource-providers/mesos" >}}).
TaskManagers connect to JobManagers, announcing themselves as available, and
are assigned work.

### JobManager

The _JobManager_ has a number of responsibilities related to coordinating the distributed execution of Flink Applications:
it decides when to schedule the next task (or set of tasks), reacts to finished
tasks or execution failures, coordinates checkpoints, and coordinates recovery on
failures, among others. This process consists of three different components:

  * **ResourceManager** 

    The _ResourceManager_ is responsible for resource de-/allocation and
    provisioning in a Flink cluster — it manages **task slots**, which are the
    unit of resource scheduling in a Flink cluster (see [TaskManagers](#taskmanagers)).
    Flink implements multiple ResourceManagers for different environments and
    resource providers such as YARN, Mesos, Kubernetes and standalone
    deployments. In a standalone setup, the ResourceManager can only distribute
    the slots of available TaskManagers and cannot start new TaskManagers on
    its own.  

  * **Dispatcher** 

    The _Dispatcher_ provides a REST interface to submit Flink applications for
    execution and starts a new JobMaster for each submitted job. It
    also runs the Flink WebUI to provide information about job executions.

  * **JobMaster** 

    A _JobMaster_ is responsible for managing the execution of a single
    [JobGraph]({{< ref "docs/concepts/glossary" >}}#logical-graph).
    Multiple jobs can run simultaneously in a Flink cluster, each having its
    own JobMaster.

There is always at least one JobManager. A high-availability setup might have
multiple JobManagers, one of which is always the *leader*, and the others are
*standby* (see [High Availability (HA)]({{< ref "docs/deployment/ha/overview" >}})).

### TaskManagers

The *TaskManagers* (also called *workers*) execute the tasks of a dataflow, and buffer and exchange the data
streams.

There must always be at least one TaskManager. The smallest unit of resource scheduling in a TaskManager is a task _slot_. The number of task slots in a
TaskManager indicates the number of concurrent processing tasks. Note that
multiple operators may execute in a task slot (see [Tasks and Operator
Chains](#tasks-and-operator-chains)).

{{< top >}}

## Tasks and Operator Chains

For distributed execution, Flink *chains* operator subtasks together into
*tasks*. Each task is executed by one thread.  Chaining operators together into
tasks is a useful optimization: it reduces the overhead of thread-to-thread
handover and buffering, and increases overall throughput while decreasing
latency.  The chaining behavior can be configured; see the [chaining docs]({{< ref "docs/dev/datastream/operators/overview" >}}#task-chaining-and-resource-groups) for details.

The sample dataflow in the figure below is executed with five subtasks, and
hence with five parallel threads.

{{< img src="/fig/tasks_chains.svg" alt="Operator chaining into Tasks" class="offset" width="80%" >}}

{{< top >}}

## Task Slots and Resources

Each worker (TaskManager) is a *JVM process*, and may execute one or more
subtasks in separate threads.  To control how many tasks a TaskManager accepts, it
has so called **task slots** (at least one).

Each *task slot* represents a fixed subset of resources of the TaskManager. A
TaskManager with three slots, for example, will dedicate 1/3 of its managed
memory to each slot. Slotting the resources means that a subtask will not
compete with subtasks from other jobs for managed memory, but instead has a
certain amount of reserved managed memory. Note that no CPU isolation happens
here; currently slots only separate the managed memory of tasks.

By adjusting the number of task slots, users can define how subtasks are
isolated from each other.  Having one slot per TaskManager means that each task
group runs in a separate JVM (which can be started in a separate container, for
example). Having multiple slots means more subtasks share the same JVM. Tasks
in the same JVM share TCP connections (via multiplexing) and heartbeat
messages. They may also share data sets and data structures, thus reducing the
per-task overhead.

{{< img src="/fig/tasks_slots.svg" alt="A TaskManager with Task Slots and Tasks" class="offset" width="80%" >}}

By default, Flink allows subtasks to share slots even if they are subtasks of
different tasks, so long as they are from the same job. The result is that one
slot may hold an entire pipeline of the job. Allowing this *slot sharing* has
two main benefits:

  - A Flink cluster needs exactly as many task slots as the highest parallelism
    used in the job.  No need to calculate how many tasks (with varying
    parallelism) a program contains in total.

  - It is easier to get better resource utilization. Without slot sharing, the
    non-intensive *source/map()* subtasks would block as many resources as the
    resource intensive *window* subtasks.  With slot sharing, increasing the
    base parallelism in our example from two to six yields full utilization of
    the slotted resources, while making sure that the heavy subtasks are fairly
    distributed among the TaskManagers.

{{< img src="/fig/slot_sharing.svg" alt="TaskManagers with shared Task Slots" class="offset" width="80%" >}}

## Flink Application Execution

A _Flink Application_ is any user program that spawns one or multiple Flink
jobs from its ``main()`` method. The execution of these jobs can happen in a
local JVM (``LocalEnvironment``) or on a remote setup of clusters with multiple
machines (``RemoteEnvironment``). For each program, the `ExecutionEnvironment`
provides methods to control the job execution (e.g. setting the parallelism) and to interact with
the outside world (see [Anatomy of a Flink Program]({{< ref "docs/dev/datastream/overview" >}}#anatomy-of-a-flink-program)).

The jobs of a Flink Application can either be submitted to a long-running
[Flink Session Cluster]({{< ref "docs/concepts/glossary" >}}#flink-session-cluster), a dedicated [Flink Job
Cluster]({{< ref "docs/concepts/glossary" >}}#flink-job-cluster), or a
[Flink Application Cluster]({{< ref "docs/concepts/glossary" >}}#flink-application-cluster). The difference between these options is mainly related to the cluster’s lifecycle and to resource
isolation guarantees.

### Flink Session Cluster

* **Cluster Lifecycle**: in a Flink Session Cluster, the client connects to a
  pre-existing, long-running cluster that can accept multiple job submissions.
  Even after all jobs are finished, the cluster (and the JobManager) will
  keep running until the session is manually stopped. The lifetime of a Flink
  Session Cluster is therefore not bound to the lifetime of any Flink Job.

* **Resource Isolation**: TaskManager slots are allocated by the
  ResourceManager on job submission and released once the job is finished.
  Because all jobs are sharing the same cluster, there is some competition for
  cluster resources — like network bandwidth in the submit-job phase. One
  limitation of this shared setup is that if one TaskManager crashes, then all
  jobs that have tasks running on this TaskManager will fail; in a similar way, if
  some fatal error occurs on the JobManager, it will affect all jobs running
  in the cluster.

* **Other considerations**: having a pre-existing cluster saves a considerable
  amount of time applying for resources and starting TaskManagers. This is
  important in scenarios where the execution time of jobs is very short and a
  high startup time would negatively impact the end-to-end user experience — as
  is the case with interactive analysis of short queries, where it is desirable
  that jobs can quickly perform computations using existing resources.

{{< hint info >}}
Formerly, a Flink Session Cluster was also known as a Flink Cluster in `session mode`.
{{< /hint >}}

### Flink Job Cluster

* **Cluster Lifecycle**: in a Flink Job Cluster, the available cluster manager
  (like YARN) is used to spin up a cluster for each submitted job
  and this cluster is available to that job only. Here, the client first
  requests resources from the cluster manager to start the JobManager and
  submits the job to the Dispatcher running inside this process. TaskManagers
  are then lazily allocated based on the resource requirements of the job. Once
  the job is finished, the Flink Job Cluster is torn down.

* **Resource Isolation**: a fatal error in the JobManager only affects the one job running in that Flink Job Cluster.

* **Other considerations**: because the ResourceManager has to apply and wait
  for external resource management components to start the TaskManager
  processes and allocate resources, Flink Job Clusters are more suited to large
  jobs that are long-running, have high-stability requirements and are not
  sensitive to longer startup times.

{{< hint info >}}
Formerly, a Flink Job Cluster was also known as a Flink Cluster in `job (or per-job) mode`.
{{< /hint >}}
{{< hint info >}}
Kubernetes doesn't support Flink Job Cluster. See details in [Standalone Kubernetes]({{< ref "docs/deployment/resource-providers/standalone/kubernetes" >}}#per-job-cluster-mode) and [Native Kubernetes]({{< ref "docs/deployment/resource-providers/native_kubernetes" >}}#per-job-cluster-mode).
{{< /hint >}}

### Flink Application Cluster

* **Cluster Lifecycle**: a Flink Application Cluster is a dedicated Flink
  cluster that only executes jobs from one Flink Application and where the
  ``main()`` method runs on the cluster rather than the client. The job
  submission is a one-step process: you don’t need to start a Flink cluster
  first and then submit a job to the existing cluster session; instead, you
  package your application logic and dependencies into a executable job JAR and
  the cluster entrypoint (``ApplicationClusterEntryPoint``)
  is responsible for calling the ``main()`` method to extract the JobGraph.
  This allows you to deploy a Flink Application like any other application on
  Kubernetes, for example. The lifetime of a Flink Application Cluster is
  therefore bound to the lifetime of the Flink Application.

* **Resource Isolation**: in a Flink Application Cluster, the ResourceManager
  and Dispatcher are scoped to a single Flink Application, which provides a
  better separation of concerns than the Flink Session Cluster.

{{< hint info >}}
A Flink Job Cluster can be seen as a “run-on-client” alternative to Flink Application Clusters.
{{< /hint >}}

{{< top >}}
