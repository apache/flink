---
title:  "General Architecture and Process Model"
---

## The Processes

When the Flink system is started, it bring up the *JobManager* and one or more *TaskManagers*. The JobManager
is the coordinator of the Flink system, while the TaskManagers are the worksers that execute parts of the
parallel programs. When starting the systen in *local* mode, a single JobManager and TaskManager are brought
up within the same JVM.

When a program is submitted, a client is created that performs the pre-processing and turns the program
into the parallel data flow form that is executed by the JobManager and TaskManagers. The figure below
illustrates the different actors in the system very coarsely.

<div style="text-align: center;">
<img src="ClientJmTm.svg" alt="The Interactions between Client, JobManager and TaskManager" height="400px" style="text-align: center;"/>
</div>

## Component Stack

An alternative view on the system is given by the stack below. The different layers of the stack build on
top of each other and raise the abstraction level of the program representations they accept:

- The **runtime** layer receive a program in the form of a *JobGraph*. A JobGraph is a generic parallel
data flow with arbitrary tasks that consume and produce data streams.

- The **optimizer** and **common api** layer takes programs in the form of operator DAGs. The operators are
specific (e.g., Map, Join, Filter, Reduce, ...), but are data type agnostic. The concrete types and their
interaction with the runtime is specified by the higher layers.

- The **API layer** implements multiple APIs that create operator DAGs for their programs. Each API needs
to provide utilities (serializers, comparators) that describe the interaction between its data types and
the runtime.

<div style="text-align: center;">
<img src="stack.svg" alt="The Flink component stack" width="800px" />
</div>

## Projects and Dependencies

The Flink system code is divided into multiple sub-projects. The goal is to reduce the number of
dependencies that a project implementing a Flink progam needs, as well as to faciltate easier testing
of smaller sub-modules.

The individual projects and their dependencies are shown in the figure below.

<div style="text-align: center;">
<img src="projects_dependencies.svg" alt="The Flink sub-projects and their dependencies" height="600px" style="text-align: center;"/>
</div>

In addition to the projects listed in the figure above, Flink currently contains the following sub-projects:

- `flink-dist`: The *distribution* project. It defines how to assemble the compiled code, scripts, and other resources
into the final folder structure that is ready to use.

- `flink-addons`: A series of projects that are in an early version. Currently contains
among other things projects for YARN support, JDBC data sources and sinks, hadoop compatibility,
graph specific operators, and HBase connectors.

- `flink-quickstart`: Scripts, maven archetypes, and example programs for the quickstarts and tutorials.






