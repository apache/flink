---
title: "Application Lifecycle"
weight: 9
type: docs
aliases:
  - /internals/application_lifecycle.html
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

# Application Lifecycle

An application in Flink represents a piece of user-defined logic for execution. 
It provides a unified abstraction for tracking the execution status of user `main()` methods and managing their associated jobs.
For more details, see [FLIP-549](https://cwiki.apache.org/confluence/display/FLINK/FLIP-549%3A+Support+Application+Management)
and [FLIP-560](https://cwiki.apache.org/confluence/display/FLINK/FLIP-560%3A+Application+Capability+Enhancement).

## Cluster-Application-Job Architecture

Flink now uses a three-tier structure: **Cluster-Application-Job**. This structure unifies different deployment modes and provides 
observability and manageability of user logic execution.

A cluster can operate in two modes:
- **Application Mode**: One cluster per application
- **Session Mode**: One cluster for multiple applications

An application can contain 0 to N jobs, with each job associated with exactly one application.

## Application Implementations

The {{< gh_link file="/flink-runtime/src/main/java/org/apache/flink/runtime/application/AbstractApplication.java" name="AbstractApplication" >}} is the base class for all applications. Flink provides two concrete implementations:

- **PackagedProgramApplication**: Wraps a user JAR and executes its `main()` method. Suitable for **Application Mode** or **Session Mode with REST submission via `/jars/:jarid/run-application`**. The application's lifecycle is tied to the execution of the user's `main()` method.

- **SingleJobApplication**: Wraps the submission of a single job as a lightweight `main()` method. Suitable for cases where a single job is submitted, such as **Session Mode with CLI submission**. The application lifecycle is tied to the job's execution status.

## Application Status

An application starts in the *created* state, then switches to *running* once execution begins. 
When the execution completes normally and all jobs associated with the application have reached a terminal state, the application transitions to *finished*.
In case of failures, an application switches first to *failing* where it cancels all its non-terminal jobs. After all jobs have reached a terminal state, the application transitions to *failed*.

In case that the user cancels the application, it will go into the *canceling* state.
This also entails the cancellation of all its non-terminal jobs.
Once all jobs have reached a terminal state, the application transitions to the state *canceled*.

The states *finished*, *canceled*, and *failed* are terminal states and trigger archiving and cleanup operations for the application.

{{< img src="/fig/application_status.png" alt="States and Transitions of Application" width="50%" >}}

## Application Submission

Applications are submitted to the cluster and start to execute through different mechanisms depending on the deployment mode and submission method.

In **Application Mode**, the cluster is started specifically for a single application. During cluster startup, a `PackagedProgramApplication` is automatically generated from the user's JAR file. The application begins executing immediately after the cluster is ready, with the lifecycle tied to the execution of the user's `main()` method.

In **Session Mode**, multiple applications can share the same cluster. Applications can be submitted through various interfaces:

- **REST API (`/jars/:jarid/run-application`)**: This endpoint creates a `PackagedProgramApplication` from the uploaded user JAR and begins execution. Like Application Mode, the application's lifecycle is tied to the user's `main()` method.

- **REST API (`/jars/:jarid/run`)** and **CLI submission**: These interfaces directly execute the user's `main()` method. When the method calls `execute()` to submit a job, the job is wrapped as a `SingleJobApplication`. In this case, the application lifecycle is tied to the job's execution status, making it a lightweight wrapper for single-job submissions.

The {{< gh_link file="/flink-runtime/src/main/java/org/apache/flink/runtime/dispatcher/Dispatcher.java" name="Dispatcher" >}} manages all applications in the cluster. It provides interfaces for querying application status, managing application lifecycle, and handling operations such as cancellation and recovery.

{{< top >}}
