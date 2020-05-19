---
title: 入门指南
nav-id: getting-started
nav-title: '<i class="fa fa-rocket title appetizer" aria-hidden="true"></i> Getting Started'
nav-parent_id: root
section-break: true
nav-show_overview: true
nav-pos: 1
always-expand: true
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

There are many ways to get started with Apache Flink. Which one is the best for
you depends on your goals and prior experience:

* take a look at the **Docker Playgrounds** if you want to see what Flink can do, via a hands-on,
  docker-based introduction to specific Flink concepts
* explore one of the **Code Walkthroughs** if you want a quick, end-to-end
  introduction to one of Flink's APIs
* work your way through the **Hands-on Training** for a comprehensive,
  step-by-step introduction to Flink
* use **Project Setup** if you already know the basics of Flink and want a
  project template for Java or Scala, or need help setting up the dependencies

### Taking a first look at Flink

The **Docker Playgrounds** provide sandboxed Flink environments that are set up in just a few minutes and which allow you to explore and play with Flink.

* The [**Operations Playground**]({% link getting-started/docker-playgrounds/flink-operations-playground.md %}) shows you how to operate streaming applications with Flink. You can experience how Flink recovers application from failures, upgrade and scale streaming applications up and down, and query application metrics.

<!--
* The [**Streaming SQL Playground**]() provides a Flink cluster with a SQL CLI client, tables which are fed by streaming data sources, and instructions for how to run continuous streaming SQL queries on these tables. This is the perfect environment for your first steps with streaming SQL.
-->

### First steps with one of Flink's APIs

The **Code Walkthroughs** are a great way to get started quickly with a step-by-step introduction to
one of Flink's APIs. Each walkthrough provides instructions for bootstrapping a small skeleton
project, and then shows how to extend it to a simple application.

* The [**DataStream API**  code walkthrough]({% link getting-started/walkthroughs/datastream_api.md %}) shows how
  to implement a simple DataStream application and how to extend it to be stateful and use timers.
  The DataStream API is Flink's main abstraction for implementing stateful streaming applications
  with sophisticated time semantics in Java or Scala.

* Flink's **Table API** is a relational API used for writing SQL-like queries in Java, Scala, or
  Python, which are then automatically optimized, and can be executed on batch or streaming data
  with identical syntax and semantics. The [Table API code walkthrough for Java and Scala]({% link
  getting-started/walkthroughs/table_api.md %}) shows how to implement a simple Table API query on a
  batch source and how to evolve it into a continuous query on a streaming source. There's also a
  similar [code walkthrough for the Python Table API]({% link
  getting-started/walkthroughs/python_table_api.md %}).

### Taking a Deep Dive with the Hands-on Training

The [**Hands-on Training**]({% link training/index.md %}) is a self-paced training course with
a set of lessons and hands-on exercises. This step-by-step introduction to Flink focuses
on learning how to use the DataStream API to meet the needs of common, real-world use cases,
and provides a complete introduction to the fundamental concepts: parallel dataflows,
stateful stream processing, event time and watermarking, and fault tolerance via state snapshots.

<!--
### Starting a new Flink application

The **Project Setup** instructions show you how to create a project for a new Flink application in just a few steps.

* [**DataStream API**]()
* [**DataSet API**]()
* [**Table API / SQL**]()
 -->
