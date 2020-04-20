---
title: "Getting Started"
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
you depends on your goal and prior experience:

* take a look at the **Docker Playgrounds** for a docker-based introduction to
  specific Flink concepts
* explore on of the **Code Walkthroughs** if you want to get an end-to-end
  introduction to using one of the Flink APIs
* use **Project Setup** if you already know the basics of Flink but want to get a
  project setup template for Java or Scala and need help setting up
  dependencies

### Taking a first look at Flink

The **Docker Playgrounds** provide sandboxed Flink environments that are set up in just a few minutes and which allow you to explore and play with Flink.

* The [**Operations Playground**](./docker-playgrounds/flink-operations-playground.html) shows you how to operate streaming applications with Flink. You can experience how Flink recovers application from failures, upgrade and scale streaming applications up and down, and query application metrics.

<!-- 
* The [**Streaming SQL Playground**]() provides a Flink cluster with a SQL CLI client, tables which are fed by streaming data sources, and instructions for how to run continuous streaming SQL queries on these tables. This is the perfect environment for your first steps with streaming SQL. 
-->

### First steps with one of Flink's APIs

The **Code Walkthroughs** are the best way to get started and introduce you step by step to an API.
A walkthrough provides instructions to bootstrap a small Flink project with a code skeleton and shows how to extend it to a simple application.

* The [**DataStream API**](./walkthroughs/datastream_api.html) code walkthrough shows how to implement a simple DataStream application and how to extend it to be stateful and use timers. The DataStream API is Flink's main abstraction to implement stateful streaming applications with sophisticated time semantics in Java or Scala.

* The [**Table API**](./walkthroughs/table_api.html) code walkthrough shows how to implement a simple Table API query on a batch source and how to evolve it into a continuous query on a streaming source. The Table API Flink's language-embedded, relational API to write SQL-like queries in Java or Scala which are automatically optimized similar to SQL queries. Table API queries can be executed on batch or streaming data with identical syntax and semantics.

<!-- 
### Starting a new Flink application

The **Project Setup** instructions show you how to create a project for a new Flink application in just a few steps.

* [**DataStream API**]()
* [**DataSet API**]()
* [**Table API / SQL**]() 
 -->
