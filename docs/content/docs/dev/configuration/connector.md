---
title: "Dependencies: Connectors and Formats"
weight: 5
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

# Dependencies: Connectors and Formats

Flink can read from and write to various external systems via [connectors]({{< ref "docs/connectors/table/overview" >}})
and define the [format]({{< ref "docs/connectors/table/formats/overview" >}}) in which to store the 
data (i.e. mapping binary data onto table columns).  

The way that the information is serialized is represented in the external system and that system needs
to know how to read this data in a format that can be read by Flink.  This is done through format dependencies.

Most applications need specific connectors to run. Flink provides a set of table formats that can be 
used with table connectors (with the dependencies for both being fairly unified). These are not part 
of Flink's core dependencies and must be added as dependencies to the application.

## Adding Connector Dependencies 

As an example, you can add the Kafka connector as a dependency like this (Maven syntax):

{{< artifact flink-connector-kafka >}}

We recommend packaging the application code and all its required dependencies into one *JAR-with-dependencies* 
which we refer to as the *application JAR*. The application JAR can be submitted to an already running 
Flink cluster, or added to a Flink application container image.

Projects created from the `Java Project Template`, the `Scala Project Template`, or Gradle are configured 
to automatically include the application dependencies into the application JAR when you run `mvn clean package`. 
For projects that are not set up from those templates, we recommend adding the Maven Shade Plugin to 
build the application jar with all required dependencies.

**Important:** For Maven (and other build tools) to correctly package the dependencies into the application jar,
these application dependencies must be specified in scope *compile* (unlike the core dependencies, which
must be specified in scope *provided*).

## Packaging Dependencies

In the Maven Repository, you will find connectors named "flink-connector-<NAME>" and
"flink-sql-connector-<NAME>". The former are thin JARs while the latter are uber JARs.

In order to use the uber JARs, you can shade them in the uber JAR of your application, or you can add
them to the `/lib` folder of the distribution (i.e. SQL client).

[ EXPLAIN PROS and CONS ]

In order to create an uber JAR to run the job, do this:

[ FILL IN ]

**Note:** You do not need to shade Flink API dependencies. You only need to do this for connectors,
formats and third-party dependencies.
