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

# Connectors and Formats

Flink can read from and write to various external systems via connectors and define the format in 
which to store the data.

The way that information is serialized is represented in the external system and that system needs
to know how to read this data in a format that can be read by Flink.  This is done through format 
dependencies.

Most applications need specific connectors to run. Flink provides a set of formats that can be used 
with connectors (with the dependencies for both being fairly unified). These are not part of Flink's 
core dependencies and must be added as dependencies to the application.

## Adding Dependencies 

For more information on how to add dependencies, refer to the build tools sections on [Maven]({{< ref "docs/dev/configuration/maven" >}})
and [Gradle]({{< ref "docs/dev/configuration/gradle" >}}). 

## Packaging Dependencies

We recommend packaging the application code and all its required dependencies into one fat/uber JAR. 
This job JAR can be submitted to an already running Flink cluster, or added to a Flink application 
container image.

On [Maven Central](https://search.maven.org), we publish connectors named "flink-connector-<NAME>" and
"flink-sql-connector-<NAME>". The former are thin JARs while the latter are uber JARs.

In order to use the uber JARs, you can shade them (including and renaming dependencies to create a 
private copy) in the uber JAR of your Flink job, or you can add them to the `/lib` folder of the 
distribution.

If you shade a dependency, you will have more control over the dependency version in the job JAR. 
In case of shading the thin JAR, you will have even more control over the transitive dependencies, 
since you can change the versions without changing the connector version (binary compatibility permitting).

If you include uber JARs directly in the distribution, this can simplify the management of dependencies 
in a shared multi-job Flink cluster, but it also means that you will lock in a specific version of the 
dependency.
