---
title: "Overview"
weight: 1
type: docs
aliases:
- /dev/project-configuration.html
- /start/dependencies.html
- /getting-started/project-setup/dependencies.html
- /quickstart/java_api_quickstart.html
- /dev/projectsetup/java_api_quickstart.html
- /dev/linking_with_flink.html
- /dev/linking.html
- /dev/projectsetup/dependencies.html
- /dev/projectsetup/java_api_quickstart.html
- /getting-started/project-setup/java_api_quickstart.html
- /dev/getting-started/project-setup/scala_api_quickstart.html
- /getting-started/project-setup/scala_api_quickstart.html
- /quickstart/scala_api_quickstart.html
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

# Project Configuration

Every Flink application depends on a set of Flink libraries. At a minimum, the application depends
on the Flink APIs and, in addition, on certain connector libraries (i.e. Kafka, Cassandra).
When running Flink applications (either in a distributed deployment or in the IDE for testing),
the Flink runtime library must be available.

## Setting up a Flink project: Getting started

Every Flink application needs, at a minimum, the API dependencies to develop against. When setting up
a project manually, you need to add the following dependencies for the Java/Scala API.

In Maven syntax, it would look like:

{{< tabs "a49d57a4-27ee-4dd3-a2b8-a673b99b011e" >}}
{{< tab "Java" >}}
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-java</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
```
{{< /tab >}}
{{< tab "Scala" >}}
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
```
{{< /tab >}}
{{< /tabs >}}

**Important:** Note that all these dependencies have their scope set to *provided*. This means that
they are needed to compile against, but that they should not be packaged into the project's resulting
application JAR file. If not set to *provided*, the best case scenario is that the resulting JAR
becomes excessively large, because it also contains all Flink core dependencies. The worst case scenario
is that the Flink core dependencies that are added to the application's JAR file clash with some of
your own dependency versions (which is normally avoided through inverted classloading).

**Note on IntelliJ:** To make the applications run within IntelliJ IDEA, it is necessary to tick the
`Include dependencies with "Provided" scope` box in the run configuration. If this option is not available
(possibly due to using an older IntelliJ IDEA version), then a workaround is to create a test that
calls the application's `main()` method.

## Which dependencies do you need?

| APIs you want to use              | Dependency you need to add    |
|-----------------------------------|-------------------------------|
| DataStream                        | flink-streaming-java          |  
| DataStream with Scala             | flink-streaming-scala         |   
| Table API                         | flink-table-api-java          |   
| Table API with Scala              | flink-table-api-scala         |
| Table API + DataStream            | flink-table-api-java-bridge   |
| Table API + DataStream with Scala | flink-table-api-scala-bridge  |
