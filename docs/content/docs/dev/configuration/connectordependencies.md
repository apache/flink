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

# Adding Connector and Library Dependencies

Most applications need specific connectors or libraries to run, for example a connector to Kafka, Cassandra, etc.
These connectors are not part of Flink's core dependencies and must be added as dependencies to the application.

Below is an example adding the connector for Kafka as a dependency (Maven syntax):
```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>{{< version >}}</version>
</dependency>
```

We recommend packaging the application code and all its required dependencies into one *jar-with-dependencies* which
we refer to as the *application jar*. The application jar can be submitted to an already running Flink cluster,
or added to a Flink application container image.

Projects created from the `Java Project Template` or `Scala Project Template` are configured to automatically include
the application dependencies into the application jar when running `mvn clean package`. For projects that are
not set up from those templates, we recommend adding the Maven Shade Plugin (as listed in the Appendix below)
to build the application jar with all required dependencies.

**Important:** For Maven (and other build tools) to correctly package the dependencies into the application jar,
these application dependencies must be specified in scope *compile* (unlike the core dependencies, which
must be specified in scope *provided*).
