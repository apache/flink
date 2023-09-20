---
title: Java Compatibility
weight: 2
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

# Java compatibility

This page lists which Java versions Flink supports and what limitations apply (if any).

## Java 8 (deprecated)

Support for Java 8 has been deprecated in 1.15.0.
It is recommended to migrate to Java 11.

## Java 11

Support for Java 11 was added in 1.10.0 and is the recommended Java version to run Flink on.

This is the default version for docker images.

### Untested Flink features

The following Flink features have not been tested with Java 11:

* Hive connector
* Hbase 1.x connector

### Untested language features

* Modularized user jars have not been tested.

## Java 17

Experimental support for Java 17 was added in 1.18.0. ([FLINK-15736](https://issues.apache.org/jira/browse/FLINK-15736))

### Untested Flink features

These Flink features have not been tested with Java 17:

* Hive connector
* Hbase 1.x connector

### JDK modularization

Starting with Java 16 Java applications have to fully cooperate with the JDK modularization, also known as [Project Jigsaw](https://openjdk.org/projects/jigsaw/).
This means that access to JDK classes/internal must be explicitly allowed by the application when it is started, on a per-module basis, in the form of --add-opens/--add-exports JVM arguments.

Since Flink uses reflection for serializing user-defined functions and data (via Kryo), this means that if your UDFs or data types use JDK classes you may have to allow access to these JDK classes.

These should be configured via the [env.java.opts.all]({{< ref "docs/deployment/config" >}}#env-java-opts-all) option.

In the default configuration in the Flink distribution this option is configured such that Flink itself works on Java 17.  
The list of configured arguments must not be shortened, but only extended.

### Known issues

* Java records are not supported. See [FLINK-32380](https://issues.apache.org/jira/browse/FLINK-32380) for updates.
* SIGSEGV in C2 Compiler thread: Early Java 17 builds are affected by a bug where the JVM can fail abruptly. Update your Java 17 installation to resolve the issue. See [JDK-8277529](https://bugs.openjdk.org/browse/JDK-8277529) for details.
