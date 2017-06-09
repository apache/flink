---
title: Building Flink from Source
nav-parent_id: start
nav-pos: 20
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

This page covers how to build Flink {{ site.version }} from sources.

* This will be replaced by the TOC
{:toc}

## Build Flink

In order to build Flink you need the source code. Either [download the source of a release]({{ site.download_url }}) or [clone the git repository]({{ site.github_url }}).

In addition you need **Maven 3** and a **JDK** (Java Development Kit). Flink requires **at least Java 7** to build. We recommend using Java 8.

*NOTE: Maven 3.3.x can build Flink, but will not properly shade away certain dependencies. Maven 3.0.3 creates the libraries properly.
To build unit tests with Java 8, use Java 8u51 or above to prevent failures in unit tests that use the PowerMock runner.*

To clone from git, enter:

~~~bash
git clone {{ site.github_url }}
~~~

The simplest way of building Flink is by running:

~~~bash
mvn clean install -DskipTests
~~~

This instructs [Maven](http://maven.apache.org) (`mvn`) to first remove all existing builds (`clean`) and then create a new Flink binary (`install`). The `-DskipTests` command prevents Maven from executing the tests.

The default build includes the YARN Client for Hadoop 2.

## Dependency Shading

Flink [shades away](https://maven.apache.org/plugins/maven-shade-plugin/) some of the libraries it uses, in order to avoid version clashes with user programs that use different versions of these libraries. Among the shaded libraries are *Google Guava*, *Asm*, *Apache Curator*, *Apache HTTP Components*, and others.

The dependency shading mechanism was recently changed in Maven and requires users to build Flink slightly differently, depending on their Maven version:

**Maven 3.0.x, 3.1.x, and 3.2.x**
It is sufficient to call `mvn clean install -DskipTests` in the root directory of Flink code base.

**Maven 3.3.x**
The build has to be done in two steps: First in the base directory, then in the distribution project:

~~~bash
mvn clean install -DskipTests
cd flink-dist
mvn clean install
~~~

*Note:* To check your Maven version, run `mvn --version`.

{% top %}

## Hadoop Versions

{% info %} Most users do not need to do this manually. The [download page]({{ site.download_url }}) contains binary packages for common Hadoop versions.

Flink has dependencies to HDFS and YARN which are both dependencies from [Apache Hadoop](http://hadoop.apache.org). There exist many different versions of Hadoop (from both the upstream project and the different Hadoop distributions). If you are using a wrong combination of versions, exceptions can occur.

Hadoop is only supported from version 2.3.0 upwards.
You can also specify a specific Hadoop version to build against:

~~~bash
mvn clean install -DskipTests -Dhadoop.version=2.6.1
~~~

#### Before Hadoop 2.3.0

Hadoop 2.x versions are only supported with YARN features from version 2.3.0 upwards. If you want to use a version lower than 2.3.0, you can exclude the YARN support using the following extra build arguments: `-P!include-yarn`.

For example, if you want to build Flink for Hadoop `2.2.0`, use the following command:

~~~bash
mvn clean install -Dhadoop.version=2.2.0 -P!include-yarn
~~~

### Vendor-specific Versions

To build Flink against a vendor specific Hadoop version, issue the following command:

~~~bash
mvn clean install -DskipTests -Pvendor-repos -Dhadoop.version=2.6.1-cdh5.0.0
~~~

The `-Pvendor-repos` activates a Maven [build profile](http://maven.apache.org/guides/introduction/introduction-to-profiles.html) that includes the repositories of popular Hadoop vendors such as Cloudera, Hortonworks, or MapR.

{% top %}

## Scala Versions

{% info %} Users that purely use the Java APIs and libraries can *ignore* this section.

Flink has APIs, libraries, and runtime modules written in [Scala](http://scala-lang.org). Users of the Scala API and libraries may have to match the Scala version of Flink with the Scala version of their projects (because Scala is not strictly backwards compatible).

**By default, Flink is built with the Scala 2.10**. To build Flink with Scala *2.11*, you can change the default Scala *binary version* with the following script:

~~~bash
# Switch Scala binary version between 2.10 and 2.11
tools/change-scala-version.sh 2.11
# Build with Scala version 2.11
mvn clean install -DskipTests
~~~

To build against custom Scala versions, you need to switch to the appropriate binary version and supply the *language version* as an additional build property. For example, to build against Scala 2.11.4, you have to execute:

~~~bash
# Switch Scala binary version to 2.11
tools/change-scala-version.sh 2.11
# Build with custom Scala version 2.11.4
mvn clean install -DskipTests -Dscala.version=2.11.4
~~~

Flink is developed against Scala *2.10* and tested additionally against Scala *2.11*. These two versions are known to be compatible. Earlier versions (like Scala *2.9*) are *not* compatible.

Newer versions may be compatible, depending on breaking changes in the language features used by Flink, and the availability of Flink's dependencies in those Scala versions. The dependencies written in Scala include for example *Kafka*, *Akka*, *Scalatest*, and *scopt*.

{% top %}

## Encrypted File Systems

If your home directory is encrypted you might encounter a `java.io.IOException: File name too long` exception. Some encrypted file systems, like encfs used by Ubuntu, do not allow long filenames, which is the cause of this error.

The workaround is to add:

~~~xml
<args>
    <arg>-Xmax-classfile-name</arg>
    <arg>128</arg>
</args>
~~~

in the compiler configuration of the `pom.xml` file of the module causing the error. For example, if the error appears in the `flink-yarn` module, the above code should be added under the `<configuration>` tag of `scala-maven-plugin`. See [this issue](https://issues.apache.org/jira/browse/FLINK-2003) for more information.

{% top %}

