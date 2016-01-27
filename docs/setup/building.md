---
title: Building Flink
top-nav-group: setup
top-nav-pos: 1
top-nav-title: Build Flink
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

{% top %}

## Hadoop Versions

{% info %} Most users do not need to do this manually. The [download page]({{ site.download_url }})  contains binary packages for common Hadoop versions.

Flink has dependencies to HDFS and YARN which are both dependencies from [Apache Hadoop](http://hadoop.apache.org). There exist many different versions of Hadoop (from both the upstream project and the different Hadoop distributions). If you are using a wrong combination of versions, exceptions can occur.

There are two main versions of Hadoop that we need to differentiate:
- **Hadoop 1**, with all versions starting with zero or one, like *0.20*, *0.23* or *1.2.1*.
- **Hadoop 2**, with all versions starting with 2, like *2.6.0*.

The main differentiation between Hadoop 1 and Hadoop 2 is the availability of [Hadoop YARN](https://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html), Hadoop's cluster resource manager.

**By default, Flink is using the Hadoop 2 dependencies**.

### Hadoop 1

To build Flink for Hadoop 1, issue the following command:

~~~bash
mvn clean install -DskipTests -Dhadoop.profile=1
~~~

The `-Dhadoop.profile=1` flag instructs Maven to build Flink for Hadoop 1. Note that the features included in Flink change when using a different Hadoop profile. In particular, there is no support for YARN and HBase in Hadoop 1 builds.

### Hadoop 2

You can also specify a specific Hadoop version to build against:

~~~bash
mvn clean install -DskipTests -Dhadoop.version=2.4.1
~~~

#### Before Hadoop 2.2.0

Maven will automatically build Flink with its YARN client. The 2.2.0 Hadoop release is *not* supported by Flink's YARN client. Therefore, you need to exclude the YARN client with the following string: `-P!include-yarn`.

So if you are building Flink for Hadoop `2.0.0-alpha`, use the following command:

~~~bash
mvn clean install -P!include-yarn -Dhadoop.version=2.0.0-alpha
~~~

### Vendor-specific Versions

To build Flink against a vendor specific Hadoop version, issue the following command:

~~~bash
mvn clean install -DskipTests -Pvendor-repos -Dhadoop.version=2.2.0-cdh5.0.0-beta-2
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

## Internals

The builds with Maven are controlled by [properties](http://maven.apache.org/pom.html#Properties) and [build profiles](http://maven.apache.org/guides/introduction/introduction-to-profiles.html). There are two profiles, one for `hadoop1` and one for `hadoop2`. When the `hadoop2` profile is enabled (default), the system will also build the YARN client.

To enable the `hadoop1` profile, set `-Dhadoop.profile=1` when building. Depending on the profile, there are two Hadoop versions, set via properties. For `hadoop1`, we use 1.2.1 by default, for `hadoop2` it is 2.3.0.

You can change these versions with the `hadoop-two.version` (or `hadoop-one.version`) property. For example `-Dhadoop-two.version=2.4.0`.

{% top %}
