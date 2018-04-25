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

In addition you need **Maven 3** and a **JDK** (Java Development Kit). Flink requires **at least Java 8** to build.

*NOTE: Maven 3.3.x can build Flink, but will not properly shade away certain dependencies. Maven 3.2.5 creates the libraries properly.
To build unit tests use Java 8u51 or above to prevent failures in unit tests that use the PowerMock runner.*

To clone from git, enter:

{% highlight bash %}
git clone {{ site.github_url }}
{% endhighlight %}

The simplest way of building Flink is by running:

{% highlight bash %}
mvn clean install -DskipTests
{% endhighlight %}

This instructs [Maven](http://maven.apache.org) (`mvn`) to first remove all existing builds (`clean`) and then create a new Flink binary (`install`).

To speed up the build you can skip tests, checkstyle, and JavaDocs: `mvn clean install -DskipTests -Dmaven.javadoc.skip=true -Dcheckstyle.skip=true`.

The default build adds a Flink-specific JAR for Hadoop 2, to allow using Flink with HDFS and YARN.

## Dependency Shading

Flink [shades away](https://maven.apache.org/plugins/maven-shade-plugin/) some of the libraries it uses, in order to avoid version clashes with user programs that use different versions of these libraries. Among the shaded libraries are *Google Guava*, *Asm*, *Apache Curator*, *Apache HTTP Components*, *Netty*, and others.

The dependency shading mechanism was recently changed in Maven and requires users to build Flink slightly differently, depending on their Maven version:

**Maven 3.0.x, 3.1.x, and 3.2.x**
It is sufficient to call `mvn clean install -DskipTests` in the root directory of Flink code base.

**Maven 3.3.x**
The build has to be done in two steps: First in the base directory, then in the distribution project:

{% highlight bash %}
mvn clean install -DskipTests
cd flink-dist
mvn clean install
{% endhighlight %}

*Note:* To check your Maven version, run `mvn --version`.

{% top %}

## Hadoop Versions

{% info %} Most users do not need to do this manually. The [download page]({{ site.download_url }}) contains binary packages for common Hadoop versions.

Flink has dependencies to HDFS and YARN which are both dependencies from [Apache Hadoop](http://hadoop.apache.org). There exist many different versions of Hadoop (from both the upstream project and the different Hadoop distributions). If you are using a wrong combination of versions, exceptions can occur.

Hadoop is only supported from version 2.4.0 upwards.
You can also specify a specific Hadoop version to build against:

{% highlight bash %}
mvn clean install -DskipTests -Dhadoop.version=2.6.1
{% endhighlight %}

### Vendor-specific Versions

To build Flink against a vendor specific Hadoop version, issue the following command:

{% highlight bash %}
mvn clean install -DskipTests -Pvendor-repos -Dhadoop.version=2.6.1-cdh5.0.0
{% endhighlight %}

The `-Pvendor-repos` activates a Maven [build profile](http://maven.apache.org/guides/introduction/introduction-to-profiles.html) that includes the repositories of popular Hadoop vendors such as Cloudera, Hortonworks, or MapR.

{% top %}

## Scala Versions

{% info %} Users that purely use the Java APIs and libraries can *ignore* this section.

Flink has APIs, libraries, and runtime modules written in [Scala](http://scala-lang.org). Users of the Scala API and libraries may have to match the Scala version of Flink with the Scala version of their projects (because Scala is not strictly backwards compatible).

Flink 1.4 currently builds only with Scala version 2.11.

We are working on supporting Scala 2.12, but certain breaking changes in Scala 2.12 make this a more involved effort. Please check out [this JIRA issue](https://issues.apache.org/jira/browse/FLINK-7811) for updates.

{% top %}

## Encrypted File Systems

If your home directory is encrypted you might encounter a `java.io.IOException: File name too long` exception. Some encrypted file systems, like encfs used by Ubuntu, do not allow long filenames, which is the cause of this error.

The workaround is to add:

{% highlight xml %}
<args>
    <arg>-Xmax-classfile-name</arg>
    <arg>128</arg>
</args>
{% endhighlight %}

in the compiler configuration of the `pom.xml` file of the module causing the error. For example, if the error appears in the `flink-yarn` module, the above code should be added under the `<configuration>` tag of `scala-maven-plugin`. See [this issue](https://issues.apache.org/jira/browse/FLINK-2003) for more information.

{% top %}

