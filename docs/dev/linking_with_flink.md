---
title: "Linking with Flink"
nav-parent_id: start
nav-pos: 2
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

To write programs with Flink, you need to include the Flink library corresponding to
your programming language in your project.

The simplest way to do this is to use one of the quickstart scripts: either for
[Java]({{ site.baseurl }}/quickstart/java_api_quickstart.html) or for [Scala]({{ site.baseurl }}/quickstart/scala_api_quickstart.html). They
create a blank project from a template (a Maven Archetype), which sets up everything for you. To
manually create the project, you can use the archetype and create a project by calling:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight bash %}
mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-quickstart-java \
    -DarchetypeVersion={{site.version }}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight bash %}
mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-quickstart-scala \
    -DarchetypeVersion={{site.version }}
{% endhighlight %}
</div>
</div>

The archetypes are working for stable releases and preview versions (`-SNAPSHOT`).

If you want to add Flink to an existing Maven project, add the following entry to your
*dependencies* section in the *pom.xml* file of your project:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight xml %}
<!-- Use this dependency if you are using the DataStream API -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-java{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
<!-- Use this dependency if you are using the DataSet API -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-java</artifactId>
  <version>{{site.version }}</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight xml %}
<!-- Use this dependency if you are using the DataStream API -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
<!-- Use this dependency if you are using the DataSet API -->
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

**Important:** When working with the Scala API you must have one of these two imports:
{% highlight scala %}
import org.apache.flink.api.scala._
{% endhighlight %}

or

{% highlight scala %}
import org.apache.flink.api.scala.createTypeInformation
{% endhighlight %}

The reason is that Flink analyzes the types that are used in a program and generates serializers
and comparaters for them. By having either of those imports you enable an implicit conversion
that creates the type information for Flink operations.

If you would rather use SBT, see [here]({{ site.baseurl }}/quickstart/scala_api_quickstart.html#sbt).
</div>
</div>

#### Scala Dependency Versions

Because Scala 2.10 binary is not compatible with Scala 2.11 binary, we provide multiple artifacts
to support both Scala versions.

Starting from the 0.10 line, we cross-build all Flink modules for both 2.10 and 2.11. If you want
to run your program on Flink with Scala 2.11, you need to add a `_2.11` suffix to the `artifactId`
values of the Flink modules in your dependencies section.

If you are looking for building Flink with Scala 2.11, please check
[build guide]({{ site.baseurl }}/start/building.html#scala-versions).

#### Hadoop Dependency Versions

If you are using Flink together with Hadoop, the version of the dependency may vary depending on the
version of Hadoop (or more specifically, HDFS) that you want to use Flink with. Please refer to the
[downloads page](http://flink.apache.org/downloads.html) for a list of available versions, and instructions
on how to link with custom versions of Hadoop.

In order to link against the latest SNAPSHOT versions of the code, please follow
[this guide](http://flink.apache.org/how-to-contribute.html#snapshots-nightly-builds).

The *flink-clients* dependency is only necessary to invoke the Flink program locally (for example to
run it standalone for testing and debugging).  If you intend to only export the program as a JAR
file and [run it on a cluster]({{ site.baseurl }}/dev/cluster_execution.html), you can skip that dependency.

{% top %}

