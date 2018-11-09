---
title: "Configuring Dependencies, Connectors, Libraries"
nav-parent_id: projectsetup
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

Every Flink application depends on a set of Flink libraries. At the bare minimum, the application depends
on the Flink APIs. Many applications depend in addition on certain connector libraries (like Kafka, Cassandra, etc.).
When running Flink applications (either in a distributed deployment, or in the IDE for testing), the Flink
runtime library must be available as well.


## Flink Core and Application Dependencies

As with most systems that run user-defined applications, there are two broad categories of dependencies and libraries in Flink:

  - **Flink Core Dependencies**: Flink itself consists of a set of classes and dependencies that are needed to run the system, for example
    coordination, networking, checkpoints, failover, APIs, operations (such as windowing), resource management, etc.
    The set of all these classes and dependencies forms the core of Flink's runtime and must be present when a Flink
    application is started.

    These core classes and dependencies are packaged in the `flink-dist` jar. They are part of Flink's `lib` folder and
    part of the basic Flink container images. Think of these dependencies as similar to Java's core library (`rt.jar`, `charsets.jar`, etc.),
    which contains the classes like `String` and `List`.

    The Flink Core Dependencies do not contain any connectors or libraries (CEP, SQL, ML, etc.) in order to avoid having an excessive
    number of dependencies and classes in the classpath by default. In fact, we try to keep the core dependencies as slim as possible
    to keep the default classpath small and avoid dependency clashes.

  - The **User Application Dependencies** are all connectors, formats, or libraries that a specific user application needs.

    The user application is typically packaged into an *application jar*, which contains the application code and the required
    connector and library dependencies.

    The user application dependencies explicitly do not include the Flink DataSet / DataStream APIs and runtime dependencies,
    because those are already part of Flink's Core Dependencies.


## Setting up a Project: Basic Dependencies

Every Flink application needs as the bare minimum the API dependencies, to develop against.
For Maven, you can use the [Java Project Template]({{ site.baseurl }}/dev/projectsetup/java_api_quickstart.html)
or [Scala Project Template]({{ site.baseurl }}/dev/projectsetup/scala_api_quickstart.html) to create
a program skeleton with these initial dependencies.

When setting up a project manually, you need to add the following dependencies for the Java/Scala API
(here presented in Maven syntax, but the same dependencies apply to other build tools (Gradle, SBT, etc.) as well.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-java</artifactId>
  <version>{{site.version }}</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-java{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
  <scope>provided</scope>
</dependency>
{% endhighlight %}
</div>
</div>

**Important:** Please note that all these dependencies have their scope set to *provided*.
That means that they are needed to compile against, but that they should not be packaged into the
project's resulting application jar file - these dependencies are Flink Core Dependencies,
which are already available in any setup.

It is highly recommended to keep the dependencies in scope *provided*. If they are not set to *provided*,
the best case is that the resulting JAR becomes excessively large, because it also contains all Flink core
dependencies. The worst case is that the Flink core dependencies that are added to the application's jar file
clash with some of your own dependency versions (which is normally avoided through inverted classloading).

**Note on IntelliJ:** To make the applications run within IntelliJ IDEA, the Flink dependencies need
to be declared in scope *compile* rather than *provided*. Otherwise IntelliJ will not add them to the classpath and
the in-IDE execution will fail with a `NoClassDefFountError`. To avoid having to declare the
dependency scope as *compile* (which is not recommended, see above), the above linked Java- and Scala
project templates use a trick: They add a profile that selectively activates when the application
is run in IntelliJ and only then promotes the dependencies to scope *compile*, without affecting
the packaging of the JAR files.


## Adding Connector and Library Dependencies

Most applications need specific connectors or libraries to run, for example a connector to Kafka, Cassandra, etc.
These connectors are not part of Flink's core dependencies and must hence be added as dependencies to the application

Below is an example adding the connector for Kafka 0.10 as a dependency (Maven syntax):
{% highlight xml %}
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka-0.10{{ site.scala_version_suffix }}</artifactId>
    <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

We recommend to package the application code and all its required dependencies into one *jar-with-dependencies* which
we refer to as the *application jar*. The application jar can be submitted to an already running Flink cluster,
or added to a Flink application container image.

Projects created from the [Java Project Template]({{ site.baseurl }}/dev/projectsetup/java_api_quickstart.html) or
[Scala Project Template]({{ site.baseurl }}/dev/projectsetup/scala_api_quickstart.html) are configured to automatically include
the application dependencies into the application jar when running `mvn clean package`. For projects that are
not set up from those templates, we recommend to add the Maven Shade Plugin (as listed in the Appendix below)
to build the application jar with all required dependencies.

**Important:** For Maven (and other build tools) to correctly package the dependencies into the application jar,
these application dependencies must be specified in scope *compile* (unlike the core dependencies, which
must be specified in scope *provided*).


## Scala Versions

Scala versions (2.10, 2.11, 2.12, etc.) are not binary compatible with one another.
For that reason, Flink for Scala 2.11 cannot be used with an application that uses
Scala 2.12.

All Flink dependencies that (transitively) depend on Scala are suffixed with the
Scala version that they are built for, for example `flink-streaming-scala_2.11`.

Developers that only use Java can pick any Scala version, Scala developers need to
pick the Scala version that matches their application's Scala version.

Please refer to the [build guide]({{ site.baseurl }}/flinkDev/building.html#scala-versions)
for details on how to build Flink for a specific Scala version.

**Note:** Because of major breaking changes in Scala 2.12, Flink 1.5 currently builds only for Scala 2.11.
We aim to add support for Scala 2.12 in the next versions.


## Hadoop Dependencies

**General rule: It should never be necessary to add Hadoop dependencies directly to your application.**
*(The only exception being when using existing Hadoop input-/output formats with Flink's Hadoop compatibility wrappers)*

If you want to use Flink with Hadoop, you need to have a Flink setup that includes the Hadoop dependencies, rather than
adding Hadoop as an application dependency. Please refer to the [Hadoop Setup Guide]({{ site.baseurl }}/ops/deployment/hadoop.html)
for details.

There are two main reasons for that design:

  - Some Hadoop interaction happens in Flink's core, possibly before the user application is started, for example
    setting up HDFS for checkpoints, authenticating via Hadoop's Kerberos tokens, or deployment on YARN.

  - Flink's inverted classloading approach hides many transitive dependencies from the core dependencies. That applies not only
    to Flink's own core dependencies, but also to Hadoop's dependencies when present in the setup.
    That way, applications can use different versions of the same dependencies without running into dependency conflicts (and
    trust us, that's a big deal, because Hadoops dependency tree is huge.)

If you need Hadoop dependencies during testing or development inside the IDE (for example for HDFS access), please configure
these dependencies similar to the scope of the dependencies to *test* or to *provided*.


## Appendix: Template for building a Jar with Dependencies

To build an application JAR that contains all dependencies required for declared connectors and libraries,
you can use the following shade plugin definition:

{% highlight xml %}
<build>
	<plugins>
		<plugin>
			<groupId>org.apache.maven.plugins</groupId>
			<artifactId>maven-shade-plugin</artifactId>
			<version>3.0.0</version>
			<executions>
				<execution>
					<phase>package</phase>
					<goals>
						<goal>shade</goal>
					</goals>
					<configuration>
						<artifactSet>
							<excludes>
								<exclude>com.google.code.findbugs:jsr305</exclude>
								<exclude>org.slf4j:*</exclude>
								<exclude>log4j:*</exclude>
							</excludes>
						</artifactSet>
						<filters>
							<filter>
								<!-- Do not copy the signatures in the META-INF folder.
								Otherwise, this might cause SecurityExceptions when using the JAR. -->
								<artifact>*:*</artifact>
								<excludes>
									<exclude>META-INF/*.SF</exclude>
									<exclude>META-INF/*.DSA</exclude>
									<exclude>META-INF/*.RSA</exclude>
								</excludes>
							</filter>
						</filters>
						<transformers>
							<transformer implementation="org.apache.maven.plugins.shade.resource.ManifestResourceTransformer">
								<mainClass>my.programs.main.clazz</mainClass>
							</transformer>
						</transformers>
					</configuration>
				</execution>
			</executions>
		</plugin>
	</plugins>
</build>
{% endhighlight %}

{% top %}

