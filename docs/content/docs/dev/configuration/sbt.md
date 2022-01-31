---
title: "Using sbt"
weight: 4
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

# How to use sbt to configure your project

You will likely need a build tool to configure your Flink project. This guide will show you how to
do so with [sbt](https://www.scala-sbt.org), an open-source build tool for managing Scala and Java 
projects. You can use it to build, compile, test, as well as manage libraries and dependencies.

## Requirements

- sbt 0.13.13 (or higher)
- Java 8.x

## Importing the project into your IDE

Once the project folder and files have been created, we recommend that you import this project into
your IDE for developing and testing.

In IntelliJ IDEA, go to `File -> New -> Project from Existing Sources...` and then choose your project's directory.
IntelliJ will then automatically detect the `build.sbt` file and set everything up.

To run your Flink job, choose the `mainRunner` module as the classpath in your __Run/Debug Configuration__
via `Run -> Edit Configurations...` and choosing `mainRunner` from the _Use classpath of module_ drop box.
This will ensure that all dependencies which are set to _provided_ will be available upon execution.

In Eclipse, you first have to create Eclipse project files for it which can be done via the
[sbteclipse](https://github.com/typesafehub/sbteclipse) plugin.

Then add the following line to your `PROJECT_DIR/project/plugins.sbt` file:

```bash
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "4.0.0")
```

Then in `sbt`, use the following command to create the Eclipse project files:

```bash
> eclipse
```

Now you can import the project into Eclipse via `File -> Import... -> Existing Projects into Workspace`
and then select the project directory.

*Note*: The default JVM heap size for Java may be too small for Flink and you have to manually increase it.
In Eclipse, choose `Run Configurations -> Arguments` and write into the `VM Arguments` box: `-Xmx800m`.
In IntelliJ IDEA recommended way to change JVM options is from the `Help | Edit Custom VM Options` menu.
See [this article](https://intellij-support.jetbrains.com/hc/en-us/articles/206544869-Configuring-JVM-options-and-platform-properties) for details.

## Building the project

To build your project, issue the `sbt clean assembly` command in the project directory. This will
create the fat-jar `your-project-name-assembly-0.1-SNAPSHOT.jar` in the directory `target/scala_your-major-scala-version/`.

To run your project, issue the `sbt run` command. By default, this will run your job in the same JVM
that `sbt` is running in. In order to run your job in a distinct JVM, add the following line to the
`build.sbt` file:

```scala
fork in run := true
```

## Adding dependencies to the project

If you have JAR files that you want to use in your project, copy them to the `/lib` folder in the root 
directory of your sbt project, and sbt will automatically find them. If those JAR files depend on other 
JAR files, you will need to download them and copy them to the `/lib` directory as well.

If you have a single managed dependency, you can add a `libraryDependencies` line to your `build.sbt` file:

```java
libraryDependencies += groupID % artifactID % revision % configuration
```

**Important:** Note that all these (core) dependencies should have their scope set to [*provided*](https://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html#dependency-scope). This means that
they are needed to compile against, but that they should not be packaged into the project's resulting
application JAR file. If not set to *provided*, the best case scenario is that the resulting JAR
becomes excessively large, because it also contains all Flink core dependencies. The worst case scenario
is that the Flink core dependencies that are added to the application's JAR file clash with some of
your own dependency versions (which is normally avoided through inverted classloading).

To correctly package the dependencies into the application JAR, these application dependencies must
be set to the *compile* scope.

## Packaging the application

Depending on your use case, you may need to package your Flink application in different ways before it
gets deployed to a Flink environment.

If you want to create a JAR for a Flink Job and use only Flink dependencies without any third-party
dependencies (i.e. using the filesystem connector with JSON format), you do not need to create an
uber/fat JAR or shade any dependencies.

If you want to create a JAR for a Flink Job and use external dependencies not built into the Flink
distribution, you can either add them to the classpath of the distribution or shade them into your
uber/fat application JAR.

With the generated uber/fat JAR, you can submit it to a local or remote cluster with:

```sh
bin/flink run -c org.example.MyJob myFatJar.jar
```
