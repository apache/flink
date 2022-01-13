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

# Using sbt to configure your Flink project

sbt is an open-source build tool for managing Scala and Java projects. You can use it to 
build, compile, test, as well as manage libraries and dependencies.

## Requirements

- sbt 0.13.13 (or higher)
- Java 8.x

## Creating a Flink project

You can scaffold a new Flink project with the following [giter8 template](https://github.com/tillrohrmann/flink-project.g8) 
and the `sbt new` command (which creates new build definitions from a template) or use the provided quickstart bash script::

{{< tabs sbt >}}
{{< tab "SBT template" >}}
```bash
$ sbt new tillrohrmann/flink-project.g8
```
{{< /tab >}}
{{< tab "Bash script" >}}
```bash
$ bash <(curl https://flink.apache.org/q/sbt-quickstart.sh)
```
{{< /tab >}}
{{< /tabs >}}

## Building the project

To build your project, issue the `sbt clean assembly` command in the project directory. This will 
create the fat-jar `your-project-name-assembly-0.1-SNAPSHOT.jar` in the directory `target/scala_your-major-scala-version/`.

## Running the project

To run your project, issue the `sbt run` command. By default, this will run your job in the same JVM
that `sbt` is running in. In order to run your job in a distinct JVM, add the following line to the 
`build.sbt` file:

```scala
fork in run := true
```

## Using an IDE

### IntelliJ

We recommend using [IntelliJ](https://www.jetbrains.com/idea/) for your Flink job development. To get
started, import your newly created project into IntelliJ via `File -> New -> Project from Existing Sources...` 
and then choose your project's directory. IntelliJ will then automatically detect the `build.sbt` file 
and set everything up.

To run your Flink job, choose the `mainRunner` module as the classpath in your __Run/Debug Configuration__
via `Run -> Edit Configurations...` and choosing `mainRunner` from the _Use classpath of module_ drop box.
This will ensure that all dependencies which are set to _provided_ will be available upon execution.

### Eclipse

To import the newly created project into [Eclipse](https://eclipse.org/), you first have to create 
Eclipse project files for it which can be done via the [sbteclipse](https://github.com/typesafehub/sbteclipse) plugin.

Add the following line to your `PROJECT_DIR/project/plugins.sbt` file:

```bash
addSbtPlugin("com.typesafe.sbteclipse" % "sbteclipse-plugin" % "4.0.0")
```

Then in `sbt`, use the following command to create the Eclipse project files:

```bash
> eclipse
```

Now you can import the project into Eclipse via `File -> Import... -> Existing Projects into Workspace` 
and then select the project directory.
