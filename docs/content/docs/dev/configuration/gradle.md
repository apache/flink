---
title: "Using Gradle"
weight: 3
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

# How to use Gradle to configure your project

You will likely need a build tool to configure your Flink project. This guide will show you how to
do so with [Gradle](https://gradle.org), an open-source general-purpose build tool that can be used 
to automate tasks in the development process.

## Requirements

- Gradle 3.x (or higher)
- Java 8.x

## Importing the project into your IDE

Once the project folder and files have been created, we recommend that you import this project into
your IDE for developing and testing.


IntelliJ IDEA supports Gradle projects via the `Gradle` plugin.

Eclipse does so via the [Eclipse Buildship](https://projects.eclipse.org/projects/tools.buildship)
plugin (make sure to specify a Gradle version >= 3.0 in the last step of the import wizard; the `shadow`
plugin requires it). You may also use [Gradle's IDE integration](https://docs.gradle.org/current/userguide/userguide.html#ide-integration)
to create project files with Gradle.

*Note*: The default JVM heap size for Java may be too small for Flink and you have to manually increase it.
In Eclipse, choose `Run Configurations -> Arguments` and write into the `VM Arguments` box: `-Xmx800m`.
In IntelliJ IDEA recommended way to change JVM options is from the `Help | Edit Custom VM Options` menu.
See [this article](https://intellij-support.jetbrains.com/hc/en-us/articles/206544869-Configuring-JVM-options-and-platform-properties) for details.

## Building the project

If you want to __build/package your project__, go to your project directory and
run the '`gradle clean shadowJar`' command.
You will __find a JAR file__ that contains your application, plus connectors and libraries
that you may have added as dependencies to the application: `build/libs/<project-name>-<version>-all.jar`.

__Note:__ If you use a different class than *StreamingJob* as the application's main class / entry point,
we recommend you change the `mainClassName` setting in the `build.gradle` file accordingly. That way, Flink
can run the application from the JAR file without additionally specifying the main class.

## Adding dependencies to the project



**Important:** Note that all these (core) dependencies should have their scope set to [*provided*](https://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html#dependency-scope). This means that
they are needed to compile against, but that they should not be packaged into the project's resulting
application JAR file. If not set to *provided*, the best case scenario is that the resulting JAR
becomes excessively large, because it also contains all Flink core dependencies. The worst case scenario
is that the Flink core dependencies that are added to the application's JAR file clash with some of
your own dependency versions (which is normally avoided through inverted classloading).

To correctly package the dependencies into the application JAR, these application dependencies must
be set to the *compile* scope.

## Shading

In order to create an uber JAR to run the job, do this:


**Note:** You do not need to shade Flink API dependencies. You only need to do this for connectors,
formats and third-party dependencies.
