---
title: "IDE Setup"
# Top navigation
top-nav-group: internals
top-nav-pos: 1
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

* Replaced by the TOC
{:toc}

## Eclipse

A brief guide how to set up Eclipse for development of the Flink core.
Flink uses mixed Scala/Java projects, which pose a challenge to some IDEs.
Below is the setup guide that works best from our personal experience.

For Eclipse users, we currently recomment the Scala IDE 3.0.3, as the most robust solution.


### Eclipse Scala IDE 3.0.3

**NOTE:** While this version of the Scala IDE is not the newest, we have found it to be the most reliably working
version for complex projects like Flink. One restriction is, though, that it works only with Java 7, not with Java 8.

**Note:** Before following this setup, make sure to run the build from the command line once
(`mvn clean package -DskipTests`)

1. Download the Scala IDE (preferred) or install the plugin to Eclipse Kepler. See section below for download links
   and instructions.
2. Add the "macroparadise" compiler plugin to the Scala compiler.
   Open "Window" -> "Preferences" -> "Scala" -> "Compiler" -> "Advanced" and put into the "Xplugin" field the path to
   the *macroparadise* jar file (typically "/home/*-your-user-*/.m2/repository/org/scalamacros/paradise_2.10.4/2.0.1/paradise_2.10.4-2.0.1.jar").
   Note: If you do not have the jar file, you probably did not ran the command line build.
3. Import the Flink Maven projects ("File" -> "Import" -> "Maven" -> "Existing Maven Projects") 
4. During the import, Eclipse will ask to automatically install additional Maven build helper plugins.
5. Close the "flink-java8" project. Since Eclipse Kepler does not support Java 8, you cannot develop this project.


#### Download links for Scala IDE 3.0.3

The Scala IDE 3.0.3 is a previous stable release, and download links are a bit hidden.

The pre-packaged Scala IDE can be downloaded from the following links:

* [Linux (64 bit)](http://downloads.typesafe.com/scalaide-pack/3.0.3.vfinal-210-20140327/scala-SDK-3.0.3-2.10-linux.gtk.x86_64.tar.gz)
* [Linux (32 bit)](http://downloads.typesafe.com/scalaide-pack/3.0.3.vfinal-210-20140327/scala-SDK-3.0.3-2.10-linux.gtk.x86.tar.gz)
* [MaxOS X Cocoa (64 bit)](http://downloads.typesafe.com/scalaide-pack/3.0.3.vfinal-210-20140327/scala-SDK-3.0.3-2.10-macosx.cocoa.x86_64.zip)
* [MaxOS X Cocoa (32 bit)](http://downloads.typesafe.com/scalaide-pack/3.0.3.vfinal-210-20140327/scala-SDK-3.0.3-2.10-macosx.cocoa.x86.zip)
* [Windows (64 bit)](http://downloads.typesafe.com/scalaide-pack/3.0.3.vfinal-210-20140327/scala-SDK-3.0.3-2.10-win32.win32.x86_64.zip)
* [Windows (32 bit)](http://downloads.typesafe.com/scalaide-pack/3.0.3.vfinal-210-20140327/scala-SDK-3.0.3-2.10-win32.win32.x86.zip)

Alternatively, you can download Eclipse Kepler from [https://eclipse.org/downloads/packages/release/Kepler/SR2](https://eclipse.org/downloads/packages/release/Kepler/SR2)
and manually add the Scala and Maven plugins by plugin site at [http://scala-ide.org/download/prev-stable.html](http://scala-ide.org/download/prev-stable.html).

* Either use the update site to install the plugin ("Help" -> "Install new Software")
* Or download the [zip file](http://download.scala-ide.org/sdk/helium/e38/scala211/stable/update-site.zip), unpack it, and move the contents of the
  "plugins" and "features" folders into the equally named folders of the Eclipse root directory


### Eclipse Scala IDE 4.0.0

**NOTE: From personal experience, the use of the Scala IDE 4.0.0 performs worse than previous versions for complex projects like Flink.**
**Version 4.0.0 does not handle mixed Java/Scala projects as robustly and it frequently raises incorrect import and type errors.**

*Note:* Before following this setup, make sure to run the build from the command line once
(`mvn clean package -DskipTests`)

1. Download the Scala IDE: [http://scala-ide.org/download/sdk.html](http://scala-ide.org/download/sdk.html)
2. Import the Flink Maven projects (File -> Import -> Maven -> Existing Maven Projects) 
3. While importing the Flink project, the IDE may ask you to install an additional maven build helper plugin. 
4. After the import, you need to set the Scala version of your projects to Scala 2.10 (from the default 2.11). 
   To do that, select all projects that contain Scala code (marked by the small *S* on the project icon),
   right click and select "Scala -> Set the Scala Installation" and pick "2.10.4".
   Currently, the project to which that is relevant are "flink-runtime", "flink-scala", "flink-scala-examples",
   "flink-streaming-example", "flink-streaming-scala", "flink-tests", "flink-test-utils", and "flink-yarn".
5. Depending on your version of the Scala IDE, you may need to add the "macroparadise" compiler plugin to the
   Scala compiler. Open "Window" -> "Preferences" -> "Scala" -> "Compiler" -> "Advanced" and put into the "Xplugin" field
   the path to the *macroparadise* jar file (typically "/home/*-your-user-*/.m2/repository/org/scalamacros/paradise_2.10.4/2.0.1/paradise_2.10.4-2.0.1.jar")
6. In order to compile the "flink-java-8" project, you may need to add a Java 8 execution environment.
   See [this post](http://stackoverflow.com/questions/25391207/how-do-i-add-execution-environment-1-8-to-eclipse-luna)
   for details.

## IntelliJ IDEA

A brief guide on how to set up IntelliJ IDEA IDE for development of the Flink core.
As Eclipse is known to have issues with mixed Scala and Java projects, more and more contributers are migrating to IntelliJ IDEA.

The following documentation describes the steps to setup IntelliJ IDEA 14.0.3 (https://www.jetbrains.com/idea/download/) with the Flink sources.

Prior to doing anything, make sure that the Flink project is built at least once from the terminal:
`mvn clean package -DskipTests`

### Installing the Scala plugin
1. Go to IntelliJ plugins settings (File -> Settings -> Plugins) and click on "Install Jetbrains plugin...". 
2. Select and install the "Scala" plugin. 
3. Restart IntelliJ

### Installing the Scala compiler plugin
1. Go to IntelliJ scala compiler settings (File -> Settings -> Build, Execution, Deployment -> Compiler -> Scala Compiler) and click on "Install Jetbrains plugin...". 
2. Click on the green plus icon on the right to add a compiler plugin
3. Point to the paradise jar: ~/.m2/repository/org/scalamacros/paradise_2.10.4/2.0.1/paradise_2.10.4-2.0.1.jar If there is no such file, this means that you should build Flink from the terminal as explained above.

### Importing Flink
1. Start IntelliJ IDEA and choose "Import Project"
2. Select the root folder of the Flink repository
3. Choose "Import project from external model" and select "Maven"
4. Leave the default options and finish the import.
