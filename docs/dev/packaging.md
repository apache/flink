---
title: "Program Packaging and Distributed Execution"
nav-title: Program Packaging
nav-parent_id: execution
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


As described earlier, Flink programs can be executed on
clusters by using a `remote environment`. Alternatively, programs can be packaged into JAR Files
(Java Archives) for execution. Packaging the program is a prerequisite to executing them through the
[command line interface]({{ site.baseurl }}/ops/cli.html).

### Packaging Programs

To support execution from a packaged JAR file via the command line or web interface, a program must
use the environment obtained by `StreamExecutionEnvironment.getExecutionEnvironment()`. This environment
will act as the cluster's environment when the JAR is submitted to the command line or web
interface. If the Flink program is invoked differently than through these interfaces, the
environment will act like a local environment.

To package the program, simply export all involved classes as a JAR file. The JAR file's manifest
must point to the class that contains the program's *entry point* (the class with the public
`main` method). The simplest way to do this is by putting the *main-class* entry into the
manifest (such as `main-class: org.apache.flinkexample.MyProgram`). The *main-class* attribute is
the same one that is used by the Java Virtual Machine to find the main method when executing a JAR
files through the command `java -jar pathToTheJarFile`. Most IDEs offer to include that attribute
automatically when exporting JAR files.

### Summary

The overall procedure to invoke a packaged program consists of two steps:

1. The JAR's manifest is searched for a *main-class* or *program-class* attribute. If both
attributes are found, the *program-class* attribute takes precedence over the *main-class*
attribute. Both the command line and the web interface support a parameter to pass the entry point
class name manually for cases where the JAR manifest contains neither attribute. 

2. The system invokes the main method of the class.

{% top %}
