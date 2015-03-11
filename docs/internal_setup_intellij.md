---
title:  "How to set up IntelliJ IDEA"
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



A brief guide on how to set up IntelliJ IDEA IDE for development of the Flink core.
As Eclipse is known to have issues with mixed Scala and Java projects, more and more contributers are migrating to IntelliJ IDEA.

The following documentation describes the steps to setup IntelliJ IDEA 14.0.3 (https://www.jetbrains.com/idea/download/) with the Flink sources.

Prior to doing anything, make sure that the Flink project is built at least once from the terminal:
`mvn clean package -DskipTests`

# IntelliJ IDEA

## Installing the Scala plugin
1. Go to IntelliJ plugins settings (File -> Settings -> Plugins) and click on "Install Jetbrains plugin...". 
2. Select and install the "Scala" plugin. 
3. Restart IntelliJ

## Installing the Scala compiler plugin
1. Go to IntelliJ scala compiler settings (File -> Settings -> Build, Execution, Deployment -> Compiler -> Scala Compiler) and click on "Install Jetbrains plugin...". 
2. Click on the green plus icon on the right to add a compiler plugin
3. Point to the paradise jar: ~/.m2/repository/org/scalamacros/paradise_2.10.4/2.0.1/paradise_2.10.4-2.0.1.jar If there is no such file, this means that you should build Flink from the terminal as explained above.

## Importing Flink
1. Start IntelliJ IDEA and choose "Import Project"
2. Select the root folder of the Flink repository
3. Choose "Import project from external model" and select "Maven"
4. Leave the default options and finish the import.

---

*This documentation is maintained by the contributors of the individual components.
We kindly ask anyone that adds and changes components to eventually provide a patch
or pull request that updates these documents as well.*

