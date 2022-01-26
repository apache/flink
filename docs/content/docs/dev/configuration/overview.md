---
title: "Overview"
weight: 1
type: docs
aliases:
- /dev/project-configuration.html
- /start/dependencies.html
- /getting-started/project-setup/dependencies.html
- /quickstart/java_api_quickstart.html
- /dev/projectsetup/java_api_quickstart.html
- /dev/linking_with_flink.html
- /dev/linking.html
- /dev/projectsetup/dependencies.html
- /dev/projectsetup/java_api_quickstart.html
- /getting-started/project-setup/java_api_quickstart.html
- /dev/getting-started/project-setup/scala_api_quickstart.html
- /getting-started/project-setup/scala_api_quickstart.html
- /quickstart/scala_api_quickstart.html
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

# Project configuration

Every Flink application depends on a set of Flink libraries. At a minimum, the application depends
on the Flink APIs and, in addition, on certain connector libraries (i.e. Kafka, Cassandra).
When running Flink applications (either in a distributed deployment or in the IDE for testing),
the Flink runtime library must be available.

The guides in this section will show you how to configure your projects via popular build tools,
add the necessary dependencies so you can start working on your Flink application, and also cover 
some advanced configuration topics. 

## Which dependencies do you need?

Depending on what you want to achieve, you are going to choose a combination of our available APIs, 
which will require different dependencies. 

Here is a table of artifact/dependency names:

| APIs you want to use              | Dependency you need to add    |
|-----------------------------------|-------------------------------|
| DataStream                        | flink-streaming-java          |  
| DataStream with Scala             | flink-streaming-scala{{< scala_version >}}         |   
| Table API                         | flink-table-api-java          |   
| Table API with Scala              | flink-table-api-scala{{< scala_version >}}         |
| Table API + DataStream            | flink-table-api-java-bridge   |
| Table API + DataStream with Scala | flink-table-api-scala-bridge{{< scala_version >}}  |


## Next steps

Check out the [build tools]({{< ref "docs/dev/configuration/buildtools" >}}) section to learn how to
add these dependencies with Maven, Gradle, or sbt. 
