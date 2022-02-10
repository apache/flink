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

# Project Configuration

Every Flink application depends on a set of Flink libraries. At a minimum, the application depends
on the Flink APIs and, in addition, on certain connector libraries (i.e. Kafka, Cassandra).
When running Flink applications in the IDE, add a provided dependency to the [Flink runtime library](https://mvnrepository.com/artifact/org.apache.flink/flink-runtime).

The guides in this section will show you how to configure your projects via popular build tools
([Maven]({{< ref "docs/dev/configuration/maven" >}}), [Gradle]({{< ref "docs/dev/configuration/gradle" >}})),
add the necessary dependencies (i.e. [connectors and formats]({{< ref "docs/dev/configuration/connector" >}}), 
[testing]({{< ref "docs/dev/configuration/testing" >}})), and cover some 
[advanced]({{< ref "docs/dev/configuration/advanced" >}}) configuration topics. 

## Getting started

To get started working on your Flink application, use the following commands, scripts, and templates 
to create a Flink project.  

{{< tabs "creating project" >}}
{{< tab "Maven" >}}

You can create a project based on an [Archetype](https://maven.apache.org/guides/introduction/introduction-to-archetypes.html)
with the Maven command below or use the provided quickstart bash script.

### Maven command
```bash
$ mvn archetype:generate                \
  -DarchetypeGroupId=org.apache.flink   \
  -DarchetypeArtifactId=flink-quickstart-java \
  -DarchetypeVersion={{< version >}}
```
This allows you to name your newly created project and will interactively ask you for the groupId,
artifactId, and package name.

### Quickstart script
```bash
$ curl https://flink.apache.org/q/quickstart.sh | bash -s {{< version >}}
```

{{< /tab >}}
{{< tab "Gradle" >}}
You can create a project with a Gradle build script or use the provided quickstart bash script.

### Gradle build script

To execute these build configuration scripts, run the `gradle` command in the directory with these scripts.

**build.gradle**

```gradle
plugins {
    id 'java'
    id 'application'
    // shadow plugin to produce fat JARs
    id 'com.github.johnrengelman.shadow' version '7.1.2'
}
// artifact properties
group = 'org.quickstart'
version = '0.1-SNAPSHOT'
mainClassName = 'org.quickstart.StreamingJob'
mainClassName = 'org.quickstart.StreamingJob'
description = """Flink Quickstart Job"""
ext {
    javaVersion = '1.8'
    flinkVersion = '{{< version >}}'
    slf4jVersion = '1.7.32'
    log4jVersion = '2.17.1'
}
sourceCompatibility = javaVersion
targetCompatibility = javaVersion
tasks.withType(JavaCompile) {
	options.encoding = 'UTF-8'
}
applicationDefaultJvmArgs = ["-Dlog4j.configurationFile=log4j2.properties"]

// declare where to find the dependencies of your project
repositories {
    mavenCentral()
}
// NOTE: We cannot use "compileOnly" or "shadow" configurations since then we could not run code
// in the IDE or with "gradle run". We also cannot exclude transitive dependencies from the
// shadowJar yet (see https://github.com/johnrengelman/shadow/issues/159).
// -> Explicitly define the // libraries we want to be included in the "flinkShadowJar" configuration!
configurations {
    flinkShadowJar // dependencies which go into the shadowJar
    // always exclude these (also from transitive dependencies) since they are provided by Flink
    flinkShadowJar.exclude group: 'org.apache.flink', module: 'force-shading'
    flinkShadowJar.exclude group: 'com.google.code.findbugs', module: 'jsr305'
    flinkShadowJar.exclude group: 'org.slf4j'
    flinkShadowJar.exclude group: 'org.apache.logging.log4j'
}
// declare the dependencies for your production and test code
dependencies {
    // --------------------------------------------------------------
    // Compile-time dependencies that should NOT be part of the
    // shadow jar and are provided in the lib folder of Flink
    // --------------------------------------------------------------
    implementation "org.apache.flink:flink-streaming-java:${flinkVersion}"
    implementation "org.apache.flink:flink-clients:${flinkVersion}"
    // --------------------------------------------------------------
    // Dependencies that should be part of the shadow jar, e.g.
    // connectors. These must be in the flinkShadowJar configuration!
    // --------------------------------------------------------------
    //flinkShadowJar "org.apache.flink:flink-connector-kafka:${flinkVersion}"
    runtimeOnly "org.apache.logging.log4j:log4j-api:${log4jVersion}"
    runtimeOnly "org.apache.logging.log4j:log4j-core:${log4jVersion}"
    runtimeOnly "org.apache.logging.log4j:log4j-slf4j-impl:${log4jVersion}"
    runtimeOnly "org.slf4j:slf4j-log4j12:${slf4jVersion}"
    // Add test dependencies here.
    // testCompile "junit:junit:4.12"
}
// make compileOnly dependencies available for tests:
sourceSets {
    main.compileClasspath += configurations.flinkShadowJar
    main.runtimeClasspath += configurations.flinkShadowJar
    test.compileClasspath += configurations.flinkShadowJar
    test.runtimeClasspath += configurations.flinkShadowJar
    javadoc.classpath += configurations.flinkShadowJar
}
run.classpath = sourceSets.main.runtimeClasspath

shadowJar {
    configurations = [project.configurations.flinkShadowJar]
}
```

**settings.gradle**

```gradle
rootProject.name = 'quickstart'
```

### Quickstart script

```bash
bash -c "$(curl https://flink.apache.org/q/gradle-quickstart.sh)" -- {{< version >}} {{< scala_version >}}
```
{{< /tab >}}
{{< /tabs >}}

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

Check out the sections on [Datastream API]({{< ref "docs/dev/datastream/overview" >}}) and 
[Table API & SQL]({{< ref "docs/dev/table/overview" >}}) to learn more.
