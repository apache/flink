---
title: "Using Maven"
weight: 2
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

# Using Maven to configure your Flink project

Maven is an open-source build automation tool developed by the Apache Group that enables you to build, 
publish, and deploy projects. You can use it to manage the entire lifecycle of your software project.

## Requirements

- Maven 3.0.4 (or higher)
- Java 8.x

## Creating a Flink project

You can create a project based on an [Archetype](https://maven.apache.org/guides/introduction/introduction-to-archetypes.html) 
with the Maven command below or use the provided quickstart bash script:

{{< tabs "maven" >}}
{{< tab "Maven command" >}}
```bash
$ mvn archetype:generate                \
  -DarchetypeGroupId=org.apache.flink   \
  -DarchetypeArtifactId=flink-quickstart-java \
  -DarchetypeVersion={{< version >}}
```
This allows you to name your newly created project and will interactively ask you for the groupId,
artifactId, and package name.
{{< /tab >}}
{{< tab "Bash script" >}}
{{< stable >}}
```bash
$ curl https://flink.apache.org/q/quickstart.sh | bash -s {{< version >}}
```
{{< /stable >}}
{{< unstable >}}
```bash
$ curl https://flink.apache.org/q/quickstart-SNAPSHOT.sh | bash -s {{< version >}}

```
{{< /unstable >}}
{{< /tab >}}
{{< /tabs >}}

{{< unstable >}}
{{< hint info >}}
For Maven 3.0 or higher, it is no longer possible to specify the repository (-DarchetypeCatalog) via
the command line. For details about this change, please refer to the <a href="http://maven.apache.org/archetype/maven-archetype-plugin/archetype-repository.html">official Maven document</a> If you wish to use a snapshot repository, you need to add a
repository entry to your `settings.xml` file. For example:

```xml
<settings>
  <activeProfiles>
    <activeProfile>apache</activeProfile>
  </activeProfiles>
  <profiles>
    <profile>
      <id>apache</id>
      <repositories>
        <repository>
          <id>apache-snapshots</id>
          <url>https://repository.apache.org/content/repositories/snapshots/</url>
        </repository>
      </repositories>
    </profile>
  </profiles>
</settings>
```

{{< /hint >}}
{{< /unstable >}}

Once the project folder and files have been created, we recommend that you import this project into
your IDE for developing and testing. IntelliJ IDEA supports Maven projects out-of-the-box and Eclipse
offers the [m2e plugin](http://www.eclipse.org/m2e/) to [import Maven projects](http://books.sonatype.com/m2eclipse-book/reference/creating-sect-importing-projects.html#fig-creating-import).

*Note*: The default JVM heap size for Java may be too small for Flink and you have to manually increase it.
In Eclipse, choose `Run Configurations -> Arguments` and write `-Xmx800m` into the `VM Arguments` box.
In IntelliJ IDEA, the recommended way to change JVM options is from the `Help | Edit Custom VM Options` menu.
See [this article](https://intellij-support.jetbrains.com/hc/en-us/articles/206544869-Configuring-JVM-options-and-platform-properties) for details.

## Building the project

If you want to build/package your project, navigate to your project directory and run the
'`mvn clean package`' command. You will find a JAR file that contains your application (plus connectors
and libraries that you may have added as dependencies to the application) here:`target/<artifact-id>-<version>.jar`.

__Note:__ If you used a different class than `DataStreamJob` as the application's main class / entry point,
we recommend you change the `mainClass` setting in the `pom.xml` file accordingly so that Flink
can run the application from the JAR file without additionally specifying the main class.
