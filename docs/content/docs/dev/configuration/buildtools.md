---
title: "With Build Tools"
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

# How to use build tools to configure your project

You will likely need a build tool to configure your Flink project. This guide will show you how to 
do so with [Maven](https://maven.apache.org), [Gradle](https://gradle.org), and [sbt](https://www.scala-sbt.org). 

{{< tabs "build tool" >}}
{{< tab "Maven" >}}
Maven is an open-source build automation tool developed by the Apache Group that enables you to build, 
publish, and deploy projects. You can use it to manage the entire lifecycle of your software project.
{{< /tab >}}
{{< tab "Gradle" >}}
Gradle is an open-source general-purpose build tool that can be used to automate tasks in the
development process.
{{< /tab >}}
{{< tab "sbt" >}}
sbt is an open-source build tool for managing Scala and Java projects. You can use it to
build, compile, test, as well as manage libraries and dependencies.
{{< /tab >}}
{{< /tabs >}}

## Requirements

{{< tabs "requirements" >}}

{{< tab "Maven" >}}
- Maven 3.0.4 (or higher)
- Java 8.x
{{< /tab >}}

{{< tab "Gradle" >}}
- Gradle 3.x (or higher)
- Java 8.x
{{< /tab >}}

{{< tab "sbt" >}}
- sbt 0.13.13 (or higher)
- Java 8.x
{{< /tab >}}

{{< /tabs >}}

## Importing the project into your IDE

Once the project folder and files have been created, we recommend that you import this project into
your IDE for developing and testing.

{{< tabs "importing project" >}}
{{< tab "Maven" >}}
IntelliJ IDEA supports Maven projects out-of-the-box.

Eclipse offers the [m2e plugin](http://www.eclipse.org/m2e/) 
to [import Maven projects](http://books.sonatype.com/m2eclipse-book/reference/creating-sect-importing-projects.html#fig-creating-import).
{{< /tab >}}
{{< tab "Gradle" >}}
IntelliJ IDEA supports Gradle projects via the `Gradle` plugin. 

Eclipse does so via the [Eclipse Buildship](https://projects.eclipse.org/projects/tools.buildship) 
plugin (make sure to specify a Gradle version >= 3.0 in the last step of the import wizard; the `shadow` 
plugin requires it). You may also use [Gradle's IDE integration](https://docs.gradle.org/current/userguide/userguide.html#ide-integration)
to create project files with Gradle.
{{< /tab >}}
{{< tab "sbt" >}}
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
{{< /tab >}}
{{< /tabs >}}

*Note*: The default JVM heap size for Java may be too small for Flink and you have to manually increase it.
In Eclipse, choose `Run Configurations -> Arguments` and write into the `VM Arguments` box: `-Xmx800m`.
In IntelliJ IDEA recommended way to change JVM options is from the `Help | Edit Custom VM Options` menu.
See [this article](https://intellij-support.jetbrains.com/hc/en-us/articles/206544869-Configuring-JVM-options-and-platform-properties) for details.

## Building the project

{{< tabs "building project" >}}
{{< tab "Maven" >}}
If you want to build/package your project, navigate to your project directory and run the
'`mvn clean package`' command. You will find a JAR file that contains your application (plus connectors
and libraries that you may have added as dependencies to the application) here:`target/<artifact-id>-<version>.jar`.

__Note:__ If you used a different class than `DataStreamJob` as the application's main class / entry point,
we recommend you change the `mainClass` setting in the `pom.xml` file accordingly so that Flink
can run the application from the JAR file without additionally specifying the main class.
{{< /tab >}}
{{< tab "Gradle" >}}
If you want to __build/package your project__, go to your project directory and
run the '`gradle clean shadowJar`' command.
You will __find a JAR file__ that contains your application, plus connectors and libraries
that you may have added as dependencies to the application: `build/libs/<project-name>-<version>-all.jar`.

__Note:__ If you use a different class than *StreamingJob* as the application's main class / entry point,
we recommend you change the `mainClassName` setting in the `build.gradle` file accordingly. That way, Flink
can run the application from the JAR file without additionally specifying the main class.
{{< /tab >}}
{{< tab "sbt" >}}
To build your project, issue the `sbt clean assembly` command in the project directory. This will
create the fat-jar `your-project-name-assembly-0.1-SNAPSHOT.jar` in the directory `target/scala_your-major-scala-version/`.

To run your project, issue the `sbt run` command. By default, this will run your job in the same JVM
that `sbt` is running in. In order to run your job in a distinct JVM, add the following line to the
`build.sbt` file:

```scala
fork in run := true
```
{{< /tab >}}
{{< /tabs >}}

## Adding dependencies to the project

{{< tabs "requirements" >}}
{{< tab "Maven" >}}

As an example, you can add the Kafka connector as a dependency like this (in Maven syntax):

{{< artifact flink-connector-kafka >}}

Projects created from the `Java Project Template`, the `Scala Project Template`, or Gradle are configured
to automatically include the application dependencies into the application JAR when you run `mvn clean package`.
For projects that are not set up from those templates, we recommend adding the Maven Shade Plugin to
build the application jar with all required dependencies.
{{< /tab >}}
{{< tab "Gradle" >}}

{{< /tab >}}
{{< tab "sbt" >}}

{{< /tab >}}
{{< /tabs >}}


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


## Template for building a JAR with Dependencies

To build an application JAR that contains all dependencies required for declared connectors and libraries,
you can use the following shade plugin definition:

```xml
<build>
    <plugins>
        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-shade-plugin</artifactId>
            <version>3.1.1</version>
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
```
