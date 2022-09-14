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

# How to use Maven to configure your project

This guide will show you how to configure a Flink job project with [Maven](https://maven.apache.org), 
an open-source build automation tool developed by the Apache Software Foundation that enables you to build, 
publish, and deploy projects. You can use it to manage the entire lifecycle of your software project.

## Requirements

- Maven 3.0.4 (or higher)
- Java 11

## Importing the project into your IDE

Once the [project folder and files]({{< ref "docs/dev/configuration/overview#getting-started" >}}) 
have been created, we recommend that you import this project into your IDE for developing and testing.

IntelliJ IDEA supports Maven projects out-of-the-box. Eclipse offers the [m2e plugin](http://www.eclipse.org/m2e/) 
to [import Maven projects](http://books.sonatype.com/m2eclipse-book/reference/creating-sect-importing-projects.html#fig-creating-import).

**Note**: The default JVM heap size for Java may be too small for Flink and you have to manually increase it.
In Eclipse, choose `Run Configurations -> Arguments` and write into the `VM Arguments` box: `-Xmx800m`.
In IntelliJ IDEA recommended way to change JVM options is from the `Help | Edit Custom VM Options` menu.
See [this article](https://intellij-support.jetbrains.com/hc/en-us/articles/206544869-Configuring-JVM-options-and-platform-properties) for details.

**Note on IntelliJ:** To make the applications run within IntelliJ IDEA, it is necessary to tick the
`Include dependencies with "Provided" scope` box in the run configuration. If this option is not available
(possibly due to using an older IntelliJ IDEA version), then a workaround is to create a test that
calls the application's `main()` method.

## Building the project

If you want to build/package your project, navigate to your project directory and run the
'`mvn clean package`' command. You will find a JAR file that contains your application (plus connectors
and libraries that you may have added as dependencies to the application) here:`target/<artifact-id>-<version>.jar`.

__Note:__ If you used a different class than `DataStreamJob` as the application's main class / entry point,
we recommend you change the `mainClass` setting in the `pom.xml` file accordingly so that Flink
can run the application from the JAR file without additionally specifying the main class.

## Adding dependencies to the project

Open the `pom.xml` file in your project directory and add the dependency in between
the `dependencies` tab.  

For example, you can add the Kafka connector as a dependency like this:

```xml
<dependencies>
    
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-kafka</artifactId>
        <version>{{< version >}}</version>
    </dependency>
    
</dependencies>
```

Then execute `mvn install` on the command line. 

Projects created from the `Java Project Template`, the `Scala Project Template`, or Gradle are configured
to automatically include the application dependencies into the application JAR when you run `mvn clean package`.
For projects that are not set up from those templates, we recommend adding the Maven Shade Plugin to
build the application jar with all required dependencies.

**Important:** Note that all these core API dependencies should have their scope set to [*provided*](https://maven.apache.org/guides/introduction/introduction-to-dependency-mechanism.html#dependency-scope). This means that
they are needed to compile against, but that they should not be packaged into the project's resulting
application JAR file. If not set to *provided*, the best case scenario is that the resulting JAR
becomes excessively large, because it also contains all Flink core dependencies. The worst case scenario
is that the Flink core dependencies that are added to the application's JAR file clash with some of
your own dependency versions (which is normally avoided through inverted classloading).

To correctly package the dependencies into the application JAR, the Flink API dependencies must 
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

To learn more about how to deploy Flink jobs, check out the [deployment guide]({{< ref "docs/deployment/cli" >}}).

## Template for creating an uber/fat JAR with dependencies

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
                                <!-- Replace this with the main class of your job -->
                                <mainClass>my.programs.main.clazz</mainClass>
                            </transformer>
                            <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                        </transformers>
                    </configuration>
                </execution>
            </executions>
        </plugin>
    </plugins>
</build>
```

The [Maven shade plugin](https://maven.apache.org/plugins/maven-shade-plugin/index.html) will include, 
by default, all the dependencies in the "runtime" and "compile" scope.
