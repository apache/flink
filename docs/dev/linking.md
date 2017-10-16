---
nav-title: "Linking with Optional Modules"
title: "Linking with modules not contained in the binary distribution"
nav-parent_id: start
nav-pos: 10
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

The binary distribution contains jar packages in the `lib` folder that are automatically
provided to the classpath of your distributed programs. Almost all of Flink classes are
located there with a few exceptions, for example the streaming connectors and some freshly
added modules. To run code depending on these modules you need to make them accessible
during runtime, for which we suggest two options:

1. Either copy the required jar files to the `lib` folder onto all of your TaskManagers.
Note that you have to restart your TaskManagers after this.
2. Or package them with your code.

The latter version is recommended as it respects the classloader management in Flink.

### Packaging dependencies with your usercode with Maven

To provide these dependencies not included by Flink we suggest two options with Maven.

1. The maven assembly plugin builds a so-called uber-jar (executable jar) containing all your dependencies.
The assembly configuration is straight-forward, but the resulting jar might become bulky.
See [maven-assembly-plugin](http://maven.apache.org/plugins/maven-assembly-plugin/usage.html) for further information.
2. The maven unpack plugin unpacks the relevant parts of the dependencies and
then packages it with your code.

Using the latter approach in order to bundle the Kafka connector, `flink-connector-kafka`
you would need to add the classes from both the connector and the Kafka API itself. Add
the following to your plugins section.

~~~xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-dependency-plugin</artifactId>
    <version>2.9</version>
    <executions>
        <execution>
            <id>unpack</id>
            <!-- executed just before the package phase -->
            <phase>prepare-package</phase>
            <goals>
                <goal>unpack</goal>
            </goals>
            <configuration>
                <artifactItems>
                    <!-- For Flink connector classes -->
                    <artifactItem>
                        <groupId>org.apache.flink</groupId>
                        <artifactId>flink-connector-kafka</artifactId>
                        <version>{{ site.version }}</version>
                        <type>jar</type>
                        <overWrite>false</overWrite>
                        <outputDirectory>${project.build.directory}/classes</outputDirectory>
                        <includes>org/apache/flink/**</includes>
                    </artifactItem>
                    <!-- For Kafka API classes -->
                    <artifactItem>
                        <groupId>org.apache.kafka</groupId>
                        <artifactId>kafka_<YOUR_SCALA_VERSION></artifactId>
                        <version><YOUR_KAFKA_VERSION></version>
                        <type>jar</type>
                        <overWrite>false</overWrite>
                        <outputDirectory>${project.build.directory}/classes</outputDirectory>
                        <includes>kafka/**</includes>
                    </artifactItem>
                </artifactItems>
            </configuration>
        </execution>
    </executions>
</plugin>
~~~

Now when running `mvn clean package` the produced jar includes the required dependencies.

{% top %}
