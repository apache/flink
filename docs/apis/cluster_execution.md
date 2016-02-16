---
title:  "Cluster Execution"
# Top-level navigation
top-nav-group: apis
top-nav-pos: 8
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

* This will be replaced by the TOC
{:toc}

Flink programs can run distributed on clusters of many machines. There
are two ways to send a program to a cluster for execution:

## Command Line Interface

The command line interface lets you submit packaged programs (JARs) to a cluster
(or single machine setup).

Please refer to the [Command Line Interface](cli.html) documentation for
details.

## Remote Environment

The remote environment lets you execute Flink Java programs on a cluster
directly. The remote environment points to the cluster on which you want to
execute the program.

### Maven Dependency

If you are developing your program as a Maven project, you have to add the
`flink-clients` module using this dependency:

~~~xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients{{ site.scala_version_suffix }}</artifactId>
  <version>{{ site.version }}</version>
</dependency>
~~~

### Example

The following illustrates the use of the `RemoteEnvironment`:

~~~java
public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment
        .createRemoteEnvironment("flink-master", 6123, "/home/user/udfs.jar");

    DataSet<String> data = env.readTextFile("hdfs://path/to/file");

    data
        .filter(new FilterFunction<String>() {
            public boolean filter(String value) {
                return value.startsWith("http://");
            }
        })
        .writeAsText("hdfs://path/to/result");

    env.execute();
}
~~~

Note that the program contains custom user code and hence requires a JAR file with
the classes of the code attached. The constructor of the remote environment
takes the path(s) to the JAR file(s).

## Linking with modules not contained in the binary distribution

The binary distribution contains jar packages in the `lib` folder that are automatically
provided to the classpath of your distrbuted programs. Almost all of Flink classes are
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
