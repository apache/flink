---
title:  "Hadoop Integration"
nav-title: Hadoop Integration
nav-parent_id: deployment
nav-pos: 8
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

## Referencing a Hadoop configuration

You can reference a Hadoop configuration by setting the environment variable `HADOOP_CONF_DIR`.

```sh
HADOOP_CONF_DIR=/path/to/etc/hadoop
```

Referencing the HDFS configuration in the [Flink configuration]({{ site.baseurl }}/ops/config.html#hdfs) is deprecated.

Another way to provide the Hadoop configuration is to have it on the class path of the Flink process, see more details below.

## Adding Hadoop Classpaths

The required classes to use Hadoop should be available in the `lib/` folder of the Flink installation
(on all machines running Flink) unless Flink is built with [Hadoop shaded dependencies]({{ site.baseurl }}/flinkDev/building.html#pre-bundled-versions).

If putting the files into the directory is not possible, Flink also respects
the `HADOOP_CLASSPATH` environment variable to add Hadoop jar files to the classpath.

Flink will use the environment variable `HADOOP_CLASSPATH` to augment the
classpath that is used when starting Flink components such as the Client,
JobManager, or TaskManager. Most Hadoop distributions and cloud environments
will not set this variable by default so if the Hadoop classpath should be
picked up by Flink the environment variable must be exported on all machines
that are running Flink components.

When running on YARN, this is usually not a problem because the components
running inside YARN will be started with the Hadoop classpaths, but it can
happen that the Hadoop dependencies must be in the classpath when submitting a
job to YARN. For this, it's usually enough to do a

{% highlight bash %}
export HADOOP_CLASSPATH=`hadoop classpath`
{% endhighlight %}

in the shell. Note that `hadoop` is the hadoop binary and that `classpath` is an argument that will make it print the configured Hadoop classpath.

Putting the Hadoop configuration in the same class path as the Hadoop libraries makes Flink pick up that configuration.

## Running a job locally

To run a job locally as one JVM process using the mini cluster, the required hadoop dependencies have to be explicitly
added to the classpath of the started JVM process.

To run an application using maven (also from IDE as a maven project), the required hadoop dependencies can be added
as provided to the pom.xml, e.g.:

```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>2.8.3</version>
    <scope>provided</scope>
</dependency>
```

This way it should work both in local and cluster run where the provided dependencies are added elsewhere as described before.

To run or debug an application in IntelliJ Idea the provided dependencies can be included to the class path
in the "Run|Edit Configurations" window.

{% top %}
