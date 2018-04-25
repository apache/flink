---
title:  "Cluster Execution"
nav-parent_id: batch
nav-pos: 12
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

Please refer to the [Command Line Interface]({{ site.baseurl }}/ops/cli.html) documentation for
details.

## Remote Environment

The remote environment lets you execute Flink Java programs on a cluster
directly. The remote environment points to the cluster on which you want to
execute the program.

### Maven Dependency

If you are developing your program as a Maven project, you have to add the
`flink-clients` module using this dependency:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients{{ site.scala_version_suffix }}</artifactId>
  <version>{{ site.version }}</version>
</dependency>
{% endhighlight %}

### Example

The following illustrates the use of the `RemoteEnvironment`:

{% highlight java %}
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
{% endhighlight %}

Note that the program contains custom user code and hence requires a JAR file with
the classes of the code attached. The constructor of the remote environment
takes the path(s) to the JAR file(s).

{% top %}
