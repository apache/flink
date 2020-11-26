---
title:  "本地执行"
nav-parent_id: batch
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

Flink can run on a single machine, even in a single Java Virtual Machine. This allows users to test and debug Flink programs locally. This section gives an overview of the local execution mechanisms.

The local environments and executors allow you to run Flink programs in a local Java Virtual Machine, or with within any JVM as part of existing programs. Most examples can be launched locally by simply hitting the "Run" button of your IDE.

There are two different kinds of local execution supported in Flink. The `LocalExecutionEnvironment` is starting the full Flink runtime, including a JobManager and a TaskManager. These include memory management and all the internal algorithms that are executed in the cluster mode.

The `CollectionEnvironment` is executing the Flink program on Java collections. This mode will not start the full Flink runtime, so the execution is very low-overhead and lightweight. For example a `DataSet.map()`-transformation will be executed by applying the `map()` function to all elements in a Java list.

* TOC
{:toc}


## Debugging

If you are running Flink programs locally, you can also debug your program like any other Java program. You can either use `System.out.println()` to write out some internal variables or you can use the debugger. It is possible to set breakpoints within `map()`, `reduce()` and all the other methods.
Please also refer to the [debugging section]({% link dev/batch/index.zh.md %}#debugging) in the Java API documentation for a guide to testing and local debugging utilities in the Java API.

## Maven Dependency

If you are developing your program in a Maven project, you have to add the `flink-clients` module using this dependency:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
</dependency>
{% endhighlight %}

## Local Environment

The `LocalEnvironment` is a handle to local execution for Flink programs. Use it to run a program within a local JVM - standalone or embedded in other programs.

The local environment is instantiated via the method `ExecutionEnvironment.createLocalEnvironment()`. By default, it will use as many local threads for execution as your machine has CPU cores (hardware contexts). You can alternatively specify the desired parallelism. The local environment can be configured to log to the console using `enableLogging()`/`disableLogging()`.

In most cases, calling `ExecutionEnvironment.getExecutionEnvironment()` is the even better way to go. That method returns a `LocalEnvironment` when the program is started locally (outside the command line interface), and it returns a pre-configured environment for cluster execution, when the program is invoked by the [command line interface]({% link deployment/cli.zh.md %}).

{% highlight java %}
public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

    DataSet<String> data = env.readTextFile("file:///path/to/file");

    data
        .filter(new FilterFunction<String>() {
            public boolean filter(String value) {
                return value.startsWith("http://");
            }
        })
        .writeAsText("file:///path/to/result");

    JobExecutionResult res = env.execute();
}
{% endhighlight %}

The `JobExecutionResult` object, which is returned after the execution finished, contains the program runtime and the accumulator results.

The `LocalEnvironment` allows also to pass custom configuration values to Flink.

{% highlight java %}
Configuration conf = new Configuration();
conf.setFloat(ConfigConstants.TASK_MANAGER_MEMORY_FRACTION_KEY, 0.5f);
final ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment(conf);
{% endhighlight %}

*Note:* The local execution environments do not start any web frontend to monitor the execution.

## Collection Environment

The execution on Java Collections using the `CollectionEnvironment` is a low-overhead approach for executing Flink programs. Typical use-cases for this mode are automated tests, debugging and code re-use.

Users can use algorithms implemented for batch processing also for cases that are more interactive. A slightly changed variant of a Flink program could be used in a Java Application Server for processing incoming requests.

**Skeleton for Collection-based execution**

{% highlight java %}
public static void main(String[] args) throws Exception {
    // initialize a new Collection-based execution environment
    final ExecutionEnvironment env = new CollectionEnvironment();

    DataSet<User> users = env.fromCollection( /* get elements from a Java Collection */);

    /* Data Set transformations ... */

    // retrieve the resulting Tuple2 elements into a ArrayList.
    Collection<...> result = new ArrayList<...>();
    resultDataSet.output(new LocalCollectionOutputFormat<...>(result));

    // kick off execution.
    env.execute();

    // Do some work with the resulting ArrayList (=Collection).
    for(... t : result) {
        System.err.println("Result = "+t);
    }
}
{% endhighlight %}

The `flink-examples-batch` module contains a full example, called `CollectionExecutionExample`.

Please note that the execution of the collection-based Flink programs is only possible on small data, which fits into the JVM heap. The execution on collections is not multi-threaded, only one thread is used.

{% top %}
