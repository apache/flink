---
title: "Managing Execution"
nav-parent_id: dev
nav-pos: 60
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

Execution Configuration
-----------------------

The `StreamExecutionEnvironment` contains the `ExecutionConfig` which allows to set job specific configuration values for the runtime.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
ExecutionConfig executionConfig = env.getConfig();
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
var executionConfig = env.getConfig
{% endhighlight %}
</div>
</div>

The following configuration options are available: (the default is bold)

- **`enableClosureCleaner()`** / `disableClosureCleaner()`. The closure cleaner is enabled by default. The closure cleaner removes unneeded references to the surrounding class of anonymous functions inside Flink programs.
With the closure cleaner disabled, it might happen that an anonymous user function is referencing the surrounding class, which is usually not Serializable. This will lead to exceptions by the serializer.

- `getParallelism()` / `setParallelism(int parallelism)` Set the default parallelism for the job.

- `getMaxParallelism()` / `setMaxParallelism(int parallelism)` Set the default maximum parallelism for the job. This setting determines the maximum degree of parallelism and specifies the upper limit for dynamic scaling.

- `getNumberOfExecutionRetries()` / `setNumberOfExecutionRetries(int numberOfExecutionRetries)` Sets the number of times that failed tasks are re-executed. A value of zero effectively disables fault tolerance. A value of `-1` indicates that the system default value (as defined in the configuration) should be used.

- `getExecutionRetryDelay()` / `setExecutionRetryDelay(long executionRetryDelay)` Sets the delay in milliseconds that the system waits after a job has failed, before re-executing it. The delay starts after all tasks have been successfully been stopped on the TaskManagers, and once the delay is past, the tasks are re-started. This parameter is useful to delay re-execution in order to let certain time-out related failures surface fully (like broken connections that have not fully timed out), before attempting a re-execution and immediately failing again due to the same problem. This parameter only has an effect if the number of execution re-tries is one or more.

- `getExecutionMode()` / `setExecutionMode()`. The default execution mode is PIPELINED. Sets the execution mode to execute the program. The execution mode defines whether data exchanges are performed in a batch or on a pipelined manner.

- `enableForceKryo()` / **`disableForceKryo`**. Kryo is not forced by default. Forces the GenericTypeInformation to use the Kryo serializer for POJOS even though we could analyze them as a POJO. In some cases this might be preferable. For example, when Flink's internal serializers fail to handle a POJO properly.

- `enableForceAvro()` / **`disableForceAvro()`**. Avro is not forced by default. Forces the Flink AvroTypeInformation to use the Avro serializer instead of Kryo for serializing Avro POJOs.

- `enableObjectReuse()` / **`disableObjectReuse()`** By default, objects are not reused in Flink. Enabling the object reuse mode will instruct the runtime to reuse user objects for better performance. Keep in mind that this can lead to bugs when the user-code function of an operation is not aware of this behavior.

- **`enableSysoutLogging()`** / `disableSysoutLogging()` JobManager status updates are printed to `System.out` by default. This setting allows to disable this behavior.

- `getGlobalJobParameters()` / `setGlobalJobParameters()` This method allows users to set custom objects as a global configuration for the job. Since the `ExecutionConfig` is accessible in all user defined functions, this is an easy method for making configuration globally available in a job.

- `addDefaultKryoSerializer(Class<?> type, Serializer<?> serializer)` Register a Kryo serializer instance for the given `type`.

- `addDefaultKryoSerializer(Class<?> type, Class<? extends Serializer<?>> serializerClass)` Register a Kryo serializer class for the given `type`.

- `registerTypeWithKryoSerializer(Class<?> type, Serializer<?> serializer)` Register the given type with Kryo and specify a serializer for it. By registering a type with Kryo, the serialization of the type will be much more efficient.

- `registerKryoType(Class<?> type)` If the type ends up being serialized with Kryo, then it will be registered at Kryo to make sure that only tags (integer IDs) are written. If a type is not registered with Kryo, its entire class-name will be serialized with every instance, leading to much higher I/O costs.

- `registerPojoType(Class<?> type)` Registers the given type with the serialization stack. If the type is eventually serialized as a POJO, then the type is registered with the POJO serializer. If the type ends up being serialized with Kryo, then it will be registered at Kryo to make sure that only tags are written. If a type is not registered with Kryo, its entire class-name will be serialized with every instance, leading to much higher I/O costs.

Note that types registered with `registerKryoType()` are not available to Flink's Kryo serializer instance.

- `disableAutoTypeRegistration()` Automatic type registration is enabled by default. The automatic type registration is registering all types (including sub-types) used by usercode with Kryo and the POJO serializer.

- `setTaskCancellationInterval(long interval)` Sets the the interval (in milliseconds) to wait between consecutive attempts to cancel a running task. When a task is canceled a new thread is created which periodically calls `interrupt()` on the task thread, if the task thread does not terminate within a certain time. This parameter refers to the time between consecutive calls to `interrupt()` and is set by default to **30000** milliseconds, or **30 seconds**.

The `RuntimeContext` which is accessible in `Rich*` functions through the `getRuntimeContext()` method also allows to access the `ExecutionConfig` in all user defined functions.

{% top %}

Program Packaging and Distributed Execution
-----------------------------------------

As described earlier, Flink programs can be executed on
clusters by using a `remote environment`. Alternatively, programs can be packaged into JAR Files
(Java Archives) for execution. Packaging the program is a prerequisite to executing them through the
[command line interface]({{ site.baseurl }}/setup/cli.html).

#### Packaging Programs

To support execution from a packaged JAR file via the command line or web interface, a program must
use the environment obtained by `StreamExecutionEnvironment.getExecutionEnvironment()`. This environment
will act as the cluster's environment when the JAR is submitted to the command line or web
interface. If the Flink program is invoked differently than through these interfaces, the
environment will act like a local environment.

To package the program, simply export all involved classes as a JAR file. The JAR file's manifest
must point to the class that contains the program's *entry point* (the class with the public
`main` method). The simplest way to do this is by putting the *main-class* entry into the
manifest (such as `main-class: org.apache.flinkexample.MyProgram`). The *main-class* attribute is
the same one that is used by the Java Virtual Machine to find the main method when executing a JAR
files through the command `java -jar pathToTheJarFile`. Most IDEs offer to include that attribute
automatically when exporting JAR files.


#### Packaging Programs through Plans

Additionally, we support packaging programs as *Plans*. Instead of defining a progam in the main
method and calling
`execute()` on the environment, plan packaging returns the *Program Plan*, which is a description of
the program's data flow. To do that, the program must implement the
`org.apache.flink.api.common.Program` interface, defining the `getPlan(String...)` method. The
strings passed to that method are the command line arguments. The program's plan can be created from
the environment via the `ExecutionEnvironment#createProgramPlan()` method. When packaging the
program's plan, the JAR manifest must point to the class implementing the
`org.apache.flinkapi.common.Program` interface, instead of the class with the main method.


#### Summary

The overall procedure to invoke a packaged program is as follows:

1. The JAR's manifest is searched for a *main-class* or *program-class* attribute. If both
attributes are found, the *program-class* attribute takes precedence over the *main-class*
attribute. Both the command line and the web interface support a parameter to pass the entry point
class name manually for cases where the JAR manifest contains neither attribute.

2. If the entry point class implements the `org.apache.flinkapi.common.Program`, then the system
calls the `getPlan(String...)` method to obtain the program plan to execute.

3. If the entry point class does not implement the `org.apache.flinkapi.common.Program` interface,
the system will invoke the main method of the class.

{% top %}

Parallel Execution
------------------

This section describes how the parallel execution of programs can be configured in Flink. A Flink
program consists of multiple tasks (transformations/operators, data sources, and sinks). A task is split into
several parallel instances for execution and each parallel instance processes a subset of the task's
input data. The number of parallel instances of a task is called its *parallelism*.


The parallelism of a task can be specified in Flink on different levels.

### Operator Level

The parallelism of an individual operator, data source, or data sink can be defined by calling its
`setParallelism()` method.  For example, like this:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> text = [...]
DataStream<Tuple2<String, Integer>> wordCounts = text
    .flatMap(new LineSplitter())
    .keyBy(0)
    .timeWindow(Time.seconds(5))
    .sum(1).setParallelism(5);

wordCounts.print();

env.execute("Word Count Example");
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment

val text = [...]
val wordCounts = text
    .flatMap{ _.split(" ") map { (_, 1) } }
    .keyBy(0)
    .timeWindow(Time.seconds(5))
    .sum(1).setParallelism(5)
wordCounts.print()

env.execute("Word Count Example")
{% endhighlight %}
</div>
</div>

### Execution Environment Level

As mentioned [here](#anatomy-of-a-flink-program) Flink programs are executed in the context
of an execution environment. An
execution environment defines a default parallelism for all operators, data sources, and data sinks
it executes. Execution environment parallelism can be overwritten by explicitly configuring the
parallelism of an operator.

The default parallelism of an execution environment can be specified by calling the
`setParallelism()` method. To execute all operators, data sources, and data sinks with a parallelism
of `3`, set the default parallelism of the execution environment as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(3);

DataStream<String> text = [...]
DataStream<Tuple2<String, Integer>> wordCounts = [...]
wordCounts.print();

env.execute("Word Count Example");
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setParallelism(3)

val text = [...]
val wordCounts = text
    .flatMap{ _.split(" ") map { (_, 1) } }
    .keyBy(0)
    .timeWindow(Time.seconds(5))
    .sum(1)
wordCounts.print()

env.execute("Word Count Example")
{% endhighlight %}
</div>
</div>

### Client Level

The parallelism can be set at the Client when submitting jobs to Flink. The
Client can either be a Java or a Scala program. One example of such a Client is
Flink's Command-line Interface (CLI).

For the CLI client, the parallelism parameter can be specified with `-p`. For
example:

    ./bin/flink run -p 10 ../examples/*WordCount-java*.jar


In a Java/Scala program, the parallelism is set as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

try {
    PackagedProgram program = new PackagedProgram(file, args);
    InetSocketAddress jobManagerAddress = RemoteExecutor.getInetFromHostport("localhost:6123");
    Configuration config = new Configuration();

    Client client = new Client(jobManagerAddress, config, program.getUserCodeClassLoader());

    // set the parallelism to 10 here
    client.run(program, 10, true);

} catch (ProgramInvocationException e) {
    e.printStackTrace();
}

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
try {
    PackagedProgram program = new PackagedProgram(file, args)
    InetSocketAddress jobManagerAddress = RemoteExecutor.getInetFromHostport("localhost:6123")
    Configuration config = new Configuration()

    Client client = new Client(jobManagerAddress, new Configuration(), program.getUserCodeClassLoader())

    // set the parallelism to 10 here
    client.run(program, 10, true)

} catch {
    case e: Exception => e.printStackTrace
}
{% endhighlight %}
</div>
</div>


### System Level

A system-wide default parallelism for all execution environments can be defined by setting the
`parallelism.default` property in `./conf/flink-conf.yaml`. See the
[Configuration]({{ site.baseurl }}/setup/config.html) documentation for details.

{% top %}

Execution Plans
---------------

Depending on various parameters such as data size or number of machines in the cluster, Flink's
optimizer automatically chooses an execution strategy for your program. In many cases, it can be
useful to know how exactly Flink will execute your program.

__Plan Visualization Tool__

Flink comes packaged with a visualization tool for execution plans. The HTML document containing
the visualizer is located under ```tools/planVisualizer.html```. It takes a JSON representation of
the job execution plan and visualizes it as a graph with complete annotations of execution
strategies.

The following code shows how to print the execution plan JSON from your program:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

...

System.out.println(env.getExecutionPlan());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment

...

println(env.getExecutionPlan())
{% endhighlight %}
</div>
</div>


To visualize the execution plan, do the following:

1. **Open** ```planVisualizer.html``` with your web browser,
2. **Paste** the JSON string into the text field, and
3. **Press** the draw button.

After these steps, a detailed execution plan will be visualized.

<img alt="A flink job execution graph." src="{{ site.baseurl }}/fig/plan_visualizer.png" width="80%">


__Web Interface__

Flink offers a web interface for submitting and executing jobs. The interface is part of the JobManager's
web interface for monitoring, per default running on port 8081. Job submission via this interfaces requires
that you have set `jobmanager.web.submit.enable: true` in `flink-conf.yaml`.

You may specify program arguments before the job is executed. The plan visualization enables you to show
the execution plan before executing the Flink job.

{% top %}
