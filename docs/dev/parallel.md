---
title: "Parallel Execution"
nav-parent_id: execution
nav-pos: 30
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

This section describes how the parallel execution of programs can be configured in Flink. A Flink
program consists of multiple tasks (transformations/operators, data sources, and sinks). A task is split into
several parallel instances for execution and each parallel instance processes a subset of the task's
input data. The number of parallel instances of a task is called its *parallelism*.

If you want to use [savepoints]({{ site.baseurl }}/ops/state/savepoints.html) you should also consider
setting a maximum parallelism (or `max parallelism`). When restoring from a savepoint you can
change the parallelism of specific operators or the whole program and this setting specifies
an upper bound on the parallelism. This is required because Flink internally partitions state
into key-groups and we cannot have `+Inf` number of key-groups because this would be detrimental
to performance.

* toc
{:toc}

## Setting the Parallelism

The parallelism of a task can be specified in Flink on different levels:

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

As mentioned [here]({{ site.baseurl }}/dev/api_concepts.html#anatomy-of-a-flink-program) Flink
programs are executed in the context of an execution environment. An
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
[Configuration]({{ site.baseurl }}/ops/config.html) documentation for details.

## Setting the Maximum Parallelism

The maximum parallelism can be set in places where you can also set a parallelism
(except client level and system level). Instead of calling `setParallelism()` you call
`setMaxParallelism()` to set the maximum parallelism.

The default setting for the maximum parallelism is roughly `operatorParallelism + (operatorParallelism / 2)` with
a lower bound of `128` and an upper bound of `32768`.

<span class="label label-danger">Attention</span> Setting the maximum parallelism to a very large
value can be detrimental to performance because some state backends have to keep internal data
structures that scale with the number of key-groups (which are the internal implementation mechanism for
rescalable state).


{% top %}
