---
title: "Parallel Execution"
weight: 31
type: docs
aliases:
  - /dev/parallel.html
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

# Parallel Execution

This section describes how the parallel execution of programs can be configured in Flink. A Flink
program consists of multiple tasks (transformations/operators, data sources, and sinks). A task is split into
several parallel instances for execution and each parallel instance processes a subset of the task's
input data. The number of parallel instances of a task is called its *parallelism*.

If you want to use [savepoints]({{< ref "docs/ops/state/savepoints" >}}) you should also consider
setting a maximum parallelism (or `max parallelism`). When restoring from a savepoint you can
change the parallelism of specific operators or the whole program and this setting specifies
an upper bound on the parallelism. This is required because Flink internally partitions state
into key-groups and we cannot have `+Inf` number of key-groups because this would be detrimental
to performance.

## Setting the Parallelism

The parallelism of a task can be specified in Flink on different levels:

### Operator Level

The parallelism of an individual operator, data source, or data sink can be defined by calling its
`setParallelism()` method.  For example, like this:

{{< tabs "d35e6dcb-7630-4e12-9ab7-2798c1c36002" >}}
{{< tab "Java" >}}
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> text = [...];
DataStream<Tuple2<String, Integer>> wordCounts = text
    .flatMap(new LineSplitter())
    .keyBy(value -> value.f0)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .sum(1).setParallelism(5);

wordCounts.print();

env.execute("Word Count Example");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment

val text = [...]
val wordCounts = text
    .flatMap{ _.split(" ") map { (_, 1) } }
    .keyBy(_._1)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .sum(1).setParallelism(5)
wordCounts.print()

env.execute("Word Count Example")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()

text = [...]
word_counts = text 
    .flat_map(lambda x: x.split(" ")) \
    .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
    .key_by(lambda i: i[0]) \
    .window(TumblingEventTimeWindows.of(Time.seconds(5))) \
    .reduce(lambda i, j: (i[0], i[1] + j[1])) \
    .set_parallelism(5)
word_counts.print()


env.execute("Word Count Example")
```
{{< /tab >}}
{{< /tabs >}}

### Execution Environment Level

As mentioned [here]({{< ref "docs/dev/datastream/overview" >}}#anatomy-of-a-flink-program) Flink
programs are executed in the context of an execution environment. An
execution environment defines a default parallelism for all operators, data sources, and data sinks
it executes. Execution environment parallelism can be overwritten by explicitly configuring the
parallelism of an operator.

The default parallelism of an execution environment can be specified by calling the
`setParallelism()` method. To execute all operators, data sources, and data sinks with a parallelism
of `3`, set the default parallelism of the execution environment as follows:

{{< tabs "f4258714-1999-4a03-b6ea-52b9c9910f6c" >}}
{{< tab "Java" >}}
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setParallelism(3);

DataStream<String> text = [...];
DataStream<Tuple2<String, Integer>> wordCounts = [...];
wordCounts.print();

env.execute("Word Count Example");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setParallelism(3)

val text = [...]
val wordCounts = text
    .flatMap{ _.split(" ") map { (_, 1) } }
    .keyBy(_._1)
    .window(TumblingEventTimeWindows.of(Time.seconds(5)))
    .sum(1)
wordCounts.print()

env.execute("Word Count Example")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(3)

text = [...]
word_counts = text
    .flat_map(lambda x: x.split(" ")) \
    .map(lambda i: (i, 1), output_type=Types.TUPLE([Types.STRING(), Types.INT()])) \
    .key_by(lambda i: i[0]) \
    .window(TumblingEventTimeWindows.of(Time.seconds(5))) \
    .reduce(lambda i, j: (i[0], i[1] + j[1]))
word_counts.print()


env.execute("Word Count Example")
```
{{< /tab >}}
{{< /tabs >}}

### Client Level

The parallelism can be set at the Client when submitting jobs to Flink. The
Client can either be a Java or a Scala program. One example of such a Client is
Flink's Command-line Interface (CLI).

For the CLI client, the parallelism parameter can be specified with `-p`. For
example:

    ./bin/flink run -p 10 ../examples/*WordCount-java*.jar


In a Java/Scala program, the parallelism is set as follows:

{{< tabs "54c4adac-7461-40e9-b85c-69bbc0b3a36d" >}}
{{< tab "Java" >}}
```java

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

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
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
```
{{< /tab >}}
{{< tab "Python" >}}
```python
Still not supported in Python API.
```
{{< /tab >}}
{{< /tabs >}}


### System Level

A system-wide default parallelism for all execution environments can be defined by setting the
`parallelism.default` property in `./conf/flink-conf.yaml`. See the
[Configuration]({{< ref "docs/deployment/config" >}}) documentation for details.

## Setting the Maximum Parallelism

The maximum parallelism can be set in places where you can also set a parallelism
(except client level and system level). Instead of calling `setParallelism()` you call
`setMaxParallelism()` to set the maximum parallelism.

The default setting for the maximum parallelism is roughly `operatorParallelism + (operatorParallelism / 2)` with
a lower bound of `128` and an upper bound of `32768`.

{{< hint warning >}}
Setting the maximum parallelism to a very large
value can be detrimental to performance because some state backends have to keep internal data
structures that scale with the number of key-groups (which are the internal implementation mechanism for
rescalable state).

Changing the maximum parallelism explicitly when recovery from original job will lead to state incompatibility.
{{< /hint >}}

{{< top >}}
