---
title: "并行执行"
weight: 31
type: docs
aliases:
  - /zh/dev/parallel.html
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

# 并行执行

本节描述了在 Flink 中配置程序的并行执行。一个 Flink 程序由多个任务 task 组成（转换/算子、数据源和数据接收器）。一个 task 包括多个并行执行的实例，且每一个实例都处理 task 输入数据的一个子集。一个 task 的并行实例数被称为该 task 的 *并行度* (parallelism)。

使用 [savepoints]({{< ref "docs/ops/state/savepoints" >}}) 时，应该考虑设置最大并行度。当作业从一个 savepoint 恢复时，你可以改变特定算子或着整个程序的并行度，并且此设置会限定整个程序的并行度的上限。由于在 Flink 内部将状态划分为了 key-groups，且性能所限不能无限制地增加 key-groups，因此设定最大并行度是有必要的。

* toc


## 设置并行度

一个 task 的并行度可以从多个层次指定：

### 算子层次

单个算子、数据源和数据接收器的并行度可以通过调用 `setParallelism()`方法来指定。如下所示：

{{< tabs "c7a9fbbc-ff25-4bee-847a-9851c0609acc" >}}
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

### 执行环境层次

如[此节]({{< ref "docs/dev/datastream/overview" >}}#anatomy-of-a-flink-program)所描述，Flink 程序运行在执行环境的上下文中。执行环境为所有执行的算子、数据源、数据接收器 (data sink) 定义了一个默认的并行度。可以显式配置算子层次的并行度去覆盖执行环境的并行度。

可以通过调用 `setParallelism()` 方法指定执行环境的默认并行度。如果想以并行度`3`来执行所有的算子、数据源和数据接收器。可以在执行环境上设置默认并行度，如下所示：

{{< tabs "1ca5bc75-0519-418b-a28d-c9ef7afd5ac9" >}}
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

### 客户端层次

将作业提交到 Flink 时可在客户端设定其并行度。客户端可以是 Java 或 Scala 程序，Flink 的命令行接口（CLI）就是一种典型的客户端。

在 CLI 客户端中，可以通过 `-p` 参数指定并行度，例如：

    ./bin/flink run -p 10 ../examples/*WordCount-java*.jar


在 Java/Scala 程序中，可以通过如下方式指定并行度：

{{< tabs "59257013-dbf1-41d8-a719-72ace65f63ff" >}}
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
Python API 中尚不支持该特性。
```
{{< /tab >}}
{{< /tabs >}}


### 系统层次

可以通过设置 `./conf/flink-conf.yaml` 文件中的 `parallelism.default` 参数，在系统层次来指定所有执行环境的默认并行度。你可以通过查阅[配置文档]({{< ref "docs/deployment/config" >}})获取更多细节。


## 设置最大并行度

最大并行度可以在所有设置并行度的地方进行设定（客户端和系统层次除外）。与调用 `setParallelism()` 方法修改并行度相似，你可以通过调用 `setMaxParallelism()` 方法来设定最大并行度。

默认的最大并行度等于将 `operatorParallelism + (operatorParallelism / 2)` 值四舍五入到大于等于该值的一个整型值，并且这个整型值是 `2` 的幂次方，注意默认最大并行度下限为 `128`，上限为 `32768`。

{{< hint warning >}} 
为最大并行度设置一个非常大的值将会降低性能，因为一些 state backends 需要维持内部的数据结构，而这些数据结构将会随着 key-groups 的数目而扩张（key-group 是状态重新分配的最小单元）。

从之前的作业恢复时，改变该作业的最大并发度将会导致状态不兼容。
{{< /hint >}}

{{< top >}}
