---
title: "旁路输出"
weight: 37
type: docs
bookToc: false
aliases:
  - /zh/dev/stream/side_output.html
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

# 旁路输出



除了由 `DataStream` 操作产生的主要流之外，你还可以产生任意数量的旁路输出结果流。结果流中的数据类型不必与主要流中的数据类型相匹配，并且不同旁路输出的类型也可以不同。当你需要拆分数据流时，通常必须复制该数据流，然后从每个流中过滤掉不需要的数据，这个操作十分有用。

使用旁路输出时，首先需要定义用于标识旁路输出流的 `OutputTag`：

{{< tabs "fce67915-0fda-4f9c-a486-0c9e12ae739f" >}}
{{< tab "Java" >}}

```java
// 这需要是一个匿名的内部类，以便我们分析类型
OutputTag<String> outputTag = new OutputTag<String>("side-output") {};
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val outputTag = OutputTag[String]("side-output")
```
{{< /tab >}}
{{< /tabs >}}

注意 `OutputTag` 是如何根据旁路输出流所包含的元素类型进行类型化的。

可以通过以下方法将数据发送到旁路输出：

- [ProcessFunction]({{< ref "docs/dev/datastream/operators/process_function" >}})
- [KeyedProcessFunction]({{< ref "docs/dev/datastream/operators/process_function" >}}#the-keyedprocessfunction)
- CoProcessFunction
- KeyedCoProcessFunction
- [ProcessWindowFunction]({{< ref "docs/dev/datastream/operators/windows" >}}#processwindowfunction)
- ProcessAllWindowFunction

你可以使用在上述方法中向用户暴露的 `Context` 参数，将数据发送到由 `OutputTag` 标识的旁路输出。这是从 `ProcessFunction` 发送数据到旁路输出的示例：

{{< tabs "b8f47d39-7ab2-4b68-ac86-69250453199e" >}}
{{< tab "Java" >}}

```java
DataStream<Integer> input = ...;

final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

SingleOutputStreamOperator<Integer> mainDataStream = input
  .process(new ProcessFunction<Integer, Integer>() {

      @Override
      public void processElement(
          Integer value,
          Context ctx,
          Collector<Integer> out) throws Exception {
        // 发送数据到主要的输出
        out.collect(value);

        // 发送数据到旁路输出
        ctx.output(outputTag, "sideout-" + String.valueOf(value));
      }
    });
```

{{< /tab >}}
{{< tab "Scala" >}}
```scala

val input: DataStream[Int] = ...
val outputTag = OutputTag[String]("side-output")

val mainDataStream = input
  .process(new ProcessFunction[Int, Int] {
    override def processElement(
        value: Int,
        ctx: ProcessFunction[Int, Int]#Context,
        out: Collector[Int]): Unit = {
      // 发送数据到主要的输出
      out.collect(value)

      // 发送数据到旁路输出
      ctx.output(outputTag, "sideout-" + String.valueOf(value))
    }
  })
```
{{< /tab >}}
{{< /tabs >}}

你可以在 `DataStream` 运算结果上使用 `getSideOutput(OutputTag)` 方法获取旁路输出流。这将产生一个与旁路输出流结果类型一致的 `DataStream`：

{{< tabs "ee95875c-8891-4e7f-8e6f-18792188d351" >}}
{{< tab "Java" >}}

```java
final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

SingleOutputStreamOperator<Integer> mainDataStream = ...;

DataStream<String> sideOutputStream = mainDataStream.getSideOutput(outputTag);
```

{{< /tab >}}
{{< tab "Scala" >}}
```scala
val outputTag = OutputTag[String]("side-output")

val mainDataStream = ...

val sideOutputStream: DataStream[String] = mainDataStream.getSideOutput(outputTag)
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
