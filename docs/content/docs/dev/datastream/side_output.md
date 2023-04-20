---
title: "Side Outputs"
weight: 37
type: docs
bookToc: false
aliases:
  - /dev/stream/side_output.html
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

# Side Outputs

In addition to the main stream that results from `DataStream` operations, you can also produce any
number of additional side output result streams. The type of data in the result streams does not
have to match the type of data in the main stream and the types of the different side outputs can
also differ. This operation can be useful when you want to split a stream of data where you would
normally have to replicate the stream and then filter out from each stream the data that you don't
want to have.

When using side outputs, you first need to define an `OutputTag` that will be used to identify a
side output stream:

{{< tabs "7cd122cb-2996-4d7c-97b4-e2787c51313e" >}}
{{< tab "Java" >}}

```java
// this needs to be an anonymous inner class, so that we can analyze the type
OutputTag<String> outputTag = new OutputTag<String>("side-output") {};
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val outputTag = OutputTag[String]("side-output")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
output_tag = OutputTag("side-output", Types.STRING())
```
{{< /tab >}}
{{< /tabs >}}

Notice how the `OutputTag` is typed according to the type of elements that the side output stream
contains.

Emitting data to a side output is possible from the following functions:

- [ProcessFunction]({{< ref "docs/dev/datastream/operators/process_function" >}})
- [KeyedProcessFunction]({{< ref "docs/dev/datastream/operators/process_function" >}}#the-keyedprocessfunction)
- CoProcessFunction
- KeyedCoProcessFunction
- [ProcessWindowFunction]({{< ref "docs/dev/datastream/operators/windows" >}}#processwindowfunction)
- ProcessAllWindowFunction

You can use the `Context` parameter, which is exposed to users in the above functions, to emit
data to a side output identified by an `OutputTag`. Here is an example of emitting side output
data from a `ProcessFunction`:

{{< tabs "ef176025-b1ae-4e4b-aa1c-14c9ea1f048e" >}}
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
        // emit data to regular output
        out.collect(value);

        // emit data to side output
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
      // emit data to regular output
      out.collect(value)

      // emit data to side output
      ctx.output(outputTag, "sideout-" + String.valueOf(value))
    }
  })
```
{{< /tab >}}
{{< tab "Python" >}}
```python
input = ...  # type: DataStream
output_tag = OutputTag("side-output", Types.STRING())

class MyProcessFunction(ProcessFunction):

    def process_element(self, value: int, ctx: ProcessFunction.Context):
        # emit data to regular output
        yield value

        # emit data to side output
        yield output_tag, "sideout-" + str(value)


main_data_stream = input \
    .process(MyProcessFunction(), Types.INT())
```
{{< /tab >}}
{{< /tabs >}}

For retrieving the side output stream you use `getSideOutput(OutputTag)`
on the result of the `DataStream` operation. This will give you a `DataStream` that is typed
to the result of the side output stream:

{{< tabs "bdae46a7-6c33-427b-84bd-53d06fd0a8af" >}}
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
{{< tab "Python" >}}
```python
output_tag = OutputTag("side-output", Types.STRING())

main_data_stream = ...  # type: DataStream

side_output_stream = main_data_stream.get_side_output(output_tag)  # type: DataStream
```
{{< /tab >}}
{{< /tabs >}}

<span class="label label-info">Note</span> If it produces side output, `get_side_output(OutputTag)`
must be called in Python API. Otherwise, the result of side output stream will be output into the
main stream which is unexpected and may fail the job when the data types are different.

{{< top >}}
