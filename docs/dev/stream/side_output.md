---
title: "Side Outputs"
nav-title: "Side Outputs"
nav-parent_id: streaming
nav-pos: 36
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

In addition to the main stream that results from `DataStream` operations, you can also produce any
number of additional side output result streams. The type of data in the result streams does not
have to match the type of data in the main stream and the types of the different side outputs can
also differ. This operation can be useful when you want to split a stream of data where you would
normally have to replicate the stream and then filter out from each stream the data that you don't
want to have.

When using side outputs, you first need to define an `OutputTag` that will be used to identify a
side output stream:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
// this needs to be an anonymous inner class, so that we can analyze the type
OutputTag<String> outputTag = new OutputTag<String>("side-output") {};
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val outputTag = OutputTag[String]("side-output")
{% endhighlight %}
</div>
</div>

Notice how the `OutputTag` is typed according to the type of elements that the side output stream
contains.

Emitting data to a side output is possible from the following functions:

- [ProcessFunction]({% link dev/stream/operators/process_function.md %})
- [KeyedProcessFunction]({% link dev/stream/operators/process_function.md %}#the-keyedprocessfunction)
- CoProcessFunction
- KeyedCoProcessFunction
- [ProcessWindowFunction]({% link dev/stream/operators/windows.md %}#processwindowfunction)
- ProcessAllWindowFunction

You can use the `Context` parameter, which is exposed to users in the above functions, to emit
data to a side output identified by an `OutputTag`. Here is an example of emitting side output
data from a `ProcessFunction`:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
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
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

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
{% endhighlight %}
</div>
</div>

For retrieving the side output stream you use `getSideOutput(OutputTag)`
on the result of the `DataStream` operation. This will give you a `DataStream` that is typed
to the result of the side output stream:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
final OutputTag<String> outputTag = new OutputTag<String>("side-output"){};

SingleOutputStreamOperator<Integer> mainDataStream = ...;

DataStream<String> sideOutputStream = mainDataStream.getSideOutput(outputTag);
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val outputTag = OutputTag[String]("side-output")

val mainDataStream = ...

val sideOutputStream: DataStream[String] = mainDataStream.getSideOutput(outputTag)
{% endhighlight %}
</div>
</div>

{% top %}
