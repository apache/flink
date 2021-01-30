---
title: "算子"
nav-id: streaming_operators
nav-show_overview: true
nav-parent_id: streaming
nav-pos: 9
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

用户通过算子能将一个或多个 DataStream 转换成新的 DataStream，在应用程序中可以将多个数据转换算子合并成一个复杂的数据流拓扑。

这部分内容将描述 Flink DataStream API 中基本的数据转换API，数据转换后各种数据分区方式，以及算子的链接策略。

* toc
{:toc}

<a name="datastream-transformations"/>

# 数据流转换

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
          <td><strong>Map</strong><br>DataStream &rarr; DataStream</td>
          <td>
            <p>输入一个数据并对应输出一个数据。 如下 map 函数将输入流的每一个数据乘2后输出:</p>
{% highlight java %}
DataStream<Integer> dataStream = //...
dataStream.map(new MapFunction<Integer, Integer>() {
    @Override
    public Integer map(Integer value) throws Exception {
        return 2 * value;
    }
});
{% endhighlight %}
          </td>
        </tr>

        <tr>
          <td><strong>FlatMap</strong><br>DataStream &rarr; DataStream</td>
          <td>
            <p>输入一个数据，产生并输出0个，1个或多个数据。 如下 flatmap 函数将输入的单个句子根据空格分隔成多个单词并逐个输出：</p>
{% highlight java %}
dataStream.flatMap(new FlatMapFunction<String, String>() {
    @Override
    public void flatMap(String value, Collector<String> out)
        throws Exception {
        for(String word: value.split(" ")){
            out.collect(word);
        }
    }
});
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Filter</strong><br>DataStream &rarr; DataStream</td>
          <td>
            <p>根据每个输入元素计算 boolean 函数，并保留函数输出值为 True 的元素。 如下是滤除值为0的过滤函数：
            </p>
{% highlight java %}
dataStream.filter(new FilterFunction<Integer>() {
    @Override
    public boolean filter(Integer value) throws Exception {
        return value != 0;
    }
});
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>KeyBy</strong><br>DataStream &rarr; KeyedStream</td>
          <td>
            <p>将一个流的数据分发到各个独立分区，相同 key 值的元素将分发到同一个分区。<em>keyBy()</em>内部使用 Hash 分发数据。Flink 支持多种方式<a href="{% link dev/stream/state/state.zh.md %}#keyed-datastream">指定 key</a>。</p>
            <p>
            这个转换操作将返回一个 <em>KeyedStream</em>，你需要通过它来使用<a href="{% link dev/stream/state/state.zh.md %}#keyed-state">keyed state</a>。</p>
{% highlight java %}
dataStream.keyBy(value -> value.getSomeKey()) // Key by field "someKey"
dataStream.keyBy(value -> value.f0) // Key by the first element of a Tuple
{% endhighlight %}
            <p>
            <span class="label label-danger">Attention</span>
            一个类如果出现以下情况，则<strong>不能作为 key</strong>：
    	    <ol>
    	    <li> 这个类是 POJO 但是没有重写 <em>hashCode()</em> 方法，仍旧依赖于 <em>Object.hashCode()</em> 实现。</li>
    	    <li> 这个类是数组类型。</li>
    	    </ol>
    	    </p>
          </td>
        </tr>
        <tr>
          <td><strong>Reduce</strong><br>KeyedStream &rarr; DataStream</td>
          <td>
            <p>以滚动的形式"压缩" KeyedStream ， 将当前元素和上一次压缩合并的值进行新一轮压缩并输出新的压缩值。
                    <br/>
            	<br/>
            <p>如下是对部分元素求和并生成一个新的数据流的压缩函数：</p>
{% highlight java %}
keyedStream.reduce(new ReduceFunction<Integer>() {
    @Override
    public Integer reduce(Integer value1, Integer value2)
    throws Exception {
        return value1 + value2;
    }
});
{% endhighlight %}
            </p>
          </td>
        </tr>
        <tr>
          <td><strong>Aggregations</strong><br>KeyedStream &rarr; DataStream</td>
          <td>
            <p>在 keyed stream 上滚动地聚合数据。min 和 minBy 的区别在于 min 直接返回最小值，而 minBy 返回在当前属性上拥有最小值的元素（max 和 maxBy 也一样）</p>
{% highlight java %}
keyedStream.sum(0);
keyedStream.sum("key");
keyedStream.min(0);
keyedStream.min("key");
keyedStream.max(0);
keyedStream.max("key");
keyedStream.minBy(0);
keyedStream.minBy("key");
keyedStream.maxBy(0);
keyedStream.maxBy("key");
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window</strong><br>KeyedStream &rarr; WindowedStream</td>
          <td>
            <p>窗口可以被定义在分区后的 KeyedStream 上。窗口将每个 key 中的数据根据某些条件（比如之前五秒的数据）组合起来。关于窗口的完整描述请见 <a href="windows.html">windows</a>。
{% highlight java %}
dataStream.keyBy(value -> value.f0).window(TumblingEventTimeWindows.of(Time.seconds(5))); // Last 5 seconds of data
{% endhighlight %}
        </p>
          </td>
        </tr>
        <tr>
          <td><strong>WindowAll</strong><br>DataStream &rarr; AllWindowedStream</td>
          <td>
              <p>窗口也可以被定义在普通的 DataStream 上。这种窗口将流中的所有事件都按照某些条件（比如之前五秒的数据）组合起来。关于窗口的完整描述请见 <a href="windows.html">windows</a>。</p>
              <p><strong>WARNING:</strong> 在很多情况下，这是一个 <strong>非并行</strong> 的转换。所有数据都会被 windowAll 算子汇集到一个 task 中。</p>
{% highlight java %}
dataStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5))); // Last 5 seconds of data
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window Apply</strong><br>WindowedStream &rarr; DataStream<br>AllWindowedStream &rarr; DataStream</td>
          <td>
            <p>对窗口使用函数。下面是一个手动对窗口内元素求和的函数。</p>
            <p><strong>注意：</strong>如果你使用的是 windowAll 转换，你需要用 AllWindowFunction 而不是 WindowFunction。</p>
{% highlight java %}
windowedStream.apply (new WindowFunction<Tuple2<String,Integer>, Integer, Tuple, Window>() {
    public void apply (Tuple tuple,
            Window window,
            Iterable<Tuple2<String, Integer>> values,
            Collector<Integer> out) throws Exception {
        int sum = 0;
        for (value t: values) {
            sum += t.f1;
        }
        out.collect (new Integer(sum));
    }
});

// applying an AllWindowFunction on non-keyed window stream
allWindowedStream.apply (new AllWindowFunction<Tuple2<String,Integer>, Integer, Window>() {
    public void apply (Window window,
            Iterable<Tuple2<String, Integer>> values,
            Collector<Integer> out) throws Exception {
        int sum = 0;
        for (value t: values) {
            sum += t.f1;
        }
        out.collect (new Integer(sum));
    }
});
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window Reduce</strong><br>WindowedStream &rarr; DataStream</td>
          <td>
            <p>对窗口使用 reduce 函数并返回 reduce 后的值。</p>
{% highlight java %}
windowedStream.reduce (new ReduceFunction<Tuple2<String,Integer>>() {
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
        return new Tuple2<String,Integer>(value1.f0, value1.f1 + value2.f1);
    }
});
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Aggregations on windows</strong><br>WindowedStream &rarr; DataStream</td>
          <td>
            <p>聚合窗口中的内容。min 和 minBy 的区别在于 min 直接返回最小值，而 minBy 返回在当前属性上拥有最小值的元素（max 和 maxBy 同理）</p>
{% highlight java %}
windowedStream.sum(0);
windowedStream.sum("key");
windowedStream.min(0);
windowedStream.min("key");
windowedStream.max(0);
windowedStream.max("key");
windowedStream.minBy(0);
windowedStream.minBy("key");
windowedStream.maxBy(0);
windowedStream.maxBy("key");
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Union</strong><br>DataStream* &rarr; DataStream</td>
          <td>
            <p>将两个或多个数据流合并成一个包含所有流的数据的新流。注意：如果一个数据流和自身进行合并，这个流中的每个数据将在合并后的流中出现两次。</p>
{% highlight java %}
dataStream.union(otherStream1, otherStream2, ...);
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window Join</strong><br>DataStream,DataStream &rarr; DataStream</td>
          <td>
            <p>根据指定的 key 和窗口，join 两个数据流。</p>
{% highlight java %}
dataStream.join(otherStream)
    .where(<key selector>).equalTo(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(3)))
    .apply (new JoinFunction () {...});
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Interval Join</strong><br>KeyedStream,KeyedStream &rarr; DataStream</td>
          <td>
            <p>将分别属于两个 keyed stream 的元素 e1 和 e2 根据一个共同的 key 和指定的时间范围 Join 在一起，使 e1.timestamp + lowerBound <= e2.timestamp <= e1.timestamp + upperBound</p>
{% highlight java %}
// this will join the two streams so that
// key1 == key2 && leftTs - 2 < rightTs < leftTs + 2
keyedStream.intervalJoin(otherKeyedStream)
    .between(Time.milliseconds(-2), Time.milliseconds(2)) // lower and upper bound
    .upperBoundExclusive(true) // optional
    .lowerBoundExclusive(true) // optional
    .process(new IntervalJoinFunction() {...});
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window CoGroup</strong><br>DataStream,DataStream &rarr; DataStream</td>
          <td>
             <p>根据指定的 key 和窗口，cogroup 两个数据流。</p>
{% highlight java %}
dataStream.coGroup(otherStream)
    .where(0).equalTo(1)
    .window(TumblingEventTimeWindows.of(Time.seconds(3)))
    .apply (new CoGroupFunction () {...});
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Connect</strong><br>DataStream,DataStream &rarr; ConnectedStreams</td>
          <td>
            <p>连接两个数据流并保留各自的类型。connect 允许这两个流共享状态。</p>
{% highlight java %}
DataStream<Integer> someStream = //...
DataStream<String> otherStream = //...

ConnectedStreams<Integer, String> connectedStreams = someStream.connect(otherStream);
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>CoMap, CoFlatMap</strong><br>ConnectedStreams &rarr; DataStream</td>
          <td>
            <p>对 ConnectedStream 中的每个流进行和单个流类似的 map/flatMap 操作。</p>
{% highlight java %}
connectedStreams.map(new CoMapFunction<Integer, String, Boolean>() {
    @Override
    public Boolean map1(Integer value) {
        return true;
    }

    @Override
    public Boolean map2(String value) {
        return false;
    }
});
connectedStreams.flatMap(new CoFlatMapFunction<Integer, String, String>() {

   @Override
   public void flatMap1(Integer value, Collector<String> out) {
       out.collect(value.toString());
   }

   @Override
   public void flatMap2(String value, Collector<String> out) {
       for (String word: value.split(" ")) {
         out.collect(word);
       }
   }
});
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Iterate</strong><br>DataStream &rarr; IterativeStream &rarr; DataStream</td>
          <td>
            <p>
                通过将某些算子的输出连接到之前算子的输入，来创建一个反馈循环。这在定义持续更新模型的算法时非常有用。下例开启了一个流，并持续迭代。大于 0 的元素会被反馈回之前的算子，剩下的会向下游传递。
{% highlight java %}
IterativeStream<Long> iteration = initialStream.iterate();
DataStream<Long> iterationBody = iteration.map (/*do something*/);
DataStream<Long> feedback = iterationBody.filter(new FilterFunction<Long>(){
    @Override
    public boolean filter(Long value) throws Exception {
        return value > 0;
    }
});
iteration.closeWith(feedback);
DataStream<Long> output = iterationBody.filter(new FilterFunction<Long>(){
    @Override
    public boolean filter(Long value) throws Exception {
        return value <= 0;
    }
});
{% endhighlight %}
            </p>
          </td>
        </tr>
  </tbody>
</table>

</div>

<div data-lang="scala" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
          <td><strong>Map</strong><br>DataStream &rarr; DataStream</td>
          <td>
            <p>输入一个数据并对应输出一个数据。 如下 map 函数将输入流的每一个数据乘2后输出:</p>
{% highlight scala %}
dataStream.map { x => x * 2 }
{% endhighlight %}
          </td>
        </tr>

        <tr>
          <td><strong>FlatMap</strong><br>DataStream &rarr; DataStream</td>
          <td>
            <p>输入一个数据，产生并输出0个，1个或多个数据。 如下 flatmap 函数将输入的单个句子根据空格分隔成多个单词并逐个输出：</p>
{% highlight scala %}
dataStream.flatMap { str => str.split(" ") }
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Filter</strong><br>DataStream &rarr; DataStream</td>
          <td>
            <p>根据每个输入元素计算 boolean 函数，并保留函数输出值为 True 的元素。 如下是滤除值为0的过滤函数：
            </p>
{% highlight scala %}
dataStream.filter { _ != 0 }
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>KeyBy</strong><br>DataStream &rarr; KeyedStream</td>
          <td>
            <p>将一个流的数据分发到各个独立分区，相同 key 值的元素将分发到同一个分区。<em>keyBy()</em>内部使用 Hash 分发数据。Flink 支持多种方式<a href="{% link dev/stream/state/state.zh.md %}#keyed-datastream">指定 key</a>。</p>
            <p>
            这个转换操作将返回一个 <em>KeyedStream</em>，你需要通过它来使用<a href="{% link dev/stream/state/state.zh.md %}#keyed-state">keyed state</a>。</p>
{% highlight scala %}
dataStream.keyBy(_.someKey) // Key by field "someKey"
dataStream.keyBy(_._1) // Key by the first element of a Tuple
{% endhighlight %}
            <p>
            <span class="label label-danger">Attention</span>
            一个类如果出现以下情况，则<strong>不能作为 key</strong>：
    	    <ol>
    	    <li> 这个类是 POJO 但是没有重写 <em>hashCode()</em> 方法，仍旧依赖于 <em>Object.hashCode()</em> 实现。</li>
    	    <li> 这个类是数组类型。</li>
    	    </ol>
    	    </p>
          </td>
        </tr>
        <tr>
          <td><strong>Reduce</strong><br>KeyedStream &rarr; DataStream</td>
          <td>
            <p>以滚动的形式"压缩" KeyedStream ， 将当前元素和上一次压缩合并的值进行新一轮压缩并输出新的压缩值。
                    <br/>
            	<br/>
            如下是对部分元素求和并生成一个新的数据流的压缩函数：</p>
{% highlight scala %}
keyedStream.reduce { _ + _ }
{% endhighlight %}
            </p>
          </td>
        </tr>
        <tr>
          <td><strong>Aggregations</strong><br>KeyedStream &rarr; DataStream</td>
          <td>
            <p>在 keyed stream 上滚动地聚合数据。min 和 minBy 的区别在于 min 直接返回最小值，而 minBy 返回在当前属性上拥有最小值的元素（max 和 maxBy 同理）</p>
{% highlight scala %}
keyedStream.sum(0)
keyedStream.sum("key")
keyedStream.min(0)
keyedStream.min("key")
keyedStream.max(0)
keyedStream.max("key")
keyedStream.minBy(0)
keyedStream.minBy("key")
keyedStream.maxBy(0)
keyedStream.maxBy("key")
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window</strong><br>KeyedStream &rarr; WindowedStream</td>
          <td>
            <p>窗口可以被定义在分区后的 KeyedStream 上。窗口将每个 key 中的数据根据某些条件（比如之前五秒的数据）组合起来。关于窗口的完整描述请见 <a href="windows.html">windows</a>。
{% highlight scala %}
dataStream.keyBy(_._1).window(TumblingEventTimeWindows.of(Time.seconds(5))) // Last 5 seconds of data
{% endhighlight %}
        </p>
          </td>
        </tr>
        <tr>
          <td><strong>WindowAll</strong><br>DataStream &rarr; AllWindowedStream</td>
          <td>
              <p>窗口也可以被定义在普通的 DataStream 上。这种窗口将流中的所有事件都按照某些条件（比如之前五秒的数据）组合起来。关于窗口的完整描述请见 <a href="windows.html">windows</a>。</p>
              <p><strong>WARNING:</strong> 在很多情况下，这是一个 <strong>非并行</strong> 的转换。所有数据都会被 windowAll 算子汇集到一个 task 中。</p>
{% highlight scala %}
dataStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5))) // Last 5 seconds of data
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window Apply</strong><br>WindowedStream &rarr; DataStream<br>AllWindowedStream &rarr; DataStream</td>
          <td>
            <p>对窗口使用函数。下面是一个手动对窗口内元素求和的函数。</p>
            <p><strong>注意：</strong>如果你使用的是 windowAll 转换，你需要用 AllWindowFunction 而不是 WindowFunction。</p>
{% highlight scala %}
windowedStream.apply { WindowFunction }

// applying an AllWindowFunction on non-keyed window stream
allWindowedStream.apply { AllWindowFunction }

{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window Reduce</strong><br>WindowedStream &rarr; DataStream</td>
          <td>
            <p>对窗口使用 reduce 函数并返回 reduce 后的值。</p>
{% highlight scala %}
windowedStream.reduce { _ + _ }
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Aggregations on windows</strong><br>WindowedStream &rarr; DataStream</td>
          <td>
            <p>聚合窗口中的内容。min 和 minBy 的区别在于 min 直接返回最小值，而 minBy 返回在当前属性上拥有最小值的元素（max 和 maxBy 同理）</p>
{% highlight scala %}
windowedStream.sum(0)
windowedStream.sum("key")
windowedStream.min(0)
windowedStream.min("key")
windowedStream.max(0)
windowedStream.max("key")
windowedStream.minBy(0)
windowedStream.minBy("key")
windowedStream.maxBy(0)
windowedStream.maxBy("key")
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Union</strong><br>DataStream* &rarr; DataStream</td>
          <td>
            <p>将两个或多个数据流合并成一个包含所有流的数据的新流。注意：如果一个数据流和自身进行合并，这个流中的每个数据将在合并后的流中出现两次。</p>
{% highlight scala %}
dataStream.union(otherStream1, otherStream2, ...)
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window Join</strong><br>DataStream,DataStream &rarr; DataStream</td>
          <td>
             <p>根据指定的 key 和窗口，join 两个数据流。</p>
{% highlight scala %}
dataStream.join(otherStream)
    .where(<key selector>).equalTo(<key selector>)
    .window(TumblingEventTimeWindows.of(Time.seconds(3)))
    .apply { ... }
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window CoGroup</strong><br>DataStream,DataStream &rarr; DataStream</td>
          <td>
            <p>根据指定的 key 和窗口，cogroup 两个数据流。</p>
{% highlight scala %}
dataStream.coGroup(otherStream)
    .where(0).equalTo(1)
    .window(TumblingEventTimeWindows.of(Time.seconds(3)))
    .apply {}
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Connect</strong><br>DataStream,DataStream &rarr; ConnectedStreams</td>
          <td>
            <p>连接两个数据流并保留各自的类型。connect 允许这两个流共享状态。</p>
{% highlight scala %}
someStream : DataStream[Int] = ...
otherStream : DataStream[String] = ...

val connectedStreams = someStream.connect(otherStream)
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>CoMap, CoFlatMap</strong><br>ConnectedStreams &rarr; DataStream</td>
          <td>
            <p>对 ConnectedStream 中的每个流进行和单个流类似的 map/flatMap 操作。</p>
{% highlight scala %}
connectedStreams.map(
    (_ : Int) => true,
    (_ : String) => false
)
connectedStreams.flatMap(
    (_ : Int) => true,
    (_ : String) => false
)
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Iterate</strong><br>DataStream &rarr; IterativeStream  &rarr; DataStream</td>
          <td>
            <p>
                通过将某些算子的输出连接到之前算子的输入，来创建一个反馈循环。这在定义持续更新模型的算法时非常有用。下例开启了一个流，并持续迭代。大于 0 的元素会被反馈回之前的算子，剩下的会向下游传递。
{% highlight java %}
initialStream.iterate {
  iteration => {
    val iterationBody = iteration.map {/*do something*/}
    (iterationBody.filter(_ > 0), iterationBody.filter(_ <= 0))
  }
}
{% endhighlight %}
            </p>
          </td>
        </tr>
  </tbody>
</table>

Extraction from tuples, case classes and collections via anonymous pattern matching, like the following:
{% highlight scala %}
val data: DataStream[(Int, String, Double)] = // [...]
data.map {
  case (id, name, temperature) => // [...]
}
{% endhighlight %}
is not supported by the API out-of-the-box. To use this feature, you should use a <a href="{% link dev/scala_api_extensions.zh.md %}">Scala API extension</a>.


</div>

<div data-lang="python" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
          <td><strong>Map</strong><br>DataStream &rarr; DataStream</td>
          <td>
            <p>输入一个数据并对应输出一个数据。 如下 map 函数将输入流的每一个数据乘2后输出:</p>
{% highlight python %}
data_stream = env.from_collection(collection=[1, 2, 3, 4, 5])
data_stream.map(lambda x: 2 * x, output_type=Types.INT())
{% endhighlight %}
          </td>
        </tr>

        <tr>
          <td><strong>FlatMap</strong><br>DataStream &rarr; DataStream</td>
          <td>
            <p>输入一个数据，产生并输出0个，1个或多个数据。 如下 flatmap 函数将输入的单个句子根据空格分隔成多个单词并逐个输出：</p>
{% highlight python %}
data_stream = env.from_collection(collection=['hello apache flink', 'streaming compute'])
data_stream.flat_map(lambda x: x.split(' '), result_type=Types.STRING())
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Filter</strong><br>DataStream &rarr; DataStream</td>
          <td>
            <p>根据每个输入元素计算 bool 函数，并保留函数输出值为 True 的元素。 如下是滤除值为0的过滤函数：
            </p>
{% highlight python %}
data_stream = env.from_collection(collection=[0, 1, 2, 3, 4, 5])
data_stream.filter(lambda x: x != 0)
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>KeyBy</strong><br>DataStream &rarr; KeyedStream</td>
          <td>
            <p>将一个流的数据分发到各个独立分区，相同 key 值的元素将分发到同一个分区。<em>keyBy()</em>内部使用 Hash 分发数据。Flink 支持多种方式<a href="{% link dev/stream/state/state.zh.md %}#keyed-datastream">指定 key</a>。</p>
            <p>
            这个转换操作将返回一个 <em>KeyedStream</em>，你需要通过它来使用<a href="{% link dev/stream/state/state.zh.md %}#keyed-state">keyed state</a>。</p>
{% highlight python %}
data_stream = env.from_collection(collection=[(1, 'a'), (2, 'a'), (3, 'b')])
data_stream.key_by(lambda x: x[1], key_type_info=Types.STRING()) // Key by the result of KeySelector
{% endhighlight %}
            <p>
            <span class="label label-danger">Attention</span>
            一个类如果出现以下情况，则<strong>不能作为 key</strong>：
    	    <ol>
    	    <li> 这个类是 POJO 但是没有重写 <em>hashCode()</em> 方法，仍旧依赖于 <em>Object.hashCode()</em> 实现。</li>
    	    <li> 这个类是数组类型。</li>
    	    </ol>
    	    </p>
          </td>
        </tr>
        <tr>
          <td><strong>Reduce</strong><br>KeyedStream &rarr; DataStream</td>
          <td>
            <p>以滚动的形式"压缩" KeyedStream ， 将当前元素和上一次压缩合并的值进行新一轮压缩并输出新的压缩值。
              <br/>
            	<br/>
            <p>如下是对部分元素求和并生成一个新的数据流的压缩函数：</p>
{% highlight python %}
data_stream = env.from_collection(collection=[(1, 'a'), (2, 'a'), (3, 'a'), (4, 'b')], type_info=Types.ROW([Types.INT(), Types.STRING()]))
data_stream.key_by(lambda x: x[1]).reduce(lambda a, b: (a[0] + b[0], b[1]))
{% endhighlight %}
            </p>
          </td>
        </tr>
         <tr>
                  <td><strong>Union</strong><br>DataStream* &rarr; DataStream</td>
                  <td>
                    <p>将两个或多个数据流合并成一个包含所有流的数据的新流。注意：如果一个数据流和自身进行合并，这个流中的每个数据将在合并后的流中出现两次。</p>
{% highlight python %}
data_stream.union(otherStream1, otherStream2, ...)
{% endhighlight %}
                  </td>
               </tr>
         <tr>
                   <td><strong>Connect</strong><br>DataStream,DataStream &rarr; ConnectedStreams</td>
                   <td>
                     <p>连接两个数据流并保留各自的类型。connect 允许这两个流共享状态。</p>
{% highlight python %}
stream_1 = ...
stream_2 = ...
connected_streams = stream_1.connect(stream_2)
{% endhighlight %}
                   </td>
                 </tr>
         <tr>
                   <td><strong>CoMap, CoFlatMap</strong><br>ConnectedStreams &rarr; DataStream</td>
                   <td>
                     <p>对 ConnectedStream 中的每个流进行和单个流类似的 map/flatMap 操作。</p>
{% highlight python %}

class MyCoMapFunction(CoMapFunction):
    
    def map1(self, value):
        return value[0] + 1, value[1]
       
    def map2(self, value):
        return value[0], value[1] + 'flink'

class MyCoFlatMapFunction(CoFlatMapFunction):
    
    def flat_map1(self, value)
        for i in range(value[0]):
            yield i
    
    def flat_map2(self, value):
        yield value[0] + 1

connectedStreams.map(MyCoMapFunction())
connectedStreams.flatMap(MyCoFlatMapFunction())
{% endhighlight %}
                   </td>
                 </tr>
  </tbody>
</table>

</div>

</div>

下面的数据转换 API 支持对元组类型的 DataStream 进行转换：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>Project</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>从元组类型的数据流中抽取元组中部分元素
{% highlight java %}
DataStream<Tuple3<Integer, Double, String>> in = // [...]
DataStream<Tuple2<String, Integer>> out = in.project(2,0);
{% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>
<div data-lang="python" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>Project</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>从元组类型的数据流中抽取元组中部分元素
{% highlight python %}
data_stream = env.from_collection([(1, 2, 3, 4), (5, 6, 7, 8)],type_info=Types.TUPLE([Types.INT(), Types.INT(), Types.INT(), Types.INT()]))
out_stream = data_stream.project(2, 0)
{% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>

</div>

<a name="physical-partitioning"/>

# 物理分区

Flink 也提供以下方法让用户根据需要在数据转换完成后对数据分区进行更细粒度的配置。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>Custom partitioning</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            根据用户定义的分区逻辑将数据分区到指定 task 。
{% highlight java %}
dataStream.partitionCustom(partitioner, "someKey");
dataStream.partitionCustom(partitioner, 0);
{% endhighlight %}
        </p>
      </td>
    </tr>
   <tr>
     <td><strong>Random partitioning</strong><br>DataStream &rarr; DataStream</td>
     <td>
       <p>
            将数据随机均匀分区。
{% highlight java %}
dataStream.shuffle();
{% endhighlight %}
       </p>
     </td>
   </tr>
   <tr>
      <td><strong>Rebalancing (Round-robin partitioning)</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            以 Round-robin 轮询的方式将数据分发到各个分区，使得每个分区负载相同。在出现数据倾斜的时候使用这种分区方式能提升性能。
{% highlight java %}
dataStream.rebalance();
{% endhighlight %}
        </p>
      </td>
    </tr>
    <tr>
      <td><strong>Rescaling</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            将元素以 Round-robin 轮询的方式分发到下游部分算子中. 如果用户希望实现数据管道， 如数据源的各个并发实例直接将数据分发到下游的部
           分算子中从而实现负载均衡， 但又不像 rebalance() 一样将数据轮询分发到所有算子中作完全负载均衡。这将会只应用到本地数据传输而不
           是网络数据传输，具体取决于其他配置值，如 TaskManager 的 slot 槽数。
        </p>
        <p>
            上游算子直接分发数据到下游算子子集的确定将取决于上下游算子各自的并发度配置。例如，如果上游算子并发度为2，下游算子并发度为6，
            那么上游算子的其中一个并发实例将数据分发到下游中的三个算子， 另外一个上游算子则将数据分发到下游的另外三个实例中。再如，当下游
           算子只有2个并发，而上游算子有6个并发的时候，上游中的三个并发将会分发数据至下游其中一个并发，而另外三个上游算子则将数据分发至另
           一个下游并发实例。
        </p>
        <p>
            如果上下游算子的并发度不成倍数关系，下游中的一个或多个算子连接的上游算子个数可能不同。
        </p>
        <p>
            下图以图片的形式阐述上述的上下游算子连接方式：
        </p>

        <div style="text-align: center">
            <img src="{% link /fig/rescale.svg %}" alt="Checkpoint barriers in data streams" />
            </div>


        <p>
{% highlight java %}
dataStream.rescale();
{% endhighlight %}

        </p>
      </td>
    </tr>
   <tr>
      <td><strong>Broadcasting</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            将每个数据广播至所有 partition 。
{% highlight java %}
dataStream.broadcast();
{% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>

<div data-lang="scala" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>Custom partitioning</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            根据用户定义的分区逻辑将数据分区到指定 task 。
{% highlight scala %}
dataStream.partitionCustom(partitioner, "someKey")
dataStream.partitionCustom(partitioner, 0)
{% endhighlight %}
        </p>
      </td>
    </tr>
   <tr>
     <td><strong>Random partitioning</strong><br>DataStream &rarr; DataStream</td>
     <td>
       <p>
            将数据随机均匀分区。
{% highlight scala %}
dataStream.shuffle()
{% endhighlight %}
       </p>
     </td>
   </tr>
   <tr>
      <td><strong>Rebalancing (Round-robin partitioning)</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            以 Round-robin 轮询的方式将数据分发到各个分区，使得每个分区负载相同。在出现数据倾斜的时候使用这种分区方式能提升性能。
{% highlight scala %}
dataStream.rebalance()
{% endhighlight %}
        </p>
      </td>
    </tr>
    <tr>
      <td><strong>Rescaling</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            将元素以 Round-robin 轮询的方式分发到下游部分算子中. 如果用户希望实现数据管道， 如数据源的各个并发实例直接将数据分发到下游的部
           分算子中从而实现负载均衡， 但又不像 rebalance() 一样将数据轮询分发到所有算子中作完全负载均衡。这将会只应用到本地数据传输而不
           是网络数据传输，具体取决于其他配置值，如 TaskManager 的 slot 槽数。
        </p>
        <p>
            上游算子直接分发数据到下游算子子集的确定将取决于上下游算子各自的并发度配置。例如，如果上游算子并发度为2，下游算子并发度为6，
            那么上游算子的其中一个并发实例将数据分发到下游中的三个算子， 另外一个上游算子则将数据分发到下游的另外三个实例中。再如，当下游
           算子只有2个并发，而上游算子有6个并发的时候，上游中的三个并发将会分发数据至下游其中一个并发，而另外三个上游算子则将数据分发至另
           一个下游并发实例。
        </p>
        <p>
            如果上下游算子的并发度不成倍数关系，下游中的一个或多个算子连接的上游算子个数可能不同。
        </p>
        <p>
            下图以图片的形式阐述上述的上下游算子连接方式：
        </p>
    
        <div style="text-align: center">
            <img src="{% link /fig/rescale.svg %}" alt="Checkpoint barriers in data streams" />
            </div>


        <p>
{% highlight java %}
dataStream.rescale()
{% endhighlight %}

        </p>
      </td>
    </tr>
   <tr>
      <td><strong>Broadcasting</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            将每个数据广播至所有 partition 。
{% highlight scala %}
dataStream.broadcast()
{% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>

<div data-lang="python" markdown="1">

<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td><strong>Custom partitioning</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            根据用户定义的分区逻辑将数据分区到指定 task 。
{% highlight python %}
data_stream = env.from_collection(collection=[(2, 'a'), (2, 'a'), (3, 'b')])
data_stream.partition_custom(lambda key, num_partition: key % partition, lambda x: x[0])
{% endhighlight %}
        </p>
      </td>
    </tr>
   <tr>
     <td><strong>Random partitioning</strong><br>DataStream &rarr; DataStream</td>
     <td>
       <p>
            将数据随机均匀分区。
{% highlight python %}
data_stream.shuffle()
{% endhighlight %}
       </p>
     </td>
   </tr>
   <tr>
      <td><strong>Rebalancing (Round-robin partitioning)</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            以 Round-robin 轮询的方式将数据分发到各个分区，使得每个分区负载相同。在出现数据倾斜的时候使用这种分区方式能提升性能。
{% highlight python %}
data_stream.rebalance()
{% endhighlight %}
        </p>
      </td>
    </tr>
    <tr>
      <td><strong>Rescaling</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            将元素以 Round-robin 轮询的方式分发到下游部分算子中. 如果用户希望实现数据管道， 如数据源的各个并发实例直接将数据分发到下游的部
           分算子中从而实现负载均衡， 但又不像 rebalance() 一样将数据轮询分发到所有算子中作完全负载均衡。这将会只应用到本地数据传输而不
           是网络数据传输，具体取决于其他配置值，如 TaskManager 的 slot 槽数。
        </p>
        <p>
            上游算子直接分发数据到下游算子子集的确定将取决于上下游算子各自的并发度配置。例如，如果上游算子并发度为2，下游算子并发度为6，
            那么上游算子的其中一个并发实例将数据分发到下游中的三个算子， 另外一个上游算子则将数据分发到下游的另外三个实例中。再如，当下游
           算子只有2个并发，而上游算子有6个并发的时候，上游中的三个并发将会分发数据至下游其中一个并发，而另外三个上游算子则将数据分发至另
           一个下游并发实例。
        </p>
        <p>
            如果上下游算子的并发度不成倍数关系，下游中的一个或多个算子连接的上游算子个数可能不同。
        </p>
        <p>
            下图以图片的形式阐述上述的上下游算子连接方式：
        </p>

        <div style="text-align: center">
            <img src="{% link /fig/rescale.svg %}" alt="Checkpoint barriers in data streams" />
            </div>


        <p>
{% highlight python %}
data_stream.rescale()
{% endhighlight %}

        </p>
      </td>
    </tr>
   <tr>
      <td><strong>Broadcasting</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            将每个数据广播至所有 partition 。
{% highlight python %}
data_stream.broadcast()
{% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>

</div>

<a name="task-chaining-and-resource-groups"/>

# 算子链和资源组
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
将两个算子链接在一起能使得它们在同一个线程中执行，从而提升性能。Flink 默认会将能链接的算子尽可能地进行链接(例如， 两个 map 转换操作)。此外，
Flink 还提供了对链接更细粒度控制的 API 以满足更多需求：

如果想对整个作业禁用算子链，可以调用 `StreamExecutionEnvironment.disableOperatorChaining()`。下列方法还提供了更细粒度的控制。需要注
意的是， 这些方法只能在 DataStream 转换操作后才能被调用，因为它们只对前一次数据转换生效。例如，可以 `someStream.map(...).startNewChain()`
这样调用，而不能 `someStream.startNewChain()`这样。

一个资源组对应着 Flink 中的一个 slot 槽，更多细节请看[slots 槽]({% link deployment/config.zh.md %}#configuring-taskmanager-processing-slots)。 
你可以根据需要手动地将各个算子隔离到不同的 slot 中。
<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td>Start new chain</td>
      <td>
        <p>以当前 operator 为起点开始新的连接。如下的两个 mapper 算子会链接在一起而 filter 算子则不会和第一个 mapper 算子进行链接。
{% highlight java %}
someStream.filter(...).map(...).startNewChain().map(...);
{% endhighlight %}
        </p>
      </td>
    </tr>
   <tr>
      <td>Disable chaining</td>
      <td>
        <p>任何算子不能和当前算子进行链接
{% highlight java %}
someStream.map(...).disableChaining();
{% endhighlight %}
        </p>
      </td>
    </tr>
    <tr>
      <td>Set slot sharing group</td>
      <td>
        <p>配置算子的资源组。Flink 会将相同资源组的算子放置到同一个 slot 槽中执行，并将不同资源组的算子分配到不同的 slot 槽中，从而实现
         slot 槽隔离。资源组将从输入算子开始继承如果所有输入操作都在同一个资源组。
         Flink 默认的资源组名称为 "default"，算子可以显式调用 slotSharingGroup("default") 加入到这个资源组中。
{% highlight java %}
someStream.filter(...).slotSharingGroup("name");
{% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>

<div data-lang="scala" markdown="1">
将两个算子链接在一起能使得它们在同一个线程中执行，从而提升性能。Flink 默认会将能链接的算子尽可能地进行链接(例如， 两个 map 转换操作)。此外，
Flink 还提供了对链接更细粒度控制的 API 以满足更多需求：

如果想对整个作业禁用算子链，可以调用 `StreamExecutionEnvironment.disableOperatorChaining()`。下列方法还提供了更细粒度的控制。需要注
意的是， 这些方法只能在 DataStream 转换操作后才能被调用，因为它们只对前一次数据转换生效。例如，可以 `someStream.map(...).startNewChain()`
这样调用，而不能 `someStream.startNewChain()`这样。

一个资源组对应着 Flink 中的一个 slot 槽，更多细节请看[slots 槽]({% link deployment/config.zh.md %}#configuring-taskmanager-processing-slots)。 
你可以根据需要手动地将各个算子隔离到不同的 slot 中。
<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td>Start new chain</td>
      <td>
        <p>以当前 operator 为起点开始新的连接。如下的两个 mapper 算子会链接在一起而 filter 算子则不会和第一个 mapper 算子进行链接。
{% highlight scala %}
someStream.filter(...).map(...).startNewChain().map(...)
{% endhighlight %}
        </p>
      </td>
    </tr>
   <tr>
      <td>Disable chaining</td>
      <td>
        <p>任何算子不能和当前算子进行链接
{% highlight scala %}
someStream.map(...).disableChaining()
{% endhighlight %}
        </p>
      </td>
    </tr>
  <tr>
      <td>Set slot sharing group</td>
      <td>
        <p>配置算子的资源组。Flink 会将相同资源组的算子放置到同一个 slot 槽中执行，并将不同资源组的算子分配到不同的 slot 槽中，从而实现
        slot 槽隔离。如果所有输入操作都在同一个资源组, 资源组将从输入算子开始继承。
        Flink 默认的资源组名称为 "default"，算子可以显式调用 slotSharingGroup("default") 加入到这个资源组中。
{% highlight java %}
someStream.filter(...).slotSharingGroup("name")
{% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>

<div data-lang="python" markdown="1">
将两个算子链接在一起能使得它们在同一个线程中执行，从而提升性能。Flink 默认会将能链接的算子尽可能地进行链接(例如， 两个 map 转换操作)。此外，
Flink 还提供了对链接更细粒度控制的 API 以满足更多需求：

如果想对整个作业禁用算子链，可以调用 `stream_execution_environment.disable_operator_chaining()`。下列方法还提供了更细粒度的控制。需要注
意的是， 这些方法只能在 DataStream 转换操作后才能被调用，因为它们只对前一次数据转换生效。例如，可以 `some_stream.map(...).start_new_chain()`
这样调用，而不能 `some_stream.start_new_chain()`这样。

一个资源组对应着 Flink 中的一个 slot 槽，更多细节请看[slots 槽]({% link deployment/config.zh.md %}#configuring-taskmanager-processing-slots)。 
你可以根据需要手动地将各个算子隔离到不同的 slot 中。
<br />

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 20%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
   <tr>
      <td>Start new chain</td>
      <td>
        <p>以当前 operator 为起点开始新的连接。如下的两个 mapper 算子会链接在一起而 filter 算子则不会和第一个 mapper 算子进行链接。
{% highlight python %}
some_stream.filter(...).map(...).start_new_chain().map(...)
{% endhighlight %}
        </p>
      </td>
    </tr>
   <tr>
      <td>Disable chaining</td>
      <td>
        <p>任何算子不能和当前算子进行链接
{% highlight python %}
some_stream.map(...).disable_chaining()
{% endhighlight %}
        </p>
      </td>
    </tr>
    <tr>
      <td>Set slot sharing group</td>
      <td>
        <p>配置算子的资源组。Flink 会将相同资源组的算子放置到同一个 slot 槽中执行，并将不同资源组的算子分配到不同的 slot 槽中，从而实现
        slot 槽隔离。如果所有输入操作都在同一个资源组, 资源组将从输入算子开始继承。
        Flink 默认的资源组名称为 "default"，算子可以显式调用 slotSharingGroup("default") 加入到这个资源组中。
{% highlight python %}
some_stream.filter(...).slot_sharing_group("name")
{% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>

</div>


{% top %}

