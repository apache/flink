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
            <p>Takes one element and produces one element. A map function that doubles the values of the input stream:</p>
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
            <p>Takes one element and produces zero, one, or more elements. A flatmap function that splits sentences to words:</p>
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
            <p>Evaluates a boolean function for each element and retains those for which the function returns true.
            A filter that filters out zero values:
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
            <p>Logically partitions a stream into disjoint partitions. All records with the same key are assigned to the same partition. Internally, <em>keyBy()</em> is implemented with hash partitioning. There are different ways to <a href="{% link dev/stream/state/state.zh.md %}#keyed-datastream">specify keys</a>.</p>
            <p>
            This transformation returns a <em>KeyedStream</em>, which is, among other things, required to use <a href="{{ site.baseurl }}/dev/stream/state/state.html#keyed-state">keyed state</a>. </p>
{% highlight java %}
dataStream.keyBy(value -> value.getSomeKey()) // Key by field "someKey"
dataStream.keyBy(value -> value.f0) // Key by the first element of a Tuple
{% endhighlight %}
            <p>
            <span class="label label-danger">Attention</span>
            A type <strong>cannot be a key</strong> if:
    	    <ol>
    	    <li> it is a POJO type but does not override the <em>hashCode()</em> method and
    	    relies on the <em>Object.hashCode()</em> implementation.</li>
    	    <li> it is an array of any type.</li>
    	    </ol>
    	    </p>
          </td>
        </tr>
        <tr>
          <td><strong>Reduce</strong><br>KeyedStream &rarr; DataStream</td>
          <td>
            <p>A "rolling" reduce on a keyed data stream. Combines the current element with the last reduced value and
            emits the new value.
              <br/>
            	<br/>
            <p>A reduce function that creates a stream of partial sums:</p>
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
            <p>Rolling aggregations on a keyed data stream. The difference between min
	    and minBy is that min returns the minimum value, whereas minBy returns
	    the element that has the minimum value in this field (same for max and maxBy).</p>
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
            <p>Windows can be defined on already partitioned KeyedStreams. Windows group the data in each
            key according to some characteristic (e.g., the data that arrived within the last 5 seconds).
            See <a href="windows.html">windows</a> for a complete description of windows.
{% highlight java %}
dataStream.keyBy(value -> value.f0).window(TumblingEventTimeWindows.of(Time.seconds(5))); // Last 5 seconds of data
{% endhighlight %}
        </p>
          </td>
        </tr>
        <tr>
          <td><strong>WindowAll</strong><br>DataStream &rarr; AllWindowedStream</td>
          <td>
              <p>Windows can be defined on regular DataStreams. Windows group all the stream events
              according to some characteristic (e.g., the data that arrived within the last 5 seconds).
              See <a href="windows.html">windows</a> for a complete description of windows.</p>
              <p><strong>WARNING:</strong> This is in many cases a <strong>non-parallel</strong> transformation. All records will be
               gathered in one task for the windowAll operator.</p>
{% highlight java %}
dataStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5))); // Last 5 seconds of data
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window Apply</strong><br>WindowedStream &rarr; DataStream<br>AllWindowedStream &rarr; DataStream</td>
          <td>
            <p>Applies a general function to the window as a whole. Below is a function that manually sums the elements of a window.</p>
            <p><strong>Note:</strong> If you are using a windowAll transformation, you need to use an AllWindowFunction instead.</p>
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
            <p>Applies a functional reduce function to the window and returns the reduced value.</p>
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
            <p>Aggregates the contents of a window. The difference between min
	    and minBy is that min returns the minimum value, whereas minBy returns
	    the element that has the minimum value in this field (same for max and maxBy).</p>
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
            <p>Union of two or more data streams creating a new stream containing all the elements from all the streams. Note: If you union a data stream
            with itself you will get each element twice in the resulting stream.</p>
{% highlight java %}
dataStream.union(otherStream1, otherStream2, ...);
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window Join</strong><br>DataStream,DataStream &rarr; DataStream</td>
          <td>
            <p>Join two data streams on a given key and a common window.</p>
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
            <p>Join two elements e1 and e2 of two keyed streams with a common key over a given time interval, so that e1.timestamp + lowerBound <= e2.timestamp <= e1.timestamp + upperBound</p>
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
            <p>Cogroups two data streams on a given key and a common window.</p>
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
            <p>"Connects" two data streams retaining their types. Connect allowing for shared state between
            the two streams.</p>
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
            <p>Similar to map and flatMap on a connected data stream</p>
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
                Creates a "feedback" loop in the flow, by redirecting the output of one operator
                to some previous operator. This is especially useful for defining algorithms that
                continuously update a model. The following code starts with a stream and applies
		the iteration body continuously. Elements that are greater than 0 are sent back
		to the feedback channel, and the rest of the elements are forwarded downstream.
		See <a href="#iterations">iterations</a> for a complete description.
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
            <p>Takes one element and produces one element. A map function that doubles the values of the input stream:</p>
{% highlight scala %}
dataStream.map { x => x * 2 }
{% endhighlight %}
          </td>
        </tr>

        <tr>
          <td><strong>FlatMap</strong><br>DataStream &rarr; DataStream</td>
          <td>
            <p>Takes one element and produces zero, one, or more elements. A flatmap function that splits sentences to words:</p>
{% highlight scala %}
dataStream.flatMap { str => str.split(" ") }
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Filter</strong><br>DataStream &rarr; DataStream</td>
          <td>
            <p>Evaluates a boolean function for each element and retains those for which the function returns true.
            A filter that filters out zero values:
            </p>
{% highlight scala %}
dataStream.filter { _ != 0 }
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>KeyBy</strong><br>DataStream &rarr; KeyedStream</td>
          <td>
            <p>Logically partitions a stream into disjoint partitions, each partition containing elements of the same key.
            Internally, this is implemented with hash partitioning. See <a href="{{ site.baseurl }}/dev/stream/state/state.html#keyed-state">keys</a> on how to specify keys.
            This transformation returns a KeyedStream.</p>
{% highlight scala %}
dataStream.keyBy(_.someKey) // Key by field "someKey"
dataStream.keyBy(_._1) // Key by the first element of a Tuple
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Reduce</strong><br>KeyedStream &rarr; DataStream</td>
          <td>
            <p>A "rolling" reduce on a keyed data stream. Combines the current element with the last reduced value and
            emits the new value.
                    <br/>
            	<br/>
            A reduce function that creates a stream of partial sums:</p>
{% highlight scala %}
keyedStream.reduce { _ + _ }
{% endhighlight %}
            </p>
          </td>
        </tr>
        <tr>
          <td><strong>Aggregations</strong><br>KeyedStream &rarr; DataStream</td>
          <td>
            <p>Rolling aggregations on a keyed data stream. The difference between min
	    and minBy is that min returns the minimum value, whereas minBy returns
	    the element that has the minimum value in this field (same for max and maxBy).</p>
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
            <p>Windows can be defined on already partitioned KeyedStreams. Windows group the data in each
            key according to some characteristic (e.g., the data that arrived within the last 5 seconds).
            See <a href="windows.html">windows</a> for a description of windows.
{% highlight scala %}
dataStream.keyBy(_._1).window(TumblingEventTimeWindows.of(Time.seconds(5))) // Last 5 seconds of data
{% endhighlight %}
        </p>
          </td>
        </tr>
        <tr>
          <td><strong>WindowAll</strong><br>DataStream &rarr; AllWindowedStream</td>
          <td>
              <p>Windows can be defined on regular DataStreams. Windows group all the stream events
              according to some characteristic (e.g., the data that arrived within the last 5 seconds).
              See <a href="windows.html">windows</a> for a complete description of windows.</p>
              <p><strong>WARNING:</strong> This is in many cases a <strong>non-parallel</strong> transformation. All records will be
               gathered in one task for the windowAll operator.</p>
{% highlight scala %}
dataStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(5))) // Last 5 seconds of data
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window Apply</strong><br>WindowedStream &rarr; DataStream<br>AllWindowedStream &rarr; DataStream</td>
          <td>
            <p>Applies a general function to the window as a whole. Below is a function that manually sums the elements of a window.</p>
            <p><strong>Note:</strong> If you are using a windowAll transformation, you need to use an AllWindowFunction instead.</p>
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
            <p>Applies a functional reduce function to the window and returns the reduced value.</p>
{% highlight scala %}
windowedStream.reduce { _ + _ }
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Aggregations on windows</strong><br>WindowedStream &rarr; DataStream</td>
          <td>
            <p>Aggregates the contents of a window. The difference between min
	    and minBy is that min returns the minimum value, whereas minBy returns
	    the element that has the minimum value in this field (same for max and maxBy).</p>
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
            <p>Union of two or more data streams creating a new stream containing all the elements from all the streams. Note: If you union a data stream
            with itself you will get each element twice in the resulting stream.</p>
{% highlight scala %}
dataStream.union(otherStream1, otherStream2, ...)
{% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window Join</strong><br>DataStream,DataStream &rarr; DataStream</td>
          <td>
            <p>Join two data streams on a given key and a common window.</p>
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
            <p>Cogroups two data streams on a given key and a common window.</p>
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
            <p>"Connects" two data streams retaining their types, allowing for shared state between
            the two streams.</p>
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
            <p>Similar to map and flatMap on a connected data stream</p>
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
                Creates a "feedback" loop in the flow, by redirecting the output of one operator
                to some previous operator. This is especially useful for defining algorithms that
                continuously update a model. The following code starts with a stream and applies
		the iteration body continuously. Elements that are greater than 0 are sent back
		to the feedback channel, and the rest of the elements are forwarded downstream.
		See <a href="#iterations">iterations</a> for a complete description.
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
is not supported by the API out-of-the-box. To use this feature, you should use a <a href="{{ site.baseurl }}/dev/scala_api_extensions.html">Scala API extension</a>.


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
            <p>根据每个输入元素计算 bool 值并保留函数输出值为 True 的元素。 如下是滤除值为0的过滤函数：
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
            <p>将一个流的数据分发到各个独立分区，相同 key 值的元素将分发到同一个分区。 <em>key_by()</em>内部实现根据 Hash 方式分发数据。</p>
            <p>这个转换操作将返回一个 <em>KeyedStream</em> , 除此之外，还会应用到 <a href="{{ site.baseurl }}/zh/dev/stream/state/state.html#keyed-state">keyed state</a>. </p>
{% highlight python %}
data_stream = env.from_collection(collection=[(1, 'a'), (2, 'a'), (3, 'b')])
data_stream.key_by(lambda x: x[1], key_type_info=Types.STRING()) // Key by the result of KeySelector
{% endhighlight %}
            <p>
            <span class="label label-danger">Attention</span>
            任意类型的数组<strong>不能作为 key 值</strong> 。
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
                    <p>将两个或多个数据流进行合并并生成一个包含所有流数据的 ConnectedStream 。需要注意的是， 如果一个数据流和自身进行合并，在新的流中每个数据将存在两份。</p>
{% highlight python %}
data_stream.union(otherStream1, otherStream2, ...)
{% endhighlight %}
                  </td>
               </tr>
         <tr>
                   <td><strong>Connect</strong><br>DataStream,DataStream &rarr; ConnectedStreams</td>
                   <td>
                     <p>将两个数据流进行连接， 并保留各自的数据类型， 这使得它们可以共享同一个 State 。</p>
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
            Uses a user-defined Partitioner to select the target task for each element.
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
            Partitions elements randomly according to a uniform distribution.
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
            Partitions elements round-robin, creating equal load per partition. Useful for performance
            optimization in the presence of data skew.
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
            Partitions elements, round-robin, to a subset of downstream operations. This is
            useful if you want to have pipelines where you, for example, fan out from
            each parallel instance of a source to a subset of several mappers to distribute load
            but don't want the full rebalance that rebalance() would incur. This would require only
            local data transfers instead of transferring data over network, depending on
            other configuration values such as the number of slots of TaskManagers.
        </p>
        <p>
            The subset of downstream operations to which the upstream operation sends
            elements depends on the degree of parallelism of both the upstream and downstream operation.
            For example, if the upstream operation has parallelism 2 and the downstream operation
            has parallelism 6, then one upstream operation would distribute elements to three
            downstream operations while the other upstream operation would distribute to the other
            three downstream operations. If, on the other hand, the downstream operation has parallelism
            2 while the upstream operation has parallelism 6 then three upstream operations would
            distribute to one downstream operation while the other three upstream operations would
            distribute to the other downstream operation.
        </p>
        <p>
            In cases where the different parallelisms are not multiples of each other one or several
            downstream operations will have a differing number of inputs from upstream operations.
        </p>
        <p>
            Please see this figure for a visualization of the connection pattern in the above
            example:
        </p>

        <div style="text-align: center">
            <img src="{{ site.baseurl }}/fig/rescale.svg" alt="Checkpoint barriers in data streams" />
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
            Broadcasts elements to every partition.
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
            Uses a user-defined Partitioner to select the target task for each element.
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
            Partitions elements randomly according to a uniform distribution.
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
            Partitions elements round-robin, creating equal load per partition. Useful for performance
            optimization in the presence of data skew.
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
            Partitions elements, round-robin, to a subset of downstream operations. This is
            useful if you want to have pipelines where you, for example, fan out from
            each parallel instance of a source to a subset of several mappers to distribute load
            but don't want the full rebalance that rebalance() would incur. This would require only
            local data transfers instead of transferring data over network, depending on
            other configuration values such as the number of slots of TaskManagers.
        </p>
        <p>
            The subset of downstream operations to which the upstream operation sends
            elements depends on the degree of parallelism of both the upstream and downstream operation.
            For example, if the upstream operation has parallelism 2 and the downstream operation
            has parallelism 4, then one upstream operation would distribute elements to two
            downstream operations while the other upstream operation would distribute to the other
            two downstream operations. If, on the other hand, the downstream operation has parallelism
            2 while the upstream operation has parallelism 4 then two upstream operations would
            distribute to one downstream operation while the other two upstream operations would
            distribute to the other downstream operations.
        </p>
        <p>
            In cases where the different parallelisms are not multiples of each other one or several
            downstream operations will have a differing number of inputs from upstream operations.

        </p>
        <p>
            Please see this figure for a visualization of the connection pattern in the above
            example:
        </p>

        <div style="text-align: center">
            <img src="{{ site.baseurl }}/fig/rescale.svg" alt="Checkpoint barriers in data streams" />
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
            Broadcasts elements to every partition.
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
            <img src="{{ site.baseurl }}/fig/rescale.svg" alt="Checkpoint barriers in data streams" />
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

一个资源组对应着 Flink 中的一个 slot 槽，更多细节请看[slots 槽]({{site.baseurl}}/ops/config.html#configuring-taskmanager-processing-slots)。 
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

一个资源组对应着 Flink 中的一个 slot 槽，更多细节请看[slots 槽]({{site.baseurl}}/ops/config.html#configuring-taskmanager-processing-slots)。 
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

一个资源组对应着 Flink 中的一个 slot 槽，更多细节请看[slots 槽]({{site.baseurl}}/ops/config.html#configuring-taskmanager-processing-slots)。 
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

