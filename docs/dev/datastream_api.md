---
title: "Flink DataStream API Programming Guide"
nav-title: Streaming (DataStream API)
nav-id: streaming
nav-parent_id: dev
nav-show_overview: true
nav-pos: 10
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

DataStream programs in Flink are regular programs that implement transformations on data streams
(e.g., filtering, updating state, defining windows, aggregating). The data streams are initially created from various
sources (e.g., message queues, socket streams, files). Results are returned via sinks, which may for
example write the data to files, or to standard output (for example the command line
terminal). Flink programs run in a variety of contexts, standalone, or embedded in other programs.
The execution can happen in a local JVM, or on clusters of many machines.

Please see [basic concepts]({{ site.baseurl }}/dev/api_concepts.html) for an introduction
to the basic concepts of the Flink API.

In order to create your own Flink DataStream program, we encourage you to start with
[anatomy of a Flink Program]({{ site.baseurl }}/dev/api_concepts.html#anatomy-of-a-flink-program)
and gradually add your own
[transformations](#datastream-transformations). The remaining sections act as references for additional
operations and advanced features.


* This will be replaced by the TOC
{:toc}


Example Program
---------------

The following program is a complete, working example of streaming window word count application, that counts the
words coming from a web socket in 5 second windows. You can copy &amp; paste the code to run it locally.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> dataStream = env
                .socketTextStream("localhost", 9999)
                .flatMap(new Splitter())
                .keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        dataStream.print();

        env.execute("Window WordCount");
    }

    public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
            for (String word: sentence.split(" ")) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }

}
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowWordCount {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .sum(1)

    counts.print

    env.execute("Window Stream WordCount")
  }
}
{% endhighlight %}
</div>

</div>

To run the example program, start the input stream with netcat first from a terminal:

~~~bash
nc -lk 9999
~~~

Just type some words hitting return for a new word. These will be the input to the
word count program. If you want to see counts greater than 1, type the same word again and again within
5 seconds (increase the window size from 5 seconds if you cannot type that fast &#9786;).

{% top %}

DataStream Transformations
--------------------------

Data transformations transform one or more DataStreams into a new DataStream. Programs can combine
multiple transformations into sophisticated topologies.

This section gives a description of all the available transformations.


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
            <p>Logically partitions a stream into disjoint partitions, each partition containing elements of the same key.
            Internally, this is implemented with hash partitioning. See <a href="{{ site.baseurl }}/dev/api_concepts.html#specifying-keys">keys</a> on how to specify keys.
            This transformation returns a KeyedDataStream.</p>
    {% highlight java %}
dataStream.keyBy("someKey") // Key by field "someKey"
dataStream.keyBy(0) // Key by the first element of a Tuple
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
          <td><strong>Fold</strong><br>KeyedStream &rarr; DataStream</td>
          <td>
          <p>A "rolling" fold on a keyed data stream with an initial value.
          Combines the current element with the last folded value and
          emits the new value.
          <br/>
          <br/>
          <p>A fold function that, when applied on the sequence (1,2,3,4,5),
          emits the sequence "start-1", "start-1-2", "start-1-2-3", ...</p>
          {% highlight java %}
DataStream<String> result =
  keyedStream.fold("start", new FoldFunction<Integer, String>() {
    @Override
    public String fold(String current, Integer value) {
        return current + "-" + value;
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
dataStream.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5))); // Last 5 seconds of data
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
          <td><strong>Window Fold</strong><br>WindowedStream &rarr; DataStream</td>
          <td>
            <p>Applies a functional fold function to the window and returns the folded value.
               The example function, when applied on the sequence (1,2,3,4,5),
               folds the sequence into the string "start-1-2-3-4-5":</p>
    {% highlight java %}
windowedStream.fold("start", new FoldFunction<Integer, String>() {
    public String fold(String current, Integer value) {
        return current + "-" + value;
    }
});
    {% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Aggregations on windows</strong><br>WindowedStream &rarr; DataStream</td>
          <td>
            <p>Aggregates the contents of a window. The difference between min
	    and minBy is that min returns the minimun value, whereas minBy returns
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
          <td><strong>Split</strong><br>DataStream &rarr; SplitStream</td>
          <td>
            <p>
                Split the stream into two or more streams according to some criterion.
                {% highlight java %}
SplitStream<Integer> split = someDataStream.split(new OutputSelector<Integer>() {
    @Override
    public Iterable<String> select(Integer value) {
        List<String> output = new ArrayList<String>();
        if (value % 2 == 0) {
            output.add("even");
        }
        else {
            output.add("odd");
        }
        return output;
    }
});
                {% endhighlight %}
            </p>
          </td>
        </tr>
        <tr>
          <td><strong>Select</strong><br>SplitStream &rarr; DataStream</td>
          <td>
            <p>
                Select one or more streams from a split stream.
                {% highlight java %}
SplitStream<Integer> split;
DataStream<Integer> even = split.select("even");
DataStream<Integer> odd = split.select("odd");
DataStream<Integer> all = split.select("even","odd");
                {% endhighlight %}
            </p>
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
    public boolean filter(Integer value) throws Exception {
        return value > 0;
    }
});
iteration.closeWith(feedback);
DataStream<Long> output = iterationBody.filter(new FilterFunction<Long>(){
    @Override
    public boolean filter(Integer value) throws Exception {
        return value <= 0;
    }
});
                {% endhighlight %}
            </p>
          </td>
        </tr>
        <tr>
          <td><strong>Extract Timestamps</strong><br>DataStream &rarr; DataStream</td>
          <td>
            <p>
                Extracts timestamps from records in order to work with windows
                that use event time semantics. See <a href="{{ site.baseurl }}/dev/event_time.html">Event Time</a>.
                {% highlight java %}
stream.assignTimestamps (new TimeStampExtractor() {...});
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
            Internally, this is implemented with hash partitioning. See <a href="{{ site.baseurl }}/dev/api_concepts.html#specifying-keys">keys</a> on how to specify keys.
            This transformation returns a KeyedDataStream.</p>
    {% highlight scala %}
dataStream.keyBy("someKey") // Key by field "someKey"
dataStream.keyBy(0) // Key by the first element of a Tuple
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
          <td><strong>Fold</strong><br>KeyedStream &rarr; DataStream</td>
          <td>
          <p>A "rolling" fold on a keyed data stream with an initial value.
          Combines the current element with the last folded value and
          emits the new value.
          <br/>
          <br/>
          <p>A fold function that, when applied on the sequence (1,2,3,4,5),
          emits the sequence "start-1", "start-1-2", "start-1-2-3", ...</p>
          {% highlight scala %}
val result: DataStream[String] =
    keyedStream.fold("start")((str, i) => { str + "-" + i })
          {% endhighlight %}
          </p>
          </td>
        </tr>
        <tr>
          <td><strong>Aggregations</strong><br>KeyedStream &rarr; DataStream</td>
          <td>
            <p>Rolling aggregations on a keyed data stream. The difference between min
	    and minBy is that min returns the minimun value, whereas minBy returns
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
dataStream.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5))) // Last 5 seconds of data
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
          <td><strong>Window Fold</strong><br>WindowedStream &rarr; DataStream</td>
          <td>
            <p>Applies a functional fold function to the window and returns the folded value.
               The example function, when applied on the sequence (1,2,3,4,5),
               folds the sequence into the string "start-1-2-3-4-5":</p>
          {% highlight scala %}
val result: DataStream[String] =
    windowedStream.fold("start", (str, i) => { str + "-" + i })
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
          <td><strong>Split</strong><br>DataStream &rarr; SplitStream</td>
          <td>
            <p>
                Split the stream into two or more streams according to some criterion.
                {% highlight scala %}
val split = someDataStream.split(
  (num: Int) =>
    (num % 2) match {
      case 0 => List("even")
      case 1 => List("odd")
    }
)
                {% endhighlight %}
            </p>
          </td>
        </tr>
        <tr>
          <td><strong>Select</strong><br>SplitStream &rarr; DataStream</td>
          <td>
            <p>
                Select one or more streams from a split stream.
                {% highlight scala %}

val even = split select "even"
val odd = split select "odd"
val all = split.select("even","odd")
                {% endhighlight %}
            </p>
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
        <tr>
          <td><strong>Extract Timestamps</strong><br>DataStream &rarr; DataStream</td>
          <td>
            <p>
                Extracts timestamps from records in order to work with windows
                that use event time semantics.
                See <a href="{{ site.baseurl }}/apis/streaming/event_time.html">Event Time</a>.
                {% highlight scala %}
stream.assignTimestamps { timestampExtractor }
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
is not supported by the API out-of-the-box. To use this feature, you should use a <a href="scala_api_extensions.html">Scala API extension</a>.


</div>
</div>

The following transformations are available on data streams of Tuples:


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
        <p>Selects a subset of fields from the tuples
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
</div>


### Physical partitioning

Flink also gives low-level control (if desired) on the exact stream partitioning after a transformation,
via the following functions.

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
        </p>
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
</div>

### Task chaining and resource groups

Chaining two subsequent transformations means co-locating them within the same thread for better
performance. Flink by default chains operators if this is possible (e.g., two subsequent map
transformations). The API gives fine-grained control over chaining if desired:

Use `StreamExecutionEnvironment.disableOperatorChaining()` if you want to disable chaining in
the whole job. For more fine grained control, the following functions are available. Note that
these functions can only be used right after a DataStream transformation as they refer to the
previous transformation. For example, you can use `someStream.map(...).startNewChain()`, but
you cannot use `someStream.startNewChain()`.

A resource group is a slot in Flink, see
[slots]({{site.baseurl}}/setup/config.html#configuring-taskmanager-processing-slots). You can
manually isolate operators in separate slots if desired.

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
      <td>Start new chain</td>
      <td>
        <p>Begin a new chain, starting with this operator. The two
	mappers will be chained, and filter will not be chained to
	the first mapper.
{% highlight java %}
someStream.filter(...).map(...).startNewChain().map(...);
{% endhighlight %}
        </p>
      </td>
    </tr>
   <tr>
      <td>Disable chaining</td>
      <td>
        <p>Do not chain the map operator
{% highlight java %}
someStream.map(...).disableChaining();
{% endhighlight %}
        </p>
      </td>
    </tr>
    <tr>
      <td>Set slot sharing group</td>
      <td>
        <p>Set the slot sharing group of an operation. Flink will put operations with the same
        slot sharing group into the same slot while keeping operations that don't have the
        slot sharing group in other slots. This can be used to isolate slots. The slot sharing
        group is inherited from input operations if all input operations are in the same slot
        sharing group.
        The name of the default slot sharing group is "default", operations can explicitly
        be put into this group by calling slotSharingGroup("default").
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
        <p>Begin a new chain, starting with this operator. The two
	mappers will be chained, and filter will not be chained to
	the first mapper.
{% highlight scala %}
someStream.filter(...).map(...).startNewChain().map(...)
{% endhighlight %}
        </p>
      </td>
    </tr>
   <tr>
      <td>Disable chaining</td>
      <td>
        <p>Do not chain the map operator
{% highlight scala %}
someStream.map(...).disableChaining()
{% endhighlight %}
        </p>
      </td>
    </tr>
  <tr>
      <td>Set slot sharing group</td>
      <td>
        <p>Set the slot sharing group of an operation. Flink will put operations with the same
        slot sharing group into the same slot while keeping operations that don't have the
        slot sharing group in other slots. This can be used to isolate slots. The slot sharing
        group is inherited from input operations if all input operations are in the same slot
        sharing group.
        The name of the default slot sharing group is "default", operations can explicitly
        be put into this group by calling slotSharingGroup("default").
{% highlight java %}
someStream.filter(...).slotSharingGroup("name")
{% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>


{% top %}

Data Sources
------------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

Sources are where your program reads its input from. You can attach a source to your program by
using `StreamExecutionEnvironment.addSource(sourceFunction)`. Flink comes with a number of pre-implemented
source functions, but you can always write your own custom sources by implementing the `SourceFunction`
for non-parallel sources, or by implementing the `ParallelSourceFunction` interface or extending the
`RichParallelSourceFunction` for parallel sources.

There are several predefined stream sources accessible from the `StreamExecutionEnvironment`:

File-based:

- `readTextFile(path)` - Reads text files, i.e. files that respect the `TextInputFormat` specification, line-by-line and returns them as Strings.

- `readFile(fileInputFormat, path)` - Reads (once) files as dictated by the specified file input format.

- `readFile(fileInputFormat, path, watchType, interval, pathFilter, typeInfo)` -  This is the method called internally by the two previous ones. It reads files in the `path` based on the given `fileInputFormat`. Depending on the provided `watchType`, this source may periodically monitor (every `interval` ms) the path for new data (`FileProcessingMode.PROCESS_CONTINUOUSLY`), or process once the data currently in the path and exit (`FileProcessingMode.PROCESS_ONCE`). Using the `pathFilter`, the user can further exclude files from being processed.

    *IMPLEMENTATION:*

    Under the hood, Flink splits the file reading process into two sub-tasks, namely *directory monitoring* and *data reading*. Each of these sub-tasks is implemented by a separate entity. Monitoring is implemented by a single, **non-parallel** (parallelism = 1) task, while reading is performed by multiple tasks running in parallel. The parallelism of the latter is equal to the job parallelism. The role of the single monitoring task is to scan the directory (periodically or only once depending on the `watchType`), find the files to be processed, divide them in *splits*, and assign these splits to the downstream readers. The readers are the ones who will read the actual data. Each split is read by only one reader, while a reader can read muplitple splits, one-by-one.

    *IMPORTANT NOTES:*

    1. If the `watchType` is set to `FileProcessingMode.PROCESS_CONTINUOUSLY`, when a file is modified, its contents are re-processed entirely. This can break the "exactly-once" semantics, as appending data at the end of a file will lead to **all** its contents being re-processed.

    2. If the `watchType` is set to `FileProcessingMode.PROCESS_ONCE`, the source scans the path **once** and exits, without waiting for the readers to finish reading the file contents. Of course the readers will continue reading until all file contents are read. Closing the source leads to no more checkpoints after that point. This may lead to slower recovery after a node failure, as the job will resume reading from the last checkpoint.

Socket-based:

- `socketTextStream` - Reads from a socket. Elements can be separated by a delimiter.

Collection-based:

- `fromCollection(Collection)` - Creates a data stream from the Java Java.util.Collection. All elements
  in the collection must be of the same type.

- `fromCollection(Iterator, Class)` - Creates a data stream from an iterator. The class specifies the
  data type of the elements returned by the iterator.

- `fromElements(T ...)` - Creates a data stream from the given sequence of objects. All objects must be
  of the same type.

- `fromParallelCollection(SplittableIterator, Class)` - Creates a data stream from an iterator, in
  parallel. The class specifies the data type of the elements returned by the iterator.

- `generateSequence(from, to)` - Generates the sequence of numbers in the given interval, in
  parallel.

Custom:

- `addSource` - Attache a new source function. For example, to read from Apache Kafka you can use
    `addSource(new FlinkKafkaConsumer08<>(...))`. See [connectors]({{ site.baseurl }}/dev/connectors/index.html) for more details.

</div>

<div data-lang="scala" markdown="1">

<br />

Sources are where your program reads its input from. You can attach a source to your program by
using `StreamExecutionEnvironment.addSource(sourceFunction)`. Flink comes with a number of pre-implemented
source functions, but you can always write your own custom sources by implementing the `SourceFunction`
for non-parallel sources, or by implementing the `ParallelSourceFunction` interface or extending the
`RichParallelSourceFunction` for parallel sources.

There are several predefined stream sources accessible from the `StreamExecutionEnvironment`:

File-based:

- `readTextFile(path)` - Reads text files, i.e. files that respect the `TextInputFormat` specification, line-by-line and returns them as Strings.

- `readFile(fileInputFormat, path)` - Reads (once) files as dictated by the specified file input format.

- `readFile(fileInputFormat, path, watchType, interval, pathFilter)` -  This is the method called internally by the two previous ones. It reads files in the `path` based on the given `fileInputFormat`. Depending on the provided `watchType`, this source may periodically monitor (every `interval` ms) the path for new data (`FileProcessingMode.PROCESS_CONTINUOUSLY`), or process once the data currently in the path and exit (`FileProcessingMode.PROCESS_ONCE`). Using the `pathFilter`, the user can further exclude files from being processed.

    *IMPLEMENTATION:*

    Under the hood, Flink splits the file reading process into two sub-tasks, namely *directory monitoring* and *data reading*. Each of these sub-tasks is implemented by a separate entity. Monitoring is implemented by a single, **non-parallel** (parallelism = 1) task, while reading is performed by multiple tasks running in parallel. The parallelism of the latter is equal to the job parallelism. The role of the single monitoring task is to scan the directory (periodically or only once depending on the `watchType`), find the files to be processed, divide them in *splits*, and assign these splits to the downstream readers. The readers are the ones who will read the actual data. Each split is read by only one reader, while a reader can read muplitple splits, one-by-one.

    *IMPORTANT NOTES:*

    1. If the `watchType` is set to `FileProcessingMode.PROCESS_CONTINUOUSLY`, when a file is modified, its contents are re-processed entirely. This can break the "exactly-once" semantics, as appending data at the end of a file will lead to **all** its contents being re-processed.

    2. If the `watchType` is set to `FileProcessingMode.PROCESS_ONCE`, the source scans the path **once** and exits, without waiting for the readers to finish reading the file contents. Of course the readers will continue reading until all file contents are read. Closing the source leads to no more checkpoints after that point. This may lead to slower recovery after a node failure, as the job will resume reading from the last checkpoint.

Socket-based:

- `socketTextStream` - Reads from a socket. Elements can be separated by a delimiter.

Collection-based:

- `fromCollection(Seq)` - Creates a data stream from the Java Java.util.Collection. All elements
  in the collection must be of the same type.

- `fromCollection(Iterator)` - Creates a data stream from an iterator. The class specifies the
  data type of the elements returned by the iterator.

- `fromElements(elements: _*)` - Creates a data stream from the given sequence of objects. All objects must be
  of the same type.

- `fromParallelCollection(SplittableIterator)` - Creates a data stream from an iterator, in
  parallel. The class specifies the data type of the elements returned by the iterator.

- `generateSequence(from, to)` - Generates the sequence of numbers in the given interval, in
  parallel.

Custom:

- `addSource` - Attach a new source function. For example, to read from Apache Kafka you can use
    `addSource(new FlinkKafkaConsumer08<>(...))`. See [connectors]({{ site.baseurl }}/dev/connectors/) for more details.

</div>
</div>

{% top %}

Data Sinks
----------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

Data sinks consume DataStreams and forward them to files, sockets, external systems, or print them.
Flink comes with a variety of built-in output formats that are encapsulated behind operations on the
DataStreams:

- `writeAsText()` / `TextOutputFormat` - Writes elements line-wise as Strings. The Strings are
  obtained by calling the *toString()* method of each element.

- `writeAsCsv(...)` / `CsvOutputFormat` - Writes tuples as comma-separated value files. Row and field
  delimiters are configurable. The value for each field comes from the *toString()* method of the objects.

- `print()` / `printToErr()`  - Prints the *toString()* value
of each element on the standard out / standard error stream. Optionally, a prefix (msg) can be provided which is
prepended to the output. This can help to distinguish between different calls to *print*. If the parallelism is
greater than 1, the output will also be prepended with the identifier of the task which produced the output.

- `writeUsingOutputFormat()` / `FileOutputFormat` - Method and base class for custom file outputs. Supports
  custom object-to-bytes conversion.

- `writeToSocket` - Writes elements to a socket according to a `SerializationSchema`

- `addSink` - Invokes a custom sink function. Flink comes bundled with connectors to other systems (such as
    Apache Kafka) that are implemented as sink functions.

</div>
<div data-lang="scala" markdown="1">

<br />

Data sinks consume DataStreams and forward them to files, sockets, external systems, or print them.
Flink comes with a variety of built-in output formats that are encapsulated behind operations on the
DataStreams:

- `writeAsText()` / `TextOutputFormat` - Writes elements line-wise as Strings. The Strings are
  obtained by calling the *toString()* method of each element.

- `writeAsCsv(...)` / `CsvOutputFormat` - Writes tuples as comma-separated value files. Row and field
  delimiters are configurable. The value for each field comes from the *toString()* method of the objects.

- `print()` / `printToErr()`  - Prints the *toString()* value
of each element on the standard out / standard error stream. Optionally, a prefix (msg) can be provided which is
prepended to the output. This can help to distinguish between different calls to *print*. If the parallelism is
greater than 1, the output will also be prepended with the identifier of the task which produced the output.

- `writeUsingOutputFormat()` / `FileOutputFormat` - Method and base class for custom file outputs. Supports
  custom object-to-bytes conversion.

- `writeToSocket` - Writes elements to a socket according to a `SerializationSchema`

- `addSink` - Invokes a custom sink function. Flink comes bundled with connectors to other systems (such as
    Apache Kafka) that are implemented as sink functions.

</div>
</div>

Note that the `write*()` methods on `DataStream` are mainly intended for debugging purposes.
They are not participating in Flink's checkpointing, this means these functions usually have
at-least-once semantics. The data flushing to the target system depends on the implementation of the
OutputFormat. This means that not all elements send to the OutputFormat are immediately showing up
in the target system. Also, in failure cases, those records might be lost.

For reliable, exactly-once delivery of a stream into a file system, use the `flink-connector-filesystem`.
Also, custom implementations through the `.addSink(...)` method can participate in Flink's checkpointing
for exactly-once semantics.

{% top %}

Iterations
----------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

Iterative streaming programs implement a step function and embed it into an `IterativeStream`. As a DataStream
program may never finish, there is no maximum number of iterations. Instead, you need to specify which part
of the stream is fed back to the iteration and which part is forwarded downstream using a `split` transformation
or a `filter`. Here, we show an example using filters. First, we define an `IterativeStream`

{% highlight java %}
IterativeStream<Integer> iteration = input.iterate();
{% endhighlight %}

Then, we specify the logic that will be executed inside the loop using a series of transformations (here
a simple `map` transformation)

{% highlight java %}
DataStream<Integer> iterationBody = iteration.map(/* this is executed many times */);
{% endhighlight %}

To close an iteration and define the iteration tail, call the `closeWith(feedbackStream)` method of the `IterativeStream`.
The DataStream given to the `closeWith` function will be fed back to the iteration head.
A common pattern is to use a filter to separate the part of the stream that is fed back,
and the part of the stream which is propagated forward. These filters can, e.g., define
the "termination" logic, where an element is allowed to propagate downstream rather
than being fed back.

{% highlight java %}
iteration.closeWith(iterationBody.filter(/* one part of the stream */));
DataStream<Integer> output = iterationBody.filter(/* some other part of the stream */);
{% endhighlight %}

By default the partitioning of the feedback stream will be automatically set to be the same as the input of the
iteration head. To override this the user can set an optional boolean flag in the `closeWith` method.

For example, here is program that continuously subtracts 1 from a series of integers until they reach zero:

{% highlight java %}
DataStream<Long> someIntegers = env.generateSequence(0, 1000);

IterativeStream<Long> iteration = someIntegers.iterate();

DataStream<Long> minusOne = iteration.map(new MapFunction<Long, Long>() {
  @Override
  public Long map(Long value) throws Exception {
    return value - 1 ;
  }
});

DataStream<Long> stillGreaterThanZero = minusOne.filter(new FilterFunction<Long>() {
  @Override
  public boolean filter(Long value) throws Exception {
    return (value > 0);
  }
});

iteration.closeWith(stillGreaterThanZero);

DataStream<Long> lessThanZero = minusOne.filter(new FilterFunction<Long>() {
  @Override
  public boolean filter(Long value) throws Exception {
    return (value <= 0);
  }
});
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">

<br />

Iterative streaming programs implement a step function and embed it into an `IterativeStream`. As a DataStream
program may never finish, there is no maximum number of iterations. Instead, you need to specify which part
of the stream is fed back to the iteration and which part is forwarded downstream using a `split` transformation
or a `filter`. Here, we show an example iteration where the body (the part of the computation that is repeated)
is a simple map transformation, and the elements that are fed back are distinguished by the elements that
are forwarded downstream using filters.

{% highlight scala %}
val iteratedStream = someDataStream.iterate(
  iteration => {
    val iterationBody = iteration.map(/* this is executed many times */)
    (tail.filter(/* one part of the stream */), tail.filter(/* some other part of the stream */))
})
{% endhighlight %}


By default the partitioning of the feedback stream will be automatically set to be the same as the input of the
iteration head. To override this the user can set an optional boolean flag in the `closeWith` method.

For example, here is program that continuously subtracts 1 from a series of integers until they reach zero:

{% highlight scala %}
val someIntegers: DataStream[Long] = env.generateSequence(0, 1000)

val iteratedStream = someIntegers.iterate(
  iteration => {
    val minusOne = iteration.map( v => v - 1)
    val stillGreaterThanZero = minusOne.filter (_ > 0)
    val lessThanZero = minusOne.filter(_ <= 0)
    (stillGreaterThanZero, lessThanZero)
  }
)
{% endhighlight %}

</div>
</div>

{% top %}

Execution Parameters
--------------------

The `StreamExecutionEnvironment` contains the `ExecutionConfig` which allows to set job specific configuration values for the runtime.

Please refer to [execution configuration]({{ site.baseurl }}/dev/execution_configuration.html)
for an explanation of most parameters. These parameters pertain specifically to the DataStream API:

- `enableTimestamps()` / **`disableTimestamps()`**: Attach a timestamp to each event emitted from a source.
    `areTimestampsEnabled()` returns the current value.

- `setAutoWatermarkInterval(long milliseconds)`: Set the interval for automatic watermark emission. You can
    get the current value with `long getAutoWatermarkInterval()`

{% top %}

### Fault Tolerance

[State & Checkpointing]({{ site.baseurl }}/dev/stream/checkpointing.html) describes how to enable and configure Flink's checkpointing mechanism.

### Controlling Latency

By default, elements are not transferred on the network one-by-one (which would cause unnecessary network traffic)
but are buffered. The size of the buffers (which are actually transferred between machines) can be set in the Flink config files.
While this method is good for optimizing throughput, it can cause latency issues when the incoming stream is not fast enough.
To control throughput and latency, you can use `env.setBufferTimeout(timeoutMillis)` on the execution environment
(or on individual operators) to set a maximum wait time for the buffers to fill up. After this time, the
buffers are sent automatically even if they are not full. The default value for this timeout is 100 ms.

Usage:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
env.setBufferTimeout(timeoutMillis);

env.generateSequence(1,10).map(new MyMapper()).setBufferTimeout(timeoutMillis);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment
env.setBufferTimeout(timeoutMillis)

env.genereateSequence(1,10).map(myMap).setBufferTimeout(timeoutMillis)
{% endhighlight %}
</div>
</div>

To maximize throughput, set `setBufferTimeout(-1)` which will remove the timeout and buffers will only be
flushed when they are full. To minimize latency, set the timeout to a value close to 0 (for example 5 or 10 ms).
A buffer timeout of 0 should be avoided, because it can cause severe performance degradation.

{% top %}

Debugging
---------

Before running a streaming program in a distributed cluster, it is a good
idea to make sure that the implemented algorithm works as desired. Hence, implementing data analysis
programs is usually an incremental process of checking results, debugging, and improving.

Flink provides features to significantly ease the development process of data analysis
programs by supporting local debugging from within an IDE, injection of test data, and collection of
result data. This section give some hints how to ease the development of Flink programs.

### Local Execution Environment

A `LocalStreamEnvironment` starts a Flink system within the same JVM process it was created in. If you
start the LocalEnvironment from an IDE, you can set breakpoints in your code and easily debug your
program.

A LocalEnvironment is created and used as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

DataStream<String> lines = env.addSource(/* some source */);
// build your program

env.execute();
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">

{% highlight scala %}
val env = StreamExecutionEnvironment.createLocalEnvironment()

val lines = env.addSource(/* some source */)
// build your program

env.execute()
{% endhighlight %}
</div>
</div>

### Collection Data Sources

Flink provides special data sources which are backed
by Java collections to ease testing. Once a program has been tested, the sources and sinks can be
easily replaced by sources and sinks that read from / write to external systems.

Collection data sources can be used as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

// Create a DataStream from a list of elements
DataStream<Integer> myInts = env.fromElements(1, 2, 3, 4, 5);

// Create a DataStream from any Java collection
List<Tuple2<String, Integer>> data = ...
DataStream<Tuple2<String, Integer>> myTuples = env.fromCollection(data);

// Create a DataStream from an Iterator
Iterator<Long> longIt = ...
DataStream<Long> myLongs = env.fromCollection(longIt, Long.class);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.createLocalEnvironment()

// Create a DataStream from a list of elements
val myInts = env.fromElements(1, 2, 3, 4, 5)

// Create a DataStream from any Collection
val data: Seq[(String, Int)] = ...
val myTuples = env.fromCollection(data)

// Create a DataStream from an Iterator
val longIt: Iterator[Long] = ...
val myLongs = env.fromCollection(longIt)
{% endhighlight %}
</div>
</div>

**Note:** Currently, the collection data source requires that data types and iterators implement
`Serializable`. Furthermore, collection data sources can not be executed in parallel (
parallelism = 1).

### Iterator Data Sink

Flink also provides a sink to collect DataStream results for testing and debugging purposes. It can be used as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.contrib.streaming.DataStreamUtils

DataStream<Tuple2<String, Integer>> myResult = ...
Iterator<Tuple2<String, Integer>> myOutput = DataStreamUtils.collect(myResult)
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">

{% highlight scala %}
import org.apache.flink.contrib.streaming.DataStreamUtils
import scala.collection.JavaConverters.asScalaIteratorConverter

val myResult: DataStream[(String, Int)] = ...
val myOutput: Iterator[(String, Int)] = DataStreamUtils.collect(myResult.getJavaStream).asScala
{% endhighlight %}
</div>
</div>

{% top %}
