---
title: "Flink DataStream API Programming Guide"
is_beta: false
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

<a href="#top"></a>

DataStream programs in Flink are regular programs that implement transformations on data streams
(e.g., filtering, updating state, defining windows, aggregating). The data streams are initially created from various
sources (e.g., message queues, socket streams, files). Results are returned via sinks, which may for
example write the data to files, or to standard output (for example the command line
terminal). Flink programs run in a variety of contexts, standalone, or embedded in other programs.
The execution can happen in a local JVM, or on clusters of many machines.

In order to create your own Flink DataStream program, we encourage you to start with the
[program skeleton](#program-skeleton) and gradually add your own
[transformations](#transformations). The remaining sections act as references for additional
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
                .timeWindow(Time.of(5, TimeUnit.SECONDS))
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
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object WindowWordCount {
  def main(args: Array[String]) {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val text = env.socketTextStream("localhost", 9999)

    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      .keyBy(0)
      .timeWindow(Time.of(5, TimeUnit.SECONDS))
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

[Back to top](#top)


Linking with Flink
------------------

To write programs with Flink, you need to include the Flink DataStream library corresponding to
your programming language in your project.

The simplest way to do this is to use one of the quickstart scripts: either for
[Java]({{ site.baseurl }}/quickstart/java_api_quickstart.html) or for [Scala]({{ site.baseurl }}/quickstart/scala_api_quickstart.html). They
create a blank project from a template (a Maven Archetype), which sets up everything for you. To
manually create the project, you can use the archetype and create a project by calling:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight bash %}
mvn archetype:generate /
    -DarchetypeGroupId=org.apache.flink/
    -DarchetypeArtifactId=flink-quickstart-java /
    -DarchetypeVersion={{site.version }}
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight bash %}
mvn archetype:generate /
    -DarchetypeGroupId=org.apache.flink/
    -DarchetypeArtifactId=flink-quickstart-scala /
    -DarchetypeVersion={{site.version }}
{% endhighlight %}
</div>
</div>

The archetypes are working for stable releases and preview versions (`-SNAPSHOT`).

If you want to add Flink to an existing Maven project, add the following entry to your
*dependencies* section in the *pom.xml* file of your project:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-java</artifactId>
  <version>{{site.version }}</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-streaming-scala</artifactId>
  <version>{{site.version }}</version>
</dependency>
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-clients</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}
</div>
</div>

In order to create your own Flink program, we encourage you to start with the
[program skeleton](#program-skeleton) and gradually add your own
[transformations](#transformations).

[Back to top](#top)

Program Skeleton
----------------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

As presented in the [example](#example-program), Flink DataStream programs look like regular Java
programs with a `main()` method. Each program consists of the same basic parts:

1. Obtaining a `StreamExecutionEnvironment`,
2. Connecting to data stream sources,
3. Specifying transformations on the data streams,
4. Specifying output for the processed data,
5. Executing the program.

We will now give an overview of each of those steps, please refer to the respective sections for
more details.

The `StreamExecutionEnvironment` is the basis for all Flink DataStream programs. You can
obtain one using these static methods on class `StreamExecutionEnvironment`:

{% highlight java %}
getExecutionEnvironment()

createLocalEnvironment()
createLocalEnvironment(int parallelism)
createLocalEnvironment(int parallelism, Configuration customConfiguration)

createRemoteEnvironment(String host, int port, String... jarFiles)
createRemoteEnvironment(String host, int port, int parallelism, String... jarFiles)
{% endhighlight %}

Typically, you only need to use `getExecutionEnvironment()`, since this
will do the right thing depending on the context: if you are executing
your program inside an IDE or as a regular Java program it will create
a local environment that will execute your program on your local machine. If
you created a JAR file from your program, and invoke it through the [command line](cli.html)
or the [web interface](web_client.html),
the Flink cluster manager will execute your main method and `getExecutionEnvironment()` will return
an execution environment for executing your program on a cluster.

For specifying data sources the execution environment has several methods
to read from files, sockets, and external systems using various methods. To just read
data from a socket (useful also for debugging), you can use:

{% highlight java %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> lines = env.socketTextStream("localhost", 9999)
{% endhighlight %}

This will give you a DataStream on which you can then apply transformations. For
more information on data sources and input formats, please refer to
[Data Sources](#data-sources).

Once you have a DataStream you can apply transformations to create a new
DataStream which you can then write to a socket, transform again,
combine with other DataStreams, or push to an external system (e.g., a message queue, or a file system).
You apply transformations by calling
methods on DataStream with your own custom transformation functions. For example,
a map transformation looks like this:

{% highlight java %}
DataStream<String> input = ...;

DataStream<Integer> intValues = input.map(new MapFunction<String, Integer>() {
    @Override
    public Integer map(String value) {
        return Integer.parseInt(value);
    }
});
{% endhighlight %}

This will create a new DataStream by converting every String in the original
stream to an Integer. For more information and a list of all the transformations,
please refer to [Transformations](#transformations).

Once you have a DataStream containing your final results, you can push the result
to an external system (HDFS, Kafka, Elasticsearch), write it to a socket, write to a file,
or print it.

{% highlight java %}
writeAsText(String path, ...)
writeAsCsv(String path, ...)
writeToSocket(String hostname, int port, ...)

print()

addSink(...)
{% endhighlight %}

Once you specified the complete program you need to **trigger the program execution** by
calling `execute()` on `StreamExecutionEnvironment`. This will either execute on
the local machine or submit the program for execution on a cluster, depending on the chosen execution environment.

{% highlight java %}
env.execute();
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">

<br />

As presented in the [example](#example-program), Flink DataStream programs look like regular Scala
programs with a `main()` method. Each program consists of the same basic parts:

1. Obtaining a `StreamExecutionEnvironment`,
2. Connecting to data stream sources,
3. Specifying transformations on the data streams,
4. Specifying output for the processed data,
5. Executing the program.

We will now give an overview of each of those steps, please refer to the respective sections for
more details.

The `StreamExecutionEnvironment` is the basis for all Flink DataStream programs. You can
obtain one using these static methods on class `StreamExecutionEnvironment`:

{% highlight scala %}
def getExecutionEnvironment

def createLocalEnvironment(parallelism: Int =  Runtime.getRuntime.availableProcessors())

def createRemoteEnvironment(host: String, port: Int, jarFiles: String*)
def createRemoteEnvironment(host: String, port: Int, parallelism: Int, jarFiles: String*)
{% endhighlight %}

Typically, you only need to use `getExecutionEnvironment`, since this
will do the right thing depending on the context: if you are executing
your program inside an IDE or as a regular Java program it will create
a local environment that will execute your program on your local machine. If
you created a JAR file from your program, and invoke it through the [command line](cli.html)
or the [web interface](web_client.html),
the Flink cluster manager will execute your main method and `getExecutionEnvironment()` will return
an execution environment for executing your program on a cluster.

For specifying data sources the execution environment has several methods
to read from files, sockets, and external systems using various methods. To just read
data from a socket (useful also for debugging), you can use:

{% highlight scala %}
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment

DataStream<String> lines = env.socketTextStream("localhost", 9999)
{% endhighlight %}

This will give you a DataStream on which you can then apply transformations. For
more information on data sources and input formats, please refer to
[Data Sources](#data-sources).

Once you have a DataStream you can apply transformations to create a new
DataStream which you can then write to a file, transform again,
combine with other DataStreams, or push to an external system.
You apply transformations by calling
methods on DataStream with your own custom transformation function. For example,
a map transformation looks like this:

{% highlight scala %}
val input: DataStream[String] = ...

val mapped = input.map { x => x.toInt }
{% endhighlight %}

This will create a new DataStream by converting every String in the original
set to an Integer. For more information and a list of all the transformations,
please refer to [Transformations](#transformations).

Once you have a DataStream containing your final results, you can push the result
to an external system (HDFS, Kafka, Elasticsearch), write it to a socket, write to a file,
or print it.

{% highlight scala %}
writeAsText(path: String, ...)
writeAsCsv(path: String, ...)
writeToSocket(hostname: String, port: Int, ...)

print()

addSink(...)
{% endhighlight %}

Once you specified the complete program you need to **trigger the program execution** by
calling `execute` on `StreamExecutionEnvironment`. This will either execute on
the local machine or submit the program for execution on a cluster, depending on the chosen execution environment.

{% highlight scala %}
env.execute()
{% endhighlight %}

</div>
</div>

[Back to top](#top)

DataStream Abstraction
----------------------

A `DataStream` is a possibly unbounded immutable collection of data items of a the same type.

Transformations may return different subtypes of `DataStream` allowing specialized transformations.
For example the `keyBy(…)` method returns a `KeyedDataStream` which is a stream of data that
is logically partitioned by a certain key, and can be further windowed.

[Back to top](#top)

Lazy Evaluation
---------------

All Flink DataStream programs are executed lazily: When the program's main method is executed, the data loading
and transformations do not happen directly. Rather, each operation is created and added to the
program's plan. The operations are actually executed when the execution is explicitly triggered by
an `execute()` call on the `StreamExecutionEnvironment` object. Whether the program is executed locally
or on a cluster depends on the type of `StreamExecutionEnvironment`.

The lazy evaluation lets you construct sophisticated programs that Flink executes as one
holistically planned unit.

[Back to top](#top)


Transformations
---------------

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
            Internally, this is implemented with hash partitioning. See <a href="#specifying-keys">keys</a> on how to specify keys.
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
	    and minBy is that min returns the minimun value, whereas minBy returns
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
            See <a href="#windows">windows</a> for a complete description of windows.
    {% highlight java %}
dataStream.keyBy(0).window(TumblingTimeWindows.of(Time.of(5, TimeUnit.SECONDS))); // Last 5 seconds of data
    {% endhighlight %}
        </p>
          </td>
        </tr>
        <tr>
          <td><strong>WindowAll</strong><br>DataStream &rarr; AllWindowedStream</td>
          <td>
              <p>Windows can be defined on regular DataStreams. Windows group all the stream events
              according to some characteristic (e.g., the data that arrived within the last 5 seconds).
              See <a href="#windows">windows</a> for a complete description of windows.</p>
              <p><strong>WARNING:</strong> This is in many cases a <strong>non-parallel</strong> transformation. All records will be
               gathered in one task for the windowAll operator.</p>
  {% highlight java %}
dataStream.windowAll(TumblingTimeWindows.of(Time.of(5, TimeUnit.SECONDS))); // Last 5 seconds of data
  {% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window Apply</strong><br>WindowedStream &rarr; DataStream<br>AllWindowedStream &rarr; DataStream</td>
          <td>
            <p>Applies a general function to the window as a whole. Below is a function that manually sums the elements of a window.</p>
            <p><strong>Note:</strong> If you are using a windowAll transformation, you need to use an AllWindowFunction instead.</p>
    {% highlight java %}
windowedStream.apply (new WindowFunction<Tuple2<String,Integer>,Integer>, Tuple, Window>() {
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
};
    {% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window Reduce</strong><br>WindowedStream &rarr; DataStream</td>
          <td>
            <p>Applies a functional reduce function to the window and returns the reduced value.</p>
    {% highlight java %}
windowedStream.reduce (new ReduceFunction<Tuple2<String,Integer>() {
    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
        return new Tuple2<String,Integer>(value1.f0, value1.f1 + value2.f1);
    }
};
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
windowedStream.fold("start-", new FoldFunction<Integer, String>() {
    public String fold(String current, Integer value) {
        return current + "-" + value;
    }
};
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
            <p>Union of two or more data streams creating a new stream containing all the elements from all the streams. Node: If you union a data stream
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
    .where(0).equalTo(1)
    .window(TumblingTimeWindows.of(Time.of(3, TimeUnit.SECONDS)))
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
    .window(TumblingTimeWindows.of(Time.of(3, TimeUnit.SECONDS)))
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
                that use event time semantics. See <a href="#working-with-time">working with time</a>.
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
            Internally, this is implemented with hash partitioning. See <a href="#specifying-keys">keys</a> on how to specify keys.
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
    keyedStream.fold("start", (str, i) => { str + "-" + i })
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
            See <a href="#windows">windows</a> for a description of windows.
    {% highlight scala %}
dataStream.keyBy(0).window(TumblingTimeWindows.of(Time.of(5, TimeUnit.SECONDS))) // Last 5 seconds of data
    {% endhighlight %}
        </p>
          </td>
        </tr>
        <tr>
          <td><strong>WindowAll</strong><br>DataStream &rarr; AllWindowedStream</td>
          <td>
              <p>Windows can be defined on regular DataStreams. Windows group all the stream events
              according to some characteristic (e.g., the data that arrived within the last 5 seconds).
              See <a href="#windows">windows</a> for a complete description of windows.</p>
              <p><strong>WARNING:</strong> This is in many cases a <strong>non-parallel</strong> transformation. All records will be
               gathered in one task for the windowAll operator.</p>
  {% highlight scala %}
dataStream.windowAll(TumblingTimeWindows.of(Time.of(5, TimeUnit.SECONDS))) // Last 5 seconds of data
  {% endhighlight %}
          </td>
        </tr>
        <tr>
          <td><strong>Window Apply</strong><br>WindowedStream &rarr; DataStream<br>AllWindowedStream &rarr; DataStream</td>
          <td>
            <p>Applies a general function to the window as a whole. Below is a function that manually sums the elements of a window.</p>
            <p><strong>Note:</strong> If you are using a windowAll transformation, you need to use an AllWindowFunction instead.</p>
    {% highlight scala %}
windowedStream.apply { applyFunction }
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
	    and minBy is that min returns the minimun value, whereas minBy returns
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
            <p>Union of two or more data streams creating a new stream containing all the elements from all the streams. Node: If you union a data stream
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
    .where(0).equalTo(1)
    .window(TumblingTimeWindows.of(Time.of(3, TimeUnit.SECONDS)))
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
    .window(TumblingTimeWindows.of(Time.of(3, TimeUnit.SECONDS)))
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
initialStream. iterate {
  iteration => {
    val iterationBody = iteration.map {/*do something*/}
    (iterationBody.filter(_ > 0), iterationBody.filter(_ <= 0))
  }
}
IterativeStream<Long> iteration = initialStream.iterate();
DataStream<Long> iterationBody = iteration.map (/*do something*/);
DataStream<Long> feedback = iterationBody.filter ( _ > 0);
iteration.closeWith(feedback);
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
                See <a href="#working-with-time">working with time</a>.
                {% highlight scala %}
stream.assignTimestamps { timestampExtractor }
                {% endhighlight %}
            </p>
          </td>
        </tr>
  </tbody>
</table>

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
      <td><strong>Project</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>Selects a subset of fields from the tuples
{% highlight scala %}
val in : DataStream[(Int,Double,String)] = // [...]
val out = in.project(2,0)
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
      <td><strong>Hash partitioning</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            Identical to keyBy but returns a DataStream instead of a KeyedStream.
            {% highlight java %}
dataStream.partitionByHash("someKey");
dataStream.partitionByHash(0);
            {% endhighlight %}
        </p>
      </td>
    </tr>
   <tr>
      <td><strong>Custom partitioning</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            Uses a user-defined Partitioner to select the target task for each element.
            {% highlight java %}
dataStream.partitionCustom(new Partitioner(){...}, "someKey");
dataStream.partitionCustom(new Partitioner(){...}, 0);
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
dataStream.partitionRandom();
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
      <td><strong>Hash partitioning</strong><br>DataStream &rarr; DataStream</td>
      <td>
        <p>
            Identical to keyBy but returns a DataStream instead of a KeyedStream.
            {% highlight scala %}
dataStream.partitionByHash("someKey")
dataStream.partitionByHash(0)
            {% endhighlight %}
        </p>
      </td>
    </tr>
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
dataStream.partitionRandom()
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
      <td>Start a new resource group</td>
      <td>
        <p>Start a new resource group containing the map and the subsequent operators.
{% highlight java %}
someStream.filter(...).startNewResourceGroup();
{% endhighlight %}
        </p>
      </td>
    </tr>
   <tr>
      <td>Isolate resources</td>
      <td>
        <p>Isolate the operator in its own slot.
{% highlight java %}
someStream.map(...).isolateResources();
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
      <td>Start a new resource group</td>
      <td>
        <p>Start a new resource group containing the map and the subsequent operators.
{% highlight scala %}
someStream.filter(...).startNewResourceGroup()
{% endhighlight %}
        </p>
      </td>
    </tr>
   <tr>
      <td>Isolate resources</td>
      <td>
        <p>Isolate the operator in its own slot.
{% highlight scala %}
someStream.map(...).isolateResources()
{% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>


[Back to top](#top)

Specifying Keys
----------------

The `keyBy` transformation requires that a key is defined on
its argument DataStream.

A DataStream is keyed as
{% highlight java %}
DataStream<...> input = // [...]
DataStream<...> windowed = input
	.keyBy(/*define key here*/)
	.window(/*define window here*/);
{% endhighlight %}

The data model of Flink is not based on key-value pairs. Therefore,
you do not need to physically pack the data stream types into keys and
values. Keys are "virtual": they are defined as functions over the
actual data to guide the grouping operator.

See [the relevant section of the DataSet API documentation](programming_guide.html#specifying-keys) on how to specify keys.
Just replace `DataSet` with `DataStream`, and `groupBy` with `keyBy`.



Passing Functions to Flink
--------------------------

Some transformations take user-defined functions as arguments.

See [the relevant section of the DataSet API documentation](programming_guide.html#passing-functions-to-flink).


[Back to top](#top)


Data Types
----------

Flink places some restrictions on the type of elements that are used in DataStreams and in results
of transformations. The reason for this is that the system analyzes the types to determine
efficient execution strategies.

See [the relevant section of the DataSet API documentation](programming_guide.html#data-types).

[Back to top](#top)


Data Sources
------------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

Sources can by created by using `StreamExecutionEnvironment.addSource(sourceFunction)`.
You can either use one of the source functions that come with Flink or write a custom source
by implementing the `SourceFunction` for non-parallel sources, or by implementing the
`ParallelSourceFunction` interface or extending `RichParallelSourceFunction` for parallel sources.

There are several predefined stream sources accessible from the `StreamExecutionEnvironment`:

File-based:

- `readTextFile(path)` / `TextInputFormat` - Reads files line wise and returns them as Strings.

- `readTextFileWithValue(path)` / `TextValueInputFormat` - Reads files line wise and returns them as
  StringValues. StringValues are mutable strings.

- `readFile(path)` / Any input format - Reads files as dictated by the input format.

- `readFileOfPrimitives(path, Class)` / `PrimitiveInputFormat` - Parses files of new-line (or another char sequence) delimited primitive data types such as `String` or `Integer`.

- `readFileStream` - create a stream by appending elements when there are changes to a file

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
    `addSource(new FlinkKafkaConsumer082<>(...))`. See [connectors](#connectors) for more details.

</div>

<div data-lang="scala" markdown="1">

<br />

Sources can by created by using `StreamExecutionEnvironment.addSource(sourceFunction)`.
You can either use one of the source functions that come with Flink or write a custom source
by implementing the `SourceFunction` for non-parallel sources, or by implementing the
`ParallelSourceFunction` interface or extending `RichParallelSourceFunction` for parallel sources.

There are several predefined stream sources accessible from the `StreamExecutionEnvironment`:

File-based:

- `readTextFile(path)` / `TextInputFormat` - Reads files line wise and returns them as Strings.

- `readTextFileWithValue(path)` / `TextValueInputFormat` - Reads files line wise and returns them as
  StringValues. StringValues are mutable strings.

- `readFile(path)` / Any input format - Reads files as dictated by the input format.

- `readFileOfPrimitives(path, Class)` / `PrimitiveInputFormat` - Parses files of new-line (or another char sequence) delimited primitive data types such as `String` or `Integer`.

- `readFileStream` - create a stream by appending elements when there are changes to a file

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

- `addSource` - Attache a new source function. For example, to read from Apache Kafka you can use
    `addSource(new FlinkKafkaConsumer082<>(...))`. See [connectors](#connectors) for more details.

</div>
</div>

[Back to top](#top)


Execution Configuration
----------

The `StreamExecutionEnvironment` also contains the `ExecutionConfig` which allows to set job specific configuration values for the runtime.

See [the relevant section of the DataSet API documentation](programming_guide.html#execution-configuration).

Parameters in the `ExecutionConfig` that pertain specifically to the DataStream API are:

- `enableTimestamps()` / **`disableTimestamps()`**: Attach a timestamp to each event emitted from a source.
    `areTimestampsEnabled()` returns the current value.

- `setAutoWatermarkInterval(long milliseconds)`: Set the interval for automatic watermark emission. You can
    get the current value with `long getAutoWatermarkInterval()`

[Back to top](#top)

Data Sinks
----------

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

<br />

Data sinks consume DataStreams and forward them to files, sockets, external systems, or print them.
Flink comes with a variety of built-in output formats that are encapsulated behind operations on the
DataStreams:

- `writeAsText()` / `TextOuputFormat` - Writes elements line-wise as Strings. The Strings are
  obtained by calling the *toString()* method of each element.

- `writeAsCsv(...)` / `CsvOutputFormat` - Writes tuples as comma-separated value files. Row and field
  delimiters are configurable. The value for each field comes from the *toString()* method of the objects.

- `print()` / `printToErr()`  - Prints the *toString()* value
of each element on the standard out / strandard error stream. Optionally, a prefix (msg) can be provided which is
prepended to the output. This can help to distinguish between different calls to *print*. If the parallelism is
greater than 1, the output will also be prepended with the identifier of the task which produced the output.

- `write()` / `FileOutputFormat` - Method and base class for custom file outputs. Supports
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

- `writeAsText()` / `TextOuputFormat` - Writes elements line-wise as Strings. The Strings are
  obtained by calling the *toString()* method of each element.

- `writeAsCsv(...)` / `CsvOutputFormat` - Writes tuples as comma-separated value files. Row and field
  delimiters are configurable. The value for each field comes from the *toString()* method of the objects.

- `print()` / `printToErr()`  - Prints the *toString()* value
of each element on the standard out / strandard error stream. Optionally, a prefix (msg) can be provided which is
prepended to the output. This can help to distinguish between different calls to *print*. If the parallelism is
greater than 1, the output will also be prepended with the identifier of the task which produced the output.

- `write()` / `FileOutputFormat` - Method and base class for custom file outputs. Supports
  custom object-to-bytes conversion.

- `writeToSocket` - Writes elements to a socket according to a `SerializationSchema`

- `addSink` - Invokes a custom sink function. Flink comes bundled with connectors to other systems (such as
    Apache Kafka) that are implemented as sink functions.

</div>
</div>


[Back to top](#top)

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
start the LocalEnvironement from an IDE, you can set breakpoints in your code and easily debug your
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

[Back to top](#top)


Windows
-------

### Working with Time

Windows are typically groups of events within a certain time period. Reasoning about time and windows assumes
a definition of time. Flink has support for three kinds of time:

- *Processing time:* Processing time is simply the wall clock time of the machine that happens to be
    executing the transformation. Processing time is the simplest notion of time and provides the best
    performance. However, in distributed and asynchronous environments processing time does not provide
    determinism.

- *Event time:* Event time is the time that each individual event occurred. This time is
    typically embedded within the records before they enter Flink or can be extracted from their contents.
    When using event time, out-of-order events can be properly handled. For example, an event with a lower
    timestamp may arrive after an event with a higher timestamp, but transformations will handle these events
    correctly. Event time processing provides predictable results, but incurs more latency, as out-of-order
    events need to be buffered

- *Ingestion time:* Ingestion time is the time that events enter Flink. In particular, the timestamp of
    an event is assigned by the source operator as the current wall clock time of the machine that executes
    the source task at the time the records enter the Flink source. Ingestion time is more predictable
    than processing time, and gives lower latencies than event time as the latency does not depend on
    external systems. Ingestion time provides thus a middle ground between processing time and event time.
    Ingestion time is a special case of event time (and indeed, it is treated by Flink identically to
    event time).

When dealing with event time, transformations need to avoid indefinite
wait times for events to arrive. *Watermarks* provide the mechanism to control the event time-processing time skew. Watermarks
are emitted by the sources. A watermark with a certain timestamp denotes the knowledge that no event
with timestamp lower than the timestamp of the watermark will ever arrive.

You can specify the semantics of time in a Flink DataStream program using `StreamExecutionEnviroment`, as

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight java %}
env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
{% endhighlight %}
</div>
</div>

The default value is `TimeCharacteristic.ProcessingTime`, so in order to write a program with processing
time semantics nothing needs to be specified (e.g., the first [example](#example-program) in this guide follows processing
time semantics).

In order to work with event time semantics, you need to follow four steps:

- Set `env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)`

- Use `DataStream.assignTimestamps(...)` in order to tell Flink how timestamps relate to events (e.g., which
    record field is the timestamp)

- Set `enableTimestamps()`, as well the interval for watermark emission (`setAutoWatermarkInterval(long milliseconds)`)
    in `ExecutionConfig`.

For example, assume that we have a data stream of tuples, in which the first field is the timestamp (assigned
by the system that generates these data streams), and we know that the lag between the current processing
time and the timestamp of an event is never more than 1 second:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple4<Long,Integer,Double,String>> stream = //...
stream.assignTimestamps(new TimestampExtractor<Tuple4<Long,Integer,Double,String>>{
    @Override
    public long extractTimestamp(Tuple4<Long,Integer,Double,String> element, long currentTimestamp) {
        return element.f0;
    }

    @Override
    public long extractWatermark(Tuple4<Long,Integer,Double,String> element, long currentTimestamp) {
        return element.f0 - 1000;
    }

    @Override
    public long getCurrentWatermark() {
        return Long.MIN_VALUE;
    }
});
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val stream: DataStream[(Long,Int,Double,String)] = null;
stream.assignTimestampts(new TimestampExtractor[(Long, Int, Double, String)] {
  override def extractTimestamp(element: (Long, Int, Double, String), currentTimestamp: Long): Long = element._1

  override def extractWatermark(element: (Long, Int, Double, String), currentTimestamp: Long): Long = element._1 - 1000

  override def getCurrentWatermark: Long = Long.MinValue
})
{% endhighlight %}
</div>
</div>

If you know that timestamps of events are always ascending, i.e., elements arrive in order, you can use
the `AscendingTimestampExtractor`, and the system generates watermarks automatically:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple4<Long,Integer,Double,String>> stream = //...
stream.assignTimestamps(new AscendingTimestampExtractor<Tuple4<Long,Integer,Double,String>>{
    @Override
    public long extractAscendingTimestamp(Tuple4<Long,Integer,Double,String> element, long currentTimestamp) {
        return element.f0;
    }
});
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
stream.extractAscendingTimestamp(record => record._1)
{% endhighlight %}
</div>
</div>

In order to write a program with ingestion time semantics, you need to
set `env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)`. You can think of this setting as a
shortcut for writing a `TimestampExtractor` which assignes timestamps to events at the sources
based on the current source wall-clock time. Flink injects this timestamp extractor automatically.


### Windows on Keyed Data Streams

Flink offers a variety of methods for defining windows on a `KeyedStream`. All of these group elements *per key*,
i.e., each window will contain elements with the same key value.

#### Basic Window Constructs

Flink offers a general window mechanism that provides flexibility, as well as a number of pre-defined windows
for common use cases. See first if your use case can be served by the pre-defined windows below before moving
to defining your own windows.

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
        <td><strong>Tumbling time window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 5 seconds, that "tumbles". This means that elements are
          grouped according to their timestamp in groups of 5 second duration, and every element belongs to exactly one window.
	  The notion of time is specified by the selected TimeCharacteristic (see <a href="#working-with-time">time</a>).
    {% highlight java %}
keyedStream.timeWindow(Time.of(5, TimeUnit.SECONDS));
    {% endhighlight %}
          </p>
        </td>
      </tr>
      <tr>
          <td><strong>Sliding time window</strong><br>KeyedStream &rarr; WindowedStream</td>
          <td>
            <p>
             Defines a window of 5 seconds, that "slides" by 1 seconds. This means that elements are
             grouped according to their timestamp in groups of 5 second duration, and elements can belong to more than
             one window (since windows overlap by at most 4 seconds)
             The notion of time is specified by the selected TimeCharacteristic (see <a href="#working-with-time">time</a>).
      {% highlight java %}
keyedStream.timeWindow(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS));
      {% endhighlight %}
            </p>
          </td>
        </tr>
      <tr>
        <td><strong>Tumbling count window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 1000 elements, that "tumbles". This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element belongs to exactly one window.
    {% highlight java %}
keyedStream.countWindow(1000);
    {% endhighlight %}
        </p>
        </td>
      </tr>
      <tr>
      <td><strong>Sliding count window</strong><br>KeyedStream &rarr; WindowedStream</td>
      <td>
        <p>
          Defines a window of 1000 elements, that "slides" every 100 elements. This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element can belong to more than one window (as windows overlap by at most 900 elements).
  {% highlight java %}
keyedStream.countWindow(1000, 100)
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
        <td><strong>Tumbling time window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 5 seconds, that "tumbles". This means that elements are
          grouped according to their timestamp in groups of 5 second duration, and every element belongs to exactly one window.
          The notion of time is specified by the selected TimeCharacteristic (see <a href="#working-with-time">time</a>).
    {% highlight scala %}
keyedStream.timeWindow(Time.of(5, TimeUnit.SECONDS))
    {% endhighlight %}
          </p>
        </td>
      </tr>
      <tr>
          <td><strong>Sliding time window</strong><br>KeyedStream &rarr; WindowedStream</td>
          <td>
            <p>
             Defines a window of 5 seconds, that "slides" by 1 seconds. This means that elements are
             grouped according to their timestamp in groups of 5 second duration, and elements can belong to more than
             one window (since windows overlap by at most 4 seconds)
             The notion of time is specified by the selected TimeCharacteristic (see <a href="#working-with-time">time</a>).
      {% highlight scala %}
keyedStream.timeWindow(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS))
      {% endhighlight %}
            </p>
          </td>
        </tr>
      <tr>
        <td><strong>Tumbling count window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 1000 elements, that "tumbles". This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element belongs to exactly one window.
    {% highlight scala %}
keyedStream.countWindow(1000)
    {% endhighlight %}
        </p>
        </td>
      </tr>
      <tr>
      <td><strong>Sliding count window</strong><br>KeyedStream &rarr; WindowedStream</td>
      <td>
        <p>
          Defines a window of 1000 elements, that "slides" every 100 elements. This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element can belong to more than one window (as windows overlap by at most 900 elements).
  {% highlight scala %}
keyedStream.countWindow(1000, 100)
  {% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>

#### Advanced Window Constructs

The general mechanism can define more powerful windows at the cost of more verbose syntax. For example,
below is a window definition where windows hold elements of the last 5 seconds and slides every 1 second,
but the execution of the window function is triggered when 100 elements have been added to the
window, and every time execution is triggered, 10 elements are retained in the window:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
keyedStream
    .window(SlidingTimeWindows.of(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS))
    .trigger(CountTrigger.of(100))
    .evictor(CountEvictor.of(10));
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
keyedStream
    .window(SlidingTimeWindows.of(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS))
    .trigger(CountTrigger.of(100))
    .evictor(CountEvictor.of(10))
{% endhighlight %}
</div>
</div>

The general recipe for building a custom window is to specify (1) a `WindowAssigner`, (2) a `Trigger` (optionally),
and (3) an `Evictor` (optionally).

The `WindowAssigner` defines how incoming elements are assigned to windows. A window is a logical group of elements
that has a begin-value, and an end-value corresponding to a begin-time and end-time. Elements with timestamp (according
to some notion of time described above within these values are part of the window).

For example, the `SlidingTimeWindows`
assigner in the code above defines a window of size 5 seconds, and a slide of 1 second. Assume that
time starts from 0 and is measured in milliseconds. Then, we have 6 windows
that overlap: [0,5000], [1000,6000], [2000,7000], [3000, 8000], [4000, 9000], and [5000, 10000]. Each incoming
element is assigned to the windows according to its timestamp. For example, an element with timestamp 2000 will be
assigned to the first three windows. Flink comes bundled with window assigners that cover the most common use cases. You can write your
own window types by extending the `WindowAssigner` class.

<div class="codetabs" markdown="1">

<div data-lang="java" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
      <tr>
        <td><strong>Global window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
	    All incoming elements of a given key are assigned to the same window.
	    The window does not contain a default trigger, hence it will never be triggered
	    if a trigger is not explicitly specified.
          </p>
    {% highlight java %}
stream.window(GlobalWindows.create());
    {% endhighlight %}
        </td>
      </tr>
      <tr>
          <td><strong>Tumbling time windows</strong><br>KeyedStream &rarr; WindowedStream</td>
          <td>
            <p>
              Incoming elements are assigned to a window of a certain size (1 second below) based on
              their timestamp. Windows do not overlap, i.e., each element is assigned to exactly one window.
	      The notion of time is picked from the specified TimeCharacteristic (see <a href="#working-with-time">time</a>).
	      The window comes with a default trigger. For event/ingestion time, a window is triggered when a
	      watermark with value higher than its end-value is received, whereas for processing time
	      when the current processing time exceeds its current end value.
            </p>
      {% highlight java %}
stream.window(TumblingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)));
      {% endhighlight %}
          </td>
        </tr>
      <tr>
        <td><strong>Sliding time windows</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
            Incoming elements are assigned to a window of a certain size (5 seconds below) based on
            their timestamp. Windows "slide" by the provided value (1 second in the example), and hence
            overlap. The window comes with a default trigger. For event/ingestion time, a window is triggered when a
	    watermark with value higher than its end-value is received, whereas for processing time
	    when the current processing time exceeds its current end value.
          </p>
    {% highlight java %}
stream.window(SlidingTimeWindows.of(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS)));
    {% endhighlight %}
        </td>
      </tr>
  </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
      <tr>
        <td><strong>Global window</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
            All incoming elements of a given key are assigned to the same window.
	    The window does not contain a default trigger, hence it will never be triggered
	    if a trigger is not explicitly specified.
          </p>
    {% highlight scala %}
stream.window(GlobalWindows.create)
    {% endhighlight %}
        </td>
      </tr>
      <tr>
          <td><strong>Tumbling time windows</strong><br>KeyedStream &rarr; WindowedStream</td>
          <td>
            <p>
              Incoming elements are assigned to a window of a certain size (1 second below) based on
              their timestamp. Windows do not overlap, i.e., each element is assigned to exactly one window.
	      The notion of time is specified by the selected TimeCharacteristic (see <a href="#working-with-time">time</a>).
	      The window comes with a default trigger. For event/ingestion time, a window is triggered when a
	      watermark with value higher than its end-value is received, whereas for processing time
	      when the current processing time exceeds its current end value.
            </p>
      {% highlight scala %}
stream.window(TumblingTimeWindows.of(Time.of(1, TimeUnit.SECONDS)))
      {% endhighlight %}
          </td>
        </tr>
      <tr>
        <td><strong>Sliding time windows</strong><br>KeyedStream &rarr; WindowedStream</td>
        <td>
          <p>
            Incoming elements are assigned to a window of a certain size (5 seconds below) based on
            their timestamp. Windows "slide" by the provided value (1 second in the example), and hence
            overlap. The window comes with a default trigger. For event/ingestion time, a window is triggered when a
	    watermark with value higher than its end-value is received, whereas for processing time
	    when the current processing time exceeds its current end value.
          </p>
    {% highlight scala %}
stream.window(SlidingTimeWindows.of(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS)))
    {% endhighlight %}
        </td>
      </tr>
  </tbody>
</table>
</div>

</div>

The `Trigger` specifies when the function that comes after the window clause (e.g., `sum`, `count`) is evaluated ("fires")
for each window. If a trigger is not specified, a default trigger for each window type is used (that is part of the
definition of the `WindowAssigner`). Flink comes bundled with a set of triggers if the ones that windows use by
default do not fit the application. You can write your own trigger by implementing the `Trigger` interface. Note that
specifying a trigger will override the default trigger of the window assigner.

<div class="codetabs" markdown="1">

<div data-lang="java" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  <tr>
    <td><strong>Processing time trigger</strong></td>
    <td>
      <p>
        A window is fired when the current processing time exceeds its end-value.
        The elements on the triggered window are henceforth discarded.
      </p>
{% highlight java %}
windowedStream.trigger(ProcessingTimeTrigger.create());
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Watermark trigger</strong></td>
    <td>
      <p>
        A window is fired when a watermark with value that exceeds the window's end-value has been received.
        The elements on the triggered window are henceforth discarded.
      </p>
{% highlight java %}
windowedStream.trigger(EventTimeTrigger.create());
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Continuous processing time trigger</strong></td>
    <td>
      <p>
        A window is periodically considered for being fired (every 5 seconds in the example).
        The window is actually fired only when the current processing time exceeds its end-value.
        The elements on the triggered window are retained.
      </p>
{% highlight java %}
windowedStream.trigger(ContinuousProcessingTimeTrigger.of(Time.of(5, TimeUnit.SECONDS)));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Continuous watermark time trigger</strong></td>
    <td>
      <p>
        A window is periodically considered for being fired (every 5 seconds in the example).
        A window is actually fired when a watermark with value that exceeds the window's end-value has been received.
        The elements on the triggered window are retained.
      </p>
{% highlight java %}
windowedStream.trigger(ContinuousEventTimeTrigger.of(Time.of(5, TimeUnit.SECONDS)));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Count trigger</strong></td>
    <td>
      <p>
        A window is fired when it has more than a certain number of elements (1000 below).
        The elements of the triggered window are retained.
      </p>
{% highlight java %}
windowedStream.trigger(CountTrigger.of(1000));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Purging trigger</strong></td>
    <td>
      <p>
        Takes any trigger as an argument and forces the triggered window elements to be
        "purged" (discarded) after triggering.
      </p>
{% highlight java %}
windowedStream.trigger(PurgingTrigger.of(CountTrigger.of(1000)));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Delta trigger</strong></td>
    <td>
      <p>
        A window is periodically considered for being fired (every 5000 milliseconds in the example).
        A window is actually fired when the value of the last added element exceeds the value of
        the first element inserted in the window according to a `DeltaFunction`.
      </p>
{% highlight java %}
windowedStream.trigger(new DeltaTrigger.of(5000.0, new DeltaFunction<Double>() {
    @Override
    public double getDelta (Double old, Double new) {
        return (new - old > 0.01);
    }
}));
{% endhighlight %}
    </td>
  </tr>
 </tbody>
</table>
</div>


<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  <tr>
    <td><strong>Processing time trigger</strong></td>
    <td>
      <p>
        A window is fired when the current processing time exceeds its end-value.
        The elements on the triggered window are henceforth discarded.
      </p>
{% highlight scala %}
windowedStream.trigger(ProcessingTimeTrigger.create);
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Watermark trigger</strong></td>
    <td>
      <p>
        A window is fired when a watermark with value that exceeds the window's end-value has been received.
        The elements on the triggered window are henceforth discarded.
      </p>
{% highlight scala %}
windowedStream.trigger(EventTimeTrigger.create);
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Continuous processing time trigger</strong></td>
    <td>
      <p>
        A window is periodically considered for being fired (every 5 seconds in the example).
        The window is actually fired only when the current processing time exceeds its end-value.
        The elements on the triggered window are retained.
      </p>
{% highlight scala %}
windowedStream.trigger(ContinuousProcessingTimeTrigger.of(Time.of(5, TimeUnit.SECONDS)));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Continuous watermark time trigger</strong></td>
    <td>
      <p>
        A window is periodically considered for being fired (every 5 seconds in the example).
        A window is actually fired when a watermark with value that exceeds the window's end-value has been received.
        The elements on the triggered window are retained.
      </p>
{% highlight scala %}
windowedStream.trigger(ContinuousEventTimeTrigger.of(Time.of(5, TimeUnit.SECONDS)));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Count trigger</strong></td>
    <td>
      <p>
        A window is fired when it has more than a certain number of elements (1000 below).
        The elements of the triggered window are retained.
      </p>
{% highlight scala %}
windowedStream.trigger(CountTrigger.of(1000));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Purging trigger</strong></td>
    <td>
      <p>
        Takes any trigger as an argument and forces the triggered window elements to be
        "purged" (discarded) after triggering.
      </p>
{% highlight scala %}
windowedStream.trigger(PurgingTrigger.of(CountTrigger.of(1000)));
{% endhighlight %}
    </td>
  </tr>
  <tr>
    <td><strong>Delta trigger</strong></td>
    <td>
      <p>
        A window is periodically considered for being fired (every 5000 milliseconds in the example).
        A window is actually fired when the value of the last added element exceeds the value of
        the first element inserted in the window according to a `DeltaFunction`.
      </p>
{% highlight scala %}
windowedStream.trigger(DeltaTrigger.of(5000.0, { (old,new) => new - old > 0.01 }))
{% endhighlight %}
    </td>
  </tr>
 </tbody>
</table>
</div>

</div>

After the trigger fires, and before the function (e.g., `sum`, `count`) is applied to the window contents, an
optional `Evictor` removes some elements from the beginning of the window before the remaining elements
are passed on to the function. Flink comes bundled with a set of evictors You can write your own evictor by
implementing the `Evictor` interface.

<div class="codetabs" markdown="1">

<div data-lang="java" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  <tr>
      <td><strong>Time evictor</strong></td>
      <td>
        <p>
         Evict all elements from the beginning of the window, so that elements from end-value - 1 second
         until end-value are retained (the resulting window size is 1 second).
        </p>
  {% highlight java %}
triggeredStream.evictor(TimeEvictor.of(Time.of(1, TimeUnit.SECONDS)));
  {% endhighlight %}
      </td>
    </tr>
   <tr>
       <td><strong>Count evictor</strong></td>
       <td>
         <p>
          Retain 1000 elements from the end of the window backwards, evicting all others.
         </p>
   {% highlight java %}
triggeredStream.evictor(CountEvictor.of(1000));
   {% endhighlight %}
       </td>
     </tr>
    <tr>
        <td><strong>Delta evictor</strong></td>
        <td>
          <p>
            Starting from the beginning of the window, evict elements until an element with
            value lower than the value of the last element is found (by a threshold and a
            DeltaFunction).
          </p>
    {% highlight java %}
triggeredStream.evictor(DeltaEvictor.of(5000, new DeltaFunction<Double>() {
  public double (Double oldValue, Double newValue) {
      return newValue - oldValue;
  }
}));
    {% endhighlight %}
        </td>
      </tr>
 </tbody>
</table>
</div>

<div data-lang="scala" markdown="1">
<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 25%">Transformation</th>
      <th class="text-center">Description</th>
    </tr>
  </thead>
  <tbody>
  <tr>
      <td><strong>Time evictor</strong></td>
      <td>
        <p>
         Evict all elements from the beginning of the window, so that elements from end-value - 1 second
         until end-value are retained (the resulting window size is 1 second).
        </p>
  {% highlight scala %}
triggeredStream.evictor(TimeEvictor.of(Time.of(1, TimeUnit.SECONDS)));
  {% endhighlight %}
      </td>
    </tr>
   <tr>
       <td><strong>Count evictor</strong></td>
       <td>
         <p>
          Retain 1000 elements from the end of the window backwards, evicting all others.
         </p>
   {% highlight scala %}
triggeredStream.evictor(CountEvictor.of(1000));
   {% endhighlight %}
       </td>
     </tr>
    <tr>
        <td><strong>Delta evictor</strong></td>
        <td>
          <p>
            Starting from the beginning of the window, evict elements until an element with
            value lower than the value of the last element is found (by a threshold and a
            DeltaFunction).
          </p>
    {% highlight scala %}
windowedStream.evictor(DeltaEvictor.of(5000.0, { (old,new) => new - old > 0.01 }))
    {% endhighlight %}
        </td>
      </tr>
 </tbody>
</table>
</div>

</div>

#### Recipes for Building Windows

The mechanism of window assigner, trigger, and evictor is very powerful, and it allows you to define
many different kinds of windows. Flink's basic window constructs are, in fact, syntactic
sugar on top of the general mechanism. Below is how some common types of windows can be
constructed using the general mechanism

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 35%">Window type</th>
      <th class="text-center">Definition</th>
    </tr>
  </thead>
  <tbody>
      <tr>
        <td>
	  <strong>Tumbling count window</strong><br>
    {% highlight java %}
stream.countWindow(1000)
    {% endhighlight %}
	</td>
        <td>
    {% highlight java %}
stream.window(GlobalWindows.create())
  .trigger(CountTrigger.of(1000)
  .evictor(CountEvictor.of(1000)))
    {% endhighlight %}
        </td>
      </tr>
      <tr>
        <td>
	  <strong>Sliding count window</strong><br>
    {% highlight java %}
stream.countWindow(1000, 100)
    {% endhighlight %}
	</td>
        <td>
    {% highlight java %}
stream.window(GlobalWindows.create())
  .evictor(CountEvictor.of(1000))
  .trigger(CountTrigger.of(100))
    {% endhighlight %}
        </td>
      </tr>
      <tr>
        <td>
	  <strong>Tumbling event time window</strong><br>
    {% highlight java %}
stream.timeWindow(Time.of(5, TimeUnit.SECONDS))
    {% endhighlight %}
	</td>
        <td>
    {% highlight java %}
stream.window(TumblingTimeWindows.of((Time.of(5, TimeUnit.SECONDS)))
  .trigger(EventTimeTrigger.create())
    {% endhighlight %}
        </td>
      </tr>
      <tr>
        <td>
	  <strong>Sliding event time window</strong><br>
    {% highlight java %}
stream.timeWindow(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS))
    {% endhighlight %}
	</td>
        <td>
    {% highlight java %}
stream.window(SlidingTimeWindows.of(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS)))
  .trigger(EventTimeTrigger.create())
    {% endhighlight %}
        </td>
      </tr>
      <tr>
        <td>
	  <strong>Tumbling processing time window</strong><br>
    {% highlight java %}
stream.timeWindow(Time.of(5, TimeUnit.SECONDS))
    {% endhighlight %}
	</td>
        <td>
    {% highlight java %}
stream.window(TumblingTimeWindows.of((Time.of(5, TimeUnit.SECONDS)))
  .trigger(ProcessingTimeTrigger.create())
    {% endhighlight %}
        </td>
      </tr>
      <tr>
        <td>
	  <strong>Sliding processing time window</strong><br>
    {% highlight java %}
stream.timeWindow(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS))
    {% endhighlight %}
	</td>
        <td>
    {% highlight java %}
stream.window(SlidingTimeWindows.of(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS)))
  .trigger(ProcessingTimeTrigger.create())
    {% endhighlight %}
        </td>
      </tr>
  </tbody>
</table>


### Windows on Unkeyed Data Streams

You can also define windows on regular (non-keyed) data streams using the `windowAll` transformation. These
windowed data streams have all the capabilities of keyed windowed data streams, but are evaluated at a single
task (and hence at a single computing node). The syntax for defining triggers and evictors is exactly the
same:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
nonKeyedStream
    .windowAll(SlidingTimeWindows.of(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS))
    .trigger(CountTrigger.of(100))
    .evictor(CountEvictor.of(10));
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
nonKeyedStream
    .windowAll(SlidingTimeWindows.of(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS))
    .trigger(CountTrigger.of(100))
    .evictor(CountEvictor.of(10))
{% endhighlight %}
</div>
</div>

Basic window definitions are also available for windows on non-keyed streams:

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
        <td><strong>Tumbling time window all</strong><br>DataStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 5 seconds, that "tumbles". This means that elements are
          grouped according to their timestamp in groups of 5 second duration, and every element belongs to exactly one window.
          The notion of time used is controlled by the StreamExecutionEnvironment.
    {% highlight java %}
nonKeyedStream.timeWindowAll(Time.of(5, TimeUnit.SECONDS));
    {% endhighlight %}
          </p>
        </td>
      </tr>
      <tr>
          <td><strong>Sliding time window all</strong><br>DataStream &rarr; WindowedStream</td>
          <td>
            <p>
             Defines a window of 5 seconds, that "slides" by 1 seconds. This means that elements are
             grouped according to their timestamp in groups of 5 second duration, and elements can belong to more than
             one window (since windows overlap by at least 4 seconds)
             The notion of time used is controlled by the StreamExecutionEnvironment.
      {% highlight java %}
nonKeyedStream.timeWindowAll(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS));
      {% endhighlight %}
            </p>
          </td>
        </tr>
      <tr>
        <td><strong>Tumbling count window all</strong><br>DataStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 1000 elements, that "tumbles". This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element belongs to exactly one window.
    {% highlight java %}
nonKeyedStream.countWindowAll(1000)
    {% endhighlight %}
        </p>
        </td>
      </tr>
      <tr>
      <td><strong>Sliding count window all</strong><br>DataStream &rarr; WindowedStream</td>
      <td>
        <p>
          Defines a window of 1000 elements, that "slides" every 100 elements. This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element can belong to more than one window (as windows overlap by at least 900 elements).
  {% highlight java %}
nonKeyedStream.countWindowAll(1000, 100)
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
        <td><strong>Tumbling time window all</strong><br>DataStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 5 seconds, that "tumbles". This means that elements are
          grouped according to their timestamp in groups of 5 second duration, and every element belongs to exactly one window.
          The notion of time used is controlled by the StreamExecutionEnvironment.
    {% highlight scala %}
nonKeyedStream.timeWindowAll(Time.of(5, TimeUnit.SECONDS));
    {% endhighlight %}
          </p>
        </td>
      </tr>
      <tr>
          <td><strong>Sliding time window all</strong><br>DataStream &rarr; WindowedStream</td>
          <td>
            <p>
             Defines a window of 5 seconds, that "slides" by 1 seconds. This means that elements are
             grouped according to their timestamp in groups of 5 second duration, and elements can belong to more than
             one window (since windows overlap by at least 4 seconds)
             The notion of time used is controlled by the StreamExecutionEnvironment.
      {% highlight scala %}
nonKeyedStream.timeWindowAll(Time.of(5, TimeUnit.SECONDS), Time.of(1, TimeUnit.SECONDS));
      {% endhighlight %}
            </p>
          </td>
        </tr>
      <tr>
        <td><strong>Tumbling count window all</strong><br>DataStream &rarr; WindowedStream</td>
        <td>
          <p>
          Defines a window of 1000 elements, that "tumbles". This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element belongs to exactly one window.
    {% highlight scala %}
nonKeyedStream.countWindowAll(1000)
    {% endhighlight %}
        </p>
        </td>
      </tr>
      <tr>
      <td><strong>Sliding count window all</strong><br>DataStream &rarr; WindowedStream</td>
      <td>
        <p>
          Defines a window of 1000 elements, that "slides" every 100 elements. This means that elements are
          grouped according to their arrival time (equivalent to processing time) in groups of 1000 elements,
          and every element can belong to more than one window (as windows overlap by at least 900 elements).
  {% highlight scala %}
nonKeyedStream.countWindowAll(1000, 100)
  {% endhighlight %}
        </p>
      </td>
    </tr>
  </tbody>
</table>

</div>
</div>

[Back to top](#top)

Execution Parameters
--------------------

### Fault Tolerance

The [Fault Tolerance Documentation]({{ site.baseurl }}/apis/fault_tolerance.html) describes the options and parameters to enable and configure Flink's checkpointing mechanism.

### Parallelism

You can control the number of parallel instances created for each operator by
calling the `operator.setParallelism(int)` method.

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

env.genereateSequence(1,10).map(new MyMapper()).setBufferTimeout(timeoutMillis);
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

[Back to top](#top)

Working with State
------------------

All transformations in Flink may look like functions (in the functional processing terminology), but
are in fact stateful operators. You can make *every* transformation (`map`, `filter`, etc) stateful
by declaring local variables or using Flink's state interface. You can register any local variable
as ***managed*** state by implementing an interface. In this case, and also in the case of using
Flink's native state interface, Flink will automatically take consistent snapshots of your state
periodically, and restore its value in the case of a failure.

The end effect is that updates to any form of state are the same under failure-free execution and
execution under failures.

First, we look at how to make local variables consistent under failures, and then we look at
Flink's state interface.

By default state checkpoints will be stored in-memory at the JobManager. For proper persistence of large
state, Flink supports storing the checkpoints on file systems (HDFS, S3, or any mounted POSIX file system),
which can be configured in the `flink-conf.yaml` or via `StreamExecutionEnvironment.setStateBackend(…)`.


### Checkpointing Local Variables

Local variables can be checkpointed by using the `Checkpointed` interface.

When the user-defined function implements the `Checkpointed` interface, the `snapshotState(…)` and `restoreState(…)`
methods will be executed to draw and restore function state.

In addition to that, user functions can also implement the `CheckpointNotifier` interface to receive notifications on
completed checkpoints via the `notifyCheckpointComplete(long checkpointId)` method.
Note that there is no guarantee for the user function to receive a notification if a failure happens between
checkpoint completion and notification. The notifications should hence be treated in a way that notifications from
later checkpoints can subsume missing notifications.

For example the same counting, reduce function shown for `OperatorState`s by using the `Checkpointed` interface instead:

{% highlight java %}
public class CounterSum extends ReduceFunction<Long>, Checkpointed<Long> {

    // persistent counter
    private long counter = 0;

    @Override
    public Long reduce(Long value1, Long value2) {
        counter++;
        return value1 + value2;
    }

    // regularly persists state during normal operation
    @Override
    public Serializable snapshotState(long checkpointId, long checkpointTimestamp) {
        return counter;
    }

    // restores state on recovery from failure
    @Override
    public void restoreState(Long state) {
        counter = state;
    }
}
{% endhighlight %}

### Using the Key/Value State Interface

The state interface gives access to key/value states, which are a collection of key/value pairs.
Because the state is partitioned by the keys (distributed accross workers), it can only be used
on the `KeyedStream`, created via `stream.keyBy(…)` (which means also that it is usable in all
types of functions on keyed windows).

The handle to the state can be obtained from the function's `RuntimeContext`. The state handle will
then give access to the value mapped under the key of the current record or window - each key consequently
has its own value.

The following code sample shows how to use the key/value state inside a reduce function.
When creating the state handle, one needs to supply a name for that state (a function can have multiple states
of different types), the type of the state (used to create efficient serializers), and the default value (returned
as a value for keys that do not yet have a value associated).

{% highlight java %}
public class CounterSum extends RichReduceFunction<Long> {

    /** The state handle */
    private OperatorState<Long> counter;

    @Override
    public Long reduce(Long value1, Long value2) {
        counter.update(counter.value() + 1);
        return value1 + value2;
    }

    @Override
    public void open(Configuration config) {
        counter = getRuntimeContext().getKeyValueState("myCounter", Long.class, 0L);
    }
}
{% endhighlight %}

State updated by this is usually kept locally inside the flink process (unless one configures explicitly
an external state backend). This means that lookups and updates are process local and this very fast.

The important implication of having the keys set implicitly is that it forces programs to group the stream
by key (via the `keyBy()` function), making the key partitioning transparent to Flink. That allows the system
to efficiently restore and redistribute keys and state.

The Scala API has shortcuts that for stateful `map()` or `flatMap()` functions on `KeyedStream`, which give the
state of the current key as an option directly into the function, and return the result with a state update:

{% highlight scala %}
val stream: DataStream[(String, Int)] = ...

val counts: DataStream[(String, Int)] = stream
  .keyBy(_._1)
  .mapWithState((in: (String, Int), count: Option[Int]) =>
    count match {
      case Some(c) => ( (in._1, c), Some(c + in._2) )
      case None => ( (in._1, 0), Some(in._2) )
    })
{% endhighlight %}


### Stateful Source Functions

Stateful sources require a bit more care as opposed to other operators.
In order to make the updates to the state and output collection atomic (required for exactly-once semantics
on failure/recovery), the user is required to get a lock from the source's context.

{% highlight java %}
public static class CounterSource extends RichParallelSourceFunction<Long>, Checkpointed<Long> {

    /**  current offset for exactly once semantics */
    private long offset;

    /** flag for job cancellation */
    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> ctx) {
        final Object lock = ctx.getCheckpointLock();

        while (isRunning) {
            // output and state update are atomic
            synchronized (lock) {
                ctx.collect(offset);
                offset += 1;
            }
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public Long snapshotState(long checkpointId, long checkpointTimestamp) {
        return offset;

    }

    @Override
	public void restoreState(Long state) {
        offset = state;
    }
}
{% endhighlight %}

Some operators might need the information when a checkpoint is fully acknowledged by Flink to communicate that with the outside world. In this case see the `flink.streaming.api.checkpoint.CheckpointNotifier` interface.

### State Checkpoints in Iterative Jobs

Flink currently only provides processing guarantees for jobs without iterations. Enabling checkpointing on an iterative job causes an exception. In order to force checkpointing on an iterative program the user needs to set a special flag when enabling checkpointing: `env.enableCheckpointing(interval, force = true)`.

Please note that records in flight in the loop edges (and the state changes associated with them) will be lost during failure.

[Back to top](#top)

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

Then, we specify the logic that will be executed inside the loop using a series of trasformations (here
a simple `map` transformation)

{% highlight java %}
DataStream<Integer> iterationBody = iteration.map(/* this is executed many times */);
{% endhighlight %}

To close an iteration and define the iteration tail, call the `closeWith(feedbackStream)` method of the `IterativeStream`.
The DataStream given to the `closeWith` function will be fed back to the iteration head.
A common pattern is to use a filter to separate the part of the strem that is fed back,
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

[Back to top](#top)

Connectors
----------

<!-- TODO: reintroduce flume -->
Connectors provide code for interfacing with various third-party systems.

Currently these systems are supported:

 * [Apache Kafka](https://kafka.apache.org/) (sink/source)
 * [Elasticsearch](https://elastic.co/) (sink)
 * [Hadoop FileSystem](http://hadoop.apache.org) (sink)
 * [RabbitMQ](http://www.rabbitmq.com/) (sink/source)
 * [Twitter Streaming API](https://dev.twitter.com/docs/streaming-apis) (source)

To run an application using one of these connectors, additional third party
components are usually required to be installed and launched, e.g. the servers
for the message queues. Further instructions for these can be found in the
corresponding subsections. [Docker containers](#docker-containers-for-connectors)
are also provided encapsulating these services to aid users getting started
with connectors.

### Apache Kafka

This connector provides access to event streams served by [Apache Kafka](https://kafka.apache.org/).

Flink provides special Kafka Connectors for reading and writing data from/to Kafka topics.
The Flink Kafka Consumer integrates with Flink's checkpointing mechanism to provide
exactly-once processing semantics. To achieve that, Flink does not purely rely on Kafka's consumer group
offset tracking, but tracks and checkpoints these offsets internally as well.

Please pick a package (maven artifact id) and class name for your use-case and environment.
For most users, the `FlinkKafkaConsumer082` (part of `flink-connector-kafka`) is appropriate.


<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left">Maven Dependency</th>
      <th class="text-left">Supported since</th>
      <th class="text-left">Class name</th>
      <th class="text-left">Kafka version</th>
      <th class="text-left">Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>flink-connector-kafka</td>
        <td>0.9.1, 0.10</td>
        <td>FlinkKafkaConsumer081</td>
        <td>0.8.1</td>
        <td>Uses the <a href="https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example">SimpleConsumer</a> API of Kafka internally. Offsets are committed to ZK by Flink.</td>
    </tr>
    <tr>
        <td>flink-connector-kafka</td>
        <td>0.9.1, 0.10</td>
        <td>FlinkKafkaConsumer082</td>
        <td>0.8.2</td>
        <td>Uses the <a href="https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example">SimpleConsumer</a> API of Kafka internally. Offsets are committed to ZK by Flink.</td>
    </tr>
  </tbody>
</table>

Then, import the connector in your maven project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kafka</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary distribution. See how to link with them for cluster execution [here](cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

#### Installing Apache Kafka

* Follow the instructions from [Kafka's quickstart](https://kafka.apache.org/documentation.html#quickstart) to download the code and launch a server (launching a Zookeeper and a Kafka server is required every time before starting the application).
* On 32 bit computers [this](http://stackoverflow.com/questions/22325364/unrecognized-vm-option-usecompressedoops-when-running-kafka-from-my-ubuntu-in) problem may occur.
* If the Kafka and Zookeeper servers are running on a remote machine, then the `advertised.host.name` setting in the `config/server.properties` file must be set to the machine's IP address.

#### Kafka Consumer

The standard `FlinkKafkaConsumer082` is a Kafka consumer providing access to one topic. It takes the following parameters to the constructor:

1. The topic name
2. A DeserializationSchema
3. Properties for the Kafka consumer.
  The following properties are required:
  - "bootstrap.servers" (comma separated list of Kafka brokers)
  - "zookeeper.connect" (comma separated list of Zookeeper servers)
  - "group.id" the id of the consumer group

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("zookeeper.connect", "localhost:2181");
properties.setProperty("group.id", "test");
DataStream<String> stream = env
	.addSource(new FlinkKafkaConsumer082<>("topic", new SimpleStringSchema(), properties))
	.print();
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("zookeeper.connect", "localhost:2181");
properties.setProperty("group.id", "test");
stream = env
    .addSource(new FlinkKafkaConsumer082[String]("topic", new SimpleStringSchema(), properties))
    .print
{% endhighlight %}
</div>
</div>

#### Kafka Consumers and Fault Tolerance

With Flink's checkpointing enabled, the Flink Kafka Consumer will consume records from a topic and periodically checkpoint all
its Kafka offsets, together with the state of other operations, in a consistent manner. In case of a job failure, Flink will restore
the streaming program to the state of the latest checkpoint and re-consume the records from Kafka, starting from the offsets that where
stored in the checkpoint.

The interval of drawing checkpoints therefore defines how much the program may have to go back at most, in case of a failure.

To use fault tolerant Kafka Consumers, checkpointing of the topology needs to be enabled at the execution environment:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000); // checkpoint every 5000 msecs
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.enableCheckpointing(5000) // checkpoint every 5000 msecs
{% endhighlight %}
</div>
</div>

Also note that Flink can only restart the topology if enough processing slots are available to restart the topology.
So if the topology fails due to loss of a TaskManager, there must still be enough slots available afterwards.
Flink on YARN supports automatic restart of lost YARN containers.

If checkpointing is not enabled, the Kafka consumer will periodically commit the offsets to Zookeeper.

#### Kafka Producer

The `FlinkKafkaProducer` writes data to a Kafka topic. The producer can specify a custom partitioner that assigns
recors to partitions.

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
stream.addSink(new FlinkKafkaProducer<String>("localhost:9092", "my-topic", new SimpleStringSchema()));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
stream.addSink(new FlinkKafkaProducer[String]("localhost:9092", "my-topic", new SimpleStringSchema()))
{% endhighlight %}
</div>
</div>

You can also define a custom Kafka producer configuration for the KafkaSink with the constructor. Please refer to
the [Apache Kafka documentation](https://kafka.apache.org/documentation.html) for details on how to configure
Kafka Producers.

[Back to top](#top)

### Elasticsearch

This connector provides a Sink that can write to an
[Elasticsearch](https://elastic.co/) Index. To use this connector, add the
following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-elasticsearch</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary
distribution. See
[here](cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution)
for information about how to package the program with the libraries for
cluster execution.

#### Installing Elasticsearch

Instructions for setting up an Elasticsearch cluster can be found
[here](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup.html).
Make sure to set and remember a cluster name. This must be set when
creating a Sink for writing to your cluster

#### Elasticsearch Sink
The connector provides a Sink that can send data to an Elasticsearch Index.

The sink can use two different methods for communicating with Elasticsearch:

1. An embedded Node
2. The TransportClient

See [here](https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/client.html)
for information about the differences between the two modes.

This code shows how to create a sink that uses an embedded Node for
communication:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> input = ...;

Map<String, String> config = Maps.newHashMap();
// This instructs the sink to emit after every element, otherwise they would be buffered
config.put("bulk.flush.max.actions", "1");
config.put("cluster.name", "my-cluster-name");

input.addSink(new ElasticsearchSink<>(config, new IndexRequestBuilder<String>() {
    @Override
    public IndexRequest createIndexRequest(String element, RuntimeContext ctx) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);

        return Requests.indexRequest()
                .index("my-index")
                .type("my-type")
                .source(json);
    }
}));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[String] = ...

val config = new util.HashMap[String, String]
config.put("bulk.flush.max.actions", "1")
config.put("cluster.name", "my-cluster-name")

text.addSink(new ElasticsearchSink(config, new IndexRequestBuilder[String] {
  override def createIndexRequest(element: String, ctx: RuntimeContext): IndexRequest = {
    val json = new util.HashMap[String, AnyRef]
    json.put("data", element)
    println("SENDING: " + element)
    Requests.indexRequest.index("my-index").`type`("my-type").source(json)
  }
}))
{% endhighlight %}
</div>
</div>

Note how a Map of Strings is used to configure the Sink. The configuration keys
are documented in the Elasticsearch documentation
[here](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html).
Especially important is the `cluster.name` parameter that must correspond to
the name of your cluster.

Internally, the sink uses a `BulkProcessor` to send index requests to the cluster.
This will buffer elements before sending a request to the cluster. The behaviour of the
`BulkProcessor` can be configured using these config keys:
 * **bulk.flush.max.actions**: Maximum amount of elements to buffer
 * **bulk.flush.max.size.mb**: Maximum amount of data (in megabytes) to buffer
 * **bulk.flush.interval.ms**: Interval at which to flush data regardless of the other two
  settings in milliseconds

This example code does the same, but with a `TransportClient`:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> input = ...;

Map<String, String> config = Maps.newHashMap();
// This instructs the sink to emit after every element, otherwise they would be buffered
config.put("bulk.flush.max.actions", "1");
config.put("cluster.name", "my-cluster-name");

List<TransportAddress> transports = new ArrayList<String>();
transports.add(new InetSocketTransportAddress("node-1", 9300));
transports.add(new InetSocketTransportAddress("node-2", 9300));

input.addSink(new ElasticsearchSink<>(config, transports, new IndexRequestBuilder<String>() {
    @Override
    public IndexRequest createIndexRequest(String element, RuntimeContext ctx) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);

        return Requests.indexRequest()
                .index("my-index")
                .type("my-type")
                .source(json);
    }
}));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[String] = ...

val config = new util.HashMap[String, String]
config.put("bulk.flush.max.actions", "1")
config.put("cluster.name", "my-cluster-name")

val transports = new ArrayList[String]
transports.add(new InetSocketTransportAddress("node-1", 9300))
transports.add(new InetSocketTransportAddress("node-2", 9300))

text.addSink(new ElasticsearchSink(config, transports, new IndexRequestBuilder[String] {
  override def createIndexRequest(element: String, ctx: RuntimeContext): IndexRequest = {
    val json = new util.HashMap[String, AnyRef]
    json.put("data", element)
    println("SENDING: " + element)
    Requests.indexRequest.index("my-index").`type`("my-type").source(json)
  }
}))
{% endhighlight %}
</div>
</div>

The difference is that we now need to provide a list of Elasticsearch Nodes
to which the sink should connect using a `TransportClient`.

More about information about Elasticsearch can be found [here](https://elastic.co).

[Back to top](#top)

### Hadoop FileSystem

This connector provides a Sink that writes rolling files to any filesystem supported by
Hadoop FileSystem. To use this connector, add the
following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-filesystem</artifactId>
  <version>{{site.version}}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary
distribution. See
[here](cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution)
for information about how to package the program with the libraries for
cluster execution.

#### Rolling File Sink

The rolling behaviour as well as the writing can be configured but we will get to that later.
This is how you can create a default rolling sink:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> input = ...;

input.addSink(new RollingSink<String>("/base/path"));

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[String] = ...

input.addSink(new RollingSink("/base/path"))

{% endhighlight %}
</div>
</div>

The only required parameter is the base path where the rolling files (buckets) will be
stored. The sink can be configured by specifying a custom bucketer, writer and batch size.

By default the rolling sink will use the pattern `"yyyy-MM-dd--HH"` to name the rolling buckets.
This pattern is passed to `SimpleDateFormat` with the current system time to form a bucket path. A
new bucket will be created whenever the bucket path changes. For example, if you have a pattern
that contains minutes as the finest granularity you will get a new bucket every minute.
Each bucket is itself a directory that contains several part files: Each parallel instance
of the sink will create its own part file and when part files get too big the sink will also
create a new part file next to the others. To specify a custom bucketer use `setBucketer()`
on a `RollingSink`.

The default writer is `StringWriter`. This will call `toString()` on the incoming elements
and write them to part files, separated by newline. To specify a custom writer use `setWriter()`
on a `RollingSink`. If you want to write Hadoop SequenceFiles you can use the provided
`SequenceFileWriter` which can also be configured to use compression.

The last configuration option is the batch size. This specifies when a part file should be closed
and a new one started. (The default part file size is 384 MB).

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple2<IntWritable,Text>> input = ...;

RollingSink sink = new RollingSink<String>("/base/path");
sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm"));
sink.setWriter(new SequenceFileWriter<IntWritable, Text>());
sink.setBatchSize(1024 * 1024 * 400); // this is 400 MB,

input.addSink(sink);

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[Tuple2[IntWritable, Text]] = ...

val sink = new RollingSink[String]("/base/path")
sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm"))
sink.setWriter(new SequenceFileWriter[IntWritable, Text]())
sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,

input.addSink(sink)

{% endhighlight %}
</div>
</div>

This will create a sink that writes to bucket files that follow this schema:

```
/base/path/{date-time}/part-{parallel-task}-{count}
```

Where `date-time` is the string that we get from the date/time format, `parallel-task` is the index
of the parallel sink instance and `count` is the running number of part files that where created
because of the batch size.

For in-depth information, please refer to the JavaDoc for
[RollingSink](http://flink.apache.org/docs/latest/api/java/org/apache/flink/streaming/connectors/fs/RollingSink.html).

[Back to top](#top)

### RabbitMQ

This connector provides access to data streams from [RabbitMQ](http://www.rabbitmq.com/). To use this connector, add the following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-rabbitmq</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary distribution. See linking with them for cluster execution [here](cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

#### Installing RabbitMQ
Follow the instructions from the [RabbitMQ download page](http://www.rabbitmq.com/download.html). After the installation the server automatically starts, and the application connecting to RabbitMQ can be launched.

#### RabbitMQ Source

A class which provides an interface for receiving data from RabbitMQ.

The followings have to be provided for the `RMQSource(…)` constructor in order:

- hostName: The RabbitMQ broker hostname.
- queueName: The RabbitMQ queue name.
- usesCorrelationId: `true` when correlation ids should be used, `false` otherwise (default is `false`).
- deserializationScehma: Deserialization schema to turn messages into Java objects.

This source can be operated in three different modes:

1. Exactly-once (when checkpointed) with RabbitMQ transactions and messages with
    unique correlation IDs.
2. At-least-once (when checkpointed) with RabbitMQ transactions but no deduplication mechanism
    (correlation id is not set).
3. No strong delivery guarantees (without checkpointing) with RabbitMQ auto-commit mode.

Correlation ids are a RabbitMQ application feature. You have to set it in the message properties
when injecting messages into RabbitMQ. If you set `usesCorrelationId` to true and do not supply
unique correlation ids, the source will throw an exception (if the correlation id is null) or ignore
messages with non-unique correlation ids. If you set `usesCorrelationId` to false, then you don't
have to supply correlation ids.

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> streamWithoutCorrelationIds = env
	.addSource(new RMQSource<String>("localhost", "hello", new SimpleStringSchema()))
	.print

DataStream<String> streamWithCorrelationIds = env
	.addSource(new RMQSource<String>("localhost", "hello", true, new SimpleStringSchema()))
	.print
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
streamWithoutCorrelationIds = env
    .addSource(new RMQSource[String]("localhost", "hello", new SimpleStringSchema))
    .print

streamWithCorrelationIds = env
    .addSource(new RMQSource[String]("localhost", "hello", true, new SimpleStringSchema))
    .print
{% endhighlight %}
</div>
</div>

#### RabbitMQ Sink
A class providing an interface for sending data to RabbitMQ.

The followings have to be provided for the `RMQSink(…)` constructor in order:

1. The hostname
2. The queue name
3. Serialization schema

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
stream.addSink(new RMQSink<String>("localhost", "hello", new StringToByteSerializer()));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
stream.addSink(new RMQSink[String]("localhost", "hello", new StringToByteSerializer))
{% endhighlight %}
</div>
</div>

More about RabbitMQ can be found [here](http://www.rabbitmq.com/).

[Back to top](#top)

### Twitter Streaming API

Twitter Streaming API provides opportunity to connect to the stream of tweets made available by Twitter. Flink Streaming comes with a built-in `TwitterSource` class for establishing a connection to this stream. To use this connector, add the following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-twitter</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary distribution. See linking with them for cluster execution [here](cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

#### Authentication
In order to connect to Twitter stream the user has to register their program and acquire the necessary information for the authentication. The process is described below.

#### Acquiring the authentication information
First of all, a Twitter account is needed. Sign up for free at [twitter.com/signup](https://twitter.com/signup) or sign in at Twitter's [Application Management](https://apps.twitter.com/) and register the application by clicking on the "Create New App" button. Fill out a form about your program and accept the Terms and Conditions.
After selecting the application, the API key and API secret (called `consumerKey` and `consumerSecret` in `TwitterSource` respectively) is located on the "API Keys" tab. The necessary OAuth Access Token data (`token` and `secret` in `TwitterSource`) can be generated and acquired on the "Keys and Access Tokens" tab.
Remember to keep these pieces of information secret and do not push them to public repositories.

#### Accessing the authentication information
Create a properties file, and pass its path in the constructor of `TwitterSource`. The content of the file should be similar to this:

~~~bash
#properties file for my app
secret=***
consumerSecret=***
token=***-***
consumerKey=***
~~~

#### Constructors
The `TwitterSource` class has two constructors.

1. `public TwitterSource(String authPath, int numberOfTweets);`
to emit a finite number of tweets
2. `public TwitterSource(String authPath);`
for streaming

Both constructors expect a `String authPath` argument determining the location of the properties file containing the authentication information. In the first case, `numberOfTweets` determines how many tweet the source emits.

#### Usage
In contrast to other connectors, the `TwitterSource` depends on no additional services. For example the following code should run gracefully:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> streamSource = env.addSource(new TwitterSource("/PATH/TO/myFile.properties"));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
streamSource = env.addSource(new TwitterSource("/PATH/TO/myFile.properties"))
{% endhighlight %}
</div>
</div>

The `TwitterSource` emits strings containing a JSON code.
To retrieve information from the JSON code you can add a FlatMap or a Map function handling JSON code. For example, there is an implementation `JSONParseFlatMap` abstract class among the examples. `JSONParseFlatMap` is an extension of the `FlatMapFunction` and has a

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
String getField(String jsonText, String field);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
getField(jsonText : String, field : String) : String
{% endhighlight %}
</div>
</div>

function which can be use to acquire the value of a given field.

There are two basic types of tweets. The usual tweets contain information such as date and time of creation, id, user, language and many more details. The other type is the delete information.

#### Example
`TwitterStream` is an example of how to use `TwitterSource`. It implements a language frequency counter program.

[Back to top](#top)

### Docker containers for connectors

A Docker container is provided with all the required configurations for test running the connectors of Apache Flink. The servers for the message queues will be running on the docker container while the example topology can be run on the user's computer.

#### Installing Docker
The official Docker installation guide can be found [here](https://docs.docker.com/installation/).
After installing Docker an image can be pulled for each connector. Containers can be started from these images where all the required configurations are set.

#### Creating a jar with all the dependencies
For the easiest setup, create a jar with all the dependencies of the *flink-streaming-connectors* project.

~~~bash
cd /PATH/TO/GIT/flink/flink-staging/flink-streaming-connectors
mvn assembly:assembly
~~~bash

This creates an assembly jar under *flink-streaming-connectors/target*.

#### RabbitMQ
Pull the docker image:

~~~bash
sudo docker pull flinkstreaming/flink-connectors-rabbitmq
~~~

To run the container, type:

~~~bash
sudo docker run -p 127.0.0.1:5672:5672 -t -i flinkstreaming/flink-connectors-rabbitmq
~~~

Now a terminal has started running from the image with all the necessary configurations to test run the RabbitMQ connector. The -p flag binds the localhost's and the Docker container's ports so RabbitMQ can communicate with the application through these.

To start the RabbitMQ server:

~~~bash
sudo /etc/init.d/rabbitmq-server start
~~~

To launch the example on the host computer, execute:

~~~bash
java -cp /PATH/TO/JAR-WITH-DEPENDENCIES org.apache.flink.streaming.connectors.rabbitmq.RMQTopology \
> log.txt 2> errorlog.txt
~~~

There are two connectors in the example. One that sends messages to RabbitMQ, and one that receives messages from the same queue. In the logger messages, the arriving messages can be observed in the following format:

~~~
<DATE> INFO rabbitmq.RMQTopology: String: <one> arrived from RMQ
<DATE> INFO rabbitmq.RMQTopology: String: <two> arrived from RMQ
<DATE> INFO rabbitmq.RMQTopology: String: <three> arrived from RMQ
<DATE> INFO rabbitmq.RMQTopology: String: <four> arrived from RMQ
<DATE> INFO rabbitmq.RMQTopology: String: <five> arrived from RMQ
~~~

#### Apache Kafka

Pull the image:

~~~bash
sudo docker pull flinkstreaming/flink-connectors-kafka
~~~

To run the container type:

~~~bash
sudo docker run -p 127.0.0.1:2181:2181 -p 127.0.0.1:9092:9092 -t -i \
flinkstreaming/flink-connectors-kafka
~~~

Now a terminal has started running from the image with all the necessary configurations to test run the Kafka connector. The -p flag binds the localhost's and the Docker container's ports so Kafka can communicate with the application through these.
First start a zookeeper in the background:

~~~bash
/kafka_2.9.2-0.8.1.1/bin/zookeeper-server-start.sh /kafka_2.9.2-0.8.1.1/config/zookeeper.properties \
> zookeeperlog.txt &
~~~

Then start the kafka server in the background:

~~~bash
/kafka_2.9.2-0.8.1.1/bin/kafka-server-start.sh /kafka_2.9.2-0.8.1.1/config/server.properties \
 > serverlog.txt 2> servererr.txt &
~~~

To launch the example on the host computer execute:

~~~bash
java -cp /PATH/TO/JAR-WITH-DEPENDENCIES org.apache.flink.streaming.connectors.kafka.KafkaTopology \
> log.txt 2> errorlog.txt
~~~


In the example there are two connectors. One that sends messages to Kafka, and one that receives messages from the same queue. In the logger messages, the arriving messages can be observed in the following format:

~~~
<DATE> INFO kafka.KafkaTopology: String: (0) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (1) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (2) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (3) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (4) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (5) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (6) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (7) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (8) arrived from Kafka
<DATE> INFO kafka.KafkaTopology: String: (9) arrived from Kafka
~~~


[Back to top](#top)

Program Packaging & Distributed Execution
-----------------------------------------

See [the relevant section of the DataSet API documentation](programming_guide.html#program-packaging-and-distributed-execution).

[Back to top](#top)

Parallel Execution
------------------

See [the relevant section of the DataSet API documentation](programming_guide.html#parallel-execution).

[Back to top](#top)

Execution Plans
---------------

See [the relevant section of the DataSet API documentation](programming_guide.html#execution-plans).

[Back to top](#top)
