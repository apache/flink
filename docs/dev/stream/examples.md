---
title:  "DataStream Examples"
nav-title: DataStream Examples
nav-parent_id: examples
nav-pos: 15
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

The following example programs showcase different applications of Flink
from simple word counting to graph algorithms. The code samples illustrate the
use of [Flink's DataStream API]({{ site.baseurl }}/dev/datastream_api.html).

The full source code of the following and more examples can be found in the __flink-examples-streaming__ module of the Flink source repository.

* This will be replaced by the TOC
{:toc}


## Running an example

In order to run a Flink example, we assume you have a running Flink instance available. The "Quickstart" and "Setup" tabs in the navigation describe various ways of starting Flink.

The easiest way is running the `./bin/start-cluster.sh`, which by default starts a local cluster with one JobManager and one TaskManager.

Each binary release of Flink contains an `examples` directory with jar files for each of the examples on this page.

To run the WordCount example, issue the following command:

{% highlight bash %}
$ ./bin/flink run ./examples/streaming/WordCount.jar
{% endhighlight %}

The other examples can be started in a similar way.

Note that many examples run without passing any arguments for them, by using build-in data. To run WordCount with real data, you have to pass the path to the data:

{% highlight bash %}
$ ./bin/flink run ./examples/streaming/WordCount.jar --input /path/to/some/text/data --output /path/to/result
{% endhighlight %}

Note that non-local file systems require a schema prefix, such as `hdfs://`.


## Word Count
WordCount is the "Hello World" of Big Data processing systems. It computes the frequency of words in a text collection. The algorithm works in two steps: First, the texts are splits the text to individual words. Second, the words are grouped and counted.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
// set up the execution environment
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// get input data
DataStream<String> text = env.readTextFile(params.get("input"));;

DataStream<Tuple2<String, Integer>> counts = text.flatMap(new Tokenizer()).setParallelism(parallelism)
    // group by the tuple field "0" and sum up tuple field "1"
    .keyBy(0).sum(1).setParallelism(parallelism);

counts.writeAsText(params.get("output"));

// execute program
env.execute("Streaming WordCount");


///////////////////////////////////////////////////////////////////////////////////////
// User-defined functions
public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
    private static final long serialVersionUID = 1L;

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
            throws Exception {
        // normalize and split the line
        String[] tokens = value.toLowerCase().split("\\W+");

        // emit the pairs
        for (String token : tokens) {
            if (token.length() > 0) {
                out.collect(new Tuple2<String, Integer>(token, 1));
            }
        }
    }
}
{% endhighlight %}

The {% gh_link flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/wordcount/WordCount.java  "WordCount example" %} implements the above described algorithm with input parameters: `--input <path> --output <path>`. As test data, any text file will do.

</div>
<div data-lang="scala" markdown="1">

{% highlight scala %}
// set up the execution environment
val env = StreamExecutionEnvironment.getExecutionEnvironment

// read the text file from given input path
val text = env.readTextFile(params.get("input"))

// split up the lines in pairs (2-tuples) containing: (word,1)
val counts: DataStream[(String, Int)] = text
    // split up the lines in pairs (2-tuples) containing: (word,1)
    .flatMap(_.toLowerCase.split("\\W+"))
    .filter(_.nonEmpty)
    .map((_, 1))
    // group by the tuple field "0" and sum up tuple field "1"
    .keyBy(0)
    .sum(1)
    
counts.writeAsText(params.get("output"))

// execute program
env.execute("Streaming WordCount")    
{% endhighlight %}

The {% gh_link flink-examples/flink-examples-streaming/src/main/scala/org/apache/flink/streaming/scala/examples/wordcount/WordCount.scala  "WordCount example" %} implements the above described algorithm with input parameters: `--input <path> --output <path>`. As test data, any text file will do.

</div>
</div>

To run the WordCount example, issue the following command:

{% highlight bash %}
$ ./bin/flink run ./examples/streaming/WordCount.jar
{% endhighlight %}

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-wordcount-run.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-wordcount-run.png" alt="Stream SQL Example: WordCount run"/></a>

Open the web: [http://localhost:8081](http://localhost:8081), and you can see the job was finished quickly.

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-wordcount-web1.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-wordcount-web1.png" alt="SQL Example: WordCount web dashboard"/></a>

Clink the job name: "Streaming WordCount", and you can see the detailed info page:

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-wordcount-web2.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-wordcount-web2.png" alt="SQL Example: WordCount web detail"/></a>

And run the following command to see the result:

{% highlight bash %}
$ tail -f ./log/flink-*-taskexecutor*.out
{% endhighlight %}

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-wordcount-result.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-wordcount-result.png" alt="Stream SQL Example: WordCount result"/></a>

## Socket Window Word Count
SocketWindowWordCount implements a streaming windowed version of the "WordCount" program.

This program connects to a server socket and reads strings from the socket.
The easiest way to try this out is to open a text server (Take port 1234 as example)

The {% gh_link flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/socket/SocketWindowWordCount.java "SocketWindowWordCount java program" %} and {% gh_link flink-examples/flink-examples-streaming/src/main/scala/org/apache/flink/streaming/scala/examples/socket/SocketWindowWordCount.scala "SocketWindowWordCount scala program" %} is the source code.

First, using the <i>netcat</i> tool via

{% highlight bash %}
$ nc -l 1234
{% endhighlight %}
  
If you get an error “Ncat: socket: Address family not supported by protocol QUITTING”, try the following command:

{% highlight bash %}
$ nc -l 0.0.0.0 1234
{% endhighlight %}
  
Then run this example with the hostname and the port as arguments.

{% highlight bash %}
$ ./bin/flink run ./examples/streaming/SocketWindowWordCount.jar --port 1234
{% endhighlight %}

Open the web: [http://localhost:8081](http://localhost:8081)

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-socketwordcount-web-run1.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-socketwordcount-web-run1.png" alt="SQL Example: WordCount web dashboard"/></a>

Clink the job name: "Socket Window WordCount", and you can see the detailed info page:

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-socketwordcount-web-run2.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-socketwordcount-web-run2.png" alt="SQL Example: WordCount web detail"/></a>

Then, you can input data in nc shell terminal:

{% highlight bash %}
$ nc -l 1234
hello flink hello world
{% endhighlight %}

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-socketwordcount-input.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-socketwordcount-input.png" alt="SQL Example: SocketWordCount input"/></a>

And run the following command to see the result:

{% highlight bash %}
$ tail -f ./log/flink-*-taskexecutor*.out
{% endhighlight %}

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-socketwordcount-output.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-socketwordcount-output.png" alt="SQL Example: SocketWordCount output"/></a>

## Top Speed Windowing

TopSpeedWindowing is an example of grouped stream windowing where different eviction and trigger policies can be used. A source fetches events from cars every 100 msec containing their id, their current speed (kmh), overall elapsed distance (m) and a timestamp. The streaming example triggers the top speed of each car every x meters elapsed for the last y seconds.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

DataStream<Tuple4<Integer, Integer, Double, Long>> carData = env.readTextFile(params.get("input")).map(new ParseCarData());

DataStream<Tuple4<Integer, Integer, Double, Long>> topSpeeds = carData
    .assignTimestampsAndWatermarks(new CarTimestamp())
    .keyBy(0)
    .window(GlobalWindows.create())
    .evictor(TimeEvictor.of(Time.of(evictionSec, TimeUnit.SECONDS)))
    .trigger(DeltaTrigger.of(triggerMeters,
            new DeltaFunction<Tuple4<Integer, Integer, Double, Long>>() {
                private static final long serialVersionUID = 1L;
    
                @Override
                public double getDelta(
                        Tuple4<Integer, Integer, Double, Long> oldDataPoint,
                        Tuple4<Integer, Integer, Double, Long> newDataPoint) {
                    return newDataPoint.f2 - oldDataPoint.f2;
                }
            }, carData.getType().createSerializer(env.getConfig())))
    .maxBy(1);

topSpeeds.writeAsText(params.get("output"));

// User-defined functions
private static class ParseCarData extends RichMapFunction<String, Tuple4<Integer, Integer, Double, Long>> {
    private static final long serialVersionUID = 1L;

    @Override
    public Tuple4<Integer, Integer, Double, Long> map(String record) {
        String rawData = record.substring(1, record.length() - 1);
        String[] data = rawData.split(",");
        return new Tuple4<>(Integer.valueOf(data[0]), Integer.valueOf(data[1]), Double.valueOf(data[2]), Long.valueOf(data[3]));
    }
}

private static class CarTimestamp extends AscendingTimestampExtractor<Tuple4<Integer, Integer, Double, Long>> {
    private static final long serialVersionUID = 1L;

    @Override
    public long extractAscendingTimestamp(Tuple4<Integer, Integer, Double, Long> element) {
        return element.f3;
    }
}
	
{% endhighlight %}

The {% gh_link flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/windowing/TopSpeedWindowing.java "TopSpeedWindowing program" %} implements the above example.

</div>
<div data-lang="scala" markdown="1">

{% highlight scala %}
// User-defined types
case class Link(sourceId: Long, targetId: Long)
case class Page(pageId: Long, rank: Double)
case class AdjacencyList(sourceId: Long, targetIds: Array[Long])

// set up execution environment
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

val cars = env.readTextFile(params.get("input"))
  .map(parseMap(_))
  .map(x => CarEvent(x._1, x._2, x._3, x._4))
 
val topSeed = cars
  .assignAscendingTimestamps( _.time )
  .keyBy("carId")
  .window(GlobalWindows.create)
  .evictor(TimeEvictor.of(Time.of(evictionSec * 1000, TimeUnit.MILLISECONDS)))
  .trigger(DeltaTrigger.of(triggerMeters, new DeltaFunction[CarEvent] {
    def getDelta(oldSp: CarEvent, newSp: CarEvent): Double = newSp.distance - oldSp.distance
  }, cars.getType().createSerializer(env.getConfig)))
  .maxBy("speed")

// emit result
topSeed.writeAsText(params.get("output"))

env.execute("TopSpeedWindowing")         

{% endhighlight %}

The {% gh_link flink-examples/flink-examples-streaming/src/main/scala/org/apache/flink/streaming/scala/examples/windowing/TopSpeedWindowing.scala "TopSpeedWindowing program" %} implements the above example.
</div>
</div>

Run the example
{% highlight bash %}
$ ./bin/flink run ./examples/streaming/TopSpeedWindowing.jar
{% endhighlight %}

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-cartopspeedwindow-run.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-cartopspeedwindow-run.png" alt="SQL Example: cartopspeedwindow run"/></a>

Open the web: [http://localhost:8081](http://localhost:8081)

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-cartopspeedwindow-web1.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-cartopspeedwindow-web1.png" alt="SQL Example: cartopspeedwindow web dashboard"/></a>

Clink the job name: "CarTopSpeedWindowingExample", and you can see the detailed info page:

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-cartopspeedwindow-web2.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-cartopspeedwindow-web2.png" alt="SQL Example: cartopspeedwindow web detail"/></a>

And run the following command to see the continuous updated result:

{% highlight bash %}
$ tail -f ./log/flink-*-taskexecutor*.out
{% endhighlight %}

<a href="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-cartopspeedwindow-result.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/quickstart-stream-example-cartopspeedwindow-result.png" alt="SQL Example: cartopspeedwindow result"/></a>

{% top %}
