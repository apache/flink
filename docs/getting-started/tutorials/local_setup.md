---
title: "Local Setup Tutorial"
nav-title: 'Local Setup'
nav-parent_id: setuptutorials
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

* This will be replaced by the TOC
{:toc}

Get a Flink example program up and running in a few simple steps.

## Setup: Download and Start Flink

Flink runs on __Linux, Mac OS X, and Windows__. To be able to run Flink, the only requirement is to have a working __Java 8.x__ installation. Windows users, please take a look at the [Flink on Windows]({{ site.baseurl }}/getting-started/tutorials/flink_on_windows.html) guide which describes how to run Flink on Windows for local setups.

You can check the correct installation of Java by issuing the following command:

{% highlight bash %}
java -version
{% endhighlight %}

If you have Java 8, the output will look something like this:

{% highlight bash %}
java version "1.8.0_111"
Java(TM) SE Runtime Environment (build 1.8.0_111-b14)
Java HotSpot(TM) 64-Bit Server VM (build 25.111-b14, mixed mode)
{% endhighlight %}

{% if site.is_stable %}
<div class="codetabs" markdown="1">

<div data-lang="Download and Unpack" markdown="1">
1. Download a binary from the [downloads page](http://flink.apache.org/downloads.html). You can pick
   any Scala variant you like. For certain features you may also have to download one of the pre-bundled Hadoop jars
   and place them into the `/lib` directory.
2. Go to the download directory.
3. Unpack the downloaded archive.

{% highlight bash %}
$ cd ~/Downloads        # Go to download directory
$ tar xzf flink-*.tgz   # Unpack the downloaded archive
$ cd flink-{{site.version}}
{% endhighlight %}
</div>

<div data-lang="MacOS X" markdown="1">
For MacOS X users, Flink can be installed through [Homebrew](https://brew.sh/).

{% highlight bash %}
$ brew install apache-flink
...
$ flink --version
Version: 1.2.0, Commit ID: 1c659cf
{% endhighlight %}
</div>

</div>

{% else %}
### Download and Compile
Clone the source code from one of our [repositories](http://flink.apache.org/community.html#source-code), e.g.:

{% highlight bash %}
$ git clone https://github.com/apache/flink.git
$ cd flink
$ mvn clean package -DskipTests # this will take up to 10 minutes
$ cd build-target               # this is where Flink is installed to
{% endhighlight %}
{% endif %}

### Start a Local Flink Cluster

{% highlight bash %}
$ ./bin/start-cluster.sh  # Start Flink
{% endhighlight %}

Check the __Dispatcher's web frontend__ at [http://localhost:8081](http://localhost:8081) and make sure everything is up and running. The web frontend should report a single available TaskManager instance.

<a href="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-1.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-1.png" alt="Dispatcher: Overview"/></a>

You can also verify that the system is running by checking the log files in the `logs` directory:

{% highlight bash %}
$ tail log/flink-*-standalonesession-*.log
INFO ... - Rest endpoint listening at localhost:8081
INFO ... - http://localhost:8081 was granted leadership ...
INFO ... - Web frontend listening at http://localhost:8081.
INFO ... - Starting RPC endpoint for StandaloneResourceManager at akka://flink/user/resourcemanager .
INFO ... - Starting RPC endpoint for StandaloneDispatcher at akka://flink/user/dispatcher .
INFO ... - ResourceManager akka.tcp://flink@localhost:6123/user/resourcemanager was granted leadership ...
INFO ... - Starting the SlotManager.
INFO ... - Dispatcher akka.tcp://flink@localhost:6123/user/dispatcher was granted leadership ...
INFO ... - Recovering all persisted jobs.
INFO ... - Registering TaskManager ... at ResourceManager
{% endhighlight %}

## Read the Code

You can find the complete source code for this SocketWindowWordCount example in [scala](https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/scala/org/apache/flink/streaming/scala/examples/socket/SocketWindowWordCount.scala) and [java](https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/socket/SocketWindowWordCount.java) on GitHub.

<div class="codetabs" markdown="1">
<div data-lang="scala" markdown="1">
{% highlight scala %}
object SocketWindowWordCount {

    def main(args: Array[String]) : Unit = {

        // the port to connect to
        val port: Int = try {
            ParameterTool.fromArgs(args).getInt("port")
        } catch {
            case e: Exception => {
                System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'")
                return
            }
        }

        // get the execution environment
        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

        // get input data by connecting to the socket
        val text = env.socketTextStream("localhost", port, '\n')

        // parse the data, group it, window it, and aggregate the counts
        val windowCounts = text
            .flatMap { w => w.split("\\s") }
            .map { w => WordWithCount(w, 1) }
            .keyBy("word")
            .timeWindow(Time.seconds(5), Time.seconds(1))
            .sum("count")

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1)

        env.execute("Socket Window WordCount")
    }

    // Data type for words with count
    case class WordWithCount(word: String, count: Long)
}
{% endhighlight %}
</div>
<div data-lang="java" markdown="1">
{% highlight java %}
public class SocketWindowWordCount {

    public static void main(String[] args) throws Exception {

        // the port to connect to
        final int port;
        try {
            final ParameterTool params = ParameterTool.fromArgs(args);
            port = params.getInt("port");
        } catch (Exception e) {
            System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
            return;
        }

        // get the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream("localhost", port, "\n");

        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts = text
            .flatMap(new FlatMapFunction<String, WordWithCount>() {
                @Override
                public void flatMap(String value, Collector<WordWithCount> out) {
                    for (String word : value.split("\\s")) {
                        out.collect(new WordWithCount(word, 1L));
                    }
                }
            })
            .keyBy("word")
            .timeWindow(Time.seconds(5), Time.seconds(1))
            .reduce(new ReduceFunction<WordWithCount>() {
                @Override
                public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                    return new WordWithCount(a.word, a.count + b.count);
                }
            });

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }

    // Data type for words with count
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
{% endhighlight %}
</div>
</div>

## Run the Example

Now, we are going to run this Flink application. It will read text from
a socket and once every 5 seconds print the number of occurrences of
each distinct word during the previous 5 seconds, i.e. a tumbling
window of processing time, as long as words are floating in.

* First of all, we use **netcat** to start local server via

{% highlight bash %}
$ nc -l 9000
{% endhighlight %}

* Submit the Flink program:

{% highlight bash %}
$ ./bin/flink run examples/streaming/SocketWindowWordCount.jar --port 9000
Starting execution of program

{% endhighlight %}

  The program connects to the socket and waits for input. You can check the web interface to verify that the job is running as expected:

  <div class="row">
    <div class="col-sm-6">
      <a href="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-2.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-2.png" alt="Dispatcher: Overview (cont'd)"/></a>
    </div>
    <div class="col-sm-6">
      <a href="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-3.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-3.png" alt="Dispatcher: Running Jobs"/></a>
    </div>
  </div>

* Words are counted in time windows of 5 seconds (processing time, tumbling
  windows) and are printed to `stdout`. Monitor the TaskManager's output file
  and write some text in `nc` (input is sent to Flink line by line after
  hitting <RETURN>):

{% highlight bash %}
$ nc -l 9000
lorem ipsum
ipsum ipsum ipsum
bye
{% endhighlight %}

  The `.out` file will print the counts at the end of each time window as long
  as words are floating in, e.g.:

{% highlight bash %}
$ tail -f log/flink-*-taskexecutor-*.out
lorem : 1
bye : 1
ipsum : 4
{% endhighlight %}

  To **stop** Flink when you're done type:

{% highlight bash %}
$ ./bin/stop-cluster.sh
{% endhighlight %}

## Next Steps

Check out some more [examples]({{ site.baseurl }}/getting-started/examples) to get a better feel for Flink's programming APIs. When you are done with that, go ahead and read the [streaming guide]({{ site.baseurl }}/dev/datastream_api.html).

{% top %}
