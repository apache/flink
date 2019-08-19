---
title: "本地安装教程"
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

只需要几个简单的步骤即可启动并运行 Flink 示例程序。

## 安装：下载并启动 Flink 

Flink 可以在 __Linux、Mac OS X 和 Windows__ 环境中运行。运行 Flink 的唯一要求是安装 __Java 8.x__ 。 Windows 用户请查阅[在 Windows 上运行 Flink]({{ site.baseurl }}/tutorials/flink_on_windows.html) 上面描述了如何在 windows 上以本地模式运行 Flink。

你可以用下面的命令来检查一下是否正确安装了 Java 程序：

{% highlight bash %}
java -version
{% endhighlight %}

如果你已经安装了 Java 8，应该输出如下内容：

{% highlight bash %}
java version "1.8.0_111"
Java(TM) SE Runtime Environment (build 1.8.0_111-b14)
Java HotSpot(TM) 64-Bit Server VM (build 25.111-b14, mixed mode)
{% endhighlight %}

{% if site.is_stable %}
<div class="codetabs" markdown="1">
<div data-lang="Download and Unpack" markdown="1">
1. 从[下载页](http://flink.apache.org/downloads.html)下载二进制文件。你可以选择任何喜欢的 Scala 版本。针对某些特性，你可能还需要下载预捆绑的 Hadoop jar 包并将它们放入 `/lib` 目录。
2. 进入下载后的目录。
3. 解压下载的文件。


{% highlight bash %}
$ cd ~/Downloads        # Go to download directory
$ tar xzf flink-*.tgz   # Unpack the downloaded archive
$ cd flink-{{site.version}}
{% endhighlight %}
</div>

<div data-lang="MacOS X" markdown="1">
对于 MacOS X 用户，Flink 可以通过 [Homebrew](https://brew.sh/) 进行安装。

{% highlight bash %}
$ brew install apache-flink
...
$ flink --version
Version: 1.2.0, Commit ID: 1c659cf
{% endhighlight %}
</div>

</div>

{% else %}
### 下载和编译
从我们的[代码仓库](http://flink.apache.org/community.html#source-code)中克隆源码，比如：

{% highlight bash %}
$ git clone https://github.com/apache/flink.git
$ cd flink
$ mvn clean package -DskipTests # this will take up to 10 minutes
$ cd build-target               # this is where Flink is installed to
{% endhighlight %}
{% endif %}

### 启动 Flink 本地集群

{% highlight bash %}
$ ./bin/start-cluster.sh  # Start Flink
{% endhighlight %}

检查位于 [http://localhost:8081](http://localhost:8081) 的 web 调度界面以确保一切正常运行。Web 界面上会仅显示一个可用的 TaskManager 实例。

<a href="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-1.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-1.png" alt="Dispatcher: Overview"/></a>

还可以通过检查 `logs` 目录中的日志文件来验证系统是否正在运行：

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
INFO ... - Registering TaskManager ... under ... at the SlotManager.
{% endhighlight %}

## 阅读代码

你可以在 Github 上看到分别用 [scala](https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/scala/org/apache/flink/streaming/scala/examples/socket/SocketWindowWordCount.scala) 和 [java](https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/socket/SocketWindowWordCount.java) 编写的 SocketWindowWordCount 的完整代码。

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

## 运行样例

现在，我们将运行 Flink 程序。只要有单词输入，它就从一个套接字中读取文本，每 5 秒打印一次前 5 秒内每个不同单词的出现次数。这里的 5 秒钟就是处理时间的翻滚窗口。

* 首先，我们用 **netcat** 来启动一个本地的服务：

{% highlight bash %}
$ nc -l 9000
{% endhighlight %}

* 提交 Flink 程序：

{% highlight bash %}
$ ./bin/flink run examples/streaming/SocketWindowWordCount.jar --port 9000
Starting execution of program

{% endhighlight %}

 程序连接到套接字并等待输入。你可以通过 web 界面来验证作业是否如期运行：

  <div class="row">
    <div class="col-sm-6">
      <a href="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-2.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-2.png" alt="Dispatcher: Overview (cont'd)"/></a>
    </div>
    <div class="col-sm-6">
      <a href="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-3.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-setup/jobmanager-3.png" alt="Dispatcher: Running Jobs"/></a>
    </div>
  </div>

* 单词以 5 秒为时间窗口（处理时间，翻滚窗口）进行计数，并打印到 `stdout`。监视 TaskManager 的输出文件并在 `nc` 中输入一些文本（输入的内容被逐行发送到 Flink）：

{% highlight bash %}
$ nc -l 9000
lorem ipsum
ipsum ipsum ipsum
bye
{% endhighlight %}

随着单词的输入，`.out` 文件会在每个时间窗口的末尾打印计数，比如：

{% highlight bash %}
$ tail -f log/flink-*-taskexecutor-*.out
lorem : 1
bye : 1
ipsum : 4
{% endhighlight %}

完成输入后停止 Flink：

{% highlight bash %}
$ ./bin/stop-cluster.sh
{% endhighlight %}

## 下一步

查看更多的[示例]({{ site.baseurl }}/examples)以便更好地理解 Flink 的 API。完成后, 请继续阅读[流处理指南]({{ site.baseurl }}/zh/dev/datastream_api.html)。

{% top %}
