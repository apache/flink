---
title: "DataStream API 教程"
nav-title: DataStream API
nav-parent_id: apitutorials
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

在本节指南中，我们将从零开始创建一个在 flink 集群上面进行流分析的 Flink 项目。

维基百科提供了一个能够记录所有对 wiki 编辑的 IRC 通道。我们将使用 Flink 读取该通道的数据，同时
在给定的时间窗口，计算出每个用户在其中编辑的字节数。这使用 Flink 很容易就能实现，但它会为你提供一个良好的基础去开始构建你自己更为复杂的分析程序。
## 创建一个 Maven 项目

我们准备使用 Flink Maven Archetype 创建项目结构。更多细节请查看 [Java API 快速指南]({{ site.baseurl }}/zh/dev/projectsetup/java_api_quickstart.html)。项目运行命令如下：

{% highlight bash %}
$ mvn archetype:generate \
    -DarchetypeGroupId=org.apache.flink \
    -DarchetypeArtifactId=flink-quickstart-java \{% unless site.is_stable %}
    -DarchetypeCatalog=https://repository.apache.org/content/repositories/snapshots/ \{% endunless %}
    -DarchetypeVersion={{ site.version }} \
    -DgroupId=wiki-edits \
    -DartifactId=wiki-edits \
    -Dversion=0.1 \
    -Dpackage=wikiedits \
    -DinteractiveMode=false
{% endhighlight %}

{% unless site.is_stable %}
<p style="border-radius: 5px; padding: 5px" class="bg-danger">
    <b>注意</b>: 对于 Maven 3.0 或着更高的版本，不再能够通过命令行(-DarchetypeCatalog)指定仓库。如果你希望使用仓库快照，你需要在 settings.xml 指定仓库。有关此更改的详细信息，请参考<a href=" http://maven.apache.org/prototype/maven-prototype-plugin/prototype-repository.html ">Maven官方文档</a>
</p>
{% endunless %}

你可以按需修改 `groupId`、`artifactId` 以及 `package`。对于上面的参数，Maven 创建项目结构如下： 

{% highlight bash %}
$ tree wiki-edits
wiki-edits/
├── pom.xml
└── src
    └── main
        ├── java
        │   └── wikiedits
        │       ├── BatchJob.java
        │       └── StreamingJob.java
        └── resources
            └── log4j.properties
{% endhighlight %}

项目根目录下的 `pom.xml` 文件已经将 Flink 依赖添加进来，同时在 `src/main/java` 目录下也有几个 Flink 实例程序。由于我们从头开始创建，我们可以删除原来实例程序相关的类：

{% highlight bash %}
$ rm wiki-edits/src/main/java/wikiedits/*.java
{% endhighlight %}

作为最后一步，我们需要添加 Flink 维基百科连接器的依赖，从而可以在项目中进行使用。修改 `pom.xml` 的 `dependencies` 部分，使它看起来像这样:

{% highlight xml %}
<dependencies>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-java</artifactId>
        <version>${flink.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-java_2.11</artifactId>
        <version>${flink.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-clients_2.11</artifactId>
        <version>${flink.version}</version>
    </dependency>
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-wikiedits_2.11</artifactId>
        <version>${flink.version}</version>
    </dependency>
</dependencies>
{% endhighlight %}

注意加入 `flink-connector-wikiedits_2.11` 依赖。(这个例子和维基百科连接器的灵感来自于 Apache Samza *Hello Samza* 示例。)
## 编写 Flink 程序

现在是编程时间。启动你最喜欢的 IDE 并导入 Maven 项目或打开文本编辑器，然后创建文件  `src/main/java/wikiedits/WikipediaAnalysis.java`:

{% highlight java %}
package wikiedits;

public class WikipediaAnalysis {

    public static void main(String[] args) throws Exception {

    }
}
{% endhighlight %}

这个程序现在很基础，但我们会边做边进行完善。注意我不会给出导入语句，因为 IDE 会自动添加它们。在本节的最后，我将展示带有导入语句的完整代码
如果需要你可以将他们复制到你的编辑器中。

在一个 Flink 程序中，首先你需要创建一个 `StreamExecutionEnvironment` (或者处理批作业环境的 `ExecutionEnvironment`)。这可以用来设置程序运行参数，同时也能够创建从外部系统读取的数据源。我们把这个添加到 main 方法中:

{% highlight java %}
StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
{% endhighlight %}

接下来我们将创建一个读取维基百科 IRC 数据的源：

{% highlight java %}
DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());
{% endhighlight %}

上面代码创建了一个 `WikipediaEditEvent` 事件的 `DataStream`，我们可以进一步处理它。这个代码实例的目的是为了确定每个用户在特定时间窗口中添加或删除的字节数，比如 5 秒一个时间窗口。首先
我们必须根据用户名来划分我们的数据流，也就是说这个流上的操作应该考虑用户名。
在我们这个统计窗口编辑的字节数的例子中，每个不同的用户每个窗口都应该计算一个结果。对于划分一个数据流，我们必须提供一个 `KeySelector`，像这样:

{% highlight java %}
KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
    .keyBy(new KeySelector<WikipediaEditEvent, String>() {
        @Override
        public String getKey(WikipediaEditEvent event) {
            return event.getUser();
        }
    });
{% endhighlight %}

上述创建了一个以 `String` 类型为关键字的 `WikipediaEditEvent` 数据流，即用户名。
现在，我们可以指定将窗口应用到该数据流，并基于这些窗口中的元素计算结果。窗口需要指定流的一个数据片段用来进行计算。当计算无限元素流上的聚合时，需要使用Windows。在我们的例子中，我们想要每5秒聚合一次编辑的字节数:

{% highlight java %}
DataStream<Tuple2<String, Long>> result = keyedEdits
    .timeWindow(Time.seconds(5))
    .aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Long>, Tuple2<String, Long>>() {
        @Override
        public Tuple2<String, Long> createAccumulator() {
            return new Tuple2<>("", 0L);
        }

        @Override
        public Tuple2<String, Long> add(WikipediaEditEvent value, Tuple2<String, Long> accumulator) {
            accumulator.f0 = value.getUser();
            accumulator.f1 += value.getByteDiff();
            return accumulator;
        }

        @Override
        public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
            return accumulator;
        }

        @Override
        public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
            return new Tuple2<>(a.f0, a.f1 + b.f1);
        }
    });
{% endhighlight %}

首先调用 `.timeWindow()` 方法指定五秒翻滚(非重叠)窗口。第二个调用方法对于每一个唯一关键字指定每个窗口片`聚合转换`。
在本例中，我们从 `(""，0L)` 初始值开始，并将每个用户编辑的字节添加到该时间窗口中。对于每个用户来说，结果流现在包含的元素为 `Tuple2<String, Long>`，它每5秒发出一次。

唯一剩下的就是将结果输出到控制台并开始执行:

{% highlight java %}
result.print();

see.execute();
{% endhighlight %}

最后一步调用方法对于启动 Flink 作业是必需的。所有算子操作，比如创建数据源、转换以及数据输出都会被构建成一个图。只有当 `execute()` 被调用后，算子计算图才会提交到集群或者在你本地的机器上运行。

完整代码如下：

{% highlight java %}
package wikiedits;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;

public class WikipediaAnalysis {

  public static void main(String[] args) throws Exception {

    StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();

    DataStream<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

    KeyedStream<WikipediaEditEvent, String> keyedEdits = edits
      .keyBy(new KeySelector<WikipediaEditEvent, String>() {
        @Override
        public String getKey(WikipediaEditEvent event) {
          return event.getUser();
        }
      });

    DataStream<Tuple2<String, Long>> result = keyedEdits
      .timeWindow(Time.seconds(5))
      .aggregate(new AggregateFunction<WikipediaEditEvent, Tuple2<String, Long>, Tuple2<String, Long>>() {
        @Override
      	public Tuple2<String, Long> createAccumulator() {
      	  return new Tuple2<>("", 0L);
      	}

      	@Override
      	public Tuple2<String, Long> add(WikipediaEditEvent value, Tuple2<String, Long> accumulator) {
      	  accumulator.f0 = value.getUser();
      	  accumulator.f1 += value.getByteDiff();
          return accumulator;
      	}

      	@Override
      	public Tuple2<String, Long> getResult(Tuple2<String, Long> accumulator) {
      	  return accumulator;
      	}

      	@Override
      	public Tuple2<String, Long> merge(Tuple2<String, Long> a, Tuple2<String, Long> b) {
      	  return new Tuple2<>(a.f0, a.f1 + b.f1);
      	}
      });

    result.print();

    see.execute();
  }
}
{% endhighlight %}

你可以在你的IDE中或者命令行中，使用Maven命令运行这个代码实例：

{% highlight bash %}
$ mvn clean package
$ mvn exec:java -Dexec.mainClass=wikiedits.WikipediaAnalysis
{% endhighlight %}

第一个命令会构建我们的项目，第二个命令开始执行我们的程序主类。结果输出如下:

{% highlight bash %}
1> (Fenix down,114)
6> (AnomieBOT,155)
8> (BD2412bot,-3690)
7> (IgnorantArmies,49)
3> (Ckh3111,69)
5> (Slade360,0)
7> (Narutolovehinata5,2195)
6> (Vuyisa2001,79)
4> (Ms Sarah Welch,269)
4> (KasparBot,-245)
{% endhighlight %}

每行数据前面的数字代表着打印接收器运行的并行实例。

这可以让你开始创建你自己的 Flink 项目。你可以查看[基本概念]({{ site.baseurl }}/zh/dev/api_concepts.html)和[DataStream API]({{ site.baseurl }}/zh/dev/datastream_api.html)指南。如果你想学习了解更多关于 Flink 集群安装以及数据写入到 [Kafka](http://kafka.apache.org),你可以自己多加以练习尝试。

## 额外练习: 集群运行并输出 Kafka

请按照我们的[本地安装教程](local_setup.html)在你的机器上构建一个Flink分布式环境，同时参考[Kafka快速指南](https://kafka.apache.org/0110/documentation.html#quickstart)安装一个我们需要使用的Kafka环境。

首先，我们必须添加 Flink Kafka 连接器依赖，这样我们才能使用 Kafka 输出。在 `pom.xml` 依赖部分进行添加：

{% highlight xml %}
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka-0.11_2.11</artifactId>
    <version>${flink.version}</version>
</dependency>
{% endhighlight %}

接下来，我们需要修改我们的程序。我们将删除 `print()` 输出，使用 Kafka 输出进行代替。新的代码如下所示：

{% highlight java %}

result
    .map(new MapFunction<Tuple2<String,Long>, String>() {
        @Override
        public String map(Tuple2<String, Long> tuple) {
            return tuple.toString();
        }
    })
    .addSink(new FlinkKafkaProducer011<>("localhost:9092", "wiki-result", new SimpleStringSchema()));
{% endhighlight %}

相关类也需要导入:
{% highlight java %}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.functions.MapFunction;
{% endhighlight %}

注意我们是如何使用 MapFunction 将 `Tuple2<String,Long>` 流转换为 `字符串` 流。这样做的原因是因为将普通字符串写入到 Kafka 更容易。然后，我们会创建一个 Kafka 输出。你可能需要根据你的设置去调整主机名和端口。`wiki-result`
是我们接下来在运行程序之前创建的 Kafka 流的主题。由于我们需要在集群上运行 jar 文件，这里使用 Maven 构建项目:

{% highlight bash %}
$ mvn clean package
{% endhighlight %}

上述命令将会生成一个在`target`目录中的 jar 文件，具体文件如下：`target/wiki-edits-0.1.jar`，之后我们将使用这个文件。

现在我们准备启动一个 Flink 集群，然后运行写入到 Kafka 的实时任务。进入到你本地安装 Flink 的位置，然后启动一个本地集群：

{% highlight bash %}
$ cd my/flink/directory
$ bin/start-cluster.sh
{% endhighlight %}

我们必须先创建一个 Kafka 主题，这样我们的程序才能往里写入数据：

{% highlight bash %}
$ cd my/kafka/directory
$ bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic wiki-results
{% endhighlight %}

现在我们准备在本地 Flink 集群运行我们的 jar 文件：
{% highlight bash %}
$ cd my/flink/directory
$ bin/flink run -c wikiedits.WikipediaAnalysis path/to/wikiedits-0.1.jar
{% endhighlight %}

如果一切按照计划运行，那个命令的输出应该如下所示：

{% highlight plain %}
03/08/2016 15:09:27 Job execution switched to status RUNNING.
03/08/2016 15:09:27 Source: Custom Source(1/1) switched to SCHEDULED
03/08/2016 15:09:27 Source: Custom Source(1/1) switched to DEPLOYING
03/08/2016 15:09:27 Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, AggregateFunction$3, PassThroughWindowFunction) -> Sink: Print to Std. Out (1/1) switched from CREATED to SCHEDULED
03/08/2016 15:09:27 Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, AggregateFunction$3, PassThroughWindowFunction) -> Sink: Print to Std. Out (1/1) switched from SCHEDULED to DEPLOYING
03/08/2016 15:09:27 Window(TumblingProcessingTimeWindows(5000), ProcessingTimeTrigger, AggregateFunction$3, PassThroughWindowFunction) -> Sink: Print to Std. Out (1/1) switched from DEPLOYING to RUNNING
03/08/2016 15:09:27 Source: Custom Source(1/1) switched to RUNNING
{% endhighlight %}

你可以看到每个算子是如何运行的。这里只有两个，出于性能原因的考虑，窗口后的操作会被链接成一个操作。在Flink中我们称它为 *链接*。

你可以通过 Kafka 控制台来检查项目输出到 Kafka 主题的情况

{% highlight bash %}
bin/kafka-console-consumer.sh  --zookeeper localhost:2181 --topic wiki-result
{% endhighlight %}

你还可以查看运行在 [http://localhost:8081](http://localhost:8081) 上的 Flink 作业仪表盘。你可以概览集群资源以及正在运行的作业：

<a href="{{ site.baseurl }}/page/img/quickstart-example/jobmanager-overview.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/jobmanager-overview.png" alt="JobManager 概览"/></a>

如果你点击正在运行的作业，你将会得到一个可以检查各个操作的视图，比如说，你可以查看处理过的元素数量:

<a href="{{ site.baseurl }}/page/img/quickstart-example/jobmanager-job.png" ><img class="img-responsive" src="{{ site.baseurl }}/page/img/quickstart-example/jobmanager-job.png" alt="样例作业视图"/></a>

这就结束了 Flink 项目构建之旅. 如果你有任何问题, 你可以在我们的 [邮件组](http://flink.apache.org/community.html#mailing-lists)提出.

{% top %}
