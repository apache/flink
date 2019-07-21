---
title: "Apache Cassandra Connector"
nav-title: Cassandra
nav-parent_id: connectors
nav-pos: 2
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


此连接器提供将数据写入 [Apache Cassandra](https://cassandra.apache.org/) 数据库的 sinks（接收器）。

<!--
  TODO：也许值得一提的是当前的DataStax Java 驱动程序版本与用户端的 Cassandra 版本相匹配。
-->

要使用此连接器，请将以下依赖项添加到项目中：

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-cassandra{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

请注意，流连接器当前不是 Flink 二进制发布包的一部分。了解如何链接它们以进行集群执行[此处]({{site.baseurl}}/zh/dev/project-configuration.html)。

## 安装 Apache Cassandra
有多种方法可以在本地计算机上启动Cassandra实例：

1. 按照[Cassandra入门页面](http://cassandra.apache.org/doc/latest/getting_started/index.html)中的说明进行操作。
2. 从[Official Docker Repository](https://hub.docker.com/_/cassandra/)启动运行 Cassandra 的容器

## Cassandra Sinks

### 配置

Flink的 Cassandra 接收器是使用静态 `CassandraSink.addSink(DataStream<IN> input)` 方法创建的。这个方法返回一个 `CassandraSinkBuilder`，它提供了进一步配置接收器的方法，最后通过 `build（）` 创建接收器实例。

可以使用以下配置方法：

1. _setQuery(String query)_
    * 设置为接收器接收的每个记录执行的 upsert 查询。
    * 查询在内部被视为 CQL 语句。
    * __DO__ 设置 upsert 查询以处理 __Tuple__ 数据类型。
    * __DO NOT__ 设置查询以处理 __POJO__ 数据类型。
2. _setClusterBuilder()_
    * 将用于配置创建更复杂的 cassandra cluster builder，例如一致性级别，重试策略等。
3. _setHost(String host[, int port])_
    * 简单版本的 setClusterBuilder()，包含连接到 Cassandra 实例的主机/端口信息
4. _setMapperOptions(MapperOptions options)_
    * 设置用于配置 DataStax ObjectMapper 的映射器选项。
    * 仅在处理 __POJO__ 数据类型时适用。
5. _setMaxConcurrentRequests(int maxConcurrentRequests, Duration timeout)_
    * 设置允许执行许可的超时的最大并发请求数。
    * 仅在未配置 __enableWriteAheadLog()__ 时适用。
6. _enableWriteAheadLog([CheckpointCommitter committer])_
    * __optional__ 设置
    * 允许对非确定性算法进行精确一次处理。
7. _setFailureHandler([CassandraFailureHandler failureHandler])_
    * __optional__ 设置。
    * 设置自定义失败处理程序。
8. _build()_
    * 完成配置并构造 CassandraSink 实例。

### 预写日志

一个 checkpoint 提交者存储有关已完成 checkpoint 的附加信息在某些资源中。此信息用于防止在发生故障时从最后一次完整保存的 checkpoint 中重播恢复数据  。
您可以使用 `CassandraCommitter` 将它们存储在 cassandra 的单独表中。请注意，Flink 不会清理此表。

如果查询是幂等的，Flink 启用了 checkpoint 情况下可以提供精确一次保证（意味着它可以应用多个时间而不更改结果）。如果失败则会从已完整保存的 checkpoint 中重播恢复数据。

此外，对于非确定性程序，必须启用预写日志。对于这样的计划重播的 checkpoint 可能与之前的尝试完全不同，后者可能会离开数据库处于不一致状态，因为可能已经编写了第一次尝试的部分内容。预写日志保证重放的 checkpoint 与第一次尝试相同。请注意，启用此功能会对延迟产生负面影响。

<p style="border-radius: 5px; padding: 5px" class="bg-danger"><b>注意</b>：预写日志功能目前是实验性的。在许多情况下，使用连接器而不启用它就足够了。请将问题报告给开发邮件列表。</p>

### Checkpointing and 容错
启用 Checkpointing 后，Cassandra Sink 保证至少一次向 C* 实例传递操作请求。

更多有关[checkpoints docs]({{site.baseurl}}/zh/dev/stream/state/checkpointing.html)和[容错保证文档]({{site.baseurl}}/zh/dev/connectors/guarantees.html)的详细信息。

## 例子

Cassandra 接收器当前支持 Tuple 和 POJO 数据类型，Flink 自动检测使用哪种类型的输入。有关那些流数据类型的一般用例，请参阅[支持的数据类型]({{ site.baseurl }}/dev/api_concepts.html)。我们展示了两个基于 [SocketWindowWordCount](https://github.com/apache/flink/blob/master/flink-examples/flink-examples-streaming/src/main/java/org/apache/flink/streaming/examples/socket/SocketWindowWordCount.java) 的实现，分别用于 Pojo 和 Tuple 数据类型。

在所有这些例子中，我们都是假设已经创建了相关的 Keyspace `example`和 Table `wordcount`。

<div class="codetabs" markdown="1">
<div data-lang="CQL" markdown="1">
{% highlight sql %}
CREATE KEYSPACE IF NOT EXISTS example
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
CREATE TABLE IF NOT EXISTS example.wordcount (
    word text,
    count bigint,
    PRIMARY KEY(word)
    );
{% endhighlight %}
</div>
</div>

### Cassandra Sink Example for Streaming Tuple Data Type
在将结果通过 Java/Scala Tuple 数据类型存储到 Cassandra 接收器时，需要设置CQL upsert 语句（通过 setQuery('stmt')）将每条记录保存回数据库。将 upsert 查询缓存为 `PreparedStatement` 时，每个 Tuple 元素都将转换为语句的参数。

有关 `PreparedStatement` 和 `BoundStatement` 的详细信息，请访问[DataStax Java 驱动程序手册](https://docs.datastax.com/en/developer/java-driver/2.1/manual/statements/prepared/)。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get the execution environment
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// get input data by connecting to the socket
DataStream<String> text = env.socketTextStream(hostname, port, "\n");

// parse the data, group it, window it, and aggregate the counts
DataStream<Tuple2<String, Long>> result = text
        .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Long>> out) {
                // normalize and split the line
                String[] words = value.toLowerCase().split("\\s");

                // emit the pairs
                for (String word : words) {
                    //Do not accept empty word, since word is defined as primary key in C* table
                    if (!word.isEmpty()) {
                        out.collect(new Tuple2<String, Long>(word, 1L));
                    }
                }
            }
        })
        .keyBy(0)
        .timeWindow(Time.seconds(5))
        .sum(1);

CassandraSink.addSink(result)
        .setQuery("INSERT INTO example.wordcount(word, count) values (?, ?);")
        .setHost("127.0.0.1")
        .build();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

// get input data by connecting to the socket
val text: DataStream[String] = env.socketTextStream(hostname, port, '\n')

// parse the data, group it, window it, and aggregate the counts
val result: DataStream[(String, Long)] = text
  // split up the lines in pairs (2-tuples) containing: (word,1)
  .flatMap(_.toLowerCase.split("\\s"))
  .filter(_.nonEmpty)
  .map((_, 1L))
  // group by the tuple field "0" and sum up tuple field "1"
  .keyBy(0)
  .timeWindow(Time.seconds(5))
  .sum(1)

CassandraSink.addSink(result)
  .setQuery("INSERT INTO example.wordcount(word, count) values (?, ?);")
  .setHost("127.0.0.1")
  .build()

result.print().setParallelism(1)
{% endhighlight %}
</div>

</div>


### 用于流式传输 POJO 数据类型的 Cassandra Sink 示例
流式传输 POJO 数据类型并将相同的 POJO 实体存储回 Cassandra 的示例。此外，此 POJO 实现需要遵循[DataStax Java 驱动程序手册](http://docs.datastax.com/en/developer/java-driver/2.1/manual/object_mapper/creating/)来注释每个字段的类使用 DataStax Java 驱动程序 `com.datastax.driver.mapping.Mapper` 类将此实体映射到指定表的关联列。

可以通过放置在 Pojo 类中的字段声明上的注释来定义每个表列的映射。有关映射的详细信息，请参阅[映射类的定义](http://docs.datastax.com/en/developer/java-driver/3.1/manual/object_mapper/creating/)和[CQL Data types](https://docs.datastax.com/en/cql/3.1/cql/cql_reference/cql_data_types_c.html)。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// get the execution environment
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// get input data by connecting to the socket
DataStream<String> text = env.socketTextStream(hostname, port, "\n");

// parse the data, group it, window it, and aggregate the counts
DataStream<WordCount> result = text
        .flatMap(new FlatMapFunction<String, WordCount>() {
            public void flatMap(String value, Collector<WordCount> out) {
                // normalize and split the line
                String[] words = value.toLowerCase().split("\\s");

                // emit the pairs
                for (String word : words) {
                    if (!word.isEmpty()) {
                        //Do not accept empty word, since word is defined as primary key in C* table
                        out.collect(new WordCount(word, 1L));
                    }
                }
            }
        })
        .keyBy("word")
        .timeWindow(Time.seconds(5))

        .reduce(new ReduceFunction<WordCount>() {
            @Override
            public WordCount reduce(WordCount a, WordCount b) {
                return new WordCount(a.getWord(), a.getCount() + b.getCount());
            }
        });

CassandraSink.addSink(result)
        .setHost("127.0.0.1")
        .setMapperOptions(() -> new Mapper.Option[]{Mapper.Option.saveNullFields(true)})
        .build();


@Table(keyspace = "example", name = "wordcount")
public class WordCount {

    @Column(name = "word")
    private String word = "";

    @Column(name = "count")
    private long count = 0;

    public WordCount() {}

    public WordCount(String word, long count) {
        this.setWord(word);
        this.setCount(count);
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return getWord() + " : " + getCount();
    }
}
{% endhighlight %}
</div>

</div>

{% top %}
