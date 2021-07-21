---
title: Elasticsearch
weight: 5
type: docs
aliases:
  - /zh/dev/connectors/elasticsearch.html
  - /zh/apis/streaming/connectors/elasticsearch.html
  - /zh/dev/connectors/elasticsearch2.html
  - /zh/apis/streaming/connectors/elasticsearch2.html
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

# Elasticsearch 连接器

此连接器提供可以向 [Elasticsearch](https://elastic.co/) 索引请求文档操作的接收器。
要使用此连接器，请根据 Elasticsearch 的安装版本将以下依赖之一添加到你的项目中：

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left">Elasticsearch 版本</th>
      <th class="text-left">Maven 依赖</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>5.x</td>
        <td>{{< artifact flink-connector-elasticsearch5 withScalaVersion >}}</td>
    </tr>
    <tr>
        <td>6.x</td>
        <td>{{< artifact flink-connector-elasticsearch6 withScalaVersion >}}</td>
    </tr>
    <tr>
        <td>7 及更高版本</td>
        <td>{{< artifact flink-connector-elasticsearch7 withScalaVersion >}}</td>
    </tr>
  </tbody>
</table>

请注意，流连接器目前不是二进制发行版的一部分。
有关如何将程序和用于集群执行的库一起打包，参考[此处]({{< ref "docs/dev/datastream/project-configuration" >}})

## 安装 Elasticsearch

Elasticsearch 集群的设置可以参考[此处](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup.html)。
确保设置并记住集群名称。这是在创建 `ElasticsearchSink` 请求集群文档操作时必须要设置的。

## Elasticsearch 接收器

`ElasticsearchSink` 使用 `TransportClient` （6.x 之前） 或者 `RestHighLevelClient` （6.x 开始） 和 Elasticsearch 集群进行通信。

下面的示例展示了如何配置并创建一个接收器：

{{< tabs "51732edd-4218-470e-adad-b1ebb4021ae4" >}}
{{< tab "java, 5.x" >}}
```java
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

DataStream<String> input = ...;

Map<String, String> config = new HashMap<>();
config.put("cluster.name", "my-cluster-name");
// 这指示接收器在接收每个元素之后立即提交，否则它们将被缓存
config.put("bulk.flush.max.actions", "1");

List<InetSocketAddress> transportAddresses = new ArrayList<>();
transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));
transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.2.3.1"), 9300));

input.addSink(new ElasticsearchSink<>(config, transportAddresses, new ElasticsearchSinkFunction<String>() {
    public IndexRequest createIndexRequest(String element) {
        Map<String, String> json = new HashMap<>();
        json.put("data", element);
    
        return Requests.indexRequest()
                .index("my-index")
                .type("my-type")
                .source(json);
    }
    
    @Override
    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
        indexer.add(createIndexRequest(element));
    }
}));```
{{< /tab >}}
{{< tab "java, Elasticsearch 6.x 及以上" >}}
```java
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

DataStream<String> input = ...;

List<HttpHost> httpHosts = new ArrayList<>();
httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
httpHosts.add(new HttpHost("10.2.3.1", 9200, "http"));

// 使用 ElasticsearchSink.Builder 创建 ElasticsearchSink
ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
    httpHosts,
    new ElasticsearchSinkFunction<String>() {
        public IndexRequest createIndexRequest(String element) {
            Map<String, String> json = new HashMap<>();
            json.put("data", element);
        
            return Requests.indexRequest()
                    .index("my-index")
                    .type("my-type")
                    .source(json);
        }
        
        @Override
        public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
            indexer.add(createIndexRequest(element));
        }
    }
);

// 批量请求的配置；这指示接收器在接收每个元素之后立即提交，否则它们将被缓存
esSinkBuilder.setBulkFlushMaxActions(1);

// 为内部创建的 REST 客户端提供一个自定义配置信息的 RestClientFactory
esSinkBuilder.setRestClientFactory(
  restClientBuilder -> {
    restClientBuilder.setDefaultHeaders(...)
    restClientBuilder.setMaxRetryTimeoutMillis(...)
    restClientBuilder.setPathPrefix(...)
    restClientBuilder.setHttpClientConfigCallback(...)
  }
);

// 最后，构建并添加接收器到作业管道中
input.addSink(esSinkBuilder.build());
```
{{< /tab >}}
{{< tab "scala, 5.x" >}}
```scala
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink

import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.ArrayList
import java.util.HashMap
import java.util.List
import java.util.Map

val input: DataStream[String] = ...

val config = new java.util.HashMap[String, String]
config.put("cluster.name", "my-cluster-name")
// 这指示接收器在接收每个元素之后立即提交，否则它们将被缓存
config.put("bulk.flush.max.actions", "1")

val transportAddresses = new java.util.ArrayList[InetSocketAddress]
transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300))
transportAddresses.add(new InetSocketAddress(InetAddress.getByName("10.2.3.1"), 9300))

input.addSink(new ElasticsearchSink(config, transportAddresses, new ElasticsearchSinkFunction[String] {
  def createIndexRequest(element: String): IndexRequest = {
    val json = new java.util.HashMap[String, String]
    json.put("data", element)
    
    return Requests.indexRequest()
            .index("my-index")
            .type("my-type")
            .source(json)
  }
}))
```
{{< /tab >}}
{{< tab "scala, Elasticsearch 6.x 及以上" >}}
```scala
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink

import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

import java.util.ArrayList
import java.util.List

val input: DataStream[String] = ...

val httpHosts = new java.util.ArrayList[HttpHost]
httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"))
httpHosts.add(new HttpHost("10.2.3.1", 9200, "http"))

val esSinkBuilder = new ElasticsearchSink.Builder[String](
  httpHosts,
  new ElasticsearchSinkFunction[String] {
     def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer) {
          val json = new java.util.HashMap[String, String]
          json.put("data", element)

          val rqst: IndexRequest = Requests.indexRequest
            .index("my-index")
            .`type`("my-type")
            .source(json)

          indexer.add(rqst)
     } 
  }
)

// 批量请求的配置；这指示接收器在接收每个元素之后立即提交，否则它们将被缓存
esSinkBuilder.setBulkFlushMaxActions(1)

// 为内部创建的 REST 客户端提供一个自定义配置信息的 RestClientFactory
esSinkBuilder.setRestClientFactory(new RestClientFactory {
  override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {
       restClientBuilder.setDefaultHeaders(...)
       restClientBuilder.setMaxRetryTimeoutMillis(...)
       restClientBuilder.setPathPrefix(...)
       restClientBuilder.setHttpClientConfigCallback(...)
  }
})

// 最后，构建并添加接收器到作业管道中
input.addSink(esSinkBuilder.build)
```
{{< /tab >}}
{{< /tabs >}}

对于仍然使用已被弃用的 `TransportClient` 和 Elasticsearch 集群通信的 Elasticsearch 版本 (即，小于或等于 5.x 的版本)，
请注意如何使用一个 `String` 类型的 `Map` 配置 `ElasticsearchSink`。在创建内部使用的 `TransportClient` 时将直接转发此配置映射。
配置键记录在[此处](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)的 Elasticsearch 文档中。
特别重要的是参数 `cluster.name` 必须和你的集群名称对应上。

对于 Elasticsearch 6.x 及以上版本，内部使用 `RestHighLevelClient` 和集群通信。
默认情况下，连接器使用 REST 客户端的默认配置。
如果要使用自定义配置的 REST 客户端，用户可以在设置构建接收器的 `ElasticsearchClient.Builder` 时提供一个 `RestClientFactory` 的实现。

另外注意，该示例仅演示了对每个传入的元素执行单个索引请求。
通常，`ElasticsearchSinkFunction` 可用于执行多个不同类型的请求（例如 `DeleteRequest`、 `UpdateRequest` 等）。

在内部，Flink Elasticsearch 接收器的每个并行实例使用一个 `BulkProcessor` 向集群发送操作请求。
这将使得元素在发送到集群之前进行批量缓存。
`BulkProcessor` 一次执行一个批量请求，即不会存在两个并行刷新缓存的操作。

### Elasticsearch 接收器和容错

启用 Flink 的检查点后，Flink Elasticsearch 接收器保证至少一次将操作请求发送到 Elasticsearch 集群。
它通过在检查点时等待 `BulkProcessor` 中所有挂起的操作请求来实现。
这有效地保证了在触发检查点之前所有的请求被 Elasticsearch 成功确认，然后继续处理发送到接收器的更多的记录。

检查点和容错更多详细信息，请参见[容错文档]({{< ref "docs/learn-flink/fault_tolerance" >}})。

要使用容错 Elasticsearch Sinks，需要在执行环境启用拓扑检查点：

{{< tabs "d00d1e93-4844-40d7-b0ec-9ec37e73145e" >}}
{{< tab "Java" >}}
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000); // checkpoint every 5000 msecs
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.enableCheckpointing(5000) // checkpoint every 5000 msecs
```
{{< /tab >}}
{{< /tabs >}}

<p style="border-radius: 5px; padding: 5px" class="bg-danger">
<b>注意</b>： 如果用户愿意，可以通过在创建的
<b> ElasticsearchSink </b>上调用 <b>disableFlushOnCheckpoint()</b> 来禁用刷新。请注意，
 这实质上意味着接收器将不再提供任何强大的交付保证，即使启用了拓扑检查点。
</p>

### 处理失败的 Elasticsearch 请求

Elasticsearch 操作请求可能由于多种原因而失败，包括节点队列容量暂时饱和或者要被索引的文档格式错误。
Flink Elasticsearch 接收器允许用户通过简单地实现一个 `ActionRequestFailureHandler` 并将其提供给构造函数来指定如何处理失败的请求。

下面是一个例子：

{{< tabs "ddb958b3-5dd5-476e-b946-ace3335628b2" >}}
{{< tab "Java" >}}
```java
DataStream<String> input = ...;

input.addSink(new ElasticsearchSink<>(
    config, transportAddresses,
    new ElasticsearchSinkFunction<String>() {...},
    new ActionRequestFailureHandler() {
        @Override
        void onFailure(ActionRequest action,
                Throwable failure,
                int restStatusCode,
                RequestIndexer indexer) throw Throwable {

            if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
                // 队列已满；重新添加文档进行索引
                indexer.add(action);
            } else if (ExceptionUtils.findThrowable(failure, ElasticsearchParseException.class).isPresent()) {
                // 文档格式错误；简单地删除请求避免接收器失败
            } else {
                // 对于所有其他失败的请求，失败的接收器
                // 这里的失败只是简单的重新抛出，但用户也可以选择抛出自定义异常
                throw failure;
            }
        }
}));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val input: DataStream[String] = ...

input.addSink(new ElasticsearchSink(
    config, transportAddresses,
    new ElasticsearchSinkFunction[String] {...},
    new ActionRequestFailureHandler {
        @throws(classOf[Throwable])
        override def onFailure(ActionRequest action,
                Throwable failure,
                int restStatusCode,
                RequestIndexer indexer) {

            if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
                 // 队列已满；重新添加文档进行索引
                indexer.add(action)
            } else if (ExceptionUtils.findThrowable(failure, ElasticsearchParseException.class).isPresent()) {
                 // 文档格式错误；简单地删除请求避免接收器失败
            } else {
                // 对于所有其他失败的请求，失败的接收器
                // 这里的失败只是简单的重新抛出，但用户也可以选择抛出自定义异常
                throw failure
            }
        }
}))
```
{{< /tab >}}
{{< /tabs >}}

上面的示例接收器重新添加由于队列容量饱和而失败的请求并丢弃文档格式错误的请求，而不会使接收器失败。
对于其它故障，接收器将会失败。如果未向构造器提供一个 `ActionRequestFailureHandler`，那么任何类型的错误都会导致接收器失败。

注意，`onFailure` 仅在 `BulkProcessor` 内部完成所有补偿重试尝试后仍发生故障时被调用。
默认情况下，`BulkProcessor` 最多重试 8 次，并采用指数补偿。有关 `BulkProcessor` 内部行为以及如何配置它的更多信息，请参阅以下部分。

默认情况下，如果未提供失败处理程序，那么接收器使用 `NoOpFailureHandler` 来简单处理所有的异常。
连接器还提供了一个 `RetryRejectedExecutionFailureHandler` 实现，它总是重新添加由于队列容量饱和导致失败的请求。

<p style="border-radius: 5px; padding: 5px" class="bg-danger">
<b>重要提示</b>：在失败时将请求重新添加回内部 <b>BulkProcessor</b> 会导致更长的检查点，因为在检查点时接收器还需要等待重新添加的请求被刷新。
例如，当使用 <b>RetryRejectedExecutionFailureHandler</b> 时，
检查点将需要等到 Elasticsearch 节点队列有足够的容量来处理所有挂起的请求。
这也就意味着如果重新添加的请求永远不成功，检查点也将永远不会完成。
</p>

### 配置内部批量处理器

通过在提供的 `Map<String, String>` 中设置以下值，内部 `BulkProcessor` 可以进一步配置其如何刷新缓存操作请求的行为：

 * **bulk.flush.max.actions**：刷新前缓存的最大操作数。
 * **bulk.flush.max.size.mb**：刷新前缓存的最大数据大小（以兆字节为单位）。
 * **bulk.flush.interval.ms**：刷新的时间间隔（不论缓存操作的数量或大小如何）。

对于 2.x 及以上版本，还支持配置如何重试临时请求错误：

 * **bulk.flush.backoff.enable**：如果一个或多个请求由于临时的 `EsRejectedExecutionException` 而失败，是否为刷新执行带有补偿延迟的重试操作。
 * **bulk.flush.backoff.type**：补偿的延迟类型，`CONSTANT` 或者 `EXPONENTIAL`。
 * **bulk.flush.backoff.delay**：补偿的延迟量。对于常量补偿来说，值是每次重试之间的间隔。对于指数补偿来说，这是基本延迟的初始值。
 * **bulk.flush.backoff.retries**：要尝试补偿重试的次数。

可以在[此处](https://elastic.co)找到 Elasticsearch 的更多信息。

## 将 Elasticsearch 连接器打包到 Uber-Jar 中

为了执行你的 Flink 程序，建议构建一个叫做 uber-jar (可执行的 jar)，其中包含了你所有的依赖
(更多信息参见[此处]({{< ref "docs/dev/datastream/project-configuration" >}}))。

或者，你可以将连接器的 jar 文件放入 Flink 的 `lib/` 目录下，使其在系统范围内可用，即用于所有正在运行的作业。

{{< top >}}
