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

此连接器提供可以向 [Elasticsearch](https://elastic.co/) 索引请求文档操作的 sinks。
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
        <td>6.x</td>
        <td>{{< artifact flink-connector-elasticsearch6 >}}</td>
    </tr>
    <tr>
        <td>7.x</td>
        <td>{{< artifact flink-connector-elasticsearch7 >}}</td>
    </tr>
  </tbody>
</table>

请注意，流连接器目前不是二进制发行版的一部分。
有关如何将程序和用于集群执行的库一起打包，参考[此文档]({{< ref "docs/dev/configuration/overview" >}})。

## 安装 Elasticsearch

Elasticsearch 集群的设置可以参考[此文档](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup.html)。

## Elasticsearch Sink

下面的示例展示了如何配置并创建一个 sink：

{{< tabs "51732edd-4218-470e-adad-b1ebb4021ae4" >}}
{{< tab "Java" >}}
Elasticsearch 6:
```java
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch6SinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

DataStream<String> input = ...;

input.sinkTo(
    new Elasticsearch6SinkBuilder<String>()
        // 下面的设置使 sink 在接收每个元素之后立即提交，否则这些元素将被缓存起来
        .setBulkFlushMaxActions(1)
        .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
        .setEmitter(
        (element, context, indexer) ->
        indexer.add(createIndexRequest(element)))
        .build());

private static IndexRequest createIndexRequest(String element) {
    Map<String, Object> json = new HashMap<>();
    json.put("data", element);

    return Requests.indexRequest()
        .index("my-index")
        .type("my-type")
        .id(element)
        .source(json);
}
```

Elasticsearch 7:
```java
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.HashMap;
import java.util.Map;

DataStream<String> input = ...;

input.sinkTo(
    new Elasticsearch7SinkBuilder<String>()
        // 下面的设置使 sink 在接收每个元素之后立即提交，否则这些元素将被缓存起来
        .setBulkFlushMaxActions(1)
        .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
        .setEmitter(
        (element, context, indexer) ->
        indexer.add(createIndexRequest(element)))
        .build());

private static IndexRequest createIndexRequest(String element) {
    Map<String, Object> json = new HashMap<>();
    json.put("data", element);

    return Requests.indexRequest()
        .index("my-index")
        .id(element)
        .source(json);
}
```
{{< /tab >}}
{{< tab "Scala" >}}
Elasticsearch 6:
```scala
import org.apache.flink.api.connector.sink.SinkWriter
import org.apache.flink.connector.elasticsearch.sink.{Elasticsearch6SinkBuilder, RequestIndexer}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

val input: DataStream[String] = ...

input.sinkTo(
  new Elasticsearch6SinkBuilder[String]
    // 下面的设置使 sink 在接收每个元素之后立即提交，否则这些元素将被缓存起来
    .setBulkFlushMaxActions(1)
    .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
    .setEmitter((element: String, context: SinkWriter.Context, indexer: RequestIndexer) => 
    indexer.add(createIndexRequest(element)))
    .build())

def createIndexRequest(element: (String)): IndexRequest = {

  val json = Map(
    "data" -> element.asInstanceOf[AnyRef]
  )

  Requests.indexRequest.index("my-index").source(mapAsJavaMap(json))
}
```

Elasticsearch 7:
```scala
import org.apache.flink.api.connector.sink.SinkWriter
import org.apache.flink.connector.elasticsearch.sink.{Elasticsearch7SinkBuilder, RequestIndexer}
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

val input: DataStream[String] = ...

input.sinkTo(
  new Elasticsearch7SinkBuilder[String]
    // 下面的设置使 sink 在接收每个元素之后立即提交，否则这些元素将被缓存起来
    .setBulkFlushMaxActions(1)
    .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
    .setEmitter((element: String, context: SinkWriter.Context, indexer: RequestIndexer) => 
    indexer.add(createIndexRequest(element)))
    .build())

def createIndexRequest(element: (String)): IndexRequest = {

  val json = Map(
    "data" -> element.asInstanceOf[AnyRef]
  )

  Requests.indexRequest.index("my-index").`type`("my-type").source(mapAsJavaMap(json))
}
```
{{< /tab >}}
{{< /tabs >}}

需要注意的是，该示例仅演示了对每个传入的元素执行单个索引请求。
通常，`ElasticsearchSinkFunction` 可用于执行多个不同类型的请求（例如 `DeleteRequest`、 `UpdateRequest` 等）。

在内部，Flink Elasticsearch Sink 的每个并行实例使用一个 `BulkProcessor` 向集群发送操作请求。
这会在元素批量发送到集群之前进行缓存。
`BulkProcessor` 一次执行一个批量请求，即不会存在两个并行刷新缓存的操作。

### Elasticsearch Sinks 和容错

通过启用 Flink checkpoint，Flink Elasticsearch Sink 保证至少一次将操作请求发送到 Elasticsearch 集群。
这是通过在进行 checkpoint 时等待 `BulkProcessor` 中所有挂起的操作请求来实现。
这有效地保证了在触发 checkpoint 之前所有的请求被 Elasticsearch 成功确认，然后继续处理发送到 sink 的记录。

关于 checkpoint 和容错的更多详细信息，请参见[容错文档]({{< ref "docs/learn-flink/fault_tolerance" >}})。

要使用具有容错特性的 Elasticsearch Sinks，需要在执行环境中启用作业拓扑的 checkpoint：

{{< tabs "d00d1e93-4844-40d7-b0ec-9ec37e73145e" >}}
{{< tab "Java" >}}
Elasticsearch 6:
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000); // 每 5000 毫秒执行一次 checkpoint

Elasticsearch6SinkBuilder sinkBuilder = new Elasticsearch6SinkBuilder<String>()
    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
    .setEmitter(
    (element, context, indexer) -> 
    indexer.add(createIndexRequest(element)));
```

Elasticsearch 7:
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000); // 每 5000 毫秒执行一次 checkpoint

Elasticsearch7SinkBuilder sinkBuilder = new Elasticsearch7SinkBuilder<String>()
    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
    .setEmitter(
    (element, context, indexer) -> 
    indexer.add(createIndexRequest(element)));
```
{{< /tab >}}
{{< tab "Scala" >}}
Elasticsearch 6:
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.enableCheckpointing(5000) // 每 5000 毫秒执行一次 checkpoint

val sinkBuilder = new Elasticsearch6SinkBuilder[String]
  .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
  .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
  .setEmitter((element: String, context: SinkWriter.Context, indexer: RequestIndexer) =>
  indexer.add(createIndexRequest(element)))
```

Elasticsearch 7:
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.enableCheckpointing(5000) // 每 5000 毫秒执行一次 checkpoint

val sinkBuilder = new Elasticsearch7SinkBuilder[String]
  .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
  .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
  .setEmitter((element: String, context: SinkWriter.Context, indexer: RequestIndexer) =>
  indexer.add(createIndexRequest(element)))
```
{{< /tab >}}
{{< /tabs >}}

<p style="border-radius: 5px; padding: 5px" class="bg-info">
Using UpdateRequests with deterministic ids and the upsert method it is possible to achieve exactly-once semantics in Elasticsearch when AT_LEAST_ONCE delivery is configured for the connector.
</p>

### 处理失败的 Elasticsearch 请求

Elasticsearch 操作请求可能由于多种原因而失败，包括节点队列容量暂时已满或者要被索引的文档格式错误。
Flink Elasticsearch Sink 允许用户通过通过指定一个退避策略来重试请求。

下面是一个例子：

{{< tabs "ddb958b3-5dd5-476e-b946-ace3335628b2" >}}
{{< tab "Java" >}}
Elasticsearch 6:
```java
DataStream<String> input = ...;

input.sinkTo(
    new Elasticsearch6SinkBuilder<String>()
        .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
        .setEmitter(
        (element, context, indexer) ->
        indexer.add(createIndexRequest(element)))
        // 这里启用了一个指数退避重试策略，初始延迟为 1000 毫秒且最大重试次数为 5
        .setBulkFlushBackoffStrategy(FlushBackoffType.EXPONENTIAL, 5, 1000)
        .build());
```

Elasticsearch 7:
```java
DataStream<String> input = ...;

input.sinkTo(
    new Elasticsearch7SinkBuilder<String>()
        .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
        .setEmitter(
        (element, context, indexer) ->
        indexer.add(createIndexRequest(element)))
        // 这里启用了一个指数退避重试策略，初始延迟为 1000 毫秒且最大重试次数为 5
        .setBulkFlushBackoffStrategy(FlushBackoffType.EXPONENTIAL, 5, 1000)
        .build());
```
{{< /tab >}}
{{< tab "Scala" >}}
Elasticsearch 6:
```scala
val input: DataStream[String] = ...

input.sinkTo(
  new Elasticsearch6SinkBuilder[String]
    .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
    .setEmitter((element: String, context: SinkWriter.Context, indexer: RequestIndexer) => 
    indexer.add(createIndexRequest(element)))
    // 这里启用了一个指数退避重试策略，初始延迟为 1000 毫秒且最大重试次数为 5
    .setBulkFlushBackoffStrategy(FlushBackoffType.EXPONENTIAL, 5, 1000)
    .build())
```

Elasticsearch 7:
```scala
val input: DataStream[String] = ...

input.sinkTo(
  new Elasticsearch7SinkBuilder[String]
    .setHosts(new HttpHost("127.0.0.1", 9200, "http"))
    .setEmitter((element: String, context: SinkWriter.Context, indexer: RequestIndexer) => 
    indexer.add(createIndexRequest(element)))
    // 这里启用了一个指数退避重试策略，初始延迟为 1000 毫秒且最大重试次数为 5
    .setBulkFlushBackoffStrategy(FlushBackoffType.EXPONENTIAL, 5, 1000)
    .build())
```
{{< /tab >}}
{{< /tabs >}}

上面的示例 sink 重新添加由于资源受限（例如：队列容量已满）而失败的请求。对于其它类型的故障，例如文档格式错误，sink 将会失败。
如若未设置 BulkFlushBackoffStrategy (或者 `FlushBackoffType.NONE`)，那么任何类型的错误都会导致 sink 失败。

<p style="border-radius: 5px; padding: 5px" class="bg-danger">
<b>重要提示</b>：在失败时将请求重新添加回内部 <b>BulkProcessor</b> 会导致更长的 checkpoint，因为在进行 checkpoint 时，sink 还需要等待重新添加的请求被刷新。
例如，当使用 <b>FlushBackoffType.EXPONENTIAL</b> 时，
checkpoint 会进行等待，直到 Elasticsearch 节点队列有足够的容量来处理所有挂起的请求，或者达到最大重试次数。
</p>

### 配置内部批量处理器

通过使用以下在 Elasticsearch6SinkBuilder 中提供的方法，可以进一步配置内部的 `BulkProcessor` 关于其如何刷新缓存操作请求的行为：

 * **setBulkFlushMaxActions(int numMaxActions)**：刷新前最大缓存的操作数。
 * **setBulkFlushMaxSizeMb(int maxSizeMb)**：刷新前最大缓存的数据量（以兆字节为单位）。
 * **setBulkFlushInterval(long intervalMillis)**：刷新的时间间隔（不论缓存操作的数量或大小如何）。

还支持配置如何对暂时性请求错误进行重试：

 * **setBulkFlushBackoffStrategy(FlushBackoffType flushBackoffType, int maxRetries, long delayMillis)**：退避延迟的类型，`CONSTANT` 或者 `EXPONENTIAL`，退避重试次数，退避重试的时间间隔。
   对于常量延迟来说，此值是每次重试间的间隔。对于指数延迟来说，此值是延迟的初始值。

可以在[此文档](https://elastic.co)找到 Elasticsearch 的更多信息。

## 将 Elasticsearch 连接器打包到 Uber-Jar 中

建议构建一个包含所有依赖的 uber-jar (可执行的 jar)，以便更好地执行你的 Flink 程序。
(更多信息参见[此文档]({{< ref "docs/dev/configuration/overview" >}}))。

或者，你可以将连接器的 jar 文件放入 Flink 的 `lib/` 目录下，使其在全局范围内可用，即可用于所有的作业。

{{< top >}}
