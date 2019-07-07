---
title: "Elasticsearch Connector"
nav-title: Elasticsearch
nav-parent_id: connectors
nav-pos: 4
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

Elasticsearch 连接器相关操作可以查看 [Elasticsearch](https://elastic.co/) 官方网站。要使用此连接器，只需要在你的项目里添加以下依赖项，具体取决于Elasticsearch安装版本：


<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left">Maven Dependency</th>
      <th class="text-left">Supported since</th>
      <th class="text-left">Elasticsearch version</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>flink-connector-elasticsearch5{{ site.scala_version_suffix }}</td>
        <td>1.3.0</td>
        <td>5.x</td>
    </tr>
    <tr>
        <td>flink-connector-elasticsearch6{{ site.scala_version_suffix }}</td>
        <td>1.6.0</td>
        <td>6.x</td>
    </tr>
    <tr>
        <td>flink-connector-elasticsearch7{{ site.scala_version_suffix }}</td>
        <td>1.10.0</td>
        <td>7 and later versions</td>
    </tr>
  </tbody>
</table>

请注意，当前 Flink 二进制安装文件不包括 Elasticsearch 流连接器。有关信息，请参见[此处]({{site.baseurl}}/zh/dev/project-configuration.html)
关于如何将程序与库打包以供集群执行。

## 安装 Elasticsearch

[这里](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup.html)可以找到有关设置Elasticsearch集群的说明。确保设置并记住集群名称。集群名称将在创建一个`ElasticsearchSink`时使用，用于请求对你安装的集群的文档操作。

## Elasticsearch Sink

`ElasticsearchSink` 使用 `TransportClient`（在6.x之前）或 `RestHighLevelClient`（从6.x开始）与
Elasticsearch 集群通信。

下面例子展示了如何配置和创建一个 `ElasticsearchSink` ：

<div class="codetabs" markdown="1">
<div data-lang="java, 5.x" markdown="1">
{% highlight java %}
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
// This instructs the sink to emit after every element, otherwise they would be buffered
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
}));{% endhighlight %}
</div>
<div data-lang="java, Elasticsearch 6.x and above" markdown="1">
{% highlight java %}
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

// use a ElasticsearchSink.Builder to create an ElasticsearchSink
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

// configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
esSinkBuilder.setBulkFlushMaxActions(1);

// provide a RestClientFactory for custom configuration on the internally created REST client
esSinkBuilder.setRestClientFactory(
  restClientBuilder -> {
    restClientBuilder.setDefaultHeaders(...)
    restClientBuilder.setMaxRetryTimeoutMillis(...)
    restClientBuilder.setPathPrefix(...)
    restClientBuilder.setHttpClientConfigCallback(...)
  }
);

// finally, build and add the sink to the job's pipeline
input.addSink(esSinkBuilder.build());
{% endhighlight %}
</div>
<div data-lang="scala, 5.x" markdown="1">
{% highlight scala %}
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
// This instructs the sink to emit after every element, otherwise they would be buffered
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
{% endhighlight %}
</div>
<div data-lang="scala, Elasticsearch 6.x and above" markdown="1">
{% highlight scala %}
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

// configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
esSinkBuilder.setBulkFlushMaxActions(1)

// provide a RestClientFactory for custom configuration on the internally created REST client
esSinkBuilder.setRestClientFactory(new RestClientFactory {
  override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {
       restClientBuilder.setDefaultHeaders(...)
       restClientBuilder.setMaxRetryTimeoutMillis(...)
       restClientBuilder.setPathPrefix(...)
       restClientBuilder.setHttpClientConfigCallback(...)
  }
})

// finally, build and add the sink to the job's pipeline
input.addSink(esSinkBuilder.build)
{% endhighlight %}
</div>
</div>

对于 Elasticsearch（即版本等于或低于5.x）的集群仍使用现已弃用的 `TransportClient` 进行通信，请注意 `ElasticsearchSink` 使用一个由 `String` 构成的 `Map` 来进行参数配置 。这个配置 map 将直接在内部创建使用`TransportClient` 时转发。配置关键参数在 Elasticsearch 文档中[此处](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)可以查看。特别重要的是 `cluster.name` 参数必须对应你集群的名称。

对于 Elasticsearch 6.x及更高版本，在内部，使用 `RestHighLevelClient` 进行集群通信。默认情况下，连接器使用 REST 客户端的默认配置。要想自定义 REST 客户端的配置，用户可以在提供 `RestClientFactory` 实现时设置构建 sink 的 `ElasticsearchClient.Builder` 。

另外请注意，该示例仅展示了执行单个索引请求的每个传入元素。通常，`ElasticsearchSinkFunction` 可用于执行不同类型的多个请求（例如，`DeleteRequest` ，`UpdateRequest` 等）。

在内部，Flink Elasticsearch Sink 的每个并行实例都使用一个 `BulkProcessor` ，用于向集群发送操作请求。这将在批量发送到集群之前缓冲元素。 `BulkProcessor` 一次执行一个批量请求，即不会有两个并发刷新正在进行的缓冲操作。

### Elasticsearch Sink 与容错处理

在启用 Flink 的 Checkpoint 后，Flink Elasticsearch Sink 可以保证至少一次向 Elasticsearch 集群传递操作请求。确实如此所以通过等待 `BulkProcessor` 中的所有待处理操作请求 checkpoints 的时间。这有效地保证了之前的所有请求在触发 checkpoint 之前已被 Elasticsearch 成功接收确认继续处理发送到接收器的更多记录。

更多有关 checkpoint 和容错的详细信息，请参见[容错相关文档]({{site.baseurl}}/zh/learn-flink/fault_tolerance.html)。

要使用容错机制的 Elasticsearch Sink，需要在执行环境中开启拓扑的 checkpoint 机制：

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

<p style =“border-radius：5px; padding：5px”class =“bg-danger”>
<b>注意</b>：如果用户希望在创建的 <b>ElasticsearchSink</b> 上禁用 flush，可以通过调用 <b>disableFlushOnCheckpoint()</b> 。注意这基本上意味着即使拓扑的 checkpoint 开启了，sink 也无法提供任何强语义的消息投递保证。</p>

### 处理失败的 Elasticsearch 请求

由于各种原因，发给 Elasticsearch 的操作请求可能会失败，如暂时饱和的节点队列容量或索引异常文档。Flink Elasticsearch Sink 允许用户指定如何处理请求失败，通过简单地实现`ActionRequestFailureHandler`接口并传给 ElasticsearchSink 的构造函数即可。

下面是一个例子：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
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
                // full queue; re-add document for indexing
                indexer.add(action);
            } else if (ExceptionUtils.findThrowable(failure, ElasticsearchParseException.class).isPresent()) {
                // malformed document; simply drop request without failing sink
            } else {
                // for all other failures, fail the sink
                // here the failure is simply rethrown, but users can also choose to throw custom exceptions
                throw failure;
            }
        }
}));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
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
                // full queue; re-add document for indexing
                indexer.add(action)
            } else if (ExceptionUtils.findThrowable(failure, ElasticsearchParseException.class).isPresent()) {
                // malformed document; simply drop request without failing sink
            } else {
                // for all other failures, fail the sink
                // here the failure is simply rethrown, but users can also choose to throw custom exceptions
                throw failure
            }
        }
}))
{% endhighlight %}
</div>
</div>

上面的示例将让 sink 重新添加由于失败而导致的请求队列容量饱和和丢弃请求与格式错误的文档，没有失败了。对于所有其他故障， sink 将失败。如果是 `ActionRequestFailureHandler` 没有提供给构造函数，接收器将因任何类型的错误而失败。

请注意，`onFailure` 被调用用于仅在之后仍然发生的故障 `BulkProcessor` 内部完成所有退避重试尝试。默认情况下，`BulkProcessor` 重试最多8次尝试指数退避。有关行为的更多信息内部 `BulkProcessor` 以及如何配置它，请参阅以下部分。

默认情况下，如果未提供失败处理程序，则接收器使用a `NoOpFailureHandler` 只是因各种异常而失败。该连接器还提供了 `RetryRejectedExecutionFailureHandler` 实现总是重新添加由于队列容量饱和而失败的请求。

<b>重要</b>：将请求重新添加回内部 <b>BulkProcessor</b> 在失败时会导致 checkpoints 变长，sink 也一样需要等待重新添加的请求在 checkpoint 时被刷新。例如，当使用 <b>RetryRejectedExecutionFailureHandler</b> 时，checkpoints 需要等待直到 Elasticsearch 节点队列有足够的容量处理所有等待的请求。这也意味着如果从不重新添加请求成功，checkpoint 将永远不会结束。</p>


### Bulk Processor 内部配置

`BulkProcessor` 内部 可以进一步配置其行为，通过在提供的 `Map <String，String>` 中设置以下值来控制刷新缓冲操作请求的行为：

 * **bulk.flush.max.actions**: 在刷新之前要缓冲的最大操作量。
 * **bulk.flush.max.size.mb**: 在刷新之前要缓冲的最大数据大小(以兆字节为单位)。
 * **bulk.flush.interval.ms**: 不论缓冲操作的数量或大小，都要刷新的时间间隔。

对于 2.x 及更高版本，还支持配置临时请求错误的方式：

 * **bulk.flush.backoff.enable**: 是否启用带回退延迟的刷新重试。如果一个或多个操作由于临时的 `EsRejectedExecutionException` 而失败。
 * **bulk.flush.backoff.type**: 回退延迟的类型，`CONSTANT` 或 `EXPONENTIAL`。
 * **bulk.flush.backoff.delay**: 回退的延迟量。对于常数回退，这个只是每次重试之间的延迟。对于指数回退，这是初始延迟。
 * **bulk.flush.backoff.retries**: 重试回退的次数。

更多有关 Elasticsearch 的信息可以在[这里](https://elastic.co)找到。

## 将 Elasticsearch Connector 打包到 Uber-Jar 中

<<<<<<< HEAD
For the execution of your Flink program, it is recommended to build a
so-called uber-jar (executable jar) containing all your dependencies
(see [here]({{site.baseurl}}/dev/project-configuration.html) for further information).
=======
为了执行 Flink 程序，建议构建一个包含所有依赖项的所谓 uber-jar（可执行 jar ）有关详细信息，请参阅[此处]({{site.baseurl}}/zh/dev/projectsetup/dependencies.html)。
>>>>>>> 76f13ed7a4b... [FLINK-12942][docs-zh] Translate Elasticsearch Connector page into Chinese

或者，您可以将连接器的 jar 文件放入 Flink 的`lib /`文件夹中以使其可用于所有系统，即所有正在运行的 job 。

{% top %}
