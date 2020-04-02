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

This connector provides sinks that can request document actions to an
[Elasticsearch](https://elastic.co/) Index. To use this connector, add one
of the following dependencies to your project, depending on the version
of the Elasticsearch installation:

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

Note that the streaming connectors are currently not part of the binary
distribution. See [here]({{site.baseurl}}/dev/projectsetup/dependencies.html) for information
about how to package the program with the libraries for cluster execution.

## Installing Elasticsearch

Instructions for setting up an Elasticsearch cluster can be found
[here](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup.html).
Make sure to set and remember a cluster name. This must be set when
creating an `ElasticsearchSink` for requesting document actions against your cluster.

## Elasticsearch Sink

The `ElasticsearchSink` uses a `TransportClient` (before 6.x) or `RestHighLevelClient` (starting with 6.x) to communicate with an
Elasticsearch cluster.

The example below shows how to configure and create a sink:

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

For Elasticsearch versions that still uses the now deprecated `TransportClient` to communicate
with the Elasticsearch cluster (i.e., versions equal or below 5.x), note how a `Map` of `String`s
is used to configure the `ElasticsearchSink`. This config map will be directly
forwarded when creating the internally used `TransportClient`.
The configuration keys are documented in the Elasticsearch documentation
[here](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html).
Especially important is the `cluster.name` parameter that must correspond to
the name of your cluster.

For Elasticsearch 6.x and above, internally, the `RestHighLevelClient` is used for cluster communication.
By default, the connector uses the default configurations for the REST client. To have custom
configuration for the REST client, users can provide a `RestClientFactory` implementation when
setting up the `ElasticsearchClient.Builder` that builds the sink.

Also note that the example only demonstrates performing a single index
request for each incoming element. Generally, the `ElasticsearchSinkFunction`
can be used to perform multiple requests of different types (ex.,
`DeleteRequest`, `UpdateRequest`, etc.).

Internally, each parallel instance of the Flink Elasticsearch Sink uses
a `BulkProcessor` to send action requests to the cluster.
This will buffer elements before sending them in bulk to the cluster. The `BulkProcessor`
executes bulk requests one at a time, i.e. there will be no two concurrent
flushes of the buffered actions in progress.

### Elasticsearch Sinks and Fault Tolerance

With Flink’s checkpointing enabled, the Flink Elasticsearch Sink guarantees
at-least-once delivery of action requests to Elasticsearch clusters. It does
so by waiting for all pending action requests in the `BulkProcessor` at the
time of checkpoints. This effectively assures that all requests before the
checkpoint was triggered have been successfully acknowledged by Elasticsearch, before
proceeding to process more records sent to the sink.

More details on checkpoints and fault tolerance are in the [fault tolerance docs]({{site.baseurl}}/internals/stream_checkpointing.html).

To use fault tolerant Elasticsearch Sinks, checkpointing of the topology needs to be enabled at the execution environment:

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

<p style="border-radius: 5px; padding: 5px" class="bg-danger">
<b>NOTE</b>: Users can disable flushing if they wish to do so, by calling
<b>disableFlushOnCheckpoint()</b> on the created <b>ElasticsearchSink</b>. Be aware
that this essentially means the sink will not provide any strong
delivery guarantees anymore, even with checkpoint for the topology enabled.
</p>

### Handling Failing Elasticsearch Requests

Elasticsearch action requests may fail due to a variety of reasons, including
temporarily saturated node queue capacity or malformed documents to be indexed.
The Flink Elasticsearch Sink allows the user to specify how request
failures are handled, by simply implementing an `ActionRequestFailureHandler` and
providing it to the constructor.

Below is an example:

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

The above example will let the sink re-add requests that failed due to
queue capacity saturation and drop requests with malformed documents, without
failing the sink. For all other failures, the sink will fail. If a `ActionRequestFailureHandler`
is not provided to the constructor, the sink will fail for any kind of error.

Note that `onFailure` is called for failures that still occur only after the
`BulkProcessor` internally finishes all backoff retry attempts.
By default, the `BulkProcessor` retries to a maximum of 8 attempts with
an exponential backoff. For more information on the behaviour of the
internal `BulkProcessor` and how to configure it, please see the following section.

By default, if a failure handler is not provided, the sink uses a
`NoOpFailureHandler` that simply fails for all kinds of exceptions. The
connector also provides a `RetryRejectedExecutionFailureHandler` implementation
that always re-add requests that have failed due to queue capacity saturation.

<p style="border-radius: 5px; padding: 5px" class="bg-danger">
<b>IMPORTANT</b>: Re-adding requests back to the internal <b>BulkProcessor</b>
on failures will lead to longer checkpoints, as the sink will also
need to wait for the re-added requests to be flushed when checkpointing.
For example, when using <b>RetryRejectedExecutionFailureHandler</b>, checkpoints
will need to wait until Elasticsearch node queues have enough capacity for
all the pending requests. This also means that if re-added requests never
succeed, the checkpoint will never finish.
</p>

### Configuring the Internal Bulk Processor

The internal `BulkProcessor` can be further configured for its behaviour
on how buffered action requests are flushed, by setting the following values in
the provided `Map<String, String>`:

 * **bulk.flush.max.actions**: Maximum amount of actions to buffer before flushing.
 * **bulk.flush.max.size.mb**: Maximum size of data (in megabytes) to buffer before flushing.
 * **bulk.flush.interval.ms**: Interval at which to flush regardless of the amount or size of buffered actions.
 
For versions 2.x and above, configuring how temporary request errors are
retried is also supported:
 
 * **bulk.flush.backoff.enable**: Whether or not to perform retries with backoff delay for a flush
 if one or more of its actions failed due to a temporary `EsRejectedExecutionException`.
 * **bulk.flush.backoff.type**: The type of backoff delay, either `CONSTANT` or `EXPONENTIAL`
 * **bulk.flush.backoff.delay**: The amount of delay for backoff. For constant backoff, this
 is simply the delay between each retry. For exponential backoff, this is the initial base delay.
 * **bulk.flush.backoff.retries**: The amount of backoff retries to attempt.

More information about Elasticsearch can be found [here](https://elastic.co).

## Packaging the Elasticsearch Connector into an Uber-Jar

For the execution of your Flink program, it is recommended to build a
so-called uber-jar (executable jar) containing all your dependencies
(see [here]({{site.baseurl}}/dev/projectsetup/dependencies.html) for further information).

Alternatively, you can put the connector's jar file into Flink's `lib/` folder to make it available
system-wide, i.e. for all job being run.

{% top %}
