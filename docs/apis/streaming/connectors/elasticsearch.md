---
title: "Elasticsearch Connector"

# Sub-level navigation
sub-nav-group: streaming
sub-nav-parent: connectors
sub-nav-pos: 2
sub-nav-title: Elasticsearch
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
