---
title: "Elasticsearch 2.x Connector"

# Sub-level navigation
sub-nav-group: streaming
sub-nav-parent: connectors
sub-nav-pos: 2
sub-nav-title: Elasticsearch 2.x
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
[Elasticsearch 2.x](https://elastic.co/) Index. To use this connector, add the
following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-elasticsearch2{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary
distribution. See
[here]({{site.baseurl}}/apis/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution)
for information about how to package the program with the libraries for
cluster execution.

#### Installing Elasticsearch 2.x

Instructions for setting up an Elasticsearch cluster can be found
[here](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup.html).
Make sure to set and remember a cluster name. This must be set when
creating a Sink for writing to your cluster

#### Elasticsearch 2.x Sink
The connector provides a Sink that can send data to an Elasticsearch 2.x Index.

The sink communicates with Elasticsearch via Transport Client

See [here](https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/transport-client.html)
for information about the Transport Client.

The code below shows how to create a sink that uses a `TransportClient` for communication:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
File dataDir = ....;

DataStream<String> input = ...;

Map<String, String> config = new HashMap<>();
// This instructs the sink to emit after every element, otherwise they would be buffered
config.put("bulk.flush.max.actions", "1");
config.put("cluster.name", "my-cluster-name");

List<InetSocketAddress> transports = new ArrayList<>();
transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));
transports.add(new InetSocketAddress(InetAddress.getByName("10.2.3.1"), 9300));

input.addSink(new ElasticsearchSink(config, transports, new ElasticsearchSinkFunction<String>() {
  public IndexRequest createIndexRequest(String element): IndexRequest = {
    Map<String,String> json = new HashMap<>()
    json.put("data", element)

    return Requests.indexRequest()
            .index("my-index")
            .type("my-type")
            .source(json);

  }
  
  @Override
  public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
    indexer.add(createIndexRequest(element))
  }
}));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val dataDir = ....;

val input: DataStream[String] = ...

val config = new util.HashMap[String, String]
config.put("bulk.flush.max.actions", "1")
config.put("cluster.name", "my-cluster-name")

val transports = new ArrayList[String]
transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300))
transports.add(new InetSocketAddress(InetAddress.getByName("10.2.3.1"), 9300));

input.addSink(new ElasticsearchSink(config, transports, new ElasticsearchSinkFunction[String] {
  def createIndexRequest(element: String): IndexRequest = {
    val json = new util.HashMap[String, AnyRef]
    json.put("data", element)
    Requests.indexRequest.index("my-index").`type`("my-type").source(json)
  }
  
  override def process(element: String, ctx: RuntimeContext, indexer: RequestIndexer) {
    indexer.add(createIndexRequest(element))
  } 
}))
{% endhighlight %}
</div>
</div>

A Map of Strings is used to configure the Sink. The configuration keys
are documented in the Elasticsearch documentation
[here](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html).
Especially important is the `cluster.name`. parameter that must correspond to
the name of your cluster and with ElasticSearch 2x you also need to specify `path.home`.

Internally, the sink uses a `BulkProcessor` to send Action requests to the cluster.
This will buffer elements and Action Requests before sending to the cluster. The behaviour of the
`BulkProcessor` can be configured using these config keys:
 * **bulk.flush.max.actions**: Maximum amount of elements to buffer
 * **bulk.flush.max.size.mb**: Maximum amount of data (in megabytes) to buffer
 * **bulk.flush.interval.ms**: Interval at which to flush data regardless of the other two
  settings in milliseconds

This now provides a list of Elasticsearch Nodes 
to which the sink should connect via a `TransportClient`.

More about information about Elasticsearch can be found [here](https://elastic.co).
