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
        <td>flink-connector-elasticsearch{{ site.scala_version_suffix }}</td>
        <td>1.0.0</td>
        <td>1.x</td>
    </tr>
    <tr>
        <td>flink-connector-elasticsearch2{{ site.scala_version_suffix }}</td>
        <td>1.0.0</td>
        <td>2.x</td>
    </tr>
    <tr>
        <td>flink-connector-elasticsearch5{{ site.scala_version_suffix }}</td>
        <td>1.2.0</td>
        <td>5.x</td>
    </tr>
  </tbody>
</table>

Note that the streaming connectors are currently not part of the binary
distribution. See [here]({{site.baseurl}}/dev/linking.html) for information
about how to package the program with the libraries for cluster execution.

#### Installing Elasticsearch

Instructions for setting up an Elasticsearch cluster can be found
[here](https://www.elastic.co/guide/en/elasticsearch/reference/current/setup.html).
Make sure to set and remember a cluster name. This must be set when
creating an `ElasticsearchSink` for requesting document actions against your cluster.

#### Elasticsearch Sink

The `ElasticsearchSink` uses a `TransportClient` to communicate with an
Elasticsearch cluster.

The example below shows how to configure and create a sink:

<div class="codetabs" markdown="1">
<div data-lang="java, Elasticsearch 1.x" markdown="1">
{% highlight java %}
DataStream<String> input = ...;

Map<String, String> config = new HashMap<>();
config.put("cluster.name", "my-cluster-name");
// This instructs the sink to emit after every element, otherwise they would be buffered
config.put("bulk.flush.max.actions", "1");

List<TransportAddress> transportAddresses = new ArrayList<String>();
transportAddresses.add(new InetSocketTransportAddress("127.0.0.1", 9300));
transportAddresses.add(new InetSocketTransportAddress("10.2.3.1", 9300));

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
}));
{% endhighlight %}
</div>
<div data-lang="java, Elasticsearch 2.x / 5.x" markdown="1">
{% highlight java %}
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
<div data-lang="scala, Elasticsearch 1.x" markdown="1">
{% highlight scala %}
val input: DataStream[String] = ...

val config = new java.util.HashMap[String, String]
config.put("cluster.name", "my-cluster-name")
// This instructs the sink to emit after every element, otherwise they would be buffered
config.put("bulk.flush.max.actions", "1")

val transportAddresses = new java.util.ArrayList[TransportAddress]
transportAddresses.add(new InetSocketTransportAddress("127.0.0.1", 9300))
transportAddresses.add(new InetSocketTransportAddress("10.2.3.1", 9300))

input.addSink(new ElasticsearchSink(config, transportAddresses, new ElasticsearchSinkFunction[String] {
  def createIndexRequest(element: String): IndexRequest = {
    val json = new java.util.HashMap[String, String]
    json.put("data", element)
    
    return Requests.indexRequest()
            .index("my-index")
            .type("my-type")
            .source(json);
  }
}))
{% endhighlight %}
</div>
<div data-lang="scala, Elasticsearch 2.x / 5.x" markdown="1">
{% highlight scala %}
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
            .source(json);
  }
}))
{% endhighlight %}
</div>
</div>

Note how a `Map` of `String`s is used to configure the `ElasticsearchSink`.
The configuration keys are documented in the Elasticsearch documentation
[here](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html).
Especially important is the `cluster.name` parameter that must correspond to
the name of your cluster.

Also note that the example only demonstrates performing a single index
request for each incoming element. Generally, the `ElasticsearchSinkFunction`
can be used to perform multiple requests of different types (ex.,
`DeleteRequest`, `UpdateRequest`, etc.). 

Internally, the sink uses a `BulkProcessor` to send action requests to the cluster.
This will buffer elements before sending them in bulk to the cluster. The behaviour of the
`BulkProcessor` can be set using these config keys in the provided `Map` configuration:
 * **bulk.flush.max.actions**: Maximum amount of elements to buffer
 * **bulk.flush.max.size.mb**: Maximum amount of data (in megabytes) to buffer
 * **bulk.flush.interval.ms**: Interval at which to flush data regardless of the other two
  settings in milliseconds

#### Communication using Embedded Node (only for Elasticsearch 1.x)

For Elasticsearch versions 1.x, communication using an embedded node is
also supported. See [here](https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/client.html)
for information about the differences between communicating with Elasticsearch
with an embedded node and a `TransportClient`.

Below is an example of how to create an `ElasticsearchSink` use an
embedded node instead of a `TransportClient`:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> input = ...;

Map<String, String> config = new HashMap<>;
// This instructs the sink to emit after every element, otherwise they would be buffered
config.put("bulk.flush.max.actions", "1");
config.put("cluster.name", "my-cluster-name");

input.addSink(new ElasticsearchSink<>(config, new ElasticsearchSinkFunction<String>() {
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
}));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[String] = ...

val config = new java.util.HashMap[String, String]
config.put("bulk.flush.max.actions", "1")
config.put("cluster.name", "my-cluster-name")

input.addSink(new ElasticsearchSink(config, new ElasticsearchSinkFunction[String] {
  def createIndexRequest(element: String): IndexRequest = {
    val json = new java.util.HashMap[String, String]
    json.put("data", element)
    
    return Requests.indexRequest()
            .index("my-index")
            .type("my-type")
            .source(json);
  }
}))
{% endhighlight %}
</div>
</div>

The difference is that now we do not need to provide a list of addresses
of Elasticsearch nodes.

More information about Elasticsearch can be found [here](https://elastic.co).

#### Packaging the Elasticsearch Connector into an Uber-Jar

For the execution of your Flink program, it is recommended to build a
so-called uber-jar (executable jar) containing all your dependencies
(see [here]({{site.baseurl}}/dev/linking.html) for further information).

However, when an uber-jar containing an Elasticsearch sink is executed,
an `IllegalArgumentException` may occur, which is caused by conflicting
files of Elasticsearch and it's dependencies in `META-INF/services`:

```
IllegalArgumentException[An SPI class of type org.apache.lucene.codecs.PostingsFormat with name 'Lucene50' does not exist.  You need to add the corresponding JAR file supporting this SPI to your classpath.  The current classpath supports the following names: [es090, completion090, XBloomFilter]]
```

If the uber-jar is built using Maven, this issue can be avoided by
adding the following to the Maven POM file in the plugins section:

~~~xml
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-shade-plugin</artifactId>
    <version>2.4.3</version>
    <executions>
        <execution>
            <phase>package</phase>
            <goals>
                <goal>shade</goal>
            </goals>
            <configuration>
                <transformers>
                    <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                </transformers>
            </configuration>
        </execution>
    </executions>
</plugin>
~~~
