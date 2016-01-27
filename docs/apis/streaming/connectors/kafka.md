---
title: "Apache Kafka Connector"

# Sub-level navigation
sub-nav-group: streaming
sub-nav-parent: connectors
sub-nav-pos: 1
sub-nav-title: Kafka
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

This connector provides access to event streams served by [Apache Kafka](https://kafka.apache.org/).

Flink provides special Kafka Connectors for reading and writing data from/to Kafka topics.
The Flink Kafka Consumer integrates with Flink's checkpointing mechanism to provide
exactly-once processing semantics. To achieve that, Flink does not purely rely on Kafka's consumer group
offset tracking, but tracks and checkpoints these offsets internally as well.

Please pick a package (maven artifact id) and class name for your use-case and environment.
For most users, the `FlinkKafkaConsumer08` (part of `flink-connector-kafka`) is appropriate.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left">Maven Dependency</th>
      <th class="text-left">Supported since</th>
      <th class="text-left">Consumer and <br>
      Producer Class name</th>
      <th class="text-left">Kafka version</th>
      <th class="text-left">Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>flink-connector-kafka</td>
        <td>0.9.1, 0.10</td>
        <td>FlinkKafkaConsumer082<br>
        FlinkKafkaProducer</td>
        <td>0.8.x</td>
        <td>Uses the <a href="https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example">SimpleConsumer</a> API of Kafka internally. Offsets are committed to ZK by Flink.</td>
    </tr>
     <tr>
        <td>flink-connector-kafka-0.8{{ site.scala_version_suffix }}</td>
        <td>1.0.0</td>
        <td>FlinkKafkaConsumer08<br>
        FlinkKafkaProducer08</td>
        <td>0.8.x</td>
        <td>Uses the <a href="https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example">SimpleConsumer</a> API of Kafka internally. Offsets are committed to ZK by Flink.</td>
    </tr>
     <tr>
        <td>flink-connector-kafka-0.9{{ site.scala_version_suffix }}</td>
        <td>1.0.0</td>
        <td>FlinkKafkaConsumer09<br>
        FlinkKafkaProducer09</td>
        <td>0.9.x</td>
        <td>Uses the new <a href="http://kafka.apache.org/documentation.html#newconsumerapi">Consumer API</a> Kafka.</td>
    </tr>
  </tbody>
</table>

Then, import the connector in your maven project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kafka-0.8{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary distribution. See how to link with them for cluster execution [here]({{ site.baseurl}}/apis/cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

#### Installing Apache Kafka

* Follow the instructions from [Kafka's quickstart](https://kafka.apache.org/documentation.html#quickstart) to download the code and launch a server (launching a Zookeeper and a Kafka server is required every time before starting the application).
* On 32 bit computers [this](http://stackoverflow.com/questions/22325364/unrecognized-vm-option-usecompressedoops-when-running-kafka-from-my-ubuntu-in) problem may occur.
* If the Kafka and Zookeeper servers are running on a remote machine, then the `advertised.host.name` setting in the `config/server.properties` file must be set to the machine's IP address.

#### Kafka Consumer

Flink's Kafka consumer is called `FlinkKafkaConsumer08` (or `09`). It provides access to one or more Kafka topics.

The constructor accepts the following arguments:

1. The topic name / list of topic names
2. A DeserializationSchema / KeyedDeserializationSchema for deserializing the data from Kafka
3. Properties for the Kafka consumer.
  The following properties are required:
  - "bootstrap.servers" (comma separated list of Kafka brokers)
  - "zookeeper.connect" (comma separated list of Zookeeper servers) (**only required for Kafka 0.8**)
  - "group.id" the id of the consumer group

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
// only required for Kafka 0.8
properties.setProperty("zookeeper.connect", "localhost:2181");
properties.setProperty("group.id", "test");
DataStream<String> stream = env
	.addSource(new FlinkKafkaConsumer08<>("topic", new SimpleStringSchema(), properties))
	.print();
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
// only required for Kafka 0.8
properties.setProperty("zookeeper.connect", "localhost:2181");
properties.setProperty("group.id", "test");
stream = env
    .addSource(new FlinkKafkaConsumer08[String]("topic", new SimpleStringSchema(), properties))
    .print
{% endhighlight %}
</div>
</div>


##### The `DeserializationSchema`

The `FlinkKafkaConsumer08` needs to know how to turn the data in Kafka into Java objects. The 
`DeserializationSchema` allows users to specify such a schema. The `T deserialize(byte[] message)`
method gets called for each Kafka message, passing the value from Kafka.
For accessing both the key and value of the Kafka message, the `KeyedDeserializationSchema` has
the following deserialize method ` T deserialize(byte[] messageKey, byte[] message, String topic, int partition, long offset)`.

For convenience, Flink provides a `TypeInformationSerializationSchema` (and `TypeInformationKeyValueSerializationSchema`) 
which creates a schema based on a Flink `TypeInformation`.

#### Kafka Consumers and Fault Tolerance

With Flink's checkpointing enabled, the Flink Kafka Consumer will consume records from a topic and periodically checkpoint all
its Kafka offsets, together with the state of other operations, in a consistent manner. In case of a job failure, Flink will restore
the streaming program to the state of the latest checkpoint and re-consume the records from Kafka, starting from the offsets that where
stored in the checkpoint.

The interval of drawing checkpoints therefore defines how much the program may have to go back at most, in case of a failure.

To use fault tolerant Kafka Consumers, checkpointing of the topology needs to be enabled at the execution environment:

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

Also note that Flink can only restart the topology if enough processing slots are available to restart the topology.
So if the topology fails due to loss of a TaskManager, there must still be enough slots available afterwards.
Flink on YARN supports automatic restart of lost YARN containers.

If checkpointing is not enabled, the Kafka consumer will periodically commit the offsets to Zookeeper.

#### Kafka Producer

The `FlinkKafkaProducer08` writes data to a Kafka topic. The producer can specify a custom partitioner that assigns
records to partitions.

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
stream.addSink(new FlinkKafkaProducer08<String>("localhost:9092", "my-topic", new SimpleStringSchema()));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
stream.addSink(new FlinkKafkaProducer08[String]("localhost:9092", "my-topic", new SimpleStringSchema()))
{% endhighlight %}
</div>
</div>

You can also define a custom Kafka producer configuration for the KafkaSink with the constructor. Please refer to
the [Apache Kafka documentation](https://kafka.apache.org/documentation.html) for details on how to configure
Kafka Producers.
