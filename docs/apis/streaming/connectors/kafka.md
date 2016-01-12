---
title: "Apache Kafka Connector"

# Sub-level navigation
sub-nav-group: streaming
sub-nav-parent: connectors
sub-nav-pos: 1
sub-nav-title: Kafka
---

This connector provides access to event streams served by [Apache Kafka](https://kafka.apache.org/).

Flink provides special Kafka Connectors for reading and writing data from/to Kafka topics.
The Flink Kafka Consumer integrates with Flink's checkpointing mechanism to provide
exactly-once processing semantics. To achieve that, Flink does not purely rely on Kafka's consumer group
offset tracking, but tracks and checkpoints these offsets internally as well.

Please pick a package (maven artifact id) and class name for your use-case and environment.
For most users, the `FlinkKafkaConsumer082` (part of `flink-connector-kafka`) is appropriate.

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left">Maven Dependency</th>
      <th class="text-left">Supported since</th>
      <th class="text-left">Class name</th>
      <th class="text-left">Kafka version</th>
      <th class="text-left">Notes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>flink-connector-kafka</td>
        <td>0.9.1, 0.10</td>
        <td>FlinkKafkaConsumer081</td>
        <td>0.8.1</td>
        <td>Uses the <a href="https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example">SimpleConsumer</a> API of Kafka internally. Offsets are committed to ZK by Flink.</td>
    </tr>
    <tr>
        <td>flink-connector-kafka</td>
        <td>0.9.1, 0.10</td>
        <td>FlinkKafkaConsumer082</td>
        <td>0.8.2</td>
        <td>Uses the <a href="https://cwiki.apache.org/confluence/display/KAFKA/0.8.0+SimpleConsumer+Example">SimpleConsumer</a> API of Kafka internally. Offsets are committed to ZK by Flink.</td>
    </tr>
  </tbody>
</table>

Then, import the connector in your maven project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kafka</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary distribution. See how to link with them for cluster execution [here](cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution).

#### Installing Apache Kafka

* Follow the instructions from [Kafka's quickstart](https://kafka.apache.org/documentation.html#quickstart) to download the code and launch a server (launching a Zookeeper and a Kafka server is required every time before starting the application).
* On 32 bit computers [this](http://stackoverflow.com/questions/22325364/unrecognized-vm-option-usecompressedoops-when-running-kafka-from-my-ubuntu-in) problem may occur.
* If the Kafka and Zookeeper servers are running on a remote machine, then the `advertised.host.name` setting in the `config/server.properties` file must be set to the machine's IP address.

#### Kafka Consumer

The standard `FlinkKafkaConsumer082` is a Kafka consumer providing access to one topic. It takes the following parameters to the constructor:

1. The topic name
2. A DeserializationSchema
3. Properties for the Kafka consumer.
  The following properties are required:
  - "bootstrap.servers" (comma separated list of Kafka brokers)
  - "zookeeper.connect" (comma separated list of Zookeeper servers)
  - "group.id" the id of the consumer group

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("zookeeper.connect", "localhost:2181");
properties.setProperty("group.id", "test");
DataStream<String> stream = env
	.addSource(new FlinkKafkaConsumer082<>("topic", new SimpleStringSchema(), properties))
	.print();
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("zookeeper.connect", "localhost:2181");
properties.setProperty("group.id", "test");
stream = env
    .addSource(new FlinkKafkaConsumer082[String]("topic", new SimpleStringSchema(), properties))
    .print
{% endhighlight %}
</div>
</div>

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

The `FlinkKafkaProducer` writes data to a Kafka topic. The producer can specify a custom partitioner that assigns
recors to partitions.

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
stream.addSink(new FlinkKafkaProducer<String>("localhost:9092", "my-topic", new SimpleStringSchema()));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
stream.addSink(new FlinkKafkaProducer[String]("localhost:9092", "my-topic", new SimpleStringSchema()))
{% endhighlight %}
</div>
</div>

You can also define a custom Kafka producer configuration for the KafkaSink with the constructor. Please refer to
the [Apache Kafka documentation](https://kafka.apache.org/documentation.html) for details on how to configure
Kafka Producers.