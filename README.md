# Apache Flink

Apache Flink is an open source stream processing framework with powerful stream- and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)


### Features

* Unified Processing: Seamlessly switch between real-time streaming and batch processing with Flink's unified API, maximizing      efficiency and simplifying development.

* Expressive & Intuitive APIs: Leverage the power of Java and Scala with Flink's elegant and fluent APIs. Focus on your logic,     not the boilerplate.

* Unmatched Performance: Experience blazing fast throughput and minimal event latency, ensuring your data is processed             efficiently and acted upon instantly.
  
* Event Time Mastery: Conquer the complexities of event-time processing with Flink's robust capabilities and out-of-order event    handling.

* Flexible Windowing: Analyze data at granular levels with Flink's comprehensive windowing options, including time, count,
  sessions, and custom triggers.

* Unwavering Reliability: Flink guarantees fault-tolerant execution and exactly-once processing semantics, delivering rock-solid   reliability for your critical data.

* Natural Back-Pressure Management: Maintain smooth and stable processing with Flink's built-in back-pressure mechanisms,
  preventing data overload.

* Beyond Data Processing: Expand your horizons with Flink's powerful libraries for graph processing, machine learning, and
  complex event processing.

* Iterative Prowess: Tackle complex challenges with Flink's efficient handling of iterative algorithms in batch processing
  through the DataSet API.

* Optimized Memory Management: Flink's custom memory management ensures efficient handling of even the largest datasets,
  seamlessly transitioning between in-memory and out-of-core processing.

* Hadoop Ecosystem Integration: Leverage the power and flexibility of the Hadoop ecosystem with Flink's seamless integration
  with YARN, HDFS, and HBase.


### Streaming Example
```scala
case class WordWithCount(word: String, count: Long)

val text = env.socketTextStream(host, port, '\n')

val windowCounts = text.flatMap { w => w.split("\\s") }
  .map { w => WordWithCount(w, 1) }
  .keyBy("word")
  .window(TumblingProcessingTimeWindow.of(Time.seconds(5)))
  .sum("count")

windowCounts.print()
```

### Batch Example
```scala
case class WordWithCount(word: String, count: Long)

val text = env.readTextFile(path)

val counts = text.flatMap { w => w.split("\\s") }
  .map { w => WordWithCount(w, 1) }
  .groupBy("word")
  .sum("count")

counts.writeAsCsv(outputPath)
```



## Building Apache Flink from Source

Prerequisites for building Flink:

* Unix-like environment (we use Linux, Mac OS X, Cygwin, WSL)
* Git
* Maven (we require version 3.8.6)
* Java 8 or 11 (Java 9 or 10 may work)

```
git clone https://github.com/apache/flink.git
cd flink
./mvnw clean package -DskipTests # this will take up to 10 minutes
```

Flink is now installed in `build-target`.

## Developing Flink

The Flink committers use IntelliJ IDEA to develop the Flink codebase.
We recommend IntelliJ IDEA for developing projects that involve Scala code.

Minimal requirements for an IDE are:
* Support for Java and Scala (also mixed projects)
* Support for Maven with Java and Scala


### IntelliJ IDEA

The IntelliJ IDE supports Maven out of the box and offers a plugin for Scala development.

* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
* IntelliJ Scala Plugin: [https://plugins.jetbrains.com/plugin/?id=1347](https://plugins.jetbrains.com/plugin/?id=1347)

Check out our [Setting up IntelliJ](https://nightlies.apache.org/flink/flink-docs-master/flinkDev/ide_setup.html#intellij-idea) guide for details.

### Eclipse Scala IDE

**NOTE:** From our experience, this setup does not work with Flink
due to deficiencies of the old Eclipse version bundled with Scala IDE 3.0.3 or
due to version incompatibilities with the bundled Scala version in Scala IDE 4.4.1.

**We recommend to use IntelliJ instead (see above)**

## Support

Don’t hesitate to ask!

Contact the developers and community on the [mailing lists](https://flink.apache.org/community.html#mailing-lists) if you need any help.

[Open an issue](https://issues.apache.org/jira/browse/FLINK) if you find a bug in Flink.


## Documentation

The documentation of Apache Flink is located on the website: [https://flink.apache.org](https://flink.apache.org)
or in the `docs/` directory of the source code.


## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it.
Contact us if you are looking for implementation tasks that fit your skills.
This article describes [how to contribute to Apache Flink](https://flink.apache.org/contributing/how-to-contribute.html).

## Externalized Connectors

Most Flink connectors have been externalized to individual repos under the [Apache Software Foundation](https://github.com/apache):

* [flink-connector-aws](https://github.com/apache/flink-connector-aws)
* [flink-connector-cassandra](https://github.com/apache/flink-connector-cassandra)
* [flink-connector-elasticsearch](https://github.com/apache/flink-connector-elasticsearch)
* [flink-connector-gcp-pubsub](https://github.com/apache/flink-connector-gcp-pubsub)
* [flink-connector-hbase](https://github.com/apache/flink-connector-hbase)
* [flink-connector-jdbc](https://github.com/apache/flink-connector-jdbc)
* [flink-connector-kafka](https://github.com/apache/flink-connector-kafka)
* [flink-connector-mongodb](https://github.com/apache/flink-connector-mongodb)
* [flink-connector-opensearch](https://github.com/apache/flink-connector-opensearch)
* [flink-connector-prometheus](https://github.com/apache/flink-connector-prometheus)
* [flink-connector-pulsar](https://github.com/apache/flink-connector-pulsar)
* [flink-connector-rabbitmq](https://github.com/apache/flink-connector-rabbitmq)

## About

Apache Flink is an open source project of The Apache Software Foundation (ASF).
The Apache Flink project originated from the [Stratosphere](http://stratosphere.eu) research project.
