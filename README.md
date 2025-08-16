# Apache Flink

Apache Flink is an open source stream processing framework with powerful stream- and batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)


### Features

* A streaming-first runtime that supports both batch processing and data streaming programs

* Elegant and fluent APIs in Java

* A runtime that supports very high throughput and low event latency at the same time

* Support for *event time* and *out-of-order* processing in the DataStream API, based on the *Dataflow Model*

* Flexible windowing (time, count, sessions, custom triggers) across different time semantics (event time, processing time)

* Fault-tolerance with *exactly-once* processing guarantees

* Natural back-pressure in streaming programs

* Libraries for Graph processing (batch), Machine Learning (batch), and Complex Event Processing (streaming)

* Custom memory management for efficient and robust switching between in-memory and out-of-core data processing algorithms

* Compatibility layers for Apache Hadoop MapReduce

* Integration with YARN, HDFS, HBase, and other components of the Apache Hadoop ecosystem


### Streaming Example
```java
// pojo class WordWithCount
public class WordWithCount {
    public String word;
    public int count;

    public WordWithCount() {}
    
    public WordWithCount(String word, int count) {
        this.word = word;
        this.count = count;
    }
}

// main method
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStreamSource<String> text = env.socketTextStream(host, port);
DataStream<WordWithCount> windowCounts = text
    .flatMap(
        (FlatMapFunction<String, String>) (line, collector) 
            -> Arrays.stream(line.split("\\s")).forEach(collector::collect)
    ).returns(String.class)
    .map(word -> new WordWithCount(word, 1)).returns(TypeInformation.of(WordWithCount.class))
    .keyBy(wordWithCnt -> wordWithCnt.word)
    .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(5)))
    .sum("count").returns(TypeInformation.of(WordWithCount.class));

windowCounts.print();
env.execute();
}
```

### Batch Example
```java
// pojo class WordWithCount
public class WordWithCount {
    public String word;
    public int count;

    public WordWithCount() {}

    public WordWithCount(String word, int count) {
        this.word = word;
        this.count = count;
    }
}

// main method
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRuntimeMode(RuntimeExecutionMode.BATCH);
FileSource<String> source = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("MyInput.txt")).build();
DataStreamSource<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "MySource");
DataStream<WordWithCount> windowCounts = text
        .flatMap((FlatMapFunction<String, String>) (line, collector) -> Arrays
                .stream(line.split("\\s"))
                .forEach(collector::collect)).returns(String.class)
        .map(word -> new WordWithCount(word, 1)).returns(TypeInformation.of(WordWithCount.class))
        .keyBy(wordWithCount -> wordWithCount.word)
        .sum("count").returns(TypeInformation.of(WordWithCount.class));

windowCounts.print();
env.execute();
```



## Building Apache Flink from Source

Prerequisites for building Flink:

* Unix-like environment (we use Linux, Mac OS X, Cygwin, WSL)
* Git
* Maven (we require version 3.8.6)
* Java (version 11, 17, or 21)

### Basic Build Instructions

First, clone the repository:

```
git clone https://github.com/apache/flink.git
cd flink
```

Then, choose one of the following commands based on your preferred Java version:

**For Java 11**

```
./mvnw clean package -DskipTests -Djdk11 -Pjava11-target
```

**For Java 17 (Default)**

```
./mvnw clean package -DskipTests -Djdk17 -Pjava17-target
```

**For Java 21**

```
./mvnw clean package -DskipTests -Djdk21 -Pjava21-target
```

The build process will take approximately 10 minutes to complete.
Flink will be installed in `build-target`.

### Notes

* Make sure your JAVA_HOME environment variable points to the correct JDK version
* The build command uses Maven wrapper (mvnw) which ensures the correct Maven version is used
* The -DskipTests flag skips running tests to speed up the build process
* Each Java version requires its corresponding profile (-Pjava<version>-target) and JDK flag (-Djdk<version>)

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
* [flink-connector-hive](https://github.com/apache/flink-connector-hive)
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
