# Custom Apache Flink with Observations Logging for PlanGeneratorFlink

This is a modified fork of Apache Flink that is used for research projects in Distributed Stream
Processing.

The changes are mainly regarding custom logging of observations of operators regarding properties
such as tupleWidthIn, tupleWidthOut, selectivity, inputRate, outputRate and others. Logging can be
done to local files or to a centralized MongoDB database.

## Prerequisites and Configuration

Both methods require certain parameters to be used. These parameters are set via global job
parameters in PlanGeneratorFlink.

- For cluster execution:
    - A MongoDB service running to save logs in a centralized way.
    - The parameters `distributedLogging`, `mongoAddress`, `mongoPort`, `mongoDatabase`
      , `mongoUsername` and `mongoPassword` are required.
    - The MongoDB service needs to have a user authentication for the database set up.
    - For a local set up you can use `docker-compose` and start a `mongoDB` service in the
      folder `mongoDBLocal` in `plangeneratorflink-management`. Note that you maybe need to adapt
      the `.env` and `mongo-init.js` files with your credentials.
    - For a kubernetes setup the address and port of mongoDB gets automatically determined by
      extracting the IP of the service `mongodb`.
- For local execution:
    - Logs are stored to disk (see `observationLogDir` parameter in PlanGeneratorFlink)
    - Local execution has only been tested with a linux machine and won`t probably run on Windows.
      But you can use windows subsystem for linux (wsl) without problems.

## Modifications

- `flink-dist/src/main/flink-bin/conf/log4j.properties` Added observation logger
- `flink-streaming-java/pom.xml` Added `org.mongodb`, `com.googlecode.json-simple` dependencies
- `flink-streaming-java/src/main/java/org/apache/flink/streaming/api/datastream/AllWindowedStream.java`
  Changed `aggregate()` method to be able to include an operator description
- `flink-streaming-java/src/main/java/org/apache/flink/streaming/api/datastream/DataStream.java`
  Changed `filter()` method to be able to include an operator description
- `flink-streaming-java/src/main/java/org/apache/flink/streaming/api/datastream/JoinedStreams.java`
  Changed `apply()` method to be able to include an operator description. Changed
  also `JoinCoGroupFunction` class to create a StreamMonitor and log observations of joins
  when `coGroup()` gets called.
- `flink-streaming-java/src/main/java/org/apache/flink/streaming/api/datastream/WindowedStream.java`
  Changed `aggregate()` method to be able to include an operator description.
- `flink-streaming-java/src/main/java/org/apache/flink/streaming/api/operators/StreamFilter.java`
  Changed `StreamFilter` class to create a StreamMonitor and log observations of filters
  when `processElement()` gets called.
- `flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/windowing/WindowOperator.java`
  Changed `WindowOperator` class to create a StreamMonitor and log observations of windows
  when `processElement()` gets called.
- `flink-streaming-java/src/main/java/org/apache/flink/streaming/runtime/operators/windowing/WindowOperatorBuilder.java`
  Changed to support operator description.
- `flink-streaming-java/src/main/java/org/apache/flink/streaming/api/operators/StreamMonitor.java`
  New class to handle observation logging to local file or centralized mongoDB database.
- `web-dashboard/src/app/app.component.html` To visualize that you are running a custom flink build,
  a reference on the top right in the web frontend is embedded

## Build & Execution

### Build

- The recommended way to build this custom flink from source is to use
  the `plangeneratorflink-management` scripts (`setupPlanGeneratorFlink.sh` or directly `build.sh`)
- alternatively you can build the source from ground up using maven by
  calling `mvn clean install -DskipTests`
- By running `mvn clean install -DskipTests -P docs-and-source -pl flink-streaming-java,flink-dist`
  only the relevant `flink-streaming-java` modul will be build again. That reduces the time to
  build.
- The generated build can be found in the folder `build-target`.

### Execution
- In case of `distributedLogging` be sure, that mongoDB has been started.
- Run `./bin/start-cluster.sh` in the build folder to start a local cluster.


---

# Apache Flink

Apache Flink is an open source stream processing framework with powerful stream- and
batch-processing capabilities.

Learn more about Flink at [https://flink.apache.org/](https://flink.apache.org/)

### Features

* A streaming-first runtime that supports both batch processing and data streaming programs

* Elegant and fluent APIs in Java and Scala

* A runtime that supports very high throughput and low event latency at the same time

* Support for *event time* and *out-of-order* processing in the DataStream API, based on the *
  Dataflow Model*

* Flexible windowing (time, count, sessions, custom triggers) across different time semantics (event
  time, processing time)

* Fault-tolerance with *exactly-once* processing guarantees

* Natural back-pressure in streaming programs

* Libraries for Graph processing (batch), Machine Learning (batch), and Complex Event Processing (
  streaming)

* Built-in support for iterative programs (BSP) in the DataSet (batch) API

* Custom memory management for efficient and robust switching between in-memory and out-of-core data
  processing algorithms

* Compatibility layers for Apache Hadoop MapReduce

* Integration with YARN, HDFS, HBase, and other components of the Apache Hadoop ecosystem

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
* Maven (we recommend version 3.2.5 and require at least 3.1.1)
* Java 8 or 11 (Java 9 or 10 may work)

```
git clone https://github.com/apache/flink.git
cd flink
mvn clean package -DskipTests # this will take up to 10 minutes
```

Flink is now installed in `build-target`.

*NOTE: Maven 3.3.x can build Flink, but will not properly shade away certain dependencies. Maven
3.1.1 creates the libraries properly. To build unit tests with Java 8, use Java 8u51 or above to
prevent failures in unit tests that use the PowerMock runner.*

## Developing Flink

The Flink committers use IntelliJ IDEA to develop the Flink codebase. We recommend IntelliJ IDEA for
developing projects that involve Scala code.

Minimal requirements for an IDE are:

* Support for Java and Scala (also mixed projects)
* Support for Maven with Java and Scala

### IntelliJ IDEA

The IntelliJ IDE supports Maven out of the box and offers a plugin for Scala development.

* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
* IntelliJ Scala
  Plugin: [https://plugins.jetbrains.com/plugin/?id=1347](https://plugins.jetbrains.com/plugin/?id=1347)

Check out
our [Setting up IntelliJ](https://nightlies.apache.org/flink/flink-docs-master/flinkDev/ide_setup.html#intellij-idea)
guide for details.

### Eclipse Scala IDE

**NOTE:** From our experience, this setup does not work with Flink due to deficiencies of the old
Eclipse version bundled with Scala IDE 3.0.3 or due to version incompatibilities with the bundled
Scala version in Scala IDE 4.4.1.

**We recommend to use IntelliJ instead (see above)**

## Support

Don’t hesitate to ask!

Contact the developers and community on
the [mailing lists](https://flink.apache.org/community.html#mailing-lists) if you need any help.

[Open an issue](https://issues.apache.org/jira/browse/FLINK) if you found a bug in Flink.

## Documentation

The documentation of Apache Flink is located on the
website: [https://flink.apache.org](https://flink.apache.org)
or in the `docs/` directory of the source code.

## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or
contribute to it. Contact us if you are looking for implementation tasks that fit your skills. This
article
describes [how to contribute to Apache Flink](https://flink.apache.org/contributing/how-to-contribute.html)
.

## About

Apache Flink is an open source project of The Apache Software Foundation (ASF). The Apache Flink
project originated from the [Stratosphere](http://stratosphere.eu) research project.
