# Apache Flink

Apache Flink is an open source stream processing framework with powerful stream- and batch-processing capabilities.

Learn more about Flink at [http://flink.apache.org/](http://flink.apache.org/)


### Features

* A streaming-first runtime that supports both batch processing and data streaming programs

* Elegant and fluent APIs in Java and Scala

* A runtime that supports very high throughput and low event latency at the same time

* Support for *event time* and *out-of-order* processing in the DataStream API, based on the *Dataflow Model*

* Flexible windowing (time, count, sessions, custom triggers) accross different time semantics (event time, processing time)

* Fault-tolerance with *exactly-once* processing guarantees

* Natural back-pressure in streaming programs.

* Libraries for Graph processing (batch), Machine Learning (batch), and Complex Event Processing (streaming)

* Built-in support for iterative programs (BSP) and in the DataSet (batch) API.

* Custom memory management to for efficient and robust switching between in-memory and out-of-core data processing algorithms.

* Compatibility layers for Apache Hadoop MapReduce and Apache Storm.

* Integration with YARN, HDFS, HBase, and other components of the Apache Hadoop ecosystem.


### Streaming Example
```scala
case class WordWithCount(word: String, count: Long)

val text = env.socketTextStream(host, port, '\n')

val windowCounts = text.flatMap { w => w.split("\\s") }
  .map { w => WordWithCount(w, 1) }
  .keyBy("word")
  .timeWindow(Time.seconds(5))
  .sum("count")

windowCounts.print()
```

### Batch Example
```scala
case class WordWithCount(word: String, count: Long)

val text = env.readTextFile(path)

val counts = text.flatMap { _.split("\\W+") }
  .map { WordWithCount(_, 1) }
  .groupBy("word")
  .sum("count")

counts.writeAsCsv(outputPath)
```



## Building Apache Flink from Source

Prerequisites for building Flink:

* Unix-like environment (We use Linux, Mac OS X, Cygwin)
* git
* Maven (we recommend version 3.0.4)
* Java 7 or 8

```
git clone https://github.com/apache/flink.git
cd flink
mvn clean package -DskipTests # this will take up to 10 minutes
```

Flink is now installed in `build-target`

*NOTE: Maven 3.3.x can build Flink, but will not properly shade away certain dependencies. Maven 3.0.3 creates the libraries properly.*

## Developing Flink

The Flink committers use IntelliJ IDEA and Eclipse IDE to develop the Flink codebase.
We recommend IntelliJ IDEA for developing projects that involve Scala code.

Minimal requirements for an IDE are:
* Support for Java and Scala (also mixed projects)
* Support for Maven with Java and Scala


### IntelliJ IDEA

The IntelliJ IDE supports Maven out of the box and offers a plugin for Scala development.

* IntelliJ download: [https://www.jetbrains.com/idea/](https://www.jetbrains.com/idea/)
* IntelliJ Scala Plugin: [http://plugins.jetbrains.com/plugin/?id=1347](http://plugins.jetbrains.com/plugin/?id=1347)

Check out our [Setting up IntelliJ](https://github.com/apache/flink/blob/master/docs/internals/ide_setup.md#intellij-idea) guide for details.

### Eclipse Scala IDE

For Eclipse users, we recommend using Scala IDE 3.0.3, based on Eclipse Kepler. While this is a slightly older version,
we found it to be the version that works most robustly for a complex project like Flink.

Further details, and a guide to newer Scala IDE versions can be found in the
[How to setup Eclipse](https://github.com/apache/flink/blob/master/docs/internals/ide_setup.md#eclipse) docs.

**Note:** Before following this setup, make sure to run the build from the command line once
(`mvn clean install -DskipTests`, see above)

1. Download the Scala IDE (preferred) or install the plugin to Eclipse Kepler. See 
   [How to setup Eclipse](https://github.com/apache/flink/blob/master/docs/internals/ide_setup.md#eclipse) for download links and instructions.
2. Add the "macroparadise" compiler plugin to the Scala compiler.
   Open "Window" -> "Preferences" -> "Scala" -> "Compiler" -> "Advanced" and put into the "Xplugin" field the path to
   the *macroparadise* jar file (typically "/home/*-your-user-*/.m2/repository/org/scalamacros/paradise_2.10.4/2.0.1/paradise_2.10.4-2.0.1.jar").
   Note: If you do not have the jar file, you probably did not run the command line build.
3. Import the Flink Maven projects ("File" -> "Import" -> "Maven" -> "Existing Maven Projects") 
4. During the import, Eclipse will ask to automatically install additional Maven build helper plugins.
5. Close the "flink-java8" project. Since Eclipse Kepler does not support Java 8, you cannot develop this project.


## Support

Don’t hesitate to ask!

Contact the developers and community on the [mailing lists](http://flink.apache.org/community.html#mailing-lists) if you need any help.

[Open an issue](https://issues.apache.org/jira/browse/FLINK) if you found a bug in Flink.


## Documentation

The documentation of Apache Flink is located on the website: [http://flink.apache.org](http://flink.apache.org)
or in the `docs/` directory of the source code.


## Fork and Contribute

This is an active open-source project. We are always open to people who want to use the system or contribute to it. 
Contact us if you are looking for implementation tasks that fit your skills.
This article describes [how to contribute to Apache Flink](http://flink.apache.org/how-to-contribute.html).


## About

Apache Flink is an open source project of The Apache Software Foundation (ASF).
The Apache Flink project originated from the [Stratosphere](http://stratosphere.eu) research project.

