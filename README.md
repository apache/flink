# Apache Flink

Apache Flink is an open source system for fast and versatile data analytics in clusters. Flink supports batch and streaming analytics,
in one system. Analytical programs can be written in concise and elegant APIs in Java and Scala.

```scala
case class WordWithCount(word: String, count: Int)

val text = env.readTextFile(path)

val counts = text.flatMap { _.split("\\W+") }
  .map { WordWithCount(_, 1) }
  .groupBy("word")
  .sum("count")

counts.writeAsCsv(outputPath)
```

Flink is highlighted by some unique features:

* Hybrid batch/streaming runtime that supports batch processing and data streaming programs.
* Custom memory management to guarantee efficient, adaptive, and highly robust switching between in-memory and data processing out-of-core algorithms.
* Flexible and expressive windowing semantics for data stream programs
* Built-in program optimizer that chooses the proper runtime operations for each program
* Custom type analysis and serialization stack for high performance


Learn more about Flink at [http://flink.apache.org/](http://flink.apache.org/)


## Building Apache Flink from Source

* Unix-like environment (We use Linux, Mac OS X, Cygwin)
* git
* Maven (at least version 3.0.4)
* Java 6, 7 or 8 (Note that Oracle's JDK 6 library will fail to build Flink, but is able to run a pre-compiled package without problem)

```
git clone https://github.com/apache/flink.git
cd flink
mvn clean package -DskipTests # this will take up to 5 minutes
```

Flink is now installed in `build-target`


## Developing Flink

The Flink committers use the IntelliJ IDE and Eclipse to develop the Flink codebase.

Minimal requirements for an IDE are:
* Support for Java and Scala (also mixed projects)
* Support for Maven with Java and Scala


### IntelliJ

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
(`mvn clean package -DskipTests`, see above)

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

Please contact the developers on our [mailing lists](http://flink.apache.org/community.html#mailing-lists) if you need help.

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

