# Apache Flink Streaming

_"Big Data looks tiny from Stratosphere."_

This repository implements stream data processing support for [Apache Flink](http://www.flink.incubator.apache.org) For more information please check ot the [Architecture Sketch](https://github.com/stratosphere/stratosphere-streaming/wiki/Architecture-Sketch).

##  Build From Source

This tutorial shows how to build Apache Flink Streaming on your own system. Please open a bug report if you have any troubles!

### Requirements
* Unix-like environment (We use Linux, Mac OS X, Cygwin)
* git
* Maven (at least version 3.0.4)
* Java 6 or 7

### Get the source & Build it

```
git clone https://github.com/stratosphere/stratosphere-streaming.git
cd stratosphere-streaming
mvn clean assembly:assembly
```

### What to contribute
* Bug reports
* Bug fixes
* Documentation
* Tools that ease the use and development of Apache Flink
* Well-written Apache Flink jobs

Let us know if you have created a system that uses Apache Flink, so that we can link to you.
