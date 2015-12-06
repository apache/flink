# flink-connector-wikiedits

A non-parallel source that parses a live stream of Wikipedia edits.

Meta data about the edits is mirrored to the IRC channel `#en.wikipedia`. The
source establishes a connection to this IRC channel and parses the messages
into `WikipediaEditEvent` instances.

The purpose of this source is to ease the setup of demos of the `DataStream`
API with live data.

The original idea is from the [Hello Samza](https://samza.apache.org/startup/hello-samza/latest/)
project of [Apache Samza](http://samza.apache.org). The Samza code for this is located in the
[samza-hello-samza](https://github.com/apache/samza-hello-samza) repository.

## Example

Add the following dependency to your project:

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-wikiedits</artifactId>
  <version>1.0-SNAPSHOT</version>
</dependency>
```

You can use the source like regular sources:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment
    .getExecutionEnvironment();

DataStream<WikipediaEditEvent> edits = env
    .addSource(new WikipediaEditsSource());
```
Remember that it is *non-parallel* source and as such it will run with
parallelism 1.
