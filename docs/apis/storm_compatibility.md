---
title: "Storm Compatibility"
is_beta: true
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

[Flink streaming](streaming_guide.html) is compatible with Apache Storm interfaces and therefore allows
reusing code that was implemented for Storm.

You can:

- execute a whole Storm `Topology` in Flink.
- use Storm `Spout`/`Bolt` as source/operator in Flink streaming programs.

This document shows how to use existing Storm code with Flink.

* This will be replaced by the TOC
{:toc}

# Project Configuration

Support for Storm is contained in the `flink-storm` Maven module.
The code resides in the `org.apache.flink.storm` package.

Add the following dependency to your `pom.xml` if you want to execute Storm code in Flink.

~~~xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-storm</artifactId>
	<version>{{site.version}}</version>
</dependency>
~~~

**Please note**: `flink-storm` is not part of the provided binary Flink distribution.
Thus, you need to include `flink-storm` classes (and their dependencies) in your program jar that is submitted to Flink's JobManager.
See *WordCount Storm* within `flink-storm-examples/pom.xml` for an example how to package a jar correctly.

# Execute Storm Topologies

Flink provides a Storm compatible API (`org.apache.flink.storm.api`) that offers replacements for the following classes:

- `TopologyBuilder` replaced by `FlinkTopologyBuilder`
- `StormSubmitter` replaced by `FlinkSubmitter`
- `NimbusClient` and `Client` replaced by `FlinkClient`
- `LocalCluster` replaced by `FlinkLocalCluster`

In order to submit a Storm topology to Flink, it is sufficient to replace the used Storm classes with their Flink replacements in the Storm *client code that assembles* the topology.
The actual runtime code, ie, Spouts and Bolts, can be uses *unmodified*.
If a topology is executed in a remote cluster, parameters `nimbus.host` and `nimbus.thrift.port` are used as `jobmanger.rpc.address` and `jobmanger.rpc.port`, respectively.
If a parameter is not specified, the value is taken from `flink-conf.yaml`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
~~~java
FlinkTopologyBuilder builder = new FlinkTopologyBuilder(); // replaces: TopologyBuilder builder = new FlinkTopology();

// actual topology assembling code and used Spouts/Bolts can be used as-is
builder.setSpout("source", new FileSpout(inputFilePath));
builder.setBolt("tokenizer", new BoltTokenizer()).shuffleGrouping("source");
builder.setBolt("counter", new BoltCounter()).fieldsGrouping("tokenizer", new Fields("word"));
builder.setBolt("sink", new BoltFileSink(outputFilePath)).shuffleGrouping("counter");

Config conf = new Config();
if(runLocal) { // submit to test cluster
	FlinkLocalCluster cluster = new FlinkLocalCluster(); // replaces: LocalCluster cluster = new LocalCluster();
	cluster.submitTopology("WordCount", conf, builder.createTopology());
} else { // submit to remote cluster
	// optional
	// conf.put(Config.NIMBUS_HOST, "remoteHost");
	// conf.put(Config.NIMBUS_THRIFT_PORT, 6123);
	FlinkSubmitter.submitTopology("WordCount", conf, builder.createTopology()); // replaces: StormSubmitter.submitTopology(topologyId, conf, builder.createTopology());
}
~~~
</div>
</div>

# Embed Storm Operators in Flink Streaming Programs 

As an alternative, Spouts and Bolts can be embedded into regular streaming programs.
The Storm compatibility layer offers a wrapper classes for each, namely `SpoutWrapper` and `BoltWrapper` (`org.apache.flink.storm.wrappers`).

Per default, both wrappers convert Storm output tuples to Flink's [Tuple](programming_guide.html#tuples-and-case-classes) types (ie, `Tuple0` to `Tuple25` according to the number of fields of the Storm tuples).
For single field output tuples a conversion to the field's data type is also possible (eg, `String` instead of `Tuple1<String>`).

Because Flink cannot infer the output field types of Storm operators, it is required to specify the output type manually.
In order to get the correct `TypeInformation` object, Flink's `TypeExtractor` can be used.

## Embed Spouts

In order to use a Spout as Flink source, use `StreamExecutionEnvironment.addSource(SourceFunction, TypeInformation)`.
The Spout object is handed to the constructor of `SpoutWrapper<OUT>` that serves as first argument to `addSource(...)`.
The generic type declaration `OUT` specifies the type of the source output stream.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
~~~java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// stream has `raw` type (single field output streams only)
DataStream<String> rawInput = env.addSource(
	new SpoutWrapper<String>(new FileSpout(localFilePath), new String[] { Utils.DEFAULT_STREAM_ID }), // emit default output stream as raw type
	TypeExtractor.getForClass(String.class)); // output type

// process data stream
[...]
~~~
</div>
</div>

If a Spout emits a finite number of tuples, `SpoutWrapper` can be configures to terminate automatically by setting `numberOfInvocations` parameter in its constructor.
This allows the Flink program to shut down automatically after all data is processed.
Per default the program will run until it is [canceled](cli.html) manually.


## Embed Bolts

In order to use a Bolt as Flink operator, use `DataStream.transform(String, TypeInformation, OneInputStreamOperator)`.
The Bolt object is handed to the constructor of `BoltWrapper<IN,OUT>` that serves as last argument to `transform(...)`.
The generic type declarations `IN` and `OUT` specify the type of the operator's input and output stream, respectively.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
~~~java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<String> text = env.readTextFile(localFilePath);

DataStream<Tuple2<String, Integer>> counts = text.transform(
	"tokenizer", // operator name
	TypeExtractor.getForObject(new Tuple2<String, Integer>("", 0)), // output type
	new BoltWrapper<String, Tuple2<String, Integer>>(new BoltTokenizer())); // Bolt operator

// do further processing
[...]
~~~
</div>
</div>

### Named Attribute Access for Embedded Bolts

Bolts can accesses input tuple fields via name (additionally to access via index).
To use this feature with embedded Bolts, you need to have either a

 1. [POJO](programming_guide.html#pojos) type input stream or
 2. [Tuple](programming_guide.html#tuples-and-case-classes) type input stream and spedify the input schema (ie, name-to-index-mapping)

For POJO input types, Flink accesses the fields via reflection.
For this case, Flink expects either a corresponding public member variable or public getter method.
For example, if a Bolt accesses a field via name `sentence` (eg, `String s = input.getStringByField("sentence");`), the input POJO class must have a member variable `public String sentence;` or method `public String getSentence() { ... };` (pay attention to camel-case naming).

For `Tuple` input types, it is required to specify the input schema using Storm's `Fields` class.
For this case, the constructor of `BoltWrapper` takes an additional argument: `new BoltWrapper<Tuple1<String>, ...>(..., new Fields("sentence"))`.
The input type is `Tuple1<String>` and `Fields("sentence")` specify that `input.getStringByField("sentence")` is equivalent to `input.getString(0)`.

See [BoltTokenizerWordCountPojo](https://github.com/apache/flink/tree/master/flink-contrib/flink-storm-examples/src/main/java/org/apache/flink/storm/wordcount/BoltTokenizerWordCountPojo.java) and [BoltTokenizerWordCountWithNames](https://github.com/apache/flink/tree/master/flink-contrib/flink-storm-examples/src/main/java/org/apache/flink/storm/wordcount/BoltTokenizerWordCountWithNames.java) for examples.  

## Configuring Spouts and Bolts

In Storm, Spouts and Bolts can be configured with a globally distributed `Map` object that is given to `submitTopology(...)` method of `LocalCluster` or `StormSubmitter`.
This `Map` is provided by the user next to the topology and gets forwarded as a parameter to the calls `Spout.open(...)` and `Bolt.prepare(...)`.
If a whole topology is executed in Flink using `FlinkTopologyBuilder` etc., there is no special attention required &ndash; it works as in regular Storm.

For embedded usage, Flink's configuration mechanism must be used.
A global configuration can be set in a `StreamExecutionEnvironment` via `.getConfig().setGlobalJobParameters(...)`.
Flink's regular `Configuration` class can be used to configure Spouts and Bolts.
However, `Configuration` does not support arbitrary key data types as Storm does (only `String` keys are allowed).
Thus, Flink additionally provides `StormConfig` class that can be used like a raw `Map` to provide full compatibility to Storm.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
~~~java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

StormConfig config = new StormConfig();
// set config values
[...]

// set global Storm configuration
env.getConfig().setGlobalJobParameters(config);

// assemble program with embedded Spouts and/or Bolts
[...]
~~~
</div>
</div>

## Multiple Output Streams

Flink can also handle the declaration of multiple output streams for Spouts and Bolts.
If a whole topology is executed in Flink using `FlinkTopologyBuilder` etc., there is no special attention required &ndash; it works as in regular Storm.

For embedded usage, the output stream will be of data type `SplitStreamType<T>` and must be split by using `DataStream.split(...)` and `SplitStream.select(...)`.
Flink provides the predefined output selector `StormStreamSelector<T>` for `.split(...)` already.
Furthermore, the wrapper type `SplitStreamTuple<T>` can be removed using `SplitStreamMapper<T>`.
If a data stream of type `SplitStreamTuple<T>` is used as input for a Bolt, it is **not** required to strip the wrapper &ndash; `BoltWrapper` removes it automatically.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
~~~java
[...]

// get DataStream from Spout or Bolt which declares two output streams s1 and s2 with output type SomeType
DataStream<SplitStreamType<SomeType>> multiStream = ...

SplitStream<SplitStreamType<SomeType>> splitStream = multiStream.split(new StormStreamSelector<SomeType>());

// remove SplitStreamMapper to get data stream of type SomeType
DataStream<SomeType> s1 = splitStream.select("s1").map(new SplitStreamMapper<SomeType>).returns(SomeType.classs);
// apply Bolt directly, without stripping SplitStreamType
DataStream<BoltOutputType> s2 = splitStream.select("s2").transform(/* use Bolt for further processing */);

// do further processing on s1 and s2
[...]
~~~
</div>
</div>

See [SpoutSplitExample.java](https://github.com/apache/flink/tree/master/flink-contrib/flink-storm-examples/src/main/java/org/apache/flink/storm/split/SpoutSplitExample.java) for a full example.

# Flink Extensions

## Finite Spouts

In Flink, streaming sources can be finite, ie, emit a finite number of records and stop after emitting the last record. However, Spouts usually emit infinite streams.
The bridge between the two approaches is the `FiniteSpout` interface which, in addition to `IRichSpout`, contains a `reachedEnd()` method, where the user can specify a stopping-condition.
The user can create a finite Spout by implementing this interface instead of (or additionally to) `IRichSpout`, and implementing the `reachedEnd()` method in addition.
In contrast to a `SpoutWrapper` that is configured to emit a finite number of tuples, `FiniteSpout` interface allows to implement more complex termination criteria.

Although finite Spouts are not necessary to embed Spouts into a Flink streaming program or to submit a whole Storm topology to Flink, there are cases where they may come in handy:

 * to achieve that a native Spout behaves the same way as a finite Flink source with minimal modifications
 * the user wants to process a stream only for some time; after that, the Spout can stop automatically
 * reading a file into a stream
 * for testing purposes

An example of a finite Spout that emits records for 10 seconds only:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
~~~java
public class TimedFiniteSpout extends BaseRichSpout implements FiniteSpout {
	[...] // implemente open(), nextTuple(), ...

	private long starttime = System.currentTimeMillis();

	public boolean reachedEnd() {
		return System.currentTimeMillis() - starttime > 10000l;
	}
}
~~~
</div>
</div>

# Storm Compatibility Examples

You can find more examples in Maven module `flink-storm-examples`.
For the different versions of WordCount, see [README.md](https://github.com/apache/flink/tree/master/flink-contrib/flink-storm-examples/README.md).
To run the examples, you need to assemble a correct jar file.
`flink-storm-examples-0.10-SNAPSHOT.jar` is **no** valid jar file for job execution (it is only a standard maven artifact).

There are example jars for embedded Spout and Bolt, namely `WordCount-SpoutSource.jar` and `WordCount-BoltTokenizer.jar`, respectively.
Compare `pom.xml` to see how both jars are built.
Furthermore, there is one example for whole Storm topologies (`WordCount-StormTopology.jar`).

You can run each of those examples via `bin/flink run <jarname>.jar`. The correct entry point class is contained in each jar's manifest file.
