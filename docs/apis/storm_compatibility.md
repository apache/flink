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

### Project Configuration

Support for Storm is contained in the `flink-storm-compatibility-core` Maven module.
The code resides in the `org.apache.flink.stormcompatibility` package.

Add the following dependency to your `pom.xml` if you want to execute Storm code in Flink.

~~~xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-storm-compatibility-core</artifactId>
	<version>{{site.version}}</version>
</dependency>
~~~

**Please note**: `flink-storm-compatibility-core` is not part of the provided binary Flink distribution. Thus, you need to include `flink-storm-compatiblitly-core` classes (and their dependencies) in your program jar that is submitted to Flink's JobManager.

### Execute Storm Topologies

Flink provides a Storm compatible API (`org.apache.flink.stormcompatibility.api`) that offers replacements for the following classes:

- `TopologyBuilder` replaced by `FlinkTopologyBuilder`
- `StormSubmitter` replaced by `FlinkSubmitter`
- `NimbusClient` and `Client` replaced by `FlinkClient`
- `LocalCluster` replaced by `FlinkLocalCluster`

In order to submit a Storm topology to Flink, it is sufficient to replace the used Storm classed with their Flink replacements in the original Storm client code that assembles the topology.
If a topology is executed in a remote cluster, parameters `nimbus.host` and `nimbus.thrift.port` are used as `jobmanger.rpc.address` and `jobmanger.rpc.port`, respectively.
If a parameter is not specified, the value is taken from `flink-conf.yaml`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
~~~java
FlinkTopologyBuilder builder = new FlinkTopologyBuilder(); // replaces: TopologyBuilder builder = new FlinkTopology();

builder.setSpout("source", new StormFileSpout(inputFilePath));
builder.setBolt("tokenizer", new StormBoltTokenizer()).shuffleGrouping("source");
builder.setBolt("counter", new StormBoltCounter()).fieldsGrouping("tokenizer", new Fields("word"));
builder.setBolt("sink", new StormBoltFileSink(outputFilePath)).shuffleGrouping("counter");

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

### Embed Storm Operators in Flink Streaming Programs 

As an alternative, Spouts and Bolts can be embedded into regular streaming programs.
The Storm compatibility layer offers a wrapper classes for each, namely `StormSpoutWrapper` and `StormBoltWrapper` (`org.apache.flink.stormcompatibility.wrappers`).

Per default, both wrappers convert Storm output tuples to Flink's `Tuple` types (ie, `Tuple1` to `Tuple25` according to the number of fields of the Storm tuples).
For single field output tuples a conversion to the field's data type is also possible (eg, `String` instead of `Tuple1<String>`).

Because Flink cannot infer the output field types of Storm operators, it is required to specify the output type manually.
In order to get the correct `TypeInformation` object, Flink's `TypeExtractor` can be used.

#### Embed Spouts

In order to use a Spout as Flink source, use `StreamExecutionEnvironment.addSource(SourceFunction, TypeInformation)`.
The Spout object is handed to the constructor of `StormSpoutWrapper<OUT>` that serves as first argument to `addSource(...)`.
The generic type declaration `OUT` specifies the type of the source output stream.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
~~~java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// stream has `raw` type (single field output streams only)
DataStream<String> rawInput = env.addSource(
	new StormSpoutWrapper<String>(new StormFileSpout(localFilePath), true), // Spout source, 'true' for raw type
	TypeExtractor.getForClass(String.class)); // output type

// process data stream
[...]
~~~
</div>
</div>

If a Spout emits a finite number of tuples, `StormFiniteSpoutWrapper` can be used instead of `StormSpoutWrapper`.
Using `StormFiniteSpoutWrapper` allows the Flink program to shut down automatically after all data is processed.
If `StormSpoutWrapper` is used, the program will run until it is [canceled](cli.html) manually.


#### Embed Bolts

In order to use a Bolt as Flink operator, use `DataStream.transform(String, TypeInformation, OneInputStreamOperator)`.
The Bolt object is handed to the constructor of `StormBoltWrapper<IN,OUT>` that serves as last argument to `transform(...)`.
The generic type declarations `IN` and `OUT` specify the type of the operator's input and output stream, respectively.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
~~~java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
DataStream<String> text = env.readTextFile(localFilePath);

DataStream<Tuple2<String, Integer>> counts = text.transform(
	"tokenizer", // operator name
	TypeExtractor.getForObject(new Tuple2<String, Integer>("", 0)), // output type
	new StormBoltWrapper<String, Tuple2<String, Integer>>(new StormBoltTokenizer())); // Bolt operator

// do further processing
[...]
~~~
</div>
</div>

### Storm Compatibility Examples

You can find more examples in Maven module `flink-storm-compatibilty-examples`.

