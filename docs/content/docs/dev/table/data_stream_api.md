---
title: "DataStream API Integration"
weight: 3
type: docs
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

# DataStream API Integration

Both Table API and DataStream API are equally important when it comes to defining a data
processing pipeline.

The DataStream API offers the primitives of stream processing (namely time, state, and dataflow
management) in a relatively low-level imperative programming API. The Table API abstracts away many
internals and provides a structured and declarative API.

Both APIs can work with bounded *and* unbounded streams.

Bounded streams need to be managed when processing historical data. Unbounded streams occur
in real-time processing scenarios that might be initialized with historical data first.

For efficient execution, both APIs offer processing bounded streams in an optimized batch execution
mode. However, since batch is just a special case of streaming, it is also possible to run pipelines
of bounded streams in regular streaming execution mode.

Pipelines in one API can be defined end-to-end without dependencies on the other API. However, it
might be useful to mix both APIs for various reasons:

- Use the table ecosystem for accessing catalogs or connecting to external systems easily, before
implementing the main pipeline in DataStream API.
- Access some of the SQL functions for stateless data normalization and cleansing, before
implementing the main pipeline in DataStream API.
- Switch to DataStream API every now and then if a more low-level operation (e.g. custom timer
handling) is not present in Table API.

Flink provides special bridging functionalities to make the integration with DataStream API as smooth
as possible.

{{< hint info >}}
Switching between DataStream and Table API adds some conversion overhead. For example, internal data
structures of the table runtime (i.e. `RowData`) that partially work on binary data need to be converted
to more user-friendly data structures (i.e. `Row`). Usually, this overhead can be neglected but is
mentioned here for completeness.
{{< /hint >}}

{{< top >}}

Converting between DataStream and Table
---------------------------------------

Flink provides a specialized `StreamTableEnvironment` for integrating with the
DataStream API. Those environments extend the regular `TableEnvironment` with additional methods
and take the `StreamExecutionEnvironment` used in the DataStream API as a parameter.

The following code shows an example of how to go back and forth between the two APIs. Column names
and types of the `Table` are automatically derived from the `TypeInformation` of the `DataStream`.
Since the DataStream API does not support changelog processing natively, the code assumes
append-only/insert-only semantics during the stream-to-table and table-to-stream conversion.

{{< tabs "6ec84aa4-d91d-4c47-9fa2-b1aae1e3cdb5" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

// create environments of both APIs
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// create a DataStream
DataStream<String> dataStream = env.fromElements("Alice", "Bob", "John");

// interpret the insert-only DataStream as a Table
Table inputTable = tableEnv.fromDataStream(dataStream);

// register the Table object as a view and query it
tableEnv.createTemporaryView("InputTable", inputTable);
Table resultTable = tableEnv.sqlQuery("SELECT UPPER(f0) FROM InputTable");

// interpret the insert-only Table as a DataStream again
DataStream<Row> resultStream = tableEnv.toDataStream(resultTable);

// add a printing sink and execute in DataStream API
resultStream.print();
env.execute();

// prints:
// +I[Alice]
// +I[Bob]
// +I[John]
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

// create environments of both APIs
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// create a DataStream
val dataStream = env.fromElements("Alice", "Bob", "John")

// interpret the insert-only DataStream as a Table
val inputTable = tableEnv.fromDataStream(dataStream)

// register the Table object as a view and query it
tableEnv.createTemporaryView("InputTable", inputTable)
val resultTable = tableEnv.sqlQuery("SELECT UPPER(f0) FROM InputTable")

// interpret the insert-only Table as a DataStream again
val resultStream = tableEnv.toDataStream(resultTable)

// add a printing sink and execute in DataStream API
resultStream.print()
env.execute()

// prints:
// +I[Alice]
// +I[Bob]
// +I[John]
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.common.typeinfo import Types

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# create a DataStream
ds = env.from_collection(["Alice", "Bob", "John"], Types.STRING())

# interpret the insert-only DataStream as a Table
t = t_env.from_data_stream(ds)

# register the Table object as a view and query it
t_env.create_temporary_view("InputTable", t)
res_table = t_env.sql_query("SELECT UPPER(f0) FROM InputTable")

# interpret the insert-only Table as a DataStream again
res_ds = t_env.to_data_stream(res_table)

# add a printing sink and execute in DataStream API
res_ds.print()

env.execute()

# prints:
# +I[Alice]
# +I[Bob]
# +I[John]
```
{{< /tab >}}
{{< /tabs >}}

The complete semantics of `fromDataStream` and `toDataStream` can be found in the [dedicated section below](#handling-of-insert-only-streams).
In particular, the section discusses how to influence the schema derivation with more complex
and nested types. It also covers working with event-time and watermarks.

Depending on the kind of query, in many cases the resulting dynamic table is a pipeline that does not
only produce insert-only changes when converting the `Table` to a `DataStream` but also produces retractions
and other kinds of updates. During table-to-stream conversion, this could lead to an exception similar to

```
Table sink 'Unregistered_DataStream_Sink_1' doesn't support consuming update changes [...].
```

in which case one needs to revise the query again or switch to `toChangelogStream`.

The following example shows how updating tables can be converted. Every result row represents
an entry in a changelog with a change flag that can be queried by calling `row.getKind()` on it. In
the example, the second score for `Alice` creates an _update before_ (`-U`) and _update after_ (`+U`)
change.

{{< tabs "f45d1374-61a0-40c0-9280-702ed87d2ed0" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

// create environments of both APIs
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// create a DataStream
DataStream<Row> dataStream = env.fromElements(
    Row.of("Alice", 12),
    Row.of("Bob", 10),
    Row.of("Alice", 100));

// interpret the insert-only DataStream as a Table
Table inputTable = tableEnv.fromDataStream(dataStream).as("name", "score");

// register the Table object as a view and query it
// the query contains an aggregation that produces updates
tableEnv.createTemporaryView("InputTable", inputTable);
Table resultTable = tableEnv.sqlQuery(
    "SELECT name, SUM(score) FROM InputTable GROUP BY name");

// interpret the updating Table as a changelog DataStream
DataStream<Row> resultStream = tableEnv.toChangelogStream(resultTable);

// add a printing sink and execute in DataStream API
resultStream.print();
env.execute();

// prints:
// +I[Alice, 12]
// +I[Bob, 10]
// -U[Alice, 12]
// +U[Alice, 112]
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row

// create environments of both APIs
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env)

// create a DataStream
val dataStream = env.fromElements(
  Row.of("Alice", Int.box(12)),
  Row.of("Bob", Int.box(10)),
  Row.of("Alice", Int.box(100))
)(Types.ROW(Types.STRING, Types.INT))

// interpret the insert-only DataStream as a Table
val inputTable = tableEnv.fromDataStream(dataStream).as("name", "score")

// register the Table object as a view and query it
// the query contains an aggregation that produces updates
tableEnv.createTemporaryView("InputTable", inputTable)
val resultTable = tableEnv.sqlQuery("SELECT name, SUM(score) FROM InputTable GROUP BY name")

// interpret the updating Table as a changelog DataStream
val resultStream = tableEnv.toChangelogStream(resultTable)

// add a printing sink and execute in DataStream API
resultStream.print()
env.execute()

// prints:
// +I[Alice, 12]
// +I[Bob, 10]
// -U[Alice, 12]
// +U[Alice, 112]
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.common.typeinfo import Types

# create environments of both APIs
env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# create a DataStream
ds = env.from_collection([("Alice", 12), ("Bob", 10), ("Alice", 100)],
                          type_info=Types.ROW_NAMED(
                          ["a", "b"],
                          [Types.STRING(), Types.INT()]))

input_table = t_env.from_data_stream(ds).alias("name", "score")

# register the Table object as a view and query it
# the query contains an aggregation that produces updates
t_env.create_temporary_view("InputTable", input_table)
res_table = t_env.sql_query("SELECT name, SUM(score) FROM InputTable GROUP BY name")

# interpret the updating Table as a changelog DataStream
res_stream = t_env.to_changelog_stream(res_table)

# add a printing sink and execute in DataStream API
res_stream.print()
env.execute()

# prints:
# +I[Alice, 12]
# +I[Bob, 10]
# -U[Alice, 12]
# +U[Alice, 112]
```
{{< /tab >}}
{{< /tabs >}}

The complete semantics of `fromChangelogStream` and `toChangelogStream` can be found in the [dedicated section below](#handling-of-insert-only-streams).
In particular, the section discusses how to influence the schema derivation with more complex and nested
types. It covers working with event-time and watermarks. It discusses how to declare a primary key and
changelog mode for the input and output streams.

The example above shows how the final result is computed incrementally by continuously emitting [row-wise
updates]({{< ref "docs/dev/table/concepts/dynamic_tables" >}}) for each incoming record. However, in
cases where the input streams are finite (i.e. *bounded*), a result can be computed more efficiently
by leveraging batch processing principles.

In batch processing, operators can be executed in successive stages that consume the entire input table
before emitting results. For example, a join operator can sort both bounded inputs before performing
the actual joining (i.e. *sort-merge join algorithm*), or build a hash table from one input before
consuming the other (i.e. build/probe phase of the *hash join algorithm*).

Both DataStream API and Table API offer a specialized *batch runtime mode*.

The following example illustrates that the unified pipeline is able to process both batch and streaming data
by just switching a flag.

{{< tabs "61a8a0b8-c38b-48f3-b52e-563546139380" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// setup DataStream API
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// set the batch runtime mode
env.setRuntimeMode(RuntimeExecutionMode.BATCH);

// uncomment this for streaming mode
// env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

// setup Table API
// the table environment adopts the runtime mode during initialization
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// define the same pipeline as above

// prints in BATCH mode:
// +I[Bob, 10]
// +I[Alice, 112]

// prints in STREAMING mode:
// +I[Alice, 12]
// +I[Bob, 10]
// -U[Alice, 12]
// +U[Alice, 112]
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

// setup DataStream API
val env = StreamExecutionEnvironment.getExecutionEnvironment()

// set the batch runtime mode
env.setRuntimeMode(RuntimeExecutionMode.BATCH)

// uncomment this for streaming mode
// env.setRuntimeMode(RuntimeExecutionMode.STREAMING)

// setup Table API
// the table environment adopts the runtime mode during initialization
val tableEnv = StreamTableEnvironment.create(env)

// define the same pipeline as above

// prints in BATCH mode:
// +I[Bob, 10]
// +I[Alice, 112]

// prints in STREAMING mode:
// +I[Alice, 12]
// +I[Bob, 10]
// -U[Alice, 12]
// +U[Alice, 112]
```
{{< /tab >}}
{{< /tabs >}}

Once the changelog is applied to an external system (e.g. a key-value store), one can see that both
modes are able to produce exactly the same output table. By consuming all input data before emitting
results, the changelog of the batch mode consists solely of insert-only changes. See
also the [dedicated batch mode section below](#batch-runtime-mode) for more insights.

### Dependencies and Imports

Projects that combine Table API with DataStream API need to add one of the following bridging modules.
They include transitive dependencies to `flink-table-api-java` or `flink-table-api-scala` and the
corresponding language-specific DataStream API module.

{{< tabs "0d2da52a-ee43-4d06-afde-b165517c0617" >}}
{{< tab "Java" >}}
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-java-bridge{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
```
{{< /tab >}}
{{< tab "Scala" >}}
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-table-api-scala-bridge{{< scala_version >}}</artifactId>
  <version>{{< version >}}</version>
  <scope>provided</scope>
</dependency>
```
{{< /tab >}}
{{< /tabs >}}

The following imports are required to declare common pipelines using either the Java or Scala version
of both DataStream API and Table API.

{{< tabs "19a47e2d-168b-4f73-a966-abfcc8a6baca" >}}
{{< tab "Java" >}}
```java
// imports for Java DataStream API
import org.apache.flink.streaming.api.*;
import org.apache.flink.streaming.api.environment.*;

// imports for Table API with bridging to Java DataStream API
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.*;
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// imports for Scala DataStream API
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala._

// imports for Table API with bridging to Scala DataStream API
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
```
{{< /tab >}}
{{< /tabs >}}

### Configuration

The `TableEnvironment` will adopt all configuration options from the passed `StreamExecutionEnvironment`.
However, it cannot be guaranteed that further changes to the configuration of `StreamExecutionEnvironment`
are propagated to the `StreamTableEnvironment` after its instantiation. The propagation of options
from Table API to DataStream API happens during planning.

We recommend setting all configuration options in DataStream API early before switching to Table API.

{{< tabs "47a32814-abea-11eb-8529-0242ac130003" >}}
{{< tab "Java" >}}
```java
import java.time.ZoneId;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

// create Java DataStream API

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// set various configuration early

env.setMaxParallelism(256);

env.getConfig().addDefaultKryoSerializer(MyCustomType.class, CustomKryoSerializer.class);

env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// then switch to Java Table API

StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// set configuration early

tableEnv.getConfig().setLocalTimeZone(ZoneId.of("Europe/Berlin"));

// start defining your pipelines in both APIs...
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import java.time.ZoneId
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.table.api.bridge.scala._

// create Scala DataStream API

val env = StreamExecutionEnvironment.getExecutionEnvironment

// set various configuration early

env.setMaxParallelism(256)

env.getConfig.addDefaultKryoSerializer(classOf[MyCustomType], classOf[CustomKryoSerializer])

env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

// then switch to Scala Table API

val tableEnv = StreamTableEnvironment.create(env)

// set configuration early

tableEnv.getConfig.setLocalTimeZone(ZoneId.of("Europe/Berlin"))

// start defining your pipelines in both APIs...
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment
from pyflink.datastream.checkpointing_mode import CheckpointingMode


# create Python DataStream API
env = StreamExecutionEnvironment.get_execution_environment()

# set various configuration early
env.set_max_parallelism(256)

env.get_config().add_default_kryo_serializer("type_class_name", "serializer_class_name")

env.get_checkpoint_config().set_checkpointing_mode(CheckpointingMode.EXACTLY_ONCE)

# then switch to Python Table API
t_env = StreamTableEnvironment.create(env)

# set configuration early
t_env.get_config().set_local_timezone("Europe/Berlin")

# start defining your pipelines in both APIs...
```
{{< /tab >}}
{{< /tabs >}}

### Execution Behavior

Both APIs provide methods to execute pipelines. In other words: if requested, they compile a job
graph that will be submitted to the cluster and triggered for execution. Results will be streamed to
the declared sinks.

Usually, both APIs mark such behavior with the term `execute` in method names. However, the execution
behavior is slightly different between Table API and DataStream API.

**DataStream API**

The DataStream API's `StreamExecutionEnvironment` uses a _builder pattern_ to construct
a complex pipeline. The pipeline possibly splits into multiple branches that might or might not end with
a sink. The environment buffers all these defined branches until it comes to job submission.

`StreamExecutionEnvironment.execute()` submits the entire constructed pipeline and clears the builder
afterward. In other words: no sources and sinks are declared anymore, and a new pipeline can be
added to the builder. Thus, every DataStream program usually ends with a call to `StreamExecutionEnvironment.execute()`.
Alternatively, `DataStream.executeAndCollect()` implicitly defines a sink for streaming the results to
the local client.

**Table API**

In the Table API, branching pipelines are only supported within a `StatementSet` where each branch must
declare a final sink. Both `TableEnvironment` and also `StreamTableEnvironment` do not offer a dedicated
general `execute()` method. Instead, they offer methods for submitting a single source-to-sink
pipeline or a statement set:

```java
// execute with explicit sink
tableEnv.from("InputTable").executeInsert("OutputTable")

tableEnv.executeSql("INSERT INTO OutputTable SELECT * FROM InputTable")

tableEnv.createStatementSet()
    .addInsert("OutputTable", tableEnv.from("InputTable"))
    .addInsert("OutputTable2", tableEnv.from("InputTable"))
    .execute()

tableEnv.createStatementSet()
    .addInsertSql("INSERT INTO OutputTable SELECT * FROM InputTable")
    .addInsertSql("INSERT INTO OutputTable2 SELECT * FROM InputTable")
    .execute()

// execute with implicit local sink

tableEnv.from("InputTable").execute().print()

tableEnv.executeSql("SELECT * FROM InputTable").print()
```

To combine both execution behaviors, every call to `StreamTableEnvironment.toDataStream`
or `StreamTableEnvironment.toChangelogStream` will materialize (i.e. compile) the Table API sub-pipeline
and insert it into the DataStream API pipeline builder. This means that `StreamExecutionEnvironment.execute()`
or `DataStream.executeAndCollect` must be called afterwards. An execution in Table API will not trigger
these "external parts".

```java
// (1)

// adds a branch with a printing sink to the StreamExecutionEnvironment
tableEnv.toDataStream(table).print()

// (2)

// executes a Table API end-to-end pipeline as a Flink job and prints locally,
// thus (1) has still not been executed
table.execute().print()

// executes the DataStream API pipeline with the sink defined in (1) as a
// Flink job, (2) was already running before
env.execute()
```

{{< top >}}

Batch Runtime Mode
------------------

The *batch runtime mode* is a specialized execution mode for *bounded* Flink programs.

Generally speaking, *boundedness* is a property of a data source that tells us whether all the records
coming from that source are known before execution or whether new data will show up, potentially
indefinitely. A job, in turn, is bounded if all its sources are bounded, and unbounded otherwise.

*Streaming runtime mode*, on the other hand, can be used for both bounded and unbounded jobs.

For more information on the different execution modes, see also the corresponding [DataStream API section]({{< ref "docs/dev/datastream/execution_mode" >}}).

The Table API & SQL planner provides a set of specialized optimizer rules and runtime operators for either
of the two modes.

Currently, the runtime mode is not derived automatically from sources, thus, it must be set explicitly
or will be adopted from `StreamExecutionEnvironment` when instantiating a `StreamTableEnvironment`:

{{< tabs "f786b6ff-facc-4102-8833-3669f2fdef38" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;

// adopt mode from StreamExecutionEnvironment
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.setRuntimeMode(RuntimeExecutionMode.BATCH);
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// or

// set mode explicitly for StreamTableEnvironment
// it will be propagated to StreamExecutionEnvironment during planning
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode());
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.common.RuntimeExecutionMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.EnvironmentSettings

// adopt mode from StreamExecutionEnvironment
val env = StreamExecutionEnvironment.getExecutionEnvironment
env.setRuntimeMode(RuntimeExecutionMode.BATCH)
val tableEnv = StreamTableEnvironment.create(env)

// or

// set mode explicitly for StreamTableEnvironment
val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env, EnvironmentSettings.inBatchMode)
```
{{< /tab >}}
{{< /tabs >}}

One must meet the following prerequisites before setting the runtime mode to `BATCH`:

- All sources must declare themselves as bounded.

- Currently, table sources must emit insert-only changes.

- Operators need a sufficient amount of [off-heap memory]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#managed-memory)
for sorting and other intermediate results.

- All table operations must be available in batch mode. Currently, some of them are only available in
streaming mode. Please check the corresponding Table API & SQL pages.

A batch execution has the following implications (among others):

- Progressive watermarks are neither generated nor used in operators. However, sources emit a maximum
watermark before shutting down.

- Exchanges between tasks might be blocking according to the [`execution.batch-shuffle-mode`]({{< ref "docs/deployment/config" >}}#execution-batch-shuffle-mode).
This also means potentially less resource requirements compared to executing the same pipeline in streaming mode.

- Checkpointing is disabled. Artificial state backends are inserted.

- Table operations don't produce incremental updates but only a complete final result which converts
to an insert-only changelog stream.

{{< hint info >}}
Since batch processing can be considered as *a special case of stream processing*, we recommend implementing
a streaming pipeline first as it is the most general implementation for both bounded and unbounded data.

In theory, a streaming pipeline can execute all operators. However, in practice, some operations might
not make much sense as they would lead to ever-growing state and are therefore not supported. A global
sort would be an example that is only available in batch mode. Simply put: it should be possible to
run a working streaming pipeline in batch mode but not necessarily vice versa.
{{< /hint >}}

The following example shows how to play around with batch mode using the [DataGen table source]({{< ref "docs/connectors/table/datagen" >}}).
Many sources offer options that implicitly make the connector bounded, for example, by defining a
terminating offset or timestamp. In our example, we limit the number of rows with the `number-of-rows`
option.

{{< tabs "3f5f5d4e-cd03-48d1-9309-917a6cf55aaa" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableDescriptor;

Table table =
    tableEnv.from(
        TableDescriptor.forConnector("datagen")
            .option("number-of-rows", "10") // make the source bounded
            .schema(
                Schema.newBuilder()
                    .column("uid", DataTypes.TINYINT())
                    .column("payload", DataTypes.STRING())
                    .build())
            .build());

// convert the Table to a DataStream and further transform the pipeline
tableEnv.toDataStream(table)
    .keyBy(r -> r.<Byte>getFieldAs("uid"))
    .map(r -> "My custom operator: " + r.<String>getFieldAs("payload"))
    .executeAndCollect()
    .forEachRemaining(System.out::println);

// prints:
// My custom operator: 9660912d30a43c7b035e15bd...
// My custom operator: 29f5f706d2144f4a4f9f52a0...
// ...
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import org.apache.flink.table.api._

val table =
    tableEnv.from(
        TableDescriptor.forConnector("datagen")
            .option("number-of-rows", "10") // make the source bounded
            .schema(
                Schema.newBuilder()
                    .column("uid", DataTypes.TINYINT())
                    .column("payload", DataTypes.STRING())
                    .build())
            .build());

// convert the Table to a DataStream and further transform the pipeline
tableEnv.toDataStream(table)
    .keyBy(r => r.getFieldAs[Byte]("uid"))
    .map(r => "My custom operator: " + r.getFieldAs[String]("payload"))
    .executeAndCollect()
    .foreach(println)

// prints:
// My custom operator: 9660912d30a43c7b035e15bd...
// My custom operator: 29f5f706d2144f4a4f9f52a0...
// ...
```
{{< /tab >}}
{{< /tabs >}}

### Changelog Unification

In most cases, the pipeline definition itself can remain constant in both Table API and DataStream API
when switching from streaming to batch mode and vice versa. However, as mentioned before, the resulting
changelog streams might differ due to the avoidance of incremental operations in batch mode.

Time-based operations that rely on event-time and leverage watermarks as a completeness marker are able
to produce an insert-only changelog stream that is independent of the runtime mode.

The following Java example illustrates a Flink program that is not only unified on an API level but also
in the resulting changelog stream. The example joins two tables in SQL (`UserTable` and `OrderTable`) using
an interval join based on the time attributes in both tables (`ts`). It uses DataStream API to implement
a custom operator that deduplicates the user name using a `KeyedProcessFunction` and value state.

```java
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

// setup DataStream API
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// use BATCH or STREAMING mode
env.setRuntimeMode(RuntimeExecutionMode.BATCH);

// setup Table API
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

// create a user stream
DataStream<Row> userStream = env
    .fromElements(
        Row.of(LocalDateTime.parse("2021-08-21T13:00:00"), 1, "Alice"),
        Row.of(LocalDateTime.parse("2021-08-21T13:05:00"), 2, "Bob"),
        Row.of(LocalDateTime.parse("2021-08-21T13:10:00"), 2, "Bob"))
    .returns(
        Types.ROW_NAMED(
            new String[] {"ts", "uid", "name"},
            Types.LOCAL_DATE_TIME, Types.INT, Types.STRING));

// create an order stream
DataStream<Row> orderStream = env
    .fromElements(
        Row.of(LocalDateTime.parse("2021-08-21T13:02:00"), 1, 122),
        Row.of(LocalDateTime.parse("2021-08-21T13:07:00"), 2, 239),
        Row.of(LocalDateTime.parse("2021-08-21T13:11:00"), 2, 999))
    .returns(
        Types.ROW_NAMED(
            new String[] {"ts", "uid", "amount"},
            Types.LOCAL_DATE_TIME, Types.INT, Types.INT));

// create corresponding tables
tableEnv.createTemporaryView(
    "UserTable",
    userStream,
    Schema.newBuilder()
        .column("ts", DataTypes.TIMESTAMP(3))
        .column("uid", DataTypes.INT())
        .column("name", DataTypes.STRING())
        .watermark("ts", "ts - INTERVAL '1' SECOND")
        .build());

tableEnv.createTemporaryView(
    "OrderTable",
    orderStream,
    Schema.newBuilder()
        .column("ts", DataTypes.TIMESTAMP(3))
        .column("uid", DataTypes.INT())
        .column("amount", DataTypes.INT())
        .watermark("ts", "ts - INTERVAL '1' SECOND")
        .build());

// perform interval join
Table joinedTable =
    tableEnv.sqlQuery(
        "SELECT U.name, O.amount " +
        "FROM UserTable U, OrderTable O " +
        "WHERE U.uid = O.uid AND O.ts BETWEEN U.ts AND U.ts + INTERVAL '5' MINUTES");

DataStream<Row> joinedStream = tableEnv.toDataStream(joinedTable);

joinedStream.print();

// implement a custom operator using ProcessFunction and value state
joinedStream
    .keyBy(r -> r.<String>getFieldAs("name"))
    .process(
        new KeyedProcessFunction<String, Row, String>() {

          ValueState<String> seen;

          @Override
          public void open(Configuration parameters) {
              seen = getRuntimeContext().getState(
                  new ValueStateDescriptor<>("seen", String.class));
          }

          @Override
          public void processElement(Row row, Context ctx, Collector<String> out)
                  throws Exception {
              String name = row.getFieldAs("name");
              if (seen.value() == null) {
                  seen.update(name);
                  out.collect(name);
              }
          }
        })
    .print();

// execute unified pipeline
env.execute();

// prints (in both BATCH and STREAMING mode):
// +I[Bob, 239]
// +I[Alice, 122]
// +I[Bob, 999]
//
// Bob
// Alice
```

{{< top >}}

Handling of (Insert-Only) Streams
---------------------------------

A `StreamTableEnvironment` offers the following methods to convert from and to DataStream API:

- `fromDataStream(DataStream)`: Interprets a stream of insert-only changes and arbitrary type as
a table. Event-time and watermarks are not propagated by default.

- `fromDataStream(DataStream, Schema)`: Interprets a stream of insert-only changes and arbitrary
type as a table. The optional schema allows to enrich column data types and add time attributes,
watermarks strategies, other computed columns, or primary keys.

- `createTemporaryView(String, DataStream)`: Registers the stream under a name to access it in SQL.
It is a shortcut for `createTemporaryView(String, fromDataStream(DataStream))`.

- `createTemporaryView(String, DataStream, Schema)`: Registers the stream under a name to access it in SQL.
It is a shortcut for `createTemporaryView(String, fromDataStream(DataStream, Schema))`.

- `toDataStream(Table)`: Converts a table into a stream of insert-only changes. The default
stream record type is `org.apache.flink.types.Row`. A single rowtime attribute column is written
back into the DataStream API's record. Watermarks are propagated as well.

- `toDataStream(Table, AbstractDataType)`: Converts a table into a stream of insert-only changes.
This method accepts a data type to express the desired stream record type. The planner might insert
implicit casts and reorders columns to map columns to fields of the (possibly nested) data type.

- `toDataStream(Table, Class)`: A shortcut for `toDataStream(Table, DataTypes.of(Class))`
to quickly create the desired data type reflectively.

From a Table API's perspective, converting from and to DataStream API is similar to reading from or
writing to a virtual table connector that has been defined using a [`CREATE TABLE` DDL]({{< ref "docs/dev/table/sql/create" >}}#create-table)
in SQL.

The schema part in the virtual `CREATE TABLE name (schema) WITH (options)` statement can be automatically
derived from the DataStream's type information, enriched, or entirely defined manually using
`org.apache.flink.table.api.Schema`.

The virtual DataStream table connector exposes the following metadata for every row:

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 25%">Key</th>
      <th class="text-center" style="width: 30%">Data Type</th>
      <th class="text-center" style="width: 40%">Description</th>
      <th class="text-center" style="width: 5%">R/W</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>rowtime</code></td>
      <td><code>TIMESTAMP_LTZ(3) NOT NULL</code></td>
      <td>Stream record's timestamp.</td>
      <td><code>R/W</code></td>
    </tr>
    </tbody>
</table>

The virtual DataStream table source implements [`SupportsSourceWatermark`]({{< ref "docs/dev/table/sourcesSinks" >}}#source-abilities)
and thus allows calling the `SOURCE_WATERMARK()` built-in function as a watermark strategy to adopt
watermarks from the DataStream API.

### Examples for `fromDataStream`

The following code shows how to use `fromDataStream` for different scenarios.

{{< tabs "079cdf25-21ef-4393-ad69-623510027a1b" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import java.time.Instant;

// some example POJO
public static class User {
  public String name;

  public Integer score;

  public Instant event_time;

  // default constructor for DataStream API
  public User() {}

  // fully assigning constructor for Table API
  public User(String name, Integer score, Instant event_time) {
    this.name = name;
    this.score = score;
    this.event_time = event_time;
  }
}

// create a DataStream
DataStream<User> dataStream =
    env.fromElements(
        new User("Alice", 4, Instant.ofEpochMilli(1000)),
        new User("Bob", 6, Instant.ofEpochMilli(1001)),
        new User("Alice", 10, Instant.ofEpochMilli(1002)));


// === EXAMPLE 1 ===

// derive all physical columns automatically

Table table = tableEnv.fromDataStream(dataStream);
table.printSchema();
// prints:
// (
//  `name` STRING,
//  `score` INT,
//  `event_time` TIMESTAMP_LTZ(9)
// )


// === EXAMPLE 2 ===

// derive all physical columns automatically
// but add computed columns (in this case for creating a proctime attribute column)

Table table = tableEnv.fromDataStream(
    dataStream,
    Schema.newBuilder()
        .columnByExpression("proc_time", "PROCTIME()")
        .build());
table.printSchema();
// prints:
// (
//  `name` STRING,
//  `score` INT NOT NULL,
//  `event_time` TIMESTAMP_LTZ(9),
//  `proc_time` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME* AS PROCTIME()
//)


// === EXAMPLE 3 ===

// derive all physical columns automatically
// but add computed columns (in this case for creating a rowtime attribute column)
// and a custom watermark strategy

Table table =
    tableEnv.fromDataStream(
        dataStream,
        Schema.newBuilder()
            .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
            .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
            .build());
table.printSchema();
// prints:
// (
//  `name` STRING,
//  `score` INT,
//  `event_time` TIMESTAMP_LTZ(9),
//  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* AS CAST(event_time AS TIMESTAMP_LTZ(3)),
//  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS rowtime - INTERVAL '10' SECOND
// )


// === EXAMPLE 4 ===

// derive all physical columns automatically
// but access the stream record's timestamp for creating a rowtime attribute column
// also rely on the watermarks generated in the DataStream API

// we assume that a watermark strategy has been defined for `dataStream` before
// (not part of this example)
Table table =
    tableEnv.fromDataStream(
        dataStream,
        Schema.newBuilder()
            .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
            .watermark("rowtime", "SOURCE_WATERMARK()")
            .build());
table.printSchema();
// prints:
// (
//  `name` STRING,
//  `score` INT,
//  `event_time` TIMESTAMP_LTZ(9),
//  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* METADATA,
//  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS SOURCE_WATERMARK()
// )


// === EXAMPLE 5 ===

// define physical columns manually
// in this example,
//   - we can reduce the default precision of timestamps from 9 to 3
//   - we also project the columns and put `event_time` to the beginning

Table table =
    tableEnv.fromDataStream(
        dataStream,
        Schema.newBuilder()
            .column("event_time", "TIMESTAMP_LTZ(3)")
            .column("name", "STRING")
            .column("score", "INT")
            .watermark("event_time", "SOURCE_WATERMARK()")
            .build());
table.printSchema();
// prints:
// (
//  `event_time` TIMESTAMP_LTZ(3) *ROWTIME*,
//  `name` VARCHAR(200),
//  `score` INT
// )
// note: the watermark strategy is not shown due to the inserted column reordering projection
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import java.time.Instant;

// some example case class
case class User(name: String, score: java.lang.Integer, event_time: java.time.Instant)

// create a DataStream
val dataStream = env.fromElements(
    User("Alice", 4, Instant.ofEpochMilli(1000)),
    User("Bob", 6, Instant.ofEpochMilli(1001)),
    User("Alice", 10, Instant.ofEpochMilli(1002)))


// === EXAMPLE 1 ===

// derive all physical columns automatically

val table = tableEnv.fromDataStream(dataStream)
table.printSchema()
// prints:
// (
//  `name` STRING,
//  `score` INT,
//  `event_time` TIMESTAMP_LTZ(9)
// )


// === EXAMPLE 2 ===

// derive all physical columns automatically
// but add computed columns (in this case for creating a proctime attribute column)

val table = tableEnv.fromDataStream(
    dataStream,
    Schema.newBuilder()
        .columnByExpression("proc_time", "PROCTIME()")
        .build())
table.printSchema()
// prints:
// (
//  `name` STRING,
//  `score` INT NOT NULL,
//  `event_time` TIMESTAMP_LTZ(9),
//  `proc_time` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME* AS PROCTIME()
//)


// === EXAMPLE 3 ===

// derive all physical columns automatically
// but add computed columns (in this case for creating a rowtime attribute column)
// and a custom watermark strategy

val table =
    tableEnv.fromDataStream(
        dataStream,
        Schema.newBuilder()
            .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
            .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
            .build())
table.printSchema()
// prints:
// (
//  `name` STRING,
//  `score` INT,
//  `event_time` TIMESTAMP_LTZ(9),
//  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* AS CAST(event_time AS TIMESTAMP_LTZ(3)),
//  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS rowtime - INTERVAL '10' SECOND
// )


// === EXAMPLE 4 ===

// derive all physical columns automatically
// but access the stream record's timestamp for creating a rowtime attribute column
// also rely on the watermarks generated in the DataStream API

// we assume that a watermark strategy has been defined for `dataStream` before
// (not part of this example)
val table =
    tableEnv.fromDataStream(
        dataStream,
        Schema.newBuilder()
            .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
            .watermark("rowtime", "SOURCE_WATERMARK()")
            .build())
table.printSchema()
// prints:
// (
//  `name` STRING,
//  `score` INT,
//  `event_time` TIMESTAMP_LTZ(9),
//  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* METADATA,
//  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS SOURCE_WATERMARK()
// )


// === EXAMPLE 5 ===

// define physical columns manually
// in this example,
//   - we can reduce the default precision of timestamps from 9 to 3
//   - we also project the columns and put `event_time` to the beginning

val table =
    tableEnv.fromDataStream(
        dataStream,
        Schema.newBuilder()
            .column("event_time", "TIMESTAMP_LTZ(3)")
            .column("name", "STRING")
            .column("score", "INT")
            .watermark("event_time", "SOURCE_WATERMARK()")
            .build())
table.printSchema()
// prints:
// (
//  `event_time` TIMESTAMP_LTZ(3) *ROWTIME*,
//  `name` VARCHAR(200),
//  `score` INT
// )
// note: the watermark strategy is not shown due to the inserted column reordering projection
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.common.time import Instant
from pyflink.common.types import Row
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, Schema

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)
ds = env.from_collection([
    Row("Alice", 12, Instant.of_epoch_milli(1000)),
    Row("Bob", 5, Instant.of_epoch_milli(1001)),
    Row("Alice", 10, Instant.of_epoch_milli(1002))],
    type_info=Types.ROW_NAMED(['name', 'score', 'event_time'], [Types.STRING(), Types.INT(), Types.INSTANT()]))

# === EXAMPLE 1 ===

# derive all physical columns automatically

table = t_env.from_data_stream(ds)
table.print_schema()

# prints:
# (
#  `name` STRING,
#  `score` INT,
#  `event_time` TIMESTAMP_LTZ(9)
# )


# === EXAMPLE 2 ===

# derive all physical columns automatically
# but add computed columns (in this case for creating a proctime attribute column)

table = t_env.from_data_stream(
    ds,
    Schema.new_builder()
        .column_by_expression("proc_time", "PROCTIME()")
        .build())
table.print_schema()

# prints:
# (
#  `name` STRING,
#  `score` INT,
#  `event_time` TIMESTAMP_LTZ(9),
#  `proc_time` TIMESTAMP_LTZ(3) NOT NULL *PROCTIME* AS PROCTIME()
# )

# === EXAMPLE 3 ===

# derive all physical columns automatically
# but add computed columns (in this case for creating a rowtime attribute column)
# and a custom watermark strategy

table = t_env.from_data_stream(
          ds,
          Schema.new_builder()
              .column_by_expression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
              .watermark("rowtime", "rowtime - INTERVAL '10' SECOND")
              .build())
table.print_schema()

# prints:
# (
#  `name` STRING,
#  `score` INT,
#  `event_time` TIMESTAMP_LTZ(9),
#  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* AS CAST(event_time AS TIMESTAMP_LTZ(3)),
#  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS rowtime - INTERVAL '10' SECOND
# )

# === EXAMPLE 4 ===

# derive all physical columns automatically
# but access the stream record's timestamp for creating a rowtime attribute column
# also rely on the watermarks generated in the DataStream API

# we assume that a watermark strategy has been defined for `dataStream` before
# (not part of this example)
table = t_env.from_data_stream(
        ds,
        Schema.new_builder()
            .column_by_metadata("rowtime", "TIMESTAMP_LTZ(3)")
            .watermark("rowtime", "SOURCE_WATERMARK()")
            .build())
table.print_schema()

# prints:
# (
#  `name` STRING,
#  `score` INT,
#  `event_time` TIMESTAMP_LTZ(9),
#  `rowtime` TIMESTAMP_LTZ(3) *ROWTIME* METADATA,
#  WATERMARK FOR `rowtime`: TIMESTAMP_LTZ(3) AS SOURCE_WATERMARK()
# )



# === EXAMPLE 5 ===

# define physical columns manually
# in this example,
#   - we can reduce the default precision of timestamps from 9 to 3
#   - we also project the columns and put `event_time` to the beginning

table = t_env.from_data_stream(
        ds,
        Schema.new_builder()
            .column("event_time", "TIMESTAMP_LTZ(3)")
            .column("name", "STRING")
            .column("score", "INT")
            .watermark("event_time", "SOURCE_WATERMARK()")
            .build())
table.print_schema()

# prints:
# (
#  `event_time` TIMESTAMP_LTZ(3) *ROWTIME*,
#  `name` STRING,
#  `score` INT
# )
# note: the watermark strategy is not shown due to the inserted column reordering projection
```
{{< /tab >}}
{{< /tabs >}}

Example 1 illustrates a simple use case when no time-based operations are needed.

Example 4 is the most common use case when time-based operations such as windows or interval
joins should be part of the pipeline. Example 2 is the most common use case when these time-based
operations should work in processing time.

Example 5 entirely relies on the declaration of the user. This can be useful to replace generic types
from the DataStream API (which would be `RAW` in the Table API) with proper data types.

Since `DataType` is richer than `TypeInformation`, we can easily enable immutable POJOs and other complex
data structures. The following example in Java shows what is possible. Check also the
[Data Types & Serialization]({{< ref "docs/dev/datastream/fault-tolerance/serialization/types_serialization" >}}) page of
the DataStream API for more information about the supported types there.

```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;

// the DataStream API does not support immutable POJOs yet,
// the class will result in a generic type that is a RAW type in Table API by default
public static class User {

    public final String name;

    public final Integer score;

    public User(String name, Integer score) {
        this.name = name;
        this.score = score;
    }
}

// create a DataStream
DataStream<User> dataStream = env.fromElements(
    new User("Alice", 4),
    new User("Bob", 6),
    new User("Alice", 10));

// since fields of a RAW type cannot be accessed, every stream record is treated as an atomic type
// leading to a table with a single column `f0`

Table table = tableEnv.fromDataStream(dataStream);
table.printSchema();
// prints:
// (
//  `f0` RAW('User', '...')
// )

// instead, declare a more useful data type for columns using the Table API's type system
// in a custom schema and rename the columns in a following `as` projection

Table table = tableEnv
    .fromDataStream(
        dataStream,
        Schema.newBuilder()
            .column("f0", DataTypes.of(User.class))
            .build())
    .as("user");
table.printSchema();
// prints:
// (
//  `user` *User<`name` STRING,`score` INT>*
// )

// data types can be extracted reflectively as above or explicitly defined

Table table3 = tableEnv
    .fromDataStream(
        dataStream,
        Schema.newBuilder()
            .column(
                "f0",
                DataTypes.STRUCTURED(
                    User.class,
                    DataTypes.FIELD("name", DataTypes.STRING()),
                    DataTypes.FIELD("score", DataTypes.INT())))
            .build())
    .as("user");
table.printSchema();
// prints:
// (
//  `user` *User<`name` STRING,`score` INT>*
// )
```

### Examples for `createTemporaryView`

A `DataStream` can be registered directly as a view (possibly enriched with a schema).

{{< hint info >}}
Views created from a `DataStream` can only be registered as temporary views. Due to their _inline_/_anonymous_
nature, it is not possible to register them in a permanent catalog.
{{< /hint >}}

The following code shows how to use `createTemporaryView` for different scenarios.

{{< tabs "03d19c44-b994-4991-8c66-00189a2ec5d5" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

// create some DataStream
DataStream<Tuple2<Long, String>> dataStream = env.fromElements(
    Tuple2.of(12L, "Alice"),
    Tuple2.of(0L, "Bob"));


// === EXAMPLE 1 ===

// register the DataStream as view "MyView" in the current session
// all columns are derived automatically

tableEnv.createTemporaryView("MyView", dataStream);

tableEnv.from("MyView").printSchema();

// prints:
// (
//  `f0` BIGINT NOT NULL,
//  `f1` STRING
// )


// === EXAMPLE 2 ===

// register the DataStream as view "MyView" in the current session,
// provide a schema to adjust the columns similar to `fromDataStream`

// in this example, the derived NOT NULL information has been removed

tableEnv.createTemporaryView(
    "MyView",
    dataStream,
    Schema.newBuilder()
        .column("f0", "BIGINT")
        .column("f1", "STRING")
        .build());

tableEnv.from("MyView").printSchema();

// prints:
// (
//  `f0` BIGINT,
//  `f1` STRING
// )


// === EXAMPLE 3 ===

// use the Table API before creating the view if it is only about renaming columns

tableEnv.createTemporaryView(
    "MyView",
    tableEnv.fromDataStream(dataStream).as("id", "name"));

tableEnv.from("MyView").printSchema();

// prints:
// (
//  `id` BIGINT NOT NULL,
//  `name` STRING
// )
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// create some DataStream
val dataStream: DataStream[(Long, String)] = env.fromElements(
    (12L, "Alice"),
    (0L, "Bob"))


// === EXAMPLE 1 ===

// register the DataStream as view "MyView" in the current session
// all columns are derived automatically

tableEnv.createTemporaryView("MyView", dataStream)

tableEnv.from("MyView").printSchema()

// prints:
// (
//  `_1` BIGINT NOT NULL,
//  `_2` STRING
// )


// === EXAMPLE 2 ===

// register the DataStream as view "MyView" in the current session,
// provide a schema to adjust the columns similar to `fromDataStream`

// in this example, the derived NOT NULL information has been removed

tableEnv.createTemporaryView(
    "MyView",
    dataStream,
    Schema.newBuilder()
        .column("_1", "BIGINT")
        .column("_2", "STRING")
        .build())

tableEnv.from("MyView").printSchema()

// prints:
// (
//  `_1` BIGINT,
//  `_2` STRING
// )


// === EXAMPLE 3 ===

// use the Table API before creating the view if it is only about renaming columns

tableEnv.createTemporaryView(
    "MyView",
    tableEnv.fromDataStream(dataStream).as("id", "name"))

tableEnv.from("MyView").printSchema()

// prints:
// (
//  `id` BIGINT NOT NULL,
//  `name` STRING
// )
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes, StreamTableEnvironment, Schema

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)
ds = env.from_collection([(12, "Alice"), (0, "Bob")], type_info=Types.TUPLE([Types.LONG(), Types.STRING()]))

# === EXAMPLE 1 ===

# register the DataStream as view "MyView" in the current session
# all columns are derived automatically

t_env.create_temporary_view("MyView", ds)

t_env.from_path("MyView").print_schema()

# prints:
# (
#  `f0` BIGINT NOT NULL,
#  `f1` STRING
# )

# === EXAMPLE 2 ===

# register the DataStream as view "MyView" in the current session,
# provide a schema to adjust the columns similar to `fromDataStream`

# in this example, the derived NOT NULL information has been removed

t_env.create_temporary_view(
    "MyView",
    ds,
    Schema.new_builder()
        .column("f0", "BIGINT")
        .column("f1", "STRING")
        .build())

t_env.from_path("MyView").print_schema()

# prints:
# (
#  `f0` BIGINT,
#  `f1` STRING
# )


# === EXAMPLE 3 ===

# use the Table API before creating the view if it is only about renaming columns

t_env.create_temporary_view(
    "MyView",
    t_env.from_data_stream(ds).alias("id", "name"))

t_env.from_path("MyView").print_schema()

# prints:
# (
#  `id` BIGINT NOT NULL,
#  `name` STRING
# )
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

### Examples for `toDataStream`

The following code shows how to use `toDataStream` for different scenarios.

{{< tabs "213ed312-f854-477a-a8be-2830f04d7154" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;
import java.time.Instant;

// POJO with mutable fields
// since no fully assigning constructor is defined, the field order
// is alphabetical [event_time, name, score]
public static class User {

    public String name;

    public Integer score;

    public Instant event_time;
}

tableEnv.executeSql(
    "CREATE TABLE GeneratedTable "
    + "("
    + "  name STRING,"
    + "  score INT,"
    + "  event_time TIMESTAMP_LTZ(3),"
    + "  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND"
    + ")"
    + "WITH ('connector'='datagen')");

Table table = tableEnv.from("GeneratedTable");


// === EXAMPLE 1 ===

// use the default conversion to instances of Row

// since `event_time` is a single rowtime attribute, it is inserted into the DataStream
// metadata and watermarks are propagated

DataStream<Row> dataStream = tableEnv.toDataStream(table);


// === EXAMPLE 2 ===

// a data type is extracted from class `User`,
// the planner reorders fields and inserts implicit casts where possible to convert internal
// data structures to the desired structured type

// since `event_time` is a single rowtime attribute, it is inserted into the DataStream
// metadata and watermarks are propagated

DataStream<User> dataStream = tableEnv.toDataStream(table, User.class);

// data types can be extracted reflectively as above or explicitly defined

DataStream<User> dataStream =
    tableEnv.toDataStream(
        table,
        DataTypes.STRUCTURED(
            User.class,
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("score", DataTypes.INT()),
            DataTypes.FIELD("event_time", DataTypes.TIMESTAMP_LTZ(3))));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api.DataTypes

case class User(name: String, score: java.lang.Integer, event_time: java.time.Instant)

tableEnv.executeSql(
  """
  CREATE TABLE GeneratedTable (
    name STRING,
    score INT,
    event_time TIMESTAMP_LTZ(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
  )
  WITH ('connector'='datagen')
  """
)

val table = tableEnv.from("GeneratedTable")


// === EXAMPLE 1 ===

// use the default conversion to instances of Row

// since `event_time` is a single rowtime attribute, it is inserted into the DataStream
// metadata and watermarks are propagated

val dataStream: DataStream[Row] = tableEnv.toDataStream(table)


// === EXAMPLE 2 ===

// a data type is extracted from class `User`,
// the planner reorders fields and inserts implicit casts where possible to convert internal
// data structures to the desired structured type

// since `event_time` is a single rowtime attribute, it is inserted into the DataStream
// metadata and watermarks are propagated

val dataStream: DataStream[User] = tableEnv.toDataStream(table, classOf[User])

// data types can be extracted reflectively as above or explicitly defined

val dataStream: DataStream[User] =
    tableEnv.toDataStream(
        table,
        DataTypes.STRUCTURED(
            classOf[User],
            DataTypes.FIELD("name", DataTypes.STRING()),
            DataTypes.FIELD("score", DataTypes.INT()),
            DataTypes.FIELD("event_time", DataTypes.TIMESTAMP_LTZ(3))))
```
{{< /tab >}}
{{< tab "Python" >}}
```python
t_env.execute_sql(
    "CREATE TABLE GeneratedTable "
    + "("
    + "  name STRING,"
    + "  score INT,"
    + "  event_time TIMESTAMP_LTZ(3),"
    + "  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND"
    + ")"
    + "WITH ('connector'='datagen')");

table = t_env.from_path("GeneratedTable");


# === EXAMPLE 1 ===

# use the default conversion to instances of Row

# since `event_time` is a single rowtime attribute, it is inserted into the DataStream
# metadata and watermarks are propagated

ds = t_env.to_data_stream(table)
```
{{< /tab >}}
{{< /tabs >}}

Note that only non-updating tables are supported by `toDataStream`. Usually, time-based operations
such as windows, interval joins, or the `MATCH_RECOGNIZE` clause are a good fit for insert-only
pipelines next to simple operations like projections and filters.

Pipelines with operations that produce updates can use `toChangelogStream`.

{{< top >}}

Handling of Changelog Streams
-----------------------------

Internally, Flink's table runtime is a changelog processor. The concepts page describes how
[dynamic tables and streams relate]({{< ref "docs/dev/table/concepts/dynamic_tables" >}})
to each other.

A `StreamTableEnvironment` offers the following methods to expose these _change data capture_ (CDC)
functionalities:

- `fromChangelogStream(DataStream)`: Interprets a stream of changelog entries as a table. The stream
record type must be `org.apache.flink.types.Row` since its `RowKind` flag is evaluated during runtime.
Event-time and watermarks are not propagated by default. This method expects a changelog containing
all kinds of changes (enumerated in `org.apache.flink.types.RowKind`) as the default `ChangelogMode`.

- `fromChangelogStream(DataStream, Schema)`: Allows to define a schema for the `DataStream` similar
to `fromDataStream(DataStream, Schema)`. Otherwise the semantics are equal to `fromChangelogStream(DataStream)`.

- `fromChangelogStream(DataStream, Schema, ChangelogMode)`: Gives full control about how to interpret a
stream as a changelog. The passed `ChangelogMode` helps the planner to distinguish between _insert-only_,
_upsert_, or _retract_ behavior.

- `toChangelogStream(Table)`: Reverse operation of `fromChangelogStream(DataStream)`. It produces a
stream with instances of `org.apache.flink.types.Row` and sets the `RowKind` flag for every record
at runtime. All kinds of updating tables are supported by this method. If the input table contains a
single rowtime column, it will be propagated into a stream record's timestamp. Watermarks will be
propagated as well.

- `toChangelogStream(Table, Schema)`: Reverse operation of `fromChangelogStream(DataStream, Schema)`.
The method can enrich the produced column data types. The planner might insert implicit casts if necessary.
It is possible to write out the rowtime as a metadata column.

- `toChangelogStream(Table, Schema, ChangelogMode)`: Gives full control about how to convert a table
to a changelog stream. The passed `ChangelogMode` helps the planner to distinguish between _insert-only_,
_upsert_, or _retract_ behavior.

From a Table API's perspective, converting from and to DataStream API is similar to reading from or
writing to a virtual table connector that has been defined using a [`CREATE TABLE` DDL]({{< ref "docs/dev/table/sql/create" >}}#create-table)
in SQL.

Because `fromChangelogStream` behaves similar to `fromDataStream`, we recommend reading
the [previous section](#handling-of-insert-only-streams) before continuing here.

This virtual connector also supports reading and writing the `rowtime` metadata of the stream record.

The virtual table source implements [`SupportsSourceWatermark`]({{< ref "docs/dev/table/sourcesSinks" >}}#source-abilities).

### Examples for `fromChangelogStream`

The following code shows how to use `fromChangelogStream` for different scenarios.

{{< tabs "11927973-ce73-4e95-b912-0759e8013f24" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

// === EXAMPLE 1 ===

// interpret the stream as a retract stream

// create a changelog DataStream
DataStream<Row> dataStream =
    env.fromElements(
        Row.ofKind(RowKind.INSERT, "Alice", 12),
        Row.ofKind(RowKind.INSERT, "Bob", 5),
        Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
        Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));

// interpret the DataStream as a Table
Table table = tableEnv.fromChangelogStream(dataStream);

// register the table under a name and perform an aggregation
tableEnv.createTemporaryView("InputTable", table);
tableEnv
    .executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0")
    .print();

// prints:
// +----+--------------------------------+-------------+
// | op |                           name |       score |
// +----+--------------------------------+-------------+
// | +I |                            Bob |           5 |
// | +I |                          Alice |          12 |
// | -D |                          Alice |          12 |
// | +I |                          Alice |         100 |
// +----+--------------------------------+-------------+


// === EXAMPLE 2 ===

// interpret the stream as an upsert stream (without a need for UPDATE_BEFORE)

// create a changelog DataStream
DataStream<Row> dataStream =
    env.fromElements(
        Row.ofKind(RowKind.INSERT, "Alice", 12),
        Row.ofKind(RowKind.INSERT, "Bob", 5),
        Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100));

// interpret the DataStream as a Table
Table table =
    tableEnv.fromChangelogStream(
        dataStream,
        Schema.newBuilder().primaryKey("f0").build(),
        ChangelogMode.upsert());

// register the table under a name and perform an aggregation
tableEnv.createTemporaryView("InputTable", table);
tableEnv
    .executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0")
    .print();

// prints:
// +----+--------------------------------+-------------+
// | op |                           name |       score |
// +----+--------------------------------+-------------+
// | +I |                            Bob |           5 |
// | +I |                          Alice |          12 |
// | -U |                          Alice |          12 |
// | +U |                          Alice |         100 |
// +----+--------------------------------+-------------+
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.table.api.Schema
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.types.{Row, RowKind}

// === EXAMPLE 1 ===

// interpret the stream as a retract stream

// create a changelog DataStream
val dataStream = env.fromElements(
    Row.ofKind(RowKind.INSERT, "Alice", Int.box(12)),
    Row.ofKind(RowKind.INSERT, "Bob", Int.box(5)),
    Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", Int.box(12)),
    Row.ofKind(RowKind.UPDATE_AFTER, "Alice", Int.box(100))
)(Types.ROW(Types.STRING, Types.INT))


// interpret the DataStream as a Table
val table = tableEnv.fromChangelogStream(dataStream)

// register the table under a name and perform an aggregation
tableEnv.createTemporaryView("InputTable", table)
tableEnv
    .executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0")
    .print()

// prints:
// +----+--------------------------------+-------------+
// | op |                           name |       score |
// +----+--------------------------------+-------------+
// | +I |                            Bob |           5 |
// | +I |                          Alice |          12 |
// | -D |                          Alice |          12 |
// | +I |                          Alice |         100 |
// +----+--------------------------------+-------------+


// === EXAMPLE 2 ===

// interpret the stream as an upsert stream (without a need for UPDATE_BEFORE)

// create a changelog DataStream
val dataStream = env.fromElements(
    Row.ofKind(RowKind.INSERT, "Alice", Int.box(12)),
    Row.ofKind(RowKind.INSERT, "Bob", Int.box(5)),
    Row.ofKind(RowKind.UPDATE_AFTER, "Alice", Int.box(100))
)(Types.ROW(Types.STRING, Types.INT))

// interpret the DataStream as a Table
val table =
    tableEnv.fromChangelogStream(
        dataStream,
        Schema.newBuilder().primaryKey("f0").build(),
        ChangelogMode.upsert())

// register the table under a name and perform an aggregation
tableEnv.createTemporaryView("InputTable", table)
tableEnv
    .executeSql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0")
    .print()

// prints:
// +----+--------------------------------+-------------+
// | op |                           name |       score |
// +----+--------------------------------+-------------+
// | +I |                            Bob |           5 |
// | +I |                          Alice |          12 |
// | -U |                          Alice |          12 |
// | +U |                          Alice |         100 |
// +----+--------------------------------+-------------+
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.common import Row, RowKind
from pyflink.common.typeinfo import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes, StreamTableEnvironment, Schema

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# === EXAMPLE 1 ===

# create a changelog DataStream

ds = env.from_collection([
        Row.of_kind(RowKind.INSERT, "Alice", 12),
        Row.of_kind(RowKind.INSERT, "Bob", 5),
        Row.of_kind(RowKind.UPDATE_BEFORE, "Alice", 12),
        Row.of_kind(RowKind.UPDATE_AFTER, "Alice", 100)],
        type_info=Types.ROW([Types.STRING(),Types.INT()]))

# interpret the DataStream as a Table
table = t_env.from_changelog_stream(ds)


# register the table under a name and perform an aggregation
t_env.create_temporary_view("InputTable", table)
t_env.execute_sql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0").print()

# prints:
# +----+--------------------------------+-------------+
# | op |                           name |       score |
# +----+--------------------------------+-------------+
# | +I |                            Bob |           5 |
# | +I |                          Alice |          12 |
# | -D |                          Alice |          12 |
# | +I |                          Alice |         100 |
# +----+--------------------------------+-------------+

# === EXAMPLE 2 ===

# interpret the stream as an upsert stream (without a need for UPDATE_BEFORE)

# create a changelog DataStream
ds = env.from_collection([
        Row.of_kind(RowKind.INSERT, "Alice", 12),
        Row.of_kind(RowKind.INSERT, "Bob", 5),
        Row.of_kind(RowKind.UPDATE_AFTER, "Alice", 100)],
        type_info=Types.ROW([Types.STRING(),Types.INT()]))

# interpret the DataStream as a Table
table = t_env.from_changelog_stream(
        ds,
        Schema.new_builder().primary_key("f0").build(),
        ChangelogMode.upsert())

# register the table under a name and perform an aggregation
t_env.create_temporary_view("InputTable", table)
t_env.execute_sql("SELECT f0 AS name, SUM(f1) AS score FROM InputTable GROUP BY f0").print()

# prints:
# +----+--------------------------------+-------------+
# | op |                           name |       score |
# +----+--------------------------------+-------------+
# | +I |                            Bob |           5 |
# | +I |                          Alice |          12 |
# | -U |                          Alice |          12 |
# | +U |                          Alice |         100 |
# +----+--------------------------------+-------------+
```
{{< /tab >}}
{{< /tabs >}}

The default `ChangelogMode` shown in example 1 should be sufficient for most use cases as it accepts
all kinds of changes.

However, example 2 shows how to limit the kinds of incoming changes for efficiency by reducing the
number of update messages by 50% using upsert mode. The number of result messages can be reduced by
defining a primary key and upsert changelog mode for `toChangelogStream`.

### Examples for `toChangelogStream`

The following code shows how to use `toChangelogStream` for different scenarios.

{{< tabs "fc4cd538-4345-49ee-b86e-b308f002e069" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.data.StringData;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import static org.apache.flink.table.api.Expressions.*;

// create Table with event-time
tableEnv.executeSql(
    "CREATE TABLE GeneratedTable "
    + "("
    + "  name STRING,"
    + "  score INT,"
    + "  event_time TIMESTAMP_LTZ(3),"
    + "  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND"
    + ")"
    + "WITH ('connector'='datagen')");

Table table = tableEnv.from("GeneratedTable");


// === EXAMPLE 1 ===

// convert to DataStream in the simplest and most general way possible (no event-time)

Table simpleTable = tableEnv
    .fromValues(row("Alice", 12), row("Alice", 2), row("Bob", 12))
    .as("name", "score")
    .groupBy($("name"))
    .select($("name"), $("score").sum());

tableEnv
    .toChangelogStream(simpleTable)
    .executeAndCollect()
    .forEachRemaining(System.out::println);

// prints:
// +I[Bob, 12]
// +I[Alice, 12]
// -U[Alice, 12]
// +U[Alice, 14]


// === EXAMPLE 2 ===

// convert to DataStream in the simplest and most general way possible (with event-time)

DataStream<Row> dataStream = tableEnv.toChangelogStream(table);

// since `event_time` is a single time attribute in the schema, it is set as the
// stream record's timestamp by default; however, at the same time, it remains part of the Row

dataStream.process(
    new ProcessFunction<Row, Void>() {
        @Override
        public void processElement(Row row, Context ctx, Collector<Void> out) {

             // prints: [name, score, event_time]
             System.out.println(row.getFieldNames(true));

             // timestamp exists twice
             assert ctx.timestamp() == row.<Instant>getFieldAs("event_time").toEpochMilli();
        }
    });
env.execute();


// === EXAMPLE 3 ===

// convert to DataStream but write out the time attribute as a metadata column which means
// it is not part of the physical schema anymore

DataStream<Row> dataStream = tableEnv.toChangelogStream(
    table,
    Schema.newBuilder()
        .column("name", "STRING")
        .column("score", "INT")
        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
        .build());

// the stream record's timestamp is defined by the metadata; it is not part of the Row

dataStream.process(
    new ProcessFunction<Row, Void>() {
        @Override
        public void processElement(Row row, Context ctx, Collector<Void> out) {

            // prints: [name, score]
            System.out.println(row.getFieldNames(true));

            // timestamp exists once
            System.out.println(ctx.timestamp());
        }
    });
env.execute();


// === EXAMPLE 4 ===

// for advanced users, it is also possible to use more internal data structures for efficiency

// note that this is only mentioned here for completeness because using internal data structures
// adds complexity and additional type handling

// however, converting a TIMESTAMP_LTZ column to `Long` or STRING to `byte[]` might be convenient,
// also structured types can be represented as `Row` if needed

DataStream<Row> dataStream = tableEnv.toChangelogStream(
    table,
    Schema.newBuilder()
        .column(
            "name",
            DataTypes.STRING().bridgedTo(StringData.class))
        .column(
            "score",
            DataTypes.INT())
        .column(
            "event_time",
            DataTypes.TIMESTAMP_LTZ(3).bridgedTo(Long.class))
        .build());

// leads to a stream of Row(name: StringData, score: Integer, event_time: Long)
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.table.api._
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import java.time.Instant

// create Table with event-time
tableEnv.executeSql(
  """
  CREATE TABLE GeneratedTable (
    name STRING,
    score INT,
    event_time TIMESTAMP_LTZ(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
  )
  WITH ('connector'='datagen')
  """
)

val table = tableEnv.from("GeneratedTable")


// === EXAMPLE 1 ===

// convert to DataStream in the simplest and most general way possible (no event-time)

val simpleTable = tableEnv
    .fromValues(row("Alice", 12), row("Alice", 2), row("Bob", 12))
    .as("name", "score")
    .groupBy($"name")
    .select($"name", $"score".sum())

tableEnv
    .toChangelogStream(simpleTable)
    .executeAndCollect()
    .foreach(println)

// prints:
// +I[Bob, 12]
// +I[Alice, 12]
// -U[Alice, 12]
// +U[Alice, 14]


// === EXAMPLE 2 ===

// convert to DataStream in the simplest and most general way possible (with event-time)

val dataStream: DataStream[Row] = tableEnv.toChangelogStream(table)

// since `event_time` is a single time attribute in the schema, it is set as the
// stream record's timestamp by default; however, at the same time, it remains part of the Row

dataStream.process(new ProcessFunction[Row, Unit] {
    override def processElement(
        row: Row,
        ctx: ProcessFunction[Row, Unit]#Context,
        out: Collector[Unit]): Unit = {

        // prints: [name, score, event_time]
        println(row.getFieldNames(true))

        // timestamp exists twice
        assert(ctx.timestamp() == row.getFieldAs[Instant]("event_time").toEpochMilli)
    }
})
env.execute()


// === EXAMPLE 3 ===

// convert to DataStream but write out the time attribute as a metadata column which means
// it is not part of the physical schema anymore

val dataStream: DataStream[Row] = tableEnv.toChangelogStream(
    table,
    Schema.newBuilder()
        .column("name", "STRING")
        .column("score", "INT")
        .columnByMetadata("rowtime", "TIMESTAMP_LTZ(3)")
        .build())

// the stream record's timestamp is defined by the metadata; it is not part of the Row

dataStream.process(new ProcessFunction[Row, Unit] {
    override def processElement(
        row: Row,
        ctx: ProcessFunction[Row, Unit]#Context,
        out: Collector[Unit]): Unit = {

        // prints: [name, score]
        println(row.getFieldNames(true))

        // timestamp exists once
        println(ctx.timestamp())
    }
})
env.execute()


// === EXAMPLE 4 ===

// for advanced users, it is also possible to use more internal data structures for better
// efficiency

// note that this is only mentioned here for completeness because using internal data structures
// adds complexity and additional type handling

// however, converting a TIMESTAMP_LTZ column to `Long` or STRING to `byte[]` might be convenient,
// also structured types can be represented as `Row` if needed

val dataStream: DataStream[Row] = tableEnv.toChangelogStream(
    table,
    Schema.newBuilder()
        .column(
            "name",
            DataTypes.STRING().bridgedTo(classOf[StringData]))
        .column(
            "score",
            DataTypes.INT())
        .column(
            "event_time",
            DataTypes.TIMESTAMP_LTZ(3).bridgedTo(class[Long]))
        .build())

// leads to a stream of Row(name: StringData, score: Integer, event_time: Long)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.common import Row
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.functions import ProcessFunction
from pyflink.table import DataTypes, StreamTableEnvironment, Schema
from pyflink.table.expressions import col

env = StreamExecutionEnvironment.get_execution_environment()
t_env = StreamTableEnvironment.create(env)

# create Table with event-time
t_env.execute_sql(
    "CREATE TABLE GeneratedTable "
    + "("
    + "  name STRING,"
    + "  score INT,"
    + "  event_time TIMESTAMP_LTZ(3),"
    + "  WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND"
    + ")"
    + "WITH ('connector'='datagen')")

table = t_env.from_path("GeneratedTable")

# === EXAMPLE 1 ===

# convert to DataStream in the simplest and most general way possible (no event-time)
simple_table = t_env.from_elements([Row("Alice", 12), Row("Alice", 2), Row("Bob", 12)],
                                  DataTypes.ROW([DataTypes.FIELD("name", DataTypes.STRING()),
                                                DataTypes.FIELD("score", DataTypes.INT())]))

simple_table = simple_table.group_by(col('name')).select(col('name'), col('score').sum)

t_env.to_changelog_stream(simple_table).print()

env.execute()

# prints:
# +I[Bob, 12]
# +I[Alice, 12]
# -U[Alice, 12]
# +U[Alice, 14]

# === EXAMPLE 2 ===

# convert to DataStream in the simplest and most general way possible (with event-time)

ds = t_env.to_changelog_stream(table)

# since `event_time` is a single time attribute in the schema, it is set as the
# stream record's timestamp by default; however, at the same time, it remains part of the Row

class MyProcessFunction(ProcessFunction):
    def process_element(self, row, ctx):
        print(row)
        assert ctx.timestamp() == row.event_time.to_epoch_milli()

ds.process(MyProcessFunction())

env.execute()

# === EXAMPLE 3 ===

# convert to DataStream but write out the time attribute as a metadata column which means
# it is not part of the physical schema anymore

ds = t_env.to_changelog_stream(
    table,
    Schema.new_builder()
        .column("name", "STRING")
        .column("score", "INT")
        .column_by_metadata("rowtime", "TIMESTAMP_LTZ(3)")
        .build())

class MyProcessFunction(ProcessFunction):
    def process_element(self, row, ctx):
        print(row)
        print(ctx.timestamp())

ds.process(MyProcessFunction())

env.execute()
```
{{< /tab >}}
{{< /tabs >}}

For more information about which conversions are supported for data types in Example 4, see the
[Table API's Data Types page]({{< ref "docs/dev/table/types" >}}).

The behavior of `toChangelogStream(Table).executeAndCollect()` is equal to calling `Table.execute().collect()`.
However, `toChangelogStream(Table)` might be more useful for tests because it allows to access the produced
watermarks in a subsequent `ProcessFunction` in DataStream API.

{{< top >}}

Adding Table API Pipelines to DataStream API
--------------------------------------------

A single Flink job can consist of multiple disconnected pipelines that run next to each other.

Source-to-sink pipelines defined in Table API can be attached *as a whole* to the `StreamExecutionEnvironment`
and will be submitted when calling one of the `execute` methods in the DataStream API.

However, a source does not necessarily have to be a table source but can also be another DataStream
pipeline that was converted to Table API before. Thus, it is possible to use table sinks for DataStream API
programs.

The functionality is available through a specialized `StreamStatementSet` instance created with
`StreamTableEnvironment.createStatementSet()`. By using a statement set, the planner can optimize all
added statements together and come up with one or more end-to-end pipelines that are added to the
`StreamExecutionEnvironment` when calling `StreamStatementSet.attachAsDataStream()`.

The following example shows how to add table programs to a DataStream API program within one job.

{{< tabs "de4cd538-4345-49ee-b86e-b308f002e069" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.*;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

StreamStatementSet statementSet = tableEnv.createStatementSet();

// create some source
TableDescriptor sourceDescriptor =
    TableDescriptor.forConnector("datagen")
        .option("number-of-rows", "3")
        .schema(
            Schema.newBuilder()
                .column("myCol", DataTypes.INT())
                .column("myOtherCol", DataTypes.BOOLEAN())
                .build())
        .build();

// create some sink
TableDescriptor sinkDescriptor = TableDescriptor.forConnector("print").build();

// add a pure Table API pipeline
Table tableFromSource = tableEnv.from(sourceDescriptor);
statementSet.addInsert(sinkDescriptor, tableFromSource);

// use table sinks for the DataStream API pipeline
DataStream<Integer> dataStream = env.fromElements(1, 2, 3);
Table tableFromStream = tableEnv.fromDataStream(dataStream);
statementSet.addInsert(sinkDescriptor, tableFromStream);

// attach both pipelines to StreamExecutionEnvironment
// (the statement set will be cleared after calling this method)
statementSet.attachAsDataStream();

// define other DataStream API parts
env.fromElements(4, 5, 6).addSink(new DiscardingSink<>());

// use DataStream API to submit the pipelines
env.execute();

// prints similar to:
// +I[1618440447, false]
// +I[1259693645, true]
// +I[158588930, false]
// +I[1]
// +I[2]
// +I[3]
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.streaming.api.functions.sink.DiscardingSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env)

val statementSet = tableEnv.createStatementSet()

// create some source
val sourceDescriptor = TableDescriptor.forConnector("datagen")
    .option("number-of-rows", "3")
    .schema(Schema.newBuilder
        .column("myCol", DataTypes.INT)
        .column("myOtherCol", DataTypes.BOOLEAN).build)
    .build

// create some sink
val sinkDescriptor = TableDescriptor.forConnector("print").build

// add a pure Table API pipeline
val tableFromSource = tableEnv.from(sourceDescriptor)
statementSet.addInsert(sinkDescriptor, tableFromSource)

// use table sinks for the DataStream API pipeline
val dataStream = env.fromElements(1, 2, 3)
val tableFromStream = tableEnv.fromDataStream(dataStream)
statementSet.addInsert(sinkDescriptor, tableFromStream)

// attach both pipelines to StreamExecutionEnvironment
// (the statement set will be cleared calling this method)
statementSet.attachAsDataStream()

// define other DataStream API parts
env.fromElements(4, 5, 6).addSink(new DiscardingSink[Int]())

// now use DataStream API to submit the pipelines
env.execute()

// prints similar to:
// +I[1618440447, false]
// +I[1259693645, true]
// +I[158588930, false]
// +I[1]
// +I[2]
// +I[3]
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

Implicit Conversions in Scala
-----------------------------

Users of the Scala API can use all the conversion methods above in a more fluent way by leveraging
Scala's *implicit* feature.

Those implicits are available in the API when importing the package object via `org.apache.flink.table.api.bridge.scala._`.

If enabled, methods such as `toTable` or `toChangelogTable` can be called directly on a `DataStream`
object. Similarly, `toDataStream` and `toChangelogStream` are available on `Table` objects. Furthermore,
`Table` objects will be converted to a changelog stream when requesting a DataStream API specific
method for `DataStream[Row]`.

{{< hint warning >}}
The use of an implicit conversion should always be a conscious decision. One should pay attention whether
the IDE proposes an actual Table API method, or a DataStream API method via implicits.

For example, a `table.execute().collect()` stays in Table API whereas `table.executeAndCollect()` implicitly
uses the DataStream API's `executeAndCollect()` method and therefore forces an API conversion.
{{< /hint >}}

```scala
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.types.Row

val env = StreamExecutionEnvironment.getExecutionEnvironment
val tableEnv = StreamTableEnvironment.create(env)

val dataStream: DataStream[(Int, String)] = env.fromElements((42, "hello"))

// call toChangelogTable() implicitly on the DataStream object
val table: Table = dataStream.toChangelogTable(tableEnv)

// force implicit conversion
val dataStreamAgain1: DataStream[Row] = table

// call toChangelogStream() implicitly on the Table object
val dataStreamAgain2: DataStream[Row] = table.toChangelogStream
```
{{< top >}}

Mapping between TypeInformation and DataType
--------------------------------------------

The DataStream API uses instances of `org.apache.flink.api.common.typeinfo.TypeInformation` to describe
the record type that travels in the stream. In particular, it defines how to serialize and deserialize
records from one DataStream operator to the other. It also helps in serializing state into savepoints
and checkpoints.

The Table API uses custom data structures to represent records internally and exposes `org.apache.flink.table.types.DataType`
to users for declaring the external format into which the data structures are converted for easier
usage in sources, sinks, UDFs, or DataStream API.

`DataType` is richer than `TypeInformation` as it also includes details about the logical SQL type.
Therefore, some details will be added implicitly during the conversion.

Column names and types of a `Table` are automatically derived from the `TypeInformation` of the
`DataStream`. Use `DataStream.getType()` to check whether the type information has been detected
correctly via the DataStream API's reflective type extraction facilities. If the outermost record's
`TypeInformation` is a `CompositeType`, it will be flattened in the first level when deriving a table's
schema.

{{< hint warning >}}
The DataStream API is not always able to extract a more specific `TypeInformation` based on [reflection](https://www.oracle.com/technical-resources/articles/java/javareflection.html).
This often happens silently and leads to `GenericTypeInfo` that is backed by the generic Kryo serializer.

For example, the `Row` class cannot be analyzed reflectively and always needs an explicit type information
declaration. If no proper type information is declared in DataStream API, the row will show as `RAW`
data type and the Table API is unable to access its fields. Use `.map(...).returns(TypeInformation)`
in Java or `.map(...)(TypeInformation)` in Scala to declare type information explicitly.
{{< /hint >}}

### TypeInformation to DataType

The following rules apply when converting `TypeInformation` to a `DataType`:

- All subclasses of `TypeInformation` are mapped to logical types, including nullability that is aligned
with Flink's built-in serializers.

- Subclasses of `TupleTypeInfoBase` are translated into a row (for `Row`) or structured type (for tuples,
POJOs, and case classes).

- `BigDecimal` is converted to `DECIMAL(38, 18)` by default.

- The order of `PojoTypeInfo` fields is determined by a constructor with all fields as its parameters.
If that is not found during the conversion, the field order will be alphabetical.

- `GenericTypeInfo` and other `TypeInformation` that cannot be represented as one of the listed
`org.apache.flink.table.api.DataTypes` will be treated as a black-box `RAW` type. The current session
configuration is used to materialize the serializer of the raw type. Composite nested fields will not
be accessible then.

- See {{< gh_link file="flink-table/flink-table-common/src/main/java/org/apache/flink/table/types/utils/TypeInfoDataTypeConverter.java" name="TypeInfoDataTypeConverter" >}}
for the full translation logic.

Use `DataTypes.of(TypeInformation)` to call the above logic in custom schema declaration or in UDFs.

### DataType to TypeInformation

The table runtime will make sure to properly serialize the output records to the first operator of the
DataStream API.

{{< hint warning >}}
Afterward, the type information semantics of the DataStream API need to be considered.
{{< /hint >}}

{{< top >}}

Legacy Conversion
-----------------

{{< hint info >}}
The following section describes outdated parts of the API that will be removed in future versions.

In particular, these parts might not be well integrated into many recent new features and refactorings
(e.g. `RowKind` is not correctly set, type systems don't integrate smoothly).
{{< /hint >}}

### Convert a DataStream into a Table

A `DataStream` can be directly converted to a `Table` in a `StreamTableEnvironment`.
The schema of the resulting view depends on the data type of the registered collection.

{{< tabs "f2b3f22a-9ffe-4373-bf20-e1e24277e5c4" >}}
{{< tab "Java" >}}
```java
StreamTableEnvironment tableEnv = ...; 
DataStream<Tuple2<Long, String>> stream = ...

Table table2 = tableEnv.fromDataStream(stream, $("myLong"), $("myString"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv: StreamTableEnvironment = ???
val stream: DataStream[(Long, String)] = ???

val table2: Table = tableEnv.fromDataStream(stream, $"myLong", $"myString")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
t_env = ... # type: StreamTableEnvironment

stream = ... # type: DataStream of Types.TUPLE([Types.LONG(), Types.STRING()])

table2 = t_env.from_data_stream(stream, col('my_long'), col('my_stram'))
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}

### Convert a Table into a DataStream 

The results of a `Table` can be converted into a `DataStream`.
In this way, custom `DataStream` programs can be run on the result of a Table API or SQL query.

When converting a `Table` into a `DataStream` you need to specify the data type of the resulting records, i.e., the data type into which the rows of the `Table` are to be converted.
Often the most convenient conversion type is `Row`.
The following list gives an overview of the features of the different options:

- **Row**: fields are mapped by position, arbitrary number of fields, support for `null` values, no type-safe access.
- **POJO**: fields are mapped by name (POJO fields must be named as `Table` fields), arbitrary number of fields, support for `null` values, type-safe access.
- **Case Class**: fields are mapped by position, no support for `null` values, type-safe access.
- **Tuple**: fields are mapped by position, limitation to 22 (Scala) or 25 (Java) fields, no support for `null` values, type-safe access.
- **Atomic Type**: `Table` must have a single field, no support for `null` values, type-safe access.

#### Convert a Table into a DataStream

A `Table` that is the result of a streaming query will be updated dynamically, i.e., it is changing as new records arrive on the query's input streams. Hence, the `DataStream` into which such a dynamic query is converted needs to encode the updates of the table. 

There are two modes to convert a `Table` into a `DataStream`:

1. **Append Mode**: This mode can only be used if the dynamic `Table` is only modified by `INSERT` changes, i.e., it is append-only and previously emitted results are never updated.
2. **Retract Mode**: This mode can always be used. It encodes `INSERT` and `DELETE` changes with a `boolean` flag.

{{< tabs "cbee265a-216d-44c7-8423-cd5cd7f56e35" >}}
{{< tab "Java" >}}
```java
StreamTableEnvironment tableEnv = ...; 

Table table = tableEnv.fromValues(
    DataTypes.Row(
        DataTypes.FIELD("name", DataTypes.STRING()),
        DataTypes.FIELD("age", DataTypes.INT()),
    row("john", 35),
    row("sarah", 32));

// Convert the Table into an append DataStream of Row by specifying the class
DataStream<Row> dsRow = tableEnv.toAppendStream(table, Row.class);

// Convert the Table into an append DataStream of Tuple2<String, Integer> with TypeInformation
TupleTypeInfo<Tuple2<String, Integer>> tupleType = new TupleTypeInfo<>(Types.STRING(), Types.INT());
DataStream<Tuple2<String, Integer>> dsTuple = tableEnv.toAppendStream(table, tupleType);

// Convert the Table into a retract DataStream of Row.
// A retract stream of type X is a DataStream<Tuple2<Boolean, X>>. 
// The boolean field indicates the type of the change. 
// True is INSERT, false is DELETE.
DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(table, Row.class);

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv: StreamTableEnvironment = ???

// Table with two fields (String name, Integer age)
val table: Table = tableEnv.fromValues(
    DataTypes.Row(
        DataTypes.FIELD("name", DataTypes.STRING()),
        DataTypes.FIELD("age", DataTypes.INT()),
    row("john", 35),
    row("sarah", 32));

// Convert the Table into an append DataStream of Row by specifying the class
val dsRow: DataStream[Row] = tableEnv.toAppendStream[Row](table)

// Convert the Table into an append DataStream of (String, Integer) with TypeInformation
val dsTuple: DataStream[(String, Int)] dsTuple = 
  tableEnv.toAppendStream[(String, Int)](table)

// Convert the Table into a retract DataStream of Row.
// A retract stream of type X is a DataStream<Tuple2<Boolean, X>>. 
// The boolean field indicates the type of the change. 
// True is INSERT, false is DELETE.
val retractStream: DataStream[(Boolean, Row)] = tableEnv.toRetractStream[Row](table)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table import DataTypes
from pyflink.common.typeinfo import Types

t_env = ...

table = t_env.from_elements([("john", 35), ("sarah", 32)], 
              DataTypes.ROW([DataTypes.FIELD("name", DataTypes.STRING()),
                            DataTypes.FIELD("age", DataTypes.INT())]))

# Convert the Table into an append DataStream of Row by specifying the type information
ds_row = t_env.to_append_stream(table, Types.ROW([Types.STRING(), Types.INT()]))

# Convert the Table into an append DataStream of Tuple[str, int] with TypeInformation
ds_tuple = t_env.to_append_stream(table, Types.TUPLE([Types.STRING(), Types.INT()]))

# Convert the Table into a retract DataStream of Row by specifying the type information
# A retract stream of type X is a DataStream of Tuple[bool, X]. 
# The boolean field indicates the type of the change. 
# True is INSERT, false is DELETE.
retract_stream = t_env.to_retract_stream(table, Types.ROW([Types.STRING(), Types.INT()]))
```
{{< /tab >}}
{{< /tabs >}}

**Note:** A detailed discussion about dynamic tables and their properties is given in the [Dynamic Tables](streaming/dynamic_tables.html) document. 

{{< hint warning >}}
Once the Table is converted to a DataStream, please use the `StreamExecutionEnvironment.execute()` method to execute the DataStream program.
{{< /hint >}}

{{< top >}}

### Mapping of Data Types to Table Schema

Flink's DataStream API supports many diverse types.
Composite types such as Tuples (built-in Scala , Flink Java tuples and Python tuples), POJOs, Scala case classes, and Flink's Row type allow for nested data structures with multiple fields that can be accessed in table expressions. Other types are treated as atomic types. In the following, we describe how the Table API converts these types into an internal row representation and show examples of converting a `DataStream` into a `Table`.

The mapping of a data type to a table schema can happen in two ways: **based on the field positions** or **based on the field names**.

**Position-based Mapping**

Position-based mapping can be used to give fields a more meaningful name while keeping the field order. This mapping is available for composite data types *with a defined field order* and atomic types. Composite data types such as tuples, rows, and case classes have such a field order. However, fields of a POJO must be mapped based on the field names (see next section). Fields can be projected out but can't be renamed using an alias `as`(Java and Scala) or `alias`(Python).

When defining a position-based mapping, the specified names must not exist in the input data type, otherwise, the API will assume that the mapping should happen based on the field names. If no field names are specified, the default field names and field order of the composite type are used or `f0` for atomic types.

{{< tabs "44be95d8-c058-462b-b836-1e0f5e3f52c3" >}}
{{< tab "Java" >}}
```java
StreamTableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section;

DataStream<Tuple2<Long, Integer>> stream = ...

// convert DataStream into Table with field "myLong" only
Table table = tableEnv.fromDataStream(stream, $("myLong"));

// convert DataStream into Table with field names "myLong" and "myInt"
Table table = tableEnv.fromDataStream(stream, $("myLong"), $("myInt"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// get a TableEnvironment
val tableEnv: StreamTableEnvironment = ... // see "Create a TableEnvironment" section

val stream: DataStream[(Long, Int)] = ...

// convert DataStream into Table with field "myLong" only
val table: Table = tableEnv.fromDataStream(stream, $"myLong")

// convert DataStream into Table with field names "myLong" and "myInt"
val table: Table = tableEnv.fromDataStream(stream, $"myLong", $"myInt")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import col

# get a TableEnvironment
t_env = ... # see "Create a TableEnvironment" section

stream = ... # type: DataStream of Types.Tuple([Types.LONG(), Types.INT()])

# convert DataStream into Table with field "my_long" only
table = t_env.from_data_stream(stream, col('my_long'))

# convert DataStream into Table with field names "my_long" and "my_int"
table = t_env.from_data_stream(stream, col('my_long'), col('my_int'))
```
{{< /tab >}}
{{< /tabs >}}

**Name-based Mapping**

Name-based mapping can be used for any data type, including POJOs. It is the most flexible way of defining a table schema mapping. All fields in the mapping are referenced by name and can be possibly renamed using an alias `as`. Fields can be reordered and projected out.

If no field names are specified, the default field names and field order of the composite type are used or `f0` for atomic types.

{{< tabs "5885a48d-7d6b-4b08-9993-ce7c8f1bec35" >}}
{{< tab "Java" >}}
```java
StreamTableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

DataStream<Tuple2<Long, Integer>> stream = ...

// convert DataStream into Table with field "f1" only
Table table = tableEnv.fromDataStream(stream, $("f1"));

// convert DataStream into Table with swapped fields
Table table = tableEnv.fromDataStream(stream, $("f1"), $("f0"));

// convert DataStream into Table with swapped fields and field names "myInt" and "myLong"
Table table = tableEnv.fromDataStream(stream, $("f1").as("myInt"), $("f0").as("myLong"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// get a TableEnvironment
val tableEnv: StreamTableEnvironment = ... // see "Create a TableEnvironment" section

val stream: DataStream[(Long, Int)] = ...

// convert DataStream into Table with field "_2" only
val table: Table = tableEnv.fromDataStream(stream, $"_2")

// convert DataStream into Table with swapped fields
val table: Table = tableEnv.fromDataStream(stream, $"_2", $"_1")

// convert DataStream into Table with swapped fields and field names "myInt" and "myLong"
val table: Table = tableEnv.fromDataStream(stream, $"_2" as "myInt", $"_1" as "myLong")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import col

# get a TableEnvironment
t_env = ... # see "Create a TableEnvironment" section

stream = ... # type: DataStream of Types.Tuple([Types.LONG(), Types.INT()])

# convert DataStream into Table with field "f1" only
table = t_env.from_data_stream(stream, col('f1'))

# convert DataStream into Table with swapped fields
table = t_env.from_data_stream(stream, col('f1'), col('f0'))

# convert DataStream into Table with swapped fields and field names "my_int" and "my_long"
table = t_env.from_data_stream(stream, col('f1').alias('my_int'), col('f0').alias('my_long'))
```
{{< /tab >}}
{{< /tabs >}}

#### Atomic Types

Flink treats primitives (`Integer`, `Double`, `String`) or generic types (types that cannot be analyzed and decomposed) as atomic types.
A `DataStream` of an atomic type is converted into a `Table` with a single column.
The type of the column is inferred from the atomic type. The name of the column can be specified.

{{< tabs "424695df-5e3a-4a8d-89cd-037b5d6a1a36" >}}
{{< tab "Java" >}}
```java
StreamTableEnvironment tableEnv = ...;

DataStream<Long> stream = ...

// Convert DataStream into Table with field name "myLong"
Table table = tableEnv.fromDataStream(stream, $("myLong"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv: StreamTableEnvironment = ???

val stream: DataStream[Long] = ...

// Convert DataStream into Table with default field name "f0"
val table: Table = tableEnv.fromDataStream(stream)

// Convert DataStream into Table with field name "myLong"
val table: Table = tableEnv.fromDataStream(stream, $"myLong")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
from pyflink.table.expressions import col

t_env = ... 

stream = ... # types: DataStream of Types.Long()

# Convert DataStream into Table with default field name "f0"
table = t_env.from_data_stream(stream)

# Convert DataStream into Table with field name "my_long"
table = t_env.from_data_stream(stream, col('my_long'))
```
{{< /tab >}}
{{< /tabs >}}

#### Tuples (Scala, Java, Python) and Case Classes (Scala only)

{{< tabs "27c312af-7427-401c-a1b1-de9fe39f7b59" >}}
{{< tab "Java" >}}
Flink provides its own tuple classes for Java.
DataStreams of the the Java tuple classes can be converted into tables.
Fields can be renamed by providing names for all fields (mapping based on position).
If no field names are specified, the default field names are used.
If the original field names (`f0`, `f1`, ... for Flink Tuples) are referenced, the API assumes that the mapping is name-based instead of position-based.
Name-based mapping allows for reordering fields and projection with alias (`as`).

```java
StreamTableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

DataStream<Tuple2<Long, String>> stream = ...

// convert DataStream into Table with renamed field names "myLong", "myString" (position-based)
Table table = tableEnv.fromDataStream(stream, $("myLong"), $("myString"));

// convert DataStream into Table with reordered fields "f1", "f0" (name-based)
Table table = tableEnv.fromDataStream(stream, $("f1"), $("f0"));

// convert DataStream into Table with projected field "f1" (name-based)
Table table = tableEnv.fromDataStream(stream, $("f1"));

// convert DataStream into Table with reordered and aliased fields "myString", "myLong" (name-based)
Table table = tableEnv.fromDataStream(stream, $("f1").as("myString"), $("f0").as("myLong"));
```
{{< /tab >}}
{{< tab "Scala" >}}
Flink supports Scala's built-in tuples.
DataStreams of Scala's built-in tuples can be converted into tables.
Fields can be renamed by providing names for all fields (mapping based on position).
If no field names are specified, the default field names are used.
If the original field names (`_1`, `_2`, ... for Scala Tuples) are referenced, the API assumes that the mapping is name-based instead of position-based.
Name-based mapping allows for reordering fields and projection with alias (`as`).

```scala
// get a TableEnvironment
val tableEnv: StreamTableEnvironment = ... // see "Create a TableEnvironment" section

val stream: DataStream[(Long, String)] = ...

// convert DataStream into Table with field names "myLong", "myString" (position-based)
val table: Table = tableEnv.fromDataStream(stream, $"myLong", $"myString")

// convert DataStream into Table with reordered fields "_2", "_1" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"_2", $"_1")

// convert DataStream into Table with projected field "_2" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"_2")

// convert DataStream into Table with reordered and aliased fields "myString", "myLong" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"_2" as "myString", $"_1" as "myLong")

// define case class
case class Person(name: String, age: Int)
val streamCC: DataStream[Person] = ...

// convert DataStream into Table with field names 'myName, 'myAge (position-based)
val table = tableEnv.fromDataStream(streamCC, $"myName", $"myAge")

// convert DataStream into Table with reordered and aliased fields "myAge", "myName" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"age" as "myAge", $"name" as "myName")

```
{{< /tab >}}
{{< tab "Python" >}}
Flink supports Python's built-in Tuples.
DataStreams of tuples can be converted into tables.
Fields can be renamed by providing names for all fields (mapping based on position).
If no field names are specified, the default field names are used.
If the original field names (`f0`, `f1`, ... ) are referenced, the API assumes that the mapping is name-based instead of position-based.
Name-based mapping allows for reordering fields and projection with alias (`alias`).

```python
from pyflink.table.expressions import col

stream = ... # type: DataStream of Types.TUPLE([Types.LONG(), Types.STRING()])

# convert DataStream into Table with renamed field names "my_long", "my_string" (position-based)
table = t_env.from_data_stream(stream, col('my_long'), col('my_string'))

# convert DataStream into Table with reordered fields "f1", "f0" (name-based)
table = t_env.from_data_stream(stream, col('f1'), col('f0'))

# convert DataStream into Table with projected field "f1" (name-based)
table = t_env.from_data_stream(stream, col('f1'))

# convert DataStream into Table with reordered and aliased fields "my_string", "my_long" (name-based)
table = t_env.from_data_stream(stream, col('f1').alias('my_string'), col('f0').alias('my_long'))

```
{{< /tab >}}
{{< /tabs >}}

#### POJO (Java and Scala)

Flink supports POJOs as composite types. The rules for what determines a POJO are documented [here]({{< ref "docs/dev/datastream/fault-tolerance/serialization/types_serialization" >}}#pojos).

When converting a POJO `DataStream` into a `Table` without specifying field names, the names of the original POJO fields are used. The name mapping requires the original names and cannot be done by positions. Fields can be renamed using an alias (with the `as` keyword), reordered, and projected.

{{< tabs "3606634a-dfb1-4410-9cb3-6ef406f261aa" >}}
{{< tab "Java" >}}
```java
StreamTableEnvironment tableEnv = ...; // see "Create a TableEnvironment" section

// Person is a POJO with fields "name" and "age"
DataStream<Person> stream = ...

// convert DataStream into Table with renamed fields "myAge", "myName" (name-based)
Table table = tableEnv.fromDataStream(stream, $("age").as("myAge"), $("name").as("myName"));

// convert DataStream into Table with projected field "name" (name-based)
Table table = tableEnv.fromDataStream(stream, $("name"));

// convert DataStream into Table with projected and renamed field "myName" (name-based)
Table table = tableEnv.fromDataStream(stream, $("name").as("myName"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
// get a TableEnvironment
val tableEnv: StreamTableEnvironment = ... // see "Create a TableEnvironment" section

// Person is a POJO with field names "name" and "age"
val stream: DataStream[Person] = ...

// convert DataStream into Table with renamed fields "myAge", "myName" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"age" as "myAge", $"name" as "myName")

// convert DataStream into Table with projected field "name" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"name")

// convert DataStream into Table with projected and renamed field "myName" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"name" as "myName")
```
{{< /tab >}}
{{< tab "Python" >}}
Custom PoJo Class is unsupported in PyFlink now.
{{< /tab >}}
{{< /tabs >}}

#### Row

The `Row` data type supports an arbitrary number of fields and fields with `null` values. Field names can be specified via a `RowTypeInfo` or when converting a `Row` `DataStream` into a `Table`.
The row type supports mapping of fields by position and by name.
Fields can be renamed by providing names for all fields (mapping based on position) or selected individually for projection/ordering/renaming (mapping based on name).

{{< tabs "8a3a527b-820f-4eb0-9d6f-cc546c988888" >}}
{{< tab "Java" >}}
```java
StreamTableEnvironment tableEnv = ...; 

// DataStream of Row with two fields "name" and "age" specified in `RowTypeInfo`
DataStream<Row> stream = ...

// Convert DataStream into Table with renamed field names "myName", "myAge" (position-based)
Table table = tableEnv.fromDataStream(stream, $("myName"), $("myAge"));

// Convert DataStream into Table with renamed fields "myName", "myAge" (name-based)
Table table = tableEnv.fromDataStream(stream, $("name").as("myName"), $("age").as("myAge"));

// Convert DataStream into Table with projected field "name" (name-based)
Table table = tableEnv.fromDataStream(stream, $("name"));

// Convert DataStream into Table with projected and renamed field "myName" (name-based)
Table table = tableEnv.fromDataStream(stream, $("name").as("myName"));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val tableEnv: StreamTableEnvironment = ???

// DataStream of Row with two fields "name" and "age" specified in `RowTypeInfo`
val stream: DataStream[Row] = ...

// Convert DataStream into Table with renamed field names "myName", "myAge" (position-based)
val table: Table = tableEnv.fromDataStream(stream, $"myName", $"myAge")

// Convert DataStream into Table with renamed fields "myName", "myAge" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"name" as "myName", $"age" as "myAge")

// Convert DataStream into Table with projected field "name" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"name")

// Convert DataStream into Table with projected and renamed field "myName" (name-based)
val table: Table = tableEnv.fromDataStream(stream, $"name" as "myName")
```
{{< /tab >}}

{{< tab "Python" >}}
```python
from pyflink.table.expressions import col

t_env = ...; 

# DataStream of Row with two fields "name" and "age" specified in `RowTypeInfo`
stream = ...

# Convert DataStream into Table with renamed field names "my_name", "my_age" (position-based)
table = t_env.from_data_stream(stream, col('my_name'), col('my_age'))

# Convert DataStream into Table with renamed fields "my_name", "my_age" (name-based)
table = t_env.from_data_stream(stream, col('name').alias('my_name'), col('age').alias('my_age'))

# Convert DataStream into Table with projected field "name" (name-based)
table = t_env.from_data_stream(stream, col('name'))

# Convert DataStream into Table with projected and renamed field "my_name" (name-based)
table = t_env.from_data_stream(stream, col('name').alias("my_name"))
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
