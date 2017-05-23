---
title: "Table Sources & Sinks"
nav-parent_id: tableapi
nav-pos: 40
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

* This will be replaced by the TOC
{:toc}

Provided TableSources
---------------------

**TODO: extend and complete**

Currently, Flink provides the `CsvTableSource` to read CSV files and various `TableSources` to read JSON or Avro objects from Kafka.
A custom `TableSource` can be defined by implementing the `BatchTableSource` or `StreamTableSource` interface.

| **Class name** | **Maven dependency** | **Batch?** | **Streaming?** | **Description**
| `CsvTableSouce` | `flink-table` | Y | Y | A simple source for CSV files.
| `Kafka08JsonTableSource` | `flink-connector-kafka-0.8` | N | Y | A Kafka 0.8 source for JSON data.
| `Kafka08AvroTableSource` | `flink-connector-kafka-0.8` | N | Y | A Kafka 0.8 source for Avro data.
| `Kafka09JsonTableSource` | `flink-connector-kafka-0.9` | N | Y | A Kafka 0.9 source for JSON data.
| `Kafka09AvroTableSource` | `flink-connector-kafka-0.9` | N | Y | A Kafka 0.9 source for Avro data.
| `Kafka010JsonTableSource` | `flink-connector-kafka-0.10` | N | Y | A Kafka 0.10 source for JSON data.
| `Kafka010AvroTableSource` | `flink-connector-kafka-0.10` | N | Y | A Kafka 0.10 source for Avro data.

All sources that come with the `flink-table` dependency can be directly used by your Table programs. For all other table sources, you have to add the respective dependency in addition to the `flink-table` dependency.

{% top %}

### KafkaJsonTableSource

To use the Kafka JSON source, you have to add the Kafka connector dependency to your project:

  - `flink-connector-kafka-0.8` for Kafka 0.8,
  - `flink-connector-kafka-0.9` for Kafka 0.9, or
  - `flink-connector-kafka-0.10` for Kafka 0.10, respectively.

You can then create the source as follows (example for Kafka 0.8):
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// specify JSON field names and types
TypeInformation<Row> typeInfo = Types.ROW(
  new String[] { "id", "name", "score" },
  new TypeInformation<?>[] { Types.INT(), Types.STRING(), Types.DOUBLE() }
);

KafkaJsonTableSource kafkaTableSource = new Kafka08JsonTableSource(
    kafkaTopic,
    kafkaProperties,
    typeInfo);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// specify JSON field names and types
val typeInfo = Types.ROW(
  Array("id", "name", "score"),
  Array(Types.INT, Types.STRING, Types.DOUBLE)
)

val kafkaTableSource = new Kafka08JsonTableSource(
    kafkaTopic,
    kafkaProperties,
    typeInfo)
{% endhighlight %}
</div>
</div>

By default, a missing JSON field does not fail the source. You can configure this via:

```java
// Fail on missing JSON field
tableSource.setFailOnMissingField(true);
```

You can work with the Table as explained in the rest of the Table API guide:

```java
tableEnvironment.registerTableSource("kafka-source", kafkaTableSource);
Table result = tableEnvironment.scan("kafka-source");
```

{% top %}

### KafkaAvroTableSource

The `KafkaAvroTableSource` allows you to read Avro's `SpecificRecord` objects from Kafka.

To use the Kafka Avro source, you have to add the Kafka connector dependency to your project:

  - `flink-connector-kafka-0.8` for Kafka 0.8,
  - `flink-connector-kafka-0.9` for Kafka 0.9, or
  - `flink-connector-kafka-0.10` for Kafka 0.10, respectively.

You can then create the source as follows (example for Kafka 0.8):
<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// pass the generated Avro class to the TableSource
Class<? extends SpecificRecord> clazz = MyAvroType.class; 

KafkaAvroTableSource kafkaTableSource = new Kafka08AvroTableSource(
    kafkaTopic,
    kafkaProperties,
    clazz);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// pass the generated Avro class to the TableSource
val clazz = classOf[MyAvroType]

val kafkaTableSource = new Kafka08AvroTableSource(
    kafkaTopic,
    kafkaProperties,
    clazz)
{% endhighlight %}
</div>
</div>

{% top %}

### CsvTableSource

The `CsvTableSource` is already included in `flink-table` without additional dependecies.

The easiest way to create a `CsvTableSource` is by using the enclosed builder `CsvTableSource.builder()`, the builder has the following methods to configure properties:

 - `path(String path)` Sets the path to the CSV file, required.
 - `field(String fieldName, TypeInformation<?> fieldType)` Adds a field with the field name and field type information, can be called multiple times, required. The call order of this method defines also the order of the fields in a row.
 - `fieldDelimiter(String delim)` Sets the field delimiter, `","` by default.
 - `lineDelimiter(String delim)` Sets the line delimiter, `"\n"` by default.
 - `quoteCharacter(Character quote)` Sets the quote character for String values, `null` by default.
 - `commentPrefix(String prefix)` Sets a prefix to indicate comments, `null` by default.
 - `ignoreFirstLine()` Ignore the first line. Disabled by default.
 - `ignoreParseErrors()` Skip records with parse error instead to fail. Throwing an exception by default.

You can create the source as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
CsvTableSource csvTableSource = CsvTableSource
    .builder()
    .path("/path/to/your/file.csv")
    .field("name", Types.STRING())
    .field("id", Types.INT())
    .field("score", Types.DOUBLE())
    .field("comments", Types.STRING())
    .fieldDelimiter("#")
    .lineDelimiter("$")
    .ignoreFirstLine()
    .ignoreParseErrors()
    .commentPrefix("%");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val csvTableSource = CsvTableSource
    .builder
    .path("/path/to/your/file.csv")
    .field("name", Types.STRING)
    .field("id", Types.INT)
    .field("score", Types.DOUBLE)
    .field("comments", Types.STRING)
    .fieldDelimiter("#")
    .lineDelimiter("$")
    .ignoreFirstLine
    .ignoreParseErrors
    .commentPrefix("%")
{% endhighlight %}
</div>
</div>

You can work with the Table as explained in the rest of the Table API guide in both stream and batch `TableEnvironment`s:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
tableEnvironment.registerTableSource("mycsv", csvTableSource);

Table streamTable = streamTableEnvironment.scan("mycsv");

Table batchTable = batchTableEnvironment.scan("mycsv");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
tableEnvironment.registerTableSource("mycsv", csvTableSource)

val streamTable = streamTableEnvironment.scan("mycsv")

val batchTable = batchTableEnvironment.scan("mycsv")
{% endhighlight %}
</div>
</div>

{% top %}

Provided TableSinks
-------------------

**TODO**

{% top %}

Define a TableSource
--------------------

### BatchTableSource

**TODO**

{% top %}

### StreamTableSource
* TimestampAssigner
* DefinedRowtimeAttribute / DefinedProctimeAttribute

**TODO**

{% top %}

### ProjectableTableSource

**TODO**

{% top %}

### FilterableTableSource

**TODO**

{% top %}

Define a TableSink
------------------

### BatchTableSink

**TODO**

{% top %}

### AppendStreamTableSink

**TODO**

{% top %}

### RetractStreamTableSink

**TODO**

{% top %}

### UpsertStreamTableSInk

**TODO**

{% top %}

