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

A `TableSource` provides access to data which is stored in external systems (database, key-value store, message queue) or files. After a [TableSource is registered in a TableEnvironment](common.html#register-a-tablesource) it can accessed by [Table API](tableApi.html) or [SQL](sql.html) queries.

A TableSink [emits a Table](common.html#emit-a-table) to an external storage system, such as a database, key-value store, message queue, or file system (in different encodings, e.g., CSV, Parquet, or ORC). 

Have a look at the [common concepts and API](common.html) page for details how to [register a TableSource](common.html#register-a-tablesource) and how to [emit a Table through a TableSink](common.html#emit-a-table).

* This will be replaced by the TOC
{:toc}

Provided TableSources
---------------------

Currently, Flink provides the `CsvTableSource` to read CSV files and a few table sources to read JSON or Avro data from Kafka.
A custom `TableSource` can be defined by implementing the `BatchTableSource` or `StreamTableSource` interface. See section on [defining a custom TableSource](#define-a-tablesource) for details.

| **Class name** | **Maven dependency** | **Batch?** | **Streaming?** | **Description**
| `CsvTableSource` | `flink-table` | Y | Y | A simple source for CSV files.
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
    .commentPrefix("%")
    .build();
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
    .build
{% endhighlight %}
</div>
</div>

{% top %}

Provided TableSinks
-------------------

The following table lists the `TableSink`s which are provided with Flink.

| **Class name** | **Maven dependency** | **Batch?** | **Streaming?** | **Description**
| `CsvTableSink` | `flink-table` | Y | Append | A simple sink for CSV files.
| `JDBCAppendTableSink` | `flink-jdbc` | Y | Append | Writes tables to a JDBC database.
| `Kafka08JsonTableSink` | `flink-connector-kafka-0.8` | N | Append | A Kafka 0.8 sink with JSON encoding.
| `Kafka09JsonTableSink` | `flink-connector-kafka-0.9` | N | Append | A Kafka 0.9 sink with JSON encoding.

All sinks that come with the `flink-table` dependency can be directly used by your Table programs. For all other table sinks, you have to add the respective dependency in addition to the `flink-table` dependency.

A custom `TableSink` can be defined by implementing the `BatchTableSink`, `AppendStreamTableSink`, `RetractStreamTableSink`, or `UpsertStreamTableSink` interface. See section on [defining a custom TableSink](#define-a-tablesink) for details.

{% top %}

### CsvTableSink

The `CsvTableSink` emits a `Table` to one or more CSV files. 

The sink only supports append-only streaming tables. It cannot be used to emit a `Table` that is continuously updated. See the [documentation on Table to Stream conversions](./streaming.html#table-to-stream-conversion) for details. When emitting a streaming table, rows are written at least once (if checkpointing is enabled) and the `CsvTableSink` does not split output files into bucket files but continuously writes to the same files. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

Table table = ...

table.writeToSink(
  new CsvTableSink(
    path,                  // output path 
    "|",                   // optional: delimit files by '|'
    1,                     // optional: write to a single file
    WriteMode.OVERWRITE)); // optional: override existing files

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

val table: Table = ???

table.writeToSink(
  new CsvTableSink(
    path,                             // output path 
    fieldDelim = "|",                 // optional: delimit files by '|'
    numFiles = 1,                     // optional: write to a single file
    writeMode = WriteMode.OVERWRITE)) // optional: override existing files

{% endhighlight %}
</div>
</div>

{% top %}

Define a TableSource
--------------------

A `TableSource` is a generic interface to access to data stored in an external system as a table. It produces a `DataSet` or `DataStream` and provides the type information to derive the schema of the generated table. There are different table sources for batch tables and streaming tables.

Schema information consists of a data type, field names, and corresponding indexes of these names in the data type.

The general interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
TableSource<T> {

  public TypeInformation<T> getReturnType();

  public String explainSource();
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
TableSource[T] {

  def getReturnType: TypeInformation[T]

  def explainSource: String

}
{% endhighlight %}
</div>
</div>

To define a `TableSource` one needs to implement `TableSource#getReturnType`. In this case field names and field indexes are derived from the returned type.

If the `TypeInformation` returned by `getReturnType` does not allow to specify custom field names, it is possible to implement the `DefinedFieldNames` interface in addition.

### BatchTableSource

Defines an external `TableSource` to create a batch table and provides access to its data.

The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
BatchTableSource<T> extends TableSource<T> {

  public DataSet<T> getDataSet(ExecutionEnvironment execEnv);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
BatchTableSource[T] extends TableSource[T] {

  def getDataSet(execEnv: ExecutionEnvironment): DataSet[T]
}
{% endhighlight %}
</div>
</div>

{% top %}

### StreamTableSource

Defines an external `TableSource` to create a streaming table and provides access to its data.

The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamTableSource<T> extends TableSource<T> {

  public DataSet<T> getDataStream(StreamExecutionEnvironment execEnv);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
StreamTableSource[T] extends TableSource[T] {

  def getDataStream(execEnv: StreamExecutionEnvironment): DataSet[T]
}
{% endhighlight %}
</div>
</div>

**Note:** If a Table needs to be processed in event-time, the `DataStream` returned by the `getDataStream()` method must carry timestamps and watermarks. Please see the documentation on [timestamp and watermark assignment]({{ site.baseurl }}/dev/event_timestamps_watermarks.html) for details on how to assign timestamps and watermarks.

**Note:** Time-based operations on streaming tables such as windows in both the [Table API](tableApi.html#group-windows) and [SQL](sql.html#group-windows) require explicitly specified time attributes. 

- `DefinedRowtimeAttribute` provides the `getRowtimeAttribute()` method to specify the name of the event-time time attribute.
- `DefinedProctimeAttribute` provides the `getProctimeAttribute()` method to specify the name of the processing-time time attribute.

Please see the documentation on [time attributes]({{ site.baseurl }}/dev/table/streaming.html#time-attributes) for details.

{% top %}

### ProjectableTableSource

The `ProjectableTableSource` interface adds support for projection push-down to a `TableSource`. A `TableSource` extending this interface is able to project the fields of the return table.

The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ProjectableTableSource<T> {

  public TableSource<T> projectFields(int[] fields);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
ProjectableTableSource[T] {

  def TableSource[T] projectFields(fields: Array[Int])
}
{% endhighlight %}
</div>
</div>

The `projectFields()` is called with an array that holds the indexes of the required fields. The method returns a new `TableSource` object that returns rows with the requested schema.

{% top %}

### NestedFieldsProjectableTableSource

The `NestedFieldsProjectableTableSource` interface adds support for projection push-down to a `TableSource` with nested fields. A `TableSource` extending this interface is able to project the nested fields of the returned table.

The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
NestedFieldsProjectableTableSource<T> {

  public TableSource<T> projectNestedFields(int[] fields, String[][] nestedFields);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
NestedFieldsProjectableTableSource[T] {

  def projectNestedFields(fields: Array[Int], nestedFields: Array[Array[String]]): TableSource[T]
}
{% endhighlight %}
</div>
</div>

### FilterableTableSource

The `FilterableTableSource` interface adds support for filtering push-down to a `TableSource`. A `TableSource` extending this interface is able to filter records before returning.

The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
FilterableTableSource<T> {

  public TableSource<T> applyPredicate(List<Expression> predicates);

  public boolean isFilterPushedDown();
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
FilterableTableSource[T] {

  def applyPredicate(predicates: java.util.List[Expression]): TableSource[T]

  def isFilterPushedDown: Boolean
}
{% endhighlight %}
</div>
</div>

The optimizer pushes predicates down by calling the `applyPredicate()` method. The `TableSource` can evaluate which predicates to evaluate by itself and which to leave for the framework. Predicates which are evaluated by the `TableSource` must be removed from the `List`. All predicates which remain in the `List` after the method call returns are evaluated by the framework. The `applyPredicate()` method returns a new `TableSource` that evaluates all selected predicates.

The `isFilterPushedDown()` method tells the optimizer whether predicates have been pushed down or not.

{% top %}

Define a TableSink
------------------

A `TableSink` specifies how to emit a `Table` to an external system or location. The interface is generic such that it can support different storage locations and formats. There are different table sinks for batch tables and streaming tables.

The general interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
TableSink<T> {

  public TypeInformation<T> getOutputType();

  public String[] getFieldNames();

  public TypeInformation[] getFieldTypes();

  public TableSink<T> configure(String[] fieldNames, TypeInformation[] fieldTypes);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
TableSink[T] {

  def getOutputType: TypeInformation<T>

  def getFieldNames: Array[String]

  def getFieldTypes: Array[TypeInformation]

  def configure(fieldNames: Array[String], fieldTypes: Array[TypeInformation]): TableSink[T]
}
{% endhighlight %}
</div>
</div>

The `TableSink#configure` method is called to pass the schema of the Table (field names and types) to emit to the `TableSink`. The method must return a new instance of the TableSink which is configured to emit the provided Table schema.

### BatchTableSink

Defines an external `TableSink` to emit a batch table.

The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
BatchTableSink<T> extends TableSink<T> {

  public void emitDataSet(DataSet<T> dataSet);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
BatchTableSink[T] extends TableSink[T] {

  def emitDataSet(dataSet: DataSet[T]): Unit
}
{% endhighlight %}
</div>
</div>

{% top %}

### AppendStreamTableSink

Defines an external `TableSink` to emit a streaming table with only insert changes.

The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
AppendStreamTableSink<T> extends TableSink<T> {

  public void emitDataStream(DataStream<T> dataStream);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
AppendStreamTableSink[T] extends TableSink[T] {

  def emitDataStream(dataStream: DataStream<T>): Unit
}
{% endhighlight %}
</div>
</div>

If the table is also modified by update or delete changes, a `TableException` will be thrown.

{% top %}

### RetractStreamTableSink

Defines an external `TableSink` to emit a streaming table with insert, update, and delete changes.

The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
RetractStreamTableSink<T> extends TableSink<Tuple2<Boolean, T>> {

  public TypeInformation<T> getRecordType();

  public void emitDataStream(DataStream<Tuple2<Boolean, T>> dataStream);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
RetractStreamTableSink[T] extends TableSink[Tuple2[Boolean, T]] {

  def getRecordType: TypeInformation[T]

  def emitDataStream(dataStream: DataStream[Tuple2[Boolean, T]]): Unit
}
{% endhighlight %}
</div>
</div>

The table will be converted into a stream of accumulate and retraction messages which are encoded as Java `Tuple2`. The first field is a boolean flag to indicate the message type (`true` indicates insert, `false` indicates delete). The second field holds the record of the requested type `T`.

{% top %}

### UpsertStreamTableSink

Defines an external `TableSink` to emit a streaming table with insert, update, and delete changes.

The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
UpsertStreamTableSink<T> extends TableSink<Tuple2<Boolean, T>> {

  public void setKeyFields(String[] keys);

  public void setIsAppendOnly(boolean isAppendOnly);

  public TypeInformation<T> getRecordType();

  public void emitDataStream(DataStream<Tuple2<Boolean, T>> dataStream);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
UpsertStreamTableSink[T] extends TableSink[Tuple2[Boolean, T]] {

  def setKeyFields(keys: Array[String]): Unit

  def setIsAppendOnly(isAppendOnly: Boolean): Unit

  def getRecordType: TypeInformation[T]

  def emitDataStream(dataStream: DataStream[Tuple2[Boolean, T]]): Unit
}
{% endhighlight %}
</div>
</div>

The table must be have unique key fields (atomic or composite) or be append-only. If the table does not have a unique key and is not append-only, a `TableException` will be thrown. The unique key of the table is configured by the `UpsertStreamTableSink#setKeyFields()` method.

The table will be converted into a stream of upsert and delete messages which are encoded as a Java `Tuple2`. The first field is a boolean flag to indicate the message type. The second field holds the record of the requested type `T`.

A message with true boolean field is an upsert message for the configured key. A message with false flag is a delete message for the configured key. If the table is append-only, all messages will have a true flag and must be interpreted as insertions.

{% top %}

