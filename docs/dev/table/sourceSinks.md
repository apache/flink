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

### JDBCAppendTableSink

The `JDBCAppendTableSink` emits a `Table` to a JDBC connection. The sink only supports append-only streaming tables. It cannot be used to emit a `Table` that is continuously updated. See the [documentation on Table to Stream conversions](./streaming.html#table-to-stream-conversion) for details. 

The `JDBCAppendTableSink` inserts each `Table` row at least once into the database table (if checkpointing is enabled). However, you can specify the insertion query using <code>REPLACE</code> or <code>INSERT OVERWRITE</code> to perform upsert writes to the database.

To use the JDBC sink, you have to add the JDBC connector dependency (<code>flink-jdbc</code>) to your project. Then you can create the sink using <code>JDBCAppendSinkBuilder</code>:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
  .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
  .setDBUrl("jdbc:derby:memory:ebookshop")
  .setQuery("INSERT INTO books (id) VALUES (?)")
  .setParameterTypes(INT_TYPE_INFO)
  .build();

Table table = ...
table.writeToSink(sink);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val sink: JDBCAppendTableSink = JDBCAppendTableSink.builder()
  .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
  .setDBUrl("jdbc:derby:memory:ebookshop")
  .setQuery("INSERT INTO books (id) VALUES (?)")
  .setParameterTypes(INT_TYPE_INFO)
  .build()

val table: Table = ???
table.writeToSink(sink)
{% endhighlight %}
</div>
</div>

Similar to using <code>JDBCOutputFormat</code>, you have to explicitly specify the name of the JDBC driver, the JDBC URL, the query to be executed, and the field types of the JDBC table. 

{% top %}

Define a TableSource
--------------------

A `TableSource` is a generic interface that gives Table API and SQL queries access to data stored in an external system. It provides the schema of the table and the records that are mapped to rows with the table's schema. Depending on whether the `TableSource` is used in a streaming or batch query, the records are produced as a `DataSet` or `DataStream`. 

If a `TableSource` is used in a streaming query it must implement the `StreamTableSource` interface, if it is used in a batch query it must implement the `BatchTableSource` interface. A `TableSource` can also implement both interfaces and be used in streaming and batch queries. 

`StreamTableSource` and `BatchTableSource` extend the base interface `TableSource` that defines the following methods:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
TableSource<T> {

  public TableSchema getTableSchema();

  public TypeInformation<T> getReturnType();

  public String explainSource();
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
TableSource[T] {

  def getTableSchema: TableSchema

  def getReturnType: TypeInformation[T]

  def explainSource: String

}
{% endhighlight %}
</div>
</div>

* `getTableSchema()`: Returns the schema of the table, i.e., the names and types of the fields of the table. The field types are defined using Flink's `TypeInformation` (see [Table API types](tableApi.html#data-types) and [SQL types](sql.html#data-types)).

* `getReturnType()`: Returns the physical type of the `DataStream` (`StreamTableSource`) or `DataSet` (`BatchTableSource`) and the records that are produced by the `TableSource`.

* `explainSource()`: Returns a String that describes the `TableSource`. This method is optional and used for display purposes only.

The `TableSource` interface separates the logical table schema from the physical type of the returned `DataStream` or `DataSet`. As a consequence, all fields of the table schema (`getTableSchema()`) must be mapped to a field with corresponding type of the physical return type (`getReturnType()`). By default, this mapping is done based on field names. For example, a `TableSource` that defines a table schema with two fields `[name: String, size: Integer]` requires a `TypeInformation` with at least two fields called `name` and `size` of type `String` and `Integer`, respectively. This could be a `PojoTypeInfo` or a `RowTypeInfo` that have two fields named `name` and `size` with matching types. 

However, some types, such as Tuple or CaseClass types, do support custom field names. If a `TableSource` returns a `DataStream` or `DataSet` of a type with fixed field names, it can implement the `DefinedFieldMapping` interface to map field names from the table schema to field names of the physical return type.

### Defining a BatchTableSource

The `BatchTableSource` interface extends the `TableSource` interface and defines one additional method:

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

* `getDataSet(execEnv)`: Returns a `DataSet` with the data of the table. The type of the `DataSet` must be identical to the return type defined by the `TableSource.getReturnType()` method. The `DataSet` can by created using a regular [data source]({{ site.baseurl }}/dev/batch/#data-sources) of the DataSet API. Commonly, a `BatchTableSource` is implemented by wrapping a `InputFormat` or [batch connector]({{ site.baseurl }}/dev/batch/connectors.html).

{% top %}

### Defining a StreamTableSource

The `StreamTableSource` interface extends the `TableSource` interface and defines one additional method: 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamTableSource<T> extends TableSource<T> {

  public DataStream<T> getDataStream(StreamExecutionEnvironment execEnv);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
StreamTableSource[T] extends TableSource[T] {

  def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[T]
}
{% endhighlight %}
</div>
</div>

* `getDataStream(execEnv)`: Returns a `DataStream` with the data of the table. The type of the `DataStream` must be identical to the return type defined by the `TableSource.getReturnType()` method. The `DataStream` can by created using a regular [data source]({{ site.baseurl }}/dev/datastream_api.html#data-sources) of the DataStream API. Commonly, a `StreamTableSource` is implemented by wrapping a `SourceFunction` or a [stream connector]({{ site.baseurl }}/dev/connectors/).

### Defining a TableSource with Time Attributes

Time-based operations of streaming [Table API](tableApi.html#group-windows) and [SQL](sql.html#group-windows) queries, such as windowed aggregations or joins, require explicitly specified [time attributes]({{ site.baseurl }}/dev/table/streaming.html#time-attributes). 

A `TableSource` defines a time attribute as a field of type `Types.SQL_TIMESTAMP` in its table schema. In contrast to all regular fields in the schema, a time attribute must not be matched to a physical field in the return type of the table source. Instead, a `TableSource` defines a time attribute by implementing a certain interface.

#### Defining a Processing Time Attribute

A `TableSource` defines a [processing time attribute](streaming.html#processing-time) by implementing the `DefinedProctimeAttribute` interface. The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DefinedProctimeAttribute {

  public String getProctimeAttribute();
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
DefinedProctimeAttribute {

  def getProctimeAttribute: String
}
{% endhighlight %}
</div>
</div>

* `getProctimeAttribute()`: Returns the name of the processing time attribute. The specified attribute must be defined of type `Types.SQL_TIMESTAMP` in the table schema and can be used in time-based operations. A `DefinedProctimeAttribute` table source can define no processing time attribute by returning `null`.

**Note** Both `StreamTableSource` and `BatchTableSource` can implement `DefinedProctimeAttribute` and define a processing time attribute. In case of a `BatchTableSource` the processing time field is initialized with the current timestamp during the table scan.

#### Defining a Rowtime Attribute

A `TableSource` defines a [rowtime attribute](streaming.html#event-time) by implementing the `DefinedRowtimeAttributes` interface. The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DefinedRowtimeAttribute {

  public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors();
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
DefinedRowtimeAttributes {

  def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor]
}
{% endhighlight %}
</div>
</div>

* `getRowtimeAttributeDescriptors()`: Returns a list of `RowtimeAttributeDescriptor`. A `RowtimeAttributeDescriptor` describes a rowtime attribute with the following properties:
  * `attributeName`: The name of the rowtime attribute in the table schema. The field must be defined with type `Types.SQL_TIMESTAMP`.
  * `timestampExtractor`: The timestamp extractor extracts the timestamp from a record with the return type. For example, it can convert convert a Long field into a timestamp or parse a String-encoded timestamp. Flink comes with a set of built-in `TimestampExtractor` implementation for common use cases. It is also possible to provide a custom implementation.
  * `watermarkStrategy`: The watermark strategy defines how watermarks are generated for the rowtime attribute. Flink comes with a set of built-in `WatermarkStrategy` implementations for common use cases. It is also possible to provide a custom implementation.
* **Note** Although the `getRowtimeAttributeDescriptors()` method returns a list of descriptors, only a single rowtime attribute is support at the moment. We plan to remove this restriction in the future and support tables with more than one rowtime attribute.

**IMPORTANT** Both, `StreamTableSource` and `BatchTableSource`, can implement `DefinedRowtimeAttributes` and define a rowtime attribute. In either case, the rowtime field is extracted using the `TimestampExtractor`. Hence, a `TableSource` that implements `StreamTableSource` and `BatchTableSource` and defines a rowtime attribute provides exactly the same data to streaming and batch queries.

{% top %}

### Defining a TableSource with Projection Push-Down

A `TableSource` supports projection push-down by implementing the `ProjectableTableSource` interface. The interface defines a single method:

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

  def projectFields(fields: Array[Int]): TableSource[T]
}
{% endhighlight %}
</div>
</div>

* `projectFields(fields)`: Returns a *copy* of the `TableSource` with adjusted physical return type. The `fields` parameter provides the indexes of the fields that must be provided by the `TableSource`. The indexes relate to the `TypeInformation` of the physical return type, *not* to the logical table schema. The copied `TableSource` must adjust its return type and the returned `DataStream` or `DataSet`. The `TableSchema` of the copied `TableSource` must not be changed, i.e, it must be the same as the original `TableSource`. If the `TableSource` implements the `DefinedFieldMapping` interface, the field mapping must be adjusted to the new return type.

The `ProjectableTableSource` adds support to project flat fields. If the `TableSource` defines a table with nested schema, it can implement the `NestedFieldsProjectableTableSource` to extend the projection to nested fields. The `NestedFieldsProjectableTableSource` is defined as follows:

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

* `projectNestedField(fields, nestedFields)`: Returns a *copy* of the `TableSource` with adjusted physical return type. Fields of the physical return type may be removed or reordered but their type must not be changed. The contract of this method is essentially the same as for the `ProjectableTableSource.projectFields()` method. In addition, the `nestedFields` parameter contains for each field index in the `fields` list, a list of paths to all nested fields that are accessed by the query. All other nested fields do not need to be read, parsed, and set in the records that are produced by the `TableSource`. **IMPORTANT** the types of the projected fields must not be changed but unused fields may be set to null or to a default value.

{% top %}

### Defining a TableSource with Filter Push-Down

The `FilterableTableSource` interface adds support for filter push-down to a `TableSource`. A `TableSource` extending this interface is able to filter records such that the returned `DataStream` or `DataSet` returns fewer records.

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

* `applyPredicate(predicates)`: Returns a *copy* of the `TableSource` with added predicates. The `predicates` parameter is a mutable list of conjunctive predicates that are "offered" to the `TableSource`. The `TableSource` accepts to evaluate a predicate by removing it from the list. Predicates that are left in the list will be evaluated by a subsequent filter operator. 
* `isFilterPushedDown()`: Returns true if the `applyPredicate()` method was called before. Hence, `isFilterPushedDown()` must return true for all `TableSource` instances returned from a `applyPredicate()` call.

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

