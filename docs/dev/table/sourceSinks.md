---
title: "User-defined Sources & Sinks"
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

A `TableSource` provides access to data which is stored in external systems (database, key-value store, message queue) or files. After a [TableSource is registered in a TableEnvironment](common.html#register-a-tablesource) it can be accessed by [Table API](tableApi.html) or [SQL](sql.html) queries.

A `TableSink` [emits a Table](common.html#emit-a-table) to an external storage system, such as a database, key-value store, message queue, or file system (in different encodings, e.g., CSV, Parquet, or ORC).

A `TableFactory` allows for separating the declaration of a connection to an external system from the actual implementation. A table factory creates configured instances of table sources and sinks from normalized, string-based properties. The properties can be generated programmatically using a `Descriptor` or via YAML configuration files for the [SQL Client](sqlClient.html).

Have a look at the [common concepts and API](common.html) page for details how to [register a TableSource](common.html#register-a-tablesource) and how to [emit a Table through a TableSink](common.html#emit-a-table). See the [built-in sources, sinks, and formats](connect.html) page for examples how to use factories.

* This will be replaced by the TOC
{:toc}

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

{% top %}

### Defining a TableSource with Time Attributes

Time-based operations of streaming [Table API](tableApi.html#group-windows) and [SQL](sql.html#group-windows) queries, such as windowed aggregations or joins, require explicitly specified [time attributes]({{ site.baseurl }}/dev/table/streaming.html#time-attributes). 

A `TableSource` defines a time attribute as a field of type `Types.SQL_TIMESTAMP` in its table schema. In contrast to all regular fields in the schema, a time attribute must not be matched to a physical field in the return type of the table source. Instead, a `TableSource` defines a time attribute by implementing a certain interface.

#### Defining a Processing Time Attribute

[Processing time attributes](streaming.html#processing-time) are commonly used in streaming queries. A processing time attribute returns the current wall-clock time of the operator that accesses it. A `TableSource` defines a processing time attribute by implementing the `DefinedProctimeAttribute` interface. The interface looks as follows:

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

<span class="label label-danger">Attention</span> Both `StreamTableSource` and `BatchTableSource` can implement `DefinedProctimeAttribute` and define a processing time attribute. In case of a `BatchTableSource` the processing time field is initialized with the current timestamp during the table scan.

#### Defining a Rowtime Attribute

[Rowtime attributes](streaming.html#event-time) are attributes of type `TIMESTAMP` and handled in a unified way in stream and batch queries.

A table schema field of type `SQL_TIMESTAMP` can be declared as rowtime attribute by specifying 

* the name of the field, 
* a `TimestampExtractor` that computes the actual value for the attribute (usually from one or more other fields), and
* a `WatermarkStrategy` that specifies how watermarks are generated for the the rowtime attribute.

A `TableSource` defines a rowtime attribute by implementing the `DefinedRowtimeAttributes` interface. The interface looks as follows:

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

<span class="label label-danger">Attention</span> Although the `getRowtimeAttributeDescriptors()` method returns a list of descriptors, only a single rowtime attribute is support at the moment. We plan to remove this restriction in the future and support tables with more than one rowtime attribute.

<span class="label label-danger">Attention</span> Both, `StreamTableSource` and `BatchTableSource`, can implement `DefinedRowtimeAttributes` and define a rowtime attribute. In either case, the rowtime field is extracted using the `TimestampExtractor`. Hence, a `TableSource` that implements `StreamTableSource` and `BatchTableSource` and defines a rowtime attribute provides exactly the same data to streaming and batch queries.

##### Provided Timestamp Extractors

Flink provides `TimestampExtractor` implementations for common use cases.

The following `TimestampExtractor` implementations are currently available:

* `ExistingField(fieldName)`: Extracts the value of a rowtime attribute from an existing `LONG`, `SQL_TIMESTAMP`, or timestamp formatted `STRING` field. One example of such a string would be '2018-05-28 12:34:56.000'.
* `StreamRecordTimestamp()`: Extracts the value of a rowtime attribute from the timestamp of the `DataStream` `StreamRecord`. Note, this `TimestampExtractor` is not available for batch table sources.

A custom `TimestampExtractor` can be defined by implementing the corresponding interface.

##### Provided Watermark Strategies

Flink provides `WatermarkStrategy` implementations for common use cases.

The following `WatermarkStrategy` implementations are currently available:

* `AscendingTimestamps`: A watermark strategy for ascending timestamps. Records with timestamps that are out-of-order will be considered late.
* `BoundedOutOfOrderTimestamps(delay)`: A watermark strategy for timestamps that are at most out-of-order by the specified delay.
* `PreserveWatermarks()`: A strategy which indicates the watermarks should be preserved from the underlying `DataStream`.

A custom `WatermarkStrategy` can be defined by implementing the corresponding interface.

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

Define a TableFactory
---------------------

A `TableFactory` allows to create different table-related instances from string-based properties. All available factories are called for matching to the given set of properties and a corresponding factory class.

Factories leverage Java's [Service Provider Interfaces (SPI)](https://docs.oracle.com/javase/tutorial/sound/SPI-intro.html) for discovering. This means that every dependency and JAR file should contain a file `org.apache.flink.table.factories.TableFactory` in the `META_INF/services` resource directory that lists all available table factories that it provides.

Every table factory needs to implement the following interface:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
package org.apache.flink.table.factories;

interface TableFactory {

  Map<String, String> requiredContext();

  List<String> supportedProperties();
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
package org.apache.flink.table.factories

trait TableFactory {

  def requiredContext(): util.Map[String, String]

  def supportedProperties(): util.List[String]
}
{% endhighlight %}
</div>
</div>

* `requiredContext()`: Specifies the context that this factory has been implemented for. The framework guarantees to only match for this factory if the specified set of properties and values are met. Typical properties might be `connector.type`, `format.type`, or `update-mode`. Property keys such as `connector.property-version` and `format.property-version` are reserved for future backwards compatibility cases.
* `supportedProperties`: List of property keys that this factory can handle. This method will be used for validation. If a property is passed that this factory cannot handle, an exception will be thrown. The list must not contain the keys that are specified by the context.

In order to create a specific instance, a factory class can implement one or more interfaces provided in `org.apache.flink.table.factories`:

* `BatchTableSourceFactory`: Creates a batch table source.
* `BatchTableSinkFactory`: Creates a batch table sink.
* `StreamTableSoureFactory`: Creates a stream table source.
* `StreamTableSinkFactory`: Creates a stream table sink.
* `DeserializationSchemaFactory`: Creates a deserialization schema format.
* `SerializationSchemaFactory`: Creates a serialization schema format.

The discovery of a factory happens in multiple stages:

- Discover all available factories.
- Filter by factory class (e.g., `StreamTableSourceFactory`).
- Filter by matching context.
- Filter by supported properties.
- Verify that exactly one factory matches, otherwise throw an `AmbiguousTableFactoryException` or `NoMatchingTableFactoryException`.

The following example shows how to provide a custom streaming source with an additional `connector.debug` property flag for parameterization.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.table.sources.StreamTableSource;
import org.apache.flink.types.Row;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class MySystemTableSourceFactory extends StreamTableSourceFactory<Row> {

  @Override
  public Map<String, String> requiredContext() {
    Map<String, String> context = new HashMap<>();
    context.put("update-mode", "append");
    context.put("connector.type", "my-system");
    return context;
  }

  @Override
  public List<String> supportedProperties() {
    List<String> list = new ArrayList<>();
    list.add("connector.debug");
    return list;
  }

  @Override
  public StreamTableSource<Row> createStreamTableSource(Map<String, String> properties) {
    boolean isDebug = Boolean.valueOf(properties.get("connector.debug"));

    # additional validation of the passed properties can also happen here

    return new MySystemAppendTableSource(isDebug);
  }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import java.util
import org.apache.flink.table.sources.StreamTableSource
import org.apache.flink.types.Row

class MySystemTableSourceFactory extends StreamTableSourceFactory[Row] {

  override def requiredContext(): util.Map[String, String] = {
    val context = new util.HashMap[String, String]()
    context.put("update-mode", "append")
    context.put("connector.type", "my-system")
    context
  }

  override def supportedProperties(): util.List[String] = {
    val properties = new util.ArrayList[String]()
    properties.add("connector.debug")
    properties
  }

  override def createStreamTableSource(properties: util.Map[String, String]): StreamTableSource[Row] = {
    val isDebug = java.lang.Boolean.valueOf(properties.get("connector.debug"))

    # additional validation of the passed properties can also happen here

    new MySystemAppendTableSource(isDebug)
  }
}
{% endhighlight %}
</div>
</div>

{% top %}

### Use a TableFactory in the SQL Client

In a SQL Client environment file, the previously presented factory could be declared as:

{% highlight yaml %}
tables:
 - name: MySystemTable
   type: source
   update-mode: append
   connector:
     type: my-system
     debug: true
{% endhighlight %}

The YAML file is translated into flattened string properties and a table factory is called with those properties that describe the connection to the external system:

{% highlight text %}
update-mode=append
connector.type=my-system
connector.debug=true
{% endhighlight %}

<span class="label label-danger">Attention</span> Properties such as `tables.#.name` or `tables.#.type` are SQL Client specifics and are not passed to any factory. The `type` property decides, depending on the execution environment, whether a `BatchTableSourceFactory`/`StreamTableSourceFactory` (for `source`), a `BatchTableSinkFactory`/`StreamTableSinkFactory` (for `sink`), or both (for `both`) need to discovered.

{% top %}

### Use a TableFactory in the Table & SQL API

For a type-safe, programmatic approach with explanatory Scaladoc/Javadoc, the Table & SQL API offers descriptors in `org.apache.flink.table.descriptors` that translate into string-based properties. See the [built-in descriptors](connect.md) for sources, sinks, and formats as a reference.

A connector for `MySystem` in our example can extend `ConnectorDescriptor` as shown below:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;

/**
  * Connector to MySystem with debug mode.
  */
public class MySystemConnector extends ConnectorDescriptor {

  public final boolean isDebug;

  public MySystemConnector(boolean isDebug) {
    super("my-system", 1, false);
    this.isDebug = isDebug;
  }

  @Override
  public void addConnectorProperties(DescriptorProperties properties) {
    properties.putString("connector.debug", Boolean.toString(isDebug));
  }
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.table.descriptors.ConnectorDescriptor
import org.apache.flink.table.descriptors.DescriptorProperties

/**
  * Connector to MySystem with debug mode.
  */
class MySystemConnector(isDebug: Boolean) extends ConnectorDescriptor("my-system", 1, formatNeeded = false) {
  
  override protected def addConnectorProperties(properties: DescriptorProperties): Unit = {
    properties.putString("connector.debug", isDebug.toString)
  }
}
{% endhighlight %}
</div>
</div>

The descriptor can then be used in the API as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamTableEnvironment tableEnv = // ...

tableEnv
  .connect(new MySystemConnector(true))
  .inAppendMode()
  .registerTableSource("MySystemTable");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val tableEnv: StreamTableEnvironment = // ...

tableEnv
  .connect(new MySystemConnector(isDebug = true))
  .inAppendMode()
  .registerTableSource("MySystemTable")
{% endhighlight %}
</div>
</div>

{% top %}
