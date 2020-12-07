---
title: "User-defined Sources & Sinks"
nav-parent_id: tableapi
nav-pos: 130
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

_Dynamic tables_ are the core concept of Flink's Table & SQL API for processing both bounded and unbounded
data in a unified fashion.

Because dynamic tables are only a logical concept, Flink does not own the data itself. Instead, the content
of a dynamic table is stored in external systems (such as databases, key-value stores, message queues) or files.

_Dynamic sources_ and _dynamic sinks_ can be used to read and write data from and to an external system. In
the documentation, sources and sinks are often summarized under the term _connector_.

Flink provides pre-defined connectors for Kafka, Hive, and different file systems. See the [connector section]({% link dev/table/connectors/index.md %})
for more information about built-in table sources and sinks.

This page focuses on how to develop a custom, user-defined connector.

<span class="label label-danger">Attention</span> New table source and table sink interfaces have been
introduced in Flink 1.11 as part of [FLIP-95](https://cwiki.apache.org/confluence/display/FLINK/FLIP-95%3A+New+TableSource+and+TableSink+interfaces).
Also the factory interfaces have been reworked. If necessary, take a look at the [old table sources and sinks page]({% link dev/table/legacySourceSinks.md %}).
Those interfaces are still supported for backwards compatibility.

* This will be replaced by the TOC
{:toc}

Overview
--------

In many cases, implementers don't need to create a new connector from scratch but would like to slightly
modify existing connectors or hook into the existing stack. In other cases, implementers would like to
create specialized connectors.

This section helps for both kinds of use cases. It explains the general architecture of table connectors
from pure declaration in the API to runtime code that will be executed on the cluster.

The filled arrows show how objects are transformed to other objects from one stage to the next stage during
the translation process.

<div style="text-align: center">
  <img width="90%" src="{% link /fig/table_connectors.svg %}" alt="Translation of table connectors" />
</div>

### Metadata

Both Table API and SQL are declarative APIs. This includes the declaration of tables. Thus, executing
a `CREATE TABLE` statement results in updated metadata in the target catalog.

For most catalog implementations, physical data in the external system is not modified for such an
operation. Connector-specific dependencies don't have to be present in the classpath yet. The options declared
in the `WITH` clause are neither validated nor otherwise interpreted.

The metadata for dynamic tables (created via DDL or provided by the catalog) is represented as instances
of `CatalogTable`. A table name will be resolved into a `CatalogTable` internally when necessary.

### Planning

When it comes to planning and optimization of the table program, a `CatalogTable` needs to be resolved
into a `DynamicTableSource` (for reading in a `SELECT` query) and `DynamicTableSink` (for writing in
an `INSERT INTO` statement).

`DynamicTableSourceFactory` and `DynamicTableSinkFactory` provide connector-specific logic for translating
the metadata of a `CatalogTable` into instances of `DynamicTableSource` and `DynamicTableSink`. In most
of the cases, a factory's purpose is to validate options (such as `'port' = '5022'` in the example),
configure encoding/decoding formats (if required), and create a parameterized instance of the table
connector.

By default, instances of `DynamicTableSourceFactory` and `DynamicTableSinkFactory` are discovered using
Java's [Service Provider Interfaces (SPI)](https://docs.oracle.com/javase/tutorial/sound/SPI-intro.html). The
`connector` option (such as `'connector' = 'custom'` in the example) must correspond to a valid factory
identifier.

Although it might not be apparent in the class naming, `DynamicTableSource` and `DynamicTableSink`
can also be seen as stateful factories that eventually produce concrete runtime implementation for reading/writing
the actual data.

The planner uses the source and sink instances to perform connector-specific bidirectional communication
until an optimal logical plan could be found. Depending on the optionally declared ability interfaces (e.g.
`SupportsProjectionPushDown` or `SupportsOverwrite`), the planner might apply changes to an instance and
thus mutate the produced runtime implementation.

### Runtime

Once the logical planning is complete, the planner will obtain the _runtime implementation_ from the table
connector. Runtime logic is implemented in Flink's core connector interfaces such as `InputFormat` or `SourceFunction`.

Those interfaces are grouped by another level of abstraction as subclasses of `ScanRuntimeProvider`,
`LookupRuntimeProvider`, and `SinkRuntimeProvider`.

For example, both `OutputFormatProvider` (providing `org.apache.flink.api.common.io.OutputFormat`) and `SinkFunctionProvider` (providing `org.apache.flink.streaming.api.functions.sink.SinkFunction`) are concrete instances of `SinkRuntimeProvider`
that the planner can handle.

{% top %}

Extension Points
----------------

This section explains the available interfaces for extending Flink's table connectors.

### Dynamic Table Factories

Dynamic table factories are used to configure a dynamic table connector for an external storage system from catalog
and session information.

`org.apache.flink.table.factories.DynamicTableSourceFactory` can be implemented to construct a `DynamicTableSource`.

`org.apache.flink.table.factories.DynamicTableSinkFactory` can be implemented to construct a `DynamicTableSink`.

By default, the factory is discovered using the value of the `connector` option as the factory identifier
and Java's Service Provider Interface.

In JAR files, references to new implementations can be added to the service file:

`META-INF/services/org.apache.flink.table.factories.Factory`

The framework will check for a single matching factory that is uniquely identified by factory identifier
and requested base class (e.g. `DynamicTableSourceFactory`).

The factory discovery process can be bypassed by the catalog implementation if necessary. For this, a
catalog needs to return an instance that implements the requested base class in `org.apache.flink.table.catalog.Catalog#getFactory`.

### Dynamic Table Source

By definition, a dynamic table can change over time.

When reading a dynamic table, the content can either be considered as:
- A changelog (finite or infinite) for which all changes are consumed continuously until the changelog
  is exhausted. This is represented by the `ScanTableSource` interface.
- A continuously changing or very large external table whose content is usually never read entirely
  but queried for individual values when necessary. This is represented by the `LookupTableSource`
  interface.

A class can implement both of these interfaces at the same time. The planner decides about their usage depending
on the specified query.

#### Scan Table Source

A `ScanTableSource` scans all rows from an external storage system during runtime.

The scanned rows don't have to contain only insertions but can also contain updates and deletions. Thus,
the table source can be used to read a (finite or infinite) changelog. The returned _changelog mode_ indicates
the set of changes that the planner can expect during runtime.

For regular batch scenarios, the source can emit a bounded stream of insert-only rows.

For regular streaming scenarios, the source can emit an unbounded stream of insert-only rows.

For change data capture (CDC) scenarios, the source can emit bounded or unbounded streams with insert,
update, and delete rows.

A table source can implement further ability interfaces such as `SupportsProjectionPushDown` that might
mutate an instance during planning. All abilities can be found in the `org.apache.flink.table.connector.source.abilities`
package and are listed in the [source abilities table](#source-abilities).

The runtime implementation of a `ScanTableSource` must produce internal data structures. Thus, records
must be emitted as `org.apache.flink.table.data.RowData`. The framework provides runtime converters such
that a source can still work on common data structures and perform a conversion at the end.

#### Lookup Table Source

A `LookupTableSource` looks up rows of an external storage system by one or more keys during runtime.

Compared to `ScanTableSource`, the source does not have to read the entire table and can lazily fetch individual
values from a (possibly continuously changing) external table when necessary.

Compared to `ScanTableSource`, a `LookupTableSource` does only support emitting insert-only changes currently.

Further abilities are not supported. See the documentation of `org.apache.flink.table.connector.source.LookupTableSource`
for more information.

The runtime implementation of a `LookupTableSource` is a `TableFunction` or `AsyncTableFunction`. The function
will be called with values for the given lookup keys during runtime.

#### Source Abilities

<table class="table table-bordered">
    <thead>
        <tr>
        <th class="text-left" style="width: 25%">Interface</th>
        <th class="text-center" style="width: 75%">Description</th>
        </tr>
    </thead>
    <tbody>
    <tr>
        <td><a href='https://github.com/apache/flink/blob/master/flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/source/abilities/SupportsFilterPushDown.java'>SupportsFilterPushDown</a></td>
        <td>Enables to push down the filter into the <code>DynamicTableSource</code>. For efficiency, a source can
        push filters further down in order to be close to the actual data generation.</td>
    </tr>
    <tr>
        <td><a href='https://github.com/apache/flink/blob/master/flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/source/abilities/SupportsLimitPushDown.java'>SupportsLimitPushDown</a></td>
        <td>Enables to push down a limit (the expected maximum number of produced records) into a <code>DynamicTableSource</code>.</td>
    </tr>
    <tr>
        <td><a href='https://github.com/apache/flink/blob/master/flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/source/abilities/SupportsPartitionPushDown.java'>SupportsPartitionPushDown</a></td>
        <td>Enables to pass available partitions to the planner and push down partitions into a <code>DynamicTableSource</code>.
        During the runtime, the source will only read data from the passed partition list for efficiency.</td>
    </tr>
    <tr>
        <td><a href='https://github.com/apache/flink/blob/master/flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/source/abilities/SupportsProjectionPushDown.java'>SupportsProjectionPushDown</a> </td>
        <td>Enables to push down a (possibly nested) projection into a <code>DynamicTableSource</code>. For efficiency,
        a source can push a projection further down in order to be close to the actual data generation. If the source
        also implements <code>SupportsReadingMetadata</code>, the source will also read the required metadata only.
        </td>
    </tr>
    <tr>
        <td><a href='https://github.com/apache/flink/blob/master/flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/source/abilities/SupportsReadingMetadata.java'>SupportsReadingMetadata</a></td>
        <td>Enables to read metadata columns from a <code>DynamicTableSource</code>. The source
        is responsible to add the required metadata at the end of the produced rows. This includes
        potentially forwarding metadata column from contained formats.</td>
    </tr>
    <tr>
        <td><a href='https://github.com/apache/flink/blob/master/flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/source/abilities/SupportsWatermarkPushDown.java'>SupportsWatermarkPushDown</a></td>
        <td>Enables to push down a watermark strategy into a <code>DynamicTableSource</code>. The watermark
        strategy is a builder/factory for timestamp extraction and watermark generation. During the runtime, the
        watermark generator is located inside the source and is able to generate per-partition watermarks.</td>
    </tr>
    </tbody>
</table>

<span class="label label-danger">Attention</span> The interfaces above are currently only available for
`ScanTableSource`, not for `LookupTableSource`.

### Dynamic Table Sink

By definition, a dynamic table can change over time.

When writing a dynamic table, the content can always be considered as a changelog (finite or infinite)
for which all changes are written out continuously until the changelog is exhausted. The returned _changelog mode_
indicates the set of changes that the sink accepts during runtime.

For regular batch scenarios, the sink can solely accept insert-only rows and write out bounded streams.

For regular streaming scenarios, the sink can solely accept insert-only rows and can write out unbounded streams.

For change data capture (CDC) scenarios, the sink can write out bounded or unbounded streams with insert,
update, and delete rows.

A table sink can implement further ability interfaces such as `SupportsOverwrite` that might mutate an
instance during planning. All abilities can be found in the `org.apache.flink.table.connector.sink.abilities`
package and are listed in the [sink abilities table](#sink-abilities).

The runtime implementation of a `DynamicTableSink` must consume internal data structures. Thus, records
must be accepted as `org.apache.flink.table.data.RowData`. The framework provides runtime converters such
that a sink can still work on common data structures and perform a conversion at the beginning.

#### Sink Abilities

<table class="table table-bordered">
    <thead>
        <tr>
        <th class="text-left" style="width: 25%">Interface</th>
        <th class="text-center" style="width: 75%">Description</th>
        </tr>
    </thead>
    <tbody>
    <tr>
        <td><a href='https://github.com/apache/flink/blob/master/flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/sink/abilities/SupportsOverwrite.java'>SupportsOverwrite</a></td>
        <td>Enables to overwrite existing data in a <code>DynamicTableSink</code>. By default, if
        this interface is not implemented, existing tables or partitions cannot be overwritten using
        e.g. the SQL <code>INSERT OVERWRITE</code> clause.</td>
    </tr>
    <tr>
        <td><a href='https://github.com/apache/flink/blob/master/flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/sink/abilities/SupportsPartitioning.java'>SupportsPartitioning</a></td>
        <td>Enables to write partitioned data in a <code>DynamicTableSink</code>.</td>
    </tr>
    <tr>
        <td><a href='https://github.com/apache/flink/blob/master/flink-table/flink-table-common/src/main/java/org/apache/flink/table/connector/sink/abilities/SupportsWritingMetadata.java'>SupportsWritingMetadata</a></td>
        <td>Enables to write metadata columns into a <code>DynamicTableSource</code>. A table sink is
        responsible for accepting requested metadata columns at the end of consumed rows and persist
        them. This includes potentially forwarding metadata columns to contained formats.</td>
    </tr>
    </tbody>
</table>

### Encoding / Decoding Formats

Some table connectors accept different formats that encode and decode keys and/or values.

Formats work similar to the pattern `DynamicTableSourceFactory -> DynamicTableSource -> ScanRuntimeProvider`,
where the factory is responsible for translating options and the source is responsible for creating runtime logic.

Because formats might be located in different modules, they are discovered using Java's Service Provider
Interface similar to [table factories](#dynamic-table-factories). In order to discover a format factory,
the dynamic table factory searches for a factory that corresponds to a factory identifier and connector-specific
base class.

For example, the Kafka table source requires a `DeserializationSchema` as runtime interface for a decoding
format. Therefore, the Kafka table source factory uses the value of the `value.format` option to discover
a `DeserializationFormatFactory`.

The following format factories are currently supported:

```
org.apache.flink.table.factories.DeserializationFormatFactory
org.apache.flink.table.factories.SerializationFormatFactory
```

The format factory translates the options into an `EncodingFormat` or a `DecodingFormat`. Those interfaces are
another kind of factory that produce specialized format runtime logic for the given data type.

For example, for a Kafka table source factory, the `DeserializationFormatFactory` would return an `EncodingFormat<DeserializationSchema>`
that can be passed into the Kafka table source.

{% top %}

Full Stack Example
------------------

This section sketches how to implement a scan table source with a decoding format that supports changelog
semantics. The example illustrates how all of the mentioned components play together. It can serve as
a reference implementation.

In particular, it shows how to
- create factories that parse and validate options,
- implement table connectors,
- implement and discover custom formats,
- and use provided utilities such as data structure converters and the `FactoryUtil`.

The table source uses a simple single-threaded `SourceFunction` to open a socket that listens for incoming
bytes. The raw bytes are decoded into rows by a pluggable format. The format expects a changelog flag
as the first column.

We will use most of the interfaces mentioned above to enable the following DDL:

{% highlight sql %}
CREATE TABLE UserScores (name STRING, score INT)
WITH (
  'connector' = 'socket',
  'hostname' = 'localhost',
  'port' = '9999',
  'byte-delimiter' = '10',
  'format' = 'changelog-csv',
  'changelog-csv.column-delimiter' = '|'
);
{% endhighlight %}

Because the format supports changelog semantics, we are able to ingest updates during runtime and create
an updating view that can continuously evaluate changing data:

{% highlight sql %}
SELECT name, SUM(score) FROM UserScores GROUP BY name;
{% endhighlight %}

Use the following command to ingest data in a terminal:
{% highlight text %}
> nc -lk 9999
INSERT|Alice|12
INSERT|Bob|5
DELETE|Alice|12
INSERT|Alice|18
{% endhighlight %}

### Factories

This section illustrates how to translate metadata coming from the catalog to concrete connector instances.

Both factories have been added to the `META-INF/services` directory.

**`SocketDynamicTableFactory`**

The `SocketDynamicTableFactory` translates the catalog table to a table source. Because the table source
requires a decoding format, we are discovering the format using the provided `FactoryUtil` for convenience.

{% highlight java %}
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

public class SocketDynamicTableFactory implements DynamicTableSourceFactory {

  // define all options statically
  public static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname")
    .stringType()
    .noDefaultValue();

  public static final ConfigOption<Integer> PORT = ConfigOptions.key("port")
    .intType()
    .noDefaultValue();

  public static final ConfigOption<Integer> BYTE_DELIMITER = ConfigOptions.key("byte-delimiter")
    .intType()
    .defaultValue(10); // corresponds to '\n'

  @Override
  public String factoryIdentifier() {
    return "socket"; // used for matching to `connector = '...'`
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(HOSTNAME);
    options.add(PORT);
    options.add(FactoryUtil.FORMAT); // use pre-defined option for format
    return options;
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(BYTE_DELIMITER);
    return options;
  }

  @Override
  public DynamicTableSource createDynamicTableSource(Context context) {
    // either implement your custom validation logic here ...
    // or use the provided helper utility
    final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

    // discover a suitable decoding format
    final DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
      DeserializationFormatFactory.class,
      FactoryUtil.FORMAT);

    // validate all options
    helper.validate();

    // get the validated options
    final ReadableConfig options = helper.getOptions();
    final String hostname = options.get(HOSTNAME);
    final int port = options.get(PORT);
    final byte byteDelimiter = (byte) (int) options.get(BYTE_DELIMITER);

    // derive the produced data type (excluding computed columns) from the catalog table
    final DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

    // create and return dynamic table source
    return new SocketDynamicTableSource(hostname, port, byteDelimiter, decodingFormat, producedDataType);
  }
}
{% endhighlight %}

**`ChangelogCsvFormatFactory`**

The `ChangelogCsvFormatFactory` translates format-specific options to a format. The `FactoryUtil` in `SocketDynamicTableFactory`
takes care of adapting the option keys accordingly and handles the prefixing like `changelog-csv.column-delimiter`.

Because this factory implements `DeserializationFormatFactory`, it could also be used for other connectors
that support deserialization formats such as the Kafka connector.

{% highlight java %}
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;

public class ChangelogCsvFormatFactory implements DeserializationFormatFactory {

  // define all options statically
  public static final ConfigOption<String> COLUMN_DELIMITER = ConfigOptions.key("column-delimiter")
    .stringType()
    .defaultValue("|");

  @Override
  public String factoryIdentifier() {
    return "changelog-csv";
  }

  @Override
  public Set<ConfigOption<?>> requiredOptions() {
    return Collections.emptySet();
  }

  @Override
  public Set<ConfigOption<?>> optionalOptions() {
    final Set<ConfigOption<?>> options = new HashSet<>();
    options.add(COLUMN_DELIMITER);
    return options;
  }

  @Override
  public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
      DynamicTableFactory.Context context,
      ReadableConfig formatOptions) {
    // either implement your custom validation logic here ...
    // or use the provided helper method
    FactoryUtil.validateFactoryOptions(this, formatOptions);

    // get the validated options
    final String columnDelimiter = formatOptions.get(COLUMN_DELIMITER);

    // create and return the format
    return new ChangelogCsvFormat(columnDelimiter);
  }
}
{% endhighlight %}

### Table Source and Decoding Format

This section illustrates how to translate from instances of the planning layer to runtime instances that
are shipped to the cluster.

**`SocketDynamicTableSource`**

The `SocketDynamicTableSource` is used during planning. In our example, we don't implement any of the
available ability interfaces. Therefore, the main logic can be found in `getScanRuntimeProvider(...)`
where we instantiate the required `SourceFunction` and its `DeserializationSchema` for runtime. Both
instances are parameterized to return internal data structures (i.e. `RowData`).

{% highlight java %}
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class SocketDynamicTableSource implements ScanTableSource {

  private final String hostname;
  private final int port;
  private final byte byteDelimiter;
  private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
  private final DataType producedDataType;

  public SocketDynamicTableSource(
      String hostname,
      int port,
      byte byteDelimiter,
      DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
      DataType producedDataType) {
    this.hostname = hostname;
    this.port = port;
    this.byteDelimiter = byteDelimiter;
    this.decodingFormat = decodingFormat;
    this.producedDataType = producedDataType;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    // in our example the format decides about the changelog mode
    // but it could also be the source itself
    return decodingFormat.getChangelogMode();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

    // create runtime classes that are shipped to the cluster

    final DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(
      runtimeProviderContext,
      producedDataType);

    final SourceFunction<RowData> sourceFunction = new SocketSourceFunction(
      hostname,
      port,
      byteDelimiter,
      deserializer);

    return SourceFunctionProvider.of(sourceFunction, false);
  }

  @Override
  public DynamicTableSource copy() {
    return new SocketDynamicTableSource(hostname, port, byteDelimiter, decodingFormat, producedDataType);
  }

  @Override
  public String asSummaryString() {
    return "Socket Table Source";
  }
}
{% endhighlight %}

**`ChangelogCsvFormat`**

The `ChangelogCsvFormat` is a decoding format that uses a `DeserializationSchema` during runtime. It
supports emitting `INSERT` and `DELETE` changes.

{% highlight java %}
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

public class ChangelogCsvFormat implements DecodingFormat<DeserializationSchema<RowData>> {

  private final String columnDelimiter;

  public ChangelogCsvFormat(String columnDelimiter) {
    this.columnDelimiter = columnDelimiter;
  }

  @Override
  @SuppressWarnings("unchecked")
  public DeserializationSchema<RowData> createRuntimeDecoder(
      DynamicTableSource.Context context,
      DataType producedDataType) {
    // create type information for the DeserializationSchema
    final TypeInformation<RowData> producedTypeInfo = (TypeInformation<RowData>) context.createTypeInformation(
      producedDataType);

    // most of the code in DeserializationSchema will not work on internal data structures
    // create a converter for conversion at the end
    final DataStructureConverter converter = context.createDataStructureConverter(producedDataType);

    // use logical types during runtime for parsing
    final List<LogicalType> parsingTypes = producedDataType.getLogicalType().getChildren();

    // create runtime class
    return new ChangelogCsvDeserializer(parsingTypes, converter, producedTypeInfo, columnDelimiter);
  }

  @Override
  public ChangelogMode getChangelogMode() {
    // define that this format can produce INSERT and DELETE rows
    return ChangelogMode.newBuilder()
      .addContainedKind(RowKind.INSERT)
      .addContainedKind(RowKind.DELETE)
      .build();
  }
}
{% endhighlight %}

### Runtime

For completeness, this section illustrates the runtime logic for both `SourceFunction` and `DeserializationSchema`.

**ChangelogCsvDeserializer**

The `ChangelogCsvDeserializer` contains a simple parsing logic for converting bytes into `Row` of `Integer`
and `String` with a row kind. The final conversion step converts those into internal data structures.

{% highlight java %}
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.RuntimeConverter.Context;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

public class ChangelogCsvDeserializer implements DeserializationSchema<RowData> {

  private final List<LogicalType> parsingTypes;
  private final DataStructureConverter converter;
  private final TypeInformation<RowData> producedTypeInfo;
  private final String columnDelimiter;

  public ChangelogCsvDeserializer(
      List<LogicalType> parsingTypes,
      DataStructureConverter converter,
      TypeInformation<RowData> producedTypeInfo,
      String columnDelimiter) {
    this.parsingTypes = parsingTypes;
    this.converter = converter;
    this.producedTypeInfo = producedTypeInfo;
    this.columnDelimiter = columnDelimiter;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    // return the type information required by Flink's core interfaces
    return producedTypeInfo;
  }

  @Override
  public void open(InitializationContext context) {
    // converters must be open
    converter.open(Context.create(ChangelogCsvDeserializer.class.getClassLoader()));
  }

  @Override
  public RowData deserialize(byte[] message) {
    // parse the columns including a changelog flag
    final String[] columns = new String(message).split(Pattern.quote(columnDelimiter));
    final RowKind kind = RowKind.valueOf(columns[0]);
    final Row row = new Row(kind, parsingTypes.size());
    for (int i = 0; i < parsingTypes.size(); i++) {
      row.setField(i, parse(parsingTypes.get(i).getTypeRoot(), columns[i + 1]));
    }
    // convert to internal data structure
    return (RowData) converter.toInternal(row);
  }

  private static Object parse(LogicalTypeRoot root, String value) {
    switch (root) {
      case INTEGER:
        return Integer.parseInt(value);
      case VARCHAR:
        return value;
      default:
        throw new IllegalArgumentException();
    }
  }

  @Override
  public boolean isEndOfStream(RowData nextElement) {
    return false;
  }
}
{% endhighlight %}

**SocketSourceFunction**

The `SocketSourceFunction` opens a socket and consumes bytes. It splits records by the given byte
delimiter (`\n` by default) and delegates the decoding to a pluggable `DeserializationSchema`. The
source function can only work with a parallelism of 1.

{% highlight java %}
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.RowData;

public class SocketSourceFunction extends RichSourceFunction<RowData> implements ResultTypeQueryable<RowData> {

  private final String hostname;
  private final int port;
  private final byte byteDelimiter;
  private final DeserializationSchema<RowData> deserializer;

  private volatile boolean isRunning = true;
  private Socket currentSocket;

  public SocketSourceFunction(String hostname, int port, byte byteDelimiter, DeserializationSchema<RowData> deserializer) {
    this.hostname = hostname;
    this.port = port;
    this.byteDelimiter = byteDelimiter;
    this.deserializer = deserializer;
  }

  @Override
  public TypeInformation<RowData> getProducedType() {
    return deserializer.getProducedType();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    deserializer.open(() -> getRuntimeContext().getMetricGroup());
  }

  @Override
  public void run(SourceContext<RowData> ctx) throws Exception {
    while (isRunning) {
      // open and consume from socket
      try (final Socket socket = new Socket()) {
        currentSocket = socket;
        socket.connect(new InetSocketAddress(hostname, port), 0);
        try (InputStream stream = socket.getInputStream()) {
          ByteArrayOutputStream buffer = new ByteArrayOutputStream();
          int b;
          while ((b = stream.read()) >= 0) {
            // buffer until delimiter
            if (b != byteDelimiter) {
              buffer.write(b);
            }
            // decode and emit record
            else {
              ctx.collect(deserializer.deserialize(buffer.toByteArray()));
              buffer.reset();
            }
          }
        }
      } catch (Throwable t) {
        t.printStackTrace(); // print and continue
      }
      Thread.sleep(1000);
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
    try {
      currentSocket.close();
    } catch (Throwable t) {
      // ignore
    }
  }
}
{% endhighlight %}

{% top %}
