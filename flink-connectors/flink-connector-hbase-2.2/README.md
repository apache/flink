# Flink HBase Connector

This connector provides classes that allow access for Flink to [HBase](https://hbase.apache.org/).

 *Version Compatibility*: This module is compatible with Apache HBase *2.2.3* (last stable version).

Note that the streaming connectors are not part of the binary distribution of Flink. You need to link them into your job jar for cluster execution.
See how to link with them for cluster execution [here](https://ci.apache.org/projects/flink/flink-docs-release-1.11/dev/project-configuration.html#adding-connector-and-library-dependencies).

## Installing HBase

Follow the instructions from the [HBase Quick Start Guide](http://hbase.apache.org/book.html#quickstart).

## HBase Configuration

Connecting to HBase always requires a `Configuration` instance. If there is an HBase gateway on the same host as the Flink gateway where the application is started, this can be obtained by invoking `HBaseConfigurationUtil.createHBaseConf()` as in the examples below. If that's not the case a configuration should be provided where the proper core-site, hdfs-site, and hbase-site are added as resources.

## DataStream API

### Reading tables into a DataStreams

To convert an HBase Table into a DataStream one must create an `HBaseTableSource` instance, then either convert it to a `DataStream` of `Row` objects with a built in function, or use the Table API and have a more flexible way to have a stream:

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, tableSettings);

HBaseTableSource hBaseSource = new HBaseTableSource(HBaseConfigurationUtil.createHBaseConf(), "t1");
hBaseSource.setRowKey("rowkey", byte[].class);
hBaseSource.addColumn("f1", "str", byte[].class);

// Direct conversion to DataStream<Row>
DataStream<Row> rowStream = hBaseSource.getDataStream(env);

// Table API
((TableEnvironmentInternal) tableEnv).registerTableSourceInternal("t1", hBaseSource);
Table table = tableEnv.sqlQuery("SELECT t.rowkey, t.f1.str FROM t1 t");
DataStream<Tuple2<byte[], byte[]>> resultStream = tableEnv.toAppendStream(table, TypeInformation.of(new TypeHint<Tuple2<byte[], byte[]>>(){}));
```

### Writing into HBase tables from DataStreams
There are two ways to write data to an HBase table from a `DataStream`:
- Instantiate an `HBaseSinkFunction`, and provide one's own `HBaseMutationConverter` implementation that can create mutations from any data received.

```java
DataStream<Tuple2<byte[], byte[]>> dataStream = ...

HBaseMutationConverter<Tuple2<byte[], byte[]>> mutationConverter = new HBaseMutationConverter<Tuple2<byte[], byte[]>>() {
	private static final long serialVersionUID = 1L;

	@Override
	public void open() {
	}

	@Override
	public Mutation convertToMutation(Tuple2<byte[], byte[]> record) {
		Put put = new Put(record.f0);
		put.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("str"), record.f1);
		return put;
	}
};

HBaseSinkFunction<Tuple2<byte[], byte[]>> hBaseSink = new HBaseSinkFunction<Tuple2<byte[], byte[]>>(
		"t2", HBaseConfigurationUtil.createHBaseConf(), mutationConverter, 10000, 2, 1000);
dataStream.addSink(hBaseSink);
```

- Use the built in `HBaseDynamicTableSink` or `HBaseUpsertTableSink` classes which convert `RowData` or `Tuple2<Boolen, Row>` objects into a mutation each based on an `HBaseTableSchema` provided to them.

```java
DataStream<<Tuple2<Boolean, Row>> dataStream = ...

HBaseTableSchema schema = new HBaseTableSchema();
schema.setRowKey("rowkey", byte[].class);
schema.addColumn("f1", "str", byte[].class);

HBaseUpsertTableSink sink = new HBaseUpsertTableSink("t3", schema, HBaseConfigurationUtil.createHBaseConf(),
		HBaseWriteOptions.builder().setBufferFlushIntervalMillis(1000).build());
sink.consumeDataStream(dataStream);
```

## Building the connector

The connector can be easily built by using maven:

```
cd flink-connectors/flink-connector-hbase
mvn clean install
```