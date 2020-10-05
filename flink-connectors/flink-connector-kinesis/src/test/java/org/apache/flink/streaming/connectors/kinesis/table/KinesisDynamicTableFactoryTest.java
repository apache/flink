/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis.table;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisConsumer;
import org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer;
import org.apache.flink.streaming.connectors.kinesis.RandomKinesisPartitioner;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TableOptionsBuilder;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.streaming.connectors.kinesis.table.RowDataKinesisDeserializationSchema.Metadata;
import static org.apache.flink.streaming.connectors.kinesis.table.RowDataKinesisDeserializationSchema.Metadata.ShardId;
import static org.apache.flink.streaming.connectors.kinesis.table.RowDataKinesisDeserializationSchema.Metadata.Timestamp;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test for {@link KinesisDynamicSource} and {@link KinesisDynamicSink} created
 * by {@link KinesisDynamicTableFactory}.
 */
public class KinesisDynamicTableFactoryTest extends TestLogger {

	private static final String STREAM_NAME = "myStream";

	private static final String TABLE_NAME = "myTable";

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	// --------------------------------------------------------------------------------------------
	// Positive tests
	// --------------------------------------------------------------------------------------------

	@Test
	public void testGoodTableSource() {
		TableSchema sourceSchema = defaultSourceSchema().build();
		Map<String, String> sourceOptions = defaultTableOptions().build();
		CatalogTable catalogTable = createSourceTable(sourceSchema, sourceOptions);

		// Construct actual DynamicTableSource using FactoryUtil
		KinesisDynamicSource actualSource = actualDynamicSource(catalogTable);

		// Construct expected DynamicTableSink using factory under test
		KinesisDynamicSource expectedSource = new KinesisDynamicSource(
			sourceSchema.toPhysicalRowDataType(),
			STREAM_NAME,
			defaultConsumerProperties(),
			new TestFormatFactory.DecodingFormatMock(",", true));

		// verify that the constructed DynamicTableSink is as expected
		assertEquals(expectedSource, actualSource);

		// verify that the copy of the constructed DynamicTableSink is as expected
		assertEquals(expectedSource, actualSource.copy());

		// verify produced sink
		ScanTableSource.ScanRuntimeProvider functionProvider =
			actualSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
		SourceFunction<RowData> sourceFunction =
			as(functionProvider, SourceFunctionProvider.class).createSourceFunction();
		assertThat(sourceFunction, instanceOf(FlinkKinesisConsumer.class));
	}

	@Test
	public void testGoodTableSourceWithMetadataFields() {
		TableSchema sourceSchema = defaultSourceSchema().build();
		Map<String, String> sourceOptions = defaultTableOptions().build();
		CatalogTable sourceTable = createSourceTable(sourceSchema, sourceOptions);

		Metadata[] requestedMetadata = new Metadata[]{ShardId, Timestamp};
		List<String> metadataKeys = Arrays.asList(ShardId.getFieldName(), Timestamp.getFieldName());
		DataType producedDataType = getProducedType(sourceSchema, requestedMetadata);

		// Construct actual DynamicTableSource using FactoryUtil
		KinesisDynamicSource actualSource = actualDynamicSource(sourceTable);
		actualSource.applyReadableMetadata(metadataKeys, producedDataType);

		// Construct expected DynamicTableSink using factory under test
		KinesisDynamicSource expectedSource = new KinesisDynamicSource(
			sourceSchema.toPhysicalRowDataType(),
			STREAM_NAME,
			defaultConsumerProperties(),
			new TestFormatFactory.DecodingFormatMock(",", true),
			producedDataType,
			Arrays.asList(requestedMetadata));

		// verify that the constructed DynamicTableSource is as expected
		assertEquals(expectedSource, actualSource);

		// verify that the copy of the constructed DynamicTableSink is as expected
		assertEquals(expectedSource, actualSource.copy());
	}

	@Test
	public void testGoodTableSinkForPartitionedTable() {
		TableSchema sinkSchema = defaultSinkSchema().build();
		Map<String, String> sinkOptions = defaultTableOptions().build();
		List<String> sinkPartitionKeys = Arrays.asList("name", "curr_id");
		CatalogTable sinkTable = createSinkTable(sinkSchema, sinkOptions, sinkPartitionKeys);

		// Construct actual DynamicTableSink using FactoryUtil
		KinesisDynamicSink actualSink = actualDynamicSink(sinkTable);

		// Construct expected DynamicTableSink using factory under test
		KinesisDynamicSink expectedSink = new KinesisDynamicSink(
			sinkSchema.toPhysicalRowDataType(),
			STREAM_NAME,
			defaultProducerProperties(),
			new TestFormatFactory.EncodingFormatMock(","),
			new RowDataFieldsKinesisPartitioner(sinkTable));

		// verify that the constructed DynamicTableSink is as expected
		assertEquals(expectedSink, actualSink);

		// verify that the copy of the constructed DynamicTableSink is as expected
		assertEquals(expectedSink.copy(), actualSink);

		// verify the produced sink
		DynamicTableSink.SinkRuntimeProvider sinkFunctionProvider =
			actualSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
		SinkFunction<RowData> sinkFunction =
			as(sinkFunctionProvider, SinkFunctionProvider.class).createSinkFunction();
		assertThat(sinkFunction, instanceOf(FlinkKinesisProducer.class));
	}

	@Test
	public void testGoodTableSinkForNonPartitionedTable() {
		TableSchema sinkSchema = defaultSinkSchema().build();
		Map<String, String> sinkOptions = defaultTableOptions().build();
		CatalogTable sinkTable = createSinkTable(sinkSchema, sinkOptions);

		// Construct actual DynamicTableSink using FactoryUtil
		KinesisDynamicSink actualSink = actualDynamicSink(sinkTable);

		// Construct expected DynamicTableSink using factory under test
		KinesisDynamicSink expectedSink = new KinesisDynamicSink(
			sinkSchema.toPhysicalRowDataType(),
			STREAM_NAME,
			defaultProducerProperties(),
			new TestFormatFactory.EncodingFormatMock(","),
			new RandomKinesisPartitioner<>());

		// verify that the constructed DynamicTableSink is as expected
		assertEquals(expectedSink, actualSink);

		// verify that the copy of the constructed DynamicTableSink is as expected
		assertEquals(expectedSink.copy(), actualSink);

		// verify the produced sink
		DynamicTableSink.SinkRuntimeProvider sinkFunctionProvider =
			actualSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
		SinkFunction<RowData> sinkFunction =
			as(sinkFunctionProvider, SinkFunctionProvider.class).createSinkFunction();
		assertThat(sinkFunction, instanceOf(FlinkKinesisProducer.class));
	}

	// --------------------------------------------------------------------------------------------
	// Negative tests
	// --------------------------------------------------------------------------------------------

	@Test
	public void testBadTableSinkForCustomPartitionerForPartitionedTable() {
		TableSchema sinkSchema = defaultSinkSchema().build();
		Map<String, String> sinkOptions = defaultTableOptions()
			.withTableOption(KinesisOptions.SINK_PARTITIONER, "random")
			.build();

		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException(String.format(
			"Cannot set %s option for a table defined with a PARTITIONED BY clause",
			KinesisOptions.SINK_PARTITIONER.key()))));

		try {
			FactoryUtil.createTableSink(
				null,
				ObjectIdentifier.of("default", "default", TABLE_NAME),
				createSinkTable(sinkSchema, sinkOptions, Arrays.asList("name", "curr_id")),
				new Configuration(),
				Thread.currentThread().getContextClassLoader(),
				false);
		} catch (ValidationException e) {
			throw (ValidationException) e.getCause(); // unpack the causing exception
		}
	}

	@Test
	public void testBadTableSinkForNonExistingPartitionerClass() {
		TableSchema sinkSchema = defaultSinkSchema().build();
		Map<String, String> sinkOptions = defaultTableOptions()
			.withTableOption(KinesisOptions.SINK_PARTITIONER, "abc")
			.build();

		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException(
			"Could not find and instantiate partitioner class 'abc'")));

		FactoryUtil.createTableSink(
			null,
			ObjectIdentifier.of("default", "default", TABLE_NAME),
			createSinkTable(sinkSchema, sinkOptions, Collections.emptyList()),
			new Configuration(),
			Thread.currentThread().getContextClassLoader(),
			false);
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	private CatalogTable createSourceTable(
		TableSchema sourceSchema,
		Map<String, String> sourceOptions) {
		return createSourceTable(sourceSchema, sourceOptions, Collections.emptyList());
	}

	private CatalogTable createSourceTable(
		TableSchema sourceSchema,
		Map<String, String> sourceOptions,
		List<String> partitionKeys) {
		return new CatalogTableImpl(sourceSchema, partitionKeys, sourceOptions, TABLE_NAME);
	}

	private CatalogTable createSinkTable(
		TableSchema sinkSchema,
		Map<String, String> sinkOptions) {
		return createSinkTable(sinkSchema, sinkOptions, Collections.emptyList());
	}

	private CatalogTable createSinkTable(
		TableSchema sinkSchema,
		Map<String, String> sinkOptions,
		List<String> partitionKeys) {
		return new CatalogTableImpl(sinkSchema, partitionKeys, sinkOptions, TABLE_NAME);
	}

	private TableSchema.Builder defaultSourceSchema() {
		return TableSchema.builder()
			.add(TableColumn.physical("name", DataTypes.STRING()))
			.add(TableColumn.physical("curr_id", DataTypes.BIGINT()))
			.add(TableColumn.physical("time", DataTypes.TIMESTAMP(3)))
			.add(TableColumn.computed("next_id", DataTypes.BIGINT(), "curr_id + 1"))
			.watermark("time", "time" + " - INTERVAL '5' SECOND", DataTypes.TIMESTAMP(3));
	}

	private TableSchema.Builder defaultSinkSchema() {
		return TableSchema.builder()
			.add(TableColumn.physical("name", DataTypes.STRING()))
			.add(TableColumn.physical("curr_id", DataTypes.BIGINT()))
			.add(TableColumn.physical("time", DataTypes.TIMESTAMP(3)));
	}

	private TableOptionsBuilder defaultTableOptions() {
		String connector = KinesisDynamicTableFactory.IDENTIFIER;
		String format = TestFormatFactory.IDENTIFIER;
		return new TableOptionsBuilder(connector, format)
			// default table options
			.withTableOption(KinesisOptions.STREAM, STREAM_NAME)
			.withTableOption("aws.region", "us-west-2")
			.withTableOption("aws.credentials.provider", "BASIC")
			.withTableOption("aws.credentials.basic.accesskeyid", "ververicka")
			.withTableOption("aws.credentials.basic.secretkey", "SuperSecretSecretSquirrel")
			.withTableOption("scan.stream.initpos", "AT_TIMESTAMP")
			.withTableOption("scan.stream.initpos-timestamp-format", "yyyy-MM-dd'T'HH:mm:ss")
			.withTableOption("scan.stream.initpos-timestamp", "2014-10-22T12:00:00")
			.withTableOption("sink.producer.collection-max-count", "100")
			// default format options
			.withFormatOption(TestFormatFactory.DELIMITER, ",")
			.withFormatOption(TestFormatFactory.FAIL_ON_MISSING, "true");
	}

	private Properties defaultConsumerProperties() {
		return new Properties() {{
			setProperty("aws.region", "us-west-2");
			setProperty("aws.credentials.provider", "BASIC");
			setProperty("aws.credentials.provider.basic.accesskeyid", "ververicka");
			setProperty("aws.credentials.provider.basic.secretkey", "SuperSecretSecretSquirrel");
			setProperty("flink.stream.initpos", "AT_TIMESTAMP");
			setProperty("flink.stream.initpos.timestamp.format", "yyyy-MM-dd'T'HH:mm:ss");
			setProperty("flink.stream.initpos.timestamp", "2014-10-22T12:00:00");
		}};
	}

	private Properties defaultProducerProperties() {
		return new Properties() {{
			setProperty("aws.region", "us-west-2");
			setProperty("aws.credentials.provider", "BASIC");
			setProperty("aws.credentials.provider.basic.accesskeyid", "ververicka");
			setProperty("aws.credentials.provider.basic.secretkey", "SuperSecretSecretSquirrel");
			setProperty("CollectionMaxCount", "100");
		}};
	}

	private KinesisDynamicSource actualDynamicSource(CatalogTable catalogTable) {
		return (KinesisDynamicSource)
			FactoryUtil.createTableSource(
				null,
				ObjectIdentifier.of("default", "default", TABLE_NAME),
				catalogTable,
				new Configuration(),
				Thread.currentThread().getContextClassLoader(),
				false);
	}

	private KinesisDynamicSink actualDynamicSink(CatalogTable catalogTable) {
		return (KinesisDynamicSink) FactoryUtil.createTableSink(
			null,
			ObjectIdentifier.of("default", "default", TABLE_NAME),
			catalogTable,
			new Configuration(),
			Thread.currentThread().getContextClassLoader(),
			false);
	}

	private DataType getProducedType(TableSchema schema, Metadata... requestedMetadata) {
		Stream<DataTypes.Field> physicalFields = IntStream.range(0, schema.getFieldCount())
			.mapToObj(i -> DataTypes.FIELD(
				schema.getFieldName(i).orElseThrow(RuntimeException::new),
				schema.getFieldDataType(i).orElseThrow(RuntimeException::new)));
		Stream<DataTypes.Field> metadataFields = Arrays.stream(requestedMetadata)
			.map(m -> DataTypes.FIELD(
				m.name(),
				m.getDataType()));
		Stream<DataTypes.Field> allFields = Stream.concat(physicalFields, metadataFields);

		return DataTypes.ROW(allFields.toArray(DataTypes.Field[]::new));
	}

	private <T> T as(Object object, Class<T> clazz) {
		assertThat(object, instanceOf(clazz));
		return clazz.cast(object);
	}
}
