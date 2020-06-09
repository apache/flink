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

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestFormatFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Abstract test base for {@link KafkaDynamicTableFactoryBase}.
 */
public abstract class KafkaDynamicTableFactoryTestBase extends TestLogger {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static final String TOPIC = "myTopic";
	private static final int PARTITION_0 = 0;
	private static final long OFFSET_0 = 100L;
	private static final int PARTITION_1 = 1;
	private static final long OFFSET_1 = 123L;
	private static final String NAME = "name";
	private static final String COUNT = "count";
	private static final String TIME = "time";
	private static final String WATERMARK_EXPRESSION = TIME + " - INTERVAL '5' SECOND";
	private static final DataType WATERMARK_DATATYPE = DataTypes.TIMESTAMP(3);
	private static final String COMPUTED_COLUMN_NAME = "computed-column";
	private static final String COMPUTED_COLUMN_EXPRESSION = COUNT + " + 1.0";
	private static final DataType COMPUTED_COLUMN_DATATYPE = DataTypes.DECIMAL(10, 3);

	private static final Properties KAFKA_PROPERTIES = new Properties();
	static {
		KAFKA_PROPERTIES.setProperty("group.id", "dummy");
		KAFKA_PROPERTIES.setProperty("bootstrap.servers", "dummy");
	}

	private static final String PROPS_SCAN_OFFSETS =
			String.format("partition:%d,offset:%d;partition:%d,offset:%d",
					PARTITION_0, OFFSET_0, PARTITION_1, OFFSET_1);

	private static final TableSchema SOURCE_SCHEMA = TableSchema.builder()
			.field(NAME, DataTypes.STRING())
			.field(COUNT, DataTypes.DECIMAL(38, 18))
			.field(TIME, DataTypes.TIMESTAMP(3))
			.field(COMPUTED_COLUMN_NAME, COMPUTED_COLUMN_DATATYPE, COMPUTED_COLUMN_EXPRESSION)
				.watermark(TIME, WATERMARK_EXPRESSION, WATERMARK_DATATYPE)
				.build();

	private static final TableSchema SINK_SCHEMA = TableSchema.builder()
			.field(NAME, DataTypes.STRING())
			.field(COUNT, DataTypes.DECIMAL(38, 18))
			.field(TIME, DataTypes.TIMESTAMP(3))
			.build();

	@Test
	@SuppressWarnings("unchecked")
	public void testTableSource() {
		// prepare parameters for Kafka table source
		final DataType producedDataType = SOURCE_SCHEMA.toPhysicalRowDataType();

		final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
		specificOffsets.put(new KafkaTopicPartition(TOPIC, PARTITION_0), OFFSET_0);
		specificOffsets.put(new KafkaTopicPartition(TOPIC, PARTITION_1), OFFSET_1);

		DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
				new TestFormatFactory.DecodingFormatMock(",", true);

		// Construct table source using options and table source factory
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
				"default",
				"default",
				"scanTable");
		CatalogTable catalogTable = createKafkaSourceCatalogTable();
		final DynamicTableSource actualSource = FactoryUtil.createTableSource(null,
				objectIdentifier,
				catalogTable,
				new Configuration(),
				Thread.currentThread().getContextClassLoader());

		// Test scan source equals
		final KafkaDynamicSourceBase expectedKafkaSource = getExpectedScanSource(
				producedDataType,
				TOPIC,
				KAFKA_PROPERTIES,
				decodingFormat,
				StartupMode.SPECIFIC_OFFSETS,
				specificOffsets,
				0);
		final KafkaDynamicSourceBase actualKafkaSource = (KafkaDynamicSourceBase) actualSource;
		assertEquals(actualKafkaSource, expectedKafkaSource);

		// Test Kafka consumer
		ScanTableSource.ScanRuntimeProvider provider =
				actualKafkaSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
		assertThat(provider, instanceOf(SourceFunctionProvider.class));
		final SourceFunctionProvider sourceFunctionProvider = (SourceFunctionProvider) provider;
		final SourceFunction<RowData> sourceFunction = sourceFunctionProvider.createSourceFunction();
		assertThat(sourceFunction, instanceOf(getExpectedConsumerClass()));
		//  Test commitOnCheckpoints flag should be true when set consumer group
		assertTrue(((FlinkKafkaConsumerBase) sourceFunction).getEnableCommitOnCheckpoints());
	}

	@Test
	public void testTableSourceCommitOnCheckpointsDisabled() {
		//Construct table source using options and table source factory
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
			"default",
			"default",
			"scanTable");
		Map<String, String> tableOptions = getFullSourceOptions();
		tableOptions.remove("properties.group.id");
		CatalogTable catalogTable = createKafkaSourceCatalogTable(tableOptions);
		final DynamicTableSource tableSource = FactoryUtil.createTableSource(null,
			objectIdentifier,
			catalogTable,
			new Configuration(),
			Thread.currentThread().getContextClassLoader());

		// Test commitOnCheckpoints flag should be false when do not set consumer group.
		assertThat(tableSource, instanceOf(KafkaDynamicSourceBase.class));
		ScanTableSource.ScanRuntimeProvider providerWithoutGroupId = ((KafkaDynamicSourceBase) tableSource)
			.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
		assertThat(providerWithoutGroupId, instanceOf(SourceFunctionProvider.class));
		final SourceFunctionProvider functionProviderWithoutGroupId = (SourceFunctionProvider) providerWithoutGroupId;
		final SourceFunction<RowData> function = functionProviderWithoutGroupId.createSourceFunction();
		assertFalse(((FlinkKafkaConsumerBase) function).getEnableCommitOnCheckpoints());
	}

	@Test
	public void testTableSink() {
		final DataType consumedDataType = SINK_SCHEMA.toPhysicalRowDataType();
		EncodingFormat<SerializationSchema<RowData>> encodingFormat =
				new TestFormatFactory.EncodingFormatMock(",");

		// Construct table sink using options and table sink factory.
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
				"default",
				"default",
				"sinkTable");
		final CatalogTable sinkTable = createKafkaSinkCatalogTable();
		final DynamicTableSink actualSink = FactoryUtil.createTableSink(
				null,
				objectIdentifier,
				sinkTable,
				new Configuration(),
				Thread.currentThread().getContextClassLoader());

		final DynamicTableSink expectedSink = getExpectedSink(
				consumedDataType,
				TOPIC,
				KAFKA_PROPERTIES,
				Optional.of(new FlinkFixedPartitioner<>()),
				encodingFormat);
		assertEquals(expectedSink, actualSink);

		// Test sink format.
		final KafkaDynamicSinkBase actualKafkaSink = (KafkaDynamicSinkBase) actualSink;
		assertEquals(encodingFormat, actualKafkaSink.encodingFormat);

		// Test kafka producer.
		DynamicTableSink.SinkRuntimeProvider provider =
				actualKafkaSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
		assertThat(provider, instanceOf(SinkFunctionProvider.class));
		final SinkFunctionProvider sinkFunctionProvider = (SinkFunctionProvider) provider;
		final SinkFunction<RowData> sinkFunction = sinkFunctionProvider.createSinkFunction();
		assertThat(sinkFunction, instanceOf(getExpectedProducerClass()));
	}

	// --------------------------------------------------------------------------------------------
	// Negative tests
	// --------------------------------------------------------------------------------------------
	@Test
	public void testInvalidScanStartupMode() {
		// Construct table source using DDL and table source factory
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
				"default",
				"default",
				"scanTable");
		final Map<String, String> modifiedOptions = getModifiedOptions(
				getFullSourceOptions(),
				options -> {
					options.put("scan.startup.mode", "abc");
				});
		CatalogTable catalogTable = createKafkaSourceCatalogTable(modifiedOptions);

		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("Invalid value for option 'scan.startup.mode'. "
				+ "Supported values are [earliest-offset, latest-offset, group-offsets, specific-offsets, timestamp], "
				+ "but was: abc")));
		FactoryUtil.createTableSource(null,
				objectIdentifier,
				catalogTable,
				new Configuration(),
				Thread.currentThread().getContextClassLoader());
	}

	@Test
	public void testMissingStartupTimestamp() {
		// Construct table source using DDL and table source factory
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
				"default",
				"default",
				"scanTable");
		final Map<String, String> modifiedOptions = getModifiedOptions(
				getFullSourceOptions(),
				options -> {
					options.put("scan.startup.mode", "timestamp");
				});
		CatalogTable catalogTable = createKafkaSourceCatalogTable(modifiedOptions);

		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("'scan.startup.timestamp-millis' "
				+ "is required in 'timestamp' startup mode but missing.")));
		FactoryUtil.createTableSource(null,
				objectIdentifier,
				catalogTable,
				new Configuration(),
				Thread.currentThread().getContextClassLoader());
	}

	@Test
	public void testMissingSpecificOffsets() {
		// Construct table source using DDL and table source factory
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
				"default",
				"default",
				"scanTable");
		final Map<String, String> modifiedOptions = getModifiedOptions(
				getFullSourceOptions(),
				options -> {
					options.remove("scan.startup.specific-offsets");
				});
		CatalogTable catalogTable = createKafkaSourceCatalogTable(modifiedOptions);

		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("'scan.startup.specific-offsets' "
				+ "is required in 'specific-offsets' startup mode but missing.")));
		FactoryUtil.createTableSource(null,
				objectIdentifier,
				catalogTable,
				new Configuration(),
				Thread.currentThread().getContextClassLoader());
	}

	@Test
	public void testInvalidSinkPartitioner() {
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
				"default",
				"default",
				"sinkTable");

		final Map<String, String> modifiedOptions = getModifiedOptions(
				getFullSourceOptions(),
				options -> {
					options.put("sink.partitioner", "abc");
				});
		final CatalogTable sinkTable = createKafkaSinkCatalogTable(modifiedOptions);

		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("Could not find and instantiate partitioner class 'abc'")));
		FactoryUtil.createTableSink(
				null,
				objectIdentifier,
				sinkTable,
				new Configuration(),
				Thread.currentThread().getContextClassLoader());
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	private CatalogTable createKafkaSourceCatalogTable() {
		return createKafkaSourceCatalogTable(getFullSourceOptions());
	}

	private CatalogTable createKafkaSinkCatalogTable() {
		return createKafkaSinkCatalogTable(getFullSinkOptions());
	}

	private CatalogTable createKafkaSourceCatalogTable(Map<String, String> options) {
		return new CatalogTableImpl(SOURCE_SCHEMA, options, "scanTable");
	}

	private CatalogTable createKafkaSinkCatalogTable(Map<String, String> options) {
		return new CatalogTableImpl(SINK_SCHEMA, options, "sinkTable");
	}

	/**
	 * Returns the full options modified by the given consumer {@code optionModifier}.
	 *
	 * @param optionModifier Consumer to modify the options
	 */
	private static Map<String, String> getModifiedOptions(
			Map<String, String> options,
			Consumer<Map<String, String>> optionModifier) {
		optionModifier.accept(options);
		return options;
	}

	private Map<String, String> getFullSourceOptions() {
		Map<String, String> tableOptions = new HashMap<>();
		// Kafka specific options.
		tableOptions.put("connector", factoryIdentifier());
		tableOptions.put("topic", TOPIC);
		tableOptions.put("properties.group.id", "dummy");
		tableOptions.put("properties.bootstrap.servers", "dummy");
		tableOptions.put("scan.startup.mode", "specific-offsets");
		tableOptions.put("scan.startup.specific-offsets", PROPS_SCAN_OFFSETS);
		// Format options.
		tableOptions.put("format", TestFormatFactory.IDENTIFIER);
		final String formatDelimiterKey = String.format("%s.%s",
				TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());
		final String failOnMissingKey = String.format("%s.%s",
				TestFormatFactory.IDENTIFIER, TestFormatFactory.FAIL_ON_MISSING.key());
		tableOptions.put(formatDelimiterKey, ",");
		tableOptions.put(failOnMissingKey, "true");
		return tableOptions;
	}

	private Map<String, String> getFullSinkOptions() {
		Map<String, String> tableOptions = new HashMap<>();
		// Kafka specific options.
		tableOptions.put("connector", factoryIdentifier());
		tableOptions.put("topic", TOPIC);
		tableOptions.put("properties.group.id", "dummy");
		tableOptions.put("properties.bootstrap.servers", "dummy");
		tableOptions.put("sink.partitioner", KafkaOptions.SINK_PARTITIONER_VALUE_FIXED);
		// Format options.
		tableOptions.put("format", TestFormatFactory.IDENTIFIER);
		final String formatDelimiterKey = String.format("%s.%s",
				TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());
		tableOptions.put(formatDelimiterKey, ",");
		return tableOptions;
	}

	// --------------------------------------------------------------------------------------------
	// For version-specific tests
	// --------------------------------------------------------------------------------------------

	protected abstract String factoryIdentifier();

	protected abstract Class<?> getExpectedConsumerClass();

	protected abstract Class<?> getExpectedProducerClass();

	protected abstract KafkaDynamicSourceBase getExpectedScanSource(
			DataType producedDataType,
			String topic,
			Properties properties,
			DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestamp
	);

	protected abstract KafkaDynamicSinkBase getExpectedSink(
			DataType consumedDataType,
			String topic,
			Properties properties,
			Optional<FlinkKafkaPartitioner<RowData>> partitioner,
			EncodingFormat<SerializationSchema<RowData>> encodingFormat
	);
}
