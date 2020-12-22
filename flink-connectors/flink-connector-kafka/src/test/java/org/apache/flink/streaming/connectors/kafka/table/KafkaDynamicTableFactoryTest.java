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
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.config.StartupMode;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.ChangelogMode;
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
import org.apache.flink.table.factories.TestFormatFactory.DecodingFormatMock;
import org.apache.flink.table.factories.TestFormatFactory.EncodingFormatMock;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.regex.Pattern;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Abstract test base for {@link KafkaDynamicTableFactory}.
 */
public class KafkaDynamicTableFactoryTest extends TestLogger {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static final String TOPIC = "myTopic";
	private static final String TOPICS = "myTopic-1;myTopic-2;myTopic-3";
	private static final String TOPIC_REGEX = "myTopic-\\d+";
	private static final List<String> TOPIC_LIST = Arrays.asList("myTopic-1", "myTopic-2", "myTopic-3");
	private static final int PARTITION_0 = 0;
	private static final long OFFSET_0 = 100L;
	private static final int PARTITION_1 = 1;
	private static final long OFFSET_1 = 123L;
	private static final String NAME = "name";
	private static final String COUNT = "count";
	private static final String TIME = "time";
	private static final String METADATA = "metadata";
	private static final String WATERMARK_EXPRESSION = TIME + " - INTERVAL '5' SECOND";
	private static final DataType WATERMARK_DATATYPE = DataTypes.TIMESTAMP(3);
	private static final String COMPUTED_COLUMN_NAME = "computed-column";
	private static final String COMPUTED_COLUMN_EXPRESSION = COUNT + " + 1.0";
	private static final DataType COMPUTED_COLUMN_DATATYPE = DataTypes.DECIMAL(10, 3);
	private static final String DISCOVERY_INTERVAL = "1000 ms";

	private static final Properties KAFKA_SOURCE_PROPERTIES = new Properties();
	private static final Properties KAFKA_FINAL_SOURCE_PROPERTIES = new Properties();
	private static final Properties KAFKA_SINK_PROPERTIES = new Properties();
	private static final Properties KAFKA_FINAL_SINK_PROPERTIES = new Properties();
	static {
		KAFKA_SOURCE_PROPERTIES.setProperty("group.id", "dummy");
		KAFKA_SOURCE_PROPERTIES.setProperty("bootstrap.servers", "dummy");
		KAFKA_SOURCE_PROPERTIES.setProperty("flink.partition-discovery.interval-millis", "1000");

		KAFKA_SINK_PROPERTIES.setProperty("group.id", "dummy");
		KAFKA_SINK_PROPERTIES.setProperty("bootstrap.servers", "dummy");

		KAFKA_FINAL_SINK_PROPERTIES.putAll(KAFKA_SINK_PROPERTIES);
		KAFKA_FINAL_SINK_PROPERTIES.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		KAFKA_FINAL_SINK_PROPERTIES.setProperty("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		KAFKA_FINAL_SINK_PROPERTIES.put("transaction.timeout.ms", 3600000);

		KAFKA_FINAL_SOURCE_PROPERTIES.putAll(KAFKA_SOURCE_PROPERTIES);
		KAFKA_FINAL_SOURCE_PROPERTIES.setProperty("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		KAFKA_FINAL_SOURCE_PROPERTIES.setProperty("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
	}

	private static final String PROPS_SCAN_OFFSETS =
			String.format("partition:%d,offset:%d;partition:%d,offset:%d",
					PARTITION_0, OFFSET_0, PARTITION_1, OFFSET_1);

	private static final TableSchema SCHEMA = TableSchema.builder()
			.add(TableColumn.physical(NAME, DataTypes.STRING()))
			.add(TableColumn.physical(COUNT, DataTypes.DECIMAL(38, 18)))
			.add(TableColumn.physical(TIME, DataTypes.TIMESTAMP(3)))
			.add(TableColumn.computed(COMPUTED_COLUMN_NAME, COMPUTED_COLUMN_DATATYPE, COMPUTED_COLUMN_EXPRESSION))
			.watermark(TIME, WATERMARK_EXPRESSION, WATERMARK_DATATYPE)
			.build();

	private static final TableSchema SCHEMA_WITH_METADATA = TableSchema.builder()
			.add(TableColumn.physical(NAME, DataTypes.STRING()))
			.add(TableColumn.physical(COUNT, DataTypes.DECIMAL(38, 18)))
			.add(TableColumn.metadata(TIME, DataTypes.TIMESTAMP(3), "timestamp"))
			.add(TableColumn.metadata(METADATA, DataTypes.STRING(), "value.metadata_2"))
			.build();

	private static final DataType SCHEMA_DATA_TYPE = SCHEMA.toPhysicalRowDataType();

	@Test
	public void testTableSource() {
		final DynamicTableSource actualSource = createTableSource(SCHEMA, getBasicSourceOptions());
		final KafkaDynamicSource actualKafkaSource = (KafkaDynamicSource) actualSource;

		final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();
		specificOffsets.put(new KafkaTopicPartition(TOPIC, PARTITION_0), OFFSET_0);
		specificOffsets.put(new KafkaTopicPartition(TOPIC, PARTITION_1), OFFSET_1);

		final DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
				new DecodingFormatMock(",", true);

		// Test scan source equals
		final KafkaDynamicSource expectedKafkaSource = createExpectedScanSource(
				SCHEMA_DATA_TYPE,
				null,
				valueDecodingFormat,
				new int[0],
				new int[]{0, 1, 2},
				null,
				Collections.singletonList(TOPIC),
				null,
				KAFKA_SOURCE_PROPERTIES,
				StartupMode.SPECIFIC_OFFSETS,
				specificOffsets,
				0);
		assertEquals(actualKafkaSource, expectedKafkaSource);

		// Test Kafka consumer
		ScanTableSource.ScanRuntimeProvider provider =
				actualKafkaSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
		assertThat(provider, instanceOf(SourceFunctionProvider.class));
		final SourceFunctionProvider sourceFunctionProvider = (SourceFunctionProvider) provider;
		final SourceFunction<RowData> sourceFunction = sourceFunctionProvider.createSourceFunction();
		assertThat(sourceFunction, instanceOf(FlinkKafkaConsumer.class));

		// Test commitOnCheckpoints flag should be true when set consumer group
		assertTrue(((FlinkKafkaConsumer<?>) sourceFunction).getEnableCommitOnCheckpoints());
	}

	@Test
	public void testTableSourceCommitOnCheckpointsDisabled() {
		final Map<String, String> modifiedOptions = getModifiedOptions(
			getBasicSourceOptions(),
			options -> options.remove("properties.group.id"));
		final DynamicTableSource tableSource = createTableSource(SCHEMA, modifiedOptions);

		// Test commitOnCheckpoints flag should be false when do not set consumer group.
		assertThat(tableSource, instanceOf(KafkaDynamicSource.class));
		ScanTableSource.ScanRuntimeProvider providerWithoutGroupId = ((KafkaDynamicSource) tableSource)
			.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
		assertThat(providerWithoutGroupId, instanceOf(SourceFunctionProvider.class));
		final SourceFunctionProvider functionProviderWithoutGroupId = (SourceFunctionProvider) providerWithoutGroupId;
		final SourceFunction<RowData> function = functionProviderWithoutGroupId.createSourceFunction();
		assertFalse(((FlinkKafkaConsumer<?>) function).getEnableCommitOnCheckpoints());
	}

	@Test
	public void testTableSourceWithPattern() {
		final Map<String, String> modifiedOptions = getModifiedOptions(
			getBasicSourceOptions(),
			options -> {
				options.remove("topic");
				options.put("topic-pattern", TOPIC_REGEX);
				options.put("scan.startup.mode", KafkaOptions.SCAN_STARTUP_MODE_VALUE_EARLIEST);
				options.remove("scan.startup.specific-offsets");
			});
		final DynamicTableSource actualSource = createTableSource(SCHEMA, modifiedOptions);

		final Map<KafkaTopicPartition, Long> specificOffsets = new HashMap<>();

		DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
			new DecodingFormatMock(",", true);

		// Test scan source equals
		final KafkaDynamicSource expectedKafkaSource = createExpectedScanSource(
			SCHEMA_DATA_TYPE,
			null,
			valueDecodingFormat,
			new int[0],
			new int[]{0, 1, 2},
			null,
			null,
			Pattern.compile(TOPIC_REGEX),
			KAFKA_SOURCE_PROPERTIES,
			StartupMode.EARLIEST,
			specificOffsets,
			0);
		final KafkaDynamicSource actualKafkaSource = (KafkaDynamicSource) actualSource;
		assertEquals(actualKafkaSource, expectedKafkaSource);

		// Test Kafka consumer
		ScanTableSource.ScanRuntimeProvider provider =
			actualKafkaSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
		assertThat(provider, instanceOf(SourceFunctionProvider.class));
		final SourceFunctionProvider sourceFunctionProvider = (SourceFunctionProvider) provider;
		final SourceFunction<RowData> sourceFunction = sourceFunctionProvider.createSourceFunction();
		assertThat(sourceFunction, instanceOf(FlinkKafkaConsumer.class));
	}

	@Test
	public void testTableSourceWithKeyValue() {
		final DynamicTableSource actualSource = createTableSource(SCHEMA, getKeyValueOptions());
		final KafkaDynamicSource actualKafkaSource = (KafkaDynamicSource) actualSource;
		// initialize stateful testing formats
		actualKafkaSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

		final DecodingFormatMock keyDecodingFormat = new DecodingFormatMock("#", false);
		keyDecodingFormat.producedDataType = DataTypes.ROW(
				DataTypes.FIELD(NAME, DataTypes.STRING()))
			.notNull();

		final DecodingFormatMock valueDecodingFormat = new DecodingFormatMock("|", false);
		valueDecodingFormat.producedDataType = DataTypes.ROW(
				DataTypes.FIELD(COUNT, DataTypes.DECIMAL(38, 18)),
				DataTypes.FIELD(TIME, DataTypes.TIMESTAMP(3)))
			.notNull();

		final KafkaDynamicSource expectedKafkaSource = createExpectedScanSource(
				SCHEMA_DATA_TYPE,
				keyDecodingFormat,
				valueDecodingFormat,
				new int[]{0},
				new int[]{1, 2},
				null,
				Collections.singletonList(TOPIC),
				null,
				KAFKA_FINAL_SOURCE_PROPERTIES,
				StartupMode.GROUP_OFFSETS,
				Collections.emptyMap(),
				0);

		assertEquals(actualSource, expectedKafkaSource);
	}

	@Test
	public void testTableSourceWithKeyValueAndMetadata() {
		final Map<String, String> options = getKeyValueOptions();
		options.put("value.test-format.readable-metadata", "metadata_1:INT, metadata_2:STRING");

		final DynamicTableSource actualSource = createTableSource(SCHEMA_WITH_METADATA, options);
		final KafkaDynamicSource actualKafkaSource = (KafkaDynamicSource) actualSource;
		// initialize stateful testing formats
		actualKafkaSource.applyReadableMetadata(Arrays.asList("timestamp", "value.metadata_2"), SCHEMA_WITH_METADATA.toRowDataType());
		actualKafkaSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);

		final DecodingFormatMock expectedKeyFormat = new DecodingFormatMock(
			"#",
			false,
			ChangelogMode.insertOnly(),
			Collections.emptyMap());
		expectedKeyFormat.producedDataType = DataTypes.ROW(
				DataTypes.FIELD(NAME, DataTypes.STRING()))
			.notNull();

		final Map<String, DataType> expectedReadableMetadata = new HashMap<>();
		expectedReadableMetadata.put("metadata_1", DataTypes.INT());
		expectedReadableMetadata.put("metadata_2", DataTypes.STRING());

		final DecodingFormatMock expectedValueFormat = new DecodingFormatMock(
			"|",
			false,
			ChangelogMode.insertOnly(),
			expectedReadableMetadata);
		expectedValueFormat.producedDataType = DataTypes.ROW(
				DataTypes.FIELD(COUNT, DataTypes.DECIMAL(38, 18)),
				DataTypes.FIELD("metadata_2", DataTypes.STRING()))
			.notNull();
		expectedValueFormat.metadataKeys = Collections.singletonList("metadata_2");

		final KafkaDynamicSource expectedKafkaSource = createExpectedScanSource(
				SCHEMA_WITH_METADATA.toPhysicalRowDataType(),
				expectedKeyFormat,
				expectedValueFormat,
				new int[]{0},
				new int[]{1},
				null,
				Collections.singletonList(TOPIC),
				null,
				KAFKA_FINAL_SOURCE_PROPERTIES,
				StartupMode.GROUP_OFFSETS,
				Collections.emptyMap(),
				0);
		expectedKafkaSource.producedDataType = SCHEMA_WITH_METADATA.toRowDataType();
		expectedKafkaSource.metadataKeys = Collections.singletonList("timestamp");

		assertEquals(actualSource, expectedKafkaSource);
	}

	@Test
	public void testTableSink() {
		final DynamicTableSink actualSink = createTableSink(SCHEMA, getBasicSinkOptions());

		final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
				new EncodingFormatMock(",");

		final DynamicTableSink expectedSink = createExpectedSink(
				SCHEMA_DATA_TYPE,
				null,
				valueEncodingFormat,
				new int[0],
				new int[]{0, 1, 2},
				null,
				TOPIC,
				KAFKA_SINK_PROPERTIES,
				new FlinkFixedPartitioner<>(),
				KafkaSinkSemantic.EXACTLY_ONCE,
				null
			);
		assertEquals(expectedSink, actualSink);

		// Test kafka producer.
		final KafkaDynamicSink actualKafkaSink = (KafkaDynamicSink) actualSink;
		DynamicTableSink.SinkRuntimeProvider provider =
				actualKafkaSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
		assertThat(provider, instanceOf(SinkFunctionProvider.class));
		final SinkFunctionProvider sinkFunctionProvider = (SinkFunctionProvider) provider;
		final SinkFunction<RowData> sinkFunction = sinkFunctionProvider.createSinkFunction();
		assertThat(sinkFunction, instanceOf(FlinkKafkaProducer.class));
	}

	@Test
	public void testTableSinkWithKeyValue() {
		final DynamicTableSink actualSink = createTableSink(SCHEMA, getKeyValueOptions());
		final KafkaDynamicSink actualKafkaSink = (KafkaDynamicSink) actualSink;
		// initialize stateful testing formats
		actualKafkaSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));

		final EncodingFormatMock keyEncodingFormat = new EncodingFormatMock("#");
		keyEncodingFormat.consumedDataType = DataTypes.ROW(
				DataTypes.FIELD(NAME, DataTypes.STRING()))
			.notNull();

		final EncodingFormatMock valueEncodingFormat = new EncodingFormatMock("|");
		valueEncodingFormat.consumedDataType = DataTypes.ROW(
				DataTypes.FIELD(COUNT, DataTypes.DECIMAL(38, 18)),
				DataTypes.FIELD(TIME, DataTypes.TIMESTAMP(3)))
			.notNull();

		final DynamicTableSink expectedSink = createExpectedSink(
				SCHEMA_DATA_TYPE,
				keyEncodingFormat,
				valueEncodingFormat,
				new int[]{0},
				new int[]{1, 2},
				null,
				TOPIC,
				KAFKA_FINAL_SINK_PROPERTIES,
				new FlinkFixedPartitioner<>(),
				KafkaSinkSemantic.EXACTLY_ONCE,
				null
			);

		assertEquals(expectedSink, actualSink);
	}

	@Test
	public void testTableSinkWithParallelism() {
		final Map<String, String> modifiedOptions = getModifiedOptions(
			getBasicSinkOptions(),
			options -> options.put("sink.parallelism", "100"));
		KafkaDynamicSink actualSink = (KafkaDynamicSink) createTableSink(SCHEMA, modifiedOptions);

		final EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
			new EncodingFormatMock(",");

		final DynamicTableSink expectedSink = createExpectedSink(
			SCHEMA_DATA_TYPE,
			null,
			valueEncodingFormat,
			new int[0],
			new int[]{0, 1, 2},
			null,
			TOPIC,
			KAFKA_SINK_PROPERTIES,
			new FlinkFixedPartitioner<>(),
			KafkaSinkSemantic.EXACTLY_ONCE,
			100
		);
		assertEquals(expectedSink, actualSink);

		final DynamicTableSink.SinkRuntimeProvider provider =
			actualSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
		assertThat(provider, instanceOf(SinkFunctionProvider.class));
		final SinkFunctionProvider sinkFunctionProvider = (SinkFunctionProvider) provider;
		assertTrue(sinkFunctionProvider.getParallelism().isPresent());
		assertEquals(100, (long) sinkFunctionProvider.getParallelism().get());
	}

	// --------------------------------------------------------------------------------------------
	// Negative tests
	// --------------------------------------------------------------------------------------------

	@Test
	public void testInvalidScanStartupMode() {
		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("Invalid value for option 'scan.startup.mode'. "
				+ "Supported values are [earliest-offset, latest-offset, group-offsets, specific-offsets, timestamp], "
				+ "but was: abc")));

		final Map<String, String> modifiedOptions = getModifiedOptions(
				getBasicSourceOptions(),
				options -> options.put("scan.startup.mode", "abc"));

		createTableSource(SCHEMA, modifiedOptions);
	}

	@Test
	public void testSourceTableWithTopicAndTopicPattern() {
		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("Option 'topic' and 'topic-pattern' shouldn't be set together.")));

		final Map<String, String> modifiedOptions = getModifiedOptions(
			getBasicSourceOptions(),
			options -> {
				options.put("topic", TOPICS);
				options.put("topic-pattern", TOPIC_REGEX);
			});

		createTableSource(SCHEMA, modifiedOptions);
	}

	@Test
	public void testMissingStartupTimestamp() {
		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("'scan.startup.timestamp-millis' "
				+ "is required in 'timestamp' startup mode but missing.")));

		final Map<String, String> modifiedOptions = getModifiedOptions(
				getBasicSourceOptions(),
				options -> options.put("scan.startup.mode", "timestamp"));

		createTableSource(SCHEMA, modifiedOptions);
	}

	@Test
	public void testMissingSpecificOffsets() {
		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("'scan.startup.specific-offsets' "
				+ "is required in 'specific-offsets' startup mode but missing.")));

		final Map<String, String> modifiedOptions = getModifiedOptions(
				getBasicSourceOptions(),
				options -> options.remove("scan.startup.specific-offsets"));

		createTableSource(SCHEMA, modifiedOptions);
	}

	@Test
	public void testInvalidSinkPartitioner() {
		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("Could not find and instantiate partitioner "
				+ "class 'abc'")));

		final Map<String, String> modifiedOptions = getModifiedOptions(
				getBasicSinkOptions(),
				options -> options.put("sink.partitioner", "abc"));

		createTableSink(SCHEMA, modifiedOptions);
	}

	@Test
	public void testInvalidRoundRobinPartitionerWithKeyFields() {
		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("Currently 'round-robin' partitioner only works " +
				"when option 'key.fields' is not specified.")));

		final Map<String, String> modifiedOptions = getModifiedOptions(
				getKeyValueOptions(),
				options -> options.put("sink.partitioner", "round-robin"));

		createTableSink(SCHEMA, modifiedOptions);
	}

	@Test
	public void testInvalidSinkSemantic() {
		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("Unsupported value 'xyz' for 'sink.semantic'. "
				+ "Supported values are ['at-least-once', 'exactly-once', 'none'].")));

		final Map<String, String> modifiedOptions = getModifiedOptions(
			getBasicSinkOptions(),
			options -> options.put("sink.semantic", "xyz"));

		createTableSink(SCHEMA, modifiedOptions);
	}

	@Test
	public void testSinkWithTopicListOrTopicPattern() {
		Map<String, String> modifiedOptions = getModifiedOptions(
			getBasicSinkOptions(),
			options -> {
				options.put("topic", TOPICS);
				options.put("scan.startup.mode", "earliest-offset");
				options.remove("specific-offsets");
			});
		final String errorMessageTemp = "Flink Kafka sink currently only supports single topic, but got %s: %s.";

		try {
			createTableSink(SCHEMA, modifiedOptions);
		} catch (Throwable t) {
			assertEquals(String.format(errorMessageTemp, "'topic'", String.format("[%s]", String.join(", ", TOPIC_LIST))),
				t.getCause().getMessage());
		}

		modifiedOptions = getModifiedOptions(
			getBasicSinkOptions(),
			options -> options.put("topic-pattern", TOPIC_REGEX));

		try {
			createTableSink(SCHEMA, modifiedOptions);
		} catch (Throwable t) {
			assertEquals(String.format(errorMessageTemp, "'topic-pattern'", TOPIC_REGEX), t.getCause().getMessage());
		}
	}

	@Test
	public void testPrimaryKeyValidation() {
		final TableSchema pkSchema = TableSchema.builder()
			.field(NAME, DataTypes.STRING().notNull())
			.field(COUNT, DataTypes.DECIMAL(38, 18))
			.field(TIME, DataTypes.TIMESTAMP(3))
			.field(COMPUTED_COLUMN_NAME, COMPUTED_COLUMN_DATATYPE, COMPUTED_COLUMN_EXPRESSION)
			.watermark(TIME, WATERMARK_EXPRESSION, WATERMARK_DATATYPE)
			.primaryKey(NAME)
			.build();

		Map<String, String> sinkOptions = getModifiedOptions(
			getBasicSinkOptions(),
			options ->
				options.put(
					String.format(
						"%s.%s",
						TestFormatFactory.IDENTIFIER,
						TestFormatFactory.CHANGELOG_MODE.key()),
					"I;UA;UB;D"));
		// pk can be defined on cdc table, should pass
		createTableSink(pkSchema, sinkOptions);

		try {
			createTableSink(pkSchema, getBasicSinkOptions());
			fail();
		} catch (Throwable t) {
			String error = "The Kafka table 'default.default.sinkTable' with 'test-format' format" +
				" doesn't support defining PRIMARY KEY constraint on the table, because it can't" +
				" guarantee the semantic of primary key.";
			assertEquals(error, t.getCause().getMessage());
		}

		Map<String, String> sourceOptions = getModifiedOptions(
				getBasicSourceOptions(),
				options ->
						options.put(
								String.format(
										"%s.%s",
										TestFormatFactory.IDENTIFIER,
										TestFormatFactory.CHANGELOG_MODE.key()),
								"I;UA;UB;D"));
		// pk can be defined on cdc table, should pass
		createTableSource(pkSchema, sourceOptions);

		try {
			createTableSource(pkSchema, getBasicSourceOptions());
			fail();
		} catch (Throwable t) {
			String error = "The Kafka table 'default.default.scanTable' with 'test-format' format" +
				" doesn't support defining PRIMARY KEY constraint on the table, because it can't" +
				" guarantee the semantic of primary key.";
			assertEquals(error, t.getCause().getMessage());
		}
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

	private static KafkaDynamicSource createExpectedScanSource(
			DataType physicalDataType,
			@Nullable DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
			DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
			int[] keyProjection,
			int[] valueProjection,
			@Nullable String keyPrefix,
			@Nullable List<String> topics,
			@Nullable Pattern topicPattern,
			Properties properties,
			StartupMode startupMode,
			Map<KafkaTopicPartition, Long> specificStartupOffsets,
			long startupTimestampMillis) {
		return new KafkaDynamicSource(
				physicalDataType,
				keyDecodingFormat,
				valueDecodingFormat,
				keyProjection,
				valueProjection,
				keyPrefix,
				topics,
				topicPattern,
				properties,
				startupMode,
				specificStartupOffsets,
				startupTimestampMillis,
				false);
	}

	private static KafkaDynamicSink createExpectedSink(
			DataType physicalDataType,
			@Nullable EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
			EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
			int[] keyProjection,
			int[] valueProjection,
			@Nullable String keyPrefix,
			String topic,
			Properties properties,
			@Nullable FlinkKafkaPartitioner<RowData> partitioner,
			KafkaSinkSemantic semantic,
			@Nullable Integer parallelism) {
		return new KafkaDynamicSink(
				physicalDataType,
				keyEncodingFormat,
				valueEncodingFormat,
				keyProjection,
				valueProjection,
				keyPrefix,
				topic,
				properties,
				partitioner,
				semantic,
				false,
				parallelism);
	}

	private static DynamicTableSource createTableSource(TableSchema schema, Map<String, String> options) {
		final ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
				"default",
				"default",
				"scanTable");
		final CatalogTable catalogTable = new CatalogTableImpl(schema, options, "scanTable");
		return FactoryUtil.createTableSource(
			null,
			objectIdentifier,
			catalogTable,
			new Configuration(),
			Thread.currentThread().getContextClassLoader(),
			false);
	}

	private static DynamicTableSink createTableSink(TableSchema schema, Map<String, String> options) {
		final ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
				"default",
				"default",
				"sinkTable");
		final CatalogTable catalogTable = new CatalogTableImpl(schema, options, "sinkTable");
		return FactoryUtil.createTableSink(
			null,
			objectIdentifier,
			catalogTable,
			new Configuration(),
			Thread.currentThread().getContextClassLoader(),
			false);
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

	private static Map<String, String> getBasicSourceOptions() {
		Map<String, String> tableOptions = new HashMap<>();
		// Kafka specific options.
		tableOptions.put("connector", KafkaDynamicTableFactory.IDENTIFIER);
		tableOptions.put("topic", TOPIC);
		tableOptions.put("properties.group.id", "dummy");
		tableOptions.put("properties.bootstrap.servers", "dummy");
		tableOptions.put("scan.startup.mode", "specific-offsets");
		tableOptions.put("scan.startup.specific-offsets", PROPS_SCAN_OFFSETS);
		tableOptions.put("scan.topic-partition-discovery.interval", DISCOVERY_INTERVAL);
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

	private static Map<String, String> getBasicSinkOptions() {
		Map<String, String> tableOptions = new HashMap<>();
		// Kafka specific options.
		tableOptions.put("connector", KafkaDynamicTableFactory.IDENTIFIER);
		tableOptions.put("topic", TOPIC);
		tableOptions.put("properties.group.id", "dummy");
		tableOptions.put("properties.bootstrap.servers", "dummy");
		tableOptions.put("sink.partitioner", KafkaOptions.SINK_PARTITIONER_VALUE_FIXED);
		tableOptions.put("sink.semantic", KafkaOptions.SINK_SEMANTIC_VALUE_EXACTLY_ONCE);
		// Format options.
		tableOptions.put("format", TestFormatFactory.IDENTIFIER);
		final String formatDelimiterKey = String.format("%s.%s",
				TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key());
		tableOptions.put(formatDelimiterKey, ",");
		return tableOptions;
	}

	private static Map<String, String> getKeyValueOptions() {
		Map<String, String> tableOptions = new HashMap<>();
		// Kafka specific options.
		tableOptions.put("connector", KafkaDynamicTableFactory.IDENTIFIER);
		tableOptions.put("topic", TOPIC);
		tableOptions.put("properties.group.id", "dummy");
		tableOptions.put("properties.bootstrap.servers", "dummy");
		tableOptions.put("scan.topic-partition-discovery.interval", DISCOVERY_INTERVAL);
		tableOptions.put("sink.partitioner", KafkaOptions.SINK_PARTITIONER_VALUE_FIXED);
		tableOptions.put("sink.semantic", KafkaOptions.SINK_SEMANTIC_VALUE_EXACTLY_ONCE);
		// Format options.
		tableOptions.put("key.format", TestFormatFactory.IDENTIFIER);
		tableOptions.put(
				String.format("key.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()),
				"#");
		tableOptions.put("key.fields", NAME);
		tableOptions.put("value.format", TestFormatFactory.IDENTIFIER);
		tableOptions.put(
				String.format("value.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()),
				"|");
		tableOptions.put("value.fields-include", KafkaOptions.ValueFieldsStrategy.EXCEPT_KEY.toString());
		return tableOptions;
	}
}
