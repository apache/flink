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
import org.apache.flink.table.api.DataTypes;
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
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Test for {@link UpsertKafkaDynamicTableFactory}.
 */
public class UpsertKafkaDynamicTableFactoryTest extends TestLogger {

	private static final String SOURCE_TOPIC = "sourceTopic_1";

	private static final String SINK_TOPIC = "sinkTopic";

	private static final TableSchema SOURCE_SCHEMA = TableSchema.builder()
			.field("window_start", DataTypes.STRING().notNull())
			.field("region", DataTypes.STRING().notNull())
			.field("view_count", DataTypes.BIGINT())
			.primaryKey("window_start", "region")
			.build();

	private static final int[] SOURCE_KEY_FIELDS = new int[]{0, 1};

	private static final int[] SOURCE_VALUE_FIELDS = new int[]{0, 1, 2};

	private static final TableSchema SINK_SCHEMA = TableSchema.builder()
			.field("region", new AtomicDataType(new VarCharType(false, 100)))
			.field("view_count", DataTypes.BIGINT())
			.primaryKey("region")
			.build();

	private static final int[] SINK_KEY_FIELDS = new int[]{0};

	private static final int[] SINK_VALUE_FIELDS = new int[]{0, 1};

	private static final Properties UPSERT_KAFKA_SOURCE_PROPERTIES = new Properties();
	private static final Properties UPSERT_KAFKA_SINK_PROPERTIES = new Properties();

	static {
		UPSERT_KAFKA_SOURCE_PROPERTIES.setProperty("bootstrap.servers", "dummy");

		UPSERT_KAFKA_SINK_PROPERTIES.setProperty("bootstrap.servers", "dummy");
	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testTableSource() {
		final DataType producedDataType = SOURCE_SCHEMA.toPhysicalRowDataType();

		DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat =
				new TestFormatFactory.DecodingFormatMock(
						",", true, ChangelogMode.insertOnly(), Collections.emptyMap());

		DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat =
				new TestFormatFactory.DecodingFormatMock(
						",", true, ChangelogMode.insertOnly(), Collections.emptyMap());

		// Construct table source using options and table source factory
		final DynamicTableSource actualSource = createActualSource(SOURCE_SCHEMA, getFullSourceOptions());

		final KafkaDynamicSource expectedSource = createExpectedScanSource(
				producedDataType,
				keyDecodingFormat,
				valueDecodingFormat,
				SOURCE_KEY_FIELDS,
				SOURCE_VALUE_FIELDS,
				null,
				SOURCE_TOPIC,
				UPSERT_KAFKA_SOURCE_PROPERTIES);
		assertEquals(actualSource, expectedSource);

		final KafkaDynamicSource actualUpsertKafkaSource = (KafkaDynamicSource) actualSource;
		ScanTableSource.ScanRuntimeProvider provider =
				actualUpsertKafkaSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
		assertThat(provider, instanceOf(SourceFunctionProvider.class));
		final SourceFunctionProvider sourceFunctionProvider = (SourceFunctionProvider) provider;
		final SourceFunction<RowData> sourceFunction = sourceFunctionProvider.createSourceFunction();
		assertThat(sourceFunction, instanceOf(FlinkKafkaConsumer.class));
	}

	@Test
	public void testTableSink() {
		EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat =
				new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());
		EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
				new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());

		// Construct table sink using options and table sink factory.
		final DynamicTableSink actualSink = createActualSink(SINK_SCHEMA, getFullSinkOptions());

		final DynamicTableSink expectedSink = createExpectedSink(
				SINK_SCHEMA.toPhysicalRowDataType(),
				keyEncodingFormat,
				valueEncodingFormat,
				SINK_KEY_FIELDS,
				SINK_VALUE_FIELDS,
				null,
				SINK_TOPIC,
				UPSERT_KAFKA_SINK_PROPERTIES,
				null);

		// Test sink format.
		final KafkaDynamicSink actualUpsertKafkaSink = (KafkaDynamicSink) actualSink;
		assertEquals(expectedSink, actualSink);

		// Test kafka producer.
		DynamicTableSink.SinkRuntimeProvider provider =
				actualUpsertKafkaSink.getSinkRuntimeProvider(new SinkRuntimeProviderContext(false));
		assertThat(provider, instanceOf(SinkFunctionProvider.class));
		final SinkFunctionProvider sinkFunctionProvider = (SinkFunctionProvider) provider;
		final SinkFunction<RowData> sinkFunction = sinkFunctionProvider.createSinkFunction();
		assertThat(sinkFunction, instanceOf(FlinkKafkaProducer.class));
	}

	@Test
	public void testTableSinkWithParallelism() {
		final Map<String, String> modifiedOptions = getModifiedOptions(
			getFullSinkOptions(),
			options -> options.put("sink.parallelism", "100"));
		final DynamicTableSink actualSink = createActualSink(SINK_SCHEMA, modifiedOptions);

		EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat =
			new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());
		EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat =
			new TestFormatFactory.EncodingFormatMock(",", ChangelogMode.insertOnly());

		final DynamicTableSink expectedSink = createExpectedSink(
			SINK_SCHEMA.toPhysicalRowDataType(),
			keyEncodingFormat,
			valueEncodingFormat,
			SINK_KEY_FIELDS,
			SINK_VALUE_FIELDS,
			null,
			SINK_TOPIC,
			UPSERT_KAFKA_SINK_PROPERTIES,
			100);
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
	public void testCreateSourceTableWithoutPK() {
		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("'upsert-kafka' tables require to define a PRIMARY KEY constraint. " +
				"The PRIMARY KEY specifies which columns should be read from or write to the Kafka message key. " +
				"The PRIMARY KEY also defines records in the 'upsert-kafka' table should update or delete on which keys.")));

		TableSchema illegalSchema = TableSchema.builder()
				.field("window_start", DataTypes.STRING())
				.field("region", DataTypes.STRING())
				.field("view_count", DataTypes.BIGINT())
				.build();
		createActualSource(illegalSchema, getFullSourceOptions());
	}

	@Test
	public void testCreateSinkTableWithoutPK() {
		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("'upsert-kafka' tables require to define a PRIMARY KEY constraint. " +
				"The PRIMARY KEY specifies which columns should be read from or write to the Kafka message key. " +
				"The PRIMARY KEY also defines records in the 'upsert-kafka' table should update or delete on which keys.")));

		TableSchema illegalSchema = TableSchema.builder()
				.field("region", DataTypes.STRING())
				.field("view_count", DataTypes.BIGINT())
				.build();
		createActualSink(illegalSchema, getFullSinkOptions());
	}

	@Test
	public void testSerWithCDCFormatAsValue() {
		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(
				new ValidationException(String.format(
						"'upsert-kafka' connector doesn't support '%s' as value format, " +
								"because '%s' is not in insert-only mode.",
						TestFormatFactory.IDENTIFIER, TestFormatFactory.IDENTIFIER
				))));

		createActualSink(SINK_SCHEMA,
				getModifiedOptions(
						getFullSinkOptions(),
						options -> options.put(
								String.format("value.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()),
								"I;UA;UB;D")));
	}

	@Test
	public void testDeserWithCDCFormatAsValue() {
		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(
				new ValidationException(String.format(
						"'upsert-kafka' connector doesn't support '%s' as value format, " +
								"because '%s' is not in insert-only mode.",
						TestFormatFactory.IDENTIFIER,
						TestFormatFactory.IDENTIFIER
				))));

		createActualSource(SOURCE_SCHEMA,
				getModifiedOptions(
						getFullSourceOptions(),
						options -> options.put(
								String.format("value.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()),
								"I;UA;UB;D")));
	}

	// --------------------------------------------------------------------------------------------
	// Utilities
	// --------------------------------------------------------------------------------------------

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

	private static Map<String, String> getFullSourceOptions() {
		// table options
		Map<String, String> options = new HashMap<>();
		options.put("connector", UpsertKafkaDynamicTableFactory.IDENTIFIER);
		options.put("topic", SOURCE_TOPIC);
		options.put("properties.bootstrap.servers", "dummy");
		// key format options
		options.put("key.format", TestFormatFactory.IDENTIFIER);
		options.put(
				String.format("key.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()), ",");
		options.put(
				String.format("key.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.FAIL_ON_MISSING.key()), "true");
		options.put(
				String.format("key.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()), "I");
		// value format options
		options.put("value.format", TestFormatFactory.IDENTIFIER);
		options.put(
				String.format("value.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()), ",");
		options.put(
				String.format("value.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.FAIL_ON_MISSING.key()), "true");
		options.put(
				String.format("value.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()), "I");
		return options;
	}

	private static Map<String, String> getFullSinkOptions() {
		Map<String, String> options = new HashMap<>();
		options.put("connector", UpsertKafkaDynamicTableFactory.IDENTIFIER);
		options.put("topic", SINK_TOPIC);
		options.put("properties.bootstrap.servers", "dummy");
		// key format options
		options.put("value.format", TestFormatFactory.IDENTIFIER);
		options.put(
				String.format("key.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()), ",");
		options.put(
				String.format("key.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()), "I"
		);
		// value format options
		options.put("key.format", TestFormatFactory.IDENTIFIER);
		options.put(
				String.format("value.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.DELIMITER.key()), ",");
		options.put(
				String.format("value.%s.%s", TestFormatFactory.IDENTIFIER, TestFormatFactory.CHANGELOG_MODE.key()), "I"
		);
		return options;
	}

	private static DynamicTableSource createActualSource(TableSchema schema, Map<String, String> options) {
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
				"default",
				"default",
				"sourceTable");
		final CatalogTable sourceTable =
				new CatalogTableImpl(schema, options, "sinkTable");

		return FactoryUtil.createTableSource(
				null,
				objectIdentifier,
				sourceTable,
				new Configuration(),
				Thread.currentThread().getContextClassLoader(),
				false);
	}

	private static DynamicTableSink createActualSink(TableSchema schema, Map<String, String> options) {
		ObjectIdentifier objectIdentifier = ObjectIdentifier.of(
				"default",
				"default",
				"sinkTable");
		final CatalogTable sinkTable =
				new CatalogTableImpl(schema, options, "sinkTable");
		return FactoryUtil.createTableSink(
				null,
				objectIdentifier,
				sinkTable,
				new Configuration(),
				Thread.currentThread().getContextClassLoader(),
				false);
	}

	private static KafkaDynamicSource createExpectedScanSource(
			DataType producedDataType,
			DecodingFormat<DeserializationSchema<RowData>> keyDecodingFormat,
			DecodingFormat<DeserializationSchema<RowData>> valueDecodingFormat,
			int[] keyFields,
			int[] valueFields,
			String keyPrefix,
			String topic,
			Properties properties) {
		return new KafkaDynamicSource(
				producedDataType,
				keyDecodingFormat,
				new UpsertKafkaDynamicTableFactory.DecodingFormatWrapper(valueDecodingFormat),
				keyFields,
				valueFields,
				keyPrefix,
				Collections.singletonList(topic),
				null,
				properties,
				StartupMode.EARLIEST,
				Collections.emptyMap(),
				0,
				true);
	}

	private static KafkaDynamicSink createExpectedSink(
			DataType consumedDataType,
			EncodingFormat<SerializationSchema<RowData>> keyEncodingFormat,
			EncodingFormat<SerializationSchema<RowData>> valueEncodingFormat,
			int[] keyProjection,
			int[] valueProjection,
			String keyPrefix,
			String topic,
			Properties properties,
			Integer parallelism) {
		return new KafkaDynamicSink(
				consumedDataType,
				keyEncodingFormat,
				new UpsertKafkaDynamicTableFactory.EncodingFormatWrapper(valueEncodingFormat),
				keyProjection,
				valueProjection,
				keyPrefix,
				topic,
				properties,
				null,
				KafkaSinkSemantic.AT_LEAST_ONCE,
				true,
				parallelism);
	}
}
