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

package org.apache.flink.formats.json.maxwell;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link MaxwellJsonFormatFactory}.
 */
public class MaxwellJsonFormatFactoryTest extends TestLogger {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static final TableSchema SCHEMA = TableSchema.builder()
		.field("a", DataTypes.STRING())
		.field("b", DataTypes.INT())
		.field("c", DataTypes.BOOLEAN())
		.build();

	private static final RowType ROW_TYPE = (RowType) SCHEMA.toRowDataType().getLogicalType();

	@Test
	public void testSeDeSchema() {
		final MaxwellJsonDeserializationSchema expectedDeser = new MaxwellJsonDeserializationSchema(
			ROW_TYPE,
			InternalTypeInfo.of(ROW_TYPE),
			true,
			TimestampFormat.ISO_8601);

		final MaxwellJsonSerializationSchema expectedSer = new MaxwellJsonSerializationSchema(
			ROW_TYPE,
			TimestampFormat.ISO_8601,
			JsonOptions.MapNullKeyMode.LITERAL,
			"null");

		final Map<String, String> options = getAllOptions();

		final DynamicTableSource actualSource = createTableSource(options);
		assert actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock;
		TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
			(TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

		DeserializationSchema<RowData> actualDeser = scanSourceMock.valueFormat
			.createRuntimeDecoder(
				ScanRuntimeProviderContext.INSTANCE,
				SCHEMA.toRowDataType());

		assertEquals(expectedDeser, actualDeser);

		final DynamicTableSink actualSink = createTableSink(options);
		assert actualSink instanceof TestDynamicTableFactory.DynamicTableSinkMock;
		TestDynamicTableFactory.DynamicTableSinkMock sinkMock = (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

		SerializationSchema<RowData> actualSer = sinkMock.valueFormat
			.createRuntimeEncoder(
				new SinkRuntimeProviderContext(false),
				SCHEMA.toRowDataType());

		assertEquals(expectedSer, actualSer);
	}

	@Test
	public void testInvalidIgnoreParseError() {
		thrown.expect(containsCause(new IllegalArgumentException(
			"Unrecognized option for boolean: abc. Expected either true or false(case insensitive)")));

		final Map<String, String> options =
			getModifiedOptions(opts -> opts.put("maxwell-json.ignore-parse-errors", "abc"));

		createTableSource(options);
	}

	@Test
	public void testInvalidOptionForTimestampFormat() {
		final Map<String, String> tableOptions =
			getModifiedOptions(opts -> opts.put("maxwell-json.timestamp-format.standard", "test"));

		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("Unsupported value 'test' for timestamp-format.standard. Supported values are [SQL, ISO-8601].")));
		createTableSource(tableOptions);
	}

	@Test
	public void testInvalidOptionForMapNullKeyMode() {
		final Map<String, String> tableOptions =
			getModifiedOptions(opts -> opts.put("maxwell-json.map-null-key.mode", "invalid"));

		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("Unsupported value 'invalid' for option map-null-key.mode. Supported values are [LITERAL, FAIL, DROP].")));
		createTableSink(tableOptions);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Returns the full options modified by the given consumer {@code optionModifier}.
	 *
	 * @param optionModifier Consumer to modify the options
	 */
	private Map<String, String> getModifiedOptions(Consumer<Map<String, String>> optionModifier) {
		Map<String, String> options = getAllOptions();
		optionModifier.accept(options);
		return options;
	}

	private Map<String, String> getAllOptions() {
		final Map<String, String> options = new HashMap<>();
		options.put("connector", TestDynamicTableFactory.IDENTIFIER);
		options.put("target", "MyTarget");
		options.put("buffer-size", "1000");

		options.put("format", "maxwell-json");
		options.put("maxwell-json.ignore-parse-errors", "true");
		options.put("maxwell-json.timestamp-format.standard", "ISO-8601");
		options.put("maxwell-json.map-null-key.mode", "LITERAL");
		options.put("maxwell-json.map-null-key.literal", "null");
		return options;
	}

	private static DynamicTableSource createTableSource(Map<String, String> options) {
		return FactoryUtil.createTableSource(
			null,
			ObjectIdentifier.of("default", "default", "t1"),
			new CatalogTableImpl(SCHEMA, options, "mock source"),
			new Configuration(),
			MaxwellJsonFormatFactoryTest.class.getClassLoader(),
			false);
	}

	private static DynamicTableSink createTableSink(Map<String, String> options) {
		return FactoryUtil.createTableSink(
			null,
			ObjectIdentifier.of("default", "default", "t1"),
			new CatalogTableImpl(SCHEMA, options, "mock sink"),
			new Configuration(),
			MaxwellJsonFormatFactoryTest.class.getClassLoader(),
			false);
	}
}
