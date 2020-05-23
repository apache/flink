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

package org.apache.flink.formats.json;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
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
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.util.CoreMatchers.containsCause;
import static org.junit.Assert.assertEquals;

/**
 * Tests for the {@link JsonFormatFactory}.
 */
public class JsonFormatFactoryTest extends TestLogger {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static final TableSchema SCHEMA = TableSchema.builder()
			.field("field1", DataTypes.BOOLEAN())
			.field("field2", DataTypes.INT())
			.build();

	private static final RowType ROW_TYPE = (RowType) SCHEMA.toRowDataType().getLogicalType();

	@Test
	public void testSeDeSchema() {
		final Map<String, String> tableOptions = getAllOptions();

		testSchemaSerializationSchema(tableOptions);

		testSchemaDeserializationSchema(tableOptions);
	}

	@Test
	public void testFailOnMissingField() {
		final Map<String, String> tableOptions = getModifyOptions(
				options -> options.put("json.fail-on-missing-field", "true"));

		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("fail-on-missing-field and ignore-parse-errors shouldn't both be true.")));
		testSchemaDeserializationSchema(tableOptions);
	}

	@Test
	public void testInvalidOptionForIgnoreParseErrors() {
		final Map<String, String> tableOptions = getModifyOptions(
				options -> options.put("json.ignore-parse-errors", "abc"));

		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new IllegalArgumentException("Unrecognized option for boolean: abc. Expected either true or false(case insensitive)")));
		testSchemaDeserializationSchema(tableOptions);
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private void testSchemaDeserializationSchema(Map<String, String> options) {
		final JsonRowDataDeserializationSchema expectedDeser =
				new JsonRowDataDeserializationSchema(
						ROW_TYPE,
						new RowDataTypeInfo(ROW_TYPE),
						false,
						true);

		final DynamicTableSource actualSource = createTableSource(options);
		assert actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock;
		TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
				(TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

		DeserializationSchema<RowData> actualDeser = scanSourceMock.sourceValueFormat
				.createScanFormat(
						ScanRuntimeProviderContext.INSTANCE,
						SCHEMA.toRowDataType());

		assertEquals(expectedDeser, actualDeser);
	}

	private void testSchemaSerializationSchema(Map<String, String> options) {
		final JsonRowDataSerializationSchema expectedSer = new JsonRowDataSerializationSchema(ROW_TYPE);

		final DynamicTableSink actualSink = createTableSink(options);
		assert actualSink instanceof TestDynamicTableFactory.DynamicTableSinkMock;
		TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
				(TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

		SerializationSchema<RowData> actualSer = sinkMock.sinkValueFormat
				.createSinkFormat(
						new SinkRuntimeProviderContext(false),
						SCHEMA.toRowDataType());

		assertEquals(expectedSer, actualSer);
	}

	/**
	 * Returns the full options modified by the given consumer {@code optionModifier}.
	 *
	 * @param optionModifier Consumer to modify the options
	 */
	private Map<String, String> getModifyOptions(Consumer<Map<String, String>> optionModifier) {
		Map<String, String> options = getAllOptions();
		optionModifier.accept(options);
		return options;
	}

	private Map<String, String> getAllOptions() {
		final Map<String, String> options = new HashMap<>();
		options.put("connector", TestDynamicTableFactory.IDENTIFIER);
		options.put("target", "MyTarget");
		options.put("buffer-size", "1000");

		options.put("format", JsonFormatFactory.IDENTIFIER);
		options.put("json.fail-on-missing-field", "false");
		options.put("json.ignore-parse-errors", "true");
		return options;
	}

	private static DynamicTableSource createTableSource(Map<String, String> options) {
		return FactoryUtil.createTableSource(
				null,
				ObjectIdentifier.of("default", "default", "t1"),
				new CatalogTableImpl(SCHEMA, options, "Mock scan table"),
				new Configuration(),
				JsonFormatFactoryTest.class.getClassLoader());
	}

	private static DynamicTableSink createTableSink(Map<String, String> options) {
		return FactoryUtil.createTableSink(
				null,
				ObjectIdentifier.of("default", "default", "t1"),
				new CatalogTableImpl(SCHEMA, options, "Mock sink table"),
				new Configuration(),
				JsonFormatFactoryTest.class.getClassLoader());
	}
}
