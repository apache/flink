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

package org.apache.flink.formats.csv;

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
 * Tests for {@link CsvFormatFactory}.
 */
public class CsvFormatFactoryTest extends TestLogger {
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
		final CsvRowDataDeserializationSchema expectedDeser =
				new CsvRowDataDeserializationSchema.Builder(ROW_TYPE, new RowDataTypeInfo(ROW_TYPE))
						.setFieldDelimiter(';')
						.setQuoteCharacter('\'')
						.setAllowComments(true)
						.setIgnoreParseErrors(true)
						.setArrayElementDelimiter("|")
						.setEscapeCharacter('\\')
						.setNullLiteral("n/a")
						.build();

		final Map<String, String> options = getAllOptions();

		final DynamicTableSource actualSource = createTableSource(options);
		assert actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock;
		TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
				(TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

		DeserializationSchema<RowData> actualDeser = scanSourceMock.sourceValueFormat
				.createScanFormat(
						ScanRuntimeProviderContext.INSTANCE,
						SCHEMA.toRowDataType());

		assertEquals(expectedDeser, actualDeser);

		final CsvRowDataSerializationSchema expectedSer = new CsvRowDataSerializationSchema.Builder(ROW_TYPE)
			.setFieldDelimiter(';')
			.setLineDelimiter("\n")
			.setQuoteCharacter('\'')
			.setArrayElementDelimiter("|")
			.setEscapeCharacter('\\')
			.setNullLiteral("n/a")
			.build();

		final DynamicTableSink actualSink = createTableSink(options);
		assert actualSink instanceof TestDynamicTableFactory.DynamicTableSinkMock;
		TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
				(TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

		SerializationSchema<RowData> actualSer = sinkMock.sinkValueFormat
				.createSinkFormat(
						null,
						SCHEMA.toRowDataType());

		assertEquals(expectedSer, actualSer);
	}

	@Test
	public void testDisableQuoteCharacter() {
		final Map<String, String> options = getModifiedOptions(opts -> {
			opts.put("csv.disable-quote-character", "true");
			opts.remove("csv.quote-character");
		});

		final CsvRowDataSerializationSchema expectedSer = new CsvRowDataSerializationSchema.Builder(ROW_TYPE)
			.setFieldDelimiter(';')
			.setLineDelimiter("\n")
			.setArrayElementDelimiter("|")
			.setEscapeCharacter('\\')
			.setNullLiteral("n/a")
			.disableQuoteCharacter()
			.build();

		final DynamicTableSink actualSink = createTableSink(options);
		assert actualSink instanceof TestDynamicTableFactory.DynamicTableSinkMock;
		TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
				(TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

		SerializationSchema<RowData> actualSer = sinkMock.sinkValueFormat
				.createSinkFormat(
						null,
						SCHEMA.toRowDataType());

		assertEquals(expectedSer, actualSer);
	}

	@Test
	public void testDisableQuoteCharacterException() {
		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("Format cannot define a quote character and disabled quote character at the same time.")));

		final Map<String, String> options =
				getModifiedOptions(opts -> opts.put("csv.disable-quote-character", "true"));

		createTableSink(options);
	}

	@Test
	public void testInvalidCharacterOption() {
		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("Option 'csv.quote-character' must be "
				+ "a string with single character, but was: abc")));

		final Map<String, String> options =
				getModifiedOptions(opts -> opts.put("csv.quote-character", "abc"));

		createTableSink(options);
	}

	@Test
	public void testInvalidLineDelimiter() {
		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException("Invalid value for option 'csv.line-delimiter'. "
				+ "Supported values are [\\r, \\n, \\r\\n, \"\"], but was: abc")));

		final Map<String, String> options =
				getModifiedOptions(opts -> opts.put("csv.line-delimiter", "abc"));

		createTableSink(options);
	}

	@Test
	public void testInvalidIgnoreParseError() {
		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new IllegalArgumentException("Unrecognized option for boolean: abc. "
						+ "Expected either true or false(case insensitive)")));

		final Map<String, String> options =
				getModifiedOptions(opts -> opts.put("csv.ignore-parse-errors", "abc"));

		createTableSink(options);
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

		options.put("format", CsvFormatFactory.IDENTIFIER);
		options.put("csv.field-delimiter", ";");
		options.put("csv.line-delimiter", "\n");
		options.put("csv.quote-character", "'");
		options.put("csv.allow-comments", "true");
		options.put("csv.ignore-parse-errors", "true");
		options.put("csv.array-element-delimiter", "|");
		options.put("csv.escape-character", "\\");
		options.put("csv.null-literal", "n/a");
		return options;
	}

	private static DynamicTableSource createTableSource(Map<String, String> options) {
		return FactoryUtil.createTableSource(
				null,
				ObjectIdentifier.of("default", "default", "t1"),
				new CatalogTableImpl(SCHEMA, options, "mock source"),
				new Configuration(),
				CsvFormatFactoryTest.class.getClassLoader());
	}

	private static DynamicTableSink createTableSink(Map<String, String> options) {
		return FactoryUtil.createTableSink(
				null,
				ObjectIdentifier.of("default", "default", "t1"),
				new CatalogTableImpl(SCHEMA, options, "mock sink"),
				new Configuration(),
				CsvFormatFactoryTest.class.getClassLoader());
	}
}
