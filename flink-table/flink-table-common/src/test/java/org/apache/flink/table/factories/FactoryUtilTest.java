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

package org.apache.flink.table.factories;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogBaseTable;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.TestDynamicTableFactory.DynamicTableSinkMock;
import org.apache.flink.table.factories.TestDynamicTableFactory.DynamicTableSourceMock;
import org.apache.flink.table.factories.TestFormatFactory.ScanFormatMock;
import org.apache.flink.table.factories.TestFormatFactory.SinkFormatMock;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import static org.apache.flink.util.CoreMatchers.containsCause;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link FactoryUtil}.
 */
public class FactoryUtilTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testMissingConnector() {
		expectError("Table options do not contain an option key 'connector' for discovering a connector.");
		testError(options -> options.remove("connector"));
	}

	@Test
	public void testInvalidConnector() {
		expectError(
			"Could not find any factory for identifier 'FAIL' that implements '" +
				DynamicTableSourceFactory.class.getName() + "' in the classpath.\n\n" +
			"Available factory identifiers are:\n\n" +
			"test-connector");
		testError(options -> options.put("connector", "FAIL"));
	}

	@Test
	public void testMissingConnectorOption() {
		expectError(
			"One or more required options are missing.\n\n" +
			"Missing required options are:\n\n" +
			"target");
		testError(options -> options.remove("target"));
	}

	@Test
	public void testInvalidConnectorOption() {
		expectError("Invalid value for option 'buffer-size'.");
		testError(options -> options.put("buffer-size", "FAIL"));
	}

	@Test
	public void testMissingFormat() {
		expectError("Could not find required scan format 'value.format'.");
		testError(options -> options.remove("value.format"));
	}

	@Test
	public void testInvalidFormat() {
		expectError(
			"Could not find any factory for identifier 'FAIL' that implements '" +
				DeserializationFormatFactory.class.getName() + "' in the classpath.\n\n" +
			"Available factory identifiers are:\n\n" +
			"test-format");
		testError(options -> options.put("value.format", "FAIL"));
	}

	@Test
	public void testMissingFormatOption() {
		expectError(
			"Error creating scan format 'test-format' in option space 'key.test-format.'.");
		expectError(
			"One or more required options are missing.\n\n" +
			"Missing required options are:\n\n" +
			"delimiter");
		testError(options -> options.remove("key.test-format.delimiter"));
	}

	@Test
	public void testInvalidFormatOption() {
		expectError("Invalid value for option 'fail-on-missing'.");
		testError(options -> options.put("key.test-format.fail-on-missing", "FAIL"));
	}

	@Test
	public void testUnconsumedOption() {
		expectError(
			"Unsupported options found for connector 'test-connector'.\n\n" +
			"Unsupported options:\n\n" +
			"this-is-also-not-consumed\n" +
			"this-is-not-consumed\n\n" +
			"Supported options:\n\n" +
			"buffer-size\n" +
			"connector\n" +
			"format\n" +
			"key.format\n" +
			"key.test-format.delimiter\n" +
			"key.test-format.fail-on-missing\n" +
			"property-version\n" +
			"target\n" +
			"value.format\n" +
			"value.test-format.delimiter\n" +
			"value.test-format.fail-on-missing");
		testError(options -> {
			options.put("this-is-not-consumed", "42");
			options.put("this-is-also-not-consumed", "true");
		});
	}

	@Test
	public void testAllOptions() {
		final Map<String, String> options = createAllOptions();
		final DynamicTableSource actualSource = createTableSource(options);
		final DynamicTableSource expectedSource = new DynamicTableSourceMock(
			"MyTarget",
			new ScanFormatMock(",", false),
			new ScanFormatMock("|", true));
		assertEquals(expectedSource, actualSource);
		final DynamicTableSink actualSink = createTableSink(options);
		final DynamicTableSink expectedSink = new DynamicTableSinkMock(
			"MyTarget",
			1000L,
			new SinkFormatMock(","),
			new SinkFormatMock("|"));
		assertEquals(expectedSink, actualSink);
	}

	@Test
	public void testOptionalFormat() {
		final Map<String, String> options = createAllOptions();
		options.remove("key.format");
		options.remove("key.test-format.delimiter");
		final DynamicTableSource actualSource = createTableSource(options);
		final DynamicTableSource expectedSource = new DynamicTableSourceMock(
			"MyTarget",
			null,
			new ScanFormatMock("|", true));
		assertEquals(expectedSource, actualSource);
		final DynamicTableSink actualSink = createTableSink(options);
		final DynamicTableSink expectedSink = new DynamicTableSinkMock(
			"MyTarget",
			1000L,
			null,
			new SinkFormatMock("|"));
		assertEquals(expectedSink, actualSink);
	}

	@Test
	public void testAlternativeValueFormat() {
		final Map<String, String> options = createAllOptions();
		options.remove("value.format");
		options.remove("value.test-format.delimiter");
		options.remove("value.test-format.fail-on-missing");
		options.put("format", "test-format");
		options.put("test-format.delimiter", ";");
		options.put("test-format.fail-on-missing", "true");
		final DynamicTableSource actualSource = createTableSource(options);
		final DynamicTableSource expectedSource = new DynamicTableSourceMock(
			"MyTarget",
			new ScanFormatMock(",", false),
			new ScanFormatMock(";", true));
		assertEquals(expectedSource, actualSource);
		final DynamicTableSink actualSink = createTableSink(options);
		final DynamicTableSink expectedSink = new DynamicTableSinkMock(
			"MyTarget",
			1000L,
			new SinkFormatMock(","),
			new SinkFormatMock(";"));
		assertEquals(expectedSink, actualSink);
	}

	// --------------------------------------------------------------------------------------------

	private void expectError(String message) {
		thrown.expect(ValidationException.class);
		thrown.expect(containsCause(new ValidationException(message)));
	}

	private static void testError(Consumer<Map<String, String>> optionModifier) {
		final Map<String, String> options = createAllOptions();
		optionModifier.accept(options);
		createTableSource(options);
	}

	private static Map<String, String> createAllOptions() {
		final Map<String, String> options = new HashMap<>();
		// we use strings here to test realistic parsing
		options.put("property-version", "1");
		options.put("connector", TestDynamicTableFactory.IDENTIFIER);
		options.put("target", "MyTarget");
		options.put("buffer-size", "1000");
		options.put("key.format", "test-format");
		options.put("key.test-format.delimiter", ",");
		options.put("value.format", "test-format");
		options.put("value.test-format.delimiter", "|");
		options.put("value.test-format.fail-on-missing", "true");
		return options;
	}

	private static DynamicTableSource createTableSource(Map<String, String> options) {
		return FactoryUtil.createTableSource(
			null,
			ObjectIdentifier.of("cat", "db", "table"),
			new CatalogTableMock(options),
			new Configuration(),
			FactoryUtilTest.class.getClassLoader());
	}

	private static DynamicTableSink createTableSink(Map<String, String> options) {
		return FactoryUtil.createTableSink(
			null,
			ObjectIdentifier.of("cat", "db", "table"),
			new CatalogTableMock(options),
			new Configuration(),
			FactoryUtilTest.class.getClassLoader());
	}

	private static class CatalogTableMock implements CatalogTable {

		final Map<String, String> options;

		CatalogTableMock(Map<String, String> options) {
			this.options = options;
		}

		@Override
		public boolean isPartitioned() {
			return false;
		}

		@Override
		public List<String> getPartitionKeys() {
			return null;
		}

		@Override
		public CatalogTable copy(Map<String, String> options) {
			return null;
		}

		@Override
		public Map<String, String> toProperties() {
			return null;
		}

		@Override
		public Map<String, String> getProperties() {
			return options;
		}

		@Override
		public TableSchema getSchema() {
			return null;
		}

		@Override
		public String getComment() {
			return null;
		}

		@Override
		public CatalogBaseTable copy() {
			return null;
		}

		@Override
		public Optional<String> getDescription() {
			return Optional.empty();
		}

		@Override
		public Optional<String> getDetailedDescription() {
			return Optional.empty();
		}
	}
}
