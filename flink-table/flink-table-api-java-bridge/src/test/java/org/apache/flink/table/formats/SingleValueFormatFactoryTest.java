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

package org.apache.flink.table.formats;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.DynamicTableSource.DataStructureConverter;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link SingleValueFormatFactory}.
 */
public class SingleValueFormatFactoryTest extends TestLogger {
	private static final TableSchema SCHEMA = TableSchema.builder()
		.field("field1", DataTypes.STRING())
		.build();

	private static final RowType ROW_TYPE = (RowType) SCHEMA.toRowDataType().getLogicalType();

	private static final TypeInformation<RowData> rowDataInformation = new MockRowDataTypeInfo(ROW_TYPE);

	@Test
	public void testSeDeSchema() {
		final Map<String, String> tableOptions = getAllOptions();

		testSchemaDeserialization(tableOptions);
		testSchemaSerialization(tableOptions);
	}

	private void testSchemaDeserialization(Map<String, String> options) {
		final SingleValueRowDataDeserialization expectedDeser =
			new SingleValueRowDataDeserialization(
				ROW_TYPE,
				rowDataInformation);

		final DynamicTableSource actualSource = createTableSource(options);
		assert actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock;
		TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
			(TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

		DeserializationSchema<RowData> actualDeser = scanSourceMock.valueFormat
			.createRuntimeDecoder(
				new TestSourceProviderContext(),
				SCHEMA.toRowDataType());

		assertEquals(expectedDeser, actualDeser);
	}

	private void testSchemaSerialization(Map<String, String> options) {
		final SingleValueRowDataSerialization expectedSer =
			new SingleValueRowDataSerialization(
				ROW_TYPE);

		final DynamicTableSink actualSink = createTableSink(options);
		assert actualSink instanceof TestDynamicTableFactory.DynamicTableSinkMock;
		TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
			(TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

		SerializationSchema<RowData> actualSer = sinkMock.valueFormat
			.createRuntimeEncoder(
				new TestSinkProviderContext(),
				SCHEMA.toRowDataType());

		assertEquals(expectedSer, actualSer);
	}

	private static class TestSourceProviderContext implements ScanTableSource.ScanContext {

		@Override
		public TypeInformation<?> createTypeInformation(
			DataType producedDataType) {
			return rowDataInformation;
		}

		@Override
		public DataStructureConverter createDataStructureConverter(
			DataType producedDataType) {
			return null;
		}
	}

	private static class TestSinkProviderContext implements DynamicTableSink.Context{

		@Override
		public boolean isBounded() {
			return false;
		}

		@Override
		public TypeInformation<?> createTypeInformation(
			DataType consumedDataType) {
			return rowDataInformation;
		}

		@Override
		public DynamicTableSink.DataStructureConverter createDataStructureConverter(
			DataType consumedDataType) {
			return null;
		}
	}

	private static DynamicTableSource createTableSource(Map<String, String> options) {
		return FactoryUtil.createTableSource(
			null,
			ObjectIdentifier.of("default", "default", "t1"),
			new CatalogTableImpl(SCHEMA, options, "Mock scan table"),
			new Configuration(),
			SingleValueFormatFactoryTest.class.getClassLoader());
	}

	private static DynamicTableSink createTableSink(Map<String, String> options) {
		return FactoryUtil.createTableSink(
			null,
			ObjectIdentifier.of("default", "default", "t1"),
			new CatalogTableImpl(SCHEMA, options, "Mock sink table"),
			new Configuration(),
			SingleValueFormatFactoryTest.class.getClassLoader());
	}

	private Map<String, String> getAllOptions() {
		final Map<String, String> options = new HashMap<>();
		options.put("connector", TestDynamicTableFactory.IDENTIFIER);
		options.put("target", "MyTarget");
		options.put("buffer-size", "1000");

		options.put("format", SingleValueFormatFactory.IDENTIFIER);
		return options;
	}
}
