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

package org.apache.flink.table.format.single;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
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

	@Test
	public void testSeDeSchema() {
		final Map<String, String> tableOptions = getAllOptions();

		final SingleValueRowSerializer expectedSerializer =
			new SingleValueRowSerializer(
				ROW_TYPE,
				new RowDataTypeInfo(ROW_TYPE));

		final DynamicTableSource actualSource = createTableSource(tableOptions);
		assert actualSource instanceof TestDynamicTableFactory.DynamicTableSourceMock;
		TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
			(TestDynamicTableFactory.DynamicTableSourceMock) actualSource;

		DeserializationSchema<RowData> actualSerializer = scanSourceMock.valueFormat
			.createRuntimeDecoder(
				ScanRuntimeProviderContext.INSTANCE,
				SCHEMA.toRowDataType());

		assertEquals(expectedSerializer, actualSerializer);
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
