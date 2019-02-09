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

package org.apache.flink.formats.parquet.utils;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import org.apache.parquet.schema.MessageType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Simple test case for conversion between Parquet schema and Flink date types.
 */
public class ParquetSchemaConverterTest extends TestUtil {
	private final TypeInformation<Row> simplyRowType = Types.ROW_NAMED(new String[] {"foo", "bar", "arr"},
		BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO, BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO);

	private final TypeInformation<Row[]> nestedArray = Types.OBJECT_ARRAY(Types.ROW_NAMED(new String[] {"type", "value"},
		BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));

	@SuppressWarnings("unchecked")
	private final TypeInformation<Map<String, Row>> nestedMap = Types.MAP(BasicTypeInfo.STRING_TYPE_INFO,
		Types.ROW_NAMED(new String[] {"type", "value"},
			BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO));

	@SuppressWarnings("unchecked")
	private final TypeInformation<Row> nestedRowType = Types.ROW_NAMED(
		new String[] {"foo", "spamMap", "bar", "arr", "strArray", "nestedMap", "nestedArray"},
		BasicTypeInfo.LONG_TYPE_INFO,
		Types.MAP(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
		Types.ROW_NAMED(new String[] {"spam"}, new TypeInformation[] {BasicTypeInfo.LONG_TYPE_INFO}),
		BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO,
		BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO,
		nestedMap,
		nestedArray);

	@Test
	public void testSimpleSchemaConversion() {
		MessageType simpleType = new MessageType("simple", SIMPLE_STANDARD_TYPES);
		RowTypeInfo rowTypeInfo = (RowTypeInfo) ParquetSchemaConverter.fromParquetType(simpleType);
		assertEquals(simplyRowType, rowTypeInfo);
	}

	@Test
	public void testNestedSchemaConversion() {
		MessageType nestedTypes = new MessageType("nested", NESTED_TYPES);
		RowTypeInfo rowTypeInfo = (RowTypeInfo) ParquetSchemaConverter.fromParquetType(nestedTypes);
		assertEquals(nestedRowType, rowTypeInfo);
	}

	@Test
	public void testSimpleRowTypeConversion() {
		MessageType simpleSchema = ParquetSchemaConverter.toParquetType(simplyRowType, true);
		assertEquals(Arrays.asList(SIMPLE_STANDARD_TYPES), simpleSchema.getFields());
	}

	@Test
	public void testNestedRowTypeConversion() {
		MessageType nestedSchema = ParquetSchemaConverter.toParquetType(nestedRowType, true);
		assertEquals(Arrays.asList(NESTED_TYPES), nestedSchema.getFields());
	}
}
