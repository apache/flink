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
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import org.apache.parquet.schema.MessageType;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Simple test case for conversion between Parquet schema and Flink date types.
 */
public class ParquetSchemaConverterTest extends TestUtil {
	private final RowTypeInfo simplyRowType = new RowTypeInfo(
		new TypeInformation[] {BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
		new String[] {"foo", "bar"}
	);

	private final ObjectArrayTypeInfo nestedArray = ObjectArrayTypeInfo.getInfoFor(
		new RowTypeInfo(
			new TypeInformation[] {BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
			new String[] {"type", "value"})
	);

	@SuppressWarnings("unchecked")
	private final MapTypeInfo nestedMap = new MapTypeInfo(
		BasicTypeInfo.STRING_TYPE_INFO,
		new RowTypeInfo(
			new TypeInformation[] {BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO},
			new String[] {"type", "value"})
	);

	@SuppressWarnings("unchecked")
	private final RowTypeInfo nestedRowType = new RowTypeInfo(
		new TypeInformation[] {
			BasicTypeInfo.LONG_TYPE_INFO,
			new MapTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO),
			new RowTypeInfo(
				new TypeInformation[] {BasicTypeInfo.LONG_TYPE_INFO},
				new String[] {"spam"}
			),
			BasicArrayTypeInfo.LONG_ARRAY_TYPE_INFO,
			BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO,
			nestedMap,
			nestedArray
		},
		new String[] {"foo", "spamMap", "bar", "arr", "strArray", "nestedMap", "nestedArray"}
	);

	@Test
	public void testSimpleSchemaConversion() {
		MessageType simpleType = new MessageType("simple", SIMPLE_TYPES);
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
		MessageType simpleSchema = ParquetSchemaConverter.toParquetType(simplyRowType);
		assertEquals(Arrays.asList(SIMPLE_TYPES), simpleSchema.getFields());
	}

	@Test
	public void testNestedRowTypeConversion() {
		MessageType nestedSchema = ParquetSchemaConverter.toParquetType(nestedRowType);
		assertEquals(Arrays.asList(NESTED_TYPES), nestedSchema.getFields());
	}
}
