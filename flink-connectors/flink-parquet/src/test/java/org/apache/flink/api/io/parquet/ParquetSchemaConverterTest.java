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

package org.apache.flink.api.io.parquet;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ParquetSchemaConverter}.
 */
public class ParquetSchemaConverterTest {

	@Test
	public void testSupportedConversionWithRequired() throws Exception {
		testSupportedConversion(true);
	}

	@Test
	public void testSupportedConversionWithOptional() throws Exception {
		testSupportedConversion(false);
	}

	private void testSupportedConversion(boolean required) {
		Type.Repetition repetition = required ? Type.Repetition.REQUIRED : Type.Repetition.OPTIONAL;

		MessageType parquetSchema = Types.buildMessage().addFields(
				Types.primitive(BOOLEAN, repetition).named("boolean"),
				Types.primitive(FLOAT, repetition).named("float"),
				Types.primitive(DOUBLE, repetition).named("double"),
				Types.primitive(INT32, repetition).as(null).named("int_original_null"),
				Types.primitive(INT32, repetition).as(OriginalType.INT_8).named("int32_original_int8"),
				Types.primitive(INT32, repetition).as(OriginalType.INT_16).named("int32_original_int16"),
				Types.primitive(INT32, repetition).as(OriginalType.INT_32).named("int32_original_int32"),
				Types.primitive(INT32, repetition).as(OriginalType.DATE).named("int32_original_date"),
				Types.primitive(INT32, repetition).as(OriginalType.DECIMAL).precision(9).scale(2)
						.named("int32_original_decimal"),
				Types.primitive(INT64, repetition).as(null).named("int64_original_null"),
				Types.primitive(INT64, repetition).as(OriginalType.INT_64).named("int64_original_int64"),
				Types.primitive(INT64, repetition).as(OriginalType.DECIMAL).precision(18).scale(2)
						.named("int64_original_decimal"),
				Types.primitive(BINARY, repetition).as(null).named("binary_original_null"),
				Types.primitive(BINARY, repetition).as(OriginalType.UTF8).named("binary_original_uft8"),
				Types.primitive(BINARY, repetition).as(OriginalType.ENUM).named("binary_original_enum"),
				Types.primitive(BINARY, repetition).as(OriginalType.JSON).named("binary_original_json"),
				Types.primitive(BINARY, repetition).as(OriginalType.BSON).named("binary_original_bson"),
				Types.primitive(BINARY, repetition).as(OriginalType.DECIMAL).precision(9).scale(2)
						.named("binary_original_decimal"))
				.named("flink-parquet");

		ParquetSchemaConverter converter = new ParquetSchemaConverter();
		Map<String, TypeInformation<?>> fieldName2TypeMap = converter.convertToTypeInformation(parquetSchema);

		Map<String, TypeInformation<?>> expectedFieldName2TypeMap = new HashMap<String, TypeInformation<?>>() {
			{
				put("boolean", BasicTypeInfo.BOOLEAN_TYPE_INFO);
				put("float", BasicTypeInfo.FLOAT_TYPE_INFO);
				put("double", BasicTypeInfo.DOUBLE_TYPE_INFO);
				put("int_original_null", BasicTypeInfo.INT_TYPE_INFO);
				put("int32_original_int8", BasicTypeInfo.BYTE_TYPE_INFO);
				put("int32_original_int16", BasicTypeInfo.SHORT_TYPE_INFO);
				put("int32_original_int32", BasicTypeInfo.INT_TYPE_INFO);
				put("int32_original_date", BasicTypeInfo.DATE_TYPE_INFO);
				put("int32_original_decimal", BasicTypeInfo.BIG_DEC_TYPE_INFO);
				put("int64_original_null", BasicTypeInfo.LONG_TYPE_INFO);
				put("int64_original_int64", BasicTypeInfo.LONG_TYPE_INFO);
				put("int64_original_decimal", BasicTypeInfo.BIG_DEC_TYPE_INFO);
				put("binary_original_null", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
				put("binary_original_uft8", BasicTypeInfo.STRING_TYPE_INFO);
				put("binary_original_enum", BasicTypeInfo.STRING_TYPE_INFO);
				put("binary_original_json", BasicTypeInfo.STRING_TYPE_INFO);
				put("binary_original_bson", PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO);
				put("binary_original_decimal", BasicTypeInfo.BIG_DEC_TYPE_INFO);
			}
		};

		assertEquals(expectedFieldName2TypeMap.size(), fieldName2TypeMap.size());
		for (Map.Entry<String, TypeInformation<?>> entry : expectedFieldName2TypeMap.entrySet()) {
			String expectedFieldName = entry.getKey();
			TypeInformation<?> expectedFieldType = entry.getValue();
			TypeInformation<?> actualTypeInfo = fieldName2TypeMap.get(expectedFieldName);
			assertEquals(expectedFieldType, actualTypeInfo);
		}
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_REPEATED() {
		MessageType parquetSchema = Types.buildMessage()
				.addFields(Types.repeated(BOOLEAN).named("repeated_boolean"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToTypeInformation(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_GroupType() {
		MessageType parquetSchema = Types.buildMessage()
				.addFields(Types.requiredGroup().addFields(
						Types.required(BOOLEAN).named("boolean"),
						Types.optional(INT32).named("int32")
				).named("group_boolean"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToTypeInformation(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_INT96() {
		MessageType parquetSchema = Types.buildMessage()
				.addField(Types.required(INT96).named("int96"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToTypeInformation(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_FIXED_LEN_BYTE_ARRAY() {
		MessageType parquetSchema = Types.buildMessage()
				.addField(Types.required(FIXED_LEN_BYTE_ARRAY).length(10).named("fixed_len_byte_array"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToTypeInformation(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_UINT8() {
		MessageType parquetSchema = Types.buildMessage()
				.addField(Types.required(INT32).as(OriginalType.UINT_8).named("int32_uint_8"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToTypeInformation(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_UINT16() {
		MessageType parquetSchema = Types.buildMessage()
				.addField(Types.required(INT32).as(OriginalType.UINT_16).named("int32_uint_16"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToTypeInformation(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_UINT32() {
		MessageType parquetSchema = Types.buildMessage()
				.addField(Types.required(INT32).as(OriginalType.UINT_32).named("int32_uint_32"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToTypeInformation(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_UINT64() {
		MessageType parquetSchema = Types.buildMessage()
				.addField(Types.required(INT64).as(OriginalType.UINT_64).named("int64_uint_64"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToTypeInformation(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_TIME_MILLIS() {
		MessageType parquetSchema = Types.buildMessage()
				.addField(Types.required(INT32).as(OriginalType.TIME_MILLIS).named("int32_time_millis"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToTypeInformation(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_TIMESTAMP_MILLIS() {
		MessageType parquetSchema = Types.buildMessage()
				.addField(Types.required(INT64).as(OriginalType.TIMESTAMP_MILLIS).named("int64_timestamp_millis"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToTypeInformation(parquetSchema);
	}
}
