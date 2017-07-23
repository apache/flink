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
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link ParquetSchemaConverter}.
 */
public class ParquetSchemaConverterTest {

	@Test
	public void testSupportedConversionWithRequired() throws Exception {
		MessageType parquetSchema = Types.buildMessage().addFields(
				Types.required(BOOLEAN).named("boolean"),
				Types.required(FLOAT).named("float"),
				Types.required(DOUBLE).named("double"),
				Types.required(INT32).as(null).named("int_original_null"),
				Types.required(INT32).as(OriginalType.INT_8).named("int32_original_int8"),
				Types.required(INT32).as(OriginalType.INT_16).named("int32_original_int16"),
				Types.required(INT32).as(OriginalType.INT_32).named("int32_original_int32"),
				Types.required(INT32).as(OriginalType.DATE).named("int32_original_date"),
				Types.required(INT32).as(OriginalType.DECIMAL).precision(9).scale(2).named("int32_original_decimal"),
				Types.required(INT64).as(null).named("int64_original_null"),
				Types.required(INT64).as(OriginalType.INT_64).named("int64_original_int64"),
				Types.required(INT64).as(OriginalType.DECIMAL).precision(9).scale(2).named("int64_original_decimal"),
				Types.required(BINARY).as(null).named("binary_original_null"),
				Types.required(BINARY).as(OriginalType.UTF8).named("binary_original_uft8"),
				Types.required(BINARY).as(OriginalType.ENUM).named("binary_original_enum"),
				Types.required(BINARY).as(OriginalType.JSON).named("binary_original_json"),
				Types.required(BINARY).as(OriginalType.BSON).named("binary_original_bson"),
				Types.required(BINARY).as(OriginalType.DECIMAL).precision(9).scale(2).named("binary_original_decimal"))
				.named("flink-parquet");

		ParquetSchemaConverter converter = new ParquetSchemaConverter();
		Tuple2<String[], TypeInformation<?>[]> fieldNamesAndTypes = converter.convertToTypeInformation(parquetSchema);
		String[] expectedFieldNames = {
				"boolean",
				"float",
				"double",
				"int_original_null",
				"int32_original_int8",
				"int32_original_int16",
				"int32_original_int32",
				"int32_original_date",
				"int32_original_decimal",
				"int64_original_null",
				"int64_original_int64",
				"int64_original_decimal",
				"binary_original_null",
				"binary_original_uft8",
				"binary_original_enum",
				"binary_original_json",
				"binary_original_bson",
				"binary_original_decimal"};

		TypeInformation<?>[] expectedFieldTypes = {
				BasicTypeInfo.BOOLEAN_TYPE_INFO,
				BasicTypeInfo.FLOAT_TYPE_INFO,
				BasicTypeInfo.DOUBLE_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.BYTE_TYPE_INFO,
				BasicTypeInfo.SHORT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.DATE_TYPE_INFO,
				BasicTypeInfo.BIG_DEC_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.BIG_DEC_TYPE_INFO,
				PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
				BasicTypeInfo.BIG_DEC_TYPE_INFO
		};

		assertArrayEquals(expectedFieldNames, fieldNamesAndTypes.f0);
		assertArrayEquals(expectedFieldTypes, fieldNamesAndTypes.f1);
	}

	@Test
	public void testSupportedConversionWithOptional() throws Exception {
		MessageType parquetSchema = Types.buildMessage().addFields(
				Types.optional(BOOLEAN).named("boolean"),
				Types.optional(FLOAT).named("float"),
				Types.optional(DOUBLE).named("double"),
				Types.optional(INT32).as(null).named("int_original_null"),
				Types.optional(INT32).as(OriginalType.INT_8).named("int32_original_int8"),
				Types.optional(INT32).as(OriginalType.INT_16).named("int32_original_int16"),
				Types.optional(INT32).as(OriginalType.INT_32).named("int32_original_int32"),
				Types.optional(INT32).as(OriginalType.DATE).named("int32_original_date"),
				Types.optional(INT32).as(OriginalType.DECIMAL).precision(9).scale(2).named("int32_original_decimal"),
				Types.optional(INT64).as(null).named("int64_original_null"),
				Types.optional(INT64).as(OriginalType.INT_64).named("int64_original_int64"),
				Types.optional(INT64).as(OriginalType.DECIMAL).precision(9).scale(2).named("int64_original_decimal"),
				Types.optional(BINARY).as(null).named("binary_original_null"),
				Types.optional(BINARY).as(OriginalType.UTF8).named("binary_original_uft8"),
				Types.optional(BINARY).as(OriginalType.ENUM).named("binary_original_enum"),
				Types.optional(BINARY).as(OriginalType.JSON).named("binary_original_json"),
				Types.optional(BINARY).as(OriginalType.BSON).named("binary_original_bson"),
				Types.optional(BINARY).as(OriginalType.DECIMAL).precision(9).scale(2).named("binary_original_decimal"))
				.named("flink-parquet");

		ParquetSchemaConverter converter = new ParquetSchemaConverter();
		Tuple2<String[], TypeInformation<?>[]> fieldNamesAndTypes = converter.convertToTypeInformation(parquetSchema);
		String[] expectedFieldNames = {
				"boolean",
				"float",
				"double",
				"int_original_null",
				"int32_original_int8",
				"int32_original_int16",
				"int32_original_int32",
				"int32_original_date",
				"int32_original_decimal",
				"int64_original_null",
				"int64_original_int64",
				"int64_original_decimal",
				"binary_original_null",
				"binary_original_uft8",
				"binary_original_enum",
				"binary_original_json",
				"binary_original_bson",
				"binary_original_decimal"};

		TypeInformation<?>[] expectedFieldTypes = {
				BasicTypeInfo.BOOLEAN_TYPE_INFO,
				BasicTypeInfo.FLOAT_TYPE_INFO,
				BasicTypeInfo.DOUBLE_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.BYTE_TYPE_INFO,
				BasicTypeInfo.SHORT_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.DATE_TYPE_INFO,
				BasicTypeInfo.BIG_DEC_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.LONG_TYPE_INFO,
				BasicTypeInfo.BIG_DEC_TYPE_INFO,
				PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
				BasicTypeInfo.BIG_DEC_TYPE_INFO
		};

		assertArrayEquals(expectedFieldNames, fieldNamesAndTypes.f0);
		assertArrayEquals(expectedFieldTypes, fieldNamesAndTypes.f1);
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
