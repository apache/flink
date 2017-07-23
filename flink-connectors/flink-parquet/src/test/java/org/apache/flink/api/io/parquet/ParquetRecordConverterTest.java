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
import org.apache.flink.types.Row;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Date;

import static junit.framework.Assert.assertEquals;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for {@link ParquetRecordConverter}.
 */
public class ParquetRecordConverterTest {

	@Test
	public void testSupportedConverters() {
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

		TypeInformation<?>[] fieldTypes = {
				BasicTypeInfo.BOOLEAN_TYPE_INFO, // 0
				BasicTypeInfo.FLOAT_TYPE_INFO, // 1
				BasicTypeInfo.DOUBLE_TYPE_INFO, // 2
				BasicTypeInfo.INT_TYPE_INFO, // 3
				BasicTypeInfo.BYTE_TYPE_INFO, // 4
				BasicTypeInfo.SHORT_TYPE_INFO, // 5
				BasicTypeInfo.INT_TYPE_INFO, // 6
				BasicTypeInfo.DATE_TYPE_INFO, // 7
				BasicTypeInfo.BIG_DEC_TYPE_INFO, // 8
				BasicTypeInfo.LONG_TYPE_INFO, // 9
				BasicTypeInfo.LONG_TYPE_INFO, // 10
				BasicTypeInfo.BIG_DEC_TYPE_INFO, // 11
				PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, // 12
				BasicTypeInfo.STRING_TYPE_INFO, // 13
				BasicTypeInfo.STRING_TYPE_INFO, // 14
				BasicTypeInfo.STRING_TYPE_INFO, // 15
				PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO, // 16
				BasicTypeInfo.BIG_DEC_TYPE_INFO // 17
		};
		ParquetRecordConverter recordConverter = new ParquetRecordConverter(parquetSchema.asGroupType(), fieldTypes);

		((PrimitiveConverter) recordConverter.getConverter(0)).addBoolean(true); // boolean
		((PrimitiveConverter) recordConverter.getConverter(1)).addFloat(6.66f); // float
		((PrimitiveConverter) recordConverter.getConverter(2)).addDouble(8.88d); // double
		((PrimitiveConverter) recordConverter.getConverter(3)).addInt(5); // int
		((PrimitiveConverter) recordConverter.getConverter(4)).addInt(8); // byte
		((PrimitiveConverter) recordConverter.getConverter(5)).addInt(16); // short
		((PrimitiveConverter) recordConverter.getConverter(6)).addInt(32); // int
		((PrimitiveConverter) recordConverter.getConverter(7)).addInt(60); // date
		((PrimitiveConverter) recordConverter.getConverter(8)).addInt(33); // int -> big_dec
		((PrimitiveConverter) recordConverter.getConverter(9)).addLong(64L); // long
		((PrimitiveConverter) recordConverter.getConverter(10)).addLong(Long.MAX_VALUE); // long
		((PrimitiveConverter) recordConverter.getConverter(11)).addLong(66L); //long -> big_dec
		((PrimitiveConverter) recordConverter.getConverter(12)).addBinary(Binary.fromString("binary")); // byte[]
		((PrimitiveConverter) recordConverter.getConverter(13)).addBinary(Binary.fromString("utf8")); // string
		((PrimitiveConverter) recordConverter.getConverter(14))
				.addBinary(Binary.fromString(OriginalType.ENUM.toString())); // string
		((PrimitiveConverter) recordConverter.getConverter(15)).addBinary(Binary.fromString("{}")); // string
		((PrimitiveConverter) recordConverter.getConverter(16)).addBinary(Binary.fromString("bson")); // byte
		((PrimitiveConverter) recordConverter.getConverter(17)).addBinary(
				Binary.fromConstantByteArray(new BigDecimal(9.998).unscaledValue().toByteArray())); // binary -> big_dec

		Row row = recordConverter.getCurrentRecord();
		assertEquals(18, row.getArity());
		assertEquals(true, row.getField(0));
		assertEquals(6.66f, row.getField(1));
		assertEquals(8.88d, row.getField(2));
		assertEquals(5, row.getField(3));
		assertEquals((byte) 8, row.getField(4));
		assertEquals((short) 16, row.getField(5));
		assertEquals(32, row.getField(6));
		assertEquals(new Date(60), row.getField(7));
		assertEquals(new BigDecimal(33), row.getField(8));
		assertEquals(64L, row.getField(9));
		assertEquals(Long.MAX_VALUE, row.getField(10));
		assertEquals(new BigDecimal(66L), row.getField(11));
		assertArrayEquals("binary".getBytes(), (byte[]) row.getField(12));
		assertEquals("utf8", row.getField(13));
		assertEquals(OriginalType.ENUM, OriginalType.valueOf(row.getField(14).toString()));
		assertEquals("{}", row.getField(15));
		assertArrayEquals("bson".getBytes(), (byte[]) row.getField(16));
		assertEquals(new BigDecimal(9.998).unscaledValue(), ((BigDecimal) row.getField(17)).unscaledValue());
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConverters_BIG_INT() {
		MessageType parquetSchema = Types.buildMessage()
				.addFields(Types.required(INT64).named("int64"))
				.named("flink-parquet");
		TypeInformation<?>[] fieldTypes = {BasicTypeInfo.BIG_INT_TYPE_INFO};
		new ParquetRecordConverter(parquetSchema.asGroupType(), fieldTypes);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConverters_CHAR() {
		MessageType parquetSchema = Types.buildMessage()
				.addFields(Types.required(BINARY).length(1).as(OriginalType.UTF8).named("char"))
				.named("flink-parquet");
		TypeInformation<?>[] fieldTypes = {BasicTypeInfo.CHAR_TYPE_INFO};
		new ParquetRecordConverter(parquetSchema.asGroupType(), fieldTypes);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConverters_ARRAY() {
		MessageType parquetSchema = Types.buildMessage()
				.addFields(Types.repeated(BOOLEAN).named("repeated_boolean"))
				.named("flink-parquet");
		TypeInformation<?>[] fieldTypes = {PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO};
		new ParquetRecordConverter(parquetSchema.asGroupType(), fieldTypes);
	}

}
