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

import org.apache.flink.api.java.typeutils.RowTypeInfo;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

/**
 * Simple test case for conversion between Parquet schema and Flink date types.
 */
public class ParquetSchemaConverterTest {

	private final Type[] simpleStandardTypes = {
		org.apache.parquet.schema.Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
			.as(OriginalType.INT_64).named("foo"),
		org.apache.parquet.schema.Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
			.as(OriginalType.UTF8).named("bar"),
		org.apache.parquet.schema.Types.optionalGroup()
			.addField(org.apache.parquet.schema.Types.repeatedGroup().addField(
				org.apache.parquet.schema.Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
					.as(OriginalType.INT_64).named("element")).named("list")).as(OriginalType.LIST)
			.named("arr")};

	private final Type[] nestedTypes = {
		org.apache.parquet.schema.Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL)
			.as(OriginalType.INT_64).named("foo"),
		org.apache.parquet.schema.Types.optionalMap().value(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
			.as(OriginalType.UTF8)
			.named("spamMap"),
		org.apache.parquet.schema.Types.optionalGroup().addField(
			org.apache.parquet.schema.Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.OPTIONAL).as(OriginalType.INT_64)
				.named("spam")).named("bar"),
		org.apache.parquet.schema.Types.optionalGroup()
			.addField(org.apache.parquet.schema.Types.repeatedGroup().addField(
				org.apache.parquet.schema.Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED).as(OriginalType.INT_64)
					.named("element")).named("list")).as(OriginalType.LIST)
			.named("arr"),
		org.apache.parquet.schema.Types.optionalGroup()
			.addField(org.apache.parquet.schema.Types.repeatedGroup().addField(
				org.apache.parquet.schema.Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED).as(OriginalType.UTF8)
					.named("element")).named("list")).as(OriginalType.LIST)
			.named("strArray"),
		org.apache.parquet.schema.Types.optionalMap().value(org.apache.parquet.schema.Types.optionalGroup()
			.addField(org.apache.parquet.schema.Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
				.as(OriginalType.UTF8).named("type"))
			.addField(org.apache.parquet.schema.Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.OPTIONAL)
				.as(OriginalType.UTF8).named("value"))
			.named("value"))
			.named("nestedMap"),
		org.apache.parquet.schema.Types.optionalGroup().addField(org.apache.parquet.schema.Types.repeatedGroup()
			.addField(org.apache.parquet.schema.Types.requiredGroup()
				.addField(org.apache.parquet.schema.Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, Type.Repetition.REQUIRED)
					.as(OriginalType.UTF8).named("type"))
				.addField(org.apache.parquet.schema.Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, Type.Repetition.REQUIRED)
					.as(OriginalType.INT_64).named("value"))
				.named("element")).named("list")).as(OriginalType.LIST)
			.named("nestedArray")
	};

	@Test
	public void testSimpleSchemaConversion() {
		MessageType simpleType = new MessageType("simple", simpleStandardTypes);
		RowTypeInfo rowTypeInfo = (RowTypeInfo) ParquetSchemaConverter.fromParquetType(simpleType);
		assertEquals(TestUtil.SIMPLE_ROW_TYPE, rowTypeInfo);
	}

	@Test
	public void testNestedSchemaConversion() {
		MessageType nestedTypes = new MessageType("nested", this.nestedTypes);
		RowTypeInfo rowTypeInfo = (RowTypeInfo) ParquetSchemaConverter.fromParquetType(nestedTypes);
		assertEquals(TestUtil.NESTED_ROW_TYPE, rowTypeInfo);
	}

	@Test
	public void testSimpleRowTypeConversion() {
		MessageType simpleSchema = ParquetSchemaConverter.toParquetType(TestUtil.SIMPLE_ROW_TYPE, false);
		assertEquals(Arrays.asList(simpleStandardTypes), simpleSchema.getFields());
	}

	@Test
	public void testNestedRowTypeConversion() {
		MessageType nestedSchema = ParquetSchemaConverter.toParquetType(TestUtil.NESTED_ROW_TYPE, false);
		assertEquals(Arrays.asList(nestedTypes), nestedSchema.getFields());
	}
}
