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

package org.apache.flink.table.types;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link LegacyTypeInfoDataTypeConverter}.
 */
@RunWith(Parameterized.class)
public class LegacyTypeInfoDataTypeConverterTest {

	@Parameters(name = "[{index}] type info: {0} data type: {1}")
	public static List<Object[]> typeInfo() {
		return Arrays.asList(
			new Object[][]{
				{Types.STRING, DataTypes.STRING()},

				{Types.STRING, DataTypes.STRING().notNull()},

				{Types.BOOLEAN, DataTypes.BOOLEAN()},

				{Types.SQL_TIMESTAMP, DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class)},

				{
					Types.GENERIC(LegacyTypeInfoDataTypeConverterTest.class),
					new AtomicDataType(
						new LegacyTypeInformationType<>(
							LogicalTypeRoot.RAW,
							Types.GENERIC(LegacyTypeInfoDataTypeConverterTest.class)))
				},

				{
					Types.ROW_NAMED(new String[] {"field1", "field2"}, Types.INT, Types.LONG),
					DataTypes.ROW(
							FIELD("field1", DataTypes.INT()),
							FIELD("field2", DataTypes.BIGINT()))
				},

				{
					Types.MAP(Types.FLOAT, Types.ROW(Types.BYTE)),
					DataTypes.MAP(DataTypes.FLOAT(), DataTypes.ROW(FIELD("f0", DataTypes.TINYINT())))
				},

				{
					Types.PRIMITIVE_ARRAY(Types.FLOAT),
					DataTypes.ARRAY(DataTypes.FLOAT().notNull().bridgedTo(float.class))
						.bridgedTo(float[].class)
				},

				{
					Types.PRIMITIVE_ARRAY(Types.BYTE),
					DataTypes.BYTES()
				},

				{
					Types.OBJECT_ARRAY(Types.PRIMITIVE_ARRAY(Types.FLOAT)),
					DataTypes.ARRAY(
						DataTypes.ARRAY(DataTypes.FLOAT().notNull().bridgedTo(float.class))
							.bridgedTo(float[].class))
						.bridgedTo(float[][].class)
				},

				{
					BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO,
					new AtomicDataType(
						new LegacyTypeInformationType<>(
							LogicalTypeRoot.ARRAY,
							BasicArrayTypeInfo.STRING_ARRAY_TYPE_INFO))
				},

				{
					ObjectArrayTypeInfo.getInfoFor(Types.STRING),
					DataTypes.ARRAY(DataTypes.STRING())
						.bridgedTo(String[].class)
				},

				{
					Types.TUPLE(Types.SHORT, Types.DOUBLE, Types.FLOAT),
					new AtomicDataType(
						new LegacyTypeInformationType<>(
							LogicalTypeRoot.STRUCTURED_TYPE,
							Types.TUPLE(Types.SHORT, Types.DOUBLE, Types.FLOAT)))
				},

				{
					TimeIndicatorTypeInfo.ROWTIME_INDICATOR,
					new AtomicDataType(new TimestampType(true, TimestampKind.ROWTIME, 3))
						.bridgedTo(java.sql.Timestamp.class)
				}
			}
		);
	}

	@Parameter
	public TypeInformation<?> inputTypeInfo;

	@Parameter(1)
	public DataType dataType;

	@Test
	public void testTypeInfoToDataTypeConversion() {
		assertThat(LegacyTypeInfoDataTypeConverter.toDataType(inputTypeInfo), equalTo(dataType.nullable()));
	}

	@Test
	public void testDataTypeToTypeInfoConversion() {
		assertThat(LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(dataType), equalTo(inputTypeInfo));
	}
}
