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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.utils.ClassDataTypeConverter;
import org.apache.flink.types.Row;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ClassDataTypeConverter}.
 */
@RunWith(Parameterized.class)
public class ClassDataTypeConverterTest {

	@Parameterized.Parameters(name = "[{index}] class: {0} type: {1}")
	public static List<Object[]> testData() {
		return Arrays.asList(
			new Object[][]{

				{long.class, DataTypes.BIGINT().notNull().bridgedTo(long.class)},

				{byte[].class, DataTypes.BYTES().nullable().bridgedTo(byte[].class)},

				{Long.class, DataTypes.BIGINT().nullable().bridgedTo(Long.class)},

				{java.sql.Time.class, DataTypes.TIME(0).nullable().bridgedTo(java.sql.Time.class)},

				{
					java.time.Duration.class,
					DataTypes.INTERVAL(DataTypes.SECOND(9))
						.bridgedTo(java.time.Duration.class)
				},

				{
					java.time.Period.class,
					DataTypes.INTERVAL(DataTypes.YEAR(4), DataTypes.MONTH())
						.bridgedTo(java.time.Period.class)
				},

				{BigDecimal.class, null},

				{
					byte[][].class,
					DataTypes.ARRAY(DataTypes.BYTES().nullable().bridgedTo(byte[].class))
						.nullable()
						.bridgedTo(byte[][].class)
				},

				{
					Byte[].class,
					DataTypes.ARRAY(DataTypes.TINYINT().nullable().bridgedTo(Byte.class))
						.nullable()
						.bridgedTo(Byte[].class)
				},

				{
					Byte[][].class,
					DataTypes.ARRAY(
						DataTypes.ARRAY(DataTypes.TINYINT().nullable().bridgedTo(Byte.class))
							.nullable()
							.bridgedTo(Byte[].class))
						.nullable()
						.bridgedTo(Byte[][].class)
				},

				{
					Integer[].class,
					DataTypes.ARRAY(DataTypes.INT().nullable().bridgedTo(Integer.class))
						.nullable()
						.bridgedTo(Integer[].class)
				},

				{
					int[].class,
					DataTypes.ARRAY(DataTypes.INT().notNull().bridgedTo(int.class))
						.nullable()
						.bridgedTo(int[].class)
				},

				{
					TimeIntervalUnit.class,
					new AtomicDataType(new SymbolType<>(TimeIntervalUnit.class))
						.bridgedTo(TimeIntervalUnit.class)
				},

				{Row.class, null}
			}
		);
	}

	@Parameterized.Parameter
	public Class<?> clazz;

	@Parameterized.Parameter(1)
	public @Nullable DataType dataType;

	@Test
	public void testClassToDataTypeConversion() {
		assertEquals(Optional.ofNullable(dataType), ClassDataTypeConverter.extractDataType(clazz));
	}
}
