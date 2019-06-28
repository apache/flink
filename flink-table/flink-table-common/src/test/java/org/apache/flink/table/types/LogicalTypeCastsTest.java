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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TypeInformationAnyType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link LogicalTypeCasts}.
 */
@RunWith(Parameterized.class)
public class LogicalTypeCastsTest {

	@Parameters(name = "{index}: [From: {0}, To: {1}, Implicit: {2}, Explicit: {3}]")
	public static List<Object[]> testData() {
		return Arrays.asList(
			new Object[][]{
				{new SmallIntType(), new BigIntType(), true, true},

				// nullability does not match
				{new SmallIntType(false), new SmallIntType(), true, true},

				{new SmallIntType(), new SmallIntType(false), false, false},

				{
					new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR),
					new SmallIntType(),
					true,
					true
				},

				// not an interval with single field
				{
					new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH),
					new SmallIntType(),
					false,
					false
				},

				{new IntType(), new DecimalType(5, 5), true, true},

				// loss of precision
				{new FloatType(), new IntType(), false, true},

				{new VarCharType(Integer.MAX_VALUE), new FloatType(), false, true},

				{new FloatType(), new VarCharType(Integer.MAX_VALUE), false, true},

				{new DecimalType(3, 2), new VarCharType(Integer.MAX_VALUE), false, true},

				{
					new TypeInformationAnyType<>(Types.GENERIC(LogicalTypesTest.class)),
					new TypeInformationAnyType<>(Types.GENERIC(LogicalTypesTest.class)),
					true,
					true
				},

				{
					new TypeInformationAnyType<>(Types.GENERIC(LogicalTypesTest.class)),
					new TypeInformationAnyType<>(Types.GENERIC(Object.class)),
					false,
					false
				},

				{new NullType(), new IntType(), true, true},

				{new ArrayType(new IntType()), new ArrayType(new BigIntType()), true, true},

				{new ArrayType(new IntType()), new ArrayType(new VarCharType(Integer.MAX_VALUE)), false, true},

				{
					new RowType(
						Arrays.asList(
							new RowField("f1", new IntType()),
							new RowField("f2", new IntType())
						)
					),
					new RowType(
						Arrays.asList(
							new RowField("f1", new IntType()),
							new RowField("f2", new BigIntType())
						)
					),
					true,
					true
				},

				{
					new RowType(
						Arrays.asList(
							new RowField("f1", new IntType(), "description"),
							new RowField("f2", new IntType())
						)
					),
					new RowType(
						Arrays.asList(
							new RowField("f1", new IntType()),
							new RowField("f2", new BigIntType())
						)
					),
					true,
					true
				},

				{
					new RowType(
						Arrays.asList(
							new RowField("f1", new IntType()),
							new RowField("f2", new IntType())
						)
					),
					new RowType(
						Arrays.asList(
							new RowField("f1", new IntType()),
							new RowField("f2", new BooleanType())
						)
					),
					false,
					false
				},

				{
					new RowType(
						Arrays.asList(
							new RowField("f1", new IntType()),
							new RowField("f2", new IntType())
						)
					),
					new VarCharType(Integer.MAX_VALUE),
					false,
					false
				}
			}
		);
	}

	@Parameter
	public LogicalType sourceType;

	@Parameter(1)
	public LogicalType targetType;

	@Parameter(2)
	public boolean supportsImplicit;

	@Parameter(3)
	public boolean supportsExplicit;

	@Test
	public void testImplicitCasting() {
		assertThat(
			LogicalTypeCasts.supportsImplicitCast(sourceType, targetType),
			equalTo(supportsImplicit));
	}

	@Test
	public void testExplicitCasting() {
		assertThat(
			LogicalTypeCasts.supportsExplicitCast(sourceType, targetType),
			equalTo(supportsExplicit));
	}
}
