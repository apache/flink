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

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link LogicalTypeMerging#findCommonType(List)}.
 */
@RunWith(Parameterized.class)
public class LogicalCommonTypeTest {

	@Parameters(name = "{index}: [Types: {0}, To: {1}]")
	public static List<Object[]> testData() {
		return Arrays.asList(
			new Object[][]{

				// simple types
				{
					Arrays.asList(new IntType(), new IntType()),
					new IntType()
				},

				// incompatible types
				{
					Arrays.asList(new IntType(), new ArrayType(new IntType())),
					null
				},

				// incompatible types
				{
					Arrays.asList(new IntType(), new VarCharType(23)),
					null
				},

				// incompatible types
				{
					Arrays.asList(new BinaryType(), new VarCharType(23)),
					null
				},

				// NOT NULL types
				{
					Arrays.asList(new IntType(false), new IntType(false)),
					new IntType(false)
				},

				// NOT NULL with different types
				{
					Arrays.asList(new IntType(true), new BigIntType(false)),
					new BigIntType()
				},

				// NULL only
				{
					Arrays.asList(new NullType(), new NullType()),
					null
				},

				// NULL with other types
				{
					Arrays.asList(new NullType(), new IntType(), new IntType()),
					new IntType()
				},

				// ARRAY types with same element type
				{
					Arrays.asList(new ArrayType(new IntType()), new ArrayType(new IntType())),
					new ArrayType(new IntType())
				},

				// ARRAY types with different element types
				{
					Arrays.asList(new ArrayType(new BigIntType()), new ArrayType(new IntType())),
					new ArrayType(new BigIntType())
				},

				// MULTISET types with different element type
				{
					Arrays.asList(new MultisetType(new BigIntType()), new MultisetType(new IntType())),
					new MultisetType(new BigIntType())
				},

				// MAP types with different element type
				{
					Arrays.asList(
						new MapType(new BigIntType(), new DoubleType()),
						new MapType(new IntType(), new DoubleType())),
					new MapType(new BigIntType(), new DoubleType())
				},

				// ROW type with different element types
				{
					Arrays.asList(
						RowType.of(new IntType(), new IntType(), new BigIntType()),
						RowType.of(new BigIntType(), new IntType(), new IntType())),
					RowType.of(new BigIntType(), new IntType(), new BigIntType())
				},

				// CHAR types of same length
				{
					Arrays.asList(new CharType(2), new CharType(2)),
					new CharType(2)
				},

				// CHAR types of length 0
				{
					Arrays.asList(CharType.ofEmptyLiteral(), CharType.ofEmptyLiteral()),
					CharType.ofEmptyLiteral()
				},

				// CHAR types of different length
				{
					Arrays.asList(new CharType(2), new CharType(4)),
					new VarCharType(4)
				},

				// VARCHAR types of different length
				{
					Arrays.asList(new VarCharType(2), new VarCharType(VarCharType.MAX_LENGTH)),
					new VarCharType(VarCharType.MAX_LENGTH)
				},

				// mixed VARCHAR and CHAR types
				{
					Arrays.asList(new VarCharType(2), new CharType(5)),
					new VarCharType(5)
				},

				// more mixed VARCHAR and CHAR types
				{
					Arrays.asList(new CharType(5), new VarCharType(2), new VarCharType(7)),
					new VarCharType(7)
				},

				// BINARY types of different length
				{
					Arrays.asList(new BinaryType(2), new BinaryType(4)),
					new VarBinaryType(4)
				},

				// mixed BINARY and VARBINARY types
				{
					Arrays.asList(new BinaryType(5), new VarBinaryType(2), new VarBinaryType(7)),
					new VarBinaryType(7)
				},

				// two APPROXIMATE_NUMERIC types
				{
					Arrays.asList(new DoubleType(), new FloatType()),
					new DoubleType()
				},

				// one APPROXIMATE_NUMERIC and one DECIMAL type
				{
					Arrays.asList(new DoubleType(), new DecimalType(2, 2)),
					new DoubleType()
				},

				// one APPROXIMATE_NUMERIC and one EXACT_NUMERIC type
				{
					Arrays.asList(new IntType(), new FloatType()),
					new FloatType()
				},

				// two APPROXIMATE_NUMERIC and one DECIMAL type
				{
					Arrays.asList(new DecimalType(2, 2), new DoubleType(), new FloatType()),
					new DoubleType()
				},

				// DECIMAL precision and scale merging
				{
					Arrays.asList(new DecimalType(2, 2), new DecimalType(5, 2), new DecimalType(7, 5)),
					new DecimalType(8, 5)
				},

				// DECIMAL precision and scale merging with other EXACT_NUMERIC types
				{
					Arrays.asList(new DecimalType(2, 2), new IntType(), new BigIntType()),
					new DecimalType(21, 2)
				},

				// unsupported time merging
				{
					Arrays.asList(new DateType(), new DateType(), new TimeType()),
					null
				},

				// time precision merging
				{
					Arrays.asList(new TimeType(3), new TimeType(5), new TimeType(2)),
					new TimeType(5)
				},

				// timestamp precision merging
				{
					Arrays.asList(new TimestampType(3), new TimestampType(5), new TimestampType(2)),
					new TimestampType(5)
				},

				// timestamp merging
				{
					Arrays.asList(new TimestampType(3), new ZonedTimestampType(5), new LocalZonedTimestampType(2)),
					new ZonedTimestampType(5)
				},

				// timestamp merging
				{
					Arrays.asList(new TimestampType(3), new LocalZonedTimestampType(2)),
					new LocalZonedTimestampType(3)
				},

				// day-time interval + DATETIME
				{
					Arrays.asList(new DayTimeIntervalType(DayTimeResolution.DAY), new DateType()),
					new DateType()
				},

				// year-month interval + DATETIME
				{
					Arrays.asList(new YearMonthIntervalType(YearMonthResolution.MONTH), new DateType()),
					new DateType()
				},

				// DATETIME + INTERVAL
				{
					Arrays.asList(new TimeType(), new DayTimeIntervalType(DayTimeResolution.MINUTE)),
					new TimeType()
				},

				// DATETIME + INTERVAL
				{
					Arrays.asList(new DateType(), new DayTimeIntervalType(DayTimeResolution.DAY)),
					new DateType()
				},

				// EXACT_NUMERIC + DATE
				{
					Arrays.asList(new IntType(), new DateType()),
					new DateType()
				},

				// TIME + EXACT_NUMERIC
				{
					Arrays.asList(new TimeType(), new DecimalType()),
					null
				},

				// TIMESTAMP + EXACT_NUMERIC
				{
					Arrays.asList(new TimestampType(), new DecimalType()),
					new TimestampType()
				},

				// day-time intervals
				{
					Arrays.asList(
						new DayTimeIntervalType(DayTimeResolution.DAY_TO_MINUTE),
						new DayTimeIntervalType(DayTimeResolution.SECOND)),
					new DayTimeIntervalType(DayTimeResolution.DAY_TO_SECOND)
				},

				// day-time intervals
				{
					Arrays.asList(
						new DayTimeIntervalType(DayTimeResolution.HOUR),
						new DayTimeIntervalType(
							DayTimeResolution.SECOND,
							DayTimeIntervalType.DEFAULT_DAY_PRECISION,
							0)),
					new DayTimeIntervalType(
						DayTimeResolution.HOUR_TO_SECOND,
						DayTimeIntervalType.DEFAULT_DAY_PRECISION,
						6)
				},

				// year-month intervals
				{
					Arrays.asList(
						new YearMonthIntervalType(YearMonthResolution.MONTH),
						new YearMonthIntervalType(YearMonthResolution.YEAR)),
					new YearMonthIntervalType(YearMonthResolution.YEAR_TO_MONTH)
				}
			}
		);
	}

	@Parameter
	public List<LogicalType> types;

	@Parameter(1)
	public LogicalType commonType;

	@Test
	public void testCommonType() {
		assertThat(
			LogicalTypeMerging.findCommonType(types),
			equalTo(Optional.ofNullable(commonType)));
	}
}
