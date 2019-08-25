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
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.AnyType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
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
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.TypeInformationAnyType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;
import org.apache.flink.types.Row;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.DataTypes.ANY;
import static org.apache.flink.table.api.DataTypes.ARRAY;
import static org.apache.flink.table.api.DataTypes.BIGINT;
import static org.apache.flink.table.api.DataTypes.BINARY;
import static org.apache.flink.table.api.DataTypes.BOOLEAN;
import static org.apache.flink.table.api.DataTypes.BYTES;
import static org.apache.flink.table.api.DataTypes.CHAR;
import static org.apache.flink.table.api.DataTypes.DATE;
import static org.apache.flink.table.api.DataTypes.DECIMAL;
import static org.apache.flink.table.api.DataTypes.DOUBLE;
import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.FLOAT;
import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.INTERVAL;
import static org.apache.flink.table.api.DataTypes.MAP;
import static org.apache.flink.table.api.DataTypes.MINUTE;
import static org.apache.flink.table.api.DataTypes.MONTH;
import static org.apache.flink.table.api.DataTypes.MULTISET;
import static org.apache.flink.table.api.DataTypes.NULL;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.SECOND;
import static org.apache.flink.table.api.DataTypes.SMALLINT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.api.DataTypes.TIME;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.DataTypes.VARBINARY;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.flink.table.types.TypeTestingUtils.hasConversionClass;
import static org.apache.flink.table.types.TypeTestingUtils.hasLogicalType;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DEFAULT_DAY_PRECISION;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.MINUTE_TO_SECOND;
import static org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter.toDataType;
import static org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter.toLogicalType;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link DataTypes} and {@link LogicalTypeDataTypeConverter}.
 */
@RunWith(Parameterized.class)
public class DataTypesTest {

	@Parameters(name = "{index}: {0}=[Logical: {1}, Class: {2}]")
	public static List<Object[]> dataTypes() {
		return Arrays.asList(
			new Object[][]{
				{CHAR(2), new CharType(2), String.class},

				{VARCHAR(2), new VarCharType(2), String.class},

				{STRING(), new VarCharType(VarCharType.MAX_LENGTH), String.class},

				{BOOLEAN(), new BooleanType(), Boolean.class},

				{BINARY(42), new BinaryType(42), byte[].class},

				{VARBINARY(42), new VarBinaryType(42), byte[].class},

				{BYTES(), new VarBinaryType(VarBinaryType.MAX_LENGTH), byte[].class},

				{DECIMAL(10, 10), new DecimalType(10, 10), BigDecimal.class},

				{TINYINT(), new TinyIntType(), Byte.class},

				{SMALLINT(), new SmallIntType(), Short.class},

				{INT(), new IntType(), Integer.class},

				{BIGINT(), new BigIntType(), Long.class},

				{FLOAT(), new FloatType(), Float.class},

				{DOUBLE(), new DoubleType(), Double.class},

				{DATE(), new DateType(), java.time.LocalDate.class},

				{TIME(3), new TimeType(3), java.time.LocalTime.class},

				{TIME(), new TimeType(0), java.time.LocalTime.class},

				{TIMESTAMP(3), new TimestampType(3), java.time.LocalDateTime.class},

				{TIMESTAMP(), new TimestampType(6), java.time.LocalDateTime.class},

				{TIMESTAMP_WITH_TIME_ZONE(3),
					new ZonedTimestampType(3),
					java.time.OffsetDateTime.class},

				{TIMESTAMP_WITH_TIME_ZONE(),
					new ZonedTimestampType(6),
					java.time.OffsetDateTime.class},

				{TIMESTAMP_WITH_LOCAL_TIME_ZONE(3),
					new LocalZonedTimestampType(3),
					java.time.Instant.class},

				{TIMESTAMP_WITH_LOCAL_TIME_ZONE(),
					new LocalZonedTimestampType(6),
					java.time.Instant.class},

				{INTERVAL(MINUTE(), SECOND(3)),
					new DayTimeIntervalType(MINUTE_TO_SECOND, DEFAULT_DAY_PRECISION, 3),
					java.time.Duration.class},

				{INTERVAL(MONTH()),
					new YearMonthIntervalType(YearMonthResolution.MONTH),
					java.time.Period.class},

				{ARRAY(ARRAY(INT())),
					new ArrayType(new ArrayType(new IntType())),
					Integer[][].class},

				{MULTISET(MULTISET(INT())),
					new MultisetType(new MultisetType(new IntType())),
					Map.class},

				{MAP(INT(), SMALLINT()),
					new MapType(new IntType(), new SmallIntType()),
					Map.class},

				{ROW(FIELD("field1", CHAR(2)), FIELD("field2", BOOLEAN())),
					new RowType(
						Arrays.asList(
							new RowType.RowField("field1", new CharType(2)),
							new RowType.RowField("field2", new BooleanType()))),
					Row.class},

				{NULL(), new NullType(), Object.class},

				{ANY(Types.GENERIC(DataTypesTest.class)),
					new TypeInformationAnyType<>(Types.GENERIC(DataTypesTest.class)),
					DataTypesTest.class},

				{ANY(Void.class, VoidSerializer.INSTANCE),
					new AnyType<>(Void.class, VoidSerializer.INSTANCE),
					Void.class}
			}
		);
	}

	@Parameter
	public DataType dataType;

	@Parameter(1)
	public LogicalType expectedLogicalType;

	@Parameter(2)
	public Class<?> expectedConversionClass;

	@Test
	public void testLogicalType() {
		assertThat(dataType, hasLogicalType(expectedLogicalType));
	}

	@Test
	public void testConversionClass() {
		assertThat(dataType, hasConversionClass(expectedConversionClass));
	}

	@Test
	public void testLogicalTypeToDataTypeConversion() {
		assertThat(toDataType(expectedLogicalType), equalTo(dataType));
	}

	@Test
	public void testDataTypeToLogicalTypeConversion() {
		assertThat(toLogicalType(dataType), equalTo(expectedLogicalType));
	}
}
