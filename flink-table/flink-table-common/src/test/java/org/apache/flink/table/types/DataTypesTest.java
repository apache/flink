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
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.table.api.DataTypes;
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
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;
import org.apache.flink.types.Row;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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
import static org.apache.flink.table.api.DataTypes.RAW;
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

	@Parameters(name = "{index}: {0}")
	public static List<TestSpec> testData() {
		return Arrays.asList(
			TestSpec
				.forDataType(CHAR(2))
				.expectLogicalType(new CharType(2))
				.expectConversionClass(String.class),

			TestSpec
				.forDataType(VARCHAR(2))
				.expectLogicalType(new VarCharType(2))
				.expectConversionClass(String.class),

			TestSpec
				.forDataType(STRING())
				.expectLogicalType(new VarCharType(VarCharType.MAX_LENGTH))
				.expectConversionClass(String.class),

			TestSpec
				.forDataType(BOOLEAN())
				.expectLogicalType(new BooleanType())
				.expectConversionClass(Boolean.class),

			TestSpec
				.forDataType(BINARY(42))
				.expectLogicalType(new BinaryType(42))
				.expectConversionClass(byte[].class),

			TestSpec
				.forDataType(VARBINARY(42))
				.expectLogicalType(new VarBinaryType(42))
				.expectConversionClass(byte[].class),

			TestSpec
				.forDataType(BYTES())
				.expectLogicalType(new VarBinaryType(VarBinaryType.MAX_LENGTH))
				.expectConversionClass(byte[].class),

			TestSpec
				.forDataType(DECIMAL(10, 10))
				.expectLogicalType(new DecimalType(10, 10))
				.expectConversionClass(BigDecimal.class),

			TestSpec
				.forDataType(TINYINT())
				.expectLogicalType(new TinyIntType())
				.expectConversionClass(Byte.class),

			TestSpec
				.forDataType(SMALLINT())
				.expectLogicalType(new SmallIntType())
				.expectConversionClass(Short.class),

			TestSpec
				.forDataType(INT())
				.expectLogicalType(new IntType())
				.expectConversionClass(Integer.class),

			TestSpec
				.forDataType(BIGINT())
				.expectLogicalType(new BigIntType())
				.expectConversionClass(Long.class),

			TestSpec
				.forDataType(FLOAT())
				.expectLogicalType(new FloatType())
				.expectConversionClass(Float.class),

			TestSpec
				.forDataType(DOUBLE())
				.expectLogicalType(new DoubleType())
				.expectConversionClass(Double.class),

			TestSpec
				.forDataType(DATE())
				.expectLogicalType(new DateType())
				.expectConversionClass(java.time.LocalDate.class),

			TestSpec
				.forDataType(TIME(3))
				.expectLogicalType(new TimeType(3))
				.expectConversionClass(java.time.LocalTime.class),

			TestSpec
				.forDataType(TIME())
				.expectLogicalType(new TimeType(0))
				.expectConversionClass(java.time.LocalTime.class),

			TestSpec
				.forDataType(TIMESTAMP(3))
				.expectLogicalType(new TimestampType(3))
				.expectConversionClass(java.time.LocalDateTime.class),

			TestSpec
				.forDataType(TIMESTAMP())
				.expectLogicalType(new TimestampType(6))
				.expectConversionClass(java.time.LocalDateTime.class),

			TestSpec
				.forDataType(TIMESTAMP_WITH_TIME_ZONE(3))
				.expectLogicalType(new ZonedTimestampType(3))
				.expectConversionClass(java.time.OffsetDateTime.class),

			TestSpec
				.forDataType(TIMESTAMP_WITH_TIME_ZONE())
				.expectLogicalType(new ZonedTimestampType(6))
				.expectConversionClass(java.time.OffsetDateTime.class),

			TestSpec
				.forDataType(TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
				.expectLogicalType(new LocalZonedTimestampType(3))
				.expectConversionClass(java.time.Instant.class),

			TestSpec
				.forDataType(TIMESTAMP_WITH_LOCAL_TIME_ZONE())
				.expectLogicalType(new LocalZonedTimestampType(6))
				.expectConversionClass(java.time.Instant.class),

			TestSpec
				.forDataType(INTERVAL(MINUTE(), SECOND(3)))
				.expectLogicalType(new DayTimeIntervalType(MINUTE_TO_SECOND, DEFAULT_DAY_PRECISION, 3))
				.expectConversionClass(java.time.Duration.class),

			TestSpec
				.forDataType(INTERVAL(MONTH()))
				.expectLogicalType(new YearMonthIntervalType(YearMonthResolution.MONTH))
				.expectConversionClass(java.time.Period.class),

			TestSpec
				.forDataType(ARRAY(ARRAY(INT())))
				.expectLogicalType(new ArrayType(new ArrayType(new IntType())))
				.expectConversionClass(Integer[][].class),

			TestSpec
				.forDataType(MULTISET(MULTISET(INT())))
				.expectLogicalType(new MultisetType(new MultisetType(new IntType())))
				.expectConversionClass(Map.class),

			TestSpec
				.forDataType(MAP(INT(), SMALLINT()))
				.expectLogicalType(new MapType(new IntType(), new SmallIntType()))
				.expectConversionClass(Map.class),

			TestSpec
				.forDataType(ROW(FIELD("field1", CHAR(2)), FIELD("field2", BOOLEAN())))
				.expectLogicalType(new RowType(
						Arrays.asList(
							new RowType.RowField("field1", new CharType(2)),
							new RowType.RowField("field2", new BooleanType()))))
				.expectConversionClass(Row.class),

			TestSpec
				.forDataType(NULL())
				.expectLogicalType(new NullType())
				.expectConversionClass(Object.class),

			TestSpec
				.forDataType(RAW(Types.GENERIC(DataTypesTest.class)))
				.expectLogicalType(new TypeInformationRawType<>(Types.GENERIC(DataTypesTest.class)))
				.expectConversionClass(DataTypesTest.class),

			TestSpec
				.forDataType(RAW(Void.class, VoidSerializer.INSTANCE))
				.expectLogicalType(new RawType<>(Void.class, VoidSerializer.INSTANCE))
				.expectConversionClass(Void.class),

			TestSpec
				.forUnresolvedDataType(DataTypes.of("INT"))
				.expectUnresolvedString("[INT]")
				.lookupReturns(INT())
				.expectLogicalType(new IntType()),

			TestSpec
				.forUnresolvedDataType(DataTypes.of(Integer.class))
				.expectUnresolvedString("['java.lang.Integer']")
				.expectResolvedDataType(INT()),

			TestSpec
				.forUnresolvedDataType(DataTypes.of(java.sql.Timestamp.class).notNull())
				.expectUnresolvedString("['java.sql.Timestamp']")
				.expectResolvedDataType(TIMESTAMP(9).notNull().bridgedTo(java.sql.Timestamp.class)),

			TestSpec
				.forUnresolvedDataType(
					DataTypes.of(java.sql.Timestamp.class).bridgedTo(java.time.LocalDateTime.class))
				.expectUnresolvedString("['java.sql.Timestamp']")
				.expectResolvedDataType(TIMESTAMP(9).bridgedTo(java.time.LocalDateTime.class)),

			TestSpec
				.forUnresolvedDataType(MAP(DataTypes.of("INT"), DataTypes.of("STRING")))
				.expectUnresolvedString("[MAP<[INT], [STRING]>]")
				.expectResolvedDataType(MAP(DataTypes.INT(), DataTypes.STRING())),

			TestSpec
				.forUnresolvedDataType(MAP(DataTypes.of("INT"), STRING().notNull()))
				.expectUnresolvedString("[MAP<[INT], STRING NOT NULL>]")
				.expectResolvedDataType(MAP(INT(), STRING().notNull())),

			TestSpec
				.forUnresolvedDataType(MULTISET(DataTypes.of("STRING")))
				.expectUnresolvedString("[MULTISET<[STRING]>]")
				.expectResolvedDataType(MULTISET(DataTypes.STRING())),

			TestSpec
				.forUnresolvedDataType(ARRAY(DataTypes.of("STRING")))
				.expectUnresolvedString("[ARRAY<[STRING]>]")
				.expectResolvedDataType(ARRAY(DataTypes.STRING())),

			TestSpec
				.forUnresolvedDataType(
					ARRAY(DataTypes.of("INT").notNull()).bridgedTo(int[].class))
				.expectUnresolvedString("[ARRAY<[INT]>]")
				.expectResolvedDataType(ARRAY(INT().notNull()).bridgedTo(int[].class)),

			TestSpec
				.forUnresolvedDataType(
					ROW(FIELD("field1", DataTypes.of("CHAR(2)")), FIELD("field2", BOOLEAN())))
				.expectUnresolvedString("[ROW<field1 [CHAR(2)], field2 BOOLEAN>]")
				.expectResolvedDataType(ROW(FIELD("field1", CHAR(2)), FIELD("field2", BOOLEAN()))),

			TestSpec
				.forUnresolvedDataType(
					ARRAY(
						ROW(
							FIELD("f0", DataTypes.of("ARRAY<INT>")),
							FIELD("f1", ARRAY(INT())))))
				.expectUnresolvedString("[ARRAY<[ROW<f0 [ARRAY<INT>], f1 ARRAY<INT>>]>]")
				.expectResolvedDataType(
					ARRAY(
						ROW(
							FIELD("f0", ARRAY(INT())),
							FIELD("f1", ARRAY(INT()))))
				),

			TestSpec
				.forUnresolvedDataType(RAW(Object.class))
				.expectUnresolvedString("[RAW('java.lang.Object', '?')]")
				.lookupReturns(DataTypes.RAW(new GenericTypeInfo<>(Object.class)))
				.expectResolvedDataType(DataTypes.RAW(new GenericTypeInfo<>(Object.class)))
		);
	}

	@Parameter
	public TestSpec testSpec;

	@Test
	public void testLogicalType() {
		if (testSpec.expectedLogicalType != null) {
			final DataType dataType = testSpec.typeFactory.createDataType(testSpec.abstractDataType);

			assertThat(dataType, hasLogicalType(testSpec.expectedLogicalType));

			assertThat(toDataType(testSpec.expectedLogicalType), equalTo(dataType));

			assertThat(toLogicalType(dataType), equalTo(testSpec.expectedLogicalType));
		}
	}

	@Test
	public void testConversionClass() {
		if (testSpec.expectedConversionClass != null) {
			final DataType dataType = testSpec.typeFactory.createDataType(testSpec.abstractDataType);
			assertThat(dataType, hasConversionClass(testSpec.expectedConversionClass));
		}
	}

	@Test
	public void testUnresolvedString() {
		if (testSpec.expectedUnresolvedString != null) {
			assertThat(testSpec.abstractDataType.toString(), equalTo(testSpec.expectedUnresolvedString));
		}
	}

	@Test
	public void testResolvedDataType() {
		if (testSpec.expectedResolvedDataType != null) {
			final DataType dataType = testSpec.typeFactory.createDataType(testSpec.abstractDataType);
			assertThat(dataType, equalTo(testSpec.expectedResolvedDataType));
		}
	}

	// --------------------------------------------------------------------------------------------

	private static class TestSpec {

		private final DataTypeFactoryMock typeFactory = new DataTypeFactoryMock();

		private final AbstractDataType<?> abstractDataType;

		private @Nullable LogicalType expectedLogicalType;

		private @Nullable Class<?> expectedConversionClass;

		private @Nullable String expectedUnresolvedString;

		private @Nullable DataType expectedResolvedDataType;

		private TestSpec(AbstractDataType<?> abstractDataType) {
			this.abstractDataType = abstractDataType;
		}

		static TestSpec forDataType(DataType dataType) {
			return new TestSpec(dataType);
		}

		static TestSpec forUnresolvedDataType(UnresolvedDataType unresolvedDataType) {
			return new TestSpec(unresolvedDataType);
		}

		TestSpec expectLogicalType(LogicalType expectedLogicalType) {
			this.expectedLogicalType = expectedLogicalType;
			return this;
		}

		TestSpec expectConversionClass(Class<?> expectedConversionClass) {
			this.expectedConversionClass = expectedConversionClass;
			return this;
		}

		TestSpec expectUnresolvedString(String expectedUnresolvedString) {
			this.expectedUnresolvedString = expectedUnresolvedString;
			return this;
		}

		TestSpec expectResolvedDataType(DataType expectedResolvedDataType) {
			this.expectedResolvedDataType = expectedResolvedDataType;
			return this;
		}

		TestSpec lookupReturns(DataType dataType) {
			this.typeFactory.dataType = Optional.of(dataType);
			return this;
		}

		@Override
		public String toString() {
			return abstractDataType.toString();
		}
	}
}
