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
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.YearMonthIntervalType.YearMonthResolution;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.utils.DataTypeFactoryMock;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;
import org.apache.flink.types.Row;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.time.DayOfWeek;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

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
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_LTZ;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.TIMESTAMP_WITH_TIME_ZONE;
import static org.apache.flink.table.api.DataTypes.TINYINT;
import static org.apache.flink.table.api.DataTypes.VARBINARY;
import static org.apache.flink.table.api.DataTypes.VARCHAR;
import static org.apache.flink.table.test.TableAssertions.assertThat;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DEFAULT_DAY_PRECISION;
import static org.apache.flink.table.types.logical.DayTimeIntervalType.DayTimeResolution.MINUTE_TO_SECOND;
import static org.apache.flink.table.types.utils.DataTypeFactoryMock.dummyRaw;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DataTypes} and {@link LogicalTypeDataTypeConverter}. */
class DataTypesTest {

    private static Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forDataType(CHAR(2))
                        .expectLogicalType(new CharType(2))
                        .expectConversionClass(String.class),
                TestSpec.forDataType(VARCHAR(2))
                        .expectLogicalType(new VarCharType(2))
                        .expectConversionClass(String.class),
                TestSpec.forDataType(STRING())
                        .expectLogicalType(VarCharType.STRING_TYPE)
                        .expectConversionClass(String.class),
                TestSpec.forDataType(BOOLEAN())
                        .expectLogicalType(new BooleanType())
                        .expectConversionClass(Boolean.class),
                TestSpec.forDataType(BINARY(42))
                        .expectLogicalType(new BinaryType(42))
                        .expectConversionClass(byte[].class),
                TestSpec.forDataType(VARBINARY(42))
                        .expectLogicalType(new VarBinaryType(42))
                        .expectConversionClass(byte[].class),
                TestSpec.forDataType(BYTES())
                        .expectLogicalType(new VarBinaryType(VarBinaryType.MAX_LENGTH))
                        .expectConversionClass(byte[].class),
                TestSpec.forDataType(DECIMAL(10, 10))
                        .expectLogicalType(new DecimalType(10, 10))
                        .expectConversionClass(BigDecimal.class),
                TestSpec.forDataType(TINYINT())
                        .expectLogicalType(new TinyIntType())
                        .expectConversionClass(Byte.class),
                TestSpec.forDataType(SMALLINT())
                        .expectLogicalType(new SmallIntType())
                        .expectConversionClass(Short.class),
                TestSpec.forDataType(INT())
                        .expectLogicalType(new IntType())
                        .expectConversionClass(Integer.class),
                TestSpec.forDataType(BIGINT())
                        .expectLogicalType(new BigIntType())
                        .expectConversionClass(Long.class),
                TestSpec.forDataType(FLOAT())
                        .expectLogicalType(new FloatType())
                        .expectConversionClass(Float.class),
                TestSpec.forDataType(DOUBLE())
                        .expectLogicalType(new DoubleType())
                        .expectConversionClass(Double.class),
                TestSpec.forDataType(DATE())
                        .expectLogicalType(new DateType())
                        .expectConversionClass(java.time.LocalDate.class),
                TestSpec.forDataType(TIME(3))
                        .expectLogicalType(new TimeType(3))
                        .expectConversionClass(java.time.LocalTime.class),
                TestSpec.forDataType(TIME())
                        .expectLogicalType(new TimeType(0))
                        .expectConversionClass(java.time.LocalTime.class),
                TestSpec.forDataType(TIMESTAMP(3))
                        .expectLogicalType(new TimestampType(3))
                        .expectConversionClass(java.time.LocalDateTime.class),
                TestSpec.forDataType(TIMESTAMP())
                        .expectLogicalType(new TimestampType(6))
                        .expectConversionClass(java.time.LocalDateTime.class),
                TestSpec.forDataType(TIMESTAMP_WITH_TIME_ZONE(3))
                        .expectLogicalType(new ZonedTimestampType(3))
                        .expectConversionClass(java.time.OffsetDateTime.class),
                TestSpec.forDataType(TIMESTAMP_WITH_TIME_ZONE())
                        .expectLogicalType(new ZonedTimestampType(6))
                        .expectConversionClass(java.time.OffsetDateTime.class),
                TestSpec.forDataType(TIMESTAMP_WITH_LOCAL_TIME_ZONE(3))
                        .expectLogicalType(new LocalZonedTimestampType(3))
                        .expectConversionClass(java.time.Instant.class),
                TestSpec.forDataType(TIMESTAMP_WITH_LOCAL_TIME_ZONE())
                        .expectLogicalType(new LocalZonedTimestampType(6))
                        .expectConversionClass(java.time.Instant.class),
                TestSpec.forDataType(TIMESTAMP_LTZ(3))
                        .expectLogicalType(new LocalZonedTimestampType(3))
                        .expectConversionClass(java.time.Instant.class),
                TestSpec.forDataType(TIMESTAMP_LTZ())
                        .expectLogicalType(new LocalZonedTimestampType(6))
                        .expectConversionClass(java.time.Instant.class),
                TestSpec.forDataType(INTERVAL(MINUTE(), SECOND(3)))
                        .expectLogicalType(
                                new DayTimeIntervalType(MINUTE_TO_SECOND, DEFAULT_DAY_PRECISION, 3))
                        .expectConversionClass(java.time.Duration.class),
                TestSpec.forDataType(INTERVAL(MONTH()))
                        .expectLogicalType(new YearMonthIntervalType(YearMonthResolution.MONTH))
                        .expectConversionClass(java.time.Period.class),
                TestSpec.forDataType(ARRAY(ARRAY(INT())))
                        .expectLogicalType(new ArrayType(new ArrayType(new IntType())))
                        .expectConversionClass(Integer[][].class),
                TestSpec.forDataType(ARRAY(ARRAY(INT().notNull())).bridgedTo(int[][].class))
                        .expectLogicalType(new ArrayType(new ArrayType(new IntType(false))))
                        .expectConversionClass(int[][].class)
                        .expectChildren(DataTypes.ARRAY(INT().notNull()).bridgedTo(int[].class)),
                TestSpec.forDataType(MULTISET(MULTISET(INT())))
                        .expectLogicalType(new MultisetType(new MultisetType(new IntType())))
                        .expectConversionClass(Map.class),
                TestSpec.forDataType(MAP(INT(), SMALLINT()))
                        .expectLogicalType(new MapType(new IntType(), new SmallIntType()))
                        .expectConversionClass(Map.class),
                TestSpec.forDataType(ROW(FIELD("field1", CHAR(2)), FIELD("field2", BOOLEAN())))
                        .expectLogicalType(
                                new RowType(
                                        Arrays.asList(
                                                new RowType.RowField("field1", new CharType(2)),
                                                new RowType.RowField("field2", new BooleanType()))))
                        .expectConversionClass(Row.class),
                TestSpec.forDataType(ROW(DataTypes.INT(), DataTypes.FLOAT()))
                        .expectResolvedDataType(
                                DataTypes.ROW(FIELD("f0", INT()), FIELD("f1", FLOAT()))),
                TestSpec.forDataType(NULL())
                        .expectLogicalType(new NullType())
                        .expectConversionClass(Object.class),
                TestSpec.forDataType(RAW(Void.class, VoidSerializer.INSTANCE))
                        .expectLogicalType(new RawType<>(Void.class, VoidSerializer.INSTANCE))
                        .expectConversionClass(Void.class),
                TestSpec.forUnresolvedDataType(RAW(Types.VOID))
                        .expectUnresolvedString("[RAW('java.lang.Void', '?')]")
                        .lookupReturns(dummyRaw(Void.class))
                        .expectResolvedDataType(dummyRaw(Void.class)),
                TestSpec.forUnresolvedDataType(DataTypes.of("INT"))
                        .expectUnresolvedString("[INT]")
                        .lookupReturns(INT())
                        .expectLogicalType(new IntType()),
                TestSpec.forUnresolvedDataType(DataTypes.of(Integer.class))
                        .expectUnresolvedString("['java.lang.Integer']")
                        .expectResolvedDataType(INT()),
                TestSpec.forUnresolvedDataType(DataTypes.of(java.sql.Timestamp.class).notNull())
                        .expectUnresolvedString("['java.sql.Timestamp']")
                        .expectResolvedDataType(
                                TIMESTAMP(9).notNull().bridgedTo(java.sql.Timestamp.class)),
                TestSpec.forUnresolvedDataType(
                                DataTypes.of(java.sql.Timestamp.class)
                                        .bridgedTo(java.time.LocalDateTime.class))
                        .expectUnresolvedString("['java.sql.Timestamp']")
                        .expectResolvedDataType(
                                TIMESTAMP(9).bridgedTo(java.time.LocalDateTime.class)),
                TestSpec.forUnresolvedDataType(MAP(DataTypes.of("INT"), DataTypes.of("STRING")))
                        .expectUnresolvedString("[MAP<[INT], [STRING]>]")
                        .expectResolvedDataType(MAP(DataTypes.INT(), DataTypes.STRING())),
                TestSpec.forUnresolvedDataType(MAP(DataTypes.of("INT"), STRING().notNull()))
                        .expectUnresolvedString("[MAP<[INT], STRING NOT NULL>]")
                        .expectResolvedDataType(MAP(INT(), STRING().notNull())),
                TestSpec.forUnresolvedDataType(MULTISET(DataTypes.of("STRING")))
                        .expectUnresolvedString("[MULTISET<[STRING]>]")
                        .expectResolvedDataType(MULTISET(DataTypes.STRING())),
                TestSpec.forUnresolvedDataType(ARRAY(DataTypes.of("STRING")))
                        .expectUnresolvedString("[ARRAY<[STRING]>]")
                        .expectResolvedDataType(ARRAY(DataTypes.STRING())),
                TestSpec.forUnresolvedDataType(
                                ARRAY(DataTypes.of("INT").notNull()).bridgedTo(int[].class))
                        .expectUnresolvedString("[ARRAY<[INT]>]")
                        .expectResolvedDataType(ARRAY(INT().notNull()).bridgedTo(int[].class)),
                TestSpec.forUnresolvedDataType(
                                ROW(
                                        FIELD("field1", DataTypes.of("CHAR(2)")),
                                        FIELD("field2", BOOLEAN())))
                        .expectUnresolvedString("[ROW<field1 [CHAR(2)], field2 BOOLEAN>]")
                        .expectResolvedDataType(
                                ROW(FIELD("field1", CHAR(2)), FIELD("field2", BOOLEAN()))),
                TestSpec.forUnresolvedDataType(ROW(DataTypes.of("CHAR(2)"), BOOLEAN()))
                        .expectResolvedDataType(ROW(FIELD("f0", CHAR(2)), FIELD("f1", BOOLEAN()))),
                TestSpec.forUnresolvedDataType(
                                ARRAY(
                                        ROW(
                                                FIELD("f0", DataTypes.of("ARRAY<INT>")),
                                                FIELD("f1", ARRAY(INT())))))
                        .expectUnresolvedString("[ARRAY<[ROW<f0 [ARRAY<INT>], f1 ARRAY<INT>>]>]")
                        .expectResolvedDataType(
                                ARRAY(ROW(FIELD("f0", ARRAY(INT())), FIELD("f1", ARRAY(INT()))))),
                TestSpec.forUnresolvedDataType(RAW(Object.class))
                        .expectUnresolvedString("[RAW('java.lang.Object', '?')]")
                        .lookupReturns(dummyRaw(Object.class))
                        .expectResolvedDataType(dummyRaw(Object.class)),
                TestSpec.forUnresolvedDataType(DataTypes.of(SimplePojo.class))
                        .expectResolvedDataType(
                                DataTypes.STRUCTURED(
                                        SimplePojo.class,
                                        DataTypes.FIELD("name", DataTypes.STRING()),
                                        DataTypes.FIELD(
                                                "count",
                                                DataTypes.INT().notNull().bridgedTo(int.class)))),
                TestSpec.forUnresolvedDataType(DataTypes.of(Types.ENUM(DayOfWeek.class)))
                        .expectUnresolvedString("['EnumTypeInfo<java.time.DayOfWeek>']")
                        .lookupReturns(dummyRaw(DayOfWeek.class))
                        .expectResolvedDataType(dummyRaw(DayOfWeek.class)));
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testLogicalType(TestSpec testSpec) {
        if (testSpec.expectedLogicalType != null) {
            final DataType dataType =
                    testSpec.typeFactory.createDataType(testSpec.abstractDataType);
            assertThat(dataType).hasLogicalType(testSpec.expectedLogicalType);
            assertThat(
                            DataTypes.of(testSpec.expectedLogicalType)
                                    .bridgedTo(dataType.getConversionClass()))
                    .isEqualTo(dataType);
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testConversionClass(TestSpec testSpec) {
        if (testSpec.expectedConversionClass != null) {
            assertThat(testSpec.typeFactory.createDataType(testSpec.abstractDataType))
                    .hasConversionClass(testSpec.expectedConversionClass);
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testChildren(TestSpec testSpec) {
        if (testSpec.expectedChildren != null) {
            assertThat(testSpec.typeFactory.createDataType(testSpec.abstractDataType))
                    .getChildren()
                    .isEqualTo(testSpec.expectedChildren);
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testUnresolvedString(TestSpec testSpec) {
        if (testSpec.expectedUnresolvedString != null) {
            assertThat(testSpec.abstractDataType.toString())
                    .isEqualTo(testSpec.expectedUnresolvedString);
        }
    }

    @ParameterizedTest(name = "{index}: {0}")
    @MethodSource("testData")
    void testResolvedDataType(TestSpec testSpec) {
        if (testSpec.expectedResolvedDataType != null) {
            assertThat(testSpec.typeFactory.createDataType(testSpec.abstractDataType))
                    .isEqualTo(testSpec.expectedResolvedDataType);
        }
    }

    // --------------------------------------------------------------------------------------------

    private static class TestSpec {

        private final DataTypeFactoryMock typeFactory = new DataTypeFactoryMock();

        private final AbstractDataType<?> abstractDataType;

        private @Nullable LogicalType expectedLogicalType;

        private @Nullable Class<?> expectedConversionClass;

        private @Nullable List<DataType> expectedChildren;

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

        TestSpec expectChildren(DataType... expectedChildren) {
            this.expectedChildren = Arrays.asList(expectedChildren);
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

    // --------------------------------------------------------------------------------------------
    // Helper classes
    // --------------------------------------------------------------------------------------------

    /** Simple POJO for testing. */
    public static class SimplePojo {
        public final String name;
        public final int count;

        public SimplePojo(String name, int count) {
            this.name = name;
            this.count = count;
        }
    }
}
