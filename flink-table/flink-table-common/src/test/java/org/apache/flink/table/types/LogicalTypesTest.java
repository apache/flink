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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.expressions.TimeIntervalUnit;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
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
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.UnresolvedUserDefinedType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.types.Row;

import org.assertj.core.api.ThrowingConsumer;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.apache.flink.table.test.TableAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for subclasses of {@link org.apache.flink.table.types.logical.LogicalType}. */
public class LogicalTypesTest {

    @Test
    void testCharType() {
        assertThat(new CharType(33))
                .satisfies(
                        baseAssertions(
                                "CHAR(33)",
                                "CHAR(33)",
                                new Class[] {String.class, byte[].class},
                                new Class[] {String.class, byte[].class},
                                new LogicalType[] {},
                                new CharType(Integer.MAX_VALUE)));
    }

    @Test
    void testVarCharType() {
        assertThat(new VarCharType(33))
                .satisfies(
                        baseAssertions(
                                "VARCHAR(33)",
                                "VARCHAR(33)",
                                new Class[] {String.class, byte[].class},
                                new Class[] {String.class, byte[].class},
                                new LogicalType[] {},
                                new VarCharType(12)));
    }

    @Test
    void testVarCharTypeWithMaximumLength() {
        assertThat(new VarCharType(Integer.MAX_VALUE))
                .satisfies(
                        baseAssertions(
                                "VARCHAR(2147483647)",
                                "STRING",
                                new Class[] {String.class, byte[].class},
                                new Class[] {String.class, byte[].class},
                                new LogicalType[] {},
                                new VarCharType(12)));
    }

    @Test
    void testBooleanType() {
        assertThat(new BooleanType())
                .satisfies(
                        baseAssertions(
                                "BOOLEAN",
                                "BOOLEAN",
                                new Class[] {Boolean.class, boolean.class},
                                new Class[] {Boolean.class},
                                new LogicalType[] {},
                                new BooleanType(false)));
    }

    @Test
    void testBinaryType() {
        assertThat(new BinaryType(22))
                .satisfies(
                        baseAssertions(
                                "BINARY(22)",
                                "BINARY(22)",
                                new Class[] {byte[].class},
                                new Class[] {byte[].class},
                                new LogicalType[] {},
                                new BinaryType()));
    }

    @Test
    void testVarBinaryType() {
        assertThat(new VarBinaryType(22))
                .satisfies(
                        baseAssertions(
                                "VARBINARY(22)",
                                "VARBINARY(22)",
                                new Class[] {byte[].class},
                                new Class[] {byte[].class},
                                new LogicalType[] {},
                                new VarBinaryType()));
    }

    @Test
    void testVarBinaryTypeWithMaximumLength() {
        assertThat(new VarBinaryType(Integer.MAX_VALUE))
                .satisfies(
                        baseAssertions(
                                "VARBINARY(2147483647)",
                                "BYTES",
                                new Class[] {byte[].class},
                                new Class[] {byte[].class},
                                new LogicalType[] {},
                                new VarBinaryType(12)));
    }

    @Test
    void testDecimalType() {
        assertThat(new DecimalType(10, 2))
                .satisfies(
                        baseAssertions(
                                "DECIMAL(10, 2)",
                                "DECIMAL(10, 2)",
                                new Class[] {BigDecimal.class},
                                new Class[] {BigDecimal.class},
                                new LogicalType[] {},
                                new DecimalType()));
    }

    @Test
    void testTinyIntType() {
        assertThat(new TinyIntType())
                .satisfies(
                        baseAssertions(
                                "TINYINT",
                                "TINYINT",
                                new Class[] {Byte.class, byte.class},
                                new Class[] {Byte.class},
                                new LogicalType[] {},
                                new TinyIntType(false)));
    }

    @Test
    void testSmallIntType() {
        assertThat(new SmallIntType())
                .satisfies(
                        baseAssertions(
                                "SMALLINT",
                                "SMALLINT",
                                new Class[] {Short.class, short.class},
                                new Class[] {Short.class},
                                new LogicalType[] {},
                                new SmallIntType(false)));
    }

    @Test
    void testIntType() {
        assertThat(new IntType())
                .satisfies(
                        baseAssertions(
                                "INT",
                                "INT",
                                new Class[] {Integer.class, int.class},
                                new Class[] {Integer.class},
                                new LogicalType[] {},
                                new IntType(false)));
    }

    @Test
    void testBigIntType() {
        assertThat(new BigIntType())
                .satisfies(
                        baseAssertions(
                                "BIGINT",
                                "BIGINT",
                                new Class[] {Long.class, long.class},
                                new Class[] {Long.class},
                                new LogicalType[] {},
                                new BigIntType(false)));
    }

    @Test
    void testFloatType() {
        assertThat(new FloatType())
                .satisfies(
                        baseAssertions(
                                "FLOAT",
                                "FLOAT",
                                new Class[] {Float.class, float.class},
                                new Class[] {Float.class},
                                new LogicalType[] {},
                                new FloatType(false)));
    }

    @Test
    void testDoubleType() {
        assertThat(new DoubleType())
                .satisfies(
                        baseAssertions(
                                "DOUBLE",
                                "DOUBLE",
                                new Class[] {Double.class, double.class},
                                new Class[] {Double.class},
                                new LogicalType[] {},
                                new DoubleType(false)));
    }

    @Test
    void testDateType() {
        assertThat(new DateType())
                .satisfies(
                        baseAssertions(
                                "DATE",
                                "DATE",
                                new Class[] {
                                    java.sql.Date.class, java.time.LocalDate.class, int.class
                                },
                                new Class[] {java.time.LocalDate.class},
                                new LogicalType[] {},
                                new DateType(false)));
    }

    @Test
    void testTimeType() {
        assertThat(new TimeType(9))
                .satisfies(
                        baseAssertions(
                                "TIME(9)",
                                "TIME(9)",
                                new Class[] {
                                    java.sql.Time.class, java.time.LocalTime.class, long.class
                                },
                                new Class[] {java.time.LocalTime.class},
                                new LogicalType[] {},
                                new TimeType()));
    }

    @Test
    void testTimestampType() {
        assertThat(new TimestampType(9))
                .satisfies(
                        baseAssertions(
                                "TIMESTAMP(9)",
                                "TIMESTAMP(9)",
                                new Class[] {
                                    java.sql.Timestamp.class, java.time.LocalDateTime.class
                                },
                                new Class[] {java.time.LocalDateTime.class},
                                new LogicalType[] {},
                                new TimestampType(3)));
    }

    @Test
    void testTimestampTypeWithTimeAttribute() {
        assertThat(new TimestampType(true, TimestampKind.ROWTIME, 9))
                .satisfies(
                        baseAssertions(
                                "TIMESTAMP(9)",
                                "TIMESTAMP(9) *ROWTIME*",
                                new Class[] {
                                    java.sql.Timestamp.class, java.time.LocalDateTime.class
                                },
                                new Class[] {java.time.LocalDateTime.class},
                                new LogicalType[] {},
                                new TimestampType(3)));
    }

    @Test
    void testZonedTimestampType() {
        assertThat(new ZonedTimestampType(9))
                .satisfies(
                        baseAssertions(
                                "TIMESTAMP(9) WITH TIME ZONE",
                                "TIMESTAMP(9) WITH TIME ZONE",
                                new Class[] {
                                    java.time.ZonedDateTime.class, java.time.OffsetDateTime.class
                                },
                                new Class[] {java.time.OffsetDateTime.class},
                                new LogicalType[] {},
                                new ZonedTimestampType(3)));
    }

    @Test
    void testZonedTimestampTypeWithTimeAttribute() {
        assertThat(new ZonedTimestampType(true, TimestampKind.ROWTIME, 9))
                .satisfies(
                        baseAssertions(
                                "TIMESTAMP(9) WITH TIME ZONE",
                                "TIMESTAMP(9) WITH TIME ZONE *ROWTIME*",
                                new Class[] {
                                    java.time.ZonedDateTime.class, java.time.OffsetDateTime.class
                                },
                                new Class[] {java.time.OffsetDateTime.class},
                                new LogicalType[] {},
                                new ZonedTimestampType(3)));
    }

    @Test
    void testLocalZonedTimestampType() {
        assertThat(new LocalZonedTimestampType(9))
                .satisfies(
                        baseAssertions(
                                "TIMESTAMP(9) WITH LOCAL TIME ZONE",
                                "TIMESTAMP_LTZ(9)",
                                new Class[] {java.time.Instant.class, long.class, int.class},
                                new Class[] {java.time.Instant.class},
                                new LogicalType[] {},
                                new LocalZonedTimestampType(3)));
    }

    @Test
    void testLocalZonedTimestampTypeWithTimeAttribute() {
        assertThat(new LocalZonedTimestampType(true, TimestampKind.ROWTIME, 9))
                .satisfies(
                        baseAssertions(
                                "TIMESTAMP(9) WITH LOCAL TIME ZONE",
                                "TIMESTAMP_LTZ(9) *ROWTIME*",
                                new Class[] {java.time.Instant.class, long.class, int.class},
                                new Class[] {java.time.Instant.class},
                                new LogicalType[] {},
                                new LocalZonedTimestampType(3)));
    }

    @Test
    void testYearMonthIntervalType() {
        assertThat(
                        new YearMonthIntervalType(
                                YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH, 2))
                .satisfies(
                        baseAssertions(
                                "INTERVAL YEAR(2) TO MONTH",
                                "INTERVAL YEAR(2) TO MONTH",
                                new Class[] {java.time.Period.class, int.class},
                                new Class[] {java.time.Period.class},
                                new LogicalType[] {},
                                new YearMonthIntervalType(
                                        YearMonthIntervalType.YearMonthResolution.MONTH)));
    }

    @Test
    void testDayTimeIntervalType() {
        assertThat(
                        new DayTimeIntervalType(
                                DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND, 2, 6))
                .satisfies(
                        baseAssertions(
                                "INTERVAL DAY(2) TO SECOND(6)",
                                "INTERVAL DAY(2) TO SECOND(6)",
                                new Class[] {java.time.Duration.class, long.class},
                                new Class[] {java.time.Duration.class},
                                new LogicalType[] {},
                                new DayTimeIntervalType(
                                        DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND,
                                        2,
                                        7)));
    }

    @Test
    void testArrayType() {
        assertThat(new ArrayType(new TimestampType()))
                .satisfies(
                        baseAssertions(
                                "ARRAY<TIMESTAMP(6)>",
                                "ARRAY<TIMESTAMP(6)>",
                                new Class[] {
                                    java.sql.Timestamp[].class,
                                    java.time.LocalDateTime[].class,
                                    List.class,
                                    ArrayList.class
                                },
                                new Class[] {
                                    java.sql.Timestamp[].class,
                                    java.time.LocalDateTime[].class,
                                    List.class
                                },
                                new LogicalType[] {new TimestampType()},
                                new ArrayType(new SmallIntType())));

        assertThat(new ArrayType(new ArrayType(new TimestampType())))
                .satisfies(
                        baseAssertions(
                                "ARRAY<ARRAY<TIMESTAMP(6)>>",
                                "ARRAY<ARRAY<TIMESTAMP(6)>>",
                                new Class[] {
                                    java.sql.Timestamp[][].class, java.time.LocalDateTime[][].class
                                },
                                new Class[] {
                                    java.sql.Timestamp[][].class, java.time.LocalDateTime[][].class
                                },
                                new LogicalType[] {new ArrayType(new TimestampType())},
                                new ArrayType(new ArrayType(new SmallIntType()))));

        final LogicalType nestedArray = new ArrayType(new ArrayType(new TimestampType()));
        assertThat(nestedArray)
                .doesNotSupportInputConversion(java.sql.Timestamp[].class)
                .doesNotSupportOutputConversion(java.sql.Timestamp[].class);
    }

    @Test
    void testMultisetType() {
        assertThat(new MultisetType(new TimestampType()))
                .satisfies(
                        baseAssertions(
                                "MULTISET<TIMESTAMP(6)>",
                                "MULTISET<TIMESTAMP(6)>",
                                new Class[] {Map.class, HashMap.class, TreeMap.class},
                                new Class[] {Map.class},
                                new LogicalType[] {new TimestampType()},
                                new MultisetType(new SmallIntType())));

        assertThat(new MultisetType(new MultisetType(new TimestampType())))
                .satisfies(
                        baseAssertions(
                                "MULTISET<MULTISET<TIMESTAMP(6)>>",
                                "MULTISET<MULTISET<TIMESTAMP(6)>>",
                                new Class[] {Map.class, HashMap.class, TreeMap.class},
                                new Class[] {Map.class},
                                new LogicalType[] {new MultisetType(new TimestampType())},
                                new MultisetType(new MultisetType(new SmallIntType()))));
    }

    @Test
    void testMapType() {
        assertThat(new MapType(new VarCharType(20), new TimestampType()))
                .satisfies(
                        baseAssertions(
                                "MAP<VARCHAR(20), TIMESTAMP(6)>",
                                "MAP<VARCHAR(20), TIMESTAMP(6)>",
                                new Class[] {Map.class, HashMap.class, TreeMap.class},
                                new Class[] {Map.class},
                                new LogicalType[] {new VarCharType(20), new TimestampType()},
                                new MapType(new VarCharType(99), new TimestampType())));
    }

    @Test
    void testRowType() {
        assertThat(
                        new RowType(
                                Arrays.asList(
                                        new RowType.RowField(
                                                "a", new VarCharType(), "Someone's desc."),
                                        new RowType.RowField("b`", new TimestampType()))))
                .satisfies(
                        baseAssertions(
                                "ROW<`a` VARCHAR(1) 'Someone''s desc.', `b``` TIMESTAMP(6)>",
                                "ROW<`a` VARCHAR(1) '...', `b``` TIMESTAMP(6)>",
                                new Class[] {Row.class},
                                new Class[] {Row.class},
                                new LogicalType[] {new VarCharType(), new TimestampType()},
                                new RowType(
                                        Arrays.asList(
                                                new RowType.RowField(
                                                        "a", new VarCharType(), "Different desc."),
                                                new RowType.RowField("b`", new TimestampType())))));

        assertThatThrownBy(
                        () ->
                                new RowType(
                                        Arrays.asList(
                                                new RowType.RowField("b", new VarCharType()),
                                                new RowType.RowField("b", new VarCharType()),
                                                new RowType.RowField("a", new VarCharType()),
                                                new RowType.RowField("a", new TimestampType()))))
                .isInstanceOf(ValidationException.class);

        assertThatThrownBy(
                        () ->
                                new RowType(
                                        Collections.singletonList(
                                                new RowType.RowField("", new VarCharType()))))
                .isInstanceOf(ValidationException.class);
    }

    @Test
    void testDistinctType() {
        assertThat(createDistinctType("Money"))
                .satisfies(
                        baseAssertions(
                                "`cat`.`db`.`Money`",
                                "`cat`.`db`.`Money`",
                                new Class[] {BigDecimal.class},
                                new Class[] {BigDecimal.class},
                                new LogicalType[] {new DecimalType(10, 2)},
                                createDistinctType("Monetary")));
    }

    @Test
    void testStructuredType() {
        assertThat(createUserType(true, true))
                .satisfies(
                        baseAssertions(
                                "`cat`.`db`.`User`",
                                "`cat`.`db`.`User`",
                                new Class[] {Row.class, User.class},
                                new Class[] {Row.class, Human.class, User.class},
                                new LogicalType[] {
                                    UDT_NAME_TYPE, UDT_SETTING_TYPE, UDT_TIMESTAMP_TYPE
                                },
                                createUserType(true, false)));

        assertThat(createHumanType(false))
                .satisfies(
                        conversions(
                                new Class[] {
                                    Row.class, Human.class, User.class // every User is Human
                                },
                                new Class[] {Row.class, Human.class}));

        // not every Human is User
        assertThat(createUserType(true, true)).doesNotSupportInputConversion(Human.class);

        // User is not implementing SpecialHuman
        assertThat(createHumanType(true)).doesNotSupportInputConversion(User.class);
    }

    @Test
    void testNullType() {
        assertThat(new NullType())
                .isJavaSerializable()
                .satisfies(nonEqualityCheckWithOtherType(new TimeType()))
                .hasSerializableString("NULL")
                .hasSummaryString("NULL")
                .supportsInputConversion(Object.class)
                .supportsOutputConversion(Object.class)
                .supportsOutputConversion(Integer.class)
                .doesNotSupportOutputConversion(int.class);
    }

    @Test
    void testTypeInformationRawType() throws Exception {
        final TypeInformationRawType<?> rawType =
                new TypeInformationRawType<>(Types.TUPLE(Types.STRING, Types.INT));

        assertThat(rawType)
                .satisfies(
                        nonEqualityCheckWithOtherType(
                                new TypeInformationRawType<>(
                                        Types.TUPLE(Types.STRING, Types.LONG))))
                .satisfies(LogicalTypesTest::nullability)
                .isJavaSerializable()
                .hasSummaryString("RAW('org.apache.flink.api.java.tuple.Tuple2', ?)")
                .hasNoSerializableString()
                .satisfies(conversions(new Class[] {Tuple2.class}, new Class[] {Tuple.class}));
    }

    @Test
    void testRawType() {
        final RawType<Human> rawType =
                new RawType<>(
                        Human.class, new KryoSerializer<>(Human.class, new ExecutionConfig()));
        final String className = "org.apache.flink.table.types.LogicalTypesTest$Human";
        // use rawType.getSerializerString() to regenerate the following string
        final String serializerString =
                "AEdvcmcuYXBhY2hlLmZsaW5rLmFwaS5qYXZhLnR5cGV1dGlscy5ydW50aW1lLmtyeW8uS3J5b1Nlcml"
                        + "hbGl6ZXJTbmFwc2hvdAAAAAIAM29yZy5hcGFjaGUuZmxpbmsudGFibGUudHlwZXMuTG9naWNhbFR5cG"
                        + "VzVGVzdCRIdW1hbgAABPLGmj1wAAAAAgAzb3JnLmFwYWNoZS5mbGluay50YWJsZS50eXBlcy5Mb2dpY"
                        + "2FsVHlwZXNUZXN0JEh1bWFuAQAAADUAM29yZy5hcGFjaGUuZmxpbmsudGFibGUudHlwZXMuTG9naWNh"
                        + "bFR5cGVzVGVzdCRIdW1hbgEAAAA5ADNvcmcuYXBhY2hlLmZsaW5rLnRhYmxlLnR5cGVzLkxvZ2ljYWx"
                        + "UeXBlc1Rlc3QkSHVtYW4AAAAAAClvcmcuYXBhY2hlLmF2cm8uZ2VuZXJpYy5HZW5lcmljRGF0YSRBcn"
                        + "JheQEAAAArAClvcmcuYXBhY2hlLmF2cm8uZ2VuZXJpYy5HZW5lcmljRGF0YSRBcnJheQEAAAC2AFVvc"
                        + "mcuYXBhY2hlLmZsaW5rLmFwaS5qYXZhLnR5cGV1dGlscy5ydW50aW1lLmtyeW8uU2VyaWFsaXplcnMk"
                        + "RHVtbXlBdnJvUmVnaXN0ZXJlZENsYXNzAAAAAQBZb3JnLmFwYWNoZS5mbGluay5hcGkuamF2YS50eXB"
                        + "ldXRpbHMucnVudGltZS5rcnlvLlNlcmlhbGl6ZXJzJER1bW15QXZyb0tyeW9TZXJpYWxpemVyQ2xhc3"
                        + "MAAATyxpo9cAAAAAAAAATyxpo9cAAAAAA=";

        assertThat(rawType)
                .satisfies(
                        baseAssertions(
                                "RAW('" + className + "', '" + serializerString + "')",
                                "RAW('org.apache.flink.table.types.LogicalTypesTest$Human', '...')",
                                new Class[] {Human.class, User.class}, // every User is Human
                                new Class[] {Human.class},
                                new LogicalType[] {},
                                new RawType<>(
                                        User.class,
                                        new KryoSerializer<>(User.class, new ExecutionConfig()))));

        assertThat(
                        RawType.restore(
                                LogicalTypesTest.class.getClassLoader(),
                                className,
                                serializerString))
                .isEqualTo(rawType);
    }

    @Test
    void testSymbolType() {
        final SymbolType<?> symbolType = new SymbolType<>();

        assertThat(symbolType)
                .hasSummaryString("SYMBOL")
                .satisfies(LogicalTypesTest::nullability)
                .isJavaSerializable()
                .satisfies(
                        conversions(
                                new Class[] {TimeIntervalUnit.class},
                                new Class[] {TimeIntervalUnit.class}))
                .hasNoSerializableString();
    }

    @Test
    void testUnresolvedUserDefinedType() {
        final UnresolvedUserDefinedType unresolvedType =
                new UnresolvedUserDefinedType(
                        UnresolvedIdentifier.of("catalog", "database", "Type"));

        assertThat(unresolvedType)
                .satisfies(
                        nonEqualityCheckWithOtherType(
                                new UnresolvedUserDefinedType(
                                        UnresolvedIdentifier.of("different", "database", "Type"))))
                .hasSummaryString("`catalog`.`database`.`Type`");
    }

    @Test
    void testEmptyStringLiterals() {
        final CharType charType = CharType.ofEmptyLiteral();
        final VarCharType varcharType = VarCharType.ofEmptyLiteral();
        final BinaryType binaryType = BinaryType.ofEmptyLiteral();
        final VarBinaryType varBinaryType = VarBinaryType.ofEmptyLiteral();

        // make the types nullable for testing
        assertThat(charType.copy(true)).satisfies(nonEqualityCheckWithOtherType(new CharType(1)));
        assertThat(varcharType.copy(true))
                .satisfies(nonEqualityCheckWithOtherType(new VarCharType(1)));
        assertThat(binaryType.copy(true))
                .satisfies(nonEqualityCheckWithOtherType(new BinaryType(1)));
        assertThat(varBinaryType.copy(true))
                .satisfies(nonEqualityCheckWithOtherType(new VarBinaryType(1)));

        assertThat(charType).hasSummaryString("CHAR(0) NOT NULL");
        assertThat(varcharType).hasSummaryString("VARCHAR(0) NOT NULL");
        assertThat(binaryType).hasSummaryString("BINARY(0) NOT NULL");
        assertThat(varBinaryType).hasSummaryString("VARBINARY(0) NOT NULL");

        assertThat(charType).hasNoSerializableString();
        assertThat(varcharType).hasNoSerializableString();
        assertThat(binaryType).hasNoSerializableString();
        assertThat(varBinaryType).hasNoSerializableString();
    }

    @Test
    void testUnregisteredStructuredType() {
        final StructuredType structuredType = createUserType(false, true);

        assertThat(structuredType)
                .satisfies(nonEqualityCheckWithOtherType(createUserType(false, false)))
                .satisfies(LogicalTypesTest::nullability)
                .isJavaSerializable()
                .hasNoSerializableString()
                .hasSummaryString(
                        String.format(
                                "*%s<`name` VARCHAR(1) '...', `setting` INT, `timestamp` TIMESTAMP(6)>*",
                                User.class.getName()))
                .satisfies(
                        conversions(
                                new Class[] {Row.class, User.class},
                                new Class[] {Row.class, Human.class, User.class}))
                .hasExactlyChildren(UDT_NAME_TYPE, UDT_SETTING_TYPE, UDT_TIMESTAMP_TYPE);
    }

    // --------------------------------------------------------------------------------------------

    private static ThrowingConsumer<LogicalType> baseAssertions(
            String serializableString,
            String summaryString,
            Class<?>[] supportedInputClasses,
            Class<?>[] supportedOutputClasses,
            LogicalType[] children,
            LogicalType otherType) {
        return nullableType ->
                assertThat(nullableType)
                        .satisfies(nonEqualityCheckWithOtherType(otherType))
                        .satisfies(LogicalTypesTest::nullability)
                        .isJavaSerializable()
                        .hasSerializableString(serializableString)
                        .hasSummaryString(summaryString)
                        .satisfies(conversions(supportedInputClasses, supportedOutputClasses))
                        .hasExactlyChildren(children);
    }

    private static ThrowingConsumer<LogicalType> nonEqualityCheckWithOtherType(
            LogicalType otherType) {
        return nullableType -> {
            assertThat(nullableType)
                    .isNullable()
                    .isEqualTo(nullableType)
                    .isEqualTo(nullableType.copy())
                    .isNotEqualTo(otherType);

            assertThat(nullableType.hashCode())
                    .isEqualTo(nullableType.hashCode())
                    .isNotEqualTo(otherType.hashCode());
        };
    }

    private static void nullability(LogicalType nullableType) {
        final LogicalType notNullInstance = nullableType.copy(false);
        assertThat(notNullInstance).isNotNullable();
        assertThat(nullableType).isNotEqualTo(notNullInstance);
    }

    private static ThrowingConsumer<LogicalType> conversions(
            Class<?>[] inputs, Class<?>[] outputs) {
        return type -> {
            assertThat(type)
                    .supportsInputConversion(type.getDefaultConversion())
                    .supportsOutputConversion(type.getDefaultConversion())
                    .doesNotSupportInputConversion(LogicalTypesTest.class)
                    .doesNotSupportOutputConversion(LogicalTypesTest.class);

            for (Class<?> clazz : inputs) {
                assertThat(type).supportsInputConversion(clazz);
            }

            for (Class<?> clazz : outputs) {
                assertThat(type).supportsOutputConversion(clazz);
            }
        };
    }

    private DistinctType createDistinctType(String typeName) {
        return DistinctType.newBuilder(
                        ObjectIdentifier.of("cat", "db", typeName), new DecimalType(10, 2))
                .description("Money type desc.")
                .build();
    }

    private static final LogicalType UDT_NAME_TYPE = new VarCharType();

    private static final LogicalType UDT_SETTING_TYPE = new IntType();

    private static final LogicalType UDT_TIMESTAMP_TYPE = new TimestampType();

    private StructuredType createHumanType(boolean useDifferentImplementation) {
        return StructuredType.newBuilder(
                        ObjectIdentifier.of("cat", "db", "Human"),
                        useDifferentImplementation ? SpecialHuman.class : Human.class)
                .attributes(
                        Collections.singletonList(
                                new StructuredType.StructuredAttribute(
                                        "name", UDT_NAME_TYPE, "Description.")))
                .description("Human type desc.")
                .setFinal(false)
                .setInstantiable(false)
                .build();
    }

    private StructuredType createUserType(boolean isRegistered, boolean isFinal) {
        final StructuredType.Builder builder;
        if (isRegistered) {
            builder =
                    StructuredType.newBuilder(ObjectIdentifier.of("cat", "db", "User"), User.class);
        } else {
            builder = StructuredType.newBuilder(User.class);
        }
        return builder.attributes(
                        Arrays.asList(
                                new StructuredType.StructuredAttribute("setting", UDT_SETTING_TYPE),
                                new StructuredType.StructuredAttribute(
                                        "timestamp", UDT_TIMESTAMP_TYPE)))
                .description("User type desc.")
                .setFinal(isFinal)
                .setInstantiable(true)
                .superType(createHumanType(false))
                .build();
    }

    private abstract static class SpecialHuman {
        public String name;
    }

    private abstract static class Human {
        public String name;
    }

    private static final class User extends Human {
        public int setting;
        public LocalDateTime timestamp;
    }
}
