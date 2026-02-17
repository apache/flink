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
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.legacy.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link LogicalTypeCasts}. */
@Execution(ExecutionMode.CONCURRENT)
class LogicalTypeCastsTest {

    private static Stream<Arguments> testData() {
        return Stream.of(
                Arguments.of(new SmallIntType(), new BigIntType(), true, true),

                // nullability does not match
                Arguments.of(new SmallIntType(false), new SmallIntType(), true, true),
                Arguments.of(new SmallIntType(), new SmallIntType(false), false, true),
                Arguments.of(
                        new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.YEAR),
                        new SmallIntType(),
                        true,
                        true),

                // not an interval with single field
                Arguments.of(
                        new YearMonthIntervalType(
                                YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH),
                        new SmallIntType(),
                        false,
                        false),
                Arguments.of(new IntType(), new DecimalType(5, 5), true, true),

                // loss of precision
                Arguments.of(new FloatType(), new IntType(), false, true),
                Arguments.of(new VarCharType(Integer.MAX_VALUE), new FloatType(), false, true),
                Arguments.of(new FloatType(), new VarCharType(Integer.MAX_VALUE), false, true),
                Arguments.of(
                        new DecimalType(3, 2), new VarCharType(Integer.MAX_VALUE), false, true),
                Arguments.of(
                        new TypeInformationRawType<>(Types.GENERIC(LogicalTypesTest.class)),
                        new TypeInformationRawType<>(Types.GENERIC(LogicalTypesTest.class)),
                        true,
                        true),
                Arguments.of(
                        new TypeInformationRawType<>(Types.GENERIC(LogicalTypesTest.class)),
                        new TypeInformationRawType<>(Types.GENERIC(Object.class)),
                        false,
                        false),
                Arguments.of(new NullType(), new IntType(), true, true),
                Arguments.of(
                        new NullType(),
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new IntType()),
                                        new RowField("f2", new IntType()))),
                        true,
                        true),
                Arguments.of(
                        new ArrayType(new IntType()), new ArrayType(new BigIntType()), true, true),
                Arguments.of(
                        new ArrayType(new IntType()),
                        new ArrayType(new VarCharType(Integer.MAX_VALUE)),
                        false,
                        true),
                Arguments.of(
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new IntType()),
                                        new RowField("f2", new IntType()))),
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new IntType()),
                                        new RowField("f2", new BigIntType()))),
                        true,
                        true),
                Arguments.of(
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new IntType(), "description"),
                                        new RowField("f2", new IntType()))),
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new IntType()),
                                        new RowField("f2", new BigIntType()))),
                        true,
                        true),
                Arguments.of(
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new IntType()),
                                        new RowField("f2", new IntType()))),
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new IntType()),
                                        new RowField("f2", new BooleanType()))),
                        false,
                        true),
                Arguments.of(
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new IntType()),
                                        new RowField("f2", new IntType()))),
                        new VarCharType(Integer.MAX_VALUE),
                        false,
                        true),

                // timestamp type and timestamp_ltz type
                Arguments.of(new TimestampType(9), new TimestampType(9), true, true),
                Arguments.of(
                        new LocalZonedTimestampType(9), new LocalZonedTimestampType(9), true, true),
                Arguments.of(new TimestampType(3), new LocalZonedTimestampType(3), true, true),
                Arguments.of(new LocalZonedTimestampType(3), new TimestampType(3), true, true),
                Arguments.of(new TimestampType(3), new LocalZonedTimestampType(6), true, true),
                Arguments.of(new LocalZonedTimestampType(3), new TimestampType(6), true, true),
                Arguments.of(
                        new TimestampType(false, 3), new LocalZonedTimestampType(6), true, true),
                Arguments.of(
                        new LocalZonedTimestampType(false, 3), new TimestampType(6), true, true),
                Arguments.of(new TimestampType(6), new LocalZonedTimestampType(3), true, true),
                Arguments.of(new LocalZonedTimestampType(6), new TimestampType(3), true, true),

                // row and structured type
                Arguments.of(
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new TimestampType()),
                                        new RowField("f2", new IntType()))),
                        StructuredType.newBuilder(ObjectIdentifier.of("cat", "db", "User"))
                                .attributes(
                                        Arrays.asList(
                                                new StructuredAttribute("f1", new TimestampType()),
                                                new StructuredAttribute("f2", new IntType())))
                                .build(),
                        true,
                        true),
                Arguments.of(
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new TimestampType()),
                                        new RowField("f2", new IntType()))),
                        StructuredType.newBuilder(ObjectIdentifier.of("cat", "db", "User"))
                                .attributes(
                                        Arrays.asList(
                                                new StructuredAttribute("f1", new TimestampType()),
                                                new StructuredAttribute("diff", new IntType())))
                                .build(),
                        true,
                        true),
                Arguments.of(
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new TimestampType()),
                                        new RowField("f2", new IntType()))),
                        StructuredType.newBuilder(ObjectIdentifier.of("cat", "db", "User"))
                                .attributes(
                                        Arrays.asList(
                                                new StructuredAttribute("f1", new TimestampType()),
                                                new StructuredAttribute("diff", new TinyIntType())))
                                .build(),
                        false,
                        true),

                // test slightly different children of anonymous structured types
                Arguments.of(
                        StructuredType.newBuilder(Void.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredAttribute("f1", new TimestampType()),
                                                new StructuredAttribute(
                                                        "diff", new TinyIntType(false))))
                                .build(),
                        StructuredType.newBuilder(Void.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredAttribute("f1", new TimestampType()),
                                                new StructuredAttribute(
                                                        "diff", new TinyIntType(true))))
                                .build(),
                        true,
                        true),
                Arguments.of(
                        StructuredType.newBuilder(Void.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredAttribute("f1", new TimestampType()),
                                                new StructuredAttribute("diff", new IntType())))
                                .build(),
                        StructuredType.newBuilder(Void.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredAttribute("f1", new TimestampType()),
                                                new StructuredAttribute("diff", new TinyIntType())))
                                .build(),
                        false,
                        true),

                // raw to binary
                Arguments.of(
                        new RawType<>(Integer.class, IntSerializer.INSTANCE),
                        new BinaryType(),
                        false,
                        true),
                // raw to binary
                Arguments.of(
                        new RawType<>(Integer.class, IntSerializer.INSTANCE),
                        VarCharType.STRING_TYPE,
                        false,
                        true));
    }

    @ParameterizedTest(name = "{index}: [From: {0}, To: {1}, Implicit: {2}, Explicit: {3}]")
    @MethodSource("testData")
    void test(
            LogicalType sourceType,
            LogicalType targetType,
            boolean supportsImplicit,
            boolean supportsExplicit) {
        assertThat(LogicalTypeCasts.supportsImplicitCast(sourceType, targetType))
                .as("Supports implicit casting")
                .isEqualTo(supportsImplicit);
        assertThat(LogicalTypeCasts.supportsExplicitCast(sourceType, targetType))
                .as("Supports explicit casting")
                .isEqualTo(supportsExplicit);
    }

    /**
     * Test data for injective cast tests. Each argument contains: (sourceType, targetType,
     * expectedInjective).
     */
    private static Stream<Arguments> injectiveCastTestData() {
        return Stream.of(
                // Integer widenings are injective
                Arguments.of(new SmallIntType(), new BigIntType(), true),
                Arguments.of(new IntType(), new BigIntType(), true),
                Arguments.of(new TinyIntType(), new IntType(), true),
                Arguments.of(new TinyIntType(), new SmallIntType(), true),

                // Explicit casts to STRING from integer types are injective
                Arguments.of(new TinyIntType(), VarCharType.STRING_TYPE, true),
                Arguments.of(new SmallIntType(), VarCharType.STRING_TYPE, true),
                Arguments.of(new IntType(), VarCharType.STRING_TYPE, true),
                Arguments.of(new BigIntType(), VarCharType.STRING_TYPE, true),

                // FLOAT/DOUBLE to STRING are injective
                Arguments.of(new FloatType(), VarCharType.STRING_TYPE, true),
                Arguments.of(new DoubleType(), VarCharType.STRING_TYPE, true),

                // Explicit casts to STRING from boolean are injective
                Arguments.of(new BooleanType(), VarCharType.STRING_TYPE, true),

                // Explicit casts to STRING from date/time types are injective
                Arguments.of(new DateType(), VarCharType.STRING_TYPE, true),
                Arguments.of(new TimeType(3), VarCharType.STRING_TYPE, true),
                Arguments.of(new TimestampType(3), VarCharType.STRING_TYPE, true),
                Arguments.of(new TimestampType(9), VarCharType.STRING_TYPE, true),
                Arguments.of(new LocalZonedTimestampType(3), VarCharType.STRING_TYPE, true),

                // Casts to CHAR are injective if the target length is sufficient
                Arguments.of(new IntType(), new CharType(100), true),
                Arguments.of(new BigIntType(), new CharType(100), true),
                Arguments.of(new IntType(), new CharType(11), true), // exact minimum
                Arguments.of(new IntType(), new CharType(3), false), // too short for "-2147483648"
                Arguments.of(new BigIntType(), new CharType(20), true), // exact minimum
                Arguments.of(new BigIntType(), new CharType(19), false), // too short
                Arguments.of(new BooleanType(), new CharType(5), true), // exact minimum for "false"
                Arguments.of(new BooleanType(), new CharType(4), false), // too short

                // CHAR → VARCHAR widening is injective
                Arguments.of(new CharType(10), VarCharType.STRING_TYPE, true),

                // BINARY → VARBINARY widening is injective
                Arguments.of(new BinaryType(10), new VarBinaryType(100), true),

                // Narrowing casts are NOT injective (lossy)
                Arguments.of(VarCharType.STRING_TYPE, new IntType(), false),
                Arguments.of(new BigIntType(), new IntType(), false),
                Arguments.of(new DoubleType(), new FloatType(), false),

                // TIMESTAMP → DATE is NOT injective (loses time-of-day information)
                // even though it is an implicit cast
                Arguments.of(new TimestampType(3), new DateType(), false),

                // DECIMAL to STRING is NOT considered injective
                Arguments.of(new DecimalType(10, 2), VarCharType.STRING_TYPE, false),

                // BYTES to STRING is NOT injective (invalid UTF-8 sequences collapse)
                Arguments.of(new VarBinaryType(100), VarCharType.STRING_TYPE, false),
                Arguments.of(new BinaryType(100), VarCharType.STRING_TYPE, false),

                // TIMESTAMP_WITH_TIME_ZONE to STRING is NOT injective
                // (theory: two timestamps with different zones could produce same string
                // depending on the implementation)
                Arguments.of(new ZonedTimestampType(3), VarCharType.STRING_TYPE, false),

                // INT → FLOAT/DOUBLE are theoretically injective
                // However, we decided not to support decimal, float and double
                // injective conversions at first since it's not a practical use case
                Arguments.of(new IntType(), new FloatType(), false),
                Arguments.of(new IntType(), new DoubleType(), false),

                // STRING → BOOLEAN is NOT injective
                Arguments.of(VarCharType.STRING_TYPE, new BooleanType(), false),

                // DOUBLE → INT is NOT injective
                Arguments.of(new DoubleType(), new IntType(), false),

                // DECIMAL → DECIMAL: only identity casts are injective
                // (changing precision/scale can lose data in various ways)
                Arguments.of(new DecimalType(10, 2), new DecimalType(10, 2), true), // identity
                Arguments.of(new DecimalType(10, 2), new DecimalType(20, 4), false), // not identity
                Arguments.of(
                        new DecimalType(10, 2), new DecimalType(15, 2), false), // precision change
                Arguments.of(new DecimalType(10, 2), new DecimalType(10, 4), false), // scale change
                Arguments.of(new DecimalType(20, 4), new DecimalType(10, 2), false), // narrowing
                Arguments.of(
                        new DecimalType(10, 4), new DecimalType(10, 2), false), // scale narrowing

                // Timestamp conversions between variants are injective
                Arguments.of(new TimestampType(3), new LocalZonedTimestampType(3), true),
                Arguments.of(new LocalZonedTimestampType(3), new TimestampType(3), true),

                // ROW types with injective field casts
                Arguments.of(
                        new RowType(
                                Arrays.asList(
                                        new RowField("id", new IntType()),
                                        new RowField("name", VarCharType.STRING_TYPE))),
                        new RowType(
                                Arrays.asList(
                                        new RowField("id", new BigIntType()),
                                        new RowField("name", VarCharType.STRING_TYPE))),
                        true),

                // ROW types with non-injective field cast (TIMESTAMP → DATE)
                Arguments.of(
                        new RowType(
                                Arrays.asList(
                                        new RowField("id", new IntType()),
                                        new RowField("ts", new TimestampType(3)))),
                        new RowType(
                                Arrays.asList(
                                        new RowField("id", new IntType()),
                                        new RowField("ts", new DateType()))),
                        false),

                // ROW types with mixed casts (one injective, one not)
                Arguments.of(
                        new RowType(
                                Arrays.asList(
                                        new RowField("id", new IntType()),
                                        new RowField("val", new DoubleType()))),
                        new RowType(
                                Arrays.asList(
                                        new RowField("id", VarCharType.STRING_TYPE),
                                        new RowField("val", new IntType()))),
                        false),

                // ---- Parameter-aware injective cast checks ----

                // CHAR length checks
                Arguments.of(new CharType(10), new CharType(10), true), // identity
                Arguments.of(new CharType(10), new CharType(20), true), // widening
                Arguments.of(new CharType(20), new CharType(10), false), // truncation

                // VARCHAR length checks
                Arguments.of(new VarCharType(10), new VarCharType(100), true), // widening
                Arguments.of(new VarCharType(100), new VarCharType(10), false), // truncation

                // CHAR → VARCHAR length checks
                Arguments.of(new CharType(10), new VarCharType(20), true), // widening
                Arguments.of(new CharType(20), new VarCharType(10), false), // truncation

                // BINARY length checks
                Arguments.of(new BinaryType(10), new BinaryType(10), true), // identity
                Arguments.of(new BinaryType(10), new BinaryType(20), true), // widening
                Arguments.of(new BinaryType(20), new BinaryType(10), false), // truncation

                // VARBINARY length checks
                Arguments.of(new VarBinaryType(10), new VarBinaryType(100), true), // widening
                Arguments.of(new VarBinaryType(100), new VarBinaryType(10), false), // truncation

                // BINARY → VARBINARY length checks
                Arguments.of(new BinaryType(10), new VarBinaryType(20), true), // widening
                Arguments.of(new BinaryType(20), new VarBinaryType(10), false), // truncation

                // TIMESTAMP precision checks (identity only)
                Arguments.of(new TimestampType(3), new TimestampType(3), true), // identity
                Arguments.of(
                        new TimestampType(3), new TimestampType(6), false), // widening rejected
                Arguments.of(
                        new TimestampType(6), new TimestampType(3), false), // narrowing rejected

                // TIMESTAMP ↔ TIMESTAMP_LTZ precision checks (identity only)
                Arguments.of(
                        new TimestampType(3), new LocalZonedTimestampType(6), false), // rejected
                Arguments.of(
                        new LocalZonedTimestampType(6), new TimestampType(3), false), // rejected

                // TIMESTAMP_LTZ precision checks (identity only)
                Arguments.of(
                        new LocalZonedTimestampType(3),
                        new LocalZonedTimestampType(3),
                        true), // identity
                Arguments.of(
                        new LocalZonedTimestampType(3),
                        new LocalZonedTimestampType(6),
                        false), // rejected

                // TIMESTAMP_TZ precision checks (identity only)
                Arguments.of(
                        new ZonedTimestampType(3), new ZonedTimestampType(3), true), // identity
                Arguments.of(
                        new ZonedTimestampType(3), new ZonedTimestampType(6), false), // rejected

                // TIME precision checks (identity only)
                Arguments.of(new TimeType(0), new TimeType(0), true), // identity
                Arguments.of(new TimeType(0), new TimeType(3), false), // widening rejected
                Arguments.of(new TimeType(3), new TimeType(0), false), // narrowing rejected

                // Cross-family casts to VARCHAR with insufficient length
                Arguments.of(new IntType(), new VarCharType(3), false), // too short
                Arguments.of(new TimestampType(3), new VarCharType(5), false), // too short

                // DOUBLE identity
                Arguments.of(new DoubleType(), new DoubleType(), true));
    }

    @ParameterizedTest(name = "{index}: [From: {0}, To: {1}, Injective: {2}]")
    @MethodSource("injectiveCastTestData")
    void testInjectiveCast(
            LogicalType sourceType, LogicalType targetType, boolean expectedInjective) {
        assertThat(LogicalTypeCasts.supportsInjectiveCast(sourceType, targetType))
                .as(
                        "Cast from %s to %s should %s injective",
                        sourceType, targetType, expectedInjective ? "be" : "not be")
                .isEqualTo(expectedInjective);
    }
}
