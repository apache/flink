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
                // Implicit casts are always injective (type widening preserves uniqueness)
                Arguments.of(new SmallIntType(), new BigIntType(), true),
                Arguments.of(new IntType(), new BigIntType(), true),
                Arguments.of(new TinyIntType(), new IntType(), true),

                // Explicit casts to STRING from integer types are injective
                Arguments.of(new TinyIntType(), VarCharType.STRING_TYPE, true),
                Arguments.of(new SmallIntType(), VarCharType.STRING_TYPE, true),
                Arguments.of(new IntType(), VarCharType.STRING_TYPE, true),
                Arguments.of(new BigIntType(), VarCharType.STRING_TYPE, true),

                // Explicit casts to STRING from floating point types are injective
                Arguments.of(new FloatType(), VarCharType.STRING_TYPE, true),
                Arguments.of(new DoubleType(), VarCharType.STRING_TYPE, true),

                // Explicit casts to STRING from boolean are injective
                Arguments.of(new BooleanType(), VarCharType.STRING_TYPE, true),

                // Explicit casts to STRING from date/time types are injective
                Arguments.of(new DateType(), VarCharType.STRING_TYPE, true),
                Arguments.of(new TimestampType(3), VarCharType.STRING_TYPE, true),
                Arguments.of(new TimestampType(9), VarCharType.STRING_TYPE, true),
                Arguments.of(new LocalZonedTimestampType(3), VarCharType.STRING_TYPE, true),
                Arguments.of(new ZonedTimestampType(3), VarCharType.STRING_TYPE, true),

                // Explicit casts to CHAR are also injective for the same source types
                Arguments.of(new IntType(), new CharType(100), true),
                Arguments.of(new BigIntType(), new CharType(100), true),

                // Narrowing casts are NOT injective (lossy)
                Arguments.of(VarCharType.STRING_TYPE, new IntType(), false),
                Arguments.of(new BigIntType(), new IntType(), false),
                Arguments.of(new DoubleType(), new FloatType(), false),
                // Note: TIMESTAMP->DATE is an implicit cast in Flink, so it returns true
                // even though time-of-day is lost. This is consistent with existing isFidelityCast.
                Arguments.of(new TimestampType(3), new DateType(), true),

                // DECIMAL to STRING is NOT considered injective
                // (different precision representations could be ambiguous)
                Arguments.of(new DecimalType(10, 2), VarCharType.STRING_TYPE, false),

                // BYTES to STRING is NOT injective (invalid UTF-8 sequences collapse)
                Arguments.of(new VarBinaryType(100), VarCharType.STRING_TYPE, false),
                Arguments.of(new BinaryType(100), VarCharType.STRING_TYPE, false),

                // TIME to STRING is NOT injective (not in the whitelist)
                Arguments.of(new TimeType(3), VarCharType.STRING_TYPE, false),

                // Casts to non-string types are generally not injective (unless implicit)
                // Note: INT->FLOAT is an implicit cast, so it returns true
                Arguments.of(new IntType(), new FloatType(), true),
                // STRING->BOOLEAN is explicit only, so not injective
                Arguments.of(VarCharType.STRING_TYPE, new BooleanType(), false),
                // INT->DOUBLE is implicit, so injective
                Arguments.of(new IntType(), new DoubleType(), true),
                // DOUBLE->INT is explicit narrowing, not injective
                Arguments.of(new DoubleType(), new IntType(), false));
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
