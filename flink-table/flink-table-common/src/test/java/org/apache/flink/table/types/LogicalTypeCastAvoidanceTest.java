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
import org.apache.flink.table.catalog.ObjectIdentifier;
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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeCasts.supportsAvoidingCast;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.of;

/** Tests for {@link LogicalTypeCasts#supportsAvoidingCast(LogicalType, LogicalType)}. */
class LogicalTypeCastAvoidanceTest {

    private static Stream<Arguments> testData() {
        return Stream.of(
                of(new CharType(), new CharType(5), false),
                of(new VarCharType(30), new VarCharType(10), false),
                of(new VarCharType(10), new VarCharType(30), true),
                of(new CharType(10), new VarCharType(30), true),
                of(new BinaryType(10), new VarBinaryType(30), true),
                of(new CharType(false, 10), new VarCharType(30), true),
                of(new BinaryType(false, 10), new VarBinaryType(30), true),
                of(new VarCharType(30), new CharType(10), false),
                of(new VarBinaryType(30), new BinaryType(10), false),
                of(new BooleanType(), new BooleanType(false), false),
                of(new BinaryType(10), new BinaryType(30), false),
                of(new VarBinaryType(10), new VarBinaryType(30), true),
                of(new VarBinaryType(30), new VarBinaryType(10), false),
                of(new DecimalType(), new DecimalType(10, 2), false),
                of(new TinyIntType(), new TinyIntType(false), false),
                of(new SmallIntType(), new SmallIntType(false), false),
                of(new IntType(), new IntType(false), false),
                of(new IntType(false), new IntType(), true),
                of(new BigIntType(), new BigIntType(false), false),
                of(new FloatType(), new FloatType(false), false),
                of(new DoubleType(), new DoubleType(false), false),
                of(new DateType(), new DateType(false), false),
                of(new TimeType(), new TimeType(9), false),
                of(new TimestampType(9), new TimestampType(3), false),
                of(new ZonedTimestampType(9), new ZonedTimestampType(3), false),
                of(
                        new ZonedTimestampType(false, TimestampKind.ROWTIME, 9),
                        new ZonedTimestampType(3),
                        false),
                of(
                        new YearMonthIntervalType(
                                YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH, 2),
                        new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.MONTH),
                        false),
                of(
                        new DayTimeIntervalType(
                                DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND, 2, 6),
                        new DayTimeIntervalType(
                                DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND, 2, 7),
                        false),
                of(new ArrayType(new TimestampType()), new ArrayType(new SmallIntType()), false),
                of(
                        new MultisetType(new TimestampType()),
                        new MultisetType(new SmallIntType()),
                        false),
                of(
                        new MapType(new VarCharType(10), new TimestampType()),
                        new MapType(new VarCharType(30), new TimestampType()),
                        true),
                of(
                        new MapType(new VarCharType(30), new TimestampType()),
                        new MapType(new VarCharType(10), new TimestampType()),
                        false),
                of(
                        new RowType(
                                Arrays.asList(
                                        new RowType.RowField("a", new VarCharType()),
                                        new RowType.RowField("b", new VarCharType()),
                                        new RowType.RowField("c", new VarCharType()),
                                        new RowType.RowField("d", new TimestampType()))),
                        new RowType(
                                Arrays.asList(
                                        new RowType.RowField("_a", new VarCharType()),
                                        new RowType.RowField("_b", new VarCharType()),
                                        new RowType.RowField("_c", new VarCharType()),
                                        new RowType.RowField("_d", new TimestampType()))),
                        // field name doesn't matter
                        true),
                of(
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new IntType()),
                                        new RowField("f2", new VarCharType()))),
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new IntType()),
                                        new RowField("f2", new BooleanType()))),
                        false),
                of(
                        new ArrayType(
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f1", new IntType()),
                                                new RowField("f2", new IntType())))),
                        new ArrayType(
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f3", new IntType()),
                                                new RowField("f4", new IntType())))),
                        true),
                of(
                        new MapType(
                                new IntType(),
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f1", new IntType()),
                                                new RowField("f2", new IntType())))),
                        new MapType(
                                new IntType(),
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f3", new IntType()),
                                                new RowField("f4", new IntType())))),
                        true),
                of(
                        new MultisetType(
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f1", new IntType()),
                                                new RowField("f2", new IntType())))),
                        new MultisetType(
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f1", new IntType()),
                                                new RowField("f2", new IntType())))),
                        true),
                of(
                        new TypeInformationRawType<>(Types.GENERIC(LogicalTypesTest.class)),
                        new TypeInformationRawType<>(Types.GENERIC(Object.class)),
                        false),
                of(
                        createUserType("User", new IntType(), new VarCharType()),
                        createUserType("User", new IntType(), new VarCharType()),
                        true),
                of(
                        createUserType("User", new IntType(), new VarCharType()),
                        createUserType("User2", new IntType(), new VarCharType()),
                        false),
                of(
                        createDistinctType("Money", new DecimalType(10, 2)),
                        createDistinctType("Money", new DecimalType(10, 2)),
                        true),
                of(
                        createDistinctType("Money", new DecimalType(10, 2)),
                        createDistinctType("Money2", new DecimalType(10, 2)),
                        true),

                // row and structured type
                of(
                        RowType.of(new IntType(), new VarCharType()),
                        createUserType("User2", new IntType(), new VarCharType()),
                        true),
                of(
                        RowType.of(new BigIntType(), new VarCharType()),
                        createUserType("User2", new IntType(), new VarCharType()),
                        false),
                of(
                        createUserType("User2", new IntType(), new VarCharType()),
                        RowType.of(new IntType(), new VarCharType()),
                        true),
                of(
                        createUserType("User2", new IntType(), new VarCharType()),
                        RowType.of(new BigIntType(), new VarCharType()),
                        false),

                // test slightly different children of anonymous structured types
                of(
                        StructuredType.newBuilder(Void.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredType.StructuredAttribute(
                                                        "f1", new TimestampType()),
                                                new StructuredType.StructuredAttribute(
                                                        "diff", new TinyIntType(false))))
                                .build(),
                        StructuredType.newBuilder(Void.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredType.StructuredAttribute(
                                                        "f1", new TimestampType()),
                                                new StructuredType.StructuredAttribute(
                                                        "diff", new TinyIntType(true))))
                                .build(),
                        true),
                of(
                        StructuredType.newBuilder(Void.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredType.StructuredAttribute(
                                                        "f1", new TimestampType()),
                                                new StructuredType.StructuredAttribute(
                                                        "diff", new TinyIntType(true))))
                                .build(),
                        StructuredType.newBuilder(Void.class)
                                .attributes(
                                        Arrays.asList(
                                                new StructuredType.StructuredAttribute(
                                                        "f1", new TimestampType()),
                                                new StructuredType.StructuredAttribute(
                                                        "diff", new TinyIntType(false))))
                                .build(),
                        false));
    }

    @ParameterizedTest(name = "{index}: [{0} COMPATIBLE {1} => {2}")
    @MethodSource("testData")
    void testSupportsAvoidingCast(LogicalType sourceType, LogicalType targetType, boolean equals) {
        assertThat(supportsAvoidingCast(sourceType, targetType)).isEqualTo(equals);
        assertThat(supportsAvoidingCast(sourceType, sourceType.copy())).isTrue();
        assertThat(supportsAvoidingCast(targetType, targetType.copy())).isTrue();
    }

    private static DistinctType createDistinctType(String name, LogicalType sourceType) {
        return DistinctType.newBuilder(ObjectIdentifier.of("cat", "db", name), sourceType)
                .description("Money type desc.")
                .build();
    }

    private static StructuredType createUserType(String name, LogicalType... children) {
        return StructuredType.newBuilder(ObjectIdentifier.of("cat", "db", name), User.class)
                .attributes(
                        Arrays.stream(children)
                                .map(lt -> new StructuredType.StructuredAttribute("field", lt))
                                .collect(Collectors.toList()))
                .description("User type desc.")
                .setFinal(true)
                .setInstantiable(true)
                .build();
    }

    private static final class User {
        public int setting;
    }
}
