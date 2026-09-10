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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.legacy.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
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
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.UuidType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;
import org.apache.flink.testutils.ClassLoaderUtils;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.lang.ref.Reference;
import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FlinkTypeFactory}. */
@Execution(ExecutionMode.CONCURRENT)
class FlinkTypeFactoryTest {

    private static final Class<?> FIRST_JOB_CLASS =
            ClassLoaderUtils.createSerializableObjectFromNewClassLoader().getObject().getClass();
    private static final Class<?> SECOND_JOB_CLASS =
            ClassLoaderUtils.createSerializableObjectFromNewClassLoader().getObject().getClass();

    static Stream<LogicalType> testInternalToRelType() {
        return Stream.of(
                new BooleanType(),
                new TinyIntType(),
                VarCharType.STRING_TYPE,
                new DoubleType(),
                new FloatType(),
                new IntType(),
                new BigIntType(),
                new SmallIntType(),
                new VarBinaryType(VarBinaryType.MAX_LENGTH),
                new DateType(),
                new TimeType(),
                new TimestampType(3),
                new LocalZonedTimestampType(3),
                new UuidType(),
                new ArrayType(new DoubleType()),
                new MapType(new DoubleType(), VarCharType.STRING_TYPE),
                RowType.of(new DoubleType(), VarCharType.STRING_TYPE),
                new RawType<>(
                        DayOfWeek.class,
                        new KryoSerializer<>(DayOfWeek.class, new SerializerConfigImpl())));
    }

    @MethodSource("testInternalToRelType")
    @ParameterizedTest
    void testInternalToRelType(LogicalType logicalType) {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        assertThat(
                        FlinkTypeFactory.toLogicalType(
                                typeFactory.createFieldTypeFromLogicalType(logicalType.copy(true))))
                .isEqualTo(logicalType.copy(true));
        assertThat(
                        FlinkTypeFactory.toLogicalType(
                                typeFactory.createFieldTypeFromLogicalType(
                                        logicalType.copy(false))))
                .isEqualTo(logicalType.copy(false));
        // twice for cache.
        assertThat(
                        FlinkTypeFactory.toLogicalType(
                                typeFactory.createFieldTypeFromLogicalType(logicalType.copy(true))))
                .isEqualTo(logicalType.copy(true));
        assertThat(
                        FlinkTypeFactory.toLogicalType(
                                typeFactory.createFieldTypeFromLogicalType(
                                        logicalType.copy(false))))
                .isEqualTo(logicalType.copy(false));
    }

    @Test
    void testInternalToRelTypeNull() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        LogicalType logicalType = new NullType();

        assertThat(
                        FlinkTypeFactory.toLogicalType(
                                typeFactory.createFieldTypeFromLogicalType(logicalType.copy(true))))
                .isEqualTo(logicalType.copy(true));

        assertThat(
                        FlinkTypeFactory.toLogicalType(
                                typeFactory.createFieldTypeFromLogicalType(logicalType.copy(true))))
                .isEqualTo(logicalType.copy(true));
    }

    @Test
    void testDayTimeIntervalLeadingPrecisionUpToMaxIsSupported() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        RelDataType intervalType =
                typeFactory.createSqlIntervalType(
                        new SqlIntervalQualifier(
                                TimeUnit.DAY,
                                DayTimeIntervalType.MAX_DAY_PRECISION,
                                TimeUnit.SECOND,
                                RelDataType.PRECISION_NOT_SPECIFIED,
                                SqlParserPos.ZERO));

        assertThat(FlinkTypeFactory.toLogicalType(intervalType))
                .isEqualTo(DataTypes.INTERVAL(DataTypes.SECOND(3)).notNull().getLogicalType());
    }

    @Test
    void testDayTimeIntervalLeadingPrecisionAboveMaxIsRejected() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        RelDataType intervalType =
                typeFactory.createSqlIntervalType(
                        new SqlIntervalQualifier(
                                TimeUnit.DAY,
                                DayTimeIntervalType.MAX_DAY_PRECISION + 1,
                                TimeUnit.SECOND,
                                RelDataType.PRECISION_NOT_SPECIFIED,
                                SqlParserPos.ZERO));

        assertThatThrownBy(() -> FlinkTypeFactory.toLogicalType(intervalType))
                .isInstanceOf(TableException.class)
                .hasMessageContaining(
                        "DAY_INTERVAL_TYPES precision is not supported: "
                                + (DayTimeIntervalType.MAX_DAY_PRECISION + 1));
    }

    @Test
    void testDecimalInferType() {
        assertThat(LogicalTypeMerging.findSumAggType(new DecimalType(10, 5)))
                .isEqualTo(new DecimalType(38, 5));
        assertThat(LogicalTypeMerging.findAvgAggType(new DecimalType(10, 5)))
                .isEqualTo(new DecimalType(38, 6));
    }

    @Test
    void testCanonizeType() {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        TypeInformation<?> genericTypeInfo = Types.GENERIC(TestClass.class);
        TypeInformation<?> genericTypeInfo2 = Types.GENERIC(TestClass2.class);
        RelDataType genericRelType =
                typeFactory.createFieldTypeFromLogicalType(
                        new TypeInformationRawType(genericTypeInfo));
        RelDataType genericRelType2 =
                typeFactory.createFieldTypeFromLogicalType(
                        new TypeInformationRawType(genericTypeInfo));
        RelDataType genericRelType3 =
                typeFactory.createFieldTypeFromLogicalType(
                        new TypeInformationRawType(genericTypeInfo2));

        assertThat(genericRelType).as("The type expect to be canonized").isEqualTo(genericRelType2);
        assertThat(genericRelType)
                .as("The type expect to be not canonized")
                .isNotEqualTo(genericRelType3);
        assertThat(typeFactory.builder().add("f0", genericRelType).build())
                .as("The type expect to be not canonized")
                .isNotEqualTo(typeFactory.builder().add("f0", genericRelType3).build());
    }

    static Stream<Arguments> testRawTypeClassLoaderIsolation() {
        final List<Arguments> arguments = new ArrayList<>();
        for (boolean legacy : new boolean[] {false, true}) {
            for (LogicalTypeRoot root :
                    new LogicalTypeRoot[] {
                        LogicalTypeRoot.RAW,
                        LogicalTypeRoot.ARRAY,
                        LogicalTypeRoot.MAP,
                        LogicalTypeRoot.MULTISET,
                        LogicalTypeRoot.ROW
                    }) {
                for (boolean rawNullable : new boolean[] {false, true}) {
                    for (boolean nullable : new boolean[] {false, true}) {
                        if (root != LogicalTypeRoot.RAW || rawNullable == nullable) {
                            arguments.add(Arguments.of(legacy, root, rawNullable, nullable));
                        }
                    }
                }
            }
        }
        return arguments.stream();
    }

    @MethodSource
    @ParameterizedTest
    void testRawTypeClassLoaderIsolation(
            boolean legacy, LogicalTypeRoot root, boolean rawNullable, boolean nullable) {
        assertRawTypeClassLoaderIsolation(
                legacy, rawNullable, rawType -> wrapRawType(rawType, root, nullable));
    }

    static Stream<Arguments> testAdditionalRawTypeClassLoaderIsolation() {
        return Stream.of(false, true)
                .flatMap(
                        legacy ->
                                Stream.of(false, true).map(mapKey -> Arguments.of(legacy, mapKey)));
    }

    @MethodSource
    @ParameterizedTest
    void testAdditionalRawTypeClassLoaderIsolation(boolean legacy, boolean mapKey) {
        assertRawTypeClassLoaderIsolation(
                legacy,
                true,
                rawType ->
                        mapKey
                                ? new MapType(rawType, new IntType(false))
                                : RowType.of(new ArrayType(rawType)));
    }

    @ValueSource(booleans = {false, true})
    @ParameterizedTest
    void testRawJoinTypeClassLoaderIsolation(boolean legacy) {
        final LogicalType firstRaw = createRawType(FIRST_JOB_CLASS, legacy, true);
        final LogicalType secondRaw = createRawType(SECOND_JOB_CLASS, legacy, true);
        final FlinkTypeFactory firstFactory =
                new FlinkTypeFactory(FIRST_JOB_CLASS.getClassLoader(), FlinkTypeSystem.INSTANCE);
        final FlinkTypeFactory secondFactory =
                new FlinkTypeFactory(SECOND_JOB_CLASS.getClassLoader(), FlinkTypeSystem.INSTANCE);
        final RelDataType firstJoinType =
                firstFactory.createJoinType(
                        firstFactory.createFieldTypeFromLogicalType(RowType.of(firstRaw)));
        try {
            final RelDataType secondJoinType =
                    secondFactory.createJoinType(
                            secondFactory.createFieldTypeFromLogicalType(RowType.of(secondRaw)));
            assertThat(
                            FlinkTypeFactory.toLogicalType(
                                    firstJoinType.getFieldList().get(0).getType()))
                    .isEqualTo(firstRaw);
            assertThat(
                            FlinkTypeFactory.toLogicalType(
                                    secondJoinType.getFieldList().get(0).getType()))
                    .as("A join type must retain the next job's RAW class and serializer")
                    .isEqualTo(secondRaw);
        } finally {
            Reference.reachabilityFence(firstJoinType);
        }
    }

    private void assertRawTypeClassLoaderIsolation(
            boolean legacy, boolean rawNullable, UnaryOperator<LogicalType> wrapper) {
        assertThat(FIRST_JOB_CLASS.getName()).isEqualTo(SECOND_JOB_CLASS.getName());
        assertThat(FIRST_JOB_CLASS).isNotSameAs(SECOND_JOB_CLASS);
        final LogicalType firstRaw = createRawType(FIRST_JOB_CLASS, legacy, rawNullable);
        final LogicalType secondRaw = createRawType(SECOND_JOB_CLASS, legacy, rawNullable);
        assertThat(firstRaw).isNotEqualTo(secondRaw);
        assertThat(firstRaw.asSummaryString()).isEqualTo(secondRaw.asSummaryString());
        if (!legacy) {
            assertThat(firstRaw.asSerializableString()).isEqualTo(secondRaw.asSerializableString());
        }
        final LogicalType firstType = wrapper.apply(firstRaw);
        final LogicalType secondType = wrapper.apply(secondRaw);
        final FlinkTypeFactory firstFactory =
                new FlinkTypeFactory(FIRST_JOB_CLASS.getClassLoader(), FlinkTypeSystem.INSTANCE);
        final FlinkTypeFactory secondFactory =
                new FlinkTypeFactory(SECOND_JOB_CLASS.getClassLoader(), FlinkTypeSystem.INSTANCE);
        final RelDataType firstRelType = firstFactory.createFieldTypeFromLogicalType(firstType);
        try {
            final RelDataType secondRelType =
                    secondFactory.createFieldTypeFromLogicalType(secondType);
            assertThat(FlinkTypeFactory.toLogicalType(firstRelType)).isEqualTo(firstType);
            assertThat(FlinkTypeFactory.toLogicalType(secondRelType))
                    .as("The next job must retain its own RAW class and serializer")
                    .isEqualTo(secondType);
            assertThat(secondRelType.getFullTypeString())
                    .isEqualTo(firstRelType.getFullTypeString());
            // Nullable RAW uses seenTypes directly, without allocating a nullability copy.
            final LogicalType cachedRaw = secondRaw.copy(true);
            final RelDataType secondRawRelType =
                    secondFactory.createFieldTypeFromLogicalType(cachedRaw);
            assertThat(secondFactory.createFieldTypeFromLogicalType(cachedRaw))
                    .as("RAW types remain cached within their factory")
                    .isSameAs(secondRawRelType);
        } finally {
            // Calcite's shared caches use weak references; keep the previous job's type live.
            Reference.reachabilityFence(firstRelType);
        }
    }

    static Stream<LogicalType> testPrimitiveTypeCanonicalizationAcrossFactories() {
        return Stream.of(
                new IntType(),
                new ArrayType(new IntType()),
                new MapType(new IntType(), new IntType()),
                new MultisetType(new IntType()),
                RowType.of(new IntType()));
    }

    @MethodSource
    @ParameterizedTest
    void testPrimitiveTypeCanonicalizationAcrossFactories(LogicalType type) {
        final FlinkTypeFactory firstFactory =
                new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
        final FlinkTypeFactory secondFactory =
                new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
        final RelDataType firstRelType = firstFactory.createFieldTypeFromLogicalType(type);
        assertThat(secondFactory.createFieldTypeFromLogicalType(type)).isSameAs(firstRelType);
    }

    private static <T> LogicalType createRawType(
            Class<T> typeClass, boolean legacy, boolean nullable) {
        return legacy
                ? new TypeInformationRawType<>(nullable, Types.GENERIC(typeClass))
                : new RawType<>(
                        nullable,
                        typeClass,
                        new KryoSerializer<>(typeClass, new SerializerConfigImpl()));
    }

    private static LogicalType wrapRawType(
            LogicalType rawType, LogicalTypeRoot root, boolean nullable) {
        switch (root) {
            case RAW:
                return rawType;
            case ARRAY:
                return new ArrayType(nullable, rawType);
            case MAP:
                return new MapType(nullable, new IntType(false), rawType);
            case MULTISET:
                return new MultisetType(nullable, rawType);
            case ROW:
                return RowType.of(nullable, rawType);
            default:
                throw new IllegalArgumentException("Unexpected type root: " + root);
        }
    }

    static Stream<Arguments> testLeastRestrictive() {
        return Stream.of(
                // Since the problem is actual for collection
                // then tests are for array, map, multiset
                // Also as https://issues.apache.org/jira/browse/CALCITE-4603 says
                // before Calcite 1.27.0  it derived the type of nested collection based on the last
                // element, for that reason the type of the last element is narrower
                // than the type of element in the middle
                Arguments.of(
                        Arrays.asList(
                                new ArrayType(new VarCharType(6)),
                                new ArrayType(VarCharType.STRING_TYPE),
                                new ArrayType(new CharType(1))),
                        new ArrayType(VarCharType.STRING_TYPE)),
                Arguments.of(
                        Arrays.asList(
                                new MultisetType(new VarCharType(6)),
                                new MultisetType(VarCharType.STRING_TYPE),
                                new MultisetType(new CharType(1))),
                        new MultisetType(VarCharType.STRING_TYPE)),
                Arguments.of(
                        Arrays.asList(
                                new MapType(new CharType(1), new CharType(1)),
                                new MapType(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE),
                                new MapType(new CharType(1), new CharType(1))),
                        new MapType(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE)),
                Arguments.of(
                        Arrays.asList(
                                new MapType(new CharType(1), new VarCharType(6)),
                                new MapType(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE),
                                new MapType(new CharType(1), new CharType(1))),
                        new MapType(VarCharType.STRING_TYPE, VarCharType.STRING_TYPE)));
    }

    @MethodSource("testLeastRestrictive")
    @ParameterizedTest
    void testLeastRestrictive(List<LogicalType> input, LogicalType expected) {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        assertThat(
                        typeFactory.leastRestrictive(
                                input.stream()
                                        .map(typeFactory::createFieldTypeFromLogicalType)
                                        .collect(Collectors.toList())))
                .isEqualTo(typeFactory.createFieldTypeFromLogicalType(expected));
    }

    public static class TestClass {
        public int f0;
        public String f1;
    }

    public static class TestClass2 {
        public int f0;
        public String f1;
    }
}
