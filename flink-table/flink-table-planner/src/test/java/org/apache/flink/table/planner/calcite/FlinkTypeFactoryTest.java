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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
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
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;

import org.apache.calcite.rel.type.RelDataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.DayOfWeek;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link FlinkTypeFactory}. */
@Execution(ExecutionMode.CONCURRENT)
class FlinkTypeFactoryTest {

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
                new ArrayType(new DoubleType()),
                new MapType(new DoubleType(), VarCharType.STRING_TYPE),
                RowType.of(new DoubleType(), VarCharType.STRING_TYPE),
                new RawType<>(
                        DayOfWeek.class,
                        new KryoSerializer<>(DayOfWeek.class, new ExecutionConfig())));
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
