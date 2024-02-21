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

package org.apache.flink.table.planner.delegation.hive.utils;

import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.utils.FlinkTypeUtils;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link org.apache.flink.table.planner.utils.FlinkTypeUtils}. */
public class FlinkTypeUtilsTest {
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
                RowType.of(new DoubleType(), VarCharType.STRING_TYPE));
    }

    @MethodSource("testInternalToRelType")
    @ParameterizedTest
    void testRelTypeToInternalType(LogicalType logicalType) {
        FlinkTypeFactory typeFactory =
                new FlinkTypeFactory(
                        Thread.currentThread().getContextClassLoader(), FlinkTypeSystem.INSTANCE);

        assertThat(
                        FlinkTypeUtils.toLogicalType(
                                typeFactory.createFieldTypeFromLogicalType(logicalType.copy(true))))
                .isEqualTo(logicalType.copy(true));
        assertThat(
                        FlinkTypeUtils.toLogicalType(
                                typeFactory.createFieldTypeFromLogicalType(
                                        logicalType.copy(false))))
                .isEqualTo(logicalType.copy(false));

        // twice for cache.
        assertThat(
                        FlinkTypeUtils.toLogicalType(
                                typeFactory.createFieldTypeFromLogicalType(logicalType.copy(true))))
                .isEqualTo(logicalType.copy(true));
        assertThat(
                        FlinkTypeUtils.toLogicalType(
                                typeFactory.createFieldTypeFromLogicalType(
                                        logicalType.copy(false))))
                .isEqualTo(logicalType.copy(false));
    }
}
