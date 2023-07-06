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

package org.apache.flink.table.planner.plan.nodes.exec.serde;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.table.functions.UserDefinedFunctionHelper;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.utils.AggSqlFunction;
import org.apache.flink.table.planner.utils.Top3;
import org.apache.flink.table.planner.utils.Top3Accum;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;

import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.stream.Stream;

import static org.apache.flink.table.planner.plan.nodes.exec.serde.JsonSerdeTestUtil.testJsonRoundTrip;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link org.apache.calcite.rel.core.AggregateCall} serialization and deserialization.
 */
@Execution(ExecutionMode.CONCURRENT)
public class AggregateCallSerdeTest {

    private static final FlinkTypeFactory FACTORY =
            new FlinkTypeFactory(
                    AggregateCallSerdeTest.class.getClassLoader(), FlinkTypeSystem.INSTANCE);

    @MethodSource("aggregateCallSpecs")
    @ParameterizedTest
    void testAggregateCallSerde(AggregateCall aggCall) throws IOException {
        testJsonRoundTrip(aggCall, AggregateCall.class);
    }

    @Test
    void testUnsupportedLegacyAggFunc() {
        assertThatThrownBy(() -> testJsonRoundTrip(getLegacyAggCall(), AggregateCall.class))
                .hasRootCauseInstanceOf(TableException.class)
                .hasRootCauseMessage(
                        "Functions of the deprecated function stack are not supported. Please update 'top3' to the new interfaces.");
    }

    public static Stream<Arguments> aggregateCallSpecs() {
        return Stream.of(
                Arguments.of(
                        AggregateCall.create(
                                SqlStdOperatorTable.RANK,
                                false,
                                false,
                                false,
                                ImmutableList.of(),
                                -1,
                                null,
                                RelCollations.EMPTY,
                                fromLogicalType(new BigIntType(false)),
                                "rk")),
                Arguments.of(
                        AggregateCall.create(
                                SqlStdOperatorTable.MAX,
                                false,
                                false,
                                false,
                                ImmutableList.of(3),
                                -1,
                                null,
                                RelCollations.EMPTY,
                                fromLogicalType(new DoubleType(false)),
                                "max_d")),
                Arguments.of(
                        AggregateCall.create(
                                SqlStdOperatorTable.COUNT,
                                false,
                                false,
                                false,
                                ImmutableList.of(),
                                -1,
                                null,
                                RelCollations.EMPTY,
                                fromLogicalType(new BigIntType(false)),
                                null)));
    }

    private static RelDataType fromLogicalType(LogicalType type) {
        return FACTORY.createFieldTypeFromLogicalType(type);
    }

    private static AggregateCall getLegacyAggCall() {
        TableAggregateFunction<Tuple2<Integer, Integer>, Top3Accum> top3 = new Top3();
        DataType externalResultType =
                TypeConversions.fromLegacyInfoToDataType(
                        UserDefinedFunctionHelper.getReturnTypeOfAggregateFunction(top3));
        DataType externalAccType =
                TypeConversions.fromLegacyInfoToDataType(
                        UserDefinedFunctionHelper.getAccumulatorTypeOfAggregateFunction(top3));
        AggSqlFunction aggFunction =
                AggSqlFunction.apply(
                        FunctionIdentifier.of("top3"),
                        "top3",
                        top3,
                        externalResultType,
                        externalAccType,
                        FACTORY,
                        false);
        return AggregateCall.create(
                aggFunction,
                false,
                false,
                false,
                ImmutableList.of(3),
                -1,
                null,
                RelCollations.of(),
                FACTORY.builder()
                        .add("f0", new BasicSqlType(FACTORY.getTypeSystem(), SqlTypeName.INTEGER))
                        .add("f1", new BasicSqlType(FACTORY.getTypeSystem(), SqlTypeName.INTEGER))
                        .build(),
                "top3");
    }
}
