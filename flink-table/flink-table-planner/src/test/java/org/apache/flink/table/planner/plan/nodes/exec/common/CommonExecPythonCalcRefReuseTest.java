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

package org.apache.flink.table.planner.plan.nodes.exec.common;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the reference reuse (deduplication) logic in {@link CommonExecPythonCalc}.
 *
 * <p>This test verifies that the {@code deduplicatePythonCalls} method correctly identifies and
 * deduplicates structurally identical deterministic RexCalls, while preserving non-deterministic
 * calls independently.
 */
class CommonExecPythonCalcRefReuseTest {

    // -------------------------------------------------------------------------
    //  Parameterized tests for deduplicatePythonCalls
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("inputForDeduplicatePythonCalls")
    void testDeduplicatePythonCalls(
            String description,
            List<RexCall> calls,
            int expectedUniqueCount,
            int[] expectedMapping) {
        CommonExecPythonCalc calc = new TestPythonCalc(Collections.emptyList());
        Tuple2<List<RexCall>, int[]> result = calc.deduplicatePythonCalls(calls);

        assertThat(result.f0).as("unique call count").hasSize(expectedUniqueCount);
        assertThat(result.f1).as("original-to-dedup mapping").containsExactly(expectedMapping);
    }

    static Stream<Arguments> inputForDeduplicatePythonCalls() {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        FlinkTypeFactory factory = new FlinkTypeFactory(classLoader, FlinkTypeSystem.INSTANCE);
        RexBuilder rb = new RexBuilder(factory);
        RelDataType intTp =
                factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.INTEGER), true);
        RelDataType bigintTp =
                factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.BIGINT), true);

        RexNode ref0 = new RexInputRef(0, intTp);
        RexNode ref1 = new RexInputRef(1, intTp);
        RexNode ref2 = new RexInputRef(2, bigintTp);

        // Deterministic calls
        RexCall plusAB1 = (RexCall) rb.makeCall(SqlStdOperatorTable.PLUS, ref0, ref1);
        RexCall plusAB2 = (RexCall) rb.makeCall(SqlStdOperatorTable.PLUS, ref0, ref1);
        RexCall plusAB3 = (RexCall) rb.makeCall(SqlStdOperatorTable.PLUS, ref0, ref1);
        RexCall minusAB = (RexCall) rb.makeCall(SqlStdOperatorTable.MINUS, ref0, ref1);
        RexCall plusAC = (RexCall) rb.makeCall(SqlStdOperatorTable.PLUS, ref0, ref2);

        // Non-deterministic calls
        RexCall randA1 = (RexCall) rb.makeCall(FlinkSqlOperatorTable.RAND, ref0);
        RexCall randA2 = (RexCall) rb.makeCall(FlinkSqlOperatorTable.RAND, ref0);
        RexCall randB = (RexCall) rb.makeCall(FlinkSqlOperatorTable.RAND, ref1);

        // Nested deterministic calls
        RexNode innerPlus = rb.makeCall(SqlStdOperatorTable.PLUS, ref0, ref1);
        RexCall nestedCall1 = (RexCall) rb.makeCall(SqlStdOperatorTable.PLUS, innerPlus, innerPlus);
        RexCall nestedCall2 = (RexCall) rb.makeCall(SqlStdOperatorTable.PLUS, innerPlus, innerPlus);

        return Stream.of(
                Arguments.of(
                        "identical deterministic calls are deduped",
                        Arrays.asList(plusAB1, plusAB2, plusAB3),
                        1,
                        new int[] {0, 0, 0}),
                Arguments.of(
                        "different deterministic calls are not deduped",
                        Arrays.asList(plusAB1, minusAB),
                        2,
                        new int[] {0, 1}),
                Arguments.of(
                        "non-deterministic calls with different args are not deduped",
                        Arrays.asList(randA1, randB),
                        2,
                        new int[] {0, 1}),
                Arguments.of(
                        "identical non-deterministic calls are not deduped",
                        Arrays.asList(randA1, randA2),
                        2,
                        new int[] {0, 1}),
                Arguments.of(
                        "mixed deterministic and non-deterministic calls",
                        Arrays.asList(plusAB1, randA1, plusAB2),
                        2,
                        new int[] {0, 1, 0}),
                Arguments.of(
                        "nested deterministic calls are deduped",
                        Arrays.asList(nestedCall1, nestedCall2),
                        1,
                        new int[] {0, 0}),
                Arguments.of(
                        "single call - no deduplication needed",
                        Collections.singletonList(plusAB1),
                        1,
                        new int[] {0}),
                Arguments.of(
                        "partial duplicates with different args",
                        Arrays.asList(plusAB1, plusAB2, plusAC),
                        2,
                        new int[] {0, 0, 1}));
    }

    // -------------------------------------------------------------------------
    //  Parameterized tests for buildRefReuseDetailName
    // -------------------------------------------------------------------------

    @ParameterizedTest(name = "{0}")
    @MethodSource("inputForBuildRefReuseDetailName")
    void testBuildRefReuseDetailName(
            String description,
            String[] fieldNames,
            int[] originalToDedup,
            int forwardedCount,
            String expectedDetailName) {
        IntType[] types = new IntType[fieldNames.length];
        Arrays.fill(types, new IntType());
        RowType outputType = RowType.of(types, fieldNames);
        CommonExecPythonCalc calc = new TestPythonCalc(Collections.emptyList(), outputType);

        String result = calc.buildRefReuseDetailName(originalToDedup, forwardedCount);

        assertThat(result).isEqualTo(expectedDetailName);
    }

    static Stream<Arguments> inputForBuildRefReuseDetailName() {
        return Stream.of(
                Arguments.of(
                        "single reuse: EXPR$2 reuses EXPR$1",
                        new String[] {"f1", "EXPR$1", "EXPR$2"},
                        new int[] {0, 0},
                        1,
                        "PythonCalcRefReuse(EXPR$2=EXPR$1)"),
                Arguments.of(
                        "multiple reuses: all reuse EXPR$0",
                        new String[] {"EXPR$0", "EXPR$1", "EXPR$2", "EXPR$3", "EXPR$4"},
                        new int[] {0, 0, 0, 0, 0},
                        0,
                        "PythonCalcRefReuse(EXPR$1=EXPR$0, EXPR$2=EXPR$0, EXPR$3=EXPR$0, EXPR$4=EXPR$0)"),
                Arguments.of(
                        "two groups of reuse",
                        new String[] {"EXPR$0", "EXPR$1", "EXPR$2", "EXPR$3", "EXPR$4"},
                        new int[] {0, 1, 0, 1, 0},
                        0,
                        "PythonCalcRefReuse(EXPR$2=EXPR$0, EXPR$3=EXPR$1, EXPR$4=EXPR$0)"),
                Arguments.of(
                        "no reuse: all calls are different",
                        new String[] {"EXPR$0", "EXPR$1", "EXPR$2"},
                        new int[] {0, 1, 2},
                        0,
                        "PythonCalcRefReuse"),
                Arguments.of(
                        "with forwarded fields: d reuses c",
                        new String[] {"a", "b", "c", "d"},
                        new int[] {0, 0},
                        2,
                        "PythonCalcRefReuse(d=c)"));
    }

    // -------------------------------------------------------------------------
    //  Test helper: minimal concrete subclass of CommonExecPythonCalc
    // -------------------------------------------------------------------------

    /**
     * A minimal concrete subclass of {@link CommonExecPythonCalc} used only for testing the
     * deduplication logic. The abstract methods are not needed for ref-reuse unit tests.
     */
    private static class TestPythonCalc extends CommonExecPythonCalc {

        TestPythonCalc(List<RexNode> projection) {
            this(projection, RowType.of(new IntType()));
        }

        TestPythonCalc(List<RexNode> projection, RowType outputType) {
            super(
                    0,
                    ExecNodeContext.newContext(CommonExecPythonCalc.class),
                    new Configuration(),
                    projection,
                    Collections.singletonList(InputProperty.DEFAULT),
                    outputType,
                    "TestPythonCalc");
        }

        @Override
        protected org.apache.flink.api.dag.Transformation<org.apache.flink.table.data.RowData>
                translateToPlanInternal(
                        org.apache.flink.table.planner.delegation.PlannerBase planner,
                        org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig config) {
            throw new UnsupportedOperationException("Not needed for ref-reuse unit tests");
        }
    }
}
