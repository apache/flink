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

package org.apache.flink.table.planner.plan.nodes.exec.utils;

import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecPythonCalc;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;
import org.apache.flink.table.planner.utils.TableTestUtil;
import org.apache.flink.table.types.logical.RowType;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the invariants of nested Python UDF flattening, driven through the real SQL pipeline so
 * that genuine Python {@link RexCall}s are used.
 *
 * <p>Flattening happens while translating the ExecNode into a transformation and is therefore not
 * visible in the optimized plan, so these invariants are asserted here rather than in a plan test.
 */
class PythonCalcNestedCseInvariantTest extends TableTestBase {

    private StreamTableTestUtil util;

    @BeforeEach
    void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        util.addTable(
                "CREATE TEMPORARY TABLE MyTable (\n"
                        + "  a INT,\n"
                        + "  b INT,\n"
                        + "  c INT\n"
                        + ") WITH (\n"
                        + "  'connector' = 'values'\n"
                        + ")");
        util.addTemporarySystemFunction("pyFunc1", new PythonScalarFunction("pyFunc1"));
        util.addTemporarySystemFunction("pyFunc2", new PythonScalarFunction("pyFunc2"));
        util.addTemporarySystemFunction("pyFunc3", new PythonScalarFunction("pyFunc3"));
    }

    /** The projection of the single PythonCalc of a query, together with its CSE result. */
    private static final class Analysis {
        private final int forwardedFields;
        private final int outputWidth;
        private final PythonCallCseResult result;
        private final List<RexCall> topLevelCalls;

        private Analysis(
                int forwardedFields,
                int outputWidth,
                PythonCallCseResult result,
                List<RexCall> topLevelCalls) {
            this.forwardedFields = forwardedFields;
            this.outputWidth = outputWidth;
            this.result = result;
            this.topLevelCalls = topLevelCalls;
        }
    }

    private Analysis analyze(String sql) {
        Table table = util.tableEnv().sqlQuery(sql);
        RelNode optimized = util.getPlanner().optimize(TableTestUtil.toRelNode(table));
        ExecNodeGraph graph =
                util.getPlanner()
                        .translateToExecNodeGraph(
                                toScala(Collections.singletonList(optimized)), false);

        CommonExecPythonCalc calc = findPythonCalc(graph);
        assertThat(calc).as("no PythonCalc found for: %s", sql).isNotNull();

        List<RexNode> projection = readProjection(calc);
        List<RexCall> topLevelCalls = new ArrayList<>();
        int forwardedFields = 0;
        for (RexNode node : projection) {
            if (node instanceof RexCall) {
                topLevelCalls.add((RexCall) node);
            } else {
                forwardedFields++;
            }
        }
        int outputWidth = ((RowType) calc.getOutputType()).getFieldCount();
        return new Analysis(
                forwardedFields,
                outputWidth,
                PythonCallDeduplicator.deduplicate(topLevelCalls),
                topLevelCalls);
    }

    private static CommonExecPythonCalc findPythonCalc(ExecNodeGraph graph) {
        for (ExecNode<?> root : graph.getRootNodes()) {
            CommonExecPythonCalc found = findPythonCalc(root);
            if (found != null) {
                return found;
            }
        }
        return null;
    }

    private static CommonExecPythonCalc findPythonCalc(ExecNode<?> node) {
        if (node instanceof CommonExecPythonCalc) {
            return (CommonExecPythonCalc) node;
        }
        return node.getInputEdges().stream()
                .map(edge -> findPythonCalc(edge.getSource()))
                .filter(java.util.Objects::nonNull)
                .findFirst()
                .orElse(null);
    }

    @SuppressWarnings("unchecked")
    private static List<RexNode> readProjection(CommonExecPythonCalc calc) {
        try {
            Field field = CommonExecPythonCalc.class.getDeclaredField("projection");
            field.setAccessible(true);
            return (List<RexNode>) field.get(calc);
        } catch (ReflectiveOperationException e) {
            throw new AssertionError("could not read the projection of the PythonCalc", e);
        }
    }

    /**
     * The worker evaluates the list sequentially and reads earlier results by index, so a
     * referenced sub-expression must always be evaluated before the call referencing it.
     */
    private static void assertReferencesResolveBackwards(PythonCallCseResult result) {
        List<RexCall> calls = result.getDeduplicatedCalls();
        Map<RexCall, Integer> refMap = result.getRefMap();
        for (int i = 0; i < calls.size(); i++) {
            for (RexNode operand : calls.get(i).getOperands()) {
                if (operand instanceof RexCall) {
                    Integer referenced = refMap.get(operand);
                    if (referenced != null) {
                        assertThat(referenced)
                                .as(
                                        "call %d references results[%d], which must be computed earlier",
                                        i, referenced)
                                .isLessThan(i);
                    }
                }
            }
        }
    }

    /** The projected results must be exactly as wide as the operator's UDF output columns. */
    private static void assertOutputIndicesSelectTheProjection(Analysis analysis) {
        List<RexCall> calls = analysis.result.getDeduplicatedCalls();
        int[] outputIndices = analysis.result.getOutputIndices();

        assertThat(outputIndices.length)
                .as("one output index per projected call")
                .isEqualTo(analysis.topLevelCalls.size());
        assertThat(analysis.outputWidth - analysis.forwardedFields)
                .as("the operator output must have one column per projected call")
                .isEqualTo(outputIndices.length);
        for (int i = 0; i < outputIndices.length; i++) {
            assertThat(calls.get(outputIndices[i]))
                    .as("output index %d must point at the projected call", i)
                    .isEqualTo(analysis.topLevelCalls.get(i));
        }
    }

    private void check(String sql, int expectedEvaluatedCalls) {
        Analysis analysis = analyze(sql);
        assertThat(analysis.result.getDeduplicatedCalls())
                .as("number of evaluated calls for: %s", sql)
                .hasSize(expectedEvaluatedCalls);
        assertReferencesResolveBackwards(analysis.result);
        assertOutputIndicesSelectTheProjection(analysis);
    }

    @Test
    void testNestedCallWithoutSharingIsNotFlattened() {
        // nothing is shared, so the call tree is left alone and the worker nests it as before
        check("SELECT pyFunc2(pyFunc1(a, b), c) FROM MyTable", 1);
    }

    @Test
    void testSharedInnerCallIsEvaluatedOnce() {
        // pyFunc1(a, b) is both projected and nested, but must be evaluated only once
        check("SELECT pyFunc1(a, b), pyFunc2(pyFunc1(a, b), c) FROM MyTable", 2);
    }

    @Test
    void testSharedInnerCallProjectedAfterOuter() {
        // same as above but the outer call comes first, so the reference points backwards only if
        // post-order is preserved
        check("SELECT pyFunc2(pyFunc1(a, b), c), pyFunc1(a, b) FROM MyTable", 2);
    }

    @Test
    void testTwoDistinctNestedCallsAreNotFlattened() {
        check("SELECT pyFunc3(pyFunc1(a, b), pyFunc2(b, c)), c FROM MyTable", 1);
    }

    @Test
    void testDeeplyNestedCallsWithoutSharingAreNotFlattened() {
        check("SELECT pyFunc2(pyFunc2(pyFunc1(a, b), c), c) FROM MyTable", 1);
    }

    @Test
    void testSharedDeeplyNestedCall() {
        // the innermost call is shared with a projection, so flattening pays off
        check("SELECT pyFunc1(a, b), pyFunc2(pyFunc2(pyFunc1(a, b), c), c) FROM MyTable", 3);
    }

    @Test
    void testForwardedFieldsAreNotAffected() {
        check("SELECT c, pyFunc1(a, b), pyFunc2(pyFunc1(a, b), c) FROM MyTable", 2);
    }
}
