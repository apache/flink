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
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.functions.python.PythonFunctionInput;
import org.apache.flink.table.functions.python.ResultRef;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecPythonCalc;
import org.apache.flink.table.planner.plan.utils.PythonUtil;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.NonDeterministicPythonScalarFunction;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.utils.JavaScalaConversionUtil.toScala;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the invariants of nested Python UDF flattening, driven through the real SQL pipeline so
 * that genuine Python {@link RexCall}s are used.
 *
 * <p>Flattening happens while translating the ExecNode into a transformation and is therefore not
 * visible in the optimized plan, so these invariants are asserted here rather than in a plan test.
 *
 * <p>The central invariant is asserted on what is actually sent to the worker: the worker evaluates
 * the list sequentially and reads earlier results by index, so every {@link ResultRef} emitted for
 * entry {@code i}, at whichever nesting depth, must point at an entry before {@code i}. The
 * reference map alone cannot tell, because a sub-expression that is evaluated inline as part of a
 * non-flattened subtree must not use it even when a structurally equal entry exists.
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
        util.addTemporarySystemFunction(
                "nonDet1", new NonDeterministicPythonScalarFunction("nonDet1"));
        util.addTemporarySystemFunction(
                "nonDet2", new NonDeterministicPythonScalarFunction("nonDet2"));
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
                .filter(Objects::nonNull)
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

    /** Serializes the evaluated list exactly as {@code CommonExecPythonCalc} does. */
    private static List<PythonFunctionInfo> serialize(PythonCallCseResult result) {
        Map<RexNode, Integer> inputNodes = new LinkedHashMap<>();
        List<PythonFunctionInfo> serialized = new ArrayList<>();
        for (RexCall call : result.getDeduplicatedCalls()) {
            serialized.add(
                    CommonPythonUtil.createPythonFunctionInfo(
                            call,
                            inputNodes,
                            Thread.currentThread().getContextClassLoader(),
                            result.getRefMap()));
        }
        return serialized;
    }

    /**
     * Every reference emitted for an entry, at whichever nesting depth, must point at an entry
     * evaluated before it. Anything below a non-flattened subtree is evaluated inline together with
     * the entry, so a reference from there to a later entry would read a result that does not exist
     * yet.
     */
    private static void assertReferencesPointBackwards(List<PythonFunctionInfo> serialized) {
        for (int i = 0; i < serialized.size(); i++) {
            assertReferencesPointBefore(serialized.get(i), i);
        }
    }

    private static void assertReferencesPointBefore(PythonFunctionInfo info, int entryIndex) {
        for (PythonFunctionInput input : info.getInputs()) {
            if (input instanceof ResultRef) {
                assertThat(((ResultRef) input).getIndex())
                        .as(
                                "entry %d reads results[%d], which is not computed yet",
                                entryIndex, ((ResultRef) input).getIndex())
                        .isLessThan(entryIndex);
            } else if (input instanceof PythonFunctionInfo) {
                assertReferencesPointBefore((PythonFunctionInfo) input, entryIndex);
            }
        }
    }

    /**
     * A non-deterministic call must be evaluated once per occurrence. Its own parent may read its
     * result, but no other place may: each non-deterministic entry is therefore referenced at most
     * once, and only by the entry containing that very occurrence.
     */
    private static void assertNonDeterministicEntriesAreNotShared(
            PythonCallCseResult result, List<PythonFunctionInfo> serialized) {
        List<RexCall> calls = result.getDeduplicatedCalls();
        Map<Integer, Long> referencesPerEntry =
                serialized.stream()
                        .flatMap(info -> collectReferences(info).stream())
                        .collect(Collectors.groupingBy(ResultRef::getIndex, Collectors.counting()));
        referencesPerEntry.forEach(
                (index, count) -> {
                    if (!calls.get(index).getOperator().isDeterministic()) {
                        assertThat(count)
                                .as(
                                        "results[%d] holds a non-deterministic call and must be read by its parent only",
                                        index)
                                .isEqualTo(1);
                    }
                });
    }

    private static List<ResultRef> collectReferences(PythonFunctionInfo info) {
        List<ResultRef> refs = new ArrayList<>();
        for (PythonFunctionInput input : info.getInputs()) {
            if (input instanceof ResultRef) {
                refs.add((ResultRef) input);
            } else if (input instanceof PythonFunctionInfo) {
                refs.addAll(collectReferences((PythonFunctionInfo) input));
            }
        }
        return refs;
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
        List<PythonFunctionInfo> serialized = serialize(analysis.result);
        assertReferencesPointBackwards(serialized);
        assertNonDeterministicEntriesAreNotShared(analysis.result, serialized);
        assertNoEntryIsEvaluatedInlineAsWell(analysis.result, serialized);
        assertOutputIndicesSelectTheProjection(analysis);
    }

    /**
     * Every Python call in the query is evaluated exactly once: either as an entry of the list, or
     * inline inside another entry, never both. An entry evaluated inline somewhere else would mean
     * its own result is computed and then thrown away.
     */
    private static void assertNoEntryIsEvaluatedInlineAsWell(
            PythonCallCseResult result, List<PythonFunctionInfo> serialized) {
        List<RexCall> calls = result.getDeduplicatedCalls();
        Map<RexCall, Integer> refMap = result.getRefMap();
        for (int i = 0; i < calls.size(); i++) {
            assertInlineChildrenHaveNoEntry(calls.get(i), serialized.get(i), i, refMap);
        }
    }

    /**
     * Walks the RexCall tree and the serialized tree side by side: a child that has an entry must
     * have been turned into a {@link ResultRef}, and a child evaluated inline must not have one.
     */
    private static void assertInlineChildrenHaveNoEntry(
            RexCall call, PythonFunctionInfo info, int entryIndex, Map<RexCall, Integer> refMap) {
        List<RexCall> pythonChildren = new ArrayList<>();
        for (RexNode operand : call.getOperands()) {
            if (operand instanceof RexCall && PythonUtil.isPythonCall((RexCall) operand)) {
                pythonChildren.add((RexCall) operand);
            }
        }
        List<PythonFunctionInput> pythonInputs = new ArrayList<>();
        for (PythonFunctionInput input : info.getInputs()) {
            if (input instanceof ResultRef || input instanceof PythonFunctionInfo) {
                pythonInputs.add(input);
            }
        }
        assertThat(pythonInputs)
                .as("entry %d: one serialized input per Python child", entryIndex)
                .hasSameSizeAs(pythonChildren);

        for (int k = 0; k < pythonChildren.size(); k++) {
            RexCall child = pythonChildren.get(k);
            PythonFunctionInput input = pythonInputs.get(k);
            Integer entry = refMap.get(child);
            if (input instanceof PythonFunctionInfo) {
                assertThat(entry)
                        .as(
                                "entry %d evaluates %s inline although results[%s] already holds it",
                                entryIndex, child, entry)
                        .isNull();
                assertInlineChildrenHaveNoEntry(
                        child, (PythonFunctionInfo) input, entryIndex, refMap);
            } else {
                assertThat(entry)
                        .as("entry %d references %s, which has no entry", entryIndex, child)
                        .isNotNull()
                        .isEqualTo(((ResultRef) input).getIndex());
            }
        }
    }

    /**
     * Like {@link #check}, and additionally proves that {@code sharedFunction} really is shared: it
     * is evaluated exactly once, and every other place it was written in the query reads that
     * single result instead of evaluating it again.
     */
    private void checkShared(String sql, int expectedEvaluatedCalls, String sharedFunction) {
        Analysis analysis = analyze(sql);
        List<PythonFunctionInfo> serialized = serialize(analysis.result);
        check(sql, expectedEvaluatedCalls);

        List<RexCall> calls = analysis.result.getDeduplicatedCalls();
        long evaluations =
                calls.stream()
                        .filter(call -> call.getOperator().getName().equals(sharedFunction))
                        .count();
        assertThat(evaluations)
                .as("%s must be evaluated exactly once in: %s", sharedFunction, sql)
                .isEqualTo(1);

        // The single evaluation is an entry of the list. Every place the function is still written
        // as a child that gets serialized, at whatever depth, must read that entry through a
        // ResultRef. A child that has an entry of its own is serialized as a reference, so nothing
        // below it is serialized and nothing below it is counted.
        Map<RexCall, Integer> refMap = analysis.result.getRefMap();
        long expectedReferences =
                calls.stream()
                        .mapToLong(entry -> countReferencedChildren(entry, sharedFunction, refMap))
                        .sum();
        long references =
                serialized.stream()
                        .flatMap(info -> collectReferences(info).stream())
                        .filter(
                                ref ->
                                        calls.get(ref.getIndex())
                                                .getOperator()
                                                .getName()
                                                .equals(sharedFunction))
                        .count();
        assertThat(references)
                .as(
                        "every nested occurrence of %s must read the shared result in: %s",
                        sharedFunction, sql)
                .isEqualTo(expectedReferences);
    }

    /**
     * Counts the children of {@code root} that are {@code function} and will be serialized as a
     * reference, descending only into children that are evaluated inline.
     */
    private static int countReferencedChildren(
            RexCall root, String function, Map<RexCall, Integer> refMap) {
        int count = 0;
        for (RexNode operand : root.getOperands()) {
            if (!(operand instanceof RexCall)) {
                continue;
            }
            RexCall child = (RexCall) operand;
            if (refMap.containsKey(child)) {
                if (child.getOperator().getName().equals(function)) {
                    count++;
                }
            } else {
                count += countReferencedChildren(child, function, refMap);
            }
        }
        return count;
    }

    // ---------------------------------------------------------------------------------------------
    // deterministic calls only
    // ---------------------------------------------------------------------------------------------

    @Test
    void testNestedCallWithoutSharingIsNotFlattened() {
        // nothing is shared, so the call tree is left alone and the worker nests it as before
        check("SELECT pyFunc2(pyFunc1(a, b), c) FROM MyTable", 1);
    }

    @Test
    void testSharedInnerCallIsEvaluatedOnce() {
        // pyFunc1(a, b) is both projected and nested, but must be evaluated only once
        checkShared("SELECT pyFunc1(a, b), pyFunc2(pyFunc1(a, b), c) FROM MyTable", 2, "pyFunc1");
    }

    @Test
    void testSharedInnerCallProjectedAfterOuter() {
        // same as above but the outer call comes first, so the reference points backwards only if
        // post-order is preserved
        checkShared("SELECT pyFunc2(pyFunc1(a, b), c), pyFunc1(a, b) FROM MyTable", 2, "pyFunc1");
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
        checkShared(
                "SELECT pyFunc1(a, b), pyFunc2(pyFunc2(pyFunc1(a, b), c), c) FROM MyTable",
                3,
                "pyFunc1");
    }

    @Test
    void testForwardedFieldsAreNotAffected() {
        checkShared(
                "SELECT c, pyFunc1(a, b), pyFunc2(pyFunc1(a, b), c) FROM MyTable", 2, "pyFunc1");
    }

    // ---------------------------------------------------------------------------------------------
    // non-deterministic calls: evaluated once per occurrence, never referenced, but anything
    // deterministic below them takes part in CSE like everywhere else
    // ---------------------------------------------------------------------------------------------

    @Test
    void testNonDeterministicCallIsNotShared() {
        check("SELECT nonDet1(a, b), pyFunc2(nonDet1(a, b), c) FROM MyTable", 2);
    }

    @Test
    void testNonDeterministicCallProjectedAfterItsNestedOccurrence() {
        check("SELECT pyFunc2(nonDet1(a, b), c), nonDet1(a, b) FROM MyTable", 2);
    }

    @Test
    void testTwoNestedOccurrencesOfANonDeterministicCall() {
        check("SELECT pyFunc2(nonDet1(a, b), c), pyFunc3(nonDet1(a, b), c) FROM MyTable", 2);
    }

    @Test
    void testDeterministicCallBelowANonDeterministicOneWithoutSharing() {
        // pyFunc1(a, b) sits below nonDet1 but is not shared with anything, so nothing is flattened
        check("SELECT pyFunc2(nonDet1(pyFunc1(a, b), c), c) FROM MyTable", 1);
    }

    @Test
    void testDeterministicCallBelowANonDeterministicOneIsSharedWithALaterProjection() {
        // The shape from the review. pyFunc1(a, b) below nonDet1 becomes its own entry in
        // post-order, so it is evaluated first and both the projected pyFunc1(a, b) and nonDet1
        // reference it. Leaving it nested inside nonDet1 would instead have made it reference the
        // projected entry, which is evaluated later.
        checkShared(
                "SELECT pyFunc2(nonDet1(pyFunc1(a, b), c), c), pyFunc1(a, b) FROM MyTable",
                3,
                "pyFunc1");
    }

    @Test
    void testDeterministicCallBelowANonDeterministicOneIsSharedWithAnEarlierProjection() {
        checkShared(
                "SELECT pyFunc1(a, b), pyFunc2(nonDet1(pyFunc1(a, b), c), c) FROM MyTable",
                3,
                "pyFunc1");
    }

    @Test
    void testDeterministicCallSharedAboveAndBelowANonDeterministicOne() {
        // pyFunc1(a, b) appears three times: projected, below nonDet1 and as a direct operand of
        // pyFunc3. All three share one evaluation.
        checkShared(
                "SELECT pyFunc1(a, b), pyFunc3(nonDet1(pyFunc1(a, b), c), pyFunc1(a, b)) FROM MyTable",
                3,
                "pyFunc1");
    }

    @Test
    void testTwoDifferentNonDeterministicCalls() {
        check("SELECT pyFunc2(nonDet1(a, b), c), pyFunc2(nonDet2(a, b), c) FROM MyTable", 2);
    }

    @Test
    void testNonDeterministicCallIsEvaluatedOncePerOccurrenceEvenWhenNested() {
        // both projections are structurally equal but non-deterministic through their subtree, so
        // the rules from #28638 leave both in place and nothing is shared between them
        check("SELECT pyFunc2(nonDet1(a, b), c), pyFunc2(nonDet1(a, b), c) FROM MyTable", 2);
    }

    // ---------------------------------------------------------------------------------------------
    // more involved shapes
    // ---------------------------------------------------------------------------------------------

    @Test
    void testBothOperandsOfOneCallAreTheSameSharedCall() {
        // both operands of pyFunc3 must become references to the single pyFunc1 entry
        checkShared(
                "SELECT pyFunc1(a, b), pyFunc3(pyFunc1(a, b), pyFunc1(a, b)) FROM MyTable",
                2,
                "pyFunc1");
    }

    @Test
    void testBothOperandsOfOneCallAreSharedWithDifferentProjections() {
        // two independent sharing chains through the same parent; the references must not cross
        Analysis analysis =
                analyze(
                        "SELECT pyFunc1(a, b), pyFunc2(b, c), pyFunc3(pyFunc1(a, b), pyFunc2(b, c)) FROM MyTable");
        check(
                "SELECT pyFunc1(a, b), pyFunc2(b, c), pyFunc3(pyFunc1(a, b), pyFunc2(b, c)) FROM MyTable",
                3);
        List<PythonFunctionInfo> serialized = serialize(analysis.result);
        PythonFunctionInfo pyFunc3 = serialized.get(2);
        assertThat(pyFunc3.getInputs()).hasSize(2);
        assertThat(((ResultRef) pyFunc3.getInputs()[0]).getIndex()).isEqualTo(0);
        assertThat(((ResultRef) pyFunc3.getInputs()[1]).getIndex()).isEqualTo(1);
    }

    @Test
    void testSharedCallAppearsAtThreeDifferentDepths() {
        // projected, one level down, and two levels down: all read the same entry
        checkShared(
                "SELECT pyFunc1(a, b), pyFunc2(pyFunc1(a, b), c), pyFunc3(pyFunc2(pyFunc1(a, b), c), c) FROM MyTable",
                3,
                "pyFunc1");
    }

    @Test
    void testTwoLayersOfDeterministicCallsBelowANonDeterministicOne() {
        // nonDet1 sits above pyFunc3, which sits above the shared pyFunc1. pyFunc3 is unique but
        // has its own entry after flattening, so nonDet1 must reference it rather than evaluate it
        // inline, and pyFunc3 in turn references pyFunc1.
        checkShared(
                "SELECT pyFunc2(nonDet1(pyFunc3(pyFunc1(a, b), c), c), c), pyFunc1(a, b) FROM MyTable",
                4,
                "pyFunc1");
    }

    @Test
    void testCallContainingANonDeterministicOneIsNotShared() {
        // pyFunc2(nonDet1(a, b), c) is non-deterministic through its subtree, so its two
        // occurrences are kept apart and nothing at all is merged. The trees are therefore left as
        // they are and each occurrence is evaluated inline with its own nonDet1.
        check(
                "SELECT pyFunc2(nonDet1(a, b), c), pyFunc3(pyFunc2(nonDet1(a, b), c), c) FROM MyTable",
                2);
    }

    @Test
    void testCallContainingANonDeterministicOneHasItsOwnEntryOncePerOccurrenceWhenFlattened() {
        // Same as above, but the shared pyFunc1 forces flattening. Both pyFunc2(nonDet1(..)) now
        // have an entry of their own and are referenced by identity: the projection reads the
        // first, pyFunc3 reads the second, and each keeps its own nonDet1 entry.
        String sql =
                "SELECT pyFunc1(a, b), pyFunc2(nonDet1(pyFunc1(a, b), b), c), pyFunc3(pyFunc2(nonDet1(pyFunc1(a, b), b), c), c) FROM MyTable";
        checkShared(sql, 6, "pyFunc1");
        Analysis analysis = analyze(sql);
        List<RexCall> calls = analysis.result.getDeduplicatedCalls();
        long nonDetEntries =
                calls.stream().filter(c -> c.getOperator().getName().equals("nonDet1")).count();
        long pyFunc2Entries =
                calls.stream().filter(c -> c.getOperator().getName().equals("pyFunc2")).count();
        assertThat(nonDetEntries).as("one nonDet1 entry per occurrence").isEqualTo(2);
        assertThat(pyFunc2Entries)
                .as("pyFunc2 is non-deterministic through nonDet1, so one entry per occurrence")
                .isEqualTo(2);
    }

    @Test
    void testNonDeterministicCallNestedInAnotherNonDeterministicCall() {
        // two non-deterministic entries, each resolved by identity; nonDet2 references nonDet1
        check("SELECT nonDet2(nonDet1(a, b), c) FROM MyTable", 1);
        check("SELECT nonDet2(nonDet1(a, b), c), pyFunc1(a, b) FROM MyTable", 2);
        checkShared(
                "SELECT nonDet2(nonDet1(pyFunc1(a, b), c), c), pyFunc1(a, b) FROM MyTable",
                3,
                "pyFunc1");
    }

    @Test
    void testNonDeterministicProjectionWithASharedChild() {
        // the simplest shape of the review case: nonDet1 is itself the projected call
        checkShared("SELECT nonDet1(pyFunc1(a, b), c), pyFunc1(a, b) FROM MyTable", 2, "pyFunc1");
        checkShared("SELECT pyFunc1(a, b), nonDet1(pyFunc1(a, b), c) FROM MyTable", 2, "pyFunc1");
    }
}
