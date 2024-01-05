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

package org.apache.flink.table.planner.plan.utils;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/** Utilities for {@link RelNode}. */
public class FlinkRelUtil {

    /**
     * Return two neighbouring {@link Filter} and {@link Calc} can merge into one {@link Calc} or
     * not. If the two nodes can merge into one, each non-deterministic {@link RexNode} of bottom
     * {@link Calc} should appear at most once in the implicit project list and condition of top
     * {@link Filter}.
     */
    public static boolean isMergeable(Filter topFilter, Calc bottomCalc) {
        final RelDataType inputRowType = topFilter.getInput().getRowType();
        final int inputFieldCnt = inputRowType.getFieldCount();
        final int[] topInputRefCounter = initializeArray(inputFieldCnt, 0);
        final RexProgram bottomProgram = bottomCalc.getProgram();
        List<RexNode> topProjects = new ArrayList<>();
        topProjects.add(topFilter.getCondition());
        // the Filter node implicitly has a projection of input row
        for (int i = 0; i < inputFieldCnt; i++) {
            topProjects.add(new RexInputRef(i, inputRowType.getFieldList().get(i).getType()));
        }
        List<RexNode> bottomProjects =
                bottomProgram.getProjectList().stream()
                        .map(bottomProgram::expandLocalRef)
                        .collect(Collectors.toList());

        return mergeable(topInputRefCounter, topProjects, bottomProjects);
    }

    /**
     * Return two neighbouring {@link Project} and {@link Calc} can merge into one {@link Calc} or
     * not. If the two nodes can merge into one, each non-deterministic {@link RexNode} of bottom
     * {@link Calc} should appear at most once in the project list of top {@link Project}.
     */
    public static boolean isMergeable(Project topProject, Calc bottomCalc) {
        final int[] topInputRefCounter =
                initializeArray(topProject.getInput().getRowType().getFieldCount(), 0);
        final RexProgram bottomProgram = bottomCalc.getProgram();
        List<RexNode> bottomProjects =
                bottomProgram.getProjectList().stream()
                        .map(bottomProgram::expandLocalRef)
                        .collect(Collectors.toList());

        return mergeable(topInputRefCounter, topProject.getProjects(), bottomProjects);
    }

    /**
     * Return two neighbouring {@link Project} can merge into one {@link Project} or not. If the two
     * {@link Project} can merge into one, each non-deterministic {@link RexNode} of bottom {@link
     * Project} should appear at most once in the project list of top {@link Project}.
     */
    public static boolean isMergeable(Project topProject, Project bottomProject) {
        final int[] topInputRefCounter =
                initializeArray(topProject.getInput().getRowType().getFieldCount(), 0);

        return mergeable(topInputRefCounter, topProject.getProjects(), bottomProject.getProjects());
    }

    /**
     * Return two neighbouring {@link Calc} can merge into one {@link Calc} or not. If the two
     * {@link Calc} can merge into one, each non-deterministic {@link RexNode} of bottom {@link
     * Calc} should appear at most once in the project list of top {@link Calc}.
     */
    public static boolean isMergeable(Calc topCalc, Calc bottomCalc) {
        final RexProgram topProgram = topCalc.getProgram();
        final RexProgram bottomProgram = bottomCalc.getProgram();
        final int[] topInputRefCounter =
                initializeArray(topCalc.getInput().getRowType().getFieldCount(), 0);

        List<RexNode> topInputRefs =
                topProgram.getProjectList().stream()
                        .map(topProgram::expandLocalRef)
                        .collect(Collectors.toList());
        List<RexNode> bottomProjects =
                bottomProgram.getProjectList().stream()
                        .map(bottomProgram::expandLocalRef)
                        .collect(Collectors.toList());
        if (null != topProgram.getCondition()) {
            topInputRefs.add(topProgram.expandLocalRef(topProgram.getCondition()));
        }

        return mergeable(topInputRefCounter, topInputRefs, bottomProjects);
    }

    /**
     * Merges the programs of two {@link Calc} instances and returns a new {@link Calc} instance
     * with the merged program.
     */
    public static Calc merge(Calc topCalc, Calc bottomCalc) {
        RexProgram topProgram = topCalc.getProgram();
        RexBuilder rexBuilder = topCalc.getCluster().getRexBuilder();

        // Merge the programs together.
        RexProgram mergedProgram =
                RexProgramBuilder.mergePrograms(topProgram, bottomCalc.getProgram(), rexBuilder);
        if (!mergedProgram.getOutputRowType().equals(topProgram.getOutputRowType())) {
            throw new IllegalArgumentException(
                    "Output row type of merged program is not the same top program.");
        }

        RexProgram newMergedProgram;
        if (mergedProgram.getCondition() != null) {
            RexNode condition = mergedProgram.expandLocalRef(mergedProgram.getCondition());
            RexNode simplifiedCondition =
                    FlinkRexUtil.simplify(
                            rexBuilder, condition, topCalc.getCluster().getPlanner().getExecutor());
            if (simplifiedCondition.equals(condition)) {
                newMergedProgram = mergedProgram;
            } else {
                RexProgramBuilder programBuilder =
                        RexProgramBuilder.forProgram(mergedProgram, rexBuilder, true);
                programBuilder.clearCondition();
                programBuilder.addCondition(simplifiedCondition);
                newMergedProgram = programBuilder.getProgram(true);
            }
        } else {
            newMergedProgram = mergedProgram;
        }

        return topCalc.copy(topCalc.getTraitSet(), bottomCalc.getInput(), newMergedProgram);
    }

    /**
     * Returns an int array with given length and initial value.
     *
     * @param length array length
     * @param initVal initial value
     * @return initialized int array
     */
    public static int[] initializeArray(int length, int initVal) {
        final int[] array = new int[length];
        Arrays.fill(array, initVal);
        return array;
    }

    /**
     * An InputRefCounter that count every inputRef's reference count number, every reference will
     * be counted, e.g., '$0 + 1' & '$0 + 2' will count 2 instead of 1.
     */
    private static class InputRefCounter extends RexVisitorImpl<Void> {
        final int[] refCounts;

        public InputRefCounter(boolean deep, int[] refCounts) {
            super(deep);
            this.refCounts = refCounts;
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
            final int index = inputRef.getIndex();
            refCounts[index]++;
            return null;
        }
    }

    /** The internal reusable method for filter, project nd calc. */
    private static boolean mergeable(
            int[] topInputRefCounter, List<RexNode> topProjects, List<RexNode> bottomProjects) {
        RexUtil.apply(new InputRefCounter(true, topInputRefCounter), topProjects, null);

        boolean mergeable = true;
        for (int idx = 0; idx < bottomProjects.size(); idx++) {
            RexNode node = bottomProjects.get(idx);

            if (!RexUtil.isDeterministic(node)) {
                assert idx < topInputRefCounter.length;
                if (topInputRefCounter[idx] > 1) {
                    mergeable = false;
                    break;
                }
            }
        }

        if (!mergeable) {
            return false;
        }

        // here, we iterate each expression among topProjects and check in the end if at least two
        // expressions from topProjects access the same expression from the bottomProjects.
        // If this is the case, if the accessed expression is an expression formed by a call to an
        // operator with zero or more expressions as operands (i.e., RexCall) we avoid merging
        // project expressions. This enables us to void merging project expressions that
        // lead to redundant computation.
        HashMap<Integer, Integer> indexAccessFrequencies = new HashMap<>();
        for (RexNode node : topProjects) {
            FieldAccessesVisitor visitor = new FieldAccessesVisitor();
            node.accept(visitor);
            HashMap<Integer, Integer> currentAccessFrequency = visitor.getIndexAccessFrequency();
            currentAccessFrequency.forEach(
                    (key, value) ->
                            indexAccessFrequencies.merge(
                                    key,
                                    value,
                                    (existingValue, newValue) -> existingValue + newValue));
        }

        boolean multipleFieldAccessExists =
                indexAccessFrequencies.entrySet().stream()
                        .anyMatch(
                                entry -> {
                                    Integer ind = entry.getKey();
                                    return (bottomProjects.get(ind) instanceof RexCall)
                                            && entry.getValue() > 1;
                                });
        return !multipleFieldAccessExists;
    }

    /**
     * This class visits a project (RexNode) and saves access frequencies of each input ref. The
     * field access frequency is updated only if the input reference is part of a RexCall. The
     * reason is that we want to avoid redundant computation (RexCall) and not just field accesses
     * (RexInputRef).
     */
    public static class FieldAccessesVisitor extends RexVisitorImpl<Void> {

        private HashMap<Integer, Integer> indexAccessFrequency = new HashMap<>();
        private boolean insideRexCallScope = false;

        protected FieldAccessesVisitor() {
            super(true);
        }

        @Override
        public Void visitCall(RexCall call) {
            insideRexCallScope = true;
            Void res = super.visitCall(call);
            insideRexCallScope = false;
            return res;
        }

        @Override
        public Void visitInputRef(RexInputRef inputRef) {
            if (insideRexCallScope) {
                int index = inputRef.getIndex();
                indexAccessFrequency.compute(index, (ind, freq) -> freq == null ? 1 : freq + 1);
            }
            return super.visitInputRef(inputRef);
        }

        public HashMap<Integer, Integer> getIndexAccessFrequency() {
            return indexAccessFrequency;
        }
    }
}
