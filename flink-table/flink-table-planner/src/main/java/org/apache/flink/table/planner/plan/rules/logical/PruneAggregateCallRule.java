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

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.Aggregate.Group;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** Planner rule that removes unreferenced AggregateCall from Aggregate. */
public abstract class PruneAggregateCallRule<T extends RelNode>
        extends RelRule<PruneAggregateCallRule.PruneAggregateCallRuleConfig> {

    public static final ProjectPruneAggregateCallRule PROJECT_ON_AGGREGATE =
            ProjectPruneAggregateCallRule.ProjectPruneAggregateCallRuleConfig.DEFAULT.toRule();
    public static final CalcPruneAggregateCallRule CALC_ON_AGGREGATE =
            CalcPruneAggregateCallRule.CalcPruneAggregateCallRuleConfig.DEFAULT.toRule();

    protected PruneAggregateCallRule(PruneAggregateCallRule.PruneAggregateCallRuleConfig config) {
        super(config);
    }

    protected abstract ImmutableBitSet getInputRefs(T relOnAgg);

    @Override
    public boolean matches(RelOptRuleCall call) {
        T relOnAgg = call.rel(0);
        Aggregate agg = call.rel(1);
        if (agg.getGroupType() != Group.SIMPLE
                || agg.getAggCallList().isEmpty()
                ||
                // at least output one column
                (agg.getGroupCount() == 0 && agg.getAggCallList().size() == 1)) {
            return false;
        }
        ImmutableBitSet inputRefs = getInputRefs(relOnAgg);
        int[] unrefAggCallIndices = getUnrefAggCallIndices(inputRefs, agg);
        return unrefAggCallIndices.length > 0;
    }

    private int[] getUnrefAggCallIndices(ImmutableBitSet inputRefs, Aggregate agg) {
        int groupCount = agg.getGroupCount();
        return IntStream.range(0, agg.getAggCallList().size())
                .filter(index -> !inputRefs.get(groupCount + index))
                .toArray();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        T relOnAgg = call.rel(0);
        Aggregate agg = call.rel(1);
        ImmutableBitSet inputRefs = getInputRefs(relOnAgg);
        int[] unrefAggCallIndices = getUnrefAggCallIndices(inputRefs, agg);
        Preconditions.checkArgument(unrefAggCallIndices.length > 0, "requirement failed");

        List<AggregateCall> newAggCalls = new ArrayList<>(agg.getAggCallList());
        // remove unreferenced AggCall from original aggCalls
        Arrays.stream(unrefAggCallIndices)
                .boxed()
                .sorted(Comparator.reverseOrder())
                .forEach(index -> newAggCalls.remove((int) index));

        if (newAggCalls.isEmpty() && agg.getGroupCount() == 0) {
            // at least output one column
            newAggCalls.add(agg.getAggCallList().get(0));
            unrefAggCallIndices =
                    Arrays.copyOfRange(unrefAggCallIndices, 1, unrefAggCallIndices.length);
        }

        Aggregate newAgg =
                agg.copy(
                        agg.getTraitSet(),
                        agg.getInput(),
                        agg.getGroupSet(),
                        List.of(agg.getGroupSet()),
                        newAggCalls);

        int newFieldIndex = 0;
        // map old agg output index to new agg output index
        Map<Integer, Integer> mapOldToNew = new HashMap<>();
        int fieldCountOfOldAgg = agg.getRowType().getFieldCount();
        List<Integer> unrefAggCallOutputIndices =
                Arrays.stream(unrefAggCallIndices)
                        .mapToObj(i -> i + agg.getGroupCount())
                        .collect(Collectors.toList());
        for (int i = 0; i < fieldCountOfOldAgg; i++) {
            if (!unrefAggCallOutputIndices.contains(i)) {
                mapOldToNew.put(i, newFieldIndex);
                newFieldIndex++;
            }
        }
        Preconditions.checkArgument(
                mapOldToNew.size() == newAgg.getRowType().getFieldCount(), "requirement failed");

        Mappings.TargetMapping mapping =
                Mappings.target(
                        mapOldToNew, fieldCountOfOldAgg, newAgg.getRowType().getFieldCount());
        RelNode newRelOnAgg = createNewRel(mapping, relOnAgg, newAgg);
        call.transformTo(newRelOnAgg);
    }

    protected abstract RelNode createNewRel(
            Mappings.TargetMapping mapping, T project, RelNode newAgg);

    public static class ProjectPruneAggregateCallRule extends PruneAggregateCallRule<Project> {

        protected ProjectPruneAggregateCallRule(ProjectPruneAggregateCallRuleConfig config) {
            super(config);
        }

        @Override
        protected ImmutableBitSet getInputRefs(Project relOnAgg) {
            return RelOptUtil.InputFinder.bits(relOnAgg.getProjects(), null);
        }

        @Override
        protected RelNode createNewRel(
                Mappings.TargetMapping mapping, Project project, RelNode newAgg) {
            List<RexNode> newProjects = RexUtil.apply(mapping, project.getProjects());
            if (projectsOnlyIdentity(newProjects, newAgg.getRowType().getFieldCount())
                    && Utilities.compare(
                                    project.getRowType().getFieldNames(),
                                    newAgg.getRowType().getFieldNames())
                            == 0) {
                return newAgg;
            } else {
                return project.copy(
                        project.getTraitSet(), newAgg, newProjects, project.getRowType());
            }
        }

        private boolean projectsOnlyIdentity(List<RexNode> projects, int inputFieldCount) {
            if (projects.size() != inputFieldCount) {
                return false;
            }
            return IntStream.range(0, projects.size())
                    .allMatch(
                            index -> {
                                RexNode project = projects.get(index);
                                if (project instanceof RexInputRef) {
                                    RexInputRef r = (RexInputRef) project;
                                    return r.getIndex() == index;
                                }
                                return false;
                            });
        }

        /** Rule configuration. */
        @Value.Immutable(singleton = false)
        public interface ProjectPruneAggregateCallRuleConfig
                extends PruneAggregateCallRule.PruneAggregateCallRuleConfig {
            ProjectPruneAggregateCallRuleConfig DEFAULT =
                    ImmutableProjectPruneAggregateCallRuleConfig.builder()
                            .build()
                            .withOperandSupplier(
                                    b0 ->
                                            b0.operand(Project.class)
                                                    .oneInput(
                                                            b1 ->
                                                                    b1.operand(Aggregate.class)
                                                                            .anyInputs()))
                            .withDescription(
                                    "PruneAggregateCallRule_" + Project.class.getCanonicalName());

            @Override
            default ProjectPruneAggregateCallRule toRule() {
                return new ProjectPruneAggregateCallRule(this);
            }
        }
    }

    public static class CalcPruneAggregateCallRule extends PruneAggregateCallRule<Calc> {

        protected CalcPruneAggregateCallRule(CalcPruneAggregateCallRuleConfig config) {
            super(config);
        }

        @Override
        protected ImmutableBitSet getInputRefs(Calc calc) {
            RexProgram program = calc.getProgram();
            RexNode condition =
                    program.getCondition() != null
                            ? program.expandLocalRef(program.getCondition())
                            : null;
            List<RexNode> projects =
                    program.getProjectList().stream()
                            .map(program::expandLocalRef)
                            .collect(Collectors.toList());
            return RelOptUtil.InputFinder.bits(projects, condition);
        }

        @Override
        protected RelNode createNewRel(Mappings.TargetMapping mapping, Calc calc, RelNode newAgg) {
            RexProgram program = calc.getProgram();
            RexNode newCondition =
                    program.getCondition() != null
                            ? RexUtil.apply(mapping, program.expandLocalRef(program.getCondition()))
                            : null;
            List<RexNode> projects =
                    program.getProjectList().stream()
                            .map(program::expandLocalRef)
                            .collect(Collectors.toList());
            List<RexNode> newProjects = RexUtil.apply(mapping, projects);
            RexProgram newProgram =
                    RexProgram.create(
                            newAgg.getRowType(),
                            newProjects,
                            newCondition,
                            program.getOutputRowType().getFieldNames(),
                            calc.getCluster().getRexBuilder());
            if (newProgram.isTrivial()
                    && Utilities.compare(
                                    calc.getRowType().getFieldNames(),
                                    newAgg.getRowType().getFieldNames())
                            == 0) {
                return newAgg;
            } else {
                return calc.copy(calc.getTraitSet(), newAgg, newProgram);
            }
        }

        /** Rule configuration. */
        @Value.Immutable(singleton = false)
        public interface CalcPruneAggregateCallRuleConfig extends PruneAggregateCallRuleConfig {
            CalcPruneAggregateCallRuleConfig DEFAULT =
                    ImmutableCalcPruneAggregateCallRuleConfig.builder()
                            .build()
                            .withOperandSupplier(
                                    b0 ->
                                            b0.operand(Calc.class)
                                                    .oneInput(
                                                            b1 ->
                                                                    b1.operand(Aggregate.class)
                                                                            .anyInputs()))
                            .withDescription(
                                    "PruneAggregateCallRule_" + Calc.class.getCanonicalName());

            @Override
            default CalcPruneAggregateCallRule toRule() {
                return new CalcPruneAggregateCallRule(this);
            }
        }
    }

    /** Rule configuration. */
    public interface PruneAggregateCallRuleConfig extends RelRule.Config {
        @Override
        PruneAggregateCallRule toRule();
    }
}
