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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mappings;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that pushes a {@link Project} down in a tree past a semi/anti {@link Join} by
 * splitting the projection into a projection on top of left child of the Join.
 */
@Value.Enclosing
public class ProjectSemiAntiJoinTransposeRule
        extends RelRule<ProjectSemiAntiJoinTransposeRule.ProjectSemiAntiJoinTransposeRuleConfig> {

    public static final ProjectSemiAntiJoinTransposeRule INSTANCE =
            ProjectSemiAntiJoinTransposeRule.ProjectSemiAntiJoinTransposeRuleConfig.DEFAULT
                    .toRule();

    private ProjectSemiAntiJoinTransposeRule(ProjectSemiAntiJoinTransposeRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(1);
        JoinRelType joinType = join.getJoinType();
        return joinType == JoinRelType.SEMI || joinType == JoinRelType.ANTI;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject project = call.rel(0);
        LogicalJoin join = call.rel(1);

        // 1. calculate every inputs reference fields
        ImmutableBitSet joinCondFields = RelOptUtil.InputFinder.bits(join.getCondition());
        ImmutableBitSet projectFields = RelOptUtil.InputFinder.bits(project.getProjects(), null);
        ImmutableBitSet allNeededFields =
                projectFields.isEmpty()
                        ? joinCondFields.union(ImmutableBitSet.of(0))
                        : joinCondFields.union(projectFields);

        int leftFieldCount = join.getLeft().getRowType().getFieldCount();
        int allInputFieldCount = leftFieldCount + join.getRight().getRowType().getFieldCount();
        if (allNeededFields.equals(ImmutableBitSet.range(0, allInputFieldCount))) {
            return;
        }

        ImmutableBitSet leftNeededFields =
                ImmutableBitSet.range(0, leftFieldCount).intersect(allNeededFields);
        ImmutableBitSet rightNeededFields =
                ImmutableBitSet.range(leftFieldCount, allInputFieldCount)
                        .intersect(allNeededFields);

        // 2. new join inputs
        RelNode newLeftInput =
                createNewJoinInput(call.builder(), join.getLeft(), leftNeededFields, 0);
        RelNode newRightInput =
                createNewJoinInput(
                        call.builder(), join.getRight(), rightNeededFields, leftFieldCount);

        // mapping origin field index to new field index,
        // used to rewrite join condition and top project
        Mappings.TargetMapping mapping =
                Mappings.target(
                        i -> allNeededFields.indexOf(i),
                        allInputFieldCount,
                        allNeededFields.cardinality());

        // 3. create new join
        RelNode newJoin = createNewJoin(join, mapping, newLeftInput, newRightInput);

        // 4. create top project
        List<RexNode> newProjects = createNewProjects(project, newJoin, mapping);
        RelNode topProject =
                call.builder()
                        .push(newJoin)
                        .project(newProjects, project.getRowType().getFieldNames())
                        .build();

        call.transformTo(topProject);
    }

    private RelNode createNewJoinInput(
            RelBuilder relBuilder,
            RelNode originInput,
            ImmutableBitSet inputNeededFields,
            int offset) {
        RexBuilder rexBuilder = originInput.getCluster().getRexBuilder();
        RelDataTypeFactory.Builder typeBuilder = relBuilder.getTypeFactory().builder();
        List<RexNode> newProjects = new ArrayList<>();
        List<String> newFieldNames = new ArrayList<>();
        for (int i : inputNeededFields.toList()) {
            newProjects.add(rexBuilder.makeInputRef(originInput, i - offset));
            newFieldNames.add(originInput.getRowType().getFieldNames().get(i - offset));
            typeBuilder.add(originInput.getRowType().getFieldList().get(i - offset));
        }
        return relBuilder.push(originInput).project(newProjects, newFieldNames).build();
    }

    private Join createNewJoin(
            Join originJoin,
            Mappings.TargetMapping mapping,
            RelNode newLeftInput,
            RelNode newRightInput) {
        RexNode newCondition = rewriteJoinCondition(originJoin, mapping);
        return LogicalJoin.create(
                newLeftInput,
                newRightInput,
                Collections.emptyList(),
                newCondition,
                originJoin.getVariablesSet(),
                originJoin.getJoinType());
    }

    private RexNode rewriteJoinCondition(Join originJoin, Mappings.TargetMapping mapping) {
        RexBuilder rexBuilder = originJoin.getCluster().getRexBuilder();
        RexShuttle rexShuttle =
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef ref) {
                        int leftFieldCount = originJoin.getLeft().getRowType().getFieldCount();
                        RelDataType fieldType =
                                ref.getIndex() < leftFieldCount
                                        ? originJoin
                                                .getLeft()
                                                .getRowType()
                                                .getFieldList()
                                                .get(ref.getIndex())
                                                .getType()
                                        : originJoin
                                                .getRight()
                                                .getRowType()
                                                .getFieldList()
                                                .get(ref.getIndex() - leftFieldCount)
                                                .getType();
                        return rexBuilder.makeInputRef(
                                fieldType, mapping.getTarget(ref.getIndex()));
                    }
                };
        return originJoin.getCondition().accept(rexShuttle);
    }

    private List<RexNode> createNewProjects(
            Project originProject, RelNode newInput, Mappings.TargetMapping mapping) {
        RexBuilder rexBuilder = originProject.getCluster().getRexBuilder();
        RexShuttle projectShuffle =
                new RexShuttle() {
                    @Override
                    public RexNode visitInputRef(RexInputRef ref) {
                        return rexBuilder.makeInputRef(newInput, mapping.getTarget(ref.getIndex()));
                    }
                };
        return originProject.getProjects().stream()
                .map(p -> p.accept(projectShuffle))
                .collect(Collectors.toList());
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface ProjectSemiAntiJoinTransposeRuleConfig extends RelRule.Config {
        ProjectSemiAntiJoinTransposeRule.ProjectSemiAntiJoinTransposeRuleConfig DEFAULT =
                ImmutableProjectSemiAntiJoinTransposeRule.ProjectSemiAntiJoinTransposeRuleConfig
                        .builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(LogicalProject.class)
                                                .inputs(
                                                        b1 ->
                                                                b1.operand(LogicalJoin.class)
                                                                        .anyInputs()))
                        .withDescription("ProjectSemiAntiJoinTransposeRule");

        @Override
        default ProjectSemiAntiJoinTransposeRule toRule() {
            return new ProjectSemiAntiJoinTransposeRule(this);
        }
    }
}
