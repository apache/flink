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

import org.apache.flink.table.planner.plan.utils.JoinNestedProjectionResult;
import org.apache.flink.table.planner.plan.utils.NestedProjectionUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

/**
 * Pushes nested field accesses below a {@link Join}.
 *
 * <p>If a query reads only some fields of a ROW column, the join carries, shuffles, and stores the
 * whole ROW. This rule extracts the accessed nested fields into flat top-level columns. It adds a
 * projection below each join input that replaces the ROW column by one column per accessed field,
 * and rewrites the join condition and the project above the join to read these columns.
 *
 * <p>Before:
 *
 * <pre>{@code
 * Project(x=[$1.a.b], y=[$1.d], z=[$3])
 * +- Join(joinType=[inner], condition=[=($0, $2)])
 *    :- Scan(id, s ROW(a ROW(b, c), d))
 *    +- Scan(id2, z)
 * }</pre>
 *
 * <p>After, with the accessed fields {@code s.a.b} and {@code s.d} as flat columns $1 and $2 of the
 * left input:
 *
 * <pre>{@code
 * Project(x=[$1], y=[$2], z=[$4])
 * +- Join(joinType=[inner], condition=[=($0, $3)])
 *    :- Project(id, $f1=[$1.a.b], $f2=[$1.d])
 *    :  +- Scan(id, s ROW(a ROW(b, c), d))
 *    +- Scan(id2, z)
 * }</pre>
 *
 * <p>The rule does nothing else:
 *
 * <ul>
 *   <li>A ROW column that is read as a whole, or whose fields are all read, is not split.
 *   <li>Top-level columns that no expression reads are not projected out. They pass through the new
 *       projection, and {@link FlinkProjectJoinTransposeRule} prunes them.
 * </ul>
 *
 * <p>The rule matches only if at least one ROW column has a field that is not read. After the rule
 * has fired, the join inputs contain flat columns instead of such ROW columns, so it does not match
 * again, and the HEP planner reaches a fixpoint.
 *
 * <p>SEMI and ANTI joins are not supported.
 */
@Value.Enclosing
public class ProjectNestedFieldJoinTransposeRule
        extends RelRule<
                ProjectNestedFieldJoinTransposeRule.ProjectNestedFieldJoinTransposeRuleConfig> {

    public static final ProjectNestedFieldJoinTransposeRule INSTANCE =
            ProjectNestedFieldJoinTransposeRuleConfig.DEFAULT.toRule();

    protected ProjectNestedFieldJoinTransposeRule(
            ProjectNestedFieldJoinTransposeRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final Join join = call.rel(1);
        // TODO: FLINK-41648 - support SEMI and ANTI joins
        if (!join.getJoinType().projectsRight()) {
            return false;
        }
        final Project project = call.rel(0);
        return NestedProjectionUtil.hasPrunableNestedLeaf(
                project.getProjects(), join.getCondition(), join.getRowType());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Project project = call.rel(0);
        final Join join = call.rel(1);
        final RelBuilder builder = call.builder();

        final JoinNestedProjectionResult result =
                NestedProjectionUtil.rewriteForJoin(
                        project.getProjects(),
                        join.getCondition(),
                        join.getRowType(),
                        join.getLeft().getRowType(),
                        join.getRight().getRowType(),
                        join.getJoinType().generatesNullsOnLeft(),
                        join.getJoinType().generatesNullsOnRight(),
                        builder.getRexBuilder());

        // Build the flattening child projects under each input.
        builder.push(join.getLeft()).project(result.leftProjects());
        final org.apache.calcite.rel.RelNode newLeft = builder.build();
        builder.push(join.getRight()).project(result.rightProjects());
        final org.apache.calcite.rel.RelNode newRight = builder.build();

        final Join newJoin =
                join.copy(
                        join.getTraitSet(),
                        result.rewrittenCondition(),
                        newLeft,
                        newRight,
                        join.getJoinType(),
                        join.isSemiJoinDone());

        builder.push(newJoin)
                .project(result.rewrittenProjects(), project.getRowType().getFieldNames());
        call.transformTo(builder.build());
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface ProjectNestedFieldJoinTransposeRuleConfig extends RelRule.Config {
        ProjectNestedFieldJoinTransposeRuleConfig DEFAULT =
                ImmutableProjectNestedFieldJoinTransposeRule
                        .ProjectNestedFieldJoinTransposeRuleConfig.builder()
                        .relBuilderFactory(RelFactories.LOGICAL_BUILDER)
                        .operandSupplier(
                                b0 ->
                                        b0.operand(Project.class)
                                                .oneInput(b1 -> b1.operand(Join.class).anyInputs()))
                        .description("ProjectNestedFieldJoinTransposeRule")
                        .build();

        @Override
        default ProjectNestedFieldJoinTransposeRule toRule() {
            return new ProjectNestedFieldJoinTransposeRule(this);
        }
    }
}
