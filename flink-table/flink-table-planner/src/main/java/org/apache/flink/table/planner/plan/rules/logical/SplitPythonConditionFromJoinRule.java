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

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.immutables.value.Value;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall;

/**
 * Rule will splits the {@link FlinkLogicalJoin} which contains Python Functions in join condition
 * into a {@link FlinkLogicalJoin} and a {@link FlinkLogicalCalc} with python Functions. Currently,
 * only inner join is supported.
 *
 * <p>After this rule is applied, there will be no Python Functions in the condition of the {@link
 * FlinkLogicalJoin}.
 */
@Value.Enclosing
public class SplitPythonConditionFromJoinRule
        extends RelRule<SplitPythonConditionFromJoinRule.SplitPythonConditionFromJoinRuleConfig> {

    public static final SplitPythonConditionFromJoinRule INSTANCE =
            SplitPythonConditionFromJoinRuleConfig.DEFAULT.toRule();

    private SplitPythonConditionFromJoinRule(SplitPythonConditionFromJoinRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalJoin join = call.rel(0);
        JoinRelType joinType = join.getJoinType();
        // matches if it is inner join and it contains Python functions in condition
        return joinType == JoinRelType.INNER
                && Optional.ofNullable(join.getCondition())
                        .map(rexNode -> containsPythonCall(rexNode, null))
                        .orElse(false);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalJoin join = call.rel(0);
        RexBuilder rexBuilder = join.getCluster().getRexBuilder();

        List<RexNode> joinFilters = RelOptUtil.conjunctions(join.getCondition());
        List<RexNode> pythonFilters =
                joinFilters.stream()
                        .filter(rexNode -> containsPythonCall(rexNode, null))
                        .collect(Collectors.toList());
        List<RexNode> remainingFilters =
                joinFilters.stream()
                        .filter(rexNode -> !containsPythonCall(rexNode, null))
                        .collect(Collectors.toList());

        RexNode newJoinCondition = RexUtil.composeConjunction(rexBuilder, remainingFilters);
        FlinkLogicalJoin bottomJoin =
                new FlinkLogicalJoin(
                        join.getCluster(),
                        join.getTraitSet(),
                        join.getLeft(),
                        join.getRight(),
                        newJoinCondition,
                        join.getHints(),
                        join.getJoinType());

        RexProgram rexProgram =
                new RexProgramBuilder(bottomJoin.getRowType(), rexBuilder).getProgram();
        RexNode topCalcCondition = RexUtil.composeConjunction(rexBuilder, pythonFilters);

        FlinkLogicalCalc topCalc =
                new FlinkLogicalCalc(
                        join.getCluster(),
                        join.getTraitSet(),
                        bottomJoin,
                        RexProgram.create(
                                bottomJoin.getRowType(),
                                rexProgram.getExprList(),
                                topCalcCondition,
                                bottomJoin.getRowType(),
                                rexBuilder));

        call.transformTo(topCalc);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface SplitPythonConditionFromJoinRuleConfig extends RelRule.Config {
        SplitPythonConditionFromJoinRuleConfig DEFAULT =
                ImmutableSplitPythonConditionFromJoinRule.SplitPythonConditionFromJoinRuleConfig
                        .builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(FlinkLogicalJoin.class).noInputs())
                        .withDescription("SplitPythonConditionFromJoinRule");

        @Override
        default SplitPythonConditionFromJoinRule toRule() {
            return new SplitPythonConditionFromJoinRule(this);
        }
    }
}
