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
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalRel;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.rules.physical.stream.StreamPhysicalCorrelateRule;
import org.apache.flink.table.planner.plan.utils.RexDefaultVisitor;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;
import org.immutables.value.Value;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.plan.utils.PythonUtil.containsPythonCall;
import static org.apache.flink.table.planner.plan.utils.PythonUtil.isNonPythonCall;

/**
 * Rule will split a {@link FlinkLogicalCalc} which is the upstream of a {@link
 * FlinkLogicalCorrelate} and contains Python Functions in condition into two {@link
 * FlinkLogicalCalc}s. One of the {@link FlinkLogicalCalc} without python function condition is the
 * upstream of the {@link FlinkLogicalCorrelate}, but the other {@link FlinkLogicalCalc} with python
 * function conditions is the downstream of the {@link FlinkLogicalCorrelate}. Currently, only inner
 * join is supported.
 *
 * <p>After this rule is applied, there will be no Python Functions in the condition of the upstream
 * {@link FlinkLogicalCalc}.
 */
@Value.Enclosing
public class SplitPythonConditionFromCorrelateRule
        extends RelRule<
                SplitPythonConditionFromCorrelateRule.SplitPythonConditionFromCorrelateRuleConfig> {

    public static final SplitPythonConditionFromCorrelateRule INSTANCE =
            SplitPythonConditionFromCorrelateRule.SplitPythonConditionFromCorrelateRuleConfig
                    .DEFAULT
                    .toRule();

    private SplitPythonConditionFromCorrelateRule(
            SplitPythonConditionFromCorrelateRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalCorrelate correlate = call.rel(0);
        FlinkLogicalCalc right = call.rel(2);
        JoinRelType joinType = correlate.getJoinType();
        FlinkLogicalCalc mergedCalc = StreamPhysicalCorrelateRule.getMergedCalc(right);
        FlinkLogicalTableFunctionScan tableScan =
                StreamPhysicalCorrelateRule.getTableScan(mergedCalc);

        return joinType == JoinRelType.INNER
                && isNonPythonCall(tableScan.getCall())
                && mergedCalc.getProgram() != null
                && mergedCalc.getProgram().getCondition() != null
                && containsPythonCall(
                        mergedCalc
                                .getProgram()
                                .expandLocalRef(mergedCalc.getProgram().getCondition()),
                        null);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalCorrelate correlate = call.rel(0);
        FlinkLogicalCalc right = call.rel(2);
        RexBuilder rexBuilder = call.builder().getRexBuilder();
        FlinkLogicalCalc mergedCalc = StreamPhysicalCorrelateRule.getMergedCalc(right);
        RexProgram mergedCalcProgram = mergedCalc.getProgram();
        RelNode input = mergedCalc.getInput();

        List<RexNode> correlateFilters =
                RelOptUtil.conjunctions(
                        mergedCalcProgram.expandLocalRef(mergedCalcProgram.getCondition()));

        List<RexNode> remainingFilters =
                correlateFilters.stream()
                        .filter(filter -> !containsPythonCall(filter))
                        .collect(Collectors.toList());

        RexNode bottomCalcCondition = RexUtil.composeConjunction(rexBuilder, remainingFilters);

        FlinkLogicalCalc newBottomCalc =
                new FlinkLogicalCalc(
                        mergedCalc.getCluster(),
                        mergedCalc.getTraitSet(),
                        input,
                        RexProgram.create(
                                input.getRowType(),
                                mergedCalcProgram.getProjectList(),
                                bottomCalcCondition,
                                mergedCalc.getRowType(),
                                rexBuilder));

        FlinkLogicalCorrelate newCorrelate =
                new FlinkLogicalCorrelate(
                        correlate.getCluster(),
                        correlate.getTraitSet(),
                        correlate.getLeft(),
                        newBottomCalc,
                        correlate.getCorrelationId(),
                        correlate.getRequiredColumns(),
                        correlate.getJoinType());

        InputRefRewriter inputRefRewriter =
                new InputRefRewriter(
                        correlate.getRowType().getFieldCount()
                                - mergedCalc.getRowType().getFieldCount());

        List<RexNode> pythonFilters =
                correlateFilters.stream()
                        .filter(filter -> containsPythonCall(filter))
                        .map(filter -> filter.accept(inputRefRewriter))
                        .collect(Collectors.toList());

        RexNode topCalcCondition = RexUtil.composeConjunction(rexBuilder, pythonFilters);

        RexProgram rexProgram =
                new RexProgramBuilder(newCorrelate.getRowType(), rexBuilder).getProgram();
        FlinkLogicalCalc newTopCalc =
                new FlinkLogicalCalc(
                        newCorrelate.getCluster(),
                        newCorrelate.getTraitSet(),
                        newCorrelate,
                        RexProgram.create(
                                newCorrelate.getRowType(),
                                rexProgram.getExprList(),
                                topCalcCondition,
                                newCorrelate.getRowType(),
                                rexBuilder));

        call.transformTo(newTopCalc);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface SplitPythonConditionFromCorrelateRuleConfig extends RelRule.Config {
        SplitPythonConditionFromCorrelateRule.SplitPythonConditionFromCorrelateRuleConfig DEFAULT =
                ImmutableSplitPythonConditionFromCorrelateRule
                        .SplitPythonConditionFromCorrelateRuleConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(FlinkLogicalCorrelate.class)
                                                .inputs(
                                                        b1 ->
                                                                b1.operand(FlinkLogicalRel.class)
                                                                        .anyInputs(),
                                                        b2 ->
                                                                b2.operand(FlinkLogicalCalc.class)
                                                                        .anyInputs()))
                        .withDescription("SplitPythonConditionFromCorrelateRule");

        @Override
        default SplitPythonConditionFromCorrelateRule toRule() {
            return new SplitPythonConditionFromCorrelateRule(this);
        }
    }

    /**
     * Because the inputRef is from the upstream calc node of the correlate node, so after the
     * inputRef is pushed to the downstream calc node of the correlate node, the inputRef need to
     * rewrite the index.
     */
    static class InputRefRewriter extends RexDefaultVisitor<RexNode> {
        private final int offset;

        /**
         * @param offset the start offset of the inputRef in the downstream calc.
         */
        public InputRefRewriter(int offset) {
            this.offset = offset;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            return new RexInputRef(inputRef.getIndex() + offset, inputRef.getType());
        }

        @Override
        public RexNode visitCall(RexCall call) {
            return call.clone(
                    call.getType(),
                    call.getOperands().stream()
                            .map(o -> o.accept(this))
                            .collect(Collectors.toList()));
        }

        @Override
        public RexNode visitNode(RexNode rexNode) {
            return rexNode;
        }
    }
}
