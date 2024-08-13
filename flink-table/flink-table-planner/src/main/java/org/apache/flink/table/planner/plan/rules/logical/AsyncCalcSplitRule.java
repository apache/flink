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

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalJoin;
import org.apache.flink.table.planner.plan.utils.AsyncUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexUtil;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import scala.Option;

/**
 * Defines split rules for async calc nodes. These largely exist to isolate and simplify the calls
 * to the async function from other calc operations, so that the operator can handle just that
 * functionality.
 */
public class AsyncCalcSplitRule {

    private static final RemoteCalcCallFinder ASYNC_CALL_FINDER = new AsyncRemoteCalcCallFinder();
    public static final RelOptRule SPLIT_CONDITION =
            new RemoteCalcSplitConditionRule(ASYNC_CALL_FINDER);
    public static final RelOptRule SPLIT_PROJECT =
            new RemoteCalcSplitProjectionRule(ASYNC_CALL_FINDER);
    public static final RelOptRule SPLIT_PROJECTION_REX_FIELD =
            new RemoteCalcSplitProjectionRexFieldRule(ASYNC_CALL_FINDER);
    public static final RelOptRule SPLIT_CONDITION_REX_FIELD =
            new RemoteCalcSplitConditionRexFieldRule(ASYNC_CALL_FINDER);
    public static final RelOptRule EXPAND_PROJECT =
            new RemoteCalcExpandProjectRule(ASYNC_CALL_FINDER);
    public static final RelOptRule PUSH_CONDITION =
            new RemoteCalcPushConditionRule(ASYNC_CALL_FINDER);
    public static final RelOptRule REWRITE_PROJECT =
            new RemoteCalcRewriteProjectionRule(ASYNC_CALL_FINDER);
    public static final RelOptRule NESTED_SPLIT = new AsyncCalcSplitNestedRule(ASYNC_CALL_FINDER);
    public static final RelOptRule ONE_PER_CALC_SPLIT =
            new AsyncCalcSplitOnePerCalcRule(ASYNC_CALL_FINDER);
    public static final RelOptRule NO_ASYNC_JOIN_CONDITIONS = new AsyncCalcSplitJoinCondition();

    /**
     * An Async implementation of {@link RemoteCalcCallFinder} which finds uses of {@link
     * org.apache.flink.table.functions.AsyncScalarFunction}.
     */
    public static class AsyncRemoteCalcCallFinder implements RemoteCalcCallFinder {
        @Override
        public boolean containsRemoteCall(RexNode node) {
            return AsyncUtil.containsAsyncCall(node);
        }

        @Override
        public boolean containsNonRemoteCall(RexNode node) {
            return AsyncUtil.containsNonAsyncCall(node);
        }

        @Override
        public boolean isRemoteCall(RexNode node) {
            return AsyncUtil.isAsyncCall(node);
        }

        @Override
        public boolean isNonRemoteCall(RexNode node) {
            return AsyncUtil.isNonAsyncCall(node);
        }
    }

    private static boolean hasNestedCalls(List<RexNode> projects) {
        return projects.stream()
                .filter(AsyncUtil::containsAsyncCall)
                .filter(expr -> expr instanceof RexCall)
                .map(expr -> (RexCall) expr)
                .anyMatch(
                        rexCall ->
                                rexCall.getOperands().stream()
                                        .anyMatch(AsyncUtil::containsAsyncCall));
    }

    /**
     * Splits nested call <- asyncCall chains so that nothing is immediately waiting on an async
     * call in a single calc.
     *
     * <p>For Example: Calc(select=[syncCall(asyncCall()]) -> Source
     *
     * <p>becomes
     *
     * <p>Calc(select=[syncCall(f0)]) -> AsyncCalc(select=[asyncCall() as f0]) -> Source
     */
    public static class AsyncCalcSplitNestedRule extends RemoteCalcSplitRuleBase<Void> {

        public AsyncCalcSplitNestedRule(RemoteCalcCallFinder callFinder) {
            super("AsyncCalcSplitNestedRule", callFinder);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            FlinkLogicalCalc calc = call.rel(0);

            // Matches if we have nested remote calls
            List<RexNode> projects =
                    calc.getProgram().getProjectList().stream()
                            .map(calc.getProgram()::expandLocalRef)
                            .collect(Collectors.toList());
            return hasNestedCalls(projects);
        }

        // We convert not on the outermost call, but anything within it
        @Override
        public boolean needConvert(RexProgram program, RexNode node, Option<Void> matchState) {
            return node instanceof RexCall
                    && !((RexCall) node)
                            .getOperands().stream().anyMatch(callFinder()::containsRemoteCall);
        }

        @Override
        public SplitComponents split(RexProgram program, ScalarFunctionSplitter splitter) {
            return new SplitComponents(
                    JavaScalaConversionUtil.toScala(Optional.<RexNode>empty()),
                    JavaScalaConversionUtil.toScala(
                            Optional.ofNullable(program.getCondition())
                                    .map(program::expandLocalRef)),
                    JavaScalaConversionUtil.toScala(
                            program.getProjectList().stream()
                                    .map(program::expandLocalRef)
                                    .map(n -> n.accept(splitter))
                                    .collect(Collectors.toList())));
        }
    }

    /**
     * Splits async calls if there are multiple across projections, so that there's one per calc.
     * This assumes that the nested rule has been run first, so there is just one per projection.
     *
     * <p>For Example: Calc(select=[asyncCall(), asyncCall()]) -> Source
     *
     * <p>becomes
     *
     * <p>AsyncCalc(select=[asyncCall(), f0]) -> AsyncCalc(select=[asyncCall() as f0]) -> Source
     */
    public static class AsyncCalcSplitOnePerCalcRule
            extends RemoteCalcSplitProjectionRuleBase<AsyncCalcSplitOnePerCalcRule.State> {

        public AsyncCalcSplitOnePerCalcRule(RemoteCalcCallFinder callFinder) {
            super("AsyncCalcSplitOnePerCalcRule", callFinder);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            FlinkLogicalCalc calc = call.rel(0);
            List<RexNode> projects =
                    calc.getProgram().getProjectList().stream()
                            .map(calc.getProgram()::expandLocalRef)
                            .collect(Collectors.toList());

            // If this has no nested calls, then this can be called to split up separate projections
            // into two different calcs. We don't want the splitter to be called to with nested
            // calls since it won't behave correctly, so this must be used in conjunction with the
            // nested rule.
            return !hasNestedCalls(projects)
                    && projects.stream().filter(callFinder()::containsRemoteCall).count() >= 2;
        }

        @Override
        public boolean needConvert(RexProgram program, RexNode node, Option<State> matchState) {
            if (AsyncUtil.containsAsyncCall(node) && !matchState.get().foundMatch) {
                matchState.get().foundMatch = true;
                return true;
            }
            return false;
        }

        @Override
        public Option<State> getMatchState() {
            return Option.apply(new State());
        }

        /** State object used to keep track of whether a match has been found yet. */
        public static class State {
            boolean foundMatch = false;
        }
    }

    public static class AsyncCalcSplitJoinCondition extends RelOptRule {

        protected AsyncCalcSplitJoinCondition() {
            super(operand(FlinkLogicalJoin.class, RelOptRule.any()), "AsyncCalcSplitJoinCondition");
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            FlinkLogicalJoin join = call.rel(0);
            if (join.getCondition() != null && AsyncUtil.containsAsyncCall(join.getCondition())) {
                if (join.getJoinType() == JoinRelType.INNER) {
                    return true;
                } else {
                    throw new TableException(
                            "AsyncScalarFunction not supported for non inner join condition");
                }
            }
            return false;
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            FlinkLogicalJoin join = call.rel(0);

            RexBuilder rexBuilder = join.getCluster().getRexBuilder();

            List<RexNode> joinFilters = RelOptUtil.conjunctions(join.getCondition());
            List<RexNode> asyncFilters =
                    joinFilters.stream()
                            .filter(AsyncUtil::containsAsyncCall)
                            .collect(Collectors.toList());
            List<RexNode> remainingFilters =
                    joinFilters.stream()
                            .filter(n -> !AsyncUtil.containsAsyncCall(n))
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
            RexNode topCalcCondition = RexUtil.composeConjunction(rexBuilder, asyncFilters);

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
    }
}
