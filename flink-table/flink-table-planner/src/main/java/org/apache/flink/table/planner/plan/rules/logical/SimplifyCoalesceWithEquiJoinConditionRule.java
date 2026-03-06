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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.mapping.IntPair;
import org.immutables.value.Value;

import java.util.function.Predicate;

/**
 * Simplifies {@code COALESCE(b.k, a.k)} to the preserved-side column reference when the two
 * arguments reference columns from opposite sides of an equi-join condition.
 *
 * <p>For a {@code LEFT JOIN a ON a.k = b.k}, {@code COALESCE(b.k, a.k)} always equals {@code a.k}:
 *
 * <ul>
 *   <li>When b matches: {@code b.k = a.k} (equi-join guarantees this), so the result is {@code
 *       a.k}.
 *   <li>When b doesn't match: {@code b.k} is NULL, so the result falls through to {@code a.k}.
 * </ul>
 *
 * <p>Both orderings ({@code COALESCE(b.k, a.k)} and {@code COALESCE(a.k, b.k)}) resolve to the
 * preserved-side key. This applies to LEFT, RIGHT, and INNER joins. FULL OUTER joins are not
 * handled because both sides can generate nulls.
 *
 * <p>This rule matches a {@link Project} or {@link Calc} whose input is a {@link Join}. It uses a
 * {@link RexShuttle} to recursively find and simplify applicable COALESCE calls, including those
 * nested inside other expressions (e.g., {@code CAST(COALESCE(b.k, a.k) AS VARCHAR)}).
 */
@Internal
@Value.Enclosing
public class SimplifyCoalesceWithEquiJoinConditionRule
        extends RelRule<SimplifyCoalesceWithEquiJoinConditionRule.Config> {

    public static final RelRule<Config> PROJECT_INSTANCE = Config.DEFAULT.withProject().toRule();

    public static final RelRule<Config> CALC_INSTANCE = Config.DEFAULT.withCalc().toRule();

    public SimplifyCoalesceWithEquiJoinConditionRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final RelNode relNode = call.rel(0);
        final Join join = call.rel(1);

        final JoinInfo joinInfo = join.analyzeCondition();
        if (joinInfo.pairs().isEmpty()) {
            return;
        }

        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        final int leftFieldCount = join.getLeft().getRowType().getFieldCount();

        final EquiJoinCoalesceSimplifier shuttle =
                new EquiJoinCoalesceSimplifier(
                        rexBuilder, joinInfo, join.getJoinType(), leftFieldCount);

        final RelNode transformed = relNode.accept(shuttle);
        if (shuttle.isSimplified()) {
            call.transformTo(transformed);
        }
    }

    // --------------------------------------------------------------------------------------------

    /**
     * A {@link RexShuttle} that replaces {@code COALESCE(refA, refB)} with the preserved-side
     * column reference when the two arguments form an equi-join pair from opposite sides.
     */
    private static class EquiJoinCoalesceSimplifier extends RexShuttle {

        private final RexBuilder rexBuilder;
        private final JoinInfo joinInfo;
        private final JoinRelType joinType;
        private final int leftFieldCount;
        private boolean simplified = false;

        private EquiJoinCoalesceSimplifier(
                RexBuilder rexBuilder,
                JoinInfo joinInfo,
                JoinRelType joinType,
                int leftFieldCount) {
            this.rexBuilder = rexBuilder;
            this.joinInfo = joinInfo;
            this.joinType = joinType;
            this.leftFieldCount = leftFieldCount;
        }

        boolean isSimplified() {
            return simplified;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            // Recurse first (bottom-up) so nested COALESCE calls are handled
            call = (RexCall) super.visitCall(call);

            if (!operatorIsCoalesce(call.getOperator())) {
                return call;
            }
            if (call.getOperands().size() != 2) {
                return call;
            }

            final RexNode op0 = call.getOperands().get(0);
            final RexNode op1 = call.getOperands().get(1);
            if (!(op0 instanceof RexInputRef) || !(op1 instanceof RexInputRef)) {
                return call;
            }

            final RexInputRef ref0 = (RexInputRef) op0;
            final RexInputRef ref1 = (RexInputRef) op1;

            // Must be from opposite sides of the join
            final boolean isLeft0 = ref0.getIndex() < leftFieldCount;
            final boolean isLeft1 = ref1.getIndex() < leftFieldCount;
            if (isLeft0 == isLeft1) {
                return call;
            }

            // Identify left-side and right-side refs
            final int leftIdx = isLeft0 ? ref0.getIndex() : ref1.getIndex();
            final int rightIdx = (isLeft0 ? ref1.getIndex() : ref0.getIndex()) - leftFieldCount;
            final RexInputRef leftRef = isLeft0 ? ref0 : ref1;
            final RexInputRef rightRef = isLeft0 ? ref1 : ref0;

            // Check if they form an equi-join pair
            if (!isEquiJoinPair(leftIdx, rightIdx)) {
                return call;
            }

            // Determine the preserved-side reference
            final RexInputRef preservedRef = resolvePreservedSide(leftRef, rightRef, ref0);
            if (preservedRef == null) {
                return call;
            }

            simplified = true;

            // Handle potential type mismatch by adding a CAST if needed
            final LogicalType preservedLogicalType =
                    FlinkTypeFactory.toLogicalType(preservedRef.getType());
            final LogicalType coalesceLogicalType = FlinkTypeFactory.toLogicalType(call.getType());

            if (LogicalTypeCasts.supportsImplicitCast(preservedLogicalType, coalesceLogicalType)) {
                return preservedRef;
            } else {
                return rexBuilder.makeCast(call.getType(), preservedRef);
            }
        }

        private boolean isEquiJoinPair(int leftIdx, int rightIdx) {
            for (final IntPair pair : joinInfo.pairs()) {
                if (pair.source == leftIdx && pair.target == rightIdx) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Returns the preserved-side reference based on the join type.
         *
         * <ul>
         *   <li>LEFT: left side is preserved (never generates nulls on left)
         *   <li>RIGHT: right side is preserved (never generates nulls on right)
         *   <li>INNER: both sides preserved, pick the first COALESCE argument to preserve user
         *       intent
         * </ul>
         */
        private RexInputRef resolvePreservedSide(
                RexInputRef leftRef, RexInputRef rightRef, RexInputRef firstOperand) {
            switch (joinType) {
                case LEFT:
                    return leftRef;
                case RIGHT:
                    return rightRef;
                case INNER:
                    return firstOperand;
                default:
                    return null;
            }
        }
    }

    // --------------------------------------------------------------------------------------------

    private static boolean operatorIsCoalesce(SqlOperator op) {
        return (op instanceof BridgingSqlFunction
                        && ((BridgingSqlFunction) op)
                                .getDefinition()
                                .equals(BuiltInFunctionDefinitions.COALESCE))
                || op.getKind() == SqlKind.COALESCE;
    }

    private static boolean hasCoalesceInvocation(RexNode node) {
        return FlinkRexUtil.hasOperatorCallMatching(
                node, SimplifyCoalesceWithEquiJoinConditionRule::operatorIsCoalesce);
    }

    private static boolean isApplicableJoin(Join join) {
        final JoinRelType joinType = join.getJoinType();
        return joinType == JoinRelType.LEFT
                || joinType == JoinRelType.RIGHT
                || joinType == JoinRelType.INNER;
    }

    // --------------------------------------------------------------------------------------------

    /** Configuration for {@link SimplifyCoalesceWithEquiJoinConditionRule}. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {

        Config DEFAULT =
                ImmutableSimplifyCoalesceWithEquiJoinConditionRule.Config.builder()
                        .build()
                        .as(Config.class);

        @Override
        default SimplifyCoalesceWithEquiJoinConditionRule toRule() {
            return new SimplifyCoalesceWithEquiJoinConditionRule(this);
        }

        default Config withProject() {
            final Predicate<Project> projectPredicate =
                    p ->
                            p.getProjects().stream()
                                    .anyMatch(
                                            SimplifyCoalesceWithEquiJoinConditionRule
                                                    ::hasCoalesceInvocation);
            final RelRule.OperandTransform projectTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(Project.class)
                                    .predicate(projectPredicate)
                                    .oneInput(
                                            b ->
                                                    b.operand(Join.class)
                                                            .predicate(
                                                                    SimplifyCoalesceWithEquiJoinConditionRule
                                                                            ::isApplicableJoin)
                                                            .anyInputs());
            return withOperandSupplier(projectTransform).as(Config.class);
        }

        default Config withCalc() {
            final Predicate<Calc> calcPredicate =
                    c ->
                            c.getProgram().getExprList().stream()
                                    .anyMatch(
                                            SimplifyCoalesceWithEquiJoinConditionRule
                                                    ::hasCoalesceInvocation);
            final RelRule.OperandTransform calcTransform =
                    operandBuilder ->
                            operandBuilder
                                    .operand(Calc.class)
                                    .predicate(calcPredicate)
                                    .oneInput(
                                            b ->
                                                    b.operand(Join.class)
                                                            .predicate(
                                                                    SimplifyCoalesceWithEquiJoinConditionRule
                                                                            ::isApplicableJoin)
                                                            .anyInputs());
            return withOperandSupplier(calcTransform).as(Config.class);
        }
    }
}
