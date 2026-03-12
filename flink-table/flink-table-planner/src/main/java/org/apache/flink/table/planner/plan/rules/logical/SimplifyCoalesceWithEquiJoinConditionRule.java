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

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Simplifies COALESCE calls that contain equi-join key references by removing redundant
 * non-preserved-side references.
 *
 * <p>For a 2-arg case like {@code COALESCE(b.k, a.k)} in a {@code LEFT JOIN ON a.k = b.k}, the
 * result always equals {@code a.k}:
 *
 * <ul>
 *   <li>When b matches: {@code b.k = a.k} (equi-join guarantees this), so the result is {@code
 *       a.k}.
 *   <li>When b doesn't match: {@code b.k} is NULL, so the result falls through to {@code a.k}.
 * </ul>
 *
 * <p>For N-arg COALESCE, the non-preserved-side reference is removed when safe:
 *
 * <ul>
 *   <li>Adjacent before preserved: {@code COALESCE(..., b.k, a.k, ...)} becomes {@code
 *       COALESCE(..., a.k, ...)} - when matched b.k=a.k so removing b.k yields the same value;
 *       when unmatched b.k is NULL and skipped anyway.
 *   <li>After preserved: {@code COALESCE(..., a.k, ..., b.k, ...)} becomes {@code COALESCE(...,
 *       a.k, ...)} - when matched a.k is non-null so b.k is never reached; when unmatched b.k is
 *       NULL anyway.
 * </ul>
 *
 * <p>For INNER joins both sides are non-null, so the later-occurring equi-join ref is always
 * unreachable and can be removed. FULL OUTER joins are not handled because both sides can generate
 * nulls.
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
     * A {@link RexShuttle} that simplifies COALESCE calls by removing non-preserved-side equi-join
     * key references when it is semantically safe to do so.
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
            if (call.getOperands().size() < 2) {
                return call;
            }

            final List<RexNode> operands = new ArrayList<>(call.getOperands());
            boolean changed = false;

            for (final IntPair pair : joinInfo.pairs()) {
                changed |= tryRemoveNonPreservedRef(operands, pair);
            }

            if (!changed) {
                return call;
            }

            simplified = true;

            if (operands.size() == 1) {
                final RexNode remaining = operands.get(0);
                final LogicalType remainingType =
                        FlinkTypeFactory.toLogicalType(remaining.getType());
                final LogicalType coalesceType =
                        FlinkTypeFactory.toLogicalType(call.getType());

                if (LogicalTypeCasts.supportsImplicitCast(remainingType, coalesceType)) {
                    return remaining;
                } else {
                    return rexBuilder.makeCast(call.getType(), remaining);
                }
            }

            return call.clone(call.getType(), operands);
        }

        /**
         * Attempts to remove the non-preserved-side reference from the operand list for a given
         * equi-join pair. Returns true if a removal was made.
         *
         * <p>For LEFT/RIGHT joins, removal is safe when the non-preserved ref is immediately before
         * the preserved ref (adjacent) or anywhere after it. For INNER joins, the later-occurring
         * ref is always safe to remove since both are non-null.
         */
        private boolean tryRemoveNonPreservedRef(List<RexNode> operands, IntPair pair) {
            final int leftJoinIdx = pair.source;
            final int rightJoinIdx = pair.target + leftFieldCount;

            int leftPos = -1;
            int rightPos = -1;

            for (int i = 0; i < operands.size(); i++) {
                final RexNode op = operands.get(i);
                if (op instanceof RexInputRef) {
                    final int idx = ((RexInputRef) op).getIndex();
                    if (idx == leftJoinIdx && leftPos == -1) {
                        leftPos = i;
                    } else if (idx == rightJoinIdx && rightPos == -1) {
                        rightPos = i;
                    }
                }
            }

            if (leftPos == -1 || rightPos == -1) {
                return false;
            }

            if (joinType == JoinRelType.INNER) {
                // Both refs are non-null in INNER join; the later one is always unreachable
                operands.remove(Math.max(leftPos, rightPos));
                return true;
            }

            final int preservedPos;
            final int nonPreservedPos;
            switch (joinType) {
                case LEFT:
                    preservedPos = leftPos;
                    nonPreservedPos = rightPos;
                    break;
                case RIGHT:
                    preservedPos = rightPos;
                    nonPreservedPos = leftPos;
                    break;
                default:
                    return false;
            }

            // Safe: non-preserved is immediately before preserved, or anywhere after preserved
            if (nonPreservedPos == preservedPos - 1 || nonPreservedPos > preservedPos) {
                operands.remove(nonPreservedPos);
                return true;
            }

            return false;
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
