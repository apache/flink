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
 * Removes redundant equi-join key references from COALESCE calls above joins.
 *
 * <p>In an equi-join {@code ON a.k = b.k}, the non-preserved side's key is either equal to the
 * preserved side's key (matched) or NULL (unmatched). This makes it redundant in a COALESCE when it
 * appears adjacent-before or anywhere after the preserved side's key:
 *
 * <ul>
 *   <li>{@code COALESCE(b.k, a.k)} → {@code a.k}
 *   <li>{@code COALESCE(b.k, a.k, x)} → {@code COALESCE(a.k, x)}
 *   <li>{@code COALESCE(x, a.k, b.k)} → {@code COALESCE(x, a.k)}
 *   <li>{@code COALESCE(b.k, x, a.k)} → unchanged (removing b.k would expose x)
 * </ul>
 *
 * <p>For INNER joins both keys are always non-null, so the later-occurring one is always
 * unreachable and can be removed regardless of position. FULL OUTER joins are not handled because
 * both sides can generate nulls.
 *
 * <p>Matches a {@link Project} or {@link Calc} on top of a {@link Join} and uses a {@link
 * RexShuttle} to recursively simplify COALESCE calls, including nested ones (e.g., {@code
 * CAST(COALESCE(b.k, a.k) AS VARCHAR)}).
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

    /** Traverses expressions bottom-up, removing redundant equi-join refs from COALESCE calls. */
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
            call = (RexCall) super.visitCall(call);

            if (!operatorIsCoalesce(call.getOperator()) || call.getOperands().size() < 2) {
                return call;
            }

            final List<RexNode> operands = new ArrayList<>(call.getOperands());
            for (final IntPair pair : joinInfo.pairs()) {
                tryRemoveRedundantRef(operands, pair);
            }

            final boolean changed = operands.size() != call.getOperands().size();
            if (!changed) {
                return call;
            }

            simplified = true;

            if (operands.size() == 1) {
                return castIfNeeded(operands.get(0), call);
            }
            return call.clone(call.getType(), operands);
        }

        /**
         * For a given equi-join pair, finds the two key references in the operand list and removes
         * the redundant one if safe.
         */
        private void tryRemoveRedundantRef(List<RexNode> operands, IntPair equiJoinPair) {
            final int leftPos = findRefPosition(operands, equiJoinPair.source);
            final int rightPos = findRefPosition(operands, equiJoinPair.target + leftFieldCount);
            if (leftPos == -1 || rightPos == -1) {
                return;
            }

            final int removablePos = findRemovablePosition(leftPos, rightPos);
            if (removablePos != -1) {
                operands.remove(removablePos);
            }
        }

        /** Returns the position of the first {@link RexInputRef} with the given index, or -1. */
        private static int findRefPosition(List<RexNode> operands, int inputRefIndex) {
            for (int i = 0; i < operands.size(); i++) {
                if (operands.get(i) instanceof RexInputRef
                        && ((RexInputRef) operands.get(i)).getIndex() == inputRefIndex) {
                    return i;
                }
            }
            return -1;
        }

        /**
         * Determines which of the two equi-join key positions can be safely removed, or returns -1.
         */
        private int findRemovablePosition(int leftPos, int rightPos) {
            switch (joinType) {
                case INNER:
                    // Both keys are non-null; the later one is unreachable
                    return Math.max(leftPos, rightPos);
                case LEFT:
                    return canSafelyRemove(rightPos, leftPos) ? rightPos : -1;
                case RIGHT:
                    return canSafelyRemove(leftPos, rightPos) ? leftPos : -1;
                default:
                    return -1;
            }
        }

        /**
         * The non-preserved ref can be safely removed when it is adjacent-before or anywhere after
         * the preserved ref. The only unsafe case is when the non-preserved ref appears earlier
         * with other operands in between - removing it would change which intermediate value
         * COALESCE returns.
         */
        private static boolean canSafelyRemove(int nonPreservedPos, int preservedPos) {
            return nonPreservedPos >= preservedPos - 1;
        }

        private RexNode castIfNeeded(RexNode node, RexCall originalCall) {
            final LogicalType nodeType = FlinkTypeFactory.toLogicalType(node.getType());
            final LogicalType targetType = FlinkTypeFactory.toLogicalType(originalCall.getType());
            if (LogicalTypeCasts.supportsImplicitCast(nodeType, targetType)) {
                return node;
            }
            return rexBuilder.makeCast(originalCall.getType(), node);
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
