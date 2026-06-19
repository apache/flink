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
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Planner rule that coerces the both sides of EQUALS(`=`) operator in Join condition to the same
 * type while sans nullability.
 *
 * <p>For most cases, we already did the type coercion during type validation by implicit type
 * coercion or during sqlNode to relNode conversion, this rule just does a rechecking to ensure a
 * strongly uniform equals type, so that during a HashJoin shuffle we can have the same hashcode of
 * the same value.
 */
@Value.Enclosing
public class JoinConditionTypeCoerceRule
        extends RelRule<JoinConditionTypeCoerceRule.JoinConditionTypeCoerceRuleConfig> {

    public static final JoinConditionTypeCoerceRule INSTANCE =
            JoinConditionTypeCoerceRule.JoinConditionTypeCoerceRuleConfig.DEFAULT.toRule();

    private JoinConditionTypeCoerceRule(JoinConditionTypeCoerceRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Join join = call.rel(0);
        if (join.getCondition().isAlwaysTrue()) {
            return false;
        }
        RelDataTypeFactory typeFactory = call.builder().getTypeFactory();
        return hasEqualsRefsOfDifferentTypes(typeFactory, join.getCondition());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Join join = call.rel(0);
        RelBuilder builder = call.builder();
        RexBuilder rexBuilder = builder.getRexBuilder();
        RelDataTypeFactory typeFactory = builder.getTypeFactory();

        List<RexNode> joinFilters = RelOptUtil.conjunctions(join.getCondition());
        List<RexNode> newJoinFilters =
                joinFilters.stream()
                        .map(
                                filter -> {
                                    if (filter instanceof RexCall) {
                                        RexCall c = (RexCall) filter;
                                        if (c.getKind() == SqlKind.EQUALS) {
                                            RexNode leftOp = c.getOperands().get(0);
                                            RexNode rightOp = c.getOperands().get(1);
                                            if (leftOp instanceof RexInputRef
                                                    && rightOp instanceof RexInputRef) {
                                                RexInputRef ref1 = (RexInputRef) leftOp;
                                                RexInputRef ref2 = (RexInputRef) rightOp;
                                                if (!SqlTypeUtil.equalSansNullability(
                                                        typeFactory,
                                                        ref1.getType(),
                                                        ref2.getType())) {
                                                    List<RelDataType> refTypes =
                                                            Arrays.asList(
                                                                    ref1.getType(), ref2.getType());
                                                    RelDataType targetType =
                                                            typeFactory.leastRestrictive(refTypes);
                                                    if (targetType == null) {
                                                        throw new TableException(
                                                                "implicit type conversion between "
                                                                        + ref1.getType()
                                                                        + " and "
                                                                        + ref2.getType()
                                                                        + " is not supported on join's condition now");
                                                    }
                                                    return builder.equals(
                                                            rexBuilder.ensureType(
                                                                    targetType, ref1, true),
                                                            rexBuilder.ensureType(
                                                                    targetType, ref2, true));
                                                }
                                            }
                                        }
                                    }
                                    return filter;
                                })
                        .collect(Collectors.toList());

        RexNode newCondExp =
                builder.and(
                        FlinkRexUtil.simplify(
                                rexBuilder,
                                builder.and(newJoinFilters),
                                join.getCluster().getPlanner().getExecutor()));

        Join newJoin =
                join.copy(
                        join.getTraitSet(),
                        newCondExp,
                        join.getLeft(),
                        join.getRight(),
                        join.getJoinType(),
                        join.isSemiJoinDone());

        call.transformTo(newJoin);
    }

    /**
     * Returns true if two input refs of an equal call have different types in join condition, else
     * false.
     */
    private boolean hasEqualsRefsOfDifferentTypes(
            RelDataTypeFactory typeFactory, RexNode predicate) {
        List<RexNode> conjunctions = RelOptUtil.conjunctions(predicate);
        return conjunctions.stream()
                .filter(node -> node instanceof RexCall && node.getKind() == SqlKind.EQUALS)
                .anyMatch(
                        c -> {
                            RexCall call = (RexCall) c;
                            RexNode ref1 = call.getOperands().get(0);
                            RexNode ref2 = call.getOperands().get(1);
                            return ref1 instanceof RexInputRef
                                    && ref2 instanceof RexInputRef
                                    && !SqlTypeUtil.equalSansNullability(
                                            typeFactory, ref1.getType(), ref2.getType());
                        });
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface JoinConditionTypeCoerceRuleConfig extends RelRule.Config {
        JoinConditionTypeCoerceRule.JoinConditionTypeCoerceRuleConfig DEFAULT =
                ImmutableJoinConditionTypeCoerceRule.JoinConditionTypeCoerceRuleConfig.builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(Join.class).anyInputs())
                        .withDescription("JoinConditionTypeCoerceRule");

        @Override
        default JoinConditionTypeCoerceRule toRule() {
            return new JoinConditionTypeCoerceRule(this);
        }
    }
}
