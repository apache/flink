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

import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Planner Rule that extracts some sub-conditions in the Join OR condition that can be pushed into
 * join inputs by {@link FlinkFilterJoinRule}.
 *
 * <p>For example, there is a join query (table A join table B): {@code SELECT * FROM A, B WHERE
 * A.f1 = B.f1 AND ((A.f2 = 'aaa1' AND B.f2 = 'bbb1') OR (A.f2 = 'aaa2' AND B.f2 = 'bbb2')) }
 *
 * <p>Hence the query rewards optimizers that can analyze complex join conditions which cannot be
 * pushed below the join, but still derive filters from such join conditions. It could immediately
 * filter the scan(A) with the condition: {@code (A.f2 = 'aaa1' OR A.f2 = 'aaa2')}.
 *
 * <p>After join condition dependent optimization, the query will be: {@code SELECT * FROM A, B
 * WHERE A.f1 = B.f1 AND ((A.f2 = 'aaa1' AND B.f2 = 'bbb1') OR (A.f2 = 'aaa2' AND B.f2 = 'bbb2'))
 * AND (A.f2 = 'aaa1' OR A.f2 = 'aaa2') AND (B.f2 = 'bbb1' OR B.f2 = 'bbb2') }.
 *
 * <p>Note: This class can only be used in HepPlanner with RULE_SEQUENCE.
 */
@Value.Enclosing
public class JoinDependentConditionDerivationRule
        extends RelRule<
                JoinDependentConditionDerivationRule.JoinDependentConditionDerivationRuleConfig> {

    public static final JoinDependentConditionDerivationRule INSTANCE =
            new JoinDependentConditionDerivationRule(
                    JoinDependentConditionDerivationRuleConfig.DEFAULT);

    protected JoinDependentConditionDerivationRule(
            JoinDependentConditionDerivationRuleConfig config) {
        super(config);
    }

    public boolean matches(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        // TODO supports more join type
        return join.getJoinType() == JoinRelType.INNER;
    }

    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        List<RexNode> conjunctions = RelOptUtil.conjunctions(join.getCondition());

        RelBuilder builder = call.builder();
        final List<RexNode> additionalConditions = new ArrayList<>();

        // and
        conjunctions.forEach(
                conjunctionRex -> {
                    List<RexNode> disjunctions = RelOptUtil.disjunctions(conjunctionRex);

                    // Apart or: (A.f2 = 'aaa1' and B.f2 = 'bbb1') or (A.f2 = 'aaa2' and B.f2 =
                    // 'bbb2')
                    if (disjunctions.size() > 1) {

                        List<RexNode> leftDisjunctions = new ArrayList<>();
                        List<RexNode> rightDisjunctions = new ArrayList<>();
                        disjunctions.forEach(
                                disjunctionRex -> {
                                    List<RexNode> leftConjunctions = new ArrayList<>();
                                    List<RexNode> rightConjunctions = new ArrayList<>();

                                    // Apart and: A.f2 = 'aaa1' and B.f2 = 'bbb1'
                                    RelOptUtil.conjunctions(disjunctionRex)
                                            .forEach(
                                                    cond -> {
                                                        int[] rCols =
                                                                RelOptUtil.InputFinder.bits(cond)
                                                                        .toArray();

                                                        // May have multi conditions, eg: A.f2 =
                                                        // 'aaa1' and A.f3 = 'aaa3' and B.f2 =
                                                        // 'bbb1'
                                                        if (Arrays.stream(rCols)
                                                                .allMatch(
                                                                        t ->
                                                                                fromJoinLeft(
                                                                                        join, t))) {
                                                            leftConjunctions.add(cond);
                                                        } else if (Arrays.stream(rCols)
                                                                .noneMatch(
                                                                        t ->
                                                                                fromJoinLeft(
                                                                                        join, t))) {
                                                            rightConjunctions.add(cond);
                                                        }
                                                    });

                                    // It is true if conjunction conditions is empty.
                                    leftDisjunctions.add(builder.and(leftConjunctions));
                                    rightDisjunctions.add(builder.and(rightConjunctions));
                                });

                        // TODO Consider whether it is worth doing a filter if we have histogram.
                        if (!leftDisjunctions.isEmpty()) {
                            additionalConditions.add(builder.or(leftDisjunctions));
                        }
                        if (!rightDisjunctions.isEmpty()) {
                            additionalConditions.add(builder.or(rightDisjunctions));
                        }
                    }
                });

        if (!additionalConditions.isEmpty()) {
            conjunctions.addAll(additionalConditions);
            RexNode newCondExp =
                    FlinkRexUtil.simplify(
                            builder.getRexBuilder(),
                            builder.and(conjunctions),
                            join.getCluster().getPlanner().getExecutor());

            if (!newCondExp.equals(join.getCondition())) {
                LogicalJoin newJoin =
                        join.copy(
                                join.getTraitSet(),
                                newCondExp,
                                join.getLeft(),
                                join.getRight(),
                                join.getJoinType(),
                                join.isSemiJoinDone());

                call.transformTo(newJoin);
            }
        }
    }

    /** Returns true if the given index is from join left, else false. */
    private boolean fromJoinLeft(Join join, int index) {
        assert (join.getSystemFieldList().isEmpty());
        return index < join.getLeft().getRowType().getFieldCount();
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface JoinDependentConditionDerivationRuleConfig extends RelRule.Config {
        JoinDependentConditionDerivationRule.JoinDependentConditionDerivationRuleConfig DEFAULT =
                ImmutableJoinDependentConditionDerivationRule
                        .JoinDependentConditionDerivationRuleConfig.builder()
                        .operandSupplier(b0 -> b0.operand(LogicalJoin.class).anyInputs())
                        .description("JoinDependentConditionDerivationRule")
                        .build();

        @Override
        default JoinDependentConditionDerivationRule toRule() {
            return new JoinDependentConditionDerivationRule(this);
        }
    }
}
