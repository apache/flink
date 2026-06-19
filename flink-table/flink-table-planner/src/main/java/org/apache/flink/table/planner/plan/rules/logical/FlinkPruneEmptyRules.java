/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.rules.SubstitutionRule;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * This rule is copied from Calcite's {@link org.apache.calcite.rel.rules.PruneEmptyRules} because
 * of * CALCITE-6955 * and should be removed after upgraded to calcite-1.40.0.
 */
public class FlinkPruneEmptyRules {

    public static final RelOptRule UNION_INSTANCE =
            FlinkPruneEmptyRules.UnionEmptyPruneRuleConfig.DEFAULT.toRule();

    public static final RelOptRule MINUS_INSTANCE =
            FlinkPruneEmptyRules.MinusEmptyPruneRuleConfig.DEFAULT.toRule();

    /** Configuration for a rule that prunes empty inputs from a Minus. */
    @Value.Immutable
    public interface UnionEmptyPruneRuleConfig
            extends FlinkPruneEmptyRules.FlinkPruneEmptyRule.Config {
        FlinkPruneEmptyRules.UnionEmptyPruneRuleConfig DEFAULT =
                ImmutableUnionEmptyPruneRuleConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(Union.class)
                                                .unorderedInputs(
                                                        b1 ->
                                                                b1.operand(Values.class)
                                                                        .predicate(Values::isEmpty)
                                                                        .noInputs()))
                        .withDescription("Union");

        @Override
        default FlinkPruneEmptyRules.FlinkPruneEmptyRule toRule() {
            return new FlinkPruneEmptyRules.FlinkPruneEmptyRule(this) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    final Union union = call.rel(0);
                    final List<RelNode> inputs = requireNonNull(union.getInputs());
                    final RelBuilder relBuilder = call.builder();
                    int nonEmptyInputs = 0;
                    for (RelNode input : inputs) {
                        if (!isEmpty(input)) {
                            relBuilder.push(input);
                            nonEmptyInputs++;
                        }
                    }
                    assert nonEmptyInputs < inputs.size()
                            : "planner promised us at least one Empty child: "
                                    + RelOptUtil.toString(union);
                    if (nonEmptyInputs == 0) {
                        relBuilder.push(union).empty();
                    } else if (nonEmptyInputs == 1 && !union.all) {
                        relBuilder.distinct();
                        relBuilder.convert(union.getRowType(), true);
                    } else {
                        relBuilder.union(union.all, nonEmptyInputs);
                        relBuilder.convert(union.getRowType(), true);
                    }
                    call.transformTo(relBuilder.build());
                }
            };
        }
    }

    /** Configuration for a rule that prunes empty inputs from a Minus. */
    @Value.Immutable
    public interface MinusEmptyPruneRuleConfig
            extends FlinkPruneEmptyRules.FlinkPruneEmptyRule.Config {
        FlinkPruneEmptyRules.MinusEmptyPruneRuleConfig DEFAULT =
                ImmutableMinusEmptyPruneRuleConfig.builder()
                        .build()
                        .withOperandSupplier(
                                b0 ->
                                        b0.operand(Minus.class)
                                                .unorderedInputs(
                                                        b1 ->
                                                                b1.operand(Values.class)
                                                                        .predicate(Values::isEmpty)
                                                                        .noInputs()))
                        .withDescription("Minus");

        @Override
        default FlinkPruneEmptyRules.FlinkPruneEmptyRule toRule() {
            return new FlinkPruneEmptyRules.FlinkPruneEmptyRule(this) {
                @Override
                public void onMatch(RelOptRuleCall call) {
                    final Minus minus = call.rel(0);
                    final List<RelNode> inputs = requireNonNull(minus.getInputs());
                    int nonEmptyInputs = 0;
                    final RelBuilder relBuilder = call.builder();
                    for (RelNode input : inputs) {
                        if (!isEmpty(input)) {
                            relBuilder.push(input);
                            nonEmptyInputs++;
                        } else if (nonEmptyInputs == 0) {
                            // If the first input of Minus is empty, the whole thing is
                            // empty.
                            break;
                        }
                    }
                    assert nonEmptyInputs < inputs.size()
                            : "planner promised us at least one Empty child: "
                                    + RelOptUtil.toString(minus);
                    if (nonEmptyInputs == 0) {
                        relBuilder.push(minus).empty();
                    } else if (nonEmptyInputs == 1 && !minus.all) {
                        relBuilder.distinct();
                        relBuilder.convert(minus.getRowType(), true);
                    } else {
                        relBuilder.minus(minus.all, nonEmptyInputs);
                        relBuilder.convert(minus.getRowType(), true);
                    }
                    call.transformTo(relBuilder.build());
                }
            };
        }
    }

    private static boolean isEmpty(RelNode node) {
        if (node instanceof Values) {
            return ((Values) node).getTuples().isEmpty();
        }
        if (node instanceof HepRelVertex) {
            return isEmpty(((HepRelVertex) node).getCurrentRel());
        }
        // Note: relation input might be a RelSubset, so we just iterate over the relations
        // in order to check if the subset is equivalent to an empty relation.
        if (!(node instanceof RelSubset)) {
            return false;
        }
        RelSubset subset = (RelSubset) node;
        for (RelNode rel : subset.getRels()) {
            if (isEmpty(rel)) {
                return true;
            }
        }
        return false;
    }

    /** Abstract prune empty rule that implements SubstitutionRule interface. */
    protected abstract static class FlinkPruneEmptyRule
            extends RelRule<FlinkPruneEmptyRules.FlinkPruneEmptyRule.Config>
            implements SubstitutionRule {
        protected FlinkPruneEmptyRule(FlinkPruneEmptyRule.Config config) {
            super(config);
        }

        @Override
        public boolean autoPruneOld() {
            return true;
        }

        /** Rule configuration. */
        public interface Config extends RelRule.Config {
            @Override
            FlinkPruneEmptyRules.FlinkPruneEmptyRule toRule();
        }
    }
}
