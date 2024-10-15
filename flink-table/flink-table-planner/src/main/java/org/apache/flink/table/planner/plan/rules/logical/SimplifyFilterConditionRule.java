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
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.immutables.value.Value;

import java.util.Optional;

/**
 * Planner rule that apply various simplifying transformations on filter condition.
 *
 * <p>If `simplifySubQuery` is true, this rule will also simplify the filter condition in {@link
 * RexSubQuery}.
 */
@Value.Enclosing
public class SimplifyFilterConditionRule
        extends RelRule<SimplifyFilterConditionRule.SimplifyFilterConditionRuleConfig> {

    public static final SimplifyFilterConditionRule INSTANCE =
            SimplifyFilterConditionRule.SimplifyFilterConditionRuleConfig.DEFAULT.toRule();
    public static final SimplifyFilterConditionRule EXTENDED =
            SimplifyFilterConditionRule.SimplifyFilterConditionRuleConfig.DEFAULT
                    .withDescription("SimplifyFilterConditionRule:simplifySubQuery")
                    .as(SimplifyFilterConditionRule.SimplifyFilterConditionRuleConfig.class)
                    .withIsSimplifySubQuery(true)
                    .toRule();

    protected SimplifyFilterConditionRule(SimplifyFilterConditionRuleConfig config) {
        super(config);
    }

    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        boolean[] changed = new boolean[] {false};
        Optional<Filter> newFilter = simplify(filter, changed);
        if (newFilter.isPresent()) {
            call.transformTo(newFilter.get());
            call.getPlanner().prune(filter);
        }
    }

    public Optional<Filter> simplify(Filter filter, boolean[] changed) {
        RexNode condition =
                config.isSimplifySubQuery()
                        ? simplifyFilterConditionInSubQuery(filter.getCondition(), changed)
                        : filter.getCondition();

        RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        RexNode simplifiedCondition =
                FlinkRexUtil.simplify(
                        rexBuilder, condition, filter.getCluster().getPlanner().getExecutor());
        RexNode newCondition = RexUtil.pullFactors(rexBuilder, simplifiedCondition);

        if (!changed[0] && !condition.equals(newCondition)) {
            changed[0] = true;
        }

        // just replace modified RexNode
        return changed[0]
                ? Optional.of(filter.copy(filter.getTraitSet(), filter.getInput(), newCondition))
                : Optional.empty();
    }

    private RexNode simplifyFilterConditionInSubQuery(RexNode condition, boolean[] changed) {
        return condition.accept(
                new RexShuttle() {
                    public RexNode visitSubQuery(RexSubQuery subQuery) {
                        RelNode newRel =
                                subQuery.rel.accept(
                                        new RelShuttleImpl() {
                                            public RelNode visit(LogicalFilter filter) {
                                                return simplify(filter, changed).orElse(filter);
                                            }
                                        });
                        if (changed[0]) {
                            return subQuery.clone(newRel);
                        } else {
                            return subQuery;
                        }
                    }
                });
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    @Value.Style(
            get = {"is*", "get*"},
            init = "with*",
            defaults = @Value.Immutable(copy = false))
    public interface SimplifyFilterConditionRuleConfig extends RelRule.Config {
        SimplifyFilterConditionRuleConfig DEFAULT =
                ImmutableSimplifyFilterConditionRule.SimplifyFilterConditionRuleConfig.builder()
                        .description("SimplifyFilterConditionRule")
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(Filter.class).anyInputs());

        @Value.Default
        default boolean isSimplifySubQuery() {
            return false;
        }

        /** Sets {@link #isSimplifySubQuery()}. */
        SimplifyFilterConditionRuleConfig withIsSimplifySubQuery(boolean simplifySubQuery);

        @Override
        default SimplifyFilterConditionRule toRule() {
            return new SimplifyFilterConditionRule(this);
        }
    }
}
