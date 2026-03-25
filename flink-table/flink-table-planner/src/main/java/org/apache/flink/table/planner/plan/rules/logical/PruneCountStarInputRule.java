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

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.immutables.value.Value;

import java.util.Collections;

/**
 * Planner rule that prunes the input columns of a {@link LogicalAggregate} representing {@code
 * COUNT(*)} (i.e. no group keys, a single COUNT aggregate call with no arguments and no filter) by
 * inserting a project of a constant literal {@code 0} between the aggregate and its input.
 *
 * <p>This avoids reading all columns from the source when only counting rows.
 *
 * <p>Before:
 *
 * <pre>
 * LogicalAggregate(group=[{}], EXPR$0=[COUNT()])
 * +- SomeInput
 * </pre>
 *
 * <p>After:
 *
 * <pre>
 * LogicalAggregate(group=[{}], EXPR$0=[COUNT()])
 * +- LogicalProject($f0=[0])
 *    +- SomeInput
 * </pre>
 */
@Value.Enclosing
public class PruneCountStarInputRule
        extends RelRule<PruneCountStarInputRule.PruneCountStarInputRuleConfig> {

    public static final PruneCountStarInputRule INSTANCE =
            PruneCountStarInputRule.PruneCountStarInputRuleConfig.DEFAULT.toRule();

    protected PruneCountStarInputRule(PruneCountStarInputRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalAggregate agg = call.rel(0);
        final RelNode input = agg.getInput();
        if (agg.getGroupCount() != 0 || agg.getAggCallList().size() != 1) {
            return false;
        }
        final AggregateCall aggCall = agg.getAggCallList().get(0);
        if (aggCall.getAggregation().getKind() != SqlKind.COUNT
                || aggCall.filterArg != -1
                || !aggCall.getArgList().isEmpty()) {
            return false;
        }
        // Only rewrite when the input has more than one field. After the rewrite, the input
        // becomes a single-field Project(0), so this condition naturally prevents repeated
        // application even if other rules in the same phase transform or remove the inserted
        // project.
        return input.getRowType().getFieldCount() > 1;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalAggregate agg = call.rel(0);
        final RelNode input = agg.getInput();

        final RelBuilder relBuilder = call.builder();
        final RelNode newInput = relBuilder.push(input).project(relBuilder.literal(0)).build();
        final RelNode newAgg = agg.copy(agg.getTraitSet(), Collections.singletonList(newInput));
        call.transformTo(newAgg);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface PruneCountStarInputRuleConfig extends RelRule.Config {
        PruneCountStarInputRule.PruneCountStarInputRuleConfig DEFAULT =
                ImmutablePruneCountStarInputRule.PruneCountStarInputRuleConfig.builder()
                        .operandSupplier(b0 -> b0.operand(LogicalAggregate.class).anyInputs())
                        .description("PruneCountStarInputRule")
                        .build();

        @Override
        default PruneCountStarInputRule toRule() {
            return new PruneCountStarInputRule(this);
        }
    }
}
