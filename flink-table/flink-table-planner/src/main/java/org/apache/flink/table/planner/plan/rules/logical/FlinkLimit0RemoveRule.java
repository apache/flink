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
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexLiteral;
import org.immutables.value.Value;

/** Planner rule that rewrites `limit 0` to empty {@link org.apache.calcite.rel.core.Values}. */
@Value.Enclosing
public class FlinkLimit0RemoveRule
        extends RelRule<FlinkLimit0RemoveRule.FlinkLimit0RemoveRuleConfig> {

    public static final FlinkLimit0RemoveRule INSTANCE =
            FlinkLimit0RemoveRule.FlinkLimit0RemoveRuleConfig.DEFAULT.toRule();

    private FlinkLimit0RemoveRule(FlinkLimit0RemoveRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Sort sort = call.rel(0);
        return sort.fetch != null && RexLiteral.intValue(sort.fetch) == 0;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Sort sort = call.rel(0);
        RelNode emptyValues = call.builder().values(sort.getRowType()).build();
        call.transformTo(emptyValues);

        // New plan is absolutely better than old plan.
        call.getPlanner().prune(sort);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface FlinkLimit0RemoveRuleConfig extends RelRule.Config {
        FlinkLimit0RemoveRule.FlinkLimit0RemoveRuleConfig DEFAULT =
                ImmutableFlinkLimit0RemoveRule.FlinkLimit0RemoveRuleConfig.builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(Sort.class).anyInputs())
                        .withDescription("FlinkLimit0RemoveRule");

        @Override
        default FlinkLimit0RemoveRule toRule() {
            return new FlinkLimit0RemoveRule(this);
        }
    }
}
