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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.utils.LateralSnapshotJoinUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexCall;
import org.immutables.value.Value;

/**
 * Rejects any {@link FlinkLogicalTableFunctionScan} that is still backed by the built-in {@code
 * SNAPSHOT} function, with a clear error message.
 *
 * <p>{@code SNAPSHOT} is a planner placeholder that is only valid as the build side of a {@code
 * LATERAL} join, where {@link LogicalJoinToLateralSnapshotJoinRule} rewrites the surrounding join
 * into a dedicated {@link
 * org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalLateralSnapshotJoin} and removes
 * the SNAPSHOT scan. This rule must therefore run <em>after</em> that rewrite (see {@code
 * FlinkStreamRuleSets.LOGICAL_REWRITE}).
 */
@Value.Enclosing
public class ForbidSnapshotOutsideLateralRule
        extends RelRule<ForbidSnapshotOutsideLateralRule.ForbidSnapshotOutsideLateralRuleConfig> {

    public static final ForbidSnapshotOutsideLateralRule INSTANCE =
            ForbidSnapshotOutsideLateralRule.ForbidSnapshotOutsideLateralRuleConfig.DEFAULT
                    .toRule();

    private ForbidSnapshotOutsideLateralRule(ForbidSnapshotOutsideLateralRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final FlinkLogicalTableFunctionScan scan = call.rel(0);
        return scan.getCall() instanceof RexCall
                && LateralSnapshotJoinUtil.isSnapshotCall((RexCall) scan.getCall());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        throw new ValidationException(
                "The SNAPSHOT function can only be used as the build side (right-hand side) of a "
                        + "LATERAL join. It cannot be used as a standalone table function or "
                        + "outside of a LATERAL context.");
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface ForbidSnapshotOutsideLateralRuleConfig extends RelRule.Config {

        ForbidSnapshotOutsideLateralRule.ForbidSnapshotOutsideLateralRuleConfig DEFAULT =
                ImmutableForbidSnapshotOutsideLateralRule.ForbidSnapshotOutsideLateralRuleConfig
                        .builder()
                        .build()
                        .withOperandSupplier(
                                b0 -> b0.operand(FlinkLogicalTableFunctionScan.class).anyInputs())
                        .withDescription("ForbidSnapshotOutsideLateralRule");

        @Override
        default ForbidSnapshotOutsideLateralRule toRule() {
            return new ForbidSnapshotOutsideLateralRule(this);
        }
    }
}
