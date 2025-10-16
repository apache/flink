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

package org.apache.flink.table.planner.plan.rules.physical.stream;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DeltaJoinSpec;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalDeltaJoin;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalJoin;
import org.apache.flink.table.planner.plan.utils.DeltaJoinUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.immutables.value.Value;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig;

/**
 * A rule that converts a {@link StreamPhysicalJoin} to a {@link StreamPhysicalDeltaJoin} if all
 * conditions are matched.
 *
 * <p>Currently, only {@link StreamPhysicalJoin} satisfied the following requirements can be
 * converted to {@link StreamPhysicalDeltaJoin}.
 *
 * <ol>
 *   <li>The join is INNER join.
 *   <li>There is at least one join key pair in the join.
 *   <li>The downstream nodes of this join can accept duplicate changes.
 *   <li>All join inputs are with changelog "I" or "I, UA".
 *   <li>If this join outputs update records, the non-equiv conditions must be applied on upsert
 *       keys of this join.
 *   <li>All upstream nodes of this join are in {@code
 *       DeltaJoinUtil#ALL_SUPPORTED_DELTA_JOIN_UPSTREAM_NODES}
 *   <li>The join keys include at least one complete index in each source table of the join input.
 *   <li>All table sources of this join inputs support async {@link LookupTableSource}.
 * </ol>
 *
 * <p>See more at {@link DeltaJoinUtil#canConvertToDeltaJoin}.
 */
@Value.Enclosing
public class DeltaJoinRewriteRule extends RelRule<DeltaJoinRewriteRule.Config> {

    public static final DeltaJoinRewriteRule INSTANCE =
            DeltaJoinRewriteRule.Config.DEFAULT.toRule();

    private DeltaJoinRewriteRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        TableConfig tableConfig = unwrapTableConfig(call);
        OptimizerConfigOptions.DeltaJoinStrategy deltaJoinStrategy =
                tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_DELTA_JOIN_STRATEGY);
        if (OptimizerConfigOptions.DeltaJoinStrategy.NONE == deltaJoinStrategy) {
            return false;
        }

        StreamPhysicalJoin join = call.rel(0);
        return DeltaJoinUtil.canConvertToDeltaJoin(join);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        StreamPhysicalJoin join = call.rel(0);
        StreamPhysicalDeltaJoin deltaJoin = convertToDeltaJoin(join);
        call.transformTo(deltaJoin);
    }

    private StreamPhysicalDeltaJoin convertToDeltaJoin(StreamPhysicalJoin join) {
        DeltaJoinSpec lookupRightTableJoinSpec = DeltaJoinUtil.getDeltaJoinSpec(join, true);
        DeltaJoinSpec lookupLeftTableJoinSpec = DeltaJoinUtil.getDeltaJoinSpec(join, false);
        return new StreamPhysicalDeltaJoin(
                join.getCluster(),
                join.getTraitSet(),
                join.getHints(),
                join.getLeft(),
                join.getRight(),
                join.getJoinType(),
                join.getCondition(),
                lookupRightTableJoinSpec,
                lookupLeftTableJoinSpec,
                join.getRowType());
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        DeltaJoinRewriteRule.Config DEFAULT =
                ImmutableDeltaJoinRewriteRule.Config.builder()
                        .build()
                        .withOperandSupplier(b0 -> b0.operand(StreamPhysicalJoin.class).anyInputs())
                        .withDescription("DeltaJoinRewriteRule");

        @Override
        default DeltaJoinRewriteRule toRule() {
            return new DeltaJoinRewriteRule(this);
        }
    }
}
