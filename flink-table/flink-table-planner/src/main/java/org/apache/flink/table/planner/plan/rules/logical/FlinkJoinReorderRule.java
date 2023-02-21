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

import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.LoptOptimizeJoinRule;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.immutables.value.Value;

/**
 * Flink join reorder rule, which can change the join order of input relNode tree.
 *
 * <p>It is triggered by the ({@link MultiJoin}).
 *
 * <p>In this rule, there are two specific join reorder strategies can be chosen, one is {@link
 * LoptOptimizeJoinRule}, another is {@link FlinkBushyJoinReorderRule}. Which rule is selected
 * depends on the parameter {@link
 * OptimizerConfigOptions#TABLE_OPTIMIZER_BUSHY_JOIN_REORDER_THRESHOLD}.
 */
@Value.Enclosing
public class FlinkJoinReorderRule extends RelRule<FlinkJoinReorderRule.Config>
        implements TransformationRule {

    public static final FlinkJoinReorderRule INSTANCE =
            FlinkJoinReorderRule.Config.DEFAULT.toRule();

    private static final LoptOptimizeJoinRule LOPT_JOIN_REORDER =
            LoptOptimizeJoinRule.Config.DEFAULT.toRule();

    private static final FlinkBushyJoinReorderRule BUSHY_JOIN_REORDER =
            FlinkBushyJoinReorderRule.Config.DEFAULT.toRule();

    /** Creates an SparkJoinReorderRule. */
    protected FlinkJoinReorderRule(FlinkJoinReorderRule.Config config) {
        super(config);
    }

    @Deprecated // to be removed before 2.0
    public FlinkJoinReorderRule(RelBuilderFactory relBuilderFactory) {
        this(
                FlinkJoinReorderRule.Config.DEFAULT
                        .withRelBuilderFactory(relBuilderFactory)
                        .as(FlinkJoinReorderRule.Config.class));
    }

    @Deprecated // to be removed before 2.0
    public FlinkJoinReorderRule(
            RelFactories.JoinFactory joinFactory,
            RelFactories.ProjectFactory projectFactory,
            RelFactories.FilterFactory filterFactory) {
        this(RelBuilder.proto(joinFactory, projectFactory, filterFactory));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final MultiJoin multiJoinRel = call.rel(0);
        int numJoinInputs = multiJoinRel.getInputs().size();
        int bushyTreeThreshold =
                ShortcutUtils.unwrapContext(multiJoinRel)
                        .getTableConfig()
                        .get(OptimizerConfigOptions.TABLE_OPTIMIZER_BUSHY_JOIN_REORDER_THRESHOLD);
        if (numJoinInputs <= bushyTreeThreshold) {
            BUSHY_JOIN_REORDER.onMatch(call);
        } else {
            LOPT_JOIN_REORDER.onMatch(call);
        }
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                ImmutableFlinkJoinReorderRule.Config.builder()
                        .build()
                        .withOperandSupplier(b -> b.operand(MultiJoin.class).anyInputs())
                        .as(FlinkJoinReorderRule.Config.class);

        @Override
        default FlinkJoinReorderRule toRule() {
            return new FlinkJoinReorderRule(this);
        }
    }
}
