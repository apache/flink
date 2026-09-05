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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.TransformationRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;
import org.immutables.value.Value;

/** Rule for transform {@link MultiJoin} with 2 inputs back to {@link Join}. */
@Value.Enclosing
public class BinaryMultiJoinToJoinRule extends RelRule<BinaryMultiJoinToJoinRule.Config>
        implements TransformationRule {

    public static final BinaryMultiJoinToJoinRule INSTANCE =
            BinaryMultiJoinToJoinRule.Config.DEFAULT.toRule();

    /** Creates a JoinToMultiJoinRule. */
    public BinaryMultiJoinToJoinRule(BinaryMultiJoinToJoinRule.Config config) {
        super(config);
    }

    @Deprecated // to be removed before 2.0
    public BinaryMultiJoinToJoinRule(Class<? extends MultiJoin> clazz) {
        this(BinaryMultiJoinToJoinRule.Config.DEFAULT.withOperandFor(clazz));
    }

    @Deprecated // to be removed before 2.0
    public BinaryMultiJoinToJoinRule(
            Class<? extends MultiJoin> joinClass, RelBuilderFactory relBuilderFactory) {
        this(
                BinaryMultiJoinToJoinRule.Config.DEFAULT
                        .withRelBuilderFactory(relBuilderFactory)
                        .as(BinaryMultiJoinToJoinRule.Config.class)
                        .withOperandFor(joinClass));
    }

    /** This rule matches binary multi joins. */
    @Override
    public boolean matches(RelOptRuleCall call) {
        MultiJoin multiJoin = call.rel(0);
        return isEnabledViaConfig(multiJoin) && multiJoin.getInputs().size() < 3;
    }

    /** This rule transform binary multi joins to regular joins. */
    @Override
    public void onMatch(RelOptRuleCall call) {
        MultiJoin multiJoin = call.rel(0);
        Preconditions.checkArgument(
                multiJoin.getInputs().size() == 2,
                "Only binary multi-join can be transformed into regular join.");

        RexNode condition = multiJoin.getOuterJoinConditions().get(1);
        Join join =
                LogicalJoin.create(
                        multiJoin.getInputs().get(0),
                        multiJoin.getInputs().get(1),
                        multiJoin.getHints(),
                        Preconditions.checkNotNull(condition),
                        multiJoin.getVariablesSet(),
                        multiJoin.getJoinTypes().get(1));
        call.transformTo(join);
    }

    /**
     * Checks if multi-join optimization and not use binary multi join option are enabled via
     * configuration.
     *
     * @param multiJoin the multi join node
     * @return true if TABLE_OPTIMIZER_MULTI_JOIN_ENABLED is set to true
     */
    private boolean isEnabledViaConfig(MultiJoin multiJoin) {
        final TableConfig tableConfig = ShortcutUtils.unwrapTableConfig(multiJoin);
        return tableConfig.get(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTI_JOIN_ENABLED)
                && tableConfig.get(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_USE_MULTI_JOIN_FOR_BINARY_JOIN);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        BinaryMultiJoinToJoinRule.Config DEFAULT =
                ImmutableBinaryMultiJoinToJoinRule.Config.builder()
                        .build()
                        .as(BinaryMultiJoinToJoinRule.Config.class)
                        .withOperandFor(MultiJoin.class);

        @Override
        default BinaryMultiJoinToJoinRule toRule() {
            return new BinaryMultiJoinToJoinRule(this);
        }

        /** Defines an operand tree for the given classes. */
        default BinaryMultiJoinToJoinRule.Config withOperandFor(
                Class<? extends MultiJoin> joinClass) {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(joinClass)
                                            .inputs(
                                                    b1 -> b1.operand(RelNode.class).anyInputs(),
                                                    b2 -> b2.operand(RelNode.class).anyInputs()))
                    .as(BinaryMultiJoinToJoinRule.Config.class);
        }
    }
}
