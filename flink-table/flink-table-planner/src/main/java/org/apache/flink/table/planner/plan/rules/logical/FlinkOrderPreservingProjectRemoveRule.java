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

import org.apache.flink.table.planner.plan.nodes.logical.FlinkOrderPreservingProjection;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.rules.TransformationRule;
import org.immutables.value.Value;

/** Rule to remove projections created by FlinkRightToLeftJoinRule. */
@Value.Enclosing
public class FlinkOrderPreservingProjectRemoveRule
        extends RelRule<FlinkOrderPreservingProjectRemoveRule.Config>
        implements TransformationRule {

    public static final FlinkOrderPreservingProjectRemoveRule INSTANCE =
            FlinkOrderPreservingProjectRemoveRule.Config.DEFAULT.toRule();

    /** Creates a FlinkOrderPreservingProjectRemoveRule. */
    public FlinkOrderPreservingProjectRemoveRule(
            FlinkOrderPreservingProjectRemoveRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkOrderPreservingProjection project = call.rel(0);
        call.transformTo(project.getDelegate());
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        FlinkOrderPreservingProjectRemoveRule.Config DEFAULT =
                (Config)
                        ImmutableFlinkOrderPreservingProjectRemoveRule.Config.builder()
                                .build()
                                .as(Config.class)
                                .withOperandSupplier(
                                        b0 ->
                                                b0.operand(FlinkOrderPreservingProjection.class)
                                                        .anyInputs());

        @Override
        default FlinkOrderPreservingProjectRemoveRule toRule() {
            return new FlinkOrderPreservingProjectRemoveRule(this);
        }
    }
}
