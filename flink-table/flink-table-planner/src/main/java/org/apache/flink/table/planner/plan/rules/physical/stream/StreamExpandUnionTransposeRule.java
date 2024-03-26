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

import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalExpand;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalUnion;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Rule to transpose the Expand and Union node. */
@Value.Enclosing
public class StreamExpandUnionTransposeRule extends RelRule<StreamExpandUnionTransposeRule.Config> {

    public static final StreamExpandUnionTransposeRule EXPAND_INSTANCE =
            new StreamExpandUnionTransposeRule(Config.EXPAND);

    /**
     * Creates a RelRule.
     *
     * @param config
     */
    protected StreamExpandUnionTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final StreamPhysicalUnion union = call.rel(3);
        final StreamPhysicalCalc calc1 = call.rel(2);
        final StreamPhysicalExpand expand = call.rel(1);
        final StreamPhysicalCalc calc2 = call.rel(0);
        final List<RelNode> unionInputs = union.getInputs();

        List<RelNode> newUnionInputs = new ArrayList<>();
        for (RelNode unionInput : unionInputs) {
            Calc newCalc1 = calc1.copy(calc1.getTraitSet(), unionInput, calc1.getProgram());
            RelNode newExpand =
                    expand.copy(expand.getTraitSet(), Collections.singletonList(newCalc1));
            Calc newCalc2 = calc2.copy(calc2.getTraitSet(), newExpand, calc2.getProgram());
            newUnionInputs.add(newCalc2);
        }

        final StreamPhysicalUnion newUnion =
                new StreamPhysicalUnion(
                        union.getCluster(),
                        union.getTraitSet(),
                        newUnionInputs,
                        union.all,
                        calc2.getRowType());

        call.transformTo(newUnion);
    }

    /** Configuration for {@link StreamExpandUnionTransposeRule}. */
    @Value.Immutable()
    public interface Config extends RelRule.Config {

        Config EXPAND = ImmutableStreamExpandUnionTransposeRule.Config.builder().build().ofExpand();

        @Override
        default RelOptRule toRule() {
            return new StreamExpandUnionTransposeRule(this);
        }

        default Config ofExpand() {

            // Calc -> Expand -> Calc -> Union -> anyInputs
            final RelRule.OperandTransform transform =
                    d ->
                            d.operand(StreamPhysicalCalc.class)
                                    .oneInput(
                                            c ->
                                                    c.operand(StreamPhysicalExpand.class)
                                                            .oneInput(
                                                                    b ->
                                                                            b.operand(
                                                                                            StreamPhysicalCalc
                                                                                                    .class)
                                                                                    .oneInput(
                                                                                            a ->
                                                                                                    a.operand(
                                                                                                                    StreamPhysicalUnion
                                                                                                                            .class)
                                                                                                            .anyInputs())));

            return withOperandSupplier(transform).as(StreamExpandUnionTransposeRule.Config.class);
        }
    }
}
