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

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalUnion;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalWindowTableFunction;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.immutables.value.Value;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Rule to transpose the Calc -> WindowTableFunction -> Union to Union -> Calc ->
 * WindowTableFunction.
 */
@Value.Enclosing
public class BatchWindowTVFUnionTransposeRule
        extends RelRule<BatchWindowTVFUnionTransposeRule.Config> {

    public static final BatchWindowTVFUnionTransposeRule INSTANCE =
            new BatchWindowTVFUnionTransposeRule(Config.DEFAULT);

    /**
     * Creates a RelRule.
     *
     * @param config
     */
    protected BatchWindowTVFUnionTransposeRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final BatchPhysicalUnion union = call.rel(2);
        final BatchPhysicalWindowTableFunction windowTF = call.rel(1);
        final BatchPhysicalCalc calc = call.rel(0);
        final List<RelNode> unionInputs = union.getInputs();

        List<RelNode> newUnionInputs = new ArrayList<>();
        for (RelNode unionInput : unionInputs) {
            RelNode newWinFunc =
                    windowTF.copy(windowTF.getTraitSet(), Collections.singletonList(unionInput));
            Calc newCalc = calc.copy(calc.getTraitSet(), newWinFunc, calc.getProgram());
            newUnionInputs.add(newCalc);
        }

        final BatchPhysicalUnion newUnion =
                new BatchPhysicalUnion(
                        union.getCluster(),
                        union.getTraitSet(),
                        newUnionInputs,
                        union.all,
                        calc.getRowType());

        call.transformTo(newUnion);
    }

    @Value.Immutable(singleton = false)
    interface Config extends RelRule.Config {

        BatchWindowTVFUnionTransposeRule.Config DEFAULT =
                ImmutableBatchWindowTVFUnionTransposeRule.Config.builder()
                        .build()
                        .ofWindowFunction();

        @Override
        default RelOptRule toRule() {
            return new BatchWindowTVFUnionTransposeRule(this);
        }

        // Calc -> WindowTableFunction -> Union
        default Config ofWindowFunction() {

            final RelRule.OperandTransform transform =
                    a ->
                            a.operand(BatchPhysicalCalc.class)
                                    .oneInput(
                                            b ->
                                                    b.operand(
                                                                    BatchPhysicalWindowTableFunction
                                                                            .class)
                                                            .oneInput(
                                                                    c ->
                                                                            c.operand(
                                                                                            BatchPhysicalUnion
                                                                                                    .class)
                                                                                    .anyInputs()));

            return withOperandSupplier(transform)
                    .withDescription("BatchPhysicalWindowTVFUnionTransposeRule")
                    .as(BatchWindowTVFUnionTransposeRule.Config.class);
        }
    }
}
