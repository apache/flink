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

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalSnapshot;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Snapshot;
import org.apache.calcite.rex.RexOver;
import org.immutables.value.Value;

/** Transpose {@link FlinkLogicalCalc} past into {@link FlinkLogicalSnapshot}. */
@Value.Enclosing
public class CalcSnapshotTransposeRule
        extends RelRule<CalcSnapshotTransposeRule.CalcSnapshotTransposeRuleConfig> {

    public static final CalcSnapshotTransposeRule INSTANCE =
            CalcSnapshotTransposeRule.CalcSnapshotTransposeRuleConfig.DEFAULT.toRule();

    protected CalcSnapshotTransposeRule(CalcSnapshotTransposeRuleConfig config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalCalc calc = call.rel(0);
        // Don't push a calc which contains windowed aggregates into a snapshot for now.
        return !RexOver.containsOver(calc.getProgram());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalCalc calc = call.rel(0);
        FlinkLogicalSnapshot snapshot = call.rel(1);
        Calc newClac = calc.copy(calc.getTraitSet(), snapshot.getInputs());
        Snapshot newSnapshot = snapshot.copy(snapshot.getTraitSet(), newClac, snapshot.getPeriod());
        call.transformTo(newSnapshot);
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface CalcSnapshotTransposeRuleConfig extends RelRule.Config {
        CalcSnapshotTransposeRule.CalcSnapshotTransposeRuleConfig DEFAULT =
                ImmutableCalcSnapshotTransposeRule.CalcSnapshotTransposeRuleConfig.builder()
                        .operandSupplier(
                                b0 ->
                                        b0.operand(FlinkLogicalCalc.class)
                                                .oneInput(
                                                        b1 ->
                                                                b1.operand(
                                                                                FlinkLogicalSnapshot
                                                                                        .class)
                                                                        .anyInputs()))
                        .description("CalcSnapshotTransposeRule")
                        .build();

        @Override
        default CalcSnapshotTransposeRule toRule() {
            return new CalcSnapshotTransposeRule(this);
        }
    }
}
