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

import org.apache.flink.table.functions.python.PythonFunctionKind;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCorrelate;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableFunctionScan;
import org.apache.flink.table.planner.plan.utils.PythonUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

import java.util.Arrays;

/** Extracts scalar calls whose batch representation cannot execute in a Python correlate. */
@Value.Enclosing
public class PythonCorrelateSplitFunctionKindRule
        extends RelRule<PythonCorrelateSplitFunctionKindRule.Config> {

    public static final PythonCorrelateSplitFunctionKindRule INSTANCE = Config.DEFAULT.toRule();

    private PythonCorrelateSplitFunctionKindRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final FlinkLogicalTableFunctionScan scan =
                RemoteCorrelateSplitRule.getTableFunctionScan(call.rel(0));
        if (scan == null) {
            return false;
        }
        final RexNode invocation = scan.getCall();
        return PythonUtil.isPythonCall(invocation, PythonFunctionKind.GENERAL)
                && Arrays.stream(PythonFunctionKind.values())
                        .filter(kind -> kind != PythonFunctionKind.GENERAL)
                        .anyMatch(kind -> PythonUtil.containsPythonCall(invocation, kind));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        call.transformTo(
                RemoteCorrelateSplitRule.splitCorrelate(
                        call.rel(0),
                        node ->
                                PythonUtil.isPythonCall(node)
                                        && !PythonUtil.isPythonCall(
                                                node, PythonFunctionKind.GENERAL),
                        new PythonRemoteCallFinder()));
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                ImmutablePythonCorrelateSplitFunctionKindRule.Config.builder()
                        .description("PythonCorrelateSplitFunctionKindRule")
                        .operandSupplier(b -> b.operand(FlinkLogicalCorrelate.class).anyInputs())
                        .build();

        @Override
        default PythonCorrelateSplitFunctionKindRule toRule() {
            return new PythonCorrelateSplitFunctionKindRule(this);
        }
    }
}
