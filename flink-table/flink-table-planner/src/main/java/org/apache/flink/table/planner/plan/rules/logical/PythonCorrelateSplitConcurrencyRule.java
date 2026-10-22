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
import org.apache.flink.table.planner.plan.utils.AsyncUtil;
import org.apache.flink.table.planner.plan.utils.PythonUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexNode;
import org.immutables.value.Value;

/** Extracts scalar calls whose concurrency conflicts with their Python correlate. */
@Value.Enclosing
public class PythonCorrelateSplitConcurrencyRule
        extends RelRule<PythonCorrelateSplitConcurrencyRule.Config> {

    public static final PythonCorrelateSplitConcurrencyRule INSTANCE = Config.DEFAULT.toRule();

    private PythonCorrelateSplitConcurrencyRule(Config config) {
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
                && !PythonUtil.containsPythonCall(invocation, PythonFunctionKind.PANDAS)
                && !PythonUtil.containsPythonCall(invocation, PythonFunctionKind.ARROW)
                && !AsyncUtil.containsAsyncCall(invocation)
                && PythonUtil.extractDistinctParallelisms(invocation).size() > 1;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final FlinkLogicalCorrelate correlate = call.rel(0);
        final FlinkLogicalTableFunctionScan scan =
                RemoteCorrelateSplitRule.getTableFunctionScan(correlate);
        final PythonCalcSplitConcurrencyRule.ConcurrencyGroup group =
                new PythonCalcSplitConcurrencyRule.ConcurrencyGroup(scan.getCall());
        call.transformTo(
                RemoteCorrelateSplitRule.splitCorrelate(
                        correlate,
                        node -> PythonUtil.isPythonCall(node) && group.contains(node),
                        new PythonRemoteCallFinder()));
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        Config DEFAULT =
                ImmutablePythonCorrelateSplitConcurrencyRule.Config.builder()
                        .description("PythonCorrelateSplitConcurrencyRule")
                        .operandSupplier(b -> b.operand(FlinkLogicalCorrelate.class).anyInputs())
                        .build();

        @Override
        default PythonCorrelateSplitConcurrencyRule toRule() {
            return new PythonCorrelateSplitConcurrencyRule(this);
        }
    }
}
