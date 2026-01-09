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

import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.nodes.FlinkConventions;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamPhysicalPythonAsyncCalc;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A physical rule for identifying logical async Python Calcs and converting it to a physical
 * RelNode.
 *
 * <p>This rule matches Calc nodes that contain Python async scalar functions and converts them to
 * {@link StreamPhysicalPythonAsyncCalc} nodes for async execution.
 */
public class StreamPhysicalPythonAsyncCalcRule extends ConverterRule {
    public static final RelOptRule INSTANCE =
            new StreamPhysicalPythonAsyncCalcRule(
                    Config.INSTANCE.withConversion(
                            FlinkLogicalCalc.class,
                            FlinkConventions.LOGICAL(),
                            FlinkConventions.STREAM_PHYSICAL(),
                            "StreamPhysicalPythonAsyncCalcRule"));

    protected StreamPhysicalPythonAsyncCalcRule(Config config) {
        super(config);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalCalc calc = call.rel(0);
        RexProgram program = calc.getProgram();
        return program.getExprList().stream().anyMatch(this::containsPythonAsyncCall);
    }

    /**
     * Checks if a RexNode contains a Python async scalar function call.
     *
     * @param node The RexNode to check
     * @return true if the node contains a Python async scalar function, false otherwise
     */
    private boolean containsPythonAsyncCall(RexNode node) {
        if (node instanceof RexCall) {
            RexCall call = (RexCall) node;

            // Check if this is a function call with async scalar kind
            if (call.getOperator() instanceof BridgingSqlFunction) {
                BridgingSqlFunction function = (BridgingSqlFunction) call.getOperator();
                Object definition = ShortcutUtils.unwrapFunctionDefinition(call);

                // Must be a Python function with ASYNC_SCALAR kind
                if (definition instanceof PythonFunction
                        && function.getDefinition().getKind() == FunctionKind.ASYNC_SCALAR) {
                    return true;
                }
            }

            // Recursively check operands
            return call.getOperands().stream().anyMatch(this::containsPythonAsyncCall);
        }
        return false;
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        FlinkLogicalCalc calc = (FlinkLogicalCalc) rel;
        RelTraitSet traitSet = rel.getTraitSet().replace(FlinkConventions.STREAM_PHYSICAL());
        RelNode newInput = RelOptRule.convert(calc.getInput(), FlinkConventions.STREAM_PHYSICAL());

        return new StreamPhysicalPythonAsyncCalc(
                rel.getCluster(), traitSet, newInput, calc.getProgram(), rel.getRowType());
    }
}
