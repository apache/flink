/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.plan.utils.AsyncUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.Optional;

/**
 * Rule to split Python async scalar functions from a Calc node into a separate AsyncCalc node. This
 * ensures Python async scalar functions are executed in a dedicated async operator.
 *
 * <p>Similar to {@link AsyncCalcSplitRule}, but specifically handles Python async scalar functions
 * by checking if the function is a {@link PythonFunction} and has async scalar function kind.
 */
public class PythonAsyncCalcSplitRule {

    private static final RemoteCallFinder PYTHON_ASYNC_CALL_FINDER =
            new PythonAsyncRemoteCallFinder();

    public static final RelOptRule SPLIT_CONDITION =
            new RemoteCalcSplitConditionRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule SPLIT_PROJECT =
            new RemoteCalcSplitProjectionRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule SPLIT_PROJECTION_REX_FIELD =
            new RemoteCalcSplitProjectionRexFieldRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule SPLIT_CONDITION_REX_FIELD =
            new RemoteCalcSplitConditionRexFieldRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule EXPAND_PROJECT =
            new RemoteCalcExpandProjectRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule PUSH_CONDITION =
            new RemoteCalcPushConditionRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule REWRITE_PROJECT =
            new RemoteCalcRewriteProjectionRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule NESTED_SPLIT =
            new AsyncCalcSplitRule.AsyncCalcSplitNestedRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule ONE_PER_CALC_SPLIT =
            new AsyncCalcSplitRule.AsyncCalcSplitOnePerCalcRule(PYTHON_ASYNC_CALL_FINDER);
    public static final RelOptRule NO_ASYNC_JOIN_CONDITIONS =
            new SplitRemoteConditionFromJoinRule(
                    PYTHON_ASYNC_CALL_FINDER,
                    JavaScalaConversionUtil.toScala(
                            Optional.of(
                                    "Python AsyncScalarFunction not supported for non inner join condition")));

    /**
     * Finder for Python async scalar functions. Extends AsyncRemoteCallFinder to also check if the
     * function is a Python function.
     */
    private static class PythonAsyncRemoteCallFinder extends AsyncUtil.AsyncRemoteCallFinder {

        public PythonAsyncRemoteCallFinder() {
            super(FunctionKind.ASYNC_SCALAR);
        }

        @Override
        public boolean containsRemoteCall(RexNode node) {
            if (!super.containsRemoteCall(node)) {
                return false;
            }

            // Additional check: must be a Python function
            if (node instanceof RexCall) {
                RexCall call = (RexCall) node;
                if (call.getOperator() instanceof BridgingSqlFunction) {
                    BridgingSqlFunction function = (BridgingSqlFunction) call.getOperator();
                    Object definition = ShortcutUtils.unwrapFunctionDefinition(call);
                    if (definition instanceof PythonFunction) {
                        return function.getDefinition().getKind() == FunctionKind.ASYNC_SCALAR;
                    }
                }
            }
            return false;
        }
    }
}
