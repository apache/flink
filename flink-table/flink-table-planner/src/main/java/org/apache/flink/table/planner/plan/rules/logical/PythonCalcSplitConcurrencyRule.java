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
import org.apache.flink.table.planner.plan.utils.PythonUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.util.Optional;

import scala.Option;

/**
 * Splits projections containing Python functions with different explicit concurrency values.
 * Functions without explicit concurrency can remain with any explicit group.
 */
public class PythonCalcSplitConcurrencyRule extends RemoteCalcSplitProjectionRuleBase<Integer> {

    public PythonCalcSplitConcurrencyRule(RemoteCallFinder callFinder) {
        super("PythonCalcSplitConcurrencyRule", callFinder);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final FlinkLogicalCalc calc = call.rel(0);
        return calc.getProgram().getProjectList().stream()
                        .map(calc.getProgram()::expandLocalRef)
                        .flatMap(node -> PythonUtil.extractDistinctParallelisms(node).stream())
                        .distinct()
                        .count()
                > 1;
    }

    @Override
    public boolean needConvert(RexProgram program, RexNode node, Option<Integer> matchState) {
        if (!callFinder().isRemoteCall(node)) {
            return false;
        }

        final Optional<Integer> ownParallelism = PythonUtil.getOwnParallelism(node);
        if (!ownParallelism.isPresent()) {
            return false;
        }

        final Optional<Integer> targetParallelism =
                program.getProjectList().stream()
                        .map(program::expandLocalRef)
                        .map(PythonUtil::firstExplicitParallelism)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .findFirst();
        return targetParallelism.isPresent()
                && !targetParallelism.get().equals(ownParallelism.get());
    }
}
