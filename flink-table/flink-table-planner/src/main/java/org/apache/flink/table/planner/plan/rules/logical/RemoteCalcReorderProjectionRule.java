/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.plan.rules.logical;

import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.util.List;
import java.util.stream.Collectors;

import scala.Option;

/**
 * Enforces that {@link RexInputRef} projections precede {@link RexCall} projections in a {@link
 * FlinkLogicalCalc} containing remote calls.
 *
 * <p>{@link RemoteCalcRewriteProjectionRule} moves expressions other than {@link RexInputRef} and
 * {@link RexCall} out of the remote calc and also establishes this projection order. Subsequent
 * optimizations, such as async calc splitting, can disrupt that order. This rule restores the
 * required order after those optimizations.
 */
public class RemoteCalcReorderProjectionRule extends RemoteCalcSplitProjectionRuleBase<Void> {

    public RemoteCalcReorderProjectionRule(RemoteCallFinder callFinder) {
        super("RemoteCalcReorderProjectionRule", callFinder);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final FlinkLogicalCalc calc = call.rel(0);
        final RexProgram program = calc.getProgram();
        final List<RexNode> projects =
                program.getProjectList().stream()
                        .map(program::expandLocalRef)
                        .collect(Collectors.toList());
        if (projects.stream().noneMatch(callFinder()::containsRemoteCall)) {
            return false;
        }
        boolean seenCall = false;
        for (RexNode project : projects) {
            if (project instanceof RexCall) {
                seenCall = true;
            } else if (project instanceof RexInputRef && seenCall) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean needConvert(RexProgram program, RexNode node, Option<Void> matchState) {
        return callFinder().isRemoteCall(node);
    }
}
