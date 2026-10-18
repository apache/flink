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
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import scala.Option;

/**
 * Splits projections containing Python functions with different explicit concurrency values.
 * Functions without explicit concurrency can remain with any explicit group.
 */
public class PythonCalcSplitConcurrencyRule
        extends RemoteCalcSplitProjectionRuleBase<PythonCalcSplitConcurrencyRule.ConcurrencyGroup> {

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
    public Option<ConcurrencyGroup> getMatchState() {
        return Option.apply(new ConcurrencyGroup());
    }

    @Override
    public boolean needConvert(
            RexProgram program, RexNode node, Option<ConcurrencyGroup> matchState) {
        if (!callFinder().isRemoteCall(node)) {
            return false;
        }
        final ConcurrencyGroup group = matchState.get();
        if (group.parallelism == null) {
            group.parallelism =
                    program.getProjectList().stream()
                            .map(program::expandLocalRef)
                            .map(PythonUtil::firstExplicitParallelism)
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .findFirst()
                            .get();
        }
        return group.contains(node);
    }

    /** Selects a lower group whose calls do not depend on another explicit concurrency. */
    static class ConcurrencyGroup {
        private Integer parallelism;
        private final Map<RexNode, Boolean> membership = new HashMap<>();

        ConcurrencyGroup() {}

        ConcurrencyGroup(RexNode invocation) {
            parallelism = PythonUtil.firstExplicitParallelism(invocation).get();
        }

        boolean contains(RexNode node) {
            final Boolean known = membership.get(node);
            if (known != null) {
                return known;
            }
            boolean compatible =
                    PythonUtil.getOwnParallelism(node)
                            .map(value -> value.equals(parallelism))
                            .orElse(true);
            if (compatible && node instanceof RexCall) {
                compatible = ((RexCall) node).getOperands().stream().allMatch(this::contains);
            } else if (compatible && node instanceof RexFieldAccess) {
                compatible = contains(((RexFieldAccess) node).getReferenceExpr());
            }
            membership.put(node, compatible);
            return compatible;
        }
    }
}
