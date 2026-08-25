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
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Shared helpers for the remote (e.g. Python) call common sub-expression elimination rules, namely
 * {@link RemoteCalcProjectionCseRule} and {@link RemoteCalcConditionProjectionCseRule}.
 *
 * <p>Keeping the reusability predicate in one place ensures both rules agree on which calls may
 * safely share a single evaluation.
 */
class RemoteCalcCseUtil {

    private RemoteCalcCseUtil() {}

    /** Expands the local refs of a calc's projection into self-contained expression trees. */
    static List<RexNode> expandProjects(FlinkLogicalCalc calc) {
        RexProgram program = calc.getProgram();
        return program.getProjectList().stream()
                .map(program::expandLocalRef)
                .collect(Collectors.toList());
    }

    /**
     * Returns true if the node is itself a remote call whose result may be reused. A
     * non-deterministic call must be evaluated once per occurrence, so it never qualifies.
     */
    static boolean isReusableRemoteCall(RexNode node, RemoteCallFinder callFinder) {
        return callFinder.isRemoteCall(node) && isReusable(node);
    }

    /**
     * Returns true if the node contains a remote call and its result may be reused. Unlike {@link
     * #isReusableRemoteCall}, the remote call may be nested inside the expression tree.
     */
    static boolean containsReusableRemoteCall(RexNode node, RemoteCallFinder callFinder) {
        return callFinder.containsRemoteCall(node) && isReusable(node);
    }

    private static boolean isReusable(RexNode node) {
        return ShortcutUtils.isDeterministicThroughProgram(node, null);
    }
}
