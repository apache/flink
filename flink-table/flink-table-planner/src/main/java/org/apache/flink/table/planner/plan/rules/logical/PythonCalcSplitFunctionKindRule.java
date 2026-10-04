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
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.utils.PythonUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;

import java.util.Arrays;

import scala.Option;

/** Separates scalar Python functions whose user-facing batch representations differ. */
public class PythonCalcSplitFunctionKindRule extends RemoteCalcSplitProjectionRuleBase<Void> {

    public PythonCalcSplitFunctionKindRule(RemoteCallFinder callFinder) {
        super("PythonCalcSplitFunctionKindRule", callFinder);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final FlinkLogicalCalc calc = call.rel(0);
        final RexProgram program = calc.getProgram();
        return Arrays.stream(PythonFunctionKind.values())
                        .filter(
                                kind ->
                                        program.getProjectList().stream()
                                                .map(program::expandLocalRef)
                                                .anyMatch(
                                                        node ->
                                                                PythonUtil.containsPythonCall(
                                                                        node, kind)))
                        .count()
                > 1;
    }

    @Override
    public boolean needConvert(RexProgram program, RexNode node, Option<Void> matchState) {
        // Keep one top-level kind above the split and extract calls of every other kind.
        final PythonFunctionKind topLevelKind =
                Arrays.stream(PythonFunctionKind.values())
                        .filter(
                                kind ->
                                        program.getProjectList().stream()
                                                .map(program::expandLocalRef)
                                                .anyMatch(
                                                        project ->
                                                                PythonUtil.isPythonCall(
                                                                        project, kind)))
                        .findFirst()
                        .orElseThrow(
                                () -> new IllegalStateException("Missing top-level Python call."));
        return PythonUtil.isPythonCall(node) && !PythonUtil.isPythonCall(node, topLevelKind);
    }
}
