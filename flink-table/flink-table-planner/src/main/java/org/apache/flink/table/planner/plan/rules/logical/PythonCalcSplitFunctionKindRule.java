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

import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.utils.PythonUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexVisitorImpl;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import scala.Option;

/** Separates Python functions with different batch representations or execution settings. */
public class PythonCalcSplitFunctionKindRule extends RemoteCalcSplitProjectionRuleBase<Void> {

    public PythonCalcSplitFunctionKindRule(RemoteCallFinder callFinder) {
        super("PythonCalcSplitFunctionKindRule", callFinder);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final FlinkLogicalCalc calc = call.rel(0);
        final RexProgram program = calc.getProgram();
        final Set<List<Object>> settings = new HashSet<>();
        final RexVisitorImpl<Void> visitor =
                new RexVisitorImpl<Void>(true) {
                    @Override
                    public Void visitCall(RexCall node) {
                        if (PythonUtil.isPythonCall(node)) {
                            settings.add(executionSettings(node));
                        }
                        return super.visitCall(node);
                    }
                };
        program.getProjectList().stream()
                .map(program::expandLocalRef)
                .forEach(node -> node.accept(visitor));
        return settings.size() > 1;
    }

    @Override
    public boolean needConvert(RexProgram program, RexNode node, Option<Void> matchState) {
        final List<Object> topLevelSettings =
                program.getProjectList().stream()
                        .map(program::expandLocalRef)
                        .filter(PythonUtil::isPythonCall)
                        .sorted(
                                Comparator.comparingInt(
                                        project ->
                                                PythonUtil.getPythonFunction(project)
                                                        .getPythonFunctionKind()
                                                        .ordinal()))
                        .map(PythonCalcSplitFunctionKindRule::executionSettings)
                        .findFirst()
                        .orElseThrow(
                                () -> new IllegalStateException("Missing top-level Python call."));
        return PythonUtil.isPythonCall(node) && !executionSettings(node).equals(topLevelSettings);
    }

    private static List<Object> executionSettings(RexNode node) {
        final PythonFunction function = PythonUtil.getPythonFunction(node);
        return Arrays.asList(
                function.getPythonFunctionKind(),
                function.getConcurrency(),
                function.getBatchSize());
    }
}
