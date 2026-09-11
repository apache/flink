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
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import scala.Tuple2;

/**
 * Pushes a filter condition from the {@link FlinkLogicalCalc} and into a {@link
 * FlinkLogicalTableSourceScan}.
 */
public class PushFilterInCalcIntoTableSourceScanRule extends PushFilterIntoSourceScanRuleBase {
    public static final PushFilterInCalcIntoTableSourceScanRule INSTANCE =
            new PushFilterInCalcIntoTableSourceScanRule();

    public PushFilterInCalcIntoTableSourceScanRule() {
        super(
                operand(FlinkLogicalCalc.class, operand(FlinkLogicalTableSourceScan.class, none())),
                "PushFilterInCalcIntoTableSourceScanRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!super.matches(call)) {
            return false;
        }

        FlinkLogicalCalc calc = call.rel(0);
        RexProgram originProgram = calc.getProgram();

        if (originProgram.getCondition() == null) {
            return false;
        }

        FlinkLogicalTableSourceScan scan = call.rel(1);
        TableSourceTable tableSourceTable = scan.getTable().unwrap(TableSourceTable.class);
        return canPushdownFilter(tableSourceTable) || canPushdownMetadataFilter(tableSourceTable);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalCalc calc = call.rel(0);
        FlinkLogicalTableSourceScan scan = call.rel(1);
        TableSourceTable table = scan.getTable().unwrap(TableSourceTable.class);
        pushFilterIntoScan(call, calc, scan, table);
    }

    private void pushFilterIntoScan(
            RelOptRuleCall call,
            Calc calc,
            FlinkLogicalTableSourceScan scan,
            TableSourceTable tableSourceTable) {

        RexProgram originProgram = calc.getProgram();

        RelBuilder relBuilder = call.builder();
        Tuple2<RexNode[], RexNode[]> extractedPredicates =
                FlinkRexUtil.extractPredicates(
                        originProgram.getInputRowType().getFieldNames().toArray(new String[0]),
                        originProgram.expandLocalRef(originProgram.getCondition()),
                        scan,
                        relBuilder.getRexBuilder());

        RexNode[] convertiblePredicates = extractedPredicates._1;
        RexNode[] unconvertedPredicates = extractedPredicates._2;
        if (convertiblePredicates.length == 0) {
            return;
        }

        FilterClassificationResult result =
                classifyAndPushFilters(convertiblePredicates, tableSourceTable, scan, relBuilder);
        if (result == null) {
            return;
        }

        List<RexNode> allRemainingRexNodes = new ArrayList<>(result.remainingPredicates);
        allRemainingRexNodes.addAll(Arrays.asList(unconvertedPredicates));

        FlinkLogicalTableSourceScan newScan =
                FlinkLogicalTableSourceScan.create(
                        scan.getCluster(), scan.getHints(), result.updatedTable);

        // build new calc program
        RexProgramBuilder programBuilder =
                RexProgramBuilder.forProgram(originProgram, call.builder().getRexBuilder(), true);
        programBuilder.clearCondition();

        if (!allRemainingRexNodes.isEmpty()) {
            RexNode remainingCondition = relBuilder.and(allRemainingRexNodes);
            RexNode simplifiedRemainingCondition =
                    FlinkRexUtil.simplify(
                            relBuilder.getRexBuilder(),
                            remainingCondition,
                            calc.getCluster().getPlanner().getExecutor());
            programBuilder.addCondition(simplifiedRemainingCondition);
        }

        RexProgram program = programBuilder.getProgram();
        if (program.isTrivial()) {
            call.transformTo(newScan);
        } else {
            FlinkLogicalCalc newCalc = FlinkLogicalCalc.create(newScan, program);
            call.transformTo(newCalc);
        }
    }
}
