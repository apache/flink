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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalCalc;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalWatermarkAssigner;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Rule to push the {@link FlinkLogicalWatermarkAssigner} across the {@link FlinkLogicalCalc} to the
 * {@link FlinkLogicalTableSourceScan}. The rule will first look for the computed column in the
 * {@link FlinkLogicalCalc} and then translate the watermark expression and the computed column into
 * a {@link WatermarkStrategy}. With the new scan the rule will build a new {@link
 * FlinkLogicalCalc}.
 */
public class PushWatermarkIntoTableSourceScanAcrossCalcRule
        extends PushWatermarkIntoTableSourceScanRuleBase {
    public static final PushWatermarkIntoTableSourceScanAcrossCalcRule INSTANCE =
            new PushWatermarkIntoTableSourceScanAcrossCalcRule();

    public PushWatermarkIntoTableSourceScanAcrossCalcRule() {
        super(
                operand(
                        FlinkLogicalWatermarkAssigner.class,
                        operand(
                                FlinkLogicalCalc.class,
                                operand(FlinkLogicalTableSourceScan.class, none()))),
                "PushWatermarkIntoFlinkTableSourceScanAcrossCalcRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        FlinkLogicalTableSourceScan scan = call.rel(2);
        return supportsWatermarkPushDown(scan);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FlinkLogicalWatermarkAssigner watermarkAssigner = call.rel(0);
        FlinkLogicalCalc calc = call.rel(1);

        RexProgram originProgram = calc.getProgram();
        List<RexNode> projectList =
                originProgram.getProjectList().stream()
                        .map(originProgram::expandLocalRef)
                        .collect(Collectors.toList());

        // get watermark expression
        RexNode rowTimeColumn = projectList.get(watermarkAssigner.rowtimeFieldIndex());
        RexNode newWatermarkExpr =
                watermarkAssigner
                        .watermarkExpr()
                        .accept(
                                new RexShuttle() {
                                    @Override
                                    public RexNode visitInputRef(RexInputRef inputRef) {
                                        return projectList.get(inputRef.getIndex());
                                    }
                                });

        // push watermark assigner into the scan
        FlinkLogicalTableSourceScan newScan =
                getNewScan(
                        watermarkAssigner,
                        newWatermarkExpr,
                        call.rel(2),
                        ShortcutUtils.unwrapContext(calc).getTableConfig(),
                        false); // useWatermarkAssignerRowType

        FlinkTypeFactory typeFactory = ShortcutUtils.unwrapTypeFactory(calc);
        RexBuilder builder = call.builder().getRexBuilder();
        // cast timestamp/timestamp_ltz type to rowtime type.
        RexNode newRowTimeColumn =
                builder.makeReinterpretCast(
                        typeFactory.createRowtimeIndicatorType(
                                rowTimeColumn.getType().isNullable(),
                                rowTimeColumn.getType().getSqlTypeName()
                                        == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE),
                        rowTimeColumn,
                        null);

        // build new calc program
        RexProgramBuilder programBuilder = new RexProgramBuilder(newScan.getRowType(), builder);

        List<String> outputFieldNames = originProgram.getOutputRowType().getFieldNames();
        for (int i = 0; i < projectList.size(); i++) {
            if (i == watermarkAssigner.rowtimeFieldIndex()) {
                // replace the origin computed column to keep type consistent
                programBuilder.addProject(newRowTimeColumn, outputFieldNames.get(i));
            } else {
                programBuilder.addProject(projectList.get(i), outputFieldNames.get(i));
            }
        }
        if (originProgram.getCondition() != null) {
            programBuilder.addCondition(originProgram.expandLocalRef(originProgram.getCondition()));
        }

        FlinkLogicalCalc newCalc = FlinkLogicalCalc.create(newScan, programBuilder.getProgram());
        call.transformTo(newCalc);
    }
}
