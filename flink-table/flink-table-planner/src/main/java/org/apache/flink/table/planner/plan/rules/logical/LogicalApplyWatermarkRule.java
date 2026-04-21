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

import org.apache.flink.table.planner.plan.nodes.calcite.WatermarkAssigner;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

/**
 * Planner rule that converts a {@link LogicalTableFunctionScan} with APPLY_WATERMARK function to a
 * {@link WatermarkAssigner}.
 *
 * <p>This rule recognizes calls to APPLY_WATERMARK(table, DESCRIPTOR(column), watermark_expr) and
 * transforms them into proper watermark assignment nodes in the relational plan.
 */
public class LogicalApplyWatermarkRule extends RelOptRule {

    public static final LogicalApplyWatermarkRule INSTANCE = new LogicalApplyWatermarkRule();

    private LogicalApplyWatermarkRule() {
        super(operand(LogicalTableFunctionScan.class, any()), "LogicalApplyWatermarkRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalTableFunctionScan scan = call.rel(0);
        RexNode rexCall = scan.getCall();

        if (!(rexCall instanceof RexCall)) {
            return false;
        }

        RexCall call0 = (RexCall) rexCall;
        String functionName = call0.getOperator().getName();

        return functionName.equalsIgnoreCase("APPLY_WATERMARK");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalTableFunctionScan scan = call.rel(0);
        RexCall applyWatermarkCall = (RexCall) scan.getCall();

        // Extract operands from APPLY_WATERMARK(table, DESCRIPTOR(column), watermark_expr)
        if (applyWatermarkCall.getOperands().size() != 3) {
            return;
        }

        // Operand 0: table expression (already converted to RelNode via scan.getInputs())
        // Operand 1: DESCRIPTOR(column_name)
        // Operand 2: watermark expression

        RelNode inputRel = scan.getInputs().get(0);

        // Extract column index from DESCRIPTOR
        RexNode descriptorArg = applyWatermarkCall.getOperands().get(1);
        int rowtimeFieldIndex = extractColumnIndex(inputRel, descriptorArg);

        if (rowtimeFieldIndex < 0) {
            return; // Column not found
        }

        // Extract watermark expression
        RexNode watermarkExpr = applyWatermarkCall.getOperands().get(2);

        // Create WatermarkAssigner node
        WatermarkAssigner watermarkAssigner =
                new WatermarkAssigner(
                        scan.getCluster(),
                        scan.getTraitSet(),
                        inputRel,
                        scan.getHints(),
                        rowtimeFieldIndex,
                        watermarkExpr) {
                    @Override
                    public RelNode copy(
                            org.apache.calcite.plan.RelTraitSet traitSet,
                            RelNode input,
                            java.util.List<org.apache.calcite.rel.hint.RelHint> hints,
                            int rowtime,
                            RexNode watermark) {
                        return new WatermarkAssigner(
                                getCluster(), traitSet, input, hints, rowtime, watermark) {
                            @Override
                            public RelNode copy(
                                    org.apache.calcite.plan.RelTraitSet traitSet,
                                    RelNode input,
                                    java.util.List<org.apache.calcite.rel.hint.RelHint> hints,
                                    int rowtime,
                                    RexNode watermark) {
                                return this;
                            }

                            @Override
                            public RelNode withHints(
                                    java.util.List<org.apache.calcite.rel.hint.RelHint> hintList) {
                                return this;
                            }
                        };
                    }

                    @Override
                    public RelNode withHints(
                            java.util.List<org.apache.calcite.rel.hint.RelHint> hintList) {
                        return this;
                    }
                };

        call.transformTo(watermarkAssigner);
    }

    private int extractColumnIndex(RelNode inputRel, RexNode descriptorArg) {
        // DESCRIPTOR(column_name) is represented as a RexCall
        if (!(descriptorArg instanceof RexCall)) {
            return -1;
        }

        RexCall descriptorCall = (RexCall) descriptorArg;
        if (descriptorCall.getOperands().isEmpty()) {
            return -1;
        }

        // The column name is the first operand of DESCRIPTOR
        RexNode columnRef = descriptorCall.getOperands().get(0);

        // Extract column name
        String columnName;
        if (columnRef instanceof org.apache.calcite.rex.RexInputRef) {
            org.apache.calcite.rex.RexInputRef inputRef =
                    (org.apache.calcite.rex.RexInputRef) columnRef;
            return inputRef.getIndex();
        } else if (columnRef instanceof org.apache.calcite.rex.RexFieldAccess) {
            org.apache.calcite.rex.RexFieldAccess fieldAccess =
                    (org.apache.calcite.rex.RexFieldAccess) columnRef;
            return fieldAccess.getField().getIndex();
        } else {
            // Try to extract column name from literal or identifier
            // This is a simplified approach
            return 0; // Default to first column if unable to determine
        }
    }
}
