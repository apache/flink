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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.util.NlsString;

import java.util.List;

/**
 * Planner rule that converts a {@link LogicalTableFunctionScan} produced by the
 * {@code APPLY_WATERMARK} built-in table function into a concrete
 * {@link LogicalWatermarkAssigner}.
 *
 * <p>The rule expects the function call to have the structure:
 *
 * <pre>{@code APPLY_WATERMARK(<table>, DESCRIPTOR(<column>), <watermark_expression>)}</pre>
 *
 * <p>Resolution rules:
 *
 * <ul>
 *   <li>The first operand is the input table, propagated as the {@link RelNode} input of the
 *       resulting {@link LogicalWatermarkAssigner} via {@code scan.getInputs()}.
 *   <li>The descriptor column is resolved against the input row type using a case-insensitive name
 *       matcher; failing to resolve raises a {@link ValidationException} (instead of silently
 *       picking field {@code 0}).
 *   <li>The watermark expression is forwarded as-is; constant folding is performed downstream by
 *       {@link org.apache.flink.table.planner.plan.nodes.calcite.WatermarkUtils#simplify}.
 * </ul>
 */
public class LogicalApplyWatermarkRule extends RelOptRule {

    public static final LogicalApplyWatermarkRule INSTANCE = new LogicalApplyWatermarkRule();

    private static final String FUNCTION_NAME = "APPLY_WATERMARK";

    private LogicalApplyWatermarkRule() {
        super(operand(LogicalTableFunctionScan.class, any()), "LogicalApplyWatermarkRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final LogicalTableFunctionScan scan = call.rel(0);
        final RexNode rexCall = scan.getCall();
        if (!(rexCall instanceof RexCall)) {
            return false;
        }
        return FUNCTION_NAME.equalsIgnoreCase(((RexCall) rexCall).getOperator().getName());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalTableFunctionScan scan = call.rel(0);
        final RexCall applyWatermark = (RexCall) scan.getCall();
        final List<RexNode> operands = applyWatermark.getOperands();
        if (operands.size() != 3) {
            // Defensive: SqlApplyWatermarkFunction.OperandMetadataImpl already enforces this,
            // but the rule must remain robust against future signature changes.
            throw new ValidationException(
                    "APPLY_WATERMARK expects exactly 3 arguments, but got " + operands.size());
        }

        if (scan.getInputs().isEmpty()) {
            throw new ValidationException(
                    "APPLY_WATERMARK first argument must be a TABLE expression.");
        }
        final RelNode input = scan.getInputs().get(0);

        final int rowtimeFieldIndex = resolveDescriptorColumnIndex(input, operands.get(1));
        final RexNode watermarkExpr = operands.get(2);

        final LogicalWatermarkAssigner assigner =
                LogicalWatermarkAssigner.create(
                        scan.getCluster(),
                        input,
                        scan.getHints(),
                        rowtimeFieldIndex,
                        watermarkExpr);

        call.transformTo(assigner);
    }

    /**
     * Resolves the column referenced by the {@code DESCRIPTOR(...)} operand of APPLY_WATERMARK to
     * an index into the input row type. We tolerate three runtime representations because the
     * Calcite SQL-to-Rel converter may yield any of them depending on validator state:
     *
     * <ul>
     *   <li>{@link RexInputRef}: already resolved against the input scope.
     *   <li>{@link RexFieldAccess}: a struct field reference with a known {@code field.index}.
     *   <li>{@link RexLiteral} of CHAR/VARCHAR: the column name as a literal (the form produced
     *       when the descriptor is captured as a SqlIdentifier wrapped in a literal).
     * </ul>
     *
     * <p>Any other shape is treated as a validation error rather than silently defaulting to
     * column {@code 0}, which previously could cause the wrong field to be marked as rowtime.
     */
    private static int resolveDescriptorColumnIndex(RelNode input, RexNode descriptorOperand) {
        if (!(descriptorOperand instanceof RexCall)) {
            throw new ValidationException(
                    "APPLY_WATERMARK second argument must be DESCRIPTOR(column).");
        }
        final RexCall descriptor = (RexCall) descriptorOperand;
        if (descriptor.getOperands().isEmpty()) {
            throw new ValidationException(
                    "APPLY_WATERMARK DESCRIPTOR must specify a column name.");
        }
        final RexNode columnRef = descriptor.getOperands().get(0);

        if (columnRef instanceof RexInputRef) {
            return ((RexInputRef) columnRef).getIndex();
        }
        if (columnRef instanceof RexFieldAccess) {
            return ((RexFieldAccess) columnRef).getField().getIndex();
        }
        if (columnRef instanceof RexLiteral) {
            final RexLiteral literal = (RexLiteral) columnRef;
            final Object value = literal.getValue2();
            final String columnName;
            if (value instanceof NlsString) {
                columnName = ((NlsString) value).getValue();
            } else if (value instanceof String) {
                columnName = (String) value;
            } else {
                columnName = null;
            }
            if (columnName != null) {
                // Case-insensitive lookup against the input row type.
                final List<String> fieldNames = input.getRowType().getFieldNames();
                for (int i = 0; i < fieldNames.size(); i++) {
                    if (fieldNames.get(i).equalsIgnoreCase(columnName)) {
                        return i;
                    }
                }
                throw new ValidationException(
                        String.format(
                                "APPLY_WATERMARK DESCRIPTOR column '%s' is not present in the "
                                        + "input. Available columns: %s.",
                                columnName, fieldNames));
            }
        }

        throw new ValidationException(
                "APPLY_WATERMARK DESCRIPTOR could not be resolved to an input column. "
                        + "Got rex node: "
                        + columnRef);
    }
}
