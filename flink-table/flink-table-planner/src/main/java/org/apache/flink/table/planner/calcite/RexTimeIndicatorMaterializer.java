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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.TimestampType;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.isProctimeIndicatorType;
import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.isRowtimeIndicatorType;
import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.isTimeIndicatorType;
import static org.apache.flink.table.planner.plan.utils.MatchUtil.isFinalOnMatchTimeIndicator;

/**
 * A {@link RexShuttle} that rewrites {@link RexInputRef}s and {@link RexCall}s whose declared type
 * carries a {@link TimeIndicatorRelDataType} marker so that they match a (possibly demoted) input
 * row type, materializing time-indicator values to plain timestamps when required.
 *
 * <p>This is shared between {@link RelTimeIndicatorConverter} (where it is used to materialize time
 * indicators consumed by calculations) and downstream rules that drop a node which previously
 * stamped a column as a time indicator (e.g. removing a redundant {@code WatermarkAssigner}).
 */
@Internal
public class RexTimeIndicatorMaterializer extends RexShuttle {

    private final RexBuilder rexBuilder;
    private final List<RelDataType> inputFieldTypes;

    public RexTimeIndicatorMaterializer(RexBuilder rexBuilder, RelNode input) {
        this(rexBuilder, RelOptUtil.getFieldTypeList(input.getRowType()));
    }

    public RexTimeIndicatorMaterializer(RexBuilder rexBuilder, List<RelDataType> inputFieldTypes) {
        this.rexBuilder = rexBuilder;
        this.inputFieldTypes = inputFieldTypes;
    }

    @Override
    public RexNode visitCall(RexCall call) {
        RexCall updatedCall = (RexCall) super.visitCall(call);

        // materialize operands with time indicators
        List<RexNode> materializedOperands;
        SqlOperator updatedCallOp = updatedCall.getOperator();
        if (updatedCallOp == FlinkSqlOperatorTable.SESSION_OLD
                || updatedCallOp == FlinkSqlOperatorTable.HOP_OLD
                || updatedCallOp == FlinkSqlOperatorTable.TUMBLE_OLD) {
            // skip materialization for special operators
            materializedOperands = updatedCall.getOperands();
        } else {
            materializedOperands =
                    updatedCall.getOperands().stream()
                            .map(this::materializeTimeIndicators)
                            .collect(Collectors.toList());
        }

        // All calls in MEASURES and DEFINE are wrapped with FINAL/RUNNING, therefore
        // we should treat FINAL(MATCH_ROWTIME) and FINAL(MATCH_PROCTIME) as a time attribute
        // extraction
        if (isFinalOnMatchTimeIndicator(call)) {
            return updatedCall;
        } else if (isTimeIndicatorType(updatedCall.getType())) {
            // do not modify window time attributes and some special operators
            if (updatedCallOp == FlinkSqlOperatorTable.TUMBLE_ROWTIME
                    || updatedCallOp == FlinkSqlOperatorTable.TUMBLE_PROCTIME
                    || updatedCallOp == FlinkSqlOperatorTable.HOP_ROWTIME
                    || updatedCallOp == FlinkSqlOperatorTable.HOP_PROCTIME
                    || updatedCallOp == FlinkSqlOperatorTable.SESSION_ROWTIME
                    || updatedCallOp == FlinkSqlOperatorTable.SESSION_PROCTIME
                    || updatedCallOp == FlinkSqlOperatorTable.MATCH_ROWTIME
                    || updatedCallOp == FlinkSqlOperatorTable.MATCH_PROCTIME
                    || updatedCallOp == FlinkSqlOperatorTable.PROCTIME
                    || updatedCallOp == SqlStdOperatorTable.AS
                    || updatedCallOp == SqlStdOperatorTable.CAST
                    || updatedCallOp == FlinkSqlOperatorTable.REINTERPRET) {
                return updatedCall;
            } else {
                // materialize function's result and operands
                return updatedCall.clone(
                        timestamp(
                                updatedCall.getType().isNullable(),
                                isTimestampLtzType(updatedCall.getType())),
                        materializedOperands);
            }
        } else {
            // materialize function's operands only
            return updatedCall.clone(updatedCall.getType(), materializedOperands);
        }
    }

    @Override
    public RexNode visitInputRef(RexInputRef inputRef) {
        RelDataType oldType = inputRef.getType();
        if (isTimeIndicatorType(oldType)) {
            RelDataType resolvedRefType = inputFieldTypes.get(inputRef.getIndex());
            if (!isTimeIndicatorType(resolvedRefType)) {
                // input has been materialized
                return new RexInputRef(inputRef.getIndex(), resolvedRefType);
            }
        }
        return super.visitInputRef(inputRef);
    }

    @Override
    public RexNode visitPatternFieldRef(RexPatternFieldRef fieldRef) {
        RelDataType oldType = fieldRef.getType();
        if (isTimeIndicatorType(oldType)) {
            RelDataType resolvedRefType = inputFieldTypes.get(fieldRef.getIndex());
            if (!isTimeIndicatorType(resolvedRefType)) {
                // input has been materialized
                return new RexPatternFieldRef(
                        fieldRef.getAlpha(), fieldRef.getIndex(), resolvedRefType);
            }
        }
        return super.visitPatternFieldRef(fieldRef);
    }

    /**
     * Wraps an expression with the appropriate cast/proctime call to materialize a time-indicator
     * value into a plain timestamp value.
     */
    private RexNode materializeTimeIndicators(RexNode expr) {
        if (isRowtimeIndicatorType(expr.getType())) {
            // cast rowTime indicator to regular timestamp
            return rexBuilder.makeAbstractCast(
                    timestamp(expr.getType().isNullable(), isTimestampLtzType(expr.getType())),
                    expr,
                    false);
        } else if (isProctimeIndicatorType(expr.getType())) {
            // generate procTime access
            return rexBuilder.makeCall(FlinkSqlOperatorTable.PROCTIME_MATERIALIZE, expr);
        } else {
            return expr;
        }
    }

    private RelDataType timestamp(boolean isNullable, boolean isTimestampLtzIndicator) {
        LogicalType logicalType;
        if (isTimestampLtzIndicator) {
            logicalType = new LocalZonedTimestampType(isNullable, 3);
        } else {
            logicalType = new TimestampType(isNullable, 3);
        }
        return ((FlinkTypeFactory) rexBuilder.getTypeFactory())
                .createFieldTypeFromLogicalType(logicalType);
    }

    private boolean isTimestampLtzType(RelDataType type) {
        return type.getSqlTypeName().equals(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }
}
