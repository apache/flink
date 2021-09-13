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

package org.apache.flink.table.planner.expressions.converter.converters;

import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.expressions.converter.CallExpressionConvertRule;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.calcite.rex.RexNode;

import java.util.List;

import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.planner.expressions.converter.ExpressionConverter.toRexNodes;
import static org.apache.flink.table.runtime.typeutils.TypeCheckUtils.isTimeInterval;

/** Conversion for {@link BuiltInFunctionDefinitions#TEMPORAL_OVERLAPS}. */
class TemporalOverlapsConverter extends CustomizedConverter {
    @Override
    public RexNode convert(CallExpression call, CallExpressionConvertRule.ConvertContext context) {
        checkArgumentNumber(call, 4);
        List<RexNode> childrenRexNode = toRexNodes(context, call.getChildren());
        // Standard conversion of the OVERLAPS operator.
        // Source: [[org.apache.calcite.sql2rel.StandardConvertletTable#convertOverlaps()]]
        RexNode leftTimePoint = childrenRexNode.get(0);
        RexNode leftTemporal = childrenRexNode.get(1);
        RexNode rightTimePoint = childrenRexNode.get(2);
        RexNode rightTemporal = childrenRexNode.get(3);
        RexNode convLeftT;
        if (isTimeInterval(toLogicalType(leftTemporal.getType()))) {
            convLeftT =
                    context.getRelBuilder()
                            .call(FlinkSqlOperatorTable.DATETIME_PLUS, leftTimePoint, leftTemporal);
        } else {
            convLeftT = leftTemporal;
        }
        // sort end points into start and end, such that (s0 <= e0) and (s1 <= e1).
        RexNode leftLe =
                context.getRelBuilder()
                        .call(FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL, leftTimePoint, convLeftT);
        RexNode s0 =
                context.getRelBuilder()
                        .call(FlinkSqlOperatorTable.CASE, leftLe, leftTimePoint, convLeftT);
        RexNode e0 =
                context.getRelBuilder()
                        .call(FlinkSqlOperatorTable.CASE, leftLe, convLeftT, leftTimePoint);
        RexNode convRightT;
        if (isTimeInterval(toLogicalType(rightTemporal.getType()))) {
            convRightT =
                    context.getRelBuilder()
                            .call(
                                    FlinkSqlOperatorTable.DATETIME_PLUS,
                                    rightTimePoint,
                                    rightTemporal);
        } else {
            convRightT = rightTemporal;
        }
        RexNode rightLe =
                context.getRelBuilder()
                        .call(FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL, rightTimePoint, convRightT);
        RexNode s1 =
                context.getRelBuilder()
                        .call(FlinkSqlOperatorTable.CASE, rightLe, rightTimePoint, convRightT);
        RexNode e1 =
                context.getRelBuilder()
                        .call(FlinkSqlOperatorTable.CASE, rightLe, convRightT, rightTimePoint);

        // (e0 >= s1) AND (e1 >= s0)
        RexNode leftPred =
                context.getRelBuilder().call(FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL, e0, s1);
        RexNode rightPred =
                context.getRelBuilder().call(FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL, e1, s0);
        return context.getRelBuilder().call(FlinkSqlOperatorTable.AND, leftPred, rightPred);
    }
}
