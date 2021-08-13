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

import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.apache.flink.table.planner.calcite.FlinkTypeFactory.toLogicalType;
import static org.apache.flink.table.planner.expressions.converter.ExpressionConverter.toRexNodes;
import static org.apache.flink.table.runtime.typeutils.TypeCheckUtils.isCharacterString;
import static org.apache.flink.table.runtime.typeutils.TypeCheckUtils.isTemporal;
import static org.apache.flink.table.runtime.typeutils.TypeCheckUtils.isTimeInterval;

/** Conversion for {@link BuiltInFunctionDefinitions#PLUS}. */
class PlusConverter extends CustomizedConverter {
    @Override
    public RexNode convert(CallExpression call, CallExpressionConvertRule.ConvertContext context) {
        checkArgumentNumber(call, 2);
        List<RexNode> childrenRexNode = toRexNodes(context, call.getChildren());
        if (isCharacterString(toLogicalType(childrenRexNode.get(0).getType()))) {
            return context.getRelBuilder()
                    .call(
                            FlinkSqlOperatorTable.CONCAT,
                            childrenRexNode.get(0),
                            context.getRelBuilder().cast(childrenRexNode.get(1), VARCHAR));
        } else if (isCharacterString(toLogicalType(childrenRexNode.get(1).getType()))) {
            return context.getRelBuilder()
                    .call(
                            FlinkSqlOperatorTable.CONCAT,
                            context.getRelBuilder().cast(childrenRexNode.get(0), VARCHAR),
                            childrenRexNode.get(1));
        } else if (isTimeInterval(toLogicalType(childrenRexNode.get(0).getType()))
                && childrenRexNode.get(0).getType() == childrenRexNode.get(1).getType()) {
            return context.getRelBuilder().call(FlinkSqlOperatorTable.PLUS, childrenRexNode);
        } else if (isTimeInterval(toLogicalType(childrenRexNode.get(0).getType()))
                && isTemporal(toLogicalType(childrenRexNode.get(1).getType()))) {
            // Calcite has a bug that can't apply INTERVAL + DATETIME (INTERVAL at left)
            // we manually switch them here
            return context.getRelBuilder()
                    .call(
                            FlinkSqlOperatorTable.DATETIME_PLUS,
                            childrenRexNode.get(1),
                            childrenRexNode.get(0));
        } else if (isTemporal(toLogicalType(childrenRexNode.get(0).getType()))
                && isTemporal(toLogicalType(childrenRexNode.get(1).getType()))) {
            return context.getRelBuilder()
                    .call(FlinkSqlOperatorTable.DATETIME_PLUS, childrenRexNode);
        } else {
            return context.getRelBuilder().call(FlinkSqlOperatorTable.PLUS, childrenRexNode);
        }
    }
}
