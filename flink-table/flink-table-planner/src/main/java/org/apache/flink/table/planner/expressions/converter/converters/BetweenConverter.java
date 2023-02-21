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

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.expressions.converter.CallExpressionConvertRule;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.calcite.rex.RexNode;

import java.util.List;

import static org.apache.flink.table.planner.expressions.converter.ExpressionConverter.toRexNodes;

/** Conversion for {@link BuiltInFunctionDefinitions#BETWEEN}. */
@Internal
class BetweenConverter extends CustomizedConverter {
    @Override
    public RexNode convert(CallExpression call, CallExpressionConvertRule.ConvertContext context) {
        checkArgumentNumber(call, 3);
        List<RexNode> childrenRexNode = toRexNodes(context, call.getChildren());
        RexNode expr = childrenRexNode.get(0);
        RexNode lowerBound = childrenRexNode.get(1);
        RexNode upperBound = childrenRexNode.get(2);
        return context.getRelBuilder()
                .and(
                        context.getRelBuilder()
                                .call(
                                        FlinkSqlOperatorTable.GREATER_THAN_OR_EQUAL,
                                        expr,
                                        lowerBound),
                        context.getRelBuilder()
                                .call(FlinkSqlOperatorTable.LESS_THAN_OR_EQUAL, expr, upperBound));
    }
}
