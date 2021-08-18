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
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.TableReferenceExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.planner.calcite.FlinkRelBuilder;
import org.apache.flink.table.planner.expressions.converter.CallExpressionConvertRule;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;

import java.util.List;

import static org.apache.flink.table.planner.expressions.converter.ExpressionConverter.toRexNodes;

/** Conversion for {@link BuiltInFunctionDefinitions#IN}. */
class InConverter extends CustomizedConverter {
    @Override
    public RexNode convert(CallExpression call, CallExpressionConvertRule.ConvertContext context) {
        checkArgument(call, call.getChildren().size() > 1);
        Expression headExpr = call.getChildren().get(1);
        if (headExpr instanceof TableReferenceExpression) {
            QueryOperation tableOperation =
                    ((TableReferenceExpression) headExpr).getQueryOperation();
            RexNode child = context.toRexNode(call.getChildren().get(0));
            return RexSubQuery.in(
                    ((FlinkRelBuilder) context.getRelBuilder())
                            .queryOperation(tableOperation)
                            .build(),
                    ImmutableList.of(child));
        } else {
            List<RexNode> child = toRexNodes(context, call.getChildren());
            return context.getRelBuilder()
                    .getRexBuilder()
                    .makeIn(child.get(0), child.subList(1, child.size()));
        }
    }
}
