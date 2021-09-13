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
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.expressions.converter.CallExpressionConvertRule;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlTrimFunction;

import java.util.List;

import static org.apache.flink.table.planner.expressions.converter.ExpressionConverter.extractValue;

/** Conversion for {@link BuiltInFunctionDefinitions#TRIM}. */
class TrimConverter extends CustomizedConverter {
    @Override
    public RexNode convert(CallExpression call, CallExpressionConvertRule.ConvertContext context) {
        checkArgumentNumber(call, 4);
        List<Expression> children = call.getChildren();
        ValueLiteralExpression removeLeadingExpr = (ValueLiteralExpression) children.get(0);
        Boolean removeLeading = extractValue(removeLeadingExpr, Boolean.class);
        ValueLiteralExpression removeTrailingExpr = (ValueLiteralExpression) children.get(1);
        Boolean removeTrailing = extractValue(removeTrailingExpr, Boolean.class);
        RexNode trimString = context.toRexNode(children.get(2));
        RexNode str = context.toRexNode(children.get(3));
        Enum trimMode;
        if (removeLeading && removeTrailing) {
            trimMode = SqlTrimFunction.Flag.BOTH;
        } else if (removeLeading) {
            trimMode = SqlTrimFunction.Flag.LEADING;
        } else if (removeTrailing) {
            trimMode = SqlTrimFunction.Flag.TRAILING;
        } else {
            throw new IllegalArgumentException("Unsupported trim mode.");
        }
        return context.getRelBuilder()
                .call(
                        FlinkSqlOperatorTable.TRIM,
                        context.getRelBuilder().getRexBuilder().makeFlag(trimMode),
                        trimString,
                        str);
    }
}
