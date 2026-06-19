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
import org.apache.calcite.sql.SqlJsonConstructorNullClause;

import java.util.LinkedList;
import java.util.List;

/** Conversion for {@link BuiltInFunctionDefinitions#JSON_ARRAY}. */
@Internal
class JsonArrayConverter extends CustomizedConverter {
    @Override
    public RexNode convert(CallExpression call, CallExpressionConvertRule.ConvertContext context) {
        checkArgument(call, call.getChildren().size() >= 1);
        final List<RexNode> operands = new LinkedList<>();

        final SqlJsonConstructorNullClause onNull = JsonConverterUtil.getOnNullArgument(call, 0);
        operands.add(context.getRelBuilder().getRexBuilder().makeFlag(onNull));

        for (int i = 1; i < call.getChildren().size(); i++) {
            operands.add(context.toRexNode(call.getChildren().get(i)));
        }

        return context.getRelBuilder()
                .getRexBuilder()
                .makeCall(FlinkSqlOperatorTable.JSON_ARRAY, operands);
    }
}
