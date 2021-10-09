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
import org.apache.flink.table.api.JsonType;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.expressions.converter.CallExpressionConvertRule;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;

/** Conversion for {@link BuiltInFunctionDefinitions#IS_JSON}. */
@Internal
class IsJsonConverter extends CustomizedConverter {

    @Override
    public RexNode convert(CallExpression call, CallExpressionConvertRule.ConvertContext context) {
        checkArgumentNumber(call, 1, 2);

        final JsonType type;
        if (call.getChildren().size() >= 2) {
            type = getJsonTypeArgument(call);
        } else {
            type = JsonType.VALUE;
        }

        return context.getRelBuilder()
                .call(getOperator(type), context.toRexNode(call.getChildren().get(0)));
    }

    private JsonType getJsonTypeArgument(CallExpression call) {
        return ((ValueLiteralExpression) call.getChildren().get(1))
                .getValueAs(JsonType.class)
                .orElseThrow(
                        () ->
                                new TableException(
                                        String.format(
                                                "Expected argument of type '%s'.",
                                                JsonType.class.getSimpleName())));
    }

    private SqlOperator getOperator(JsonType type) {
        switch (type) {
            case VALUE:
                return FlinkSqlOperatorTable.IS_JSON_VALUE;
            case SCALAR:
                return FlinkSqlOperatorTable.IS_JSON_SCALAR;
            case ARRAY:
                return FlinkSqlOperatorTable.IS_JSON_ARRAY;
            case OBJECT:
                return FlinkSqlOperatorTable.IS_JSON_OBJECT;
            default:
                throw new TableException(String.format("Unknown JSON type '%s'.", type));
        }
    }
}
