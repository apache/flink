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

import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.expressions.converter.CallExpressionConvertRule;
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlJsonEmptyOrError;
import org.apache.calcite.sql.SqlJsonValueEmptyOrErrorBehavior;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import static org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTypeFactory;

/** Conversion for {@link BuiltInFunctionDefinitions#JSON_VALUE}. */
class JsonValueConverter extends CustomizedConverter {

    @Override
    public RexNode convert(CallExpression call, CallExpressionConvertRule.ConvertContext context) {
        checkArgumentNumber(call, 7);

        final List<RexNode> operands = new LinkedList<>();
        operands.add(context.toRexNode(call.getChildren().get(0)));
        operands.add(context.toRexNode(call.getChildren().get(1)));

        operands.addAll(getBehaviorOperands(call, context, SqlJsonEmptyOrError.EMPTY));
        operands.addAll(getBehaviorOperands(call, context, SqlJsonEmptyOrError.ERROR));

        final FlinkTypeFactory typeFactory = unwrapTypeFactory(context.getRelBuilder());
        final RelDataType returnRelType =
                typeFactory.createFieldTypeFromLogicalType(
                        call.getOutputDataType().getLogicalType());

        return context.getRelBuilder()
                .getRexBuilder()
                .makeCall(returnRelType, FlinkSqlOperatorTable.JSON_VALUE, operands);
    }

    private List<RexNode> getBehaviorOperands(
            CallExpression call,
            CallExpressionConvertRule.ConvertContext context,
            SqlJsonEmptyOrError mode) {
        final int idx = getArgumentIndexForBehavior(mode);
        final SqlJsonValueEmptyOrErrorBehavior behavior = getBehavior(call, idx);
        final Expression defaultExpression = call.getChildren().get(idx + 1);

        final List<RexNode> operands = new ArrayList<>();
        operands.add(context.getRelBuilder().getRexBuilder().makeFlag(behavior));
        if (behavior == SqlJsonValueEmptyOrErrorBehavior.DEFAULT) {
            operands.add(context.toRexNode(defaultExpression));
        }

        operands.add(context.getRelBuilder().getRexBuilder().makeFlag(mode));
        return operands;
    }

    private SqlJsonValueEmptyOrErrorBehavior getBehavior(CallExpression call, int idx) {
        return ((ValueLiteralExpression) call.getChildren().get(idx))
                .getValueAs(JsonValueOnEmptyOrError.class)
                .map(JsonValueOnEmptyOrError::name)
                .map(SqlJsonValueEmptyOrErrorBehavior::valueOf)
                .orElseThrow(
                        () ->
                                new TableException(
                                        String.format(
                                                "Did not find a '%s' at position %d. This is a bug. Please consider filing an issue.",
                                                JsonValueOnEmptyOrError.class.getSimpleName(),
                                                idx)));
    }

    private int getArgumentIndexForBehavior(SqlJsonEmptyOrError mode) {
        switch (mode) {
            case EMPTY:
                return 3;
            case ERROR:
                return 5;
            default:
                throw new TableException(
                        String.format(
                                "Unexpected behavior mode '%s'. This is a bug. Please consider filing an issue.",
                                mode));
        }
    }
}
