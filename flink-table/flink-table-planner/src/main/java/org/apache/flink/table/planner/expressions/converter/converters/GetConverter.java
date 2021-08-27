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
import org.apache.flink.table.expressions.ExpressionUtils;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.expressions.converter.CallExpressionConvertRule;

import org.apache.calcite.rex.RexNode;

import java.util.Optional;

import static org.apache.flink.table.planner.expressions.converter.ExpressionConverter.extractValue;

/** Conversion for {@link BuiltInFunctionDefinitions#GET}. */
class GetConverter extends CustomizedConverter {
    @Override
    public RexNode convert(CallExpression call, CallExpressionConvertRule.ConvertContext context) {
        checkArgumentNumber(call, 2);
        RexNode child = context.toRexNode(call.getChildren().get(0));
        ValueLiteralExpression keyLiteral = (ValueLiteralExpression) call.getChildren().get(1);
        Optional<Integer> indexOptional =
                ExpressionUtils.extractValue(keyLiteral, String.class)
                        .map(child.getType().getFieldNames()::indexOf);
        int index = indexOptional.orElseGet(() -> extractValue(keyLiteral, Integer.class));
        return context.getRelBuilder().getRexBuilder().makeFieldAccess(child, index);
    }
}
