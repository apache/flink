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
import org.apache.flink.table.expressions.TypeLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.planner.expressions.converter.CallExpressionConvertRule;

import org.apache.calcite.rex.RexNode;

/** Conversion for {@link BuiltInFunctionDefinitions#REINTERPRET_CAST}. */
class ReinterpretCastConverter extends CustomizedConverter {
    @Override
    public RexNode convert(CallExpression call, CallExpressionConvertRule.ConvertContext context) {
        checkArgumentNumber(call, 3);
        RexNode child = context.toRexNode(call.getChildren().get(0));
        TypeLiteralExpression type = (TypeLiteralExpression) call.getChildren().get(1);
        RexNode checkOverflow = context.toRexNode(call.getChildren().get(2));
        return context.getRelBuilder()
                .getRexBuilder()
                .makeReinterpretCast(
                        context.getTypeFactory()
                                .createFieldTypeFromLogicalType(
                                        type.getOutputDataType()
                                                .getLogicalType()
                                                .copy(child.getType().isNullable())),
                        child,
                        checkOverflow);
    }
}
