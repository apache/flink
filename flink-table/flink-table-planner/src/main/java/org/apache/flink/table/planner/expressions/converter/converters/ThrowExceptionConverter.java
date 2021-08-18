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
import org.apache.flink.table.planner.expressions.converter.CallExpressionConvertRule;
import org.apache.flink.table.planner.functions.InternalFunctionDefinitions;
import org.apache.flink.table.planner.functions.sql.SqlThrowExceptionFunction;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.rex.RexNode;

import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType;

/** Conversion for {@link InternalFunctionDefinitions#THROW_EXCEPTION}. */
class ThrowExceptionConverter extends CustomizedConverter {
    @Override
    public RexNode convert(CallExpression call, CallExpressionConvertRule.ConvertContext context) {
        checkArgumentNumber(call, 2);
        DataType type = ((TypeLiteralExpression) call.getChildren().get(1)).getOutputDataType();
        SqlThrowExceptionFunction function =
                new SqlThrowExceptionFunction(
                        context.getTypeFactory()
                                .createFieldTypeFromLogicalType(fromDataTypeToLogicalType(type)));
        return context.getRelBuilder().call(function, context.toRexNode(call.getChildren().get(0)));
    }
}
