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

package org.apache.flink.table.functions.hive;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.greaterThan;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.nullOf;

/** built-in hive max aggregate function. */
public class HiveMaxAggFunction extends HiveDeclarativeAggregateFunction {

    private final UnresolvedReferenceExpression max = unresolvedRef("max");
    private DataType resultType;

    @Override
    public int operandCount() {
        return 1;
    }

    @Override
    public UnresolvedReferenceExpression[] aggBufferAttributes() {
        return new UnresolvedReferenceExpression[] {max};
    }

    @Override
    public DataType[] getAggBufferTypes() {
        return new DataType[] {getResultType()};
    }

    @Override
    public DataType getResultType() {
        return resultType;
    }

    @Override
    public Expression[] initialValuesExpressions() {
        return new Expression[] {/* max = */ nullOf(getResultType())};
    }

    @Override
    public Expression[] accumulateExpressions() {
        return new Expression[] {
            /* max = */ ifThenElse(
                    isNull(operand(0)),
                    max,
                    ifThenElse(
                            isNull(max),
                            operand(0),
                            ifThenElse(greaterThan(operand(0), max), operand(0), max)))
        };
    }

    @Override
    public Expression[] retractExpressions() {
        throw new TableException("Max aggregate function does not support retraction.");
    }

    @Override
    public Expression[] mergeExpressions() {
        return new Expression[] {
            /* max = */ ifThenElse(
                    isNull(mergeOperand(max)),
                    max,
                    ifThenElse(
                            isNull(max),
                            mergeOperand(max),
                            ifThenElse(
                                    greaterThan(mergeOperand(max), max), mergeOperand(max), max)))
        };
    }

    @Override
    public Expression getValueExpression() {
        return max;
    }

    @Override
    public void setArguments(CallContext callContext) {
        if (resultType == null) {
            checkArgumentNum(callContext.getArgumentDataTypes());
            // check argument type firstly
            checkMinMaxArgumentType(
                    callContext.getArgumentDataTypes().get(0).getLogicalType(), "max");
            resultType = callContext.getArgumentDataTypes().get(0);
        }
    }
}
