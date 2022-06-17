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

package org.apache.flink.table.planner.functions.aggfunctions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.not;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.nullOf;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.or;

/** last_value function. */
public class LastValueAggFunctionNew extends DeclarativeAggregateFunction {

    private final DataType argDataType;
    private final UnresolvedReferenceExpression lastValue = unresolvedRef("lastValue");
    private final UnresolvedReferenceExpression valueSet = unresolvedRef("valueSet");

    public LastValueAggFunctionNew(DataType argDataType) {
        this.argDataType = argDataType;
    }

    @Override
    public int operandCount() {
        return 1;
    }

    @Override
    public UnresolvedReferenceExpression[] aggBufferAttributes() {
        return new UnresolvedReferenceExpression[] {lastValue, valueSet};
    }

    @Override
    public DataType[] getAggBufferTypes() {
        return new DataType[] {argDataType, DataTypes.BOOLEAN().notNull()};
    }

    @Override
    public DataType getResultType() {
        return argDataType;
    }

    @Override
    public Expression[] initialValuesExpressions() {
        return new Expression[] {nullOf(argDataType), literal(false)};
    }

    @Override
    public Expression[] accumulateExpressions() {
        return new Expression[] {
            ifThenElse(isNull(operand(0)), lastValue, operand(0)),
            or(valueSet, not(isNull(operand(0))))
        };
    }

    @Override
    public Expression[] retractExpressions() {
        throw new TableException("This function does not support retraction.");
    }

    @Override
    public Expression[] mergeExpressions() {
        return new Expression[] {
            ifThenElse(mergeOperand(valueSet), mergeOperand(lastValue), lastValue),
            or(mergeOperand(valueSet), valueSet)
        };
    }

    @Override
    public Expression getValueExpression() {
        return lastValue;
    }
}
