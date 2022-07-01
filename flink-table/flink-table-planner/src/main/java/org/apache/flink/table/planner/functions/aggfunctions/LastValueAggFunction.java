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

/** Built-in LAST_VALUE aggregate function. */
public class LastValueAggFunction extends DeclarativeAggregateFunction {

    private final DataType[] argDataTypes;
    private final UnresolvedReferenceExpression lastValue = unresolvedRef("lastValue");
    private final UnresolvedReferenceExpression valueSet = unresolvedRef("valueSet");
    private final boolean hasIgnoreNullArg;
    private final boolean ignoreNullByDefault;

    public LastValueAggFunction(DataType[] argDataTypes, boolean nullTreatmentByDefault) {
        this.argDataTypes = argDataTypes;
        this.hasIgnoreNullArg = argDataTypes.length == 2;
        this.ignoreNullByDefault = !nullTreatmentByDefault;
    }

    @Override
    public int operandCount() {
        return argDataTypes.length;
    }

    @Override
    public UnresolvedReferenceExpression[] aggBufferAttributes() {
        return new UnresolvedReferenceExpression[] {lastValue, valueSet};
    }

    @Override
    public DataType[] getAggBufferTypes() {
        return new DataType[] {argDataTypes[0], DataTypes.BOOLEAN().notNull()};
    }

    @Override
    public DataType getResultType() {
        return argDataTypes[0];
    }

    @Override
    public Expression[] initialValuesExpressions() {
        return new Expression[] {nullOf(argDataTypes[0]), literal(false)};
    }

    @Override
    public Expression[] accumulateExpressions() {
        // pseudo code:
        // if (hasIgnoreNullArg) {
        //   if (operand(1)) {
        //     // ignore null
        //     lastValue = operand(0) == null ? lastValue : operand(0)
        //     valueSet = valueSet || operand(0) != null
        //   } else {
        //     // not ignore null
        //     lastValue = operand(0)
        //     valueSet = true
        //   }
        // } else {
        //   // otherwise, if ignore null by default
        //   if (nullTreatmentByDefault) {
        //     lastValue = operand(0) == null ? lastValue : operand(0)
        //     valueSet = valueSet || operand(0) != null
        //   } else {
        //     // if not ignore null by default
        //     lastValue = operand(0)
        //     valueSet = true
        //   }
        // }
        if (hasIgnoreNullArg) {
            return new Expression[] {
                /* lastValue = */ ifThenElse(
                        operand(1),
                        ifThenElse(isNull(operand(0)), lastValue, operand(0)),
                        operand(0)),
                /* valueSet = */ ifThenElse(
                        operand(1), or(valueSet, not(isNull(operand(0)))), literal(true))
            };
        } else {
            // if ignore null by default
            if (ignoreNullByDefault) {
                return new Expression[] {
                    /* lastValue = */ ifThenElse(isNull(operand(0)), lastValue, operand(0)),
                    /* valueSet = */ or(valueSet, not(isNull(operand(0))))
                };
            } else {
                // otherwise, not ignore null by default
                return new Expression[] {
                    /* lastValue = */ operand(0), /* valueSet = */ literal(true)
                };
            }
        }
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

    @Override
    public boolean isDeterministic() {
        return false;
    }
}
