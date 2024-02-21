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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.plus;

/** built-in hive count aggregate function. */
public class HiveCountAggFunction extends HiveDeclarativeAggregateFunction {

    private final UnresolvedReferenceExpression count = unresolvedRef("count");
    private Integer arguments;
    private boolean countLiteral;

    @Override
    public int operandCount() {
        return arguments;
    }

    @Override
    public UnresolvedReferenceExpression[] aggBufferAttributes() {
        return new UnresolvedReferenceExpression[] {count};
    }

    @Override
    public DataType[] getAggBufferTypes() {
        return new DataType[] {DataTypes.BIGINT()};
    }

    @Override
    public DataType getResultType() {
        return DataTypes.BIGINT();
    }

    @Override
    public Expression[] initialValuesExpressions() {
        return new Expression[] {/* count = */ literal(0L, getResultType().notNull())};
    }

    @Override
    public Expression[] accumulateExpressions() {
        // count(*) and count(literal) mean that count all elements
        if (arguments == 0 || countLiteral) {
            return new Expression[] {/* count = */ plus(count, literal(1L))};
        }

        // other case need to determine the value of the element
        List<Expression> operandExpressions = new ArrayList<>();
        for (int i = 0; i < arguments; i++) {
            operandExpressions.add(operand(i));
        }
        Expression operandExpression =
                operandExpressions.stream()
                        .map(ExpressionBuilder::isNull)
                        .reduce(ExpressionBuilder::or)
                        .get();
        return new Expression[] {
            /* count = */ ifThenElse(operandExpression, count, plus(count, literal(1L)))
        };
    }

    @Override
    public Expression[] retractExpressions() {
        throw new TableException("Count aggregate function does not support retraction.");
    }

    @Override
    public Expression[] mergeExpressions() {
        return new Expression[] {/* count = */ plus(count, mergeOperand(count))};
    }

    @Override
    public Expression getValueExpression() {
        return count;
    }

    @Override
    public void setArguments(CallContext callContext) {
        if (arguments == null) {
            arguments = callContext.getArgumentDataTypes().size();
            if (arguments == 1) {
                // If the argument is literal indicates use count(literal)
                countLiteral = callContext.isArgumentLiteral(0);
            }
        }
    }
}
