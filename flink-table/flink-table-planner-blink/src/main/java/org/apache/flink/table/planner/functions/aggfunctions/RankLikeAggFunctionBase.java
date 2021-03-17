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

import org.apache.flink.api.common.typeutils.base.LocalDateSerializer;
import org.apache.flink.api.common.typeutils.base.LocalDateTimeSerializer;
import org.apache.flink.api.common.typeutils.base.LocalTimeSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.planner.expressions.ExpressionBuilder;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Optional;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.equalTo;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromLogicalTypeToDataType;

/** built-in rank like aggregate function, e.g. rank, dense_rank */
public abstract class RankLikeAggFunctionBase extends DeclarativeAggregateFunction {
    protected UnresolvedReferenceExpression sequence = unresolvedRef("sequence");
    protected UnresolvedReferenceExpression[] lastValues;
    protected LogicalType[] orderKeyTypes;

    public RankLikeAggFunctionBase(LogicalType[] orderKeyTypes) {
        this.orderKeyTypes = orderKeyTypes;
        lastValues = new UnresolvedReferenceExpression[orderKeyTypes.length];
        for (int i = 0; i < orderKeyTypes.length; ++i) {
            lastValues[i] = unresolvedRef("lastValue_" + i);
        }
    }

    @Override
    public int operandCount() {
        return orderKeyTypes.length;
    }

    @Override
    public DataType getResultType() {
        return DataTypes.BIGINT();
    }

    @Override
    public Expression[] retractExpressions() {
        throw new TableException("This function does not support retraction.");
    }

    @Override
    public Expression[] mergeExpressions() {
        throw new TableException("This function does not support merge.");
    }

    @Override
    public Expression getValueExpression() {
        return sequence;
    }

    protected Expression orderKeyEqualsExpression() {
        Expression[] orderKeyEquals = new Expression[orderKeyTypes.length];
        for (int i = 0; i < orderKeyTypes.length; ++i) {
            // pseudo code:
            // if (lastValue_i is null) {
            //   if (operand(i) is null) true else false
            // } else {
            //   lastValue_i equalTo orderKey(i)
            // }
            Expression lasValue = lastValues[i];
            orderKeyEquals[i] =
                    ifThenElse(
                            isNull(lasValue),
                            ifThenElse(isNull(operand(i)), literal(true), literal(false)),
                            equalTo(lasValue, operand(i)));
        }
        Optional<Expression> ret = Arrays.stream(orderKeyEquals).reduce(ExpressionBuilder::and);
        return ret.orElseGet(() -> literal(true));
    }

    protected Expression generateInitLiteral(LogicalType orderType) {
        Object value;
        switch (orderType.getTypeRoot()) {
            case BOOLEAN:
                value = false;
                break;
            case TINYINT:
                value = (byte) 0;
                break;
            case SMALLINT:
                value = (short) 0;
                break;
            case INTEGER:
                value = 0;
                break;
            case BIGINT:
                value = 0L;
                break;
            case FLOAT:
                value = 0.0f;
                break;
            case DOUBLE:
                value = 0.0d;
                break;
            case DECIMAL:
                value = BigDecimal.ZERO;
                break;
            case CHAR:
            case VARCHAR:
                value = "";
                break;
            case DATE:
                value = LocalDateSerializer.INSTANCE.createInstance();
                break;
            case TIME_WITHOUT_TIME_ZONE:
                value = LocalTimeSerializer.INSTANCE.createInstance();
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                value = LocalDateTimeSerializer.INSTANCE.createInstance();
                break;
            default:
                throw new TableException("Unsupported type: " + orderType);
        }
        return valueLiteral(value, fromLogicalTypeToDataType(orderType).notNull());
    }
}
