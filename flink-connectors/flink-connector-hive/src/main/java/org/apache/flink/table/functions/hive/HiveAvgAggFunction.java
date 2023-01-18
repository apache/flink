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
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.logical.LogicalType;

import java.math.BigDecimal;

import static org.apache.flink.connectors.hive.HiveOptions.TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.cast;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.div;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.equalTo;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.hiveAggDecimalPlus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.nullOf;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.plus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.tryCast;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.typeLiteral;
import static org.apache.flink.table.types.logical.DecimalType.MAX_PRECISION;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;

/** built-in hive avg aggregate function. */
public class HiveAvgAggFunction extends HiveDeclarativeAggregateFunction {

    private final UnresolvedReferenceExpression sum = unresolvedRef("sum");
    private final UnresolvedReferenceExpression count = unresolvedRef("count");
    private DataType resultType;
    private DataType sumType;

    @Override
    public int operandCount() {
        return 1;
    }

    @Override
    public UnresolvedReferenceExpression[] aggBufferAttributes() {
        return new UnresolvedReferenceExpression[] {sum, count};
    }

    @Override
    public DataType[] getAggBufferTypes() {
        return new DataType[] {getResultType(), DataTypes.BIGINT()};
    }

    @Override
    public DataType getResultType() {
        return resultType;
    }

    @Override
    public Expression[] initialValuesExpressions() {
        switch (resultType.getLogicalType().getTypeRoot()) {
            case DECIMAL:
                return new Expression[] {literal(BigDecimal.ZERO, sumType.notNull()), literal(0L)};
            case DOUBLE:
            case FLOAT:
                return new Expression[] {literal(0D), literal(0L)};
            default:
                return new Expression[] {
                    /* sum = */ literal(0L, sumType.notNull()), /* count = */ literal(0L)
                };
        }
    }

    @Override
    public Expression[] accumulateExpressions() {
        Expression tryCastOperand = tryCast(operand(0), typeLiteral(getResultType()));
        return new Expression[] {
            /* sum = */ ifThenElse(isNull(tryCastOperand), sum, adjustedPlus(sum, tryCastOperand)),
            /* count = */ ifThenElse(isNull(tryCastOperand), count, plus(count, literal(1L))),
        };
    }

    @Override
    public Expression[] retractExpressions() {
        throw new TableException("Avg aggregate function does not support retraction.");
    }

    @Override
    public Expression[] mergeExpressions() {
        return new Expression[] {
            /* sum = */ adjustedPlus(sum, mergeOperand(sum)),
            /* count = */ plus(count, mergeOperand(count))
        };
    }

    @Override
    public Expression getValueExpression() {
        Expression ifTrue = nullOf(getResultType());
        Expression ifFalse = cast(div(sum, count), typeLiteral(getResultType()));
        return ifThenElse(equalTo(count, literal(0L)), ifTrue, ifFalse);
    }

    @Override
    public void setArguments(CallContext callContext) {
        if (resultType == null) {
            // check argument type firstly
            checkArgumentType(callContext.getArgumentDataTypes().get(0).getLogicalType());
            resultType = initResultType(callContext.getArgumentDataTypes().get(0));
            sumType = resultType;
        }
    }

    private DataType initResultType(DataType argsType) {
        switch (argsType.getLogicalType().getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return DataTypes.BIGINT();
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
                return DataTypes.DOUBLE();
            case DECIMAL:
                int precision =
                        Math.min(MAX_PRECISION, getPrecision(argsType.getLogicalType()) + 10);
                return DataTypes.DECIMAL(precision, getScale(argsType.getLogicalType()));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                throw new TableException(
                        String.format(
                                "Native hive avg aggregate function does not support type: %s. Please set option '%s' to false.",
                                argsType, TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED.key()));
            default:
                throw new TableException(
                        String.format(
                                "Only numeric type arguments are accepted but %s is passed.",
                                argsType));
        }
    }

    private void checkArgumentType(LogicalType logicalType) {
        switch (logicalType.getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return;
            default:
                throw new TableException(
                        String.format(
                                "Only numeric type arguments are accepted but %s is passed.",
                                logicalType.getTypeRoot()));
        }
    }

    private UnresolvedCallExpression adjustedPlus(Expression arg1, Expression arg2) {
        if (getResultType().getLogicalType().is(DECIMAL)) {
            return hiveAggDecimalPlus(arg1, arg2);
        } else {
            return plus(arg1, arg2);
        }
    }
}
