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
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;

import java.math.BigDecimal;

import static org.apache.flink.connectors.hive.HiveOptions.TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.and;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.coalesce;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.isTrue;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.nullOf;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.tryCast;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.typeLiteral;
import static org.apache.flink.table.types.logical.DecimalType.MAX_PRECISION;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;

/** built-in hive sum aggregate function. */
public class HiveSumAggFunction extends HiveDeclarativeAggregateFunction {

    private final UnresolvedReferenceExpression sum = unresolvedRef("sum");
    private final UnresolvedReferenceExpression isEmpty = unresolvedRef("isEmpty");

    private DataType resultType;
    private ValueLiteralExpression zero;

    @Override
    public int operandCount() {
        return 1;
    }

    @Override
    public UnresolvedReferenceExpression[] aggBufferAttributes() {
        return new UnresolvedReferenceExpression[] {sum, isEmpty};
    }

    @Override
    public DataType[] getAggBufferTypes() {
        return new DataType[] {getResultType(), DataTypes.BOOLEAN()};
    }

    @Override
    public DataType getResultType() {
        return resultType;
    }

    @Override
    public Expression[] initialValuesExpressions() {
        return new Expression[] {/* sum = */ nullOf(getResultType()), valueLiteral(true)};
    }

    @Override
    public Expression[] accumulateExpressions() {
        Expression tryCastOperand = tryCast(operand(0), typeLiteral(getResultType()));
        Expression coalesceSum = coalesce(sum, zero);
        return new Expression[] {
            /* sum = */ ifThenElse(
                    isNull(tryCastOperand),
                    coalesceSum,
                    adjustedPlus(getResultType(), coalesceSum, tryCastOperand)),
            and(isEmpty, isNull(operand(0)))
        };
    }

    @Override
    public Expression[] retractExpressions() {
        throw new TableException("Sum aggregate function does not support retraction.");
    }

    @Override
    public Expression[] mergeExpressions() {
        Expression coalesceSum = coalesce(sum, zero);
        return new Expression[] {
            /* sum = */ ifThenElse(
                    isNull(mergeOperand(sum)),
                    coalesceSum,
                    adjustedPlus(getResultType(), coalesceSum, mergeOperand(sum))),
            and(isEmpty, mergeOperand(isEmpty))
        };
    }

    @Override
    public Expression getValueExpression() {
        return ifThenElse(isTrue(isEmpty), nullOf(getResultType()), sum);
    }

    @Override
    public void setArguments(CallContext callContext) {
        if (resultType == null) {
            checkArgumentNum(callContext.getArgumentDataTypes());
            resultType = initResultType(callContext.getArgumentDataTypes().get(0));
            zero = defaultValue(resultType);
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
                                "Native hive sum aggregate function does not support type: %s. "
                                        + "Please set option '%s' to false to fall back to Hive's own sum function.",
                                argsType, TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED.key()));
            default:
                throw new TableException(
                        String.format(
                                "Only numeric or string type arguments are accepted but %s is passed.",
                                argsType));
        }
    }

    private ValueLiteralExpression defaultValue(DataType dataType) {
        switch (dataType.getLogicalType().getTypeRoot()) {
            case BIGINT:
                return valueLiteral(0L);
            case DOUBLE:
                return valueLiteral(0.0);
            case DECIMAL:
                return valueLiteral(
                        BigDecimal.valueOf(0, getScale(dataType.getLogicalType())),
                        dataType.notNull());
            default:
                throw new TableException(
                        String.format(
                                "Unsupported type %s is passed when initialize the default value.",
                                dataType));
        }
    }
}
