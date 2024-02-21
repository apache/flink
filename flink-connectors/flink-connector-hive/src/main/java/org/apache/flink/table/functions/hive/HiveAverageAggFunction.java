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
import org.apache.flink.table.types.logical.LogicalType;

import java.math.BigDecimal;

import static org.apache.flink.connectors.hive.HiveOptions.TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.cast;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.div;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.equalTo;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.nullOf;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.plus;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.tryCast;
import static org.apache.flink.table.planner.delegation.hive.expressions.ExpressionBuilder.typeLiteral;
import static org.apache.flink.table.types.logical.DecimalType.MAX_PRECISION;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getPrecision;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getScale;

/** built-in hive average aggregate function. */
public class HiveAverageAggFunction extends HiveDeclarativeAggregateFunction {

    private final UnresolvedReferenceExpression sum = unresolvedRef("sum");
    private final UnresolvedReferenceExpression count = unresolvedRef("count");
    private DataType resultType;
    private DataType bufferedSumType;

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
        return new DataType[] {getBufferedSumType(), DataTypes.BIGINT()};
    }

    @Override
    public DataType getResultType() {
        return resultType;
    }

    @Override
    public Expression[] initialValuesExpressions() {
        return new Expression[] {/* sum = */ sumInitialValue(), /* count = */ literal(0L)};
    }

    @Override
    public Expression[] accumulateExpressions() {
        // cast the operand to sum needed type
        Expression tryCastOperand = tryCast(operand(0), typeLiteral(getBufferedSumType()));
        return new Expression[] {
            /* sum = */ ifThenElse(
                    isNull(tryCastOperand),
                    sum,
                    adjustedPlus(getBufferedSumType(), sum, tryCastOperand)),
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
            /* sum = */ adjustedPlus(getBufferedSumType(), sum, mergeOperand(sum)),
            /* count = */ plus(count, mergeOperand(count))
        };
    }

    @Override
    public Expression getValueExpression() {
        // If all input are nulls, count will be 0 and we will get null after the division.
        Expression ifTrue = nullOf(getResultType());
        Expression ifFalse = cast(div(sum, count), typeLiteral(getResultType()));
        return ifThenElse(equalTo(count, literal(0L)), ifTrue, ifFalse);
    }

    @Override
    public void setArguments(CallContext callContext) {
        if (resultType == null) {
            if (callContext.getArgumentDataTypes().size() != 1) {
                throw new TableException("Exactly one argument is expected.");
            }
            DataType argsType = callContext.getArgumentDataTypes().get(0);
            resultType = initResultType(argsType);
            bufferedSumType = initBufferedSumType(argsType);
        }
    }

    private DataType initResultType(DataType argsType) {
        switch (argsType.getLogicalType().getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
                return DataTypes.DOUBLE();
            case DECIMAL:
                return getResultTypeForDecimal(argsType.getLogicalType());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                throw new TableException(
                        String.format(
                                "Native hive avg aggregate function does not support type: %s. "
                                        + "Please set option '%s' to false to fall back to Hive's own avg function.",
                                argsType, TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED.key()));
            default:
                throw new TableException(
                        String.format(
                                "Only numeric or string type arguments are accepted but %s is passed.",
                                argsType));
        }
    }

    private DataType getResultTypeForDecimal(LogicalType decimalType) {
        int precision = getPrecision(decimalType);
        int scale = getScale(decimalType);

        int intPart = precision - scale;
        // The avg result type has the same number of integer digits and 4 more decimal digits.
        scale = Math.min(scale + 4, MAX_SCALE - intPart);
        return DataTypes.DECIMAL(intPart + scale, scale);
    }

    private DataType initBufferedSumType(DataType argsType) {
        switch (argsType.getLogicalType().getTypeRoot()) {
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case CHAR:
            case VARCHAR:
                return DataTypes.DOUBLE();
            case DECIMAL:
                return getBufferedSumTypeForDecimal(argsType.getLogicalType());
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                throw new TableException(
                        String.format(
                                "Native hive avg aggregate function does not support type: %s. "
                                        + "Please set option '%s' to false to fall back to Hive's own avg function.",
                                argsType, TABLE_EXEC_HIVE_NATIVE_AGG_FUNCTION_ENABLED.key()));
            default:
                throw new TableException(
                        String.format(
                                "Only numeric or string type arguments are accepted but %s is passed.",
                                argsType));
        }
    }

    private DataType getBufferedSumTypeForDecimal(LogicalType decimalType) {
        int precision = getPrecision(decimalType);
        int scale = getScale(decimalType);

        int intPart = precision - scale;
        // The intermediate sum field has 10 more integer digits with the same scale.
        intPart = Math.min(intPart + 10, MAX_PRECISION - scale);
        return DataTypes.DECIMAL(intPart + scale, scale);
    }

    private DataType getBufferedSumType() {
        return bufferedSumType;
    }

    private ValueLiteralExpression sumInitialValue() {
        if (getBufferedSumType().getLogicalType().is(DECIMAL)) {
            return literal(BigDecimal.ZERO, getBufferedSumType());
        } else {
            return literal(0D, getBufferedSumType());
        }
    }
}
