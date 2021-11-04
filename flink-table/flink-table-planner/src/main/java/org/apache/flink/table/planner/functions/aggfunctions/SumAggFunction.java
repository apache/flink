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
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.aggDecimalPlus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.nullOf;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.plus;

/** built-in sum aggregate function. */
public abstract class SumAggFunction extends DeclarativeAggregateFunction {

    protected UnresolvedReferenceExpression sum = unresolvedRef("sum");

    @Override
    public int operandCount() {
        return 1;
    }

    @Override
    public UnresolvedReferenceExpression[] aggBufferAttributes() {
        return new UnresolvedReferenceExpression[] {sum};
    }

    @Override
    public DataType[] getAggBufferTypes() {
        return new DataType[] {getResultType()};
    }

    @Override
    public Expression[] initialValuesExpressions() {
        return new Expression[] {/* sum = */ nullOf(getResultType())};
    }

    @Override
    public Expression[] accumulateExpressions() {
        return new Expression[] {
            /* sum = */ ifThenElse(
                    isNull(operand(0)),
                    sum,
                    ifThenElse(
                            isNull(operand(0)),
                            sum,
                            ifThenElse(isNull(sum), operand(0), doPlus(sum, operand(0)))))
        };
    }

    @Override
    public Expression[] retractExpressions() {
        throw new TableException(
                "This function does not support retraction, Please choose SumWithRetractAggFunction.");
    }

    @Override
    public Expression[] mergeExpressions() {
        return new Expression[] {
            /* sum = */ ifThenElse(
                    isNull(mergeOperand(sum)),
                    sum,
                    ifThenElse(isNull(sum), mergeOperand(sum), doPlus(sum, mergeOperand(sum))))
        };
    }

    protected UnresolvedCallExpression doPlus(
            UnresolvedReferenceExpression arg1, UnresolvedReferenceExpression arg2) {
        return plus(arg1, arg2);
    }

    @Override
    public Expression getValueExpression() {
        return sum;
    }

    /** Built-in Int Sum aggregate function. */
    public static class IntSumAggFunction extends SumAggFunction {

        @Override
        public DataType getResultType() {
            return DataTypes.INT();
        }
    }

    /** Built-in Byte Sum aggregate function. */
    public static class ByteSumAggFunction extends SumAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.TINYINT();
        }
    }

    /** Built-in Short Sum aggregate function. */
    public static class ShortSumAggFunction extends SumAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.SMALLINT();
        }
    }

    /** Built-in Long Sum aggregate function. */
    public static class LongSumAggFunction extends SumAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.BIGINT();
        }
    }

    /** Built-in Float Sum aggregate function. */
    public static class FloatSumAggFunction extends SumAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.FLOAT();
        }
    }

    /** Built-in Double Sum aggregate function. */
    public static class DoubleSumAggFunction extends SumAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.DOUBLE();
        }
    }

    /** Built-in Decimal Sum aggregate function. */
    public static class DecimalSumAggFunction extends SumAggFunction {
        private DecimalType decimalType;
        private DataType returnType;

        public DecimalSumAggFunction(DecimalType decimalType) {
            this.decimalType = decimalType;
        }

        @Override
        public DataType getResultType() {
            if (returnType == null) {
                DecimalType sumType = (DecimalType) LogicalTypeMerging.findSumAggType(decimalType);
                returnType = DataTypes.DECIMAL(sumType.getPrecision(), sumType.getScale());
            }
            return returnType;
        }

        @Override
        protected UnresolvedCallExpression doPlus(
                UnresolvedReferenceExpression arg1, UnresolvedReferenceExpression arg2) {
            return aggDecimalPlus(arg1, arg2);
        }
    }
}
