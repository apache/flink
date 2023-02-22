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
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.functions.DeclarativeAggregateFunction;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;

import java.math.BigDecimal;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.aggDecimalMinus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.aggDecimalPlus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.cast;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.minus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.plus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.typeLiteral;

/** built-in sum0 aggregate function. */
public abstract class Sum0AggFunction extends DeclarativeAggregateFunction {

    private final UnresolvedReferenceExpression sum0 = unresolvedRef("sum");

    @Override
    public int operandCount() {
        return 1;
    }

    @Override
    public UnresolvedReferenceExpression[] aggBufferAttributes() {
        return new UnresolvedReferenceExpression[] {sum0};
    }

    @Override
    public DataType[] getAggBufferTypes() {
        return new DataType[] {getResultType()};
    }

    @Override
    public Expression[] accumulateExpressions() {
        return new Expression[] {
            /* sum0 = */ adjustSumType(
                    ifThenElse(isNull(operand(0)), sum0, adjustedPlus(sum0, operand(0))))
        };
    }

    @Override
    public Expression[] retractExpressions() {
        return new Expression[] {
            /* sum0 = */ adjustSumType(
                    ifThenElse(isNull(operand(0)), sum0, adjustedMinus(sum0, operand(0))))
        };
    }

    @Override
    public Expression[] mergeExpressions() {
        return new Expression[] {
            /* sum0 = */ adjustSumType(adjustedPlus(sum0, mergeOperand(sum0)))
        };
    }

    private UnresolvedCallExpression adjustSumType(UnresolvedCallExpression sumExpr) {
        return cast(sumExpr, typeLiteral(getResultType()));
    }

    @Override
    public Expression getValueExpression() {
        return sum0;
    }

    protected UnresolvedCallExpression adjustedPlus(
            UnresolvedReferenceExpression arg1, UnresolvedReferenceExpression arg2) {
        return plus(arg1, arg2);
    }

    protected UnresolvedCallExpression adjustedMinus(
            UnresolvedReferenceExpression arg1, UnresolvedReferenceExpression arg2) {
        return minus(arg1, arg2);
    }

    /** Built-in Int Sum0 aggregate function. */
    public static class IntSum0AggFunction extends Sum0AggFunction {

        @Override
        public DataType getResultType() {
            return DataTypes.INT();
        }

        @Override
        public Expression[] initialValuesExpressions() {
            return new Expression[] {/* sum0 = */ literal(0, getResultType().notNull())};
        }
    }

    /** Built-in Byte Sum0 aggregate function. */
    public static class ByteSum0AggFunction extends Sum0AggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.TINYINT();
        }

        @Override
        public Expression[] initialValuesExpressions() {
            return new Expression[] {/* sum0 = */ literal((byte) 0, getResultType().notNull())};
        }
    }

    /** Built-in Short Sum0 aggregate function. */
    public static class ShortSum0AggFunction extends Sum0AggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.SMALLINT();
        }

        @Override
        public Expression[] initialValuesExpressions() {
            return new Expression[] {/* sum0 = */ literal((short) 0, getResultType().notNull())};
        }
    }

    /** Built-in Long Sum0 aggregate function. */
    public static class LongSum0AggFunction extends Sum0AggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.BIGINT();
        }

        @Override
        public Expression[] initialValuesExpressions() {
            return new Expression[] {/* sum0 = */ literal(0L, getResultType().notNull())};
        }
    }

    /** Built-in Float Sum0 aggregate function. */
    public static class FloatSum0AggFunction extends Sum0AggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.FLOAT();
        }

        @Override
        public Expression[] initialValuesExpressions() {
            return new Expression[] {/* sum0 = */ literal(0.0f, getResultType().notNull())};
        }
    }

    /** Built-in Double Sum0 aggregate function. */
    public static class DoubleSum0AggFunction extends Sum0AggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.DOUBLE();
        }

        @Override
        public Expression[] initialValuesExpressions() {
            return new Expression[] {/* sum0 = */ literal(0.0d, getResultType().notNull())};
        }
    }

    /** Built-in Decimal Sum0 aggregate function. */
    public static class DecimalSum0AggFunction extends Sum0AggFunction {
        private final DataType returnType;

        public DecimalSum0AggFunction(DecimalType decimalType) {
            DecimalType sumType = (DecimalType) LogicalTypeMerging.findSumAggType(decimalType);
            this.returnType = DataTypes.DECIMAL(sumType.getPrecision(), sumType.getScale());
        }

        @Override
        public DataType getResultType() {
            return returnType;
        }

        @Override
        public Expression[] initialValuesExpressions() {
            return new Expression[] {
                /* sum0 = */ literal(new BigDecimal(0), getResultType().notNull())
            };
        }

        @Override
        protected UnresolvedCallExpression adjustedPlus(
                UnresolvedReferenceExpression arg1, UnresolvedReferenceExpression arg2) {
            return aggDecimalPlus(arg1, arg2);
        }

        @Override
        protected UnresolvedCallExpression adjustedMinus(
                UnresolvedReferenceExpression arg1, UnresolvedReferenceExpression arg2) {
            return aggDecimalMinus(arg1, arg2);
        }
    }
}
