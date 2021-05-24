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
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.utils.LogicalTypeMerging;

import java.math.BigDecimal;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.cast;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.div;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.equalTo;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.minus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.nullOf;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.plus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.typeLiteral;

/** built-in avg aggregate function. */
public abstract class AvgAggFunction extends DeclarativeAggregateFunction {

    private UnresolvedReferenceExpression sum = unresolvedRef("sum");
    private UnresolvedReferenceExpression count = unresolvedRef("count");

    public abstract DataType getSumType();

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
        return new DataType[] {getSumType(), DataTypes.BIGINT()};
    }

    @Override
    public Expression[] initialValuesExpressions() {
        return new Expression[] {
            /* sum = */ literal(0L, getSumType().notNull()), /* count = */ literal(0L)
        };
    }

    @Override
    public Expression[] accumulateExpressions() {
        return new Expression[] {
            /* sum = */ adjustSumType(ifThenElse(isNull(operand(0)), sum, plus(sum, operand(0)))),
            /* count = */ ifThenElse(isNull(operand(0)), count, plus(count, literal(1L))),
        };
    }

    @Override
    public Expression[] retractExpressions() {
        return new Expression[] {
            /* sum = */ adjustSumType(ifThenElse(isNull(operand(0)), sum, minus(sum, operand(0)))),
            /* count = */ ifThenElse(isNull(operand(0)), count, minus(count, literal(1L))),
        };
    }

    @Override
    public Expression[] mergeExpressions() {
        return new Expression[] {
            /* sum = */ adjustSumType(plus(sum, mergeOperand(sum))),
            /* count = */ plus(count, mergeOperand(count))
        };
    }

    private UnresolvedCallExpression adjustSumType(UnresolvedCallExpression sumExpr) {
        return cast(sumExpr, typeLiteral(getSumType()));
    }

    /** If all input are nulls, count will be 0 and we will get null after the division. */
    @Override
    public Expression getValueExpression() {
        Expression ifTrue = nullOf(getResultType());
        Expression ifFalse = cast(div(sum, count), typeLiteral(getResultType()));
        return ifThenElse(equalTo(count, literal(0L)), ifTrue, ifFalse);
    }

    /** Built-in Byte Avg aggregate function. */
    public static class ByteAvgAggFunction extends AvgAggFunction {

        @Override
        public DataType getResultType() {
            return DataTypes.TINYINT();
        }

        @Override
        public DataType getSumType() {
            return DataTypes.BIGINT();
        }
    }

    /** Built-in Short Avg aggregate function. */
    public static class ShortAvgAggFunction extends AvgAggFunction {

        @Override
        public DataType getResultType() {
            return DataTypes.SMALLINT();
        }

        @Override
        public DataType getSumType() {
            return DataTypes.BIGINT();
        }
    }

    /** Built-in Integer Avg aggregate function. */
    public static class IntAvgAggFunction extends AvgAggFunction {

        @Override
        public DataType getResultType() {
            return DataTypes.INT();
        }

        @Override
        public DataType getSumType() {
            return DataTypes.BIGINT();
        }
    }

    /** Built-in Long Avg aggregate function. */
    public static class LongAvgAggFunction extends AvgAggFunction {

        @Override
        public DataType getResultType() {
            return DataTypes.BIGINT();
        }

        @Override
        public DataType getSumType() {
            return DataTypes.BIGINT();
        }
    }

    /** Built-in Float Avg aggregate function. */
    public static class FloatAvgAggFunction extends AvgAggFunction {

        @Override
        public DataType getResultType() {
            return DataTypes.FLOAT();
        }

        @Override
        public DataType getSumType() {
            return DataTypes.DOUBLE();
        }

        @Override
        public Expression[] initialValuesExpressions() {
            return new Expression[] {literal(0D), literal(0L)};
        }
    }

    /** Built-in Double Avg aggregate function. */
    public static class DoubleAvgAggFunction extends AvgAggFunction {

        @Override
        public DataType getResultType() {
            return DataTypes.DOUBLE();
        }

        @Override
        public DataType getSumType() {
            return DataTypes.DOUBLE();
        }

        @Override
        public Expression[] initialValuesExpressions() {
            return new Expression[] {literal(0D), literal(0L)};
        }
    }

    /** Built-in Decimal Avg aggregate function. */
    public static class DecimalAvgAggFunction extends AvgAggFunction {

        private final DecimalType type;

        public DecimalAvgAggFunction(DecimalType type) {
            this.type = type;
        }

        @Override
        public DataType getResultType() {
            DecimalType t = (DecimalType) LogicalTypeMerging.findAvgAggType(type);
            return DataTypes.DECIMAL(t.getPrecision(), t.getScale());
        }

        @Override
        public DataType getSumType() {
            DecimalType t = (DecimalType) LogicalTypeMerging.findSumAggType(type);
            return DataTypes.DECIMAL(t.getPrecision(), t.getScale());
        }

        @Override
        public Expression[] initialValuesExpressions() {
            return new Expression[] {literal(BigDecimal.ZERO, getSumType().notNull()), literal(0L)};
        }
    }
}
