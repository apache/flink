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
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.lessThan;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.nullOf;

/** built-in min aggregate function. */
public abstract class MinAggFunction extends DeclarativeAggregateFunction {
    private UnresolvedReferenceExpression min = unresolvedRef("min");

    @Override
    public int operandCount() {
        return 1;
    }

    @Override
    public UnresolvedReferenceExpression[] aggBufferAttributes() {
        return new UnresolvedReferenceExpression[] {min};
    }

    @Override
    public DataType[] getAggBufferTypes() {
        return new DataType[] {getResultType()};
    }

    @Override
    public Expression[] initialValuesExpressions() {
        return new Expression[] {/* min = */ nullOf(getResultType())};
    }

    @Override
    public Expression[] accumulateExpressions() {
        return new Expression[] {
            /* min = */ ifThenElse(
                    isNull(operand(0)),
                    min,
                    ifThenElse(
                            isNull(min),
                            operand(0),
                            ifThenElse(lessThan(operand(0), min), operand(0), min)))
        };
    }

    @Override
    public Expression[] retractExpressions() {
        // TODO FLINK-12295, ignore exception now
        //		throw new TableException("This function does not support retraction, Please choose
        // MinWithRetractAggFunction.");
        return new Expression[0];
    }

    @Override
    public Expression[] mergeExpressions() {
        return new Expression[] {
            /* min = */ ifThenElse(
                    isNull(mergeOperand(min)),
                    min,
                    ifThenElse(
                            isNull(min),
                            mergeOperand(min),
                            ifThenElse(lessThan(mergeOperand(min), min), mergeOperand(min), min)))
        };
    }

    @Override
    public Expression getValueExpression() {
        return min;
    }

    /** Built-in Int Min aggregate function. */
    public static class IntMinAggFunction extends MinAggFunction {

        @Override
        public DataType getResultType() {
            return DataTypes.INT();
        }
    }

    /** Built-in Byte Min aggregate function. */
    public static class ByteMinAggFunction extends MinAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.TINYINT();
        }
    }

    /** Built-in Short Min aggregate function. */
    public static class ShortMinAggFunction extends MinAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.SMALLINT();
        }
    }

    /** Built-in Long Min aggregate function. */
    public static class LongMinAggFunction extends MinAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.BIGINT();
        }
    }

    /** Built-in Float Min aggregate function. */
    public static class FloatMinAggFunction extends MinAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.FLOAT();
        }
    }

    /** Built-in Double Min aggregate function. */
    public static class DoubleMinAggFunction extends MinAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.DOUBLE();
        }
    }

    /** Built-in Decimal Min aggregate function. */
    public static class DecimalMinAggFunction extends MinAggFunction {
        private DecimalType decimalType;

        public DecimalMinAggFunction(DecimalType decimalType) {
            this.decimalType = decimalType;
        }

        @Override
        public DataType getResultType() {
            return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale());
        }
    }

    /** Built-in Boolean Min aggregate function. */
    public static class BooleanMinAggFunction extends MinAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.BOOLEAN();
        }
    }

    /** Built-in String Min aggregate function. */
    public static class StringMinAggFunction extends MinAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.STRING();
        }
    }

    /** Built-in Date Min aggregate function. */
    public static class DateMinAggFunction extends MinAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.DATE();
        }
    }

    /** Built-in Time Min aggregate function. */
    public static class TimeMinAggFunction extends MinAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.TIME(TimeType.DEFAULT_PRECISION);
        }
    }

    /** Built-in Timestamp Min aggregate function. */
    public static class TimestampMinAggFunction extends MinAggFunction {

        private final TimestampType type;

        public TimestampMinAggFunction(TimestampType type) {
            this.type = type;
        }

        @Override
        public DataType getResultType() {
            return DataTypes.TIMESTAMP(type.getPrecision());
        }
    }

    /** Built-in TimestampLtz Min aggregate function. */
    public static class TimestampLtzMinAggFunction extends MinAggFunction {

        private final LocalZonedTimestampType type;

        public TimestampLtzMinAggFunction(LocalZonedTimestampType type) {
            this.type = type;
        }

        @Override
        public DataType getResultType() {
            return DataTypes.TIMESTAMP_LTZ(type.getPrecision());
        }
    }
}
