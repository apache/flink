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
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.greaterThan;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.isNull;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.nullOf;

/** built-in max aggregate function. */
public abstract class MaxAggFunction extends DeclarativeAggregateFunction {
    private UnresolvedReferenceExpression max = unresolvedRef("max");

    @Override
    public int operandCount() {
        return 1;
    }

    @Override
    public UnresolvedReferenceExpression[] aggBufferAttributes() {
        return new UnresolvedReferenceExpression[] {max};
    }

    @Override
    public DataType[] getAggBufferTypes() {
        return new DataType[] {getResultType()};
    }

    @Override
    public Expression[] initialValuesExpressions() {
        return new Expression[] {/* max = */ nullOf(getResultType())};
    }

    @Override
    public Expression[] accumulateExpressions() {
        return new Expression[] {
            /* max = */ ifThenElse(
                    isNull(operand(0)),
                    max,
                    ifThenElse(
                            isNull(max),
                            operand(0),
                            ifThenElse(greaterThan(operand(0), max), operand(0), max)))
        };
    }

    @Override
    public Expression[] retractExpressions() {
        // TODO FLINK-12295, ignore exception now
        //		throw new TableException("This function does not support retraction, Please choose
        // MaxWithRetractAggFunction.");
        return new Expression[0];
    }

    @Override
    public Expression[] mergeExpressions() {
        return new Expression[] {
            /* max = */ ifThenElse(
                    isNull(mergeOperand(max)),
                    max,
                    ifThenElse(
                            isNull(max),
                            mergeOperand(max),
                            ifThenElse(
                                    greaterThan(mergeOperand(max), max), mergeOperand(max), max)))
        };
    }

    @Override
    public Expression getValueExpression() {
        return max;
    }

    /** Built-in Int Max aggregate function. */
    public static class IntMaxAggFunction extends MaxAggFunction {

        @Override
        public DataType getResultType() {
            return DataTypes.INT();
        }
    }

    /** Built-in Byte Max aggregate function. */
    public static class ByteMaxAggFunction extends MaxAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.TINYINT();
        }
    }

    /** Built-in Short Max aggregate function. */
    public static class ShortMaxAggFunction extends MaxAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.SMALLINT();
        }
    }

    /** Built-in Long Max aggregate function. */
    public static class LongMaxAggFunction extends MaxAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.BIGINT();
        }
    }

    /** Built-in Float Max aggregate function. */
    public static class FloatMaxAggFunction extends MaxAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.FLOAT();
        }
    }

    /** Built-in Double Max aggregate function. */
    public static class DoubleMaxAggFunction extends MaxAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.DOUBLE();
        }
    }

    /** Built-in Decimal Max aggregate function. */
    public static class DecimalMaxAggFunction extends MaxAggFunction {
        private DecimalType decimalType;

        public DecimalMaxAggFunction(DecimalType decimalType) {
            this.decimalType = decimalType;
        }

        @Override
        public DataType getResultType() {
            return DataTypes.DECIMAL(decimalType.getPrecision(), decimalType.getScale());
        }
    }

    /** Built-in Boolean Max aggregate function. */
    public static class BooleanMaxAggFunction extends MaxAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.BOOLEAN();
        }
    }

    /** Built-in String Max aggregate function. */
    public static class StringMaxAggFunction extends MaxAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.STRING();
        }
    }

    /** Built-in Date Max aggregate function. */
    public static class DateMaxAggFunction extends MaxAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.DATE();
        }
    }

    /** Built-in Time Max aggregate function. */
    public static class TimeMaxAggFunction extends MaxAggFunction {
        @Override
        public DataType getResultType() {
            return DataTypes.TIME(TimeType.DEFAULT_PRECISION);
        }
    }

    /** Built-in Timestamp Max aggregate function. */
    public static class TimestampMaxAggFunction extends MaxAggFunction {

        private final TimestampType type;

        public TimestampMaxAggFunction(TimestampType type) {
            this.type = type;
        }

        @Override
        public DataType getResultType() {
            return DataTypes.TIMESTAMP(type.getPrecision());
        }
    }

    /** Built-in TimestampLtz Max aggregate function. */
    public static class TimestampLtzMaxAggFunction extends MaxAggFunction {

        private final LocalZonedTimestampType type;

        public TimestampLtzMaxAggFunction(LocalZonedTimestampType type) {
            this.type = type;
        }

        @Override
        public DataType getResultType() {
            return DataTypes.TIMESTAMP_LTZ(type.getPrecision());
        }
    }
}
