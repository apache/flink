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
import org.apache.flink.table.functions.DeclarativeAggregateFunction;
import org.apache.flink.table.types.DataType;

import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.sql.fun.SqlLiteralAggFunction;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.nullOf;

/**
 * Built-in literal aggregate function. This function is used for internal optimizations. It accepts
 * zero regular aggregate arguments and returns a constant value. For more details see <a
 * href="https://issues.apache.org/jira/browse/CALCITE-4334">CALCITE-4334</a> and {@link
 * SqlLiteralAggFunction}.
 */
public abstract class LiteralAggFunction extends DeclarativeAggregateFunction {

    private final UnresolvedReferenceExpression literalAgg = unresolvedRef("literalAgg");
    private final RexLiteral rexLiteral;

    public LiteralAggFunction(RexLiteral rexLiteral) {
        this.rexLiteral = rexLiteral;
    }

    @Override
    public int operandCount() {
        return 0;
    }

    @Override
    public UnresolvedReferenceExpression[] aggBufferAttributes() {
        return new UnresolvedReferenceExpression[] {literalAgg};
    }

    @Override
    public DataType[] getAggBufferTypes() {
        return new DataType[] {getResultType()};
    }

    @Override
    public Expression[] initialValuesExpressions() {
        return new Expression[] {nullOf(getResultType())};
    }

    @Override
    public Expression[] accumulateExpressions() {
        return new Expression[] {literal(rexLiteral.getValue(), getResultType())};
    }

    @Override
    public Expression[] retractExpressions() {
        return new Expression[] {literal(rexLiteral.getValue(), getResultType())};
    }

    @Override
    public Expression[] mergeExpressions() {
        return new Expression[] {literal(rexLiteral.getValue(), getResultType())};
    }

    @Override
    public Expression getValueExpression() {
        return literal(rexLiteral.getValue(), getResultType());
    }

    /** Built-in Boolean Literal aggregate function. */
    public static class BooleanLiteralAggFunction extends LiteralAggFunction {

        public BooleanLiteralAggFunction(RexLiteral rexLiteral) {
            super(rexLiteral);
        }

        @Override
        public DataType getResultType() {
            return DataTypes.BOOLEAN();
        }
    }

    /** Built-in Byte Literal aggregate function. */
    public static class ByteLiteralAggFunction extends LiteralAggFunction {

        public ByteLiteralAggFunction(RexLiteral rexLiteral) {
            super(rexLiteral);
        }

        @Override
        public DataType getResultType() {
            return DataTypes.TINYINT();
        }
    }

    /** Built-in Short Literal aggregate function. */
    public static class ShortLiteralAggFunction extends LiteralAggFunction {

        public ShortLiteralAggFunction(RexLiteral rexLiteral) {
            super(rexLiteral);
        }

        @Override
        public DataType getResultType() {
            return DataTypes.SMALLINT();
        }
    }

    /** Built-in Integer Literal aggregate function. */
    public static class IntLiteralAggFunction extends LiteralAggFunction {

        public IntLiteralAggFunction(RexLiteral rexLiteral) {
            super(rexLiteral);
        }

        @Override
        public DataType getResultType() {
            return DataTypes.INT();
        }
    }

    /** Built-in Long Literal aggregate function. */
    public static class LongLiteralAggFunction extends LiteralAggFunction {

        public LongLiteralAggFunction(RexLiteral rexLiteral) {
            super(rexLiteral);
        }

        @Override
        public DataType getResultType() {
            return DataTypes.BIGINT();
        }
    }

    /** Built-in Float Literal aggregate function. */
    public static class FloatLiteralAggFunction extends LiteralAggFunction {

        public FloatLiteralAggFunction(RexLiteral rexLiteral) {
            super(rexLiteral);
        }

        @Override
        public DataType getResultType() {
            return DataTypes.FLOAT();
        }
    }

    /** Built-in Double Literal aggregate function. */
    public static class DoubleLiteralAggFunction extends LiteralAggFunction {

        public DoubleLiteralAggFunction(RexLiteral rexLiteral) {
            super(rexLiteral);
        }

        @Override
        public DataType getResultType() {
            return DataTypes.DOUBLE();
        }
    }
}
