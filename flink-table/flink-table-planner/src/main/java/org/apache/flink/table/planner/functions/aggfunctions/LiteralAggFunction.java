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

import org.apache.calcite.sql.fun.SqlLiteralAggFunction;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;

/**
 * Built-in literal aggregate function. This function is used for internal optimizations. It accepts
 * zero regular aggregate arguments and returns a constant value. For more details see <a
 * href="https://issues.apache.org/jira/browse/CALCITE-4334">CALCITE-4334</a> and {@link
 * SqlLiteralAggFunction}.
 */
public abstract class LiteralAggFunction extends DeclarativeAggregateFunction {

    private final UnresolvedReferenceExpression literalAgg = unresolvedRef("literalAgg");
    private static final Expression[] EMPTY_EXPRS = new Expression[0];
    private static final UnresolvedReferenceExpression[] EMPTY_UNRESOLVED_EXPRS =
            new UnresolvedReferenceExpression[0];
    private static final DataType[] EMPTY_DATATYPES = new DataType[0];

    protected LiteralAggFunction() {}

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
        return EMPTY_DATATYPES;
    }

    @Override
    public Expression[] initialValuesExpressions() {
        return EMPTY_EXPRS;
    }

    @Override
    public Expression[] accumulateExpressions() {
        return EMPTY_EXPRS;
    }

    @Override
    public Expression[] retractExpressions() {
        return EMPTY_EXPRS;
    }

    @Override
    public Expression[] mergeExpressions() {
        return EMPTY_EXPRS;
    }

    @Override
    public Expression getValueExpression() {
        return literal(getResultType());
    }

    public Expression[] getValueExpressions() {
        return new Expression[] {getValueExpression()};
    }

    public String getAttrName() {
        return "literalAgg";
    }

    /** Built-in Boolean Literal aggregate function. */
    public static class BooleanLiteralAggFunction extends LiteralAggFunction {

        public static final BooleanLiteralAggFunction INSTANCE = new BooleanLiteralAggFunction();

        private BooleanLiteralAggFunction() {}

        @Override
        public DataType getResultType() {
            return DataTypes.BOOLEAN();
        }

        @Override
        public Expression getValueExpression() {
            return literal(Boolean.TRUE, getResultType());
        }
    }

    /** Built-in Byte Literal aggregate function. */
    public static class ByteLiteralAggFunction extends LiteralAggFunction {

        public static final ByteLiteralAggFunction INSTANCE = new ByteLiteralAggFunction();

        private ByteLiteralAggFunction() {}

        @Override
        public DataType getResultType() {
            return DataTypes.TINYINT();
        }
    }

    /** Built-in Short Literal aggregate function. */
    public static class ShortLiteralAggFunction extends LiteralAggFunction {

        public static final ShortLiteralAggFunction INSTANCE = new ShortLiteralAggFunction();

        private ShortLiteralAggFunction() {}

        @Override
        public DataType getResultType() {
            return DataTypes.SMALLINT();
        }
    }

    /** Built-in Integer Literal aggregate function. */
    public static class IntLiteralAggFunction extends LiteralAggFunction {

        public static final IntLiteralAggFunction INSTANCE = new IntLiteralAggFunction();

        private IntLiteralAggFunction() {}

        @Override
        public DataType getResultType() {
            return DataTypes.INT();
        }
    }

    /** Built-in Long Literal aggregate function. */
    public static class LongLiteralAggFunction extends LiteralAggFunction {

        public static final LongLiteralAggFunction INSTANCE = new LongLiteralAggFunction();

        private LongLiteralAggFunction() {}

        @Override
        public DataType getResultType() {
            return DataTypes.BIGINT();
        }
    }

    /** Built-in Float Literal aggregate function. */
    public static class FloatLiteralAggFunction extends LiteralAggFunction {

        public static final FloatLiteralAggFunction INSTANCE = new FloatLiteralAggFunction();

        private FloatLiteralAggFunction() {}

        @Override
        public DataType getResultType() {
            return DataTypes.FLOAT();
        }
    }

    /** Built-in Double Literal aggregate function. */
    public static class DoubleLiteralAggFunction extends LiteralAggFunction {

        public static final DoubleLiteralAggFunction INSTANCE = new DoubleLiteralAggFunction();

        private DoubleLiteralAggFunction() {}

        @Override
        public DataType getResultType() {
            return DataTypes.DOUBLE();
        }
    }
}
