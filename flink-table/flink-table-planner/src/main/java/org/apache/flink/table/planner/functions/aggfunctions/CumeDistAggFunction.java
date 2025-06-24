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
import org.apache.flink.table.expressions.UnresolvedReferenceExpression;
import org.apache.flink.table.functions.DeclarativeAggregateFunction;
import org.apache.flink.table.types.DataType;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.cast;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.div;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.plus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.typeLiteral;

/** built-in CUME_DIST agg function. */
public class CumeDistAggFunction extends DeclarativeAggregateFunction
        implements SizeBasedWindowFunction {

    private final UnresolvedReferenceExpression sequence = unresolvedRef("seq");

    @Override
    public int operandCount() {
        return 0;
    }

    @Override
    public UnresolvedReferenceExpression[] aggBufferAttributes() {
        return new UnresolvedReferenceExpression[] {sequence};
    }

    @Override
    public DataType[] getAggBufferTypes() {
        return new DataType[] {DataTypes.INT()};
    }

    @Override
    public DataType getResultType() {
        return DataTypes.DOUBLE();
    }

    @Override
    public Expression[] initialValuesExpressions() {
        return new Expression[] {literal(0, DataTypes.INT())};
    }

    @Override
    public Expression[] accumulateExpressions() {
        return new Expression[] {plus(sequence, literal(1, DataTypes.INT()))};
    }

    @Override
    public Expression[] retractExpressions() {
        throw new TableException("This function does not support retraction.");
    }

    @Override
    public Expression[] mergeExpressions() {
        throw new TableException("This function does not support merge.");
    }

    @Override
    public Expression getValueExpression() {
        return div(
                cast(sequence, typeLiteral(DataTypes.DOUBLE())),
                cast(windowSizeAttribute(), typeLiteral(DataTypes.DOUBLE())));
    }
}
