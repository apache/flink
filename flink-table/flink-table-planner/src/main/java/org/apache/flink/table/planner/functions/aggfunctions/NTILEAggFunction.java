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
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.div;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.greaterThan;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.lessThanOrEqual;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.mod;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.plus;

/** built-in NTILE agg function. */
public class NTILEAggFunction extends DeclarativeAggregateFunction
        implements SizeBasedWindowFunction {

    private final Expression bucketSize;
    private final Expression bucketsWithPadding;
    private final UnresolvedReferenceExpression rowNumber = unresolvedRef("rowNumber");
    private final UnresolvedReferenceExpression bucket = unresolvedRef("bucket");
    private final UnresolvedReferenceExpression bucketThreshold = unresolvedRef("bucketThreshold");

    public NTILEAggFunction() {
        this.bucketSize = div(windowSizeAttribute(), operand(0));
        this.bucketsWithPadding = mod(windowSizeAttribute(), operand(0));
    }

    @Override
    public int operandCount() {
        return 1;
    }

    @Override
    public UnresolvedReferenceExpression[] aggBufferAttributes() {
        return new UnresolvedReferenceExpression[] {rowNumber, bucket, bucketThreshold};
    }

    @Override
    public DataType[] getAggBufferTypes() {
        return new DataType[] {DataTypes.BIGINT(), DataTypes.BIGINT(), DataTypes.BIGINT()};
    }

    @Override
    public DataType getResultType() {
        return DataTypes.BIGINT();
    }

    @Override
    public Expression[] initialValuesExpressions() {
        return new Expression[] {valueLiteral(0L), valueLiteral(0L), valueLiteral(0L)};
    }

    @Override
    public Expression[] accumulateExpressions() {
        return new Expression[] {
            /* rowNumber = */ plus(rowNumber, literal(1L)),
            /* bucket = */ plus(bucket, bucketOverflowThen(literal(1L))),
            /* bucketThreshold = */ plus(
                    bucketThreshold,
                    bucketOverflowThen(
                            plus(
                                    bucketSize,
                                    ifThenElse(
                                            lessThanOrEqual(bucket, bucketsWithPadding),
                                            valueLiteral(1),
                                            valueLiteral(0)))))
        };
    }

    private Expression bucketOverflowThen(Expression e) {
        return ifThenElse(greaterThan(rowNumber, bucketThreshold), e, valueLiteral(0L));
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
        return bucket;
    }
}
