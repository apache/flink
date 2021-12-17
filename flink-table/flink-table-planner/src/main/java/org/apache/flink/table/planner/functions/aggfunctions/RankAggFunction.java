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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter;

import java.util.Arrays;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.and;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.equalTo;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.literal;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.not;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.plus;

/** built-in rank aggregate function. */
public class RankAggFunction extends RankLikeAggFunctionBase {

    private UnresolvedReferenceExpression currNumber = unresolvedRef("currNumber");

    public RankAggFunction(LogicalType[] orderKeyTypes) {
        super(orderKeyTypes);
    }

    @Override
    public UnresolvedReferenceExpression[] aggBufferAttributes() {
        UnresolvedReferenceExpression[] aggBufferAttrs =
                new UnresolvedReferenceExpression[2 + lastValues.length];
        aggBufferAttrs[0] = currNumber;
        aggBufferAttrs[1] = sequence;
        System.arraycopy(lastValues, 0, aggBufferAttrs, 2, lastValues.length);
        return aggBufferAttrs;
    }

    @Override
    public DataType[] getAggBufferTypes() {
        DataType[] aggBufferTypes = new DataType[2 + orderKeyTypes.length];
        aggBufferTypes[0] = DataTypes.BIGINT();
        aggBufferTypes[1] = DataTypes.BIGINT();
        System.arraycopy(
                Arrays.stream(orderKeyTypes)
                        .map(LogicalTypeDataTypeConverter::toDataType)
                        .toArray(DataType[]::new),
                0,
                aggBufferTypes,
                2,
                orderKeyTypes.length);
        return aggBufferTypes;
    }

    @Override
    public Expression[] initialValuesExpressions() {
        Expression[] initExpressions = new Expression[2 + orderKeyTypes.length];
        // currNumber = 0L
        initExpressions[0] = literal(0L);
        // sequence = 0L
        initExpressions[1] = literal(0L);
        for (int i = 0; i < orderKeyTypes.length; ++i) {
            // lastValue_i = init value
            initExpressions[i + 2] = generateInitLiteral(orderKeyTypes[i]);
        }
        return initExpressions;
    }

    @Override
    public Expression[] accumulateExpressions() {
        Expression[] accExpressions = new Expression[2 + operands().length];
        // currNumber = currNumber + 1
        accExpressions[0] = plus(currNumber, literal(1L));
        // sequence = if (lastValues equalTo orderKeys and sequence != 0) sequence else currNumber
        accExpressions[1] =
                ifThenElse(
                        and(orderKeyEqualsExpression(), not(equalTo(sequence, literal(0L)))),
                        sequence,
                        currNumber);
        Expression[] operands = operands();
        System.arraycopy(operands, 0, accExpressions, 2, operands.length);
        return accExpressions;
    }
}
