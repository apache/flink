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
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.types.logical.LogicalType;

import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.cast;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.div;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.greaterThan;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.ifThenElse;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.minus;
import static org.apache.flink.table.planner.expressions.ExpressionBuilder.typeLiteral;

/** built-in percent_rank aggregate function. */
public class PercentRankAggFunction extends RankAggFunction implements SizeBasedWindowFunction {

    private final ValueLiteralExpression one = valueLiteral(1);

    public PercentRankAggFunction(LogicalType[] orderKeyTypes) {
        super(orderKeyTypes);
    }

    @Override
    public Expression getValueExpression() {
        return ifThenElse(
                greaterThan(windowSizeAttribute(), one),
                div(
                        cast(minus(sequence, one), typeLiteral(DataTypes.DOUBLE())),
                        cast(minus(windowSizeAttribute(), one), typeLiteral(DataTypes.DOUBLE()))),
                valueLiteral(0.0d));
    }
}
