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

package org.apache.flink.table.operations.utils;

import org.apache.flink.table.api.OverWindowRange;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.expressions.UnresolvedCallExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedCall;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unresolvedRef;
import static org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral;
import static org.assertj.core.api.Assertions.assertThat;

class OperationExpressionsUtilsTest {

    @Test
    void keepsAggregatesInsideExpandedOverExpressionsInProjection() {
        Expression overExpression =
                unresolvedCall(
                        BuiltInFunctionDefinitions.OVER,
                        Arrays.asList(
                                unresolvedCall(
                                        BuiltInFunctionDefinitions.SUM, unresolvedRef("amount")),
                                unresolvedRef("event_time"),
                                valueLiteral(1L),
                                valueLiteral(OverWindowRange.CURRENT_ROW)));

        OperationExpressionsUtils.CategorizedExpressions categorized =
                OperationExpressionsUtils.extractAggregationsAndProperties(List.of(overExpression));
        UnresolvedCallExpression projectedOver =
                (UnresolvedCallExpression) categorized.getProjections().get(0);

        assertThat(categorized.getAggregations()).isEmpty();
        assertThat(projectedOver.getFunctionDefinition())
                .isEqualTo(BuiltInFunctionDefinitions.OVER);
        assertThat(projectedOver.getChildren().get(0)).isInstanceOf(UnresolvedCallExpression.class);
        assertThat(
                        ((UnresolvedCallExpression) projectedOver.getChildren().get(0))
                                .getFunctionDefinition())
                .isEqualTo(BuiltInFunctionDefinitions.SUM);
    }
}
