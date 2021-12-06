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

package org.apache.flink.table.planner.functions.casting;

import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import java.lang.reflect.Method;

import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.DECIMAL_TO_DOUBLE;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.DECIMAL_TO_INTEGRAL;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.castToPrimitive;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

/**
 * {@link LogicalTypeRoot#DECIMAL} to {@link LogicalTypeFamily#INTEGER_NUMERIC} and {@link
 * LogicalTypeFamily#APPROXIMATE_NUMERIC} cast rule.
 */
class DecimalToNumericPrimitiveCastRule
        extends AbstractExpressionCodeGeneratorCastRule<DecimalData, Number> {

    static final DecimalToNumericPrimitiveCastRule INSTANCE =
            new DecimalToNumericPrimitiveCastRule();

    private DecimalToNumericPrimitiveCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.DECIMAL)
                        .target(LogicalTypeFamily.INTEGER_NUMERIC)
                        .target(LogicalTypeFamily.APPROXIMATE_NUMERIC)
                        .build());
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        Method method =
                targetLogicalType.is(LogicalTypeFamily.INTEGER_NUMERIC)
                        ? DECIMAL_TO_INTEGRAL()
                        : DECIMAL_TO_DOUBLE();
        return castToPrimitive(targetLogicalType, staticCall(method, inputTerm));
    }
}
