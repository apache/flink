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
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.DOUBLE_TO_DECIMAL;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.INTEGRAL_TO_DECIMAL;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.cast;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

/**
 * {@link LogicalTypeFamily#INTEGER_NUMERIC} and {@link LogicalTypeFamily#APPROXIMATE_NUMERIC} to
 * {@link LogicalTypeRoot#DECIMAL} cast rule.
 */
class NumericPrimitiveToDecimalCastRule
        extends AbstractExpressionCodeGeneratorCastRule<Number, DecimalData> {

    static final NumericPrimitiveToDecimalCastRule INSTANCE =
            new NumericPrimitiveToDecimalCastRule();

    private NumericPrimitiveToDecimalCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.INTEGER_NUMERIC)
                        .input(LogicalTypeFamily.APPROXIMATE_NUMERIC)
                        .target(LogicalTypeRoot.DECIMAL)
                        .build());
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final DecimalType targetDecimalType = (DecimalType) targetLogicalType;
        if (inputLogicalType.is(LogicalTypeFamily.INTEGER_NUMERIC)) {
            return staticCall(
                    INTEGRAL_TO_DECIMAL(),
                    cast("long", inputTerm),
                    targetDecimalType.getPrecision(),
                    targetDecimalType.getScale());
        }
        return staticCall(
                DOUBLE_TO_DECIMAL(),
                cast("double", inputTerm),
                targetDecimalType.getPrecision(),
                targetDecimalType.getScale());
    }
}
