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

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.STRING_DATA_TO_BYTE;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.STRING_DATA_TO_DOUBLE;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.STRING_DATA_TO_FLOAT;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.STRING_DATA_TO_INT;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.STRING_DATA_TO_LONG;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.STRING_DATA_TO_SHORT;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

/**
 * {@link LogicalTypeFamily#CHARACTER_STRING} to {@link LogicalTypeFamily#INTEGER_NUMERIC} and
 * {@link LogicalTypeFamily#APPROXIMATE_NUMERIC} cast rule.
 */
class StringToNumericPrimitiveCastRule
        extends AbstractExpressionCodeGeneratorCastRule<StringData, Number> {

    static final StringToNumericPrimitiveCastRule INSTANCE = new StringToNumericPrimitiveCastRule();

    private StringToNumericPrimitiveCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.CHARACTER_STRING)
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
        final String trimmedInputTerm = methodCall(inputTerm, "trim");
        switch (targetLogicalType.getTypeRoot()) {
            case TINYINT:
                return staticCall(STRING_DATA_TO_BYTE(), trimmedInputTerm);
            case SMALLINT:
                return staticCall(STRING_DATA_TO_SHORT(), trimmedInputTerm);
            case INTEGER:
                return staticCall(STRING_DATA_TO_INT(), trimmedInputTerm);
            case BIGINT:
                return staticCall(STRING_DATA_TO_LONG(), trimmedInputTerm);
            case FLOAT:
                return staticCall(STRING_DATA_TO_FLOAT(), trimmedInputTerm);
            case DOUBLE:
                return staticCall(STRING_DATA_TO_DOUBLE(), trimmedInputTerm);
        }
        throw new IllegalArgumentException("This is a bug. Please file an issue.");
    }

    @Override
    public boolean canFail(LogicalType inputLogicalType, LogicalType targetLogicalType) {
        return true;
    }
}
