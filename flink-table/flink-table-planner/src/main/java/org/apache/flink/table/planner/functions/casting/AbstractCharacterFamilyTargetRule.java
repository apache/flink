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

import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.BINARY_STRING_DATA_FROM_STRING;

/**
 * Base class for cast rules converting to {@link LogicalTypeFamily#CHARACTER_STRING} with code
 * generation.
 */
abstract class AbstractCharacterFamilyTargetRule<IN>
        extends AbstractExpressionCodeGeneratorCastRule<IN, StringData> {

    protected AbstractCharacterFamilyTargetRule(CastRulePredicate predicate) {
        super(predicate);
    }

    public abstract String generateStringExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType);

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final String stringExpr =
                generateStringExpression(context, inputTerm, inputLogicalType, targetLogicalType);

        return CastRuleUtils.staticCall(BINARY_STRING_DATA_FROM_STRING(), stringExpr);
    }
}
