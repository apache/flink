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
import org.apache.flink.table.data.binary.BinaryStringDataUtil;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.BINARY_STRING_DATA_FROM_STRING;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.EMPTY_STR_LITERAL;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.accessStaticField;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.stringConcat;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.ternaryOperator;
import static org.apache.flink.table.types.logical.VarCharType.STRING_TYPE;

/** {@link LogicalTypeRoot#BOOLEAN} to {@link LogicalTypeFamily#CHARACTER_STRING} cast rule. */
class BooleanToStringCastRule extends AbstractExpressionCodeGeneratorCastRule<Boolean, StringData> {

    static final BooleanToStringCastRule INSTANCE = new BooleanToStringCastRule();

    private BooleanToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.BOOLEAN)
                        .target(STRING_TYPE)
                        .build());
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        if (context.legacyBehaviour()) {
            return CastRuleUtils.staticCall(
                    BINARY_STRING_DATA_FROM_STRING(), stringConcat(EMPTY_STR_LITERAL, inputTerm));
        }
        return ternaryOperator(
                inputTerm,
                accessStaticField(BinaryStringDataUtil.class, "TRUE_STRING"),
                accessStaticField(BinaryStringDataUtil.class, "FALSE_STRING"));
    }
}
