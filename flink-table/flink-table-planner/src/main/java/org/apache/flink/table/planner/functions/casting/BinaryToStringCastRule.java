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

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import java.nio.charset.StandardCharsets;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.accessStaticField;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.types.logical.VarCharType.STRING_TYPE;

/**
 * {@link LogicalTypeFamily#BINARY_STRING} to {@link LogicalTypeFamily#CHARACTER_STRING} cast rule.
 */
class BinaryToStringCastRule extends AbstractCharacterFamilyTargetRule<byte[]> {

    static final BinaryToStringCastRule INSTANCE = new BinaryToStringCastRule();

    private BinaryToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.BINARY_STRING)
                        .target(STRING_TYPE)
                        .build());
    }

    @Override
    public String generateStringExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        return constructorCall(
                String.class, inputTerm, accessStaticField(StandardCharsets.class, "UTF_8"));
    }
}
