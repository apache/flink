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

package org.apache.flink.table.planner.functions.casting.rules;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.functions.casting.CastRulePredicate;
import org.apache.flink.table.planner.functions.casting.CodeGeneratorCastRule;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import java.nio.charset.StandardCharsets;

import static org.apache.flink.table.planner.functions.casting.rules.CastRuleUtils.accessStaticField;
import static org.apache.flink.table.planner.functions.casting.rules.CastRuleUtils.constructorCall;

/**
 * {@link LogicalTypeFamily#BINARY_STRING} to {@link LogicalTypeFamily#CHARACTER_STRING} cast rule.
 */
@Internal
public class BinaryToStringCastRule extends AbstractCharacterFamilyTargetRule<byte[]> {

    public static final BinaryToStringCastRule INSTANCE = new BinaryToStringCastRule();

    private BinaryToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.BINARY_STRING)
                        .target(LogicalTypeFamily.CHARACTER_STRING)
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
