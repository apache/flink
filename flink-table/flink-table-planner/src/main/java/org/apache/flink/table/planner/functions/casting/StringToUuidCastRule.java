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
import org.apache.flink.table.runtime.functions.UuidCastUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

/**
 * {@link LogicalTypeFamily#CHARACTER_STRING} to {@link LogicalTypeRoot#UUID} cast rule, the inverse
 * of {@link UuidToStringCastRule}.
 *
 * <p>Parses the string with {@link UuidCastUtils#toUuidBytes(String)} (lenient, PostgreSQL style).
 * A malformed value fails {@code CAST}; since the rule {@link #canFail}, the framework wraps the
 * call in a {@code try/catch} that yields {@code null} for {@code TRY_CAST}. Example generated
 * code:
 *
 * <pre>
 * result$0 = UuidCastUtils.toUuidBytes(str$0.toString());
 * </pre>
 */
class StringToUuidCastRule extends AbstractExpressionCodeGeneratorCastRule<StringData, byte[]> {

    static final StringToUuidCastRule INSTANCE = new StringToUuidCastRule();

    private StringToUuidCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.CHARACTER_STRING)
                        .target(LogicalTypeRoot.UUID)
                        .build());
    }

    @Override
    public boolean canFail(LogicalType inputLogicalType, LogicalType targetLogicalType) {
        return true;
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        return staticCall(UuidCastUtils.class, "toUuidBytes", methodCall(inputTerm, "toString"));
    }
}
