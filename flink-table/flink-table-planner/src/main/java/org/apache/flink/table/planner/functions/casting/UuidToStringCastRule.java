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

import org.apache.flink.table.runtime.functions.UuidCastUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;
import static org.apache.flink.table.types.logical.VarCharType.STRING_TYPE;

/**
 * {@link LogicalTypeRoot#UUID} to {@link LogicalTypeFamily#CHARACTER_STRING} cast rule, the inverse
 * of {@link StringToUuidCastRule}.
 *
 * <p>Renders the value with {@link UuidCastUtils#toStringValue(byte[])}; the base rule wraps the
 * result in a {@code BinaryStringData}, and a bounded {@code CHAR(n)}/{@code VARCHAR(n)} target is
 * trimmed or padded by {@link CharVarCharTrimPadCastRule}. Example generated code:
 *
 * <pre>
 * result$0 = BinaryStringData.fromString(UuidCastUtils.toStringValue(uuid$0));
 * </pre>
 */
class UuidToStringCastRule extends AbstractCharacterFamilyTargetRule<byte[]> {

    static final UuidToStringCastRule INSTANCE = new UuidToStringCastRule();

    private UuidToStringCastRule() {
        super(CastRulePredicate.builder().input(LogicalTypeRoot.UUID).target(STRING_TYPE).build());
    }

    @Override
    public String generateStringExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        return staticCall(UuidCastUtils.class, "toStringValue", inputTerm);
    }
}
