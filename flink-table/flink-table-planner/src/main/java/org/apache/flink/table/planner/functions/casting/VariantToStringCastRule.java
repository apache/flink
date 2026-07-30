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

import org.apache.flink.table.runtime.functions.VariantCastUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.variant.Variant;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

/**
 * {@link LogicalTypeRoot#VARIANT} to {@link LogicalTypeFamily#CHARACTER_STRING} cast rule.
 *
 * <p>Renders the scalar value the way a regular SQL cast of the stored kind would, so a boolean
 * becomes {@code TRUE}, a timestamp uses the SQL format, and a {@code TIMESTAMP_LTZ} is shifted
 * into the session time zone and a binary value is read as UTF-8. A variant holding an object or
 * array has no scalar rendering and fails; use {@code JSON_STRING} for its JSON representation.
 *
 * <p>A binary value that is not well-formed UTF-8 fails instead of decoding to {@code U+FFFD}. Cast
 * it to {@code BYTES} to inspect the raw value, or wrap that in {@code MAKE_VALID_UTF8} to accept
 * the lenient decode explicitly.
 *
 * <p>The target {@code CHAR}/{@code VARCHAR} length is enforced strictly: a value that does not fit
 * fails {@code CAST} and yields {@code null} for {@code TRY_CAST}, with no padding or truncation.
 */
class VariantToStringCastRule extends AbstractCharacterFamilyTargetRule<Variant> {

    static final VariantToStringCastRule INSTANCE = new VariantToStringCastRule();

    private VariantToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.VARIANT)
                        .target(LogicalTypeRoot.CHAR)
                        .target(LogicalTypeRoot.VARCHAR)
                        .build());
    }

    @Override
    public boolean canFail(LogicalType inputLogicalType, LogicalType targetLogicalType) {
        return true;
    }

    /**
     * Treats a variant that stores a JSON {@code null} as a {@code NULL} input, so it casts to SQL
     * {@code NULL} instead of the text {@code null}. Only applied for a nullable target: a {@code
     * NOT NULL} result cannot carry {@code NULL}, so a null-valued variant then fails.
     */
    @Override
    public CastCodeBlock generateCodeBlock(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String inputIsNullTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        if (!targetLogicalType.isNullable()) {
            return super.generateCodeBlock(
                    context, inputTerm, inputIsNullTerm, inputLogicalType, targetLogicalType);
        }
        final String isNullTerm =
                "(" + inputIsNullTerm + " || " + methodCall(inputTerm, "isNull") + ")";
        return super.generateCodeBlock(
                context, inputTerm, isNullTerm, inputLogicalType, targetLogicalType);
    }

    @Override
    public String generateStringExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        return staticCall(
                VariantCastUtils.class,
                "toStringValue",
                inputTerm,
                context.getSessionTimeZoneTerm(),
                LogicalTypeChecks.getLength(targetLogicalType),
                targetLogicalType.is(LogicalTypeRoot.CHAR));
    }
}
