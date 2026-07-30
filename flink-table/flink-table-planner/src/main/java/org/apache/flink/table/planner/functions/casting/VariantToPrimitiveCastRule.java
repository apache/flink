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

import org.apache.flink.table.planner.functions.casting.CastRuleUtils.CodeWriter;
import org.apache.flink.table.runtime.functions.VariantCastUtils;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.variant.Variant;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.primitiveTypeTermForType;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.cast;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.strLiteral;

/**
 * {@link LogicalTypeRoot#VARIANT} to primitive type cast rule.
 *
 * <p>A cast succeeds only when the target holds the stored value without altering it, otherwise it
 * fails {@code CAST} and yields {@code null} for {@code TRY_CAST}. An integer widens or narrows as
 * long as it stays in range, a {@code DECIMAL} has to fit the precision and scale without rounding,
 * and a timestamp has to fit the target precision. {@code FLOAT} and {@code DOUBLE} are approximate
 * by definition, so they take any numeric kind and reject only a magnitude out of range. Changing
 * the kind itself is not implicit, so a decimal is not read as an integer and a {@code TIMESTAMP}
 * is not read as a {@code TIMESTAMP_LTZ}.
 *
 * <p>{@code CHARACTER_STRING} is handled by {@link VariantToStringCastRule}; {@code TIME} has no
 * variant counterpart and is unsupported.
 */
class VariantToPrimitiveCastRule extends AbstractNullAwareCodeGeneratorCastRule<Variant, Object> {

    static final VariantToPrimitiveCastRule INSTANCE = new VariantToPrimitiveCastRule();

    private VariantToPrimitiveCastRule() {
        super(
                CastRulePredicate.builder()
                        .predicate(
                                (input, target) ->
                                        input.is(LogicalTypeRoot.VARIANT)
                                                && isSupportedTarget(target))
                        .build());
    }

    private static boolean isSupportedTarget(LogicalType targetType) {
        switch (targetType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case BINARY:
            case VARBINARY:
            case DATE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return true;
            default:
                return false;
        }
    }

    @Override
    public boolean canFail(LogicalType inputLogicalType, LogicalType targetLogicalType) {
        return true;
    }

    /**
     * Treats a variant that stores a JSON {@code null} as a {@code NULL} input, so it casts to SQL
     * {@code NULL} instead of failing in the type-specific accessor. Only applied for a nullable
     * target: a {@code NOT NULL} result cannot carry {@code NULL}, so a null-valued variant then
     * fails as a regular type mismatch.
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
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final CodeWriter writer = new CastRuleUtils.CodeWriter();
        switch (targetLogicalType.getTypeRoot()) {
            case BOOLEAN:
                writer.assignStmt(returnVariable, methodCall(inputTerm, "getBoolean"));
                break;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                writer.assignStmt(
                        returnVariable,
                        cast(
                                primitiveTypeTermForType(targetLogicalType),
                                staticCall(
                                        VariantCastUtils.class,
                                        "toIntegral",
                                        inputTerm,
                                        // A long literal needs the suffix to compile, since
                                        // Long.MIN_VALUE does not fit an int literal.
                                        integralMin(targetLogicalType) + "L",
                                        integralMax(targetLogicalType) + "L",
                                        strLiteral(targetLogicalType.getTypeRoot().name()))));
                break;
            case FLOAT:
                writer.assignStmt(
                        returnVariable, staticCall(VariantCastUtils.class, "toFloat", inputTerm));
                break;
            case DOUBLE:
                writer.assignStmt(
                        returnVariable, staticCall(VariantCastUtils.class, "toDouble", inputTerm));
                break;
            case DECIMAL:
                final DecimalType decimalType = (DecimalType) targetLogicalType;
                writer.assignStmt(
                        returnVariable,
                        staticCall(
                                VariantCastUtils.class,
                                "toDecimal",
                                inputTerm,
                                decimalType.getPrecision(),
                                decimalType.getScale()));
                break;
            case BINARY:
            case VARBINARY:
                writer.assignStmt(
                        returnVariable,
                        staticCall(
                                VariantCastUtils.class,
                                "toBytes",
                                inputTerm,
                                LogicalTypeChecks.getLength(targetLogicalType),
                                targetLogicalType.is(LogicalTypeRoot.BINARY)));
                break;
            case DATE:
                writer.assignStmt(
                        returnVariable,
                        cast("int", methodCall(methodCall(inputTerm, "getDate"), "toEpochDay")));
                break;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                writer.assignStmt(
                        returnVariable,
                        staticCall(
                                VariantCastUtils.class,
                                "toTimestamp",
                                inputTerm,
                                LogicalTypeChecks.getPrecision(targetLogicalType)));
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                writer.assignStmt(
                        returnVariable,
                        staticCall(
                                VariantCastUtils.class,
                                "toTimestampLtz",
                                inputTerm,
                                LogicalTypeChecks.getPrecision(targetLogicalType)));
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported target type for casting from VARIANT: " + targetLogicalType);
        }
        return writer.toString();
    }

    private static long integralMin(LogicalType target) {
        switch (target.getTypeRoot()) {
            case TINYINT:
                return Byte.MIN_VALUE;
            case SMALLINT:
                return Short.MIN_VALUE;
            case INTEGER:
                return Integer.MIN_VALUE;
            default:
                return Long.MIN_VALUE;
        }
    }

    private static long integralMax(LogicalType target) {
        switch (target.getTypeRoot()) {
            case TINYINT:
                return Byte.MAX_VALUE;
            case SMALLINT:
                return Short.MAX_VALUE;
            case INTEGER:
                return Integer.MAX_VALUE;
            default:
                return Long.MAX_VALUE;
        }
    }
}
