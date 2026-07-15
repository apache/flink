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
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.functions.casting.CastRuleUtils.CodeWriter;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.variant.Variant;

import java.math.BigDecimal;
import java.util.Arrays;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.className;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.arrayLength;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.cast;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.ternaryOperator;

/**
 * {@link LogicalTypeRoot#VARIANT} to primitive type cast rule.
 *
 * <p>Numeric targets are lenient and follow regular numeric cast semantics; other targets require
 * the stored value to match the target kind. On a mismatch {@code CAST} fails and {@code TRY_CAST}
 * returns {@code null}.
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
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                writer.assignStmt(returnVariable, numericExpression(inputTerm, targetLogicalType));
                break;
            case BINARY:
            case VARBINARY:
                generateToBytes(context, inputTerm, returnVariable, targetLogicalType, writer);
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
                                TimestampData.class,
                                "fromLocalDateTime",
                                methodCall(inputTerm, "getDateTime")));
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                writer.assignStmt(
                        returnVariable,
                        staticCall(
                                TimestampData.class,
                                "fromInstant",
                                methodCall(inputTerm, "getInstant")));
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported target type for casting from VARIANT: " + targetLogicalType);
        }
        return writer.toString();
    }

    /**
     * Converts a numeric variant to the numeric {@code target} via the matching {@link Number}
     * accessor, mirroring regular numeric cast semantics. A non-numeric variant raises {@link
     * ClassCastException}, failing {@code CAST} and yielding {@code null} for {@code TRY_CAST}.
     */
    private static String numericExpression(String inputTerm, LogicalType target) {
        final String number = cast(className(Number.class), methodCall(inputTerm, "get"));
        if (!target.is(LogicalTypeRoot.DECIMAL)) {
            return methodCall(number, numberAccessor(target));
        }
        final DecimalType decimalType = (DecimalType) target;
        return staticCall(
                DecimalData.class,
                "fromBigDecimal",
                constructorCall(BigDecimal.class, methodCall(number, "toString")),
                decimalType.getPrecision(),
                decimalType.getScale());
    }

    private static String numberAccessor(LogicalType target) {
        switch (target.getTypeRoot()) {
            case TINYINT:
                return "byteValue";
            case SMALLINT:
                return "shortValue";
            case INTEGER:
                return "intValue";
            case BIGINT:
                return "longValue";
            case FLOAT:
                return "floatValue";
            case DOUBLE:
                return "doubleValue";
            default:
                throw new IllegalArgumentException(
                        "Unsupported numeric target for casting from VARIANT: " + target);
        }
    }

    private static void generateToBytes(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType targetLogicalType,
            CodeWriter writer) {
        final int targetLength = LogicalTypeChecks.getLength(targetLogicalType);
        // Read the bytes once to avoid decoding the variant twice.
        final String bytesTerm = newName(context.getCodeGeneratorContext(), "variantBytes");
        writer.declStmt("byte[]", bytesTerm, methodCall(inputTerm, "getBytes"));
        if (BinaryToBinaryCastRule.couldPad(targetLogicalType, targetLength)) {
            // BINARY(n): pad or trim to the exact target length.
            writer.assignStmt(
                    returnVariable,
                    ternaryOperator(
                            arrayLength(bytesTerm) + " == " + targetLength,
                            bytesTerm,
                            staticCall(Arrays.class, "copyOf", bytesTerm, targetLength)));
        } else if (BinaryToBinaryCastRule.couldTrim(targetLength)) {
            // VARBINARY(n): trim only when longer than the target length.
            writer.assignStmt(
                    returnVariable,
                    ternaryOperator(
                            arrayLength(bytesTerm) + " <= " + targetLength,
                            bytesTerm,
                            staticCall(Arrays.class, "copyOf", bytesTerm, targetLength)));
        } else {
            writer.assignStmt(returnVariable, bytesTerm);
        }
    }
}
