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

import org.apache.flink.table.data.utils.FromVariantConverter;
import org.apache.flink.table.data.utils.VariantCastUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;
import org.apache.flink.types.variant.Variant;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.boxedTypeTermForType;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.BINARY_STRING_DATA_FROM_STRING;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.cast;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.unbox;

/**
 * Cast rule from {@link LogicalTypeRoot#VARIANT} to every type that {@link LogicalTypeCasts}
 * allows. The generated code calls a {@link FromVariantConverter}, which also trims and pads a
 * {@code CHAR(n)} or {@code VARCHAR(n)} target. Printing calls {@link
 * VariantCastUtils#toPrintString} instead, because a printed value must never fail.
 */
class FromVariantCastRule extends AbstractNullAwareCodeGeneratorCastRule<Variant, Object> {

    static final FromVariantCastRule INSTANCE = new FromVariantCastRule();

    private FromVariantCastRule() {
        super(CastRulePredicate.builder().predicate(FromVariantCastRule::matches).build());
    }

    private static boolean matches(final LogicalType input, final LogicalType target) {
        return input.is(LogicalTypeRoot.VARIANT)
                && !target.is(LogicalTypeRoot.VARIANT)
                && LogicalTypeCasts.supportsExplicitCast(input, target);
    }

    private static boolean isPrinting(
            final CodeGeneratorCastRule.Context context, final LogicalType target) {
        return context.isPrinting() && target.is(LogicalTypeFamily.CHARACTER_STRING);
    }

    @Override
    public boolean canFail(
            final LogicalType inputLogicalType, final LogicalType targetLogicalType) {
        return true;
    }

    /** Maps a variant null to SQL {@code NULL} for a nullable target, except when printing. */
    @Override
    public CastCodeBlock generateCodeBlock(
            final CodeGeneratorCastRule.Context context,
            final String inputTerm,
            final String inputIsNullTerm,
            final LogicalType inputLogicalType,
            final LogicalType targetLogicalType) {
        if (!targetLogicalType.isNullable() || isPrinting(context, targetLogicalType)) {
            return super.generateCodeBlock(
                    context, inputTerm, inputIsNullTerm, inputLogicalType, targetLogicalType);
        }
        final String isNullTerm =
                "(" + inputIsNullTerm + " || " + methodCall(inputTerm, "isNull") + ")";
        return super.generateCodeBlock(
                context, inputTerm, isNullTerm, inputLogicalType, targetLogicalType);
    }

    /* Example generated code for INT, inside the null check of the base class:

    result$1 = ((java.lang.Integer)(fromVariantConverter$2.convert(variant$0))).intValue();

    */
    @Override
    protected String generateCodeBlockInternal(
            final CodeGeneratorCastRule.Context context,
            final String inputTerm,
            final String returnVariable,
            final LogicalType inputLogicalType,
            final LogicalType targetLogicalType) {
        if (isPrinting(context, targetLogicalType)) {
            // toPrintString returns a String, which the result has to wrap.
            return new CastRuleUtils.CodeWriter()
                    .assignStmt(
                            returnVariable,
                            staticCall(
                                    BINARY_STRING_DATA_FROM_STRING(),
                                    staticCall(
                                            VariantCastUtils.class,
                                            "toPrintString",
                                            inputTerm,
                                            context.getSessionTimeZoneTerm())))
                    .toString();
        }
        final String converterTerm =
                context.declareReusableObject(
                        FromVariantConverter.create(targetLogicalType, context.getSessionZoneId()),
                        "fromVariantConverter");
        return new CastRuleUtils.CodeWriter()
                .assignStmt(
                        returnVariable,
                        unbox(
                                cast(
                                        boxedTypeTermForType(targetLogicalType),
                                        methodCall(converterTerm, "convert", inputTerm)),
                                targetLogicalType))
                .toString();
    }
}
