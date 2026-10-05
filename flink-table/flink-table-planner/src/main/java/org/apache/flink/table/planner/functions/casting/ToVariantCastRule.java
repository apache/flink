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

import org.apache.flink.table.data.utils.ToVariantConverter;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeCasts;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.variant.Variant;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.box;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;

/**
 * Cast rule to {@link LogicalTypeRoot#VARIANT} from every type that {@link LogicalTypeCasts}
 * allows.
 *
 * <p>The generated code calls a {@link ToVariantConverter} created once for the input type, which
 * describes how a value is stored. See {@link #canFail} for when a cast can fail.
 */
class ToVariantCastRule extends AbstractNullAwareCodeGeneratorCastRule<Object, Variant> {

    static final ToVariantCastRule INSTANCE = new ToVariantCastRule();

    /** A character takes up to 4 bytes in UTF-8, which a declared length counts as one. */
    private static final int MAX_UTF8_BYTES_PER_CHAR = 4;

    private ToVariantCastRule() {
        super(CastRulePredicate.builder().predicate(ToVariantCastRule::matches).build());
    }

    private static boolean matches(LogicalType input, LogicalType target) {
        // No cast rule takes the NULL type, although LogicalTypeCasts allows it to cast to any
        // type.
        return target.is(LogicalTypeRoot.VARIANT)
                && !input.is(LogicalTypeRoot.NULL)
                && LogicalTypeCasts.supportsExplicitCast(input, target);
    }

    /**
     * Returns whether a value of the input type can fail the cast, so that {@code TRY_CAST} returns
     * {@code NULL} for it instead of failing. Three kinds of types can fail:
     *
     * <ul>
     *   <li>A {@code TIMESTAMP(p)} or {@code TIMESTAMP_LTZ(p)} with {@code p > 6} is stored with
     *       nanoseconds, which only cover 1677-09-21 to 2262-04-11.
     *   <li>A {@code TIME} holds the milliseconds of one day. Only a source or a function can
     *       produce a value outside that range, but such a value fails.
     *   <li>A string or binary value over {@link ToVariantConverter#MAX_PAYLOAD_BYTES} does not fit
     *       into a {@code VARIANT}. Only a type whose declared length allows such a value can fail.
     * </ul>
     *
     * <p>Every other type never fails.
     *
     * <p>The length check trusts the declared length. Flink does not enforce that length on values
     * from a source, so a longer value can still reach the cast. {@code TRY_CAST} then fails for it
     * instead of returning {@code NULL}.
     */
    @Override
    public boolean canFail(LogicalType inputLogicalType, LogicalType targetLogicalType) {
        LogicalType type = inputLogicalType;
        while (type.is(LogicalTypeRoot.DISTINCT_TYPE)) {
            type = ((DistinctType) type).getSourceType();
        }
        switch (type.getTypeRoot()) {
            case TIME_WITHOUT_TIME_ZONE:
                return true;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return LogicalTypeChecks.getPrecision(type) > ToVariantConverter.TIMESTAMP_PRECISION;
            case CHAR:
            case VARCHAR:
                return (long) MAX_UTF8_BYTES_PER_CHAR * LogicalTypeChecks.getLength(type)
                        > ToVariantConverter.MAX_PAYLOAD_BYTES;
            case BINARY:
            case VARBINARY:
                return LogicalTypeChecks.getLength(type) > ToVariantConverter.MAX_PAYLOAD_BYTES;
            default:
                return false;
        }
    }

    /* Example generated code for INT, inside the null check of the base class. The converter is a
    field, created once when the code is generated:

    result$1 = toVariantConverter$2.convert(java.lang.Integer.valueOf(int$0));

    */
    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final String converterTerm =
                context.declareReusableObject(
                        ToVariantConverter.create(inputLogicalType), "toVariantConverter");
        return new CastRuleUtils.CodeWriter()
                .assignStmt(
                        returnVariable,
                        methodCall(converterTerm, "convert", box(inputTerm, inputLogicalType)))
                .toString();
    }
}
