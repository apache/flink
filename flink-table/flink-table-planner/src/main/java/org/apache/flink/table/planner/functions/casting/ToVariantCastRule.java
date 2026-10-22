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
import org.apache.flink.types.variant.BinaryVariantUtil;
import org.apache.flink.types.variant.Variant;

import java.util.ArrayDeque;
import java.util.Deque;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.box;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;

/**
 * Cast rule to {@link LogicalTypeRoot#VARIANT} from every type that {@link LogicalTypeCasts}
 * allows: a primitive type, or an {@link LogicalTypeRoot#ARRAY}, {@link LogicalTypeRoot#MAP},
 * {@link LogicalTypeRoot#ROW} or {@link LogicalTypeRoot#STRUCTURED_TYPE} whose leaves all cast to
 * {@code VARIANT} and whose {@code MAP} keys are character strings.
 *
 * <p>The generated code calls a {@link ToVariantConverter} created once for the input type, which
 * describes how a value is stored. A failure anywhere in the value fails the whole cast, so {@code
 * TRY_CAST} returns {@code NULL} rather than a partial result. See {@link #canFail} for when a cast
 * can fail.
 */
class ToVariantCastRule extends AbstractNullAwareCodeGeneratorCastRule<Object, Variant> {

    static final ToVariantCastRule INSTANCE = new ToVariantCastRule();

    /** A character takes up to 4 bytes in UTF-8, which a declared length counts as one. */
    private static final int MAX_UTF8_BYTES_PER_CHAR = 4;

    /** The largest fixed-size value: a 16-byte decimal with its header and scale. */
    private static final long MAX_FIXED_SIZE = 18;

    /** A header byte and a 4-byte size or length, the largest header a value has. */
    private static final long MAX_HEADER_SIZE = 1 + BinaryVariantUtil.U32_SIZE;

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
     * {@code NULL} for it instead of failing. A value can fail in four ways:
     *
     * <ul>
     *   <li>A {@code TIMESTAMP(p)} or {@code TIMESTAMP_LTZ(p)} with {@code p > 6} is stored with
     *       nanoseconds, which only cover 1677-09-21 to 2262-04-11.
     *   <li>A {@code TIME} holds the milliseconds of one day. Only a source or a function can
     *       produce a value outside that range, but such a value fails.
     *   <li>The value does not fit into the 16 MiB of a {@code VARIANT}. Any {@code ARRAY}, {@code
     *       MAP} or nested {@code VARIANT} can reach that size. A string or binary type can when
     *       its declared length allows more, and a {@code ROW} when the declared sizes of its
     *       fields add up to more.
     *   <li>A {@code MAP} with a {@code NULL} key fails. Every {@code MAP} can already fail on its
     *       size, so this makes no other type fallible.
     * </ul>
     *
     * <p>Every other type never fails.
     *
     * <p>The size check trusts the declared length. Flink does not enforce that length on values
     * from a source, so a longer value can still reach the cast. {@code TRY_CAST} then fails for it
     * instead of returning {@code NULL}.
     */
    @Override
    public boolean canFail(LogicalType inputLogicalType, LogicalType targetLogicalType) {
        // Without an ARRAY, MAP or VARIANT, the size of a value is bounded by the sum of its
        // leaves and object headers.
        long maxSize = 0;
        final Deque<LogicalType> pending = new ArrayDeque<>();
        pending.push(inputLogicalType);
        while (!pending.isEmpty()) {
            final LogicalType type = pending.pop();
            if (type.is(LogicalTypeRoot.DISTINCT_TYPE)) {
                pending.push(((DistinctType) type).getSourceType());
                continue;
            }
            switch (type.getTypeRoot()) {
                case ARRAY:
                case MAP:
                case VARIANT:
                case TIME_WITHOUT_TIME_ZONE:
                    return true;
                case ROW:
                case STRUCTURED_TYPE:
                    LogicalTypeChecks.getFieldTypes(type).forEach(pending::push);
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    if (LogicalTypeChecks.getPrecision(type)
                            > ToVariantConverter.TIMESTAMP_PRECISION) {
                        return true;
                    }
                    break;
                default:
                    break;
            }
            maxSize += maxOwnSize(type);
        }
        return maxSize > BinaryVariantUtil.SIZE_LIMIT;
    }

    private static long maxOwnSize(LogicalType type) {
        switch (type.getTypeRoot()) {
            case ROW:
            case STRUCTURED_TYPE:
                // Only the object header. Its fields are counted on their own. A 4-byte id per
                // field, and a 4-byte offset per field plus one for the end.
                return MAX_HEADER_SIZE
                        + BinaryVariantUtil.U32_SIZE
                                * (2L * LogicalTypeChecks.getFieldCount(type) + 1);
            case CHAR:
            case VARCHAR:
                return MAX_HEADER_SIZE
                        + (long) MAX_UTF8_BYTES_PER_CHAR * LogicalTypeChecks.getLength(type);
            case BINARY:
            case VARBINARY:
                return MAX_HEADER_SIZE + LogicalTypeChecks.getLength(type);
            default:
                return MAX_FIXED_SIZE;
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
