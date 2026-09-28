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
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.variant.Variant;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

/**
 * Cast rule from a primitive type to {@link LogicalTypeRoot#VARIANT}.
 *
 * <p>The value keeps the kind of its SQL type. For example, a {@code BIGINT} is stored as a {@code
 * BIGINT}, even when it would fit a smaller kind. A {@code NaN} or infinite {@code FLOAT} or {@code
 * DOUBLE} is stored as is, although {@code PARSE_JSON} rejects it. Some types can fail for some
 * values, see {@link #canFail}.
 */
class PrimitiveToVariantCastRule extends AbstractExpressionCodeGeneratorCastRule<Object, Variant> {

    static final PrimitiveToVariantCastRule INSTANCE = new PrimitiveToVariantCastRule();

    /** A character takes up to 4 bytes in UTF-8, which a declared length counts as one. */
    private static final int MAX_UTF8_BYTES_PER_CHAR = 4;

    private PrimitiveToVariantCastRule() {
        super(
                CastRulePredicate.builder()
                        .predicate(
                                (input, target) ->
                                        target.is(LogicalTypeRoot.VARIANT)
                                                && isSupportedSource(input))
                        .build());
    }

    private static boolean isSupportedSource(LogicalType inputType) {
        switch (inputType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case CHAR:
            case VARCHAR:
            case BINARY:
            case VARBINARY:
            case DATE:
            case TIME_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
            case UUID:
                return true;
            default:
                return false;
        }
    }

    /**
     * Returns whether a value of the input type can fail the cast, so that {@code TRY_CAST} returns
     * {@code NULL} for it instead of failing. Two kinds of types can fail:
     *
     * <ul>
     *   <li>A {@code TIMESTAMP(p)} or {@code TIMESTAMP_LTZ(p)} with {@code p > 6} is stored with
     *       nanoseconds, which only cover 1677-09-21 to 2262-04-11.
     *   <li>A string or binary value over {@link VariantCastUtils#MAX_PAYLOAD_BYTES} does not fit
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
        switch (inputLogicalType.getTypeRoot()) {
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return LogicalTypeChecks.getPrecision(inputLogicalType)
                        > VariantCastUtils.TIMESTAMP_PRECISION;
            case CHAR:
            case VARCHAR:
                return (long) MAX_UTF8_BYTES_PER_CHAR
                                * LogicalTypeChecks.getLength(inputLogicalType)
                        > VariantCastUtils.MAX_PAYLOAD_BYTES;
            case BINARY:
            case VARBINARY:
                return LogicalTypeChecks.getLength(inputLogicalType)
                        > VariantCastUtils.MAX_PAYLOAD_BYTES;
            default:
                return false;
        }
    }

    /* Example generated code for INT:

    result$0 = org.apache.flink.table.runtime.functions.VariantCastUtils.fromIntegral(int$0);

    */
    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        if (isTimestamp(inputLogicalType)) {
            return staticCall(
                    VariantCastUtils.class,
                    helperName(inputLogicalType),
                    inputTerm,
                    LogicalTypeChecks.getPrecision(inputLogicalType));
        }
        return staticCall(VariantCastUtils.class, helperName(inputLogicalType), inputTerm);
    }

    private static boolean isTimestamp(LogicalType inputLogicalType) {
        return inputLogicalType.is(LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                || inputLogicalType.is(LogicalTypeRoot.TIMESTAMP_WITH_LOCAL_TIME_ZONE);
    }

    private static String helperName(LogicalType inputLogicalType) {
        switch (inputLogicalType.getTypeRoot()) {
            case BOOLEAN:
                return "fromBoolean";
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
                return "fromIntegral";
            case FLOAT:
                return "fromFloat";
            case DOUBLE:
                return "fromDouble";
            case DECIMAL:
                return "fromDecimal";
            case CHAR:
            case VARCHAR:
                return "fromString";
            case BINARY:
            case VARBINARY:
                return "fromBytes";
            case DATE:
                return "fromDate";
            case TIME_WITHOUT_TIME_ZONE:
                return "fromTime";
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return "fromTimestamp";
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return "fromTimestampLtz";
            case UUID:
                return "fromUuid";
            default:
                throw new IllegalArgumentException(
                        "Unsupported source type for casting to VARIANT: " + inputLogicalType);
        }
    }
}
