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

import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.util.Arrays;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.arrayLength;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.ternaryOperator;

/** {@link LogicalTypeFamily#BINARY_STRING} to {@link LogicalTypeFamily#BINARY_STRING} cast rule. */
class BinaryToBinaryCastRule extends AbstractExpressionCodeGeneratorCastRule<byte[], byte[]> {

    static final BinaryToBinaryCastRule INSTANCE = new BinaryToBinaryCastRule();

    private BinaryToBinaryCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.BINARY_STRING)
                        .target(LogicalTypeFamily.BINARY_STRING)
                        .build());
    }

    /**
     * Generates code for casting between BINARY and VARBINARY types.
     *
     * <p>For VARBINARY targets: preserves original length if it fits within the target constraint,
     * otherwise truncates to target length.
     *
     * <p>For BINARY targets: pads shorter inputs to exact target length, truncates longer inputs.
     *
     * <p>Example generated code for {@code CAST(input AS VARBINARY(4))}:
     *
     * <p>New behavior:
     *
     * <pre>
     * ((input.length <= 4) ? ((byte[])(inputValue)): java.util.Arrays.copyOf(((byte[])(inputValue)), 4))
     * </pre>
     *
     * <p>Legacy behavior:
     *
     * <pre>
     * ((byte[])(inputValue))
     * </pre>
     */
    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final int targetLength = LogicalTypeChecks.getLength(targetLogicalType);

        // Legacy behavior: always return input unchanged
        if (context.legacyBehaviour()
                || noTransformationNeeded(inputLogicalType, targetLogicalType)) {
            return inputTerm;
        }

        // Generate runtime transformation code
        final String operand = couldPad(targetLogicalType, targetLength) ? " == " : " <= ";
        return ternaryOperator(
                arrayLength(inputTerm) + operand + targetLength,
                inputTerm,
                staticCall(Arrays.class, "copyOf", inputTerm, targetLength));
    }

    /**
     * Determines if no runtime transformation is needed for the cast.
     *
     * <p>No transformation is needed when:
     *
     * <ul>
     *   <li>Target has no length constraint (unlimited length)
     *   <li>Target is VARBINARY and input's declared length fits within target constraint
     * </ul>
     *
     * <p>Transformation is always needed for BINARY targets (for padding/truncation).
     */
    private static boolean noTransformationNeeded(
            LogicalType inputLogicalType, LogicalType targetLogicalType) {
        final int inputLength = LogicalTypeChecks.getLength(inputLogicalType);
        final int targetLength = LogicalTypeChecks.getLength(targetLogicalType);

        // Target has no length constraint - always use input as-is
        // or
        // BINARY targets always need transformation for exact length semantics
        if (!couldTrim(targetLength) || couldPad(targetLogicalType, targetLength)) {
            return false;
        }

        // VARBINARY targets: no transformation if input fits within constraint
        // (assumes input respects its declared length)
        return inputLength <= targetLength;
    }

    /** Determines if the target has a length constraint that could lead to trimming. */
    static boolean couldTrim(int targetLength) {
        return targetLength < BinaryType.MAX_LENGTH;
    }

    /** Determines if the target is a BINARY with length constraint. */
    static boolean couldPad(LogicalType targetType, int targetLength) {
        return targetType.is(LogicalTypeRoot.BINARY) && targetLength < BinaryType.MAX_LENGTH;
    }

    static void trimOrPadByteArray(
            String returnVariable,
            int targetLength,
            String deserializedByteArrayTerm,
            CastRuleUtils.CodeWriter writer) {
        writer.assignStmt(
                returnVariable,
                staticCall(Arrays.class, "copyOf", deserializedByteArrayTerm, targetLength));
    }
}
