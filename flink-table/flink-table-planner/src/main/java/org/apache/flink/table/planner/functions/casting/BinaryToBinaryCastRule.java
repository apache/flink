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
     * <pre>
     * ((input.length <= 4) ? input : java.util.Arrays.copyOf(input, 4))
     * </pre>
     */
    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final int targetLength = LogicalTypeChecks.getLength(targetLogicalType);

        // Legacy behavior or no length constraints - return as-is
        if (context.legacyBehaviour() || !couldTrim(targetLength)) {
            return inputTerm;
        }

        final String operand = couldPad(targetLogicalType, targetLength) ? " == " : " <= ";
        return ternaryOperator(
                arrayLength(inputTerm) + operand + targetLength,
                inputTerm,
                staticCall(Arrays.class, "copyOf", inputTerm, targetLength));
    }

    static boolean couldTrim(int targetLength) {
        return targetLength < BinaryType.MAX_LENGTH;
    }

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
