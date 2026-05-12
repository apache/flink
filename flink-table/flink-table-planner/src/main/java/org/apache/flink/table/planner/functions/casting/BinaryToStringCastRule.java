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

import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.utils.EncodingUtils;

import java.nio.charset.StandardCharsets;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.BINARY_STRING_DATA_FROM_STRING;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.BINARY_STRING_DATA_FROM_UTF8_BYTES;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.accessStaticField;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;
import static org.apache.flink.table.planner.functions.casting.CharVarCharTrimPadCastRule.couldPad;
import static org.apache.flink.table.planner.functions.casting.CharVarCharTrimPadCastRule.couldTrim;

/**
 * {@link LogicalTypeFamily#BINARY_STRING} to {@link LogicalTypeFamily#CHARACTER_STRING} cast rule.
 *
 * <p>Strict UTF-8 mode is the default: invalid input bytes throw a {@code TableRuntimeException}.
 * Setting {@link ExecutionConfigOptions#TABLE_EXEC_LEGACY_BYTES_TO_STRING_CAST} to {@code true}
 * restores the prior behavior, where invalid sequences are silently replaced by {@code U+FFFD}.
 */
class BinaryToStringCastRule extends AbstractNullAwareCodeGeneratorCastRule<byte[], String> {

    static final BinaryToStringCastRule INSTANCE = new BinaryToStringCastRule();

    private BinaryToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.BINARY_STRING)
                        .target(LogicalTypeFamily.CHARACTER_STRING)
                        .build());
    }

    /* Example generated code

    --- Strict UTF-8 mode fast path: STRING / VARCHAR(MAX) target. No String allocation, no re-encoding.
    isNull$0 = _myInputIsNull;
    if (!isNull$0) {
        result$1 = org.apache.flink.table.data.binary.BinaryStringData.fromUtf8Bytes(_myInput);
        isNull$0 = result$1 == null;
    } else {
        result$1 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
    }

    --- Round-trip path: legacy mode (silent U+FFFD substitution) or strict UTF-8 mode + CHAR(n)/VARCHAR(n) (trim/pad).
    --- The decode line below is the legacy variant; in strict UTF-8 mode it becomes:
    ---     resultString$0 = org.apache.flink.table.data.binary.BinaryStringData.fromUtf8Bytes(_myInput).toString();
    isNull$0 = _myInputIsNull;
    if (!isNull$0) {
        java.lang.String resultString$0;
        resultString$0 = new java.lang.String(_myInput, java.nio.charset.StandardCharsets.UTF_8);
        java.lang.String resultPadOrTrim$538;
        resultPadOrTrim$538 = resultString$0.toString();
        if (resultString$0.length() > 12) {
            resultPadOrTrim$538 = resultString$0.substring(0, java.lang.Math.min(resultString$0.length(), 12));
        } else {
            if (resultPadOrTrim$538.length() < 12) {
                int padLength$539;
                padLength$539 = 12 - resultPadOrTrim$538.length();
                java.lang.StringBuilder sbPadding$540;
                sbPadding$540 = new java.lang.StringBuilder();
                for (int i$541 = 0; i$541 < padLength$539; i$541++) {
                    sbPadding$540.append(" ");
                }
                resultPadOrTrim$538 = resultPadOrTrim$538 + sbPadding$540.toString();
            }
        }
        resultString$0 = resultPadOrTrim$538;
        result$1 = org.apache.flink.table.data.binary.BinaryStringData.fromString(resultString$0);
        isNull$0 = result$1 == null;
    } else {
        result$1 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
    }

     */

    @Override
    public boolean canFail(LogicalType inputLogicalType, LogicalType targetLogicalType) {
        // Strict UTF-8 mode validates the input and can throw on malformed bytes.
        return true;
    }

    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final boolean legacy =
                context.getCodeGeneratorContext()
                        .tableConfig()
                        .get(ExecutionConfigOptions.TABLE_EXEC_LEGACY_BYTES_TO_STRING_CAST);
        final int length = LogicalTypeChecks.getLength(targetLogicalType);
        final boolean needsTrimOrPad = couldTrim(length) || couldPad(targetLogicalType, length);

        // Strict UTF-8 mode fast path: unbounded target. Wrap the input bytes directly with no
        // intermediate String. Legacy mode always needs the round-trip below because the JDK
        // decoder is what substitutes U+FFFD for invalid sequences.
        if (!context.isPrinting() && !legacy && !needsTrimOrPad) {
            return new CastRuleUtils.CodeWriter()
                    .assignStmt(
                            returnVariable,
                            staticCall(BINARY_STRING_DATA_FROM_UTF8_BYTES(), inputTerm))
                    .toString();
        }

        final String resultStringTerm = newName(context.getCodeGeneratorContext(), "resultString");
        final CastRuleUtils.CodeWriter writer = new CastRuleUtils.CodeWriter();

        writer.declStmt(String.class, resultStringTerm);
        if (context.isPrinting()) {
            writer.assignStmt(resultStringTerm, "\"x'\"")
                    .assignPlusStmt(
                            resultStringTerm, staticCall(EncodingUtils.class, "hex", inputTerm))
                    .assignPlusStmt(resultStringTerm, "\"'\"");
        } else if (legacy) {
            // Legacy mode: lenient JDK decode, invalid sequences become U+FFFD.
            writer.assignStmt(
                    resultStringTerm,
                    constructorCall(
                            String.class,
                            inputTerm,
                            accessStaticField(StandardCharsets.class, "UTF_8")));
        } else {
            // Strict UTF-8 mode: validates, then materializes the String for trim/pad below.
            writer.assignStmt(
                    resultStringTerm,
                    staticCall(BINARY_STRING_DATA_FROM_UTF8_BYTES(), inputTerm) + ".toString()");
        }

        if (!context.legacyBehaviour() && !context.isPrinting()) {
            final String resultPadOrTrim =
                    newName(context.getCodeGeneratorContext(), "resultPadOrTrim");
            CharVarCharTrimPadCastRule.padAndTrimStringIfNeeded(
                    writer,
                    targetLogicalType,
                    context.legacyBehaviour(),
                    length,
                    resultPadOrTrim,
                    resultStringTerm,
                    context.getCodeGeneratorContext());
            writer.assignStmt(resultStringTerm, resultPadOrTrim);
        }
        return writer
                // Assign the result value
                .assignStmt(
                        returnVariable,
                        CastRuleUtils.staticCall(
                                BINARY_STRING_DATA_FROM_STRING(), resultStringTerm))
                .toString();
    }
}
