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

import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.utils.EncodingUtils;

import java.nio.charset.StandardCharsets;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.BINARY_STRING_DATA_FROM_STRING;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.accessStaticField;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

/**
 * {@link LogicalTypeFamily#BINARY_STRING} to {@link LogicalTypeFamily#CHARACTER_STRING} cast rule.
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

    isNull$0 = _myInputIsNull;
    if (!isNull$0) {
        java.lang.String hexString$0;
        hexString$0 = org.apache.flink.table.utils.EncodingUtils.hex(_myInput);
        java.lang.String resultString$152;
        resultString$152 = hexString$0.toString();
        if (hexString$0.length() > 3) {
            resultString$152 = hexString$0.substring(0, java.lang.Math.min(hexString$0.length(), 3));
        } else {
            if (resultString$1.length() < 12) {
                int padLength$3;
                padLength$3 = 12 - resultString$152.length();
                java.lang.StringBuilder sbPadding$4;
                sbPadding$4 = new java.lang.StringBuilder();
                for (int i$5 = 0; i$5 < padLength$3; i$5++) {
                    sbPadding$4.append(" ");
                }
                resultString$152 = resultString$152 + sbPadding$4.toString();
            }
        }
        result$1 = org.apache.flink.table.data.binary.BinaryStringData.fromString(resultString$152);
        isNull$0 = result$1 == null;
    } else {
        result$1 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
    }

     */

    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final String resultStringTerm = newName("resultString");
        CastRuleUtils.CodeWriter writer = new CastRuleUtils.CodeWriter();
        if (context.legacyBehaviour()) {
            writer.declStmt(String.class, resultStringTerm)
                    .assignStmt(
                            resultStringTerm,
                            constructorCall(
                                    String.class,
                                    inputTerm,
                                    accessStaticField(StandardCharsets.class, "UTF_8")));
        } else {
            final int length = LogicalTypeChecks.getLength(targetLogicalType);

            final String hexStringTerm = newName("hexString");
            writer.declStmt(String.class, hexStringTerm)
                    .assignStmt(hexStringTerm, staticCall(EncodingUtils.class, "hex", inputTerm));
            writer =
                    CharVarCharTrimPadCastRule.padAndTrimStringIfNeeded(
                            writer,
                            targetLogicalType,
                            context.legacyBehaviour(),
                            length,
                            resultStringTerm,
                            hexStringTerm);
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
