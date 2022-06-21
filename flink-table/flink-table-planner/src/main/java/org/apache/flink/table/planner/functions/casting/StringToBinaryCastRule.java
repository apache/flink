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

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import static org.apache.flink.table.planner.functions.casting.BinaryToBinaryCastRule.couldPad;
import static org.apache.flink.table.planner.functions.casting.BinaryToBinaryCastRule.trimOrPadByteArray;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.arrayLength;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;

/**
 * {@link LogicalTypeFamily#CHARACTER_STRING} to {@link LogicalTypeFamily#BINARY_STRING} cast rule.
 */
class StringToBinaryCastRule extends AbstractNullAwareCodeGeneratorCastRule<StringData, byte[]> {

    static final StringToBinaryCastRule INSTANCE = new StringToBinaryCastRule();

    private StringToBinaryCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.CHARACTER_STRING)
                        .target(LogicalTypeFamily.BINARY_STRING)
                        .build());
    }

    /* Example generated code for VARBINARY(2):

    // legacy behavior
    isNull$0 = _myInputIsNull;
    if (!isNull$0) {
        result$1 = _myInput.toBytes();
        isNull$0 = result$1 == null;
    } else {
        result$1 = null;
    }

    // new behavior
    isNull$0 = _myInputIsNull;
    if (!isNull$0) {
        byte[] byteArrayTerm$0 = _myInput.toBytes();
        if (byteArrayTerm$0.length <= 2) {
            // If could pad
            result$1 = java.util.Arrays.copyOf(byteArrayTerm$0, 2);
            // result$1 = byteArrayTerm$0 // If could not pad
        } else {
            result$1 = java.util.Arrays.copyOf(byteArrayTerm$0, 2);
        }
        isNull$0 = result$1 == null;
    } else {
        result$1 = null;
    }

    */

    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        if (context.legacyBehaviour()) {
            return new CastRuleUtils.CodeWriter()
                    .assignStmt(returnVariable, methodCall(inputTerm, "toBytes"))
                    .toString();
        } else {
            final int targetLength = LogicalTypeChecks.getLength(targetLogicalType);
            final String byteArrayTerm = CodeGenUtils.newName("byteArrayTerm");

            return new CastRuleUtils.CodeWriter()
                    .declStmt(byte[].class, byteArrayTerm, methodCall(inputTerm, "toBytes"))
                    .ifStmt(
                            arrayLength(byteArrayTerm) + " <= " + targetLength,
                            thenWriter -> {
                                if (couldPad(targetLogicalType, targetLength)) {
                                    trimOrPadByteArray(
                                            returnVariable,
                                            targetLength,
                                            byteArrayTerm,
                                            thenWriter);
                                } else {
                                    thenWriter.assignStmt(returnVariable, byteArrayTerm);
                                }
                            },
                            elseWriter ->
                                    trimOrPadByteArray(
                                            returnVariable,
                                            targetLength,
                                            byteArrayTerm,
                                            elseWriter))
                    .toString();
        }
    }
}
