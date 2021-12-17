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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.util.Arrays;

import static org.apache.flink.table.codesplit.CodeSplitUtil.newName;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.arrayLength;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

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

    /* Example generated code for BINARY(2):

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
            result$1 = byteArrayTerm$0;
        } else {
            result$1 = java.util.Arrays.copyOfRange(byteArrayTerm$0, 0, 2);
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
        final int targetLength = LogicalTypeChecks.getLength(targetLogicalType);

        final String byteArrayTerm = newName("byteArrayTerm");

        if (context.legacyBehaviour()) {
            return new CastRuleUtils.CodeWriter()
                    .assignStmt(returnVariable, methodCall(inputTerm, "toBytes"))
                    .toString();
        } else {
            return new CastRuleUtils.CodeWriter()
                    .declStmt(byte[].class, byteArrayTerm, methodCall(inputTerm, "toBytes"))
                    .ifStmt(
                            arrayLength(byteArrayTerm) + " <= " + targetLength,
                            thenWriter -> thenWriter.assignStmt(returnVariable, byteArrayTerm),
                            elseWriter ->
                                    elseWriter.assignStmt(
                                            returnVariable,
                                            staticCall(
                                                    Arrays.class,
                                                    "copyOfRange",
                                                    byteArrayTerm,
                                                    0,
                                                    targetLength)))
                    .toString();
        }
    }
}
