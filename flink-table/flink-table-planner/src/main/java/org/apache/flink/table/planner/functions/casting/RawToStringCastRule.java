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

import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.BINARY_STRING_DATA_FROM_STRING;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;

/** {@link LogicalTypeRoot#RAW} to {@link LogicalTypeFamily#CHARACTER_STRING} cast rule. */
class RawToStringCastRule extends AbstractNullAwareCodeGeneratorCastRule<Object, String> {

    static final RawToStringCastRule INSTANCE = new RawToStringCastRule();

    private RawToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.RAW)
                        .target(LogicalTypeFamily.CHARACTER_STRING)
                        .build());
    }

    /* Example RAW(LocalDateTime.class) -> CHAR(12)

    isNull$0 = _myInputIsNull;
    if (!isNull$0) {
        java.lang.Object deserializedObj$0 = _myInput.toObject(typeSerializer$2);
        if (deserializedObj$0 != null) {
            java.lang.String resultString$1;
            resultString$1 = deserializedObj$0.toString().toString();
            if (deserializedObj$0.toString().length() > 12) {
                resultString$1 = deserializedObj$0.toString().substring(0, java.lang.Math.min(deserializedObj$0.toString().length(), 12));
            } else {
                if (resultString$1.length() < 12) {
                    int padLength$2;
                    padLength$2 = 12 - resultString$1.length();
                    java.lang.StringBuilder sbPadding$3;
                    sbPadding$3 = new java.lang.StringBuilder();
                    for (int i$4 = 0; i$4 < padLength$2; i$4++) {
                        sbPadding$3.append(" ");
                    }
                    resultString$1 = resultString$1 + sbPadding$3.toString();
                }
            }
            result$1 = org.apache.flink.table.data.binary.BinaryStringData.fromString(resultString$1);
        } else {
            result$1 = null;
        }
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
        final String typeSerializer = context.declareTypeSerializer(inputLogicalType);
        final String deserializedObjTerm = CodeGenUtils.newName("deserializedObj");

        final String resultStringTerm = CodeGenUtils.newName("resultString");
        final int length = LogicalTypeChecks.getLength(targetLogicalType);

        return new CastRuleUtils.CodeWriter()
                .declStmt(
                        Object.class,
                        deserializedObjTerm,
                        methodCall(inputTerm, "toObject", typeSerializer))
                .ifStmt(
                        deserializedObjTerm + " != null",
                        thenWriter ->
                                CharVarCharTrimPadCastRule.padAndTrimStringIfNeeded(
                                                thenWriter,
                                                targetLogicalType,
                                                context.legacyBehaviour(),
                                                length,
                                                resultStringTerm,
                                                methodCall(deserializedObjTerm, "toString"))
                                        .assignStmt(
                                                returnVariable,
                                                CastRuleUtils.staticCall(
                                                        BINARY_STRING_DATA_FROM_STRING(),
                                                        resultStringTerm)),
                        elseWriter -> elseWriter.assignStmt(returnVariable, "null"))
                .toString();
    }
}
