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

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.className;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.rowFieldReadAccess;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.BINARY_STRING_DATA_FROM_STRING;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.nullLiteral;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.strLiteral;
import static org.apache.flink.table.planner.functions.casting.CharVarCharTrimPadCastRule.couldTrim;
import static org.apache.flink.table.planner.functions.casting.CharVarCharTrimPadCastRule.stringExceedsLength;
import static org.apache.flink.table.types.logical.VarCharType.STRING_TYPE;

/** {@link LogicalTypeRoot#ARRAY} to {@link LogicalTypeFamily#CHARACTER_STRING} cast rule. */
class ArrayToStringCastRule extends AbstractNullAwareCodeGeneratorCastRule<ArrayData, String> {

    static final ArrayToStringCastRule INSTANCE = new ArrayToStringCastRule();

    private ArrayToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .predicate(
                                (input, target) ->
                                        input.is(LogicalTypeRoot.ARRAY)
                                                && target.is(LogicalTypeFamily.CHARACTER_STRING)
                                                && CastRuleProvider.exists(
                                                        ((ArrayType) input).getElementType(),
                                                        target))
                        .build());
    }

    /* Example generated code for ARRAY<INT> -> CHAR(10)

    isNull$0 = _myInputIsNull;
    if (!isNull$0) {
        builder$1.setLength(0);
        builder$1.append("[");
        for (int i$3 = 0; i$3 < _myInput.size(); i$3++) {
            if (builder$1.length() > 10) {
                break;
            }
            if (i$3 != 0) {
                builder$1.append(", ");
            }
            int element$4 = -1;
            boolean elementIsNull$5 = _myInput.isNullAt(i$3);
            if (!elementIsNull$5) {
                element$4 = _myInput.getInt(i$3);
                isNull$2 = false;
                if (!isNull$2) {
                    result$3 = org.apache.flink.table.data.binary.BinaryStringData.fromString("" + element$4);
                    isNull$2 = result$3 == null;
                } else {
                    result$3 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
                }
                builder$1.append(result$3);
            } else {
                builder$1.append("NULL");
            }
        }
        builder$1.append("]");
        java.lang.String resultString$2;
        resultString$2 = builder$1.toString();
        if (builder$1.length() > 10) {
            resultString$2 = builder$1.substring(0, java.lang.Math.min(builder$1.length(), 10));
        } else {
            if (resultString$2.length() < 10) {
                int padLength$6;
                padLength$6 = 10 - resultString$2.length();
                java.lang.StringBuilder sbPadding$7;
                sbPadding$7 = new java.lang.StringBuilder();
                for (int i$8 = 0; i$8 < padLength$6; i$8++) {
                    sbPadding$7.append(" ");
                }
                resultString$2 = resultString$2 + sbPadding$7.toString();
            }
        }
        result$1 = org.apache.flink.table.data.binary.BinaryStringData.fromString(resultString$2);
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
        final LogicalType innerInputType = ((ArrayType) inputLogicalType).getElementType();

        final String builderTerm = newName("builder");
        context.declareClassField(
                className(StringBuilder.class), builderTerm, constructorCall(StringBuilder.class));

        final String resultStringTerm = newName("resultString");
        final int length = LogicalTypeChecks.getLength(targetLogicalType);

        CastRuleUtils.CodeWriter writer =
                new CastRuleUtils.CodeWriter()
                        .stmt(methodCall(builderTerm, "setLength", 0))
                        .stmt(methodCall(builderTerm, "append", strLiteral("[")))
                        .forStmt(
                                methodCall(inputTerm, "size"),
                                (indexTerm, loopBodyWriter) -> {
                                    String elementTerm = newName("element");
                                    String elementIsNullTerm = newName("elementIsNull");

                                    CastCodeBlock codeBlock =
                                            CastRuleProvider.generateCodeBlock(
                                                    context,
                                                    elementTerm,
                                                    "false",
                                                    // Null check is done at the array
                                                    // access level
                                                    innerInputType.copy(false),
                                                    STRING_TYPE);

                                    if (!context.legacyBehaviour() && couldTrim(length)) {
                                        // Break if the target length is already exceeded
                                        loopBodyWriter.ifStmt(
                                                stringExceedsLength(builderTerm, length),
                                                thenBodyWriter -> thenBodyWriter.stmt("break"));
                                    }
                                    loopBodyWriter
                                            // Write the comma
                                            .ifStmt(
                                                    indexTerm + " != 0",
                                                    thenBodyWriter ->
                                                            thenBodyWriter.stmt(
                                                                    methodCall(
                                                                            builderTerm,
                                                                            "append",
                                                                            strLiteral(", "))))
                                            // Extract element from array
                                            .declPrimitiveStmt(innerInputType, elementTerm)
                                            .declStmt(
                                                    boolean.class,
                                                    elementIsNullTerm,
                                                    methodCall(inputTerm, "isNullAt", indexTerm))
                                            .ifStmt(
                                                    "!" + elementIsNullTerm,
                                                    thenBodyWriter ->
                                                            thenBodyWriter
                                                                    // If element not null,
                                                                    // extract it and
                                                                    // execute the cast
                                                                    .assignStmt(
                                                                            elementTerm,
                                                                            rowFieldReadAccess(
                                                                                    indexTerm,
                                                                                    inputTerm,
                                                                                    innerInputType))
                                                                    .append(codeBlock)
                                                                    .stmt(
                                                                            methodCall(
                                                                                    builderTerm,
                                                                                    "append",
                                                                                    codeBlock
                                                                                            .getReturnTerm())),
                                                    elseBodyWriter ->
                                                            // If element is null, just
                                                            // write NULL
                                                            elseBodyWriter.stmt(
                                                                    methodCall(
                                                                            builderTerm,
                                                                            "append",
                                                                            nullLiteral(
                                                                                    context
                                                                                            .legacyBehaviour()))));
                                })
                        .stmt(methodCall(builderTerm, "append", strLiteral("]")));
        return CharVarCharTrimPadCastRule.padAndTrimStringIfNeeded(
                        writer,
                        targetLogicalType,
                        context.legacyBehaviour(),
                        length,
                        resultStringTerm,
                        builderTerm)
                // Assign the result value
                .assignStmt(
                        returnVariable,
                        CastRuleUtils.staticCall(
                                BINARY_STRING_DATA_FROM_STRING(), resultStringTerm))
                .toString();
    }
}
