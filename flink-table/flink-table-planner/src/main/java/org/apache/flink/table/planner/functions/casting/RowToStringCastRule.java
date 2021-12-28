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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.util.List;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.className;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.rowFieldReadAccess;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.BINARY_STRING_DATA_FROM_STRING;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.nullLiteral;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.strLiteral;

/**
 * {@link LogicalTypeRoot#ROW} and {@link LogicalTypeRoot#STRUCTURED_TYPE} to {@link
 * LogicalTypeFamily#CHARACTER_STRING} cast rule.
 */
class RowToStringCastRule extends AbstractNullAwareCodeGeneratorCastRule<ArrayData, String> {

    static final RowToStringCastRule INSTANCE = new RowToStringCastRule();

    private RowToStringCastRule() {
        super(CastRulePredicate.builder().predicate(RowToStringCastRule::matches).build());
    }

    private static boolean matches(LogicalType input, LogicalType target) {
        return (input.is(LogicalTypeRoot.ROW) || input.is(LogicalTypeRoot.STRUCTURED_TYPE))
                && target.is(LogicalTypeFamily.CHARACTER_STRING)
                && LogicalTypeChecks.getFieldTypes(input).stream()
                        .allMatch(fieldType -> CastRuleProvider.exists(fieldType, target));
    }

    /* Example generated code for ROW<`f0` INT, `f1` STRING> -> CHAR(12):

    isNull$0 = _myInputIsNull;
    if (!isNull$0) {
        builder$1.setLength(0);
        builder$1.append("(");
        int f0Value$3 = -1;
        boolean f0IsNull$4 = _myInput.isNullAt(0);
        if (!f0IsNull$4) {
            f0Value$3 = _myInput.getInt(0);
            isNull$2 = f0IsNull$4;
            if (!isNull$2) {
                result$3 = org.apache.flink.table.data.binary.BinaryStringData.fromString("" + f0Value$3);
                isNull$2 = result$3 == null;
            } else {
                result$3 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            }
            builder$1.append(result$3);
        } else {
            builder$1.append("NULL");
        }
        builder$1.append(", ");
        org.apache.flink.table.data.binary.BinaryStringData f1Value$5 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
        boolean f1IsNull$6 = _myInput.isNullAt(1);
        if (!f1IsNull$6) {
            f1Value$5 = ((org.apache.flink.table.data.binary.BinaryStringData) _myInput.getString(1));
            builder$1.append(f1Value$5);
        } else {
            builder$1.append("NULL");
        }
        builder$1.append(")");
        java.lang.String resultString$2;
        resultString$2 = builder$1.toString();
        if (builder$1.length() > 12) {
            resultString$2 = builder$1.substring(0, java.lang.Math.min(builder$1.length(), 12));
        } else {
            if (resultString$2.length() < 12) {
                int padLength$7;
                padLength$7 = 12 - resultString$2.length();
                java.lang.StringBuilder sbPadding$8;
                sbPadding$8 = new java.lang.StringBuilder();
                for (int i$9 = 0; i$9 < padLength$7; i$9++) {
                    sbPadding$8.append(" ");
                }
                resultString$2 = resultString$2 + sbPadding$8.toString();
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
        final List<LogicalType> fields = LogicalTypeChecks.getFieldTypes(inputLogicalType);

        final String builderTerm = newName("builder");
        context.declareClassField(
                className(StringBuilder.class), builderTerm, constructorCall(StringBuilder.class));

        final String resultStringTerm = newName("resultString");
        final int length = LogicalTypeChecks.getLength(targetLogicalType);
        final LogicalType targetTypeForElementCast =
                targetLogicalType.is(LogicalTypeFamily.CHARACTER_STRING)
                        ? VarCharType.STRING_TYPE
                        : targetLogicalType;

        final CastRuleUtils.CodeWriter writer =
                new CastRuleUtils.CodeWriter()
                        .stmt(methodCall(builderTerm, "setLength", 0))
                        .stmt(methodCall(builderTerm, "append", strLiteral("(")));

        for (int i = 0; i < fields.size(); i++) {
            final int fieldIndex = i;
            final LogicalType fieldType = fields.get(fieldIndex);

            final String fieldTerm = newName("f" + fieldIndex + "Value");
            final String fieldIsNullTerm = newName("f" + fieldIndex + "IsNull");

            final CastCodeBlock codeBlock =
                    CastRuleProvider.generateCodeBlock(
                            context,
                            fieldTerm,
                            fieldIsNullTerm,
                            // Null check is done at the row access level
                            fieldType.copy(false),
                            targetTypeForElementCast);

            // Write the comma
            if (fieldIndex != 0) {
                final String comma = getDelimiter(context);
                writer.stmt(methodCall(builderTerm, "append", comma));
            }

            writer
                    // Extract value from row
                    .declPrimitiveStmt(fieldType, fieldTerm)
                    .declStmt(
                            boolean.class,
                            fieldIsNullTerm,
                            methodCall(inputTerm, "isNullAt", fieldIndex))
                    .ifStmt(
                            "!" + fieldIsNullTerm,
                            thenBodyWriter ->
                                    thenBodyWriter
                                            // If element not null, extract it and
                                            // execute the cast
                                            .assignStmt(
                                                    fieldTerm,
                                                    rowFieldReadAccess(
                                                            fieldIndex, inputTerm, fieldType))
                                            .append(codeBlock)
                                            .stmt(
                                                    methodCall(
                                                            builderTerm,
                                                            "append",
                                                            codeBlock.getReturnTerm())),
                            elseBodyWriter ->
                                    // If element is null, just write NULL
                                    elseBodyWriter.stmt(
                                            methodCall(
                                                    builderTerm,
                                                    "append",
                                                    nullLiteral(context.legacyBehaviour()))));
        }

        writer.stmt(methodCall(builderTerm, "append", strLiteral(")")));
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

    private static String getDelimiter(CodeGeneratorCastRule.Context context) {
        final String comma;
        if (context.legacyBehaviour()) {
            comma = strLiteral(",");
        } else {
            comma = strLiteral(", ");
        }
        return comma;
    }
}
