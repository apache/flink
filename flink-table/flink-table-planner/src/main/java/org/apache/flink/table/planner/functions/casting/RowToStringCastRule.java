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
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
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

/** {@link LogicalTypeRoot#ROW} to {@link LogicalTypeFamily#CHARACTER_STRING} cast rule. */
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
        builder$0.setLength(0);
        builder$0.append("(");
        int f0Value$2 = -1;
        boolean f0IsNull$3 = _myInput.isNullAt(0);
        if (!f0IsNull$3) {
            f0Value$2 = _myInput.getInt(0);
            isNull$2 = false;
            if (!isNull$2) {
                result$3 = org.apache.flink.table.data.binary.BinaryStringData.fromString("" + f0Value$2);
                isNull$2 = result$3 == null;
            } else {
                result$3 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            }
            builder$0.append(result$3);
        } else {
            builder$0.append("NULL");
        }
        builder$0.append(", ");
        org.apache.flink.table.data.binary.BinaryStringData f1Value$4 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
        boolean f1IsNull$5 = _myInput.isNullAt(1);
        if (!f1IsNull$5) {
            f1Value$4 = ((org.apache.flink.table.data.binary.BinaryStringData) _myInput.getString(1));
            builder$0.append(f1Value$4);
        } else {
            builder$0.append("NULL");
        }
        builder$0.append(")");
        java.lang.String resultString$1;
        if (builder$0.length() > 12) {
            resultString$1 = builder$0.substring(0, 12);
        } else {
            resultString$1 = builder$0.toString();
            if (builder$0.length() < 12) {
                int padLength$6;
                padLength$6 = 12 - builder$0.length();
                resultString$1 = resultString$1 + " ".repeat(padLength$6);
            }
        }
        result$1 = org.apache.flink.table.data.binary.BinaryStringData.fromString(resultString$1);
        isNull$0 = result$1 == null;
    } else {
        result$1 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
    }

    returnTerm = result$1
    isNullTerm = isNull$0

    */
    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final List<LogicalType> fields = LogicalTypeChecks.getFieldTypes(inputLogicalType);
        CodeGeneratorContext codeGeneratorContext = context.getCodeGeneratorContext();

        final String builderTerm = newName(codeGeneratorContext, "builder");
        context.declareClassField(
                className(StringBuilder.class), builderTerm, constructorCall(StringBuilder.class));

        final String resultStringTerm = newName(codeGeneratorContext, "resultString");
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

            final String fieldTerm = newName(codeGeneratorContext, "f" + fieldIndex + "Value");
            final String fieldIsNullTerm =
                    newName(codeGeneratorContext, "f" + fieldIndex + "IsNull");

            final CastCodeBlock codeBlock =
                    // Null check is done at the row access level
                    CastRuleProvider.generateAlwaysNonNullCodeBlock(
                            context, fieldTerm, fieldType, targetTypeForElementCast);

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
                        builderTerm,
                        context.getCodeGeneratorContext())
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
