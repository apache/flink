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
import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.className;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.BINARY_STRING_DATA_FROM_STRING;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.nullLiteral;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.strLiteral;

/**
 * {@link LogicalTypeRoot#STRUCTURED_TYPE} to {@link LogicalTypeFamily#CHARACTER_STRING} cast rule.
 */
class StructuredToStringCastRule extends AbstractNullAwareCodeGeneratorCastRule<ArrayData, String> {

    static final StructuredToStringCastRule INSTANCE = new StructuredToStringCastRule();

    private StructuredToStringCastRule() {
        super(CastRulePredicate.builder().predicate(StructuredToStringCastRule::matches).build());
    }

    private static boolean matches(LogicalType input, LogicalType target) {
        return target.is(LogicalTypeFamily.CHARACTER_STRING)
                && input.is(LogicalTypeRoot.STRUCTURED_TYPE)
                && LogicalTypeChecks.getFieldTypes(input).stream()
                        .allMatch(fieldType -> CastRuleProvider.exists(fieldType, target));
    }

    /* Example generated code for MyStructuredType in CastRulesTest:

    builder$287.setLength(0);
    builder$287.append("(");
    long f0Value$289 = -1L;
    boolean f0IsNull$290 = _myInput.isNullAt(0);
    if (!f0IsNull$290) {
        f0Value$289 = _myInput.getLong(0);
        isNull$2 = f0IsNull$290;
        if (!isNull$2) {
            result$3 = org.apache.flink.table.data.binary.BinaryStringData.fromString("" + f0Value$289);
            isNull$2 = result$3 == null;
        } else {
            result$3 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
        }
        builder$287.append("a=" + result$3);
    } else {
        builder$287.append("a=" + "NULL");
    }
    builder$287.append(", ");
    long f1Value$291 = -1L;
    boolean f1IsNull$292 = _myInput.isNullAt(1);
    if (!f1IsNull$292) {
        f1Value$291 = _myInput.getLong(1);
        isNull$4 = f1IsNull$292;
        if (!isNull$4) {
            result$5 = org.apache.flink.table.data.binary.BinaryStringData.fromString("" + f1Value$291);
            isNull$4 = result$5 == null;
        } else {
            result$5 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
        }
        builder$287.append("b=" + result$5);
    } else {
        builder$287.append("b=" + "NULL");
    }
    builder$287.append(", ");
    org.apache.flink.table.data.binary.BinaryStringData f2Value$293 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
    boolean f2IsNull$294 = _myInput.isNullAt(2);
    if (!f2IsNull$294) {
        f2Value$293 = ((org.apache.flink.table.data.binary.BinaryStringData) _myInput.getString(2));
        builder$287.append("c=" + f2Value$293);
    } else {
        builder$287.append("c=" + "NULL");
    }
    builder$287.append(", ");
    org.apache.flink.table.data.ArrayData f3Value$295 = null;
    boolean f3IsNull$296 = _myInput.isNullAt(3);
    if (!f3IsNull$296) {
        f3Value$295 = _myInput.getArray(3);
        isNull$6 = f3IsNull$296;
        if (!isNull$6) {
            builder$297.setLength(0);
            builder$297.append("[");
            for (int i$299 = 0; i$299 < f3Value$295.size(); i$299++) {
                if (i$299 != 0) {
                    builder$297.append(", ");
                }
                org.apache.flink.table.data.binary.BinaryStringData element$300 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
                boolean elementIsNull$301 = f3Value$295.isNullAt(i$299);
                if (!elementIsNull$301) {
                    element$300 = ((org.apache.flink.table.data.binary.BinaryStringData) f3Value$295.getString(i$299));
                    builder$297.append(element$300);
                } else {
                    builder$297.append("NULL");
                }
            }
            builder$297.append("]");
            java.lang.String resultString$298;
            resultString$298 = builder$297.toString();
            result$7 = org.apache.flink.table.data.binary.BinaryStringData.fromString(resultString$298);
            isNull$6 = result$7 == null;
        } else {
            result$7 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
        }
        builder$287.append("d=" + result$7);
    } else {
        builder$287.append("d=" + "NULL");
    }
    builder$287.append(")");
    java.lang.String resultString$288;
    resultString$288 = builder$287.toString();
    result$1 = org.apache.flink.table.data.binary.BinaryStringData.fromString(resultString$288);

     */
    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        StructuredType inputStructuredType = (StructuredType) inputLogicalType;

        final String builderTerm = newName("builder");
        context.declareClassField(
                className(StringBuilder.class), builderTerm, constructorCall(StringBuilder.class));

        final String resultStringTerm = newName("resultString");
        final int length = LogicalTypeChecks.getLength(targetLogicalType);
        final CastRuleUtils.CodeWriter writer =
                new CastRuleUtils.CodeWriter()
                        .stmt(methodCall(builderTerm, "setLength", 0))
                        .stmt(methodCall(builderTerm, "append", strLiteral("(")));

        for (int i = 0; i < inputStructuredType.getAttributes().size(); i++) {
            final int fieldIndex = i;
            final StructuredType.StructuredAttribute attribute =
                    inputStructuredType.getAttributes().get(fieldIndex);

            final String fieldTerm = newName("f" + fieldIndex + "Value");
            final String fieldIsNullTerm = newName("f" + fieldIndex + "IsNull");

            final CastCodeBlock codeBlock =
                    // Null check is done at the row access level
                    CastRuleProvider.generateAlwaysNonNullCodeBlock(
                            context, fieldTerm, attribute.getType(), VarCharType.STRING_TYPE);

            // Write the comma
            if (fieldIndex != 0) {
                writer.stmt(methodCall(builderTerm, "append", strLiteral(", ")));
            }

            writer
                    // Extract value from row
                    .declPrimitiveStmt(attribute.getType(), fieldTerm)
                    .declStmt(
                            boolean.class,
                            fieldIsNullTerm,
                            methodCall(inputTerm, "isNullAt", fieldIndex))
                    .ifStmt(
                            "!" + fieldIsNullTerm,
                            thenBodyWriter ->
                                    thenBodyWriter
                                            // If attribute not null, extract it and execute the
                                            // cast
                                            .assignStmt(
                                                    fieldTerm,
                                                    CodeGenUtils.rowFieldReadAccess(
                                                            fieldIndex,
                                                            inputTerm,
                                                            attribute.getType()))
                                            .append(codeBlock)
                                            .stmt(
                                                    methodCall(
                                                            builderTerm,
                                                            "append",
                                                            strLiteral(attribute.getName() + "=")
                                                                    + " + "
                                                                    + codeBlock.getReturnTerm())),
                            elseBodyWriter ->
                                    // If attribute is null, just write NULL
                                    elseBodyWriter.stmt(
                                            methodCall(
                                                    builderTerm,
                                                    "append",
                                                    strLiteral(attribute.getName() + "=")
                                                            + " + "
                                                            + nullLiteral(
                                                                    context.legacyBehaviour()))));
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
}
