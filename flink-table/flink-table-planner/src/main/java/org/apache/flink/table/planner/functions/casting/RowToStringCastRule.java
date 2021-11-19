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
import org.apache.flink.table.types.logical.RowType;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.className;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.rowFieldReadAccess;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.BINARY_STRING_DATA_FROM_STRING;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.NULL_STR_LITERAL;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.strLiteral;

/** {@link LogicalTypeRoot#ROW} to {@link LogicalTypeFamily#CHARACTER_STRING} cast rule. */
class RowToStringCastRule extends AbstractNullAwareCodeGeneratorCastRule<ArrayData, String> {

    static final RowToStringCastRule INSTANCE = new RowToStringCastRule();

    private RowToStringCastRule() {
        super(CastRulePredicate.builder().predicate(RowToStringCastRule::matches).build());
    }

    private static boolean matches(LogicalType input, LogicalType target) {
        return input.is(LogicalTypeRoot.ROW)
                && target.is(LogicalTypeFamily.CHARACTER_STRING)
                && ((RowType) input)
                        .getFields().stream()
                                .allMatch(
                                        field -> CastRuleProvider.exists(field.getType(), target));
    }

    /* Example generated code for ROW<`f0` INT, `f1` STRING>:

    isNull$0 = _myInputIsNull;
    if (!isNull$0) {
        builder$1.setLength(0);
        builder$1.append("(");
        int f0Value$2 = -1;
        boolean f0IsNull$3 = _myInput.isNullAt(0);
        if (!f0IsNull$3) {
            f0Value$2 = _myInput.getInt(0);
            result$2 = org.apache.flink.table.data.binary.BinaryStringData.fromString("" + f0Value$2);
            builder$1.append(result$2);
        } else {
            builder$1.append("null");
        }
        builder$1.append(",");
        org.apache.flink.table.data.binary.BinaryStringData f1Value$4 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
        boolean f1IsNull$5 = _myInput.isNullAt(1);
        if (!f1IsNull$5) {
            f1Value$4 = ((org.apache.flink.table.data.binary.BinaryStringData) _myInput.getString(1));
            builder$1.append(f1Value$4);
        } else {
            builder$1.append("null");
        }
        builder$1.append(")");
        result$1 = org.apache.flink.table.data.binary.BinaryStringData.fromString(builder$1.toString());
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
        final List<LogicalType> fields =
                ((RowType) inputLogicalType)
                        .getFields().stream()
                                .map(RowType.RowField::getType)
                                .collect(Collectors.toList());

        final String builderTerm = newName("builder");
        context.declareClassField(
                className(StringBuilder.class), builderTerm, constructorCall(StringBuilder.class));

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
                            targetLogicalType);

            // Write the comma
            if (fieldIndex != 0) {
                writer.stmt(methodCall(builderTerm, "append", strLiteral(",")));
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
                                            methodCall(builderTerm, "append", NULL_STR_LITERAL)));
        }

        writer.stmt(methodCall(builderTerm, "append", strLiteral(")")))
                // Assign the result value
                .assignStmt(
                        returnVariable,
                        CastRuleUtils.staticCall(
                                BINARY_STRING_DATA_FROM_STRING(),
                                methodCall(builderTerm, "toString")));

        return writer.toString();
    }
}
