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
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;

import java.util.function.Consumer;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.className;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.rowFieldReadAccess;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.BINARY_STRING_DATA_FROM_STRING;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.NULL_STR_LITERAL;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.strLiteral;

/**
 * {@link LogicalTypeRoot#MAP} and {@link LogicalTypeRoot#MULTISET} to {@link
 * LogicalTypeFamily#CHARACTER_STRING} cast rule.
 */
class MapAndMultisetToStringCastRule
        extends AbstractNullAwareCodeGeneratorCastRule<ArrayData, String> {

    static final MapAndMultisetToStringCastRule INSTANCE = new MapAndMultisetToStringCastRule();

    private MapAndMultisetToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .predicate(MapAndMultisetToStringCastRule::isMapOrMultiset)
                        .build());
    }

    private static boolean isMapOrMultiset(LogicalType input, LogicalType target) {
        return target.is(LogicalTypeFamily.CHARACTER_STRING)
                && ((input.is(LogicalTypeRoot.MAP)
                                && CastRuleProvider.exists(((MapType) input).getKeyType(), target)
                                && CastRuleProvider.exists(
                                        ((MapType) input).getValueType(), target))
                        || (input.is(LogicalTypeRoot.MULTISET)
                                && CastRuleProvider.exists(
                                        ((MultisetType) input).getElementType(), target)));
    }

    /* Example generated code for MAP<STRING, INTERVAL MONTH>:

    isNull$0 = _myInputIsNull;
    if (!isNull$0) {
        org.apache.flink.table.data.ArrayData keys$2 = _myInput.keyArray();
        org.apache.flink.table.data.ArrayData values$3 = _myInput.valueArray();
        builder$1.setLength(0);
        builder$1.append("{");
        for (int i$4 = 0; i$4 < _myInput.size(); i$4++) {
            if (i$4 != 0) {
                builder$1.append(", ");
            }
            org.apache.flink.table.data.binary.BinaryStringData key$5 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            boolean keyIsNull$6 = keys$2.isNullAt(i$4);
            int value$7 = -1;
            boolean valueIsNull$8 = values$3.isNullAt(i$4);
            if (!keyIsNull$6) {
                key$5 = ((org.apache.flink.table.data.binary.BinaryStringData) keys$2.getString(i$4));
                builder$1.append(key$5);
            } else {
                builder$1.append("null");
            }
            builder$1.append("=");
            if (!valueIsNull$8) {
                value$7 = values$3.getInt(i$4);
                result$2 = org.apache.flink.table.data.binary.BinaryStringData.fromString(org.apache.flink.table.utils.DateTimeUtils.intervalYearMonthToString(value$7));
                builder$1.append(result$2);
            } else {
                builder$1.append("null");
            }
        }
        builder$1.append("}");
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
        final LogicalType keyType =
                inputLogicalType.is(LogicalTypeRoot.MULTISET)
                        ? ((MultisetType) inputLogicalType).getElementType()
                        : ((MapType) inputLogicalType).getKeyType();
        final LogicalType valueType =
                inputLogicalType.is(LogicalTypeRoot.MULTISET)
                        ? new IntType(false)
                        : ((MapType) inputLogicalType).getValueType();

        final String builderTerm = newName("builder");
        context.declareClassField(
                className(StringBuilder.class), builderTerm, constructorCall(StringBuilder.class));

        final String keyArrayTerm = newName("keys");
        final String valueArrayTerm = newName("values");

        return new CastRuleUtils.CodeWriter()
                .declStmt(ArrayData.class, keyArrayTerm, methodCall(inputTerm, "keyArray"))
                .declStmt(ArrayData.class, valueArrayTerm, methodCall(inputTerm, "valueArray"))
                .stmt(methodCall(builderTerm, "setLength", 0))
                .stmt(methodCall(builderTerm, "append", strLiteral("{")))
                .forStmt(
                        methodCall(inputTerm, "size"),
                        (indexTerm, loopBodyWriter) -> {
                            String keyTerm = newName("key");
                            String keyIsNullTerm = newName("keyIsNull");
                            String valueTerm = newName("value");
                            String valueIsNullTerm = newName("valueIsNull");

                            CastCodeBlock keyCast =
                                    CastRuleProvider.generateCodeBlock(
                                            context,
                                            keyTerm,
                                            keyIsNullTerm,
                                            // Null check is done at the key array access level
                                            keyType.copy(false),
                                            targetLogicalType);
                            CastCodeBlock valueCast =
                                    CastRuleProvider.generateCodeBlock(
                                            context,
                                            valueTerm,
                                            valueIsNullTerm,
                                            // Null check is done at the value array access level
                                            valueType.copy(false),
                                            targetLogicalType);

                            Consumer<CastRuleUtils.CodeWriter> appendNonNullValue =
                                    bodyWriter ->
                                            bodyWriter
                                                    // If value not null, extract it and
                                                    // execute the cast
                                                    .assignStmt(
                                                            valueTerm,
                                                            rowFieldReadAccess(
                                                                    indexTerm,
                                                                    valueArrayTerm,
                                                                    valueType))
                                                    .append(valueCast)
                                                    .stmt(
                                                            methodCall(
                                                                    builderTerm,
                                                                    "append",
                                                                    valueCast.getReturnTerm()));
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
                                    // Declare key and values variables
                                    .declPrimitiveStmt(keyType, keyTerm)
                                    .declStmt(
                                            boolean.class,
                                            keyIsNullTerm,
                                            methodCall(keyArrayTerm, "isNullAt", indexTerm))
                                    .declPrimitiveStmt(valueType, valueTerm)
                                    .declStmt(
                                            boolean.class,
                                            valueIsNullTerm,
                                            methodCall(valueArrayTerm, "isNullAt", indexTerm))
                                    // Execute casting if inner key/value not null
                                    .ifStmt(
                                            "!" + keyIsNullTerm,
                                            thenBodyWriter ->
                                                    thenBodyWriter
                                                            // If key not null, extract it and
                                                            // execute the cast
                                                            .assignStmt(
                                                                    keyTerm,
                                                                    rowFieldReadAccess(
                                                                            indexTerm,
                                                                            keyArrayTerm,
                                                                            keyType))
                                                            .append(keyCast)
                                                            .stmt(
                                                                    methodCall(
                                                                            builderTerm,
                                                                            "append",
                                                                            keyCast
                                                                                    .getReturnTerm())),
                                            elseBodyWriter ->
                                                    elseBodyWriter.stmt(
                                                            methodCall(
                                                                    builderTerm,
                                                                    "append",
                                                                    NULL_STR_LITERAL)))
                                    .stmt(methodCall(builderTerm, "append", strLiteral("=")));
                            if (inputLogicalType.is(LogicalTypeRoot.MULTISET)) {
                                appendNonNullValue.accept(loopBodyWriter);
                            } else {
                                loopBodyWriter.ifStmt(
                                        "!" + valueIsNullTerm,
                                        appendNonNullValue,
                                        elseBodyWriter ->
                                                elseBodyWriter.stmt(
                                                        methodCall(
                                                                builderTerm,
                                                                "append",
                                                                NULL_STR_LITERAL)));
                            }
                        })
                .stmt(methodCall(builderTerm, "append", strLiteral("}")))
                // Assign the result value
                .assignStmt(
                        returnVariable,
                        CastRuleUtils.staticCall(
                                BINARY_STRING_DATA_FROM_STRING(),
                                methodCall(builderTerm, "toString")))
                .toString();
    }
}
