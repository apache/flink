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
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MultisetType;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.className;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;

/**
 * {@link LogicalTypeRoot#MULTISET} to {@link LogicalTypeFamily#CHARACTER_STRING} cast rule. Since
 * conversion is possible through a map that assigns each value to an integer multiplicity ({@code
 * Map<t, Integer>}) it extends from ({@code MapToStringCastRule})
 */
public class MultisetToStringCastRule extends MapToStringCastRule {
    static final MultisetToStringCastRule INSTANCE = new MultisetToStringCastRule();

    private MultisetToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .predicate(
                                (input, target) ->
                                        input.is(LogicalTypeRoot.MULTISET)
                                                && target.is(LogicalTypeFamily.CHARACTER_STRING)
                                                && CastRuleProvider.exists(
                                                        ((MultisetType) input).getElementType(),
                                                        target))
                        .build());
    }

    /* Example generated code for MULTISET<STRING>:

    isNull$0 = _myInputIsNull;
    if (!isNull$0) {
        org.apache.flink.table.data.ArrayData element$2 = _myInput.keyArray();
        org.apache.flink.table.data.ArrayData values$3 = _myInput.valueArray();
        builder$1.setLength(0);
        builder$1.append("{");
        for (int i$4 = 0; i$4 < _myInput.size(); i$4++) {
            if (i$4 != 0) {
                builder$1.append(", ");
            }
            org.apache.flink.table.data.binary.BinaryStringData element$5 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
            boolean elementIsNull$6 = element$2.isNullAt(i$4);
            int value$7 = -1;
            boolean valueIsNull$8 = values$3.isNullAt(i$4);
            if (!elementIsNull$6) {
                element$5 = ((org.apache.flink.table.data.binary.BinaryStringData) element$2.getString(i$4));
                builder$1.append(element$5);
            } else {
                builder$1.append("null");
            }
            builder$1.append("=");
            if (!valueIsNull$8) {
                value$7 = values$3.getInt(i$4);
                isNull$2 = valueIsNull$8;
                if (!isNull$2) {
                    result$3 = org.apache.flink.table.data.binary.BinaryStringData.fromString("" + value$7);
                    isNull$2 = result$3 == null;
                } else {
                    result$3 = org.apache.flink.table.data.binary.BinaryStringData.EMPTY_UTF8;
                }
                builder$1.append(result$3);
            } else {
                builder$1.append("null");
            }
        }
        builder$1.append("}");
        result$1 = org.apache.flink.table.data.binary.BinaryStringData.fromString(builder$1.toString());
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
        final LogicalType elementType = ((MultisetType) inputLogicalType).getElementType();
        final LogicalType valueType = INT().getLogicalType();

        final String builderTerm = newName("builder");
        context.declareClassField(
                className(StringBuilder.class), builderTerm, constructorCall(StringBuilder.class));

        final String elementArrayTerm = newName("element");
        final String valueArrayTerm = newName("values");

        return generateMapToString(
                context,
                inputTerm,
                returnVariable,
                targetLogicalType,
                elementType,
                valueType,
                builderTerm,
                elementArrayTerm,
                valueArrayTerm);
    }
}
