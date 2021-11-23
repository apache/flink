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
import org.apache.flink.table.types.logical.TinyIntType;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.arrFieldAccess;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.className;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.BINARY_STRING_DATA_FROM_STRING;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.accessField;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.strLiteral;

class BinaryToStringCastRule extends AbstractNullAwareCodeGeneratorCastRule<byte[], String> {

    static final BinaryToStringCastRule INSTANCE = new BinaryToStringCastRule();

    private BinaryToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.BINARY_STRING)
                        .target(LogicalTypeFamily.CHARACTER_STRING)
                        .build());
    }

    /* Example generated code

    isNull$0 = _myInputIsNull;
    if (!isNull$0) {
        builder$1.setLength(0);
        builder$1.append("[");
        for (int i$2 = 0; i$2 < _myInput.length; i$2++) {
            if (i$2 != 0) {
                builder$1.append(", ");
            }
        byte element$3 = -1;
        element$3 = _myInput[i$2];
        builder$1.append(element$3);
        }
        builder$1.append("]");
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
        final LogicalType innerInputType = new TinyIntType();

        final String builderTerm = newName("builder");
        context.declareClassField(
                className(StringBuilder.class), builderTerm, constructorCall(StringBuilder.class));

        return new CastRuleUtils.CodeWriter()
                .stmt(methodCall(builderTerm, "setLength", 0))
                .stmt(methodCall(builderTerm, "append", strLiteral("[")))
                .forStmt(
                        accessField(inputTerm, "length"),
                        (indexTerm, loopBodyWriter) -> {
                            String elementTerm = newName("element");

                            loopBodyWriter
                                    // Write the comma
                                    .ifStmt(
                                            indexTerm + " != 0",
                                            thenBodyWriter ->
                                                    thenBodyWriter.stmt(
                                                            methodCall(
                                                                    builderTerm,
                                                                    "append",
                                                                    strLiteral((", ")))))
                                    // Extract element from array
                                    .declPrimitiveStmt(innerInputType, elementTerm)
                                    .assignStmt(elementTerm, arrFieldAccess(indexTerm, inputTerm))
                                    .stmt(methodCall(builderTerm, "append", elementTerm));
                        })
                .stmt(methodCall(builderTerm, "append", strLiteral("]")))
                // Assign the result value
                .assignStmt(
                        returnVariable,
                        CastRuleUtils.staticCall(
                                BINARY_STRING_DATA_FROM_STRING(),
                                methodCall(builderTerm, "toString")))
                .toString();
    }
}
