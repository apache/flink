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
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.rowFieldReadAccess;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.newArray;

/** {@link LogicalTypeRoot#ARRAY} to {@link LogicalTypeRoot#ARRAY} cast rule. */
class ArrayToArrayCastRule extends AbstractNullAwareCodeGeneratorCastRule<ArrayData, ArrayData>
        implements ConstructedToConstructedCastRule<ArrayData, ArrayData> {

    static final ArrayToArrayCastRule INSTANCE = new ArrayToArrayCastRule();

    private ArrayToArrayCastRule() {
        super(
                CastRulePredicate.builder()
                        .predicate(
                                (input, target) ->
                                        input.is(LogicalTypeRoot.ARRAY)
                                                && target.is(LogicalTypeRoot.ARRAY)
                                                && isValidArrayCasting(
                                                        ((ArrayType) input).getElementType(),
                                                        ((ArrayType) target).getElementType()))
                        .build());
    }

    private static boolean isValidArrayCasting(
            LogicalType innerInputType, LogicalType innerTargetType) {
        return CastRuleProvider.resolve(innerInputType, innerTargetType) != null;
    }

    @SuppressWarnings("rawtypes")
    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final LogicalType innerInputType = ((ArrayType) inputLogicalType).getElementType();
        final LogicalType innerTargetType = ((ArrayType) targetLogicalType).getElementType();

        final String innerTargetTypeTerm = arrayElementType(innerTargetType);
        final String arraySize = methodCall(inputTerm, "size");
        final String objArrayTerm = newName("objArray");

        return new CastRuleUtils.CodeWriter()
                .declStmt(
                        innerTargetTypeTerm + "[]",
                        objArrayTerm,
                        newArray(innerTargetTypeTerm, arraySize))
                .forStmt(
                        arraySize,
                        (index, loopWriter) -> {
                            CastCodeBlock codeBlock =
                                    // Null check is done at the array access level
                                    CastRuleProvider.generateAlwaysNonNullCodeBlock(
                                            context,
                                            rowFieldReadAccess(index, inputTerm, innerInputType),
                                            innerInputType,
                                            innerTargetType);

                            if (innerTargetType.isNullable()) {
                                loopWriter.ifStmt(
                                        "!" + methodCall(inputTerm, "isNullAt", index),
                                        thenWriter ->
                                                thenWriter
                                                        .append(codeBlock)
                                                        .assignArrayStmt(
                                                                objArrayTerm,
                                                                index,
                                                                codeBlock.getReturnTerm()));
                            } else {
                                loopWriter
                                        .append(codeBlock)
                                        .assignArrayStmt(
                                                objArrayTerm, index, codeBlock.getReturnTerm());
                            }
                        })
                .assignStmt(returnVariable, constructorCall(GenericArrayData.class, objArrayTerm))
                .toString();
    }

    private static String arrayElementType(LogicalType t) {
        if (t.isNullable()) {
            return CodeGenUtils.boxedTypeTermForType(t);
        }
        return CodeGenUtils.primitiveTypeTermForType(t);
    }
}
