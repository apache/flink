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
import org.apache.flink.table.runtime.functions.VariantCastUtils;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.types.variant.Variant;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.newArray;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.strLiteral;

/**
 * {@link LogicalTypeRoot#VARIANT} to {@link LogicalTypeRoot#ARRAY} cast rule.
 *
 * <p>The variant must be an array, otherwise the cast fails. Each element is itself a variant and
 * casts to the target element type by the full {@code VARIANT}-to-element rule, recursively. An
 * element that stores a JSON {@code null} maps to SQL {@code NULL} when the element type is
 * nullable and fails the cast when it is {@code NOT NULL}.
 */
class VariantToArrayCastRule extends AbstractVariantToConstructedCastRule<ArrayData> {

    static final VariantToArrayCastRule INSTANCE = new VariantToArrayCastRule();

    private VariantToArrayCastRule() {
        super(
                CastRulePredicate.builder()
                        .predicate(
                                (input, target) ->
                                        input.is(LogicalTypeRoot.VARIANT)
                                                && target.is(LogicalTypeRoot.ARRAY)
                                                && CastRuleProvider.resolve(
                                                                input,
                                                                ((ArrayType) target)
                                                                        .getElementType())
                                                        != null)
                        .build());
    }

    /* Example generated code for ARRAY<INT>:

    int arraySize$2 =
            org.apache.flink.table.runtime.functions.VariantCastUtils.arraySize(
                    variant$1, "ARRAY<INT>");
    java.lang.Integer[] objArray$3 = new java.lang.Integer[arraySize$2];
    for (int i$4 = 0; i$4 < arraySize$2; i$4++) {
        org.apache.flink.types.variant.Variant element$5 = variant$1.getElement(i$4);
        if (!element$5.isNull()) {
            result$6 =
                    ((int) org.apache.flink.table.runtime.functions.VariantCastUtils.toIntegral(
                            element$5, -2147483648L, 2147483647L, "INTEGER"));
            objArray$3[i$4] = result$6;
        }
    }
    result$0 = new org.apache.flink.table.data.GenericArrayData(objArray$3);

    A JSON null element leaves the slot null (SQL NULL); a NOT NULL element type emits a throw instead.

    */
    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final LogicalType elementType = ((ArrayType) targetLogicalType).getElementType();
        final String elementTypeTerm = arrayElementType(elementType);
        final String sizeTerm = newName(context.getCodeGeneratorContext(), "arraySize");
        final String arrayTerm = newName(context.getCodeGeneratorContext(), "objArray");
        final String elementTerm = newName(context.getCodeGeneratorContext(), "element");

        // The element is guaranteed non-null here, since the JSON null is handled below, so the
        // inner cast is the plain VARIANT-to-element rule. A JSON null element maps to SQL NULL for
        // a nullable element type, or fails the cast for a NOT NULL one.
        final CastCodeBlock elementCast =
                CastRuleProvider.generateAlwaysNonNullCodeBlock(
                        context, elementTerm, inputLogicalType, elementType);

        return new CastRuleUtils.CodeWriter()
                .declStmt(
                        int.class,
                        sizeTerm,
                        staticCall(
                                VariantCastUtils.class,
                                "arraySize",
                                inputTerm,
                                strLiteral(targetLogicalType.asSummaryString())))
                .declStmt(elementTypeTerm + "[]", arrayTerm, newArray(elementTypeTerm, sizeTerm))
                .forStmt(
                        sizeTerm,
                        (index, loopWriter) -> {
                            loopWriter.declStmt(
                                    Variant.class,
                                    elementTerm,
                                    methodCall(inputTerm, "getElement", index));
                            final String isPresent = "!" + methodCall(elementTerm, "isNull");
                            if (elementType.isNullable()) {
                                loopWriter.ifStmt(
                                        isPresent,
                                        thenWriter ->
                                                thenWriter
                                                        .append(elementCast)
                                                        .assignArrayStmt(
                                                                arrayTerm,
                                                                index,
                                                                elementCast.getReturnTerm()));
                            } else {
                                loopWriter.ifStmt(
                                        isPresent,
                                        thenWriter ->
                                                thenWriter
                                                        .append(elementCast)
                                                        .assignArrayStmt(
                                                                arrayTerm,
                                                                index,
                                                                elementCast.getReturnTerm()),
                                        elseWriter ->
                                                elseWriter.throwStmt(
                                                        "new org.apache.flink.table.api.TableRuntimeException(\"Cannot cast a VARIANT null array element to a NOT NULL element type.\")"));
                            }
                        },
                        context.getCodeGeneratorContext())
                .assignStmt(returnVariable, constructorCall(GenericArrayData.class, arrayTerm))
                .toString();
    }

    private static String arrayElementType(LogicalType elementType) {
        if (elementType.isNullable()) {
            return CodeGenUtils.boxedTypeTermForType(elementType);
        }
        return CodeGenUtils.primitiveTypeTermForType(elementType);
    }
}
