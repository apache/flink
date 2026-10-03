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

import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.runtime.functions.VariantCastUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.variant.Variant;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.boxedTypeTermForType;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.className;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.cast;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.strLiteral;

/**
 * {@link LogicalTypeRoot#VARIANT} to {@link LogicalTypeRoot#MAP} cast rule.
 *
 * <p>The variant must be an object. Each field name becomes a key and each field value casts to the
 * target value type by the full {@code VARIANT}-to-value rule, recursively. The key type must be a
 * character string, because a variant object's keys are always strings; a non-string key type is
 * rejected at validation. This is the only way to read an object whose keys are dynamic or unknown
 * at query time, and {@code MAP<STRING, VARIANT>} is the schemaless read of an object.
 *
 * <p>A {@link LogicalTypeRoot#VARIANT} value type is the identity, so a VARIANT null value is kept
 * as a variant null rather than downgraded to SQL {@code NULL}, matching {@code ARRAY<VARIANT>} and
 * a {@code ROW} with {@code VARIANT} fields.
 */
class VariantToMapCastRule extends AbstractVariantToConstructedCastRule<MapData> {

    static final VariantToMapCastRule INSTANCE = new VariantToMapCastRule();

    private VariantToMapCastRule() {
        super(
                CastRulePredicate.builder()
                        .predicate(
                                (input, target) ->
                                        input.is(LogicalTypeRoot.VARIANT)
                                                && target.is(LogicalTypeRoot.MAP)
                                                && ((MapType) target)
                                                        .getKeyType()
                                                        .is(LogicalTypeFamily.CHARACTER_STRING)
                                                && CastRuleProvider.resolve(
                                                                input,
                                                                ((MapType) target).getValueType())
                                                        != null)
                        .build());
    }

    /* Example generated code for MAP<`STRING`, `INT`>. Each field name becomes a key and each value
    runs the leaf cast; a nullable value that is a VARIANT null is left as SQL NULL:

    org.apache.flink.table.runtime.functions.VariantCastUtils.requireObject(
            variant$2, "MAP<STRING, INT>");
    java.util.List fieldNames$4 = variant$2.getFieldNames();
    int size$5 = fieldNames$4.size();
    java.util.Map map$6 = new java.util.HashMap(size$5);
    for (int i = 0; i < size$5; i++) {
        java.lang.String name$7 = (java.lang.String) fieldNames$4.get(i);
        org.apache.flink.types.variant.Variant valueVariant$8 = variant$2.getField(name$7);
        java.lang.Integer value$9 = null;
        if (!valueVariant$8.isNull()) {
            result$10 =
                    ((int) org.apache.flink.table.runtime.functions.VariantCastUtils.toIntegral(
                            valueVariant$8, -2147483648L, 2147483647L, "INTEGER"));
            value$9 = result$10;
        }
        map$6.put(
                org.apache.flink.table.runtime.functions.VariantCastUtils.variantKey(
                        name$7, 2147483647, false),
                value$9);
    }
    result$3 = new org.apache.flink.table.data.GenericMapData(map$6);

    A NOT NULL value type throws instead of leaving value$9 null for a VARIANT null value. A VARIANT
    value type takes the identity cast unconditionally, so a VARIANT null is kept as a variant null.

    */
    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final MapType mapType = (MapType) targetLogicalType;
        final LogicalType keyType = mapType.getKeyType();
        final LogicalType valueType = mapType.getValueType();
        final CodeGeneratorContext codeGeneratorContext = context.getCodeGeneratorContext();

        final int keyLength = LogicalTypeChecks.getLength(keyType);
        final boolean charKey = keyType.is(LogicalTypeRoot.CHAR);
        final String valueTypeTerm = boxedTypeTermForType(valueType);

        final String namesTerm = newName(codeGeneratorContext, "fieldNames");
        final String sizeTerm = newName(codeGeneratorContext, "size");
        final String mapTerm = newName(codeGeneratorContext, "map");
        final String nameTerm = newName(codeGeneratorContext, "name");
        final String valueVariantTerm = newName(codeGeneratorContext, "valueVariant");
        final String valueTerm = newName(codeGeneratorContext, "value");

        // The value is guaranteed non-null here, since a VARIANT null is handled below, so the
        // inner
        // cast is the plain VARIANT-to-value rule. A VARIANT null value maps to SQL NULL for a
        // nullable value type, or fails the cast for a NOT NULL one.
        final CastCodeBlock valueCast =
                CastRuleProvider.generateAlwaysNonNullCodeBlock(
                        context, valueVariantTerm, inputLogicalType, valueType);
        final String putKey =
                staticCall(
                        VariantCastUtils.class,
                        "variantKey",
                        nameTerm,
                        String.valueOf(keyLength),
                        charKey);

        return new CastRuleUtils.CodeWriter()
                .stmt(
                        staticCall(
                                VariantCastUtils.class,
                                "requireObject",
                                inputTerm,
                                strLiteral(targetLogicalType.asSummaryString())))
                .declStmt(className(List.class), namesTerm, methodCall(inputTerm, "getFieldNames"))
                .declStmt(int.class, sizeTerm, methodCall(namesTerm, "size"))
                .declStmt(className(Map.class), mapTerm, constructorCall(HashMap.class, sizeTerm))
                .forStmt(
                        sizeTerm,
                        (index, loopWriter) -> {
                            loopWriter
                                    .declStmt(
                                            String.class,
                                            nameTerm,
                                            cast(
                                                    className(String.class),
                                                    methodCall(namesTerm, "get", index)))
                                    .declStmt(
                                            Variant.class,
                                            valueVariantTerm,
                                            methodCall(inputTerm, "getField", nameTerm))
                                    .declStmt(valueTypeTerm, valueTerm, "null");
                            final String isPresent = "!" + methodCall(valueVariantTerm, "isNull");
                            if (valueType.is(LogicalTypeRoot.VARIANT)) {
                                // The value cast is the identity, so a present VARIANT null value
                                // is
                                // a valid variant null and is kept as-is rather than downgraded to
                                // SQL NULL, matching ARRAY<VARIANT> and ROW<VARIANT>.
                                loopWriter
                                        .append(valueCast)
                                        .assignStmt(valueTerm, valueCast.getReturnTerm());
                            } else if (valueType.isNullable()) {
                                loopWriter.ifStmt(
                                        isPresent,
                                        thenWriter ->
                                                thenWriter
                                                        .append(valueCast)
                                                        .assignStmt(
                                                                valueTerm,
                                                                valueCast.getReturnTerm()));
                            } else {
                                loopWriter.ifStmt(
                                        isPresent,
                                        thenWriter ->
                                                thenWriter
                                                        .append(valueCast)
                                                        .assignStmt(
                                                                valueTerm,
                                                                valueCast.getReturnTerm()),
                                        elseWriter ->
                                                elseWriter.throwStmt(
                                                        "new org.apache.flink.table.api.TableRuntimeException(\"Cannot cast a VARIANT null object value to a NOT NULL map value type.\")"));
                            }
                            loopWriter.stmt(methodCall(mapTerm, "put", putKey, valueTerm));
                        },
                        codeGeneratorContext)
                .assignStmt(returnVariable, constructorCall(GenericMapData.class, mapTerm))
                .toString();
    }
}
