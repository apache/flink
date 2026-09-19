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
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.boxedTypeTermForType;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.className;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.codegen.CodeGenUtils.rowFieldReadAccess;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;

/**
 * {@link LogicalTypeRoot#MAP} to {@link LogicalTypeRoot#MAP} and {@link LogicalTypeRoot#MULTISET}
 * to {@link LogicalTypeRoot#MULTISET} cast rule.
 */
class MapToMapAndMultisetToMultisetCastRule
        extends AbstractNullAwareCodeGeneratorCastRule<MapData, MapData>
        implements ConstructedToConstructedCastRule<MapData, MapData> {

    static final MapToMapAndMultisetToMultisetCastRule INSTANCE =
            new MapToMapAndMultisetToMultisetCastRule();

    private MapToMapAndMultisetToMultisetCastRule() {
        super(
                CastRulePredicate.builder()
                        .predicate(
                                MapToMapAndMultisetToMultisetCastRule
                                        ::isValidMapToMapOrMultisetToMultisetCasting)
                        .build());
    }

    private static boolean isValidMapToMapOrMultisetToMultisetCasting(
            LogicalType input, LogicalType target) {
        return input.is(LogicalTypeRoot.MAP)
                        && target.is(LogicalTypeRoot.MAP)
                        && CastRuleProvider.resolve(
                                        ((MapType) input).getKeyType(),
                                        ((MapType) target).getKeyType())
                                != null
                        && CastRuleProvider.resolve(
                                        ((MapType) input).getValueType(),
                                        ((MapType) target).getValueType())
                                != null
                || input.is(LogicalTypeRoot.MULTISET)
                        && target.is(LogicalTypeRoot.MULTISET)
                        && CastRuleProvider.resolve(
                                        ((MultisetType) input).getElementType(),
                                        ((MultisetType) target).getElementType())
                                != null;
    }

    /* Example generated code for MULTISET<INT> -> MULTISET<FLOAT>:
    org.apache.flink.table.data.MapData _myInput = ((org.apache.flink.table.data.MapData)(_myInputObj));
    boolean _myInputIsNull = _myInputObj == null;
    boolean isNull$0;
    org.apache.flink.table.data.MapData result$1;
    float result$2;
    isNull$0 = _myInputIsNull;
    if (!isNull$0) {
        int size$2 = _myInput.size();
        org.apache.flink.table.data.ArrayData keyArray$0 = _myInput.keyArray();
        org.apache.flink.table.data.ArrayData valueArray$1 = _myInput.valueArray();
        java.util.Map map$3 = new java.util.HashMap(size$2);
        for (int i$6 = 0; i$6 < size$2; i$6++) {
            java.lang.Float key$4 = null;
            java.lang.Integer value$5 = null;
            if (!keyArray$0.isNullAt(i$6)) {
                result$2 = ((float)(keyArray$0.getInt(i$6)));
                key$4 = result$2;
            }
            if (!valueArray$1.isNullAt(i$6)) {
                value$5 = valueArray$1.getInt(i$6);
            }
            map$3.put(key$4, value$5);
        }
        result$1 = new org.apache.flink.table.data.GenericMapData(map$3);
        isNull$0 = result$1 == null;
    } else {
        result$1 = null;
    }

     */
    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final LogicalType innerInputKeyType;
        final LogicalType innerInputValueType;

        final LogicalType innerTargetKeyType;
        final LogicalType innerTargetValueType;
        if (inputLogicalType.is(LogicalTypeRoot.MULTISET)) {
            innerInputKeyType = ((MultisetType) inputLogicalType).getElementType();
            innerInputValueType = new IntType(false);
            innerTargetKeyType = ((MultisetType) targetLogicalType).getElementType();
            innerTargetValueType = new IntType(false);
        } else {
            innerInputKeyType = ((MapType) inputLogicalType).getKeyType();
            innerInputValueType = ((MapType) inputLogicalType).getValueType();
            innerTargetKeyType = ((MapType) targetLogicalType).getKeyType();
            innerTargetValueType = ((MapType) targetLogicalType).getValueType();
        }

        CodeGeneratorContext codeGeneratorContext = context.getCodeGeneratorContext();

        final String innerTargetKeyTypeTerm = boxedTypeTermForType(innerTargetKeyType);
        final String innerTargetValueTypeTerm = boxedTypeTermForType(innerTargetValueType);
        final String keyArray = newName(codeGeneratorContext, "keyArray");
        final String valueArray = newName(codeGeneratorContext, "valueArray");
        final String size = newName(codeGeneratorContext, "size");
        final String map = newName(codeGeneratorContext, "map");
        final String key = newName(codeGeneratorContext, "key");
        final String value = newName(codeGeneratorContext, "value");

        return new CastRuleUtils.CodeWriter()
                .declStmt(int.class, size, methodCall(inputTerm, "size"))
                .declStmt(ArrayData.class, keyArray, methodCall(inputTerm, "keyArray"))
                .declStmt(ArrayData.class, valueArray, methodCall(inputTerm, "valueArray"))
                .declStmt(className(Map.class), map, constructorCall(HashMap.class, size))
                .forStmt(
                        size,
                        (index, codeWriter) -> {
                            final CastCodeBlock keyCodeBlock =
                                    CastRuleProvider.generateAlwaysNonNullCodeBlock(
                                            context,
                                            rowFieldReadAccess(index, keyArray, innerInputKeyType),
                                            innerInputKeyType,
                                            innerTargetKeyType);
                            assert keyCodeBlock != null;

                            final CastCodeBlock valueCodeBlock =
                                    CastRuleProvider.generateAlwaysNonNullCodeBlock(
                                            context,
                                            rowFieldReadAccess(
                                                    index, valueArray, innerInputValueType),
                                            innerInputValueType,
                                            innerTargetValueType);
                            assert valueCodeBlock != null;

                            codeWriter
                                    .declStmt(innerTargetKeyTypeTerm, key, null)
                                    .declStmt(innerTargetValueTypeTerm, value, null);
                            iterateOverElements(
                                    index,
                                    codeWriter,
                                    keyArray,
                                    keyCodeBlock,
                                    key,
                                    !innerTargetKeyType.isNullable());

                            iterateOverElements(
                                    index,
                                    codeWriter,
                                    valueArray,
                                    valueCodeBlock,
                                    value,
                                    !inputLogicalType.is(LogicalTypeRoot.MAP)
                                            || !innerTargetValueType.isNullable());
                            codeWriter.stmt(methodCall(map, "put", key, value));
                        },
                        codeGeneratorContext)
                .assignStmt(returnVariable, constructorCall(GenericMapData.class, map))
                .toString();
    }

    private static void iterateOverElements(
            String index,
            CastRuleUtils.CodeWriter codeWriter,
            String keyArray,
            CastCodeBlock keyCodeBlock,
            String key,
            boolean throwIfNull) {
        if (throwIfNull) {
            codeWriter.ifStmt(
                    "!" + methodCall(keyArray, "isNullAt", index),
                    thenWriter ->
                            thenWriter
                                    .append(keyCodeBlock)
                                    .assignStmt(key, keyCodeBlock.getReturnTerm()),
                    elseWriter ->
                            elseWriter.throwStmt(
                                    "new org.apache.flink.table.api.TableRuntimeException(\"Target is not nullable but a NULL was found.\")"));
        } else {
            codeWriter.ifStmt(
                    "!" + methodCall(keyArray, "isNullAt", index),
                    thenWriter ->
                            thenWriter
                                    .append(keyCodeBlock)
                                    .assignStmt(key, keyCodeBlock.getReturnTerm()));
        }
    }
}
