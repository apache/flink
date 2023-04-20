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
        java.util.Map map$838 = new java.util.HashMap();
        for (int i$841 = 0; i$841 < _myInput.size(); i$841++) {
            java.lang.Float key$839 = null;
            java.lang.Integer value$840 = null;
            if (!_myInput.keyArray().isNullAt(i$841)) {
                result$2 = ((float)(_myInput.keyArray().getInt(i$841)));
                key$839 = result$2;
            }
            value$840 = _myInput.valueArray().getInt(i$841);
            map$838.put(key$839, value$840);
        }
        result$1 = new org.apache.flink.table.data.GenericMapData(map$838);
        isNull$0 = result$1 == null;
    } else {
        result$1 = null;
    }
    return result$1;

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

        final String innerTargetKeyTypeTerm = boxedTypeTermForType(innerTargetKeyType);
        final String innerTargetValueTypeTerm = boxedTypeTermForType(innerTargetValueType);
        final String keyArrayTerm = methodCall(inputTerm, "keyArray");
        final String valueArrayTerm = methodCall(inputTerm, "valueArray");
        final String size = methodCall(inputTerm, "size");
        final String map = newName("map");
        final String key = newName("key");
        final String value = newName("value");

        return new CastRuleUtils.CodeWriter()
                .declStmt(className(Map.class), map, constructorCall(HashMap.class))
                .forStmt(
                        size,
                        (index, codeWriter) -> {
                            final CastCodeBlock keyCodeBlock =
                                    CastRuleProvider.generateAlwaysNonNullCodeBlock(
                                            context,
                                            rowFieldReadAccess(
                                                    index, keyArrayTerm, innerInputKeyType),
                                            innerInputKeyType,
                                            innerTargetKeyType);
                            assert keyCodeBlock != null;

                            final CastCodeBlock valueCodeBlock =
                                    CastRuleProvider.generateAlwaysNonNullCodeBlock(
                                            context,
                                            rowFieldReadAccess(
                                                    index, valueArrayTerm, innerInputValueType),
                                            innerInputValueType,
                                            innerTargetValueType);
                            assert valueCodeBlock != null;

                            codeWriter
                                    .declStmt(innerTargetKeyTypeTerm, key, null)
                                    .declStmt(innerTargetValueTypeTerm, value, null);
                            if (innerTargetKeyType.isNullable()) {
                                codeWriter.ifStmt(
                                        "!" + methodCall(keyArrayTerm, "isNullAt", index),
                                        thenWriter ->
                                                thenWriter
                                                        .append(keyCodeBlock)
                                                        .assignStmt(
                                                                key, keyCodeBlock.getReturnTerm()));
                            } else {
                                codeWriter
                                        .append(keyCodeBlock)
                                        .assignStmt(key, keyCodeBlock.getReturnTerm());
                            }

                            if (inputLogicalType.is(LogicalTypeRoot.MAP)
                                    && innerTargetValueType.isNullable()) {
                                codeWriter.ifStmt(
                                        "!" + methodCall(valueArrayTerm, "isNullAt", index),
                                        thenWriter ->
                                                thenWriter
                                                        .append(valueCodeBlock)
                                                        .assignStmt(
                                                                value,
                                                                valueCodeBlock.getReturnTerm()));
                            } else {
                                codeWriter
                                        .append(valueCodeBlock)
                                        .assignStmt(value, valueCodeBlock.getReturnTerm());
                            }
                            codeWriter.stmt(methodCall(map, "put", key, value));
                        })
                .assignStmt(returnVariable, constructorCall(GenericMapData.class, map))
                .toString();
    }
}
