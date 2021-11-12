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

import static org.apache.flink.table.codesplit.CodeSplitUtil.newName;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.BINARY_STRING_DATA_FROM_STRING;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;

/** {@link LogicalTypeRoot#RAW} to {@link LogicalTypeFamily#CHARACTER_STRING} cast rule. */
class RawToStringCastRule extends AbstractNullAwareCodeGeneratorCastRule<Object, String> {

    static final RawToStringCastRule INSTANCE = new RawToStringCastRule();

    private RawToStringCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.RAW)
                        .target(LogicalTypeFamily.CHARACTER_STRING)
                        .build());
    }

    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final String typeSerializer = context.declareTypeSerializer(inputLogicalType);
        final String deserializedObjTerm = newName("deserializedObj");

        return new CastRuleUtils.CodeWriter()
                .declStmt(
                        Object.class,
                        deserializedObjTerm,
                        methodCall(inputTerm, "toObject", typeSerializer))
                .ifStmt(
                        deserializedObjTerm + " != null",
                        thenWriter ->
                                thenWriter.assignStmt(
                                        returnVariable,
                                        CastRuleUtils.staticCall(
                                                BINARY_STRING_DATA_FROM_STRING(),
                                                methodCall(deserializedObjTerm, "toString"))),
                        elseWriter -> elseWriter.assignStmt(returnVariable, "null"))
                .toString();
    }
}
