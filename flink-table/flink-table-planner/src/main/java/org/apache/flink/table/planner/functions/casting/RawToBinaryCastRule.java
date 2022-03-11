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
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import static org.apache.flink.table.codesplit.CodeSplitUtil.newName;
import static org.apache.flink.table.planner.functions.casting.BinaryToBinaryCastRule.couldPad;
import static org.apache.flink.table.planner.functions.casting.BinaryToBinaryCastRule.couldTrim;
import static org.apache.flink.table.planner.functions.casting.BinaryToBinaryCastRule.trimOrPadByteArray;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.arrayLength;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;

/** {@link LogicalTypeRoot#RAW} to {@link LogicalTypeFamily#BINARY_STRING} cast rule. */
class RawToBinaryCastRule extends AbstractNullAwareCodeGeneratorCastRule<Object, byte[]> {

    static final RawToBinaryCastRule INSTANCE = new RawToBinaryCastRule();

    private RawToBinaryCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.RAW)
                        .target(LogicalTypeFamily.BINARY_STRING)
                        .build());
    }

    @Override
    public boolean canFail(LogicalType inputLogicalType, LogicalType targetLogicalType) {
        return true;
    }

    /* Example generated code for BINARY(3):

    // legacy behavior
    isNull$290 = isNull$289;
    if (!isNull$290) {
        result$291 = result$289.toBytes(typeSerializer$292);
        isNull$290 = result$291 == null;
    } else {
        result$291 = null;
    }

    // new behavior
    isNull$290 = isNull$289;
    if (!isNull$290) {
        byte[] deserializedByteArray$76 = result$289.toBytes(typeSerializer$292);
        if (deserializedByteArray$76.length <= 3) {
            result$291 = deserializedByteArray$76;
        } else {
            result$291 = java.util.Arrays.copyOf(deserializedByteArray$76, 3);
        }
        isNull$290 = result$291 == null;
    } else {
        result$291 = null;
    }

    */

    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final int targetLength = LogicalTypeChecks.getLength(targetLogicalType);

        // Get serializer for RAW type
        final String typeSerializer = context.declareTypeSerializer(inputLogicalType);
        final String deserializedByteArrayTerm = newName("deserializedByteArray");

        if (context.legacyBehaviour()
                || !(couldTrim(targetLength) || (couldPad(targetLogicalType, targetLength)))) {
            return new CastRuleUtils.CodeWriter()
                    .assignStmt(returnVariable, methodCall(inputTerm, "toBytes", typeSerializer))
                    .toString();
        } else {
            return new CastRuleUtils.CodeWriter()
                    .declStmt(
                            byte[].class,
                            deserializedByteArrayTerm,
                            methodCall(inputTerm, "toBytes", typeSerializer))
                    .ifStmt(
                            arrayLength(deserializedByteArrayTerm) + " <= " + targetLength,
                            thenWriter -> {
                                if (couldPad(targetLogicalType, targetLength)) {
                                    trimOrPadByteArray(
                                            returnVariable,
                                            targetLength,
                                            deserializedByteArrayTerm,
                                            thenWriter);
                                } else {
                                    thenWriter.assignStmt(
                                            returnVariable, deserializedByteArrayTerm);
                                }
                            },
                            elseWriter ->
                                    trimOrPadByteArray(
                                            returnVariable,
                                            targetLength,
                                            deserializedByteArrayTerm,
                                            elseWriter))
                    .toString();
        }
    }
}
