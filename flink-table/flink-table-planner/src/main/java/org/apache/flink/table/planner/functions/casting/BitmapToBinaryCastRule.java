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

import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.bitmap.Bitmap;

import static org.apache.flink.table.planner.functions.casting.BinaryToBinaryCastRule.couldPad;
import static org.apache.flink.table.planner.functions.casting.BinaryToBinaryCastRule.couldTrim;
import static org.apache.flink.table.planner.functions.casting.BinaryToBinaryCastRule.trimOrPadByteArray;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.arrayLength;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;

/** {@link LogicalTypeRoot#BITMAP} to {@link LogicalTypeFamily#BINARY_STRING} cast rule. */
class BitmapToBinaryCastRule extends AbstractNullAwareCodeGeneratorCastRule<Bitmap, byte[]> {

    static final BitmapToBinaryCastRule INSTANCE = new BitmapToBinaryCastRule();

    private BitmapToBinaryCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.BITMAP)
                        .target(LogicalTypeFamily.BINARY_STRING)
                        .build());
    }

    @Override
    protected String generateCodeBlockInternal(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            String returnVariable,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final int targetLength = LogicalTypeChecks.getLength(targetLogicalType);
        final String byteArrayTerm =
                CodeGenUtils.newName(context.getCodeGeneratorContext(), "bitmapBytes");

        if (context.legacyBehaviour()
                || !(couldTrim(targetLength) || couldPad(targetLogicalType, targetLength))) {
            return new CastRuleUtils.CodeWriter()
                    .assignStmt(returnVariable, methodCall(inputTerm, "toBytes"))
                    .toString();
        } else {
            return new CastRuleUtils.CodeWriter()
                    .declStmt(byte[].class, byteArrayTerm, methodCall(inputTerm, "toBytes"))
                    .ifStmt(
                            arrayLength(byteArrayTerm) + " <= " + targetLength,
                            thenWriter -> {
                                if (couldPad(targetLogicalType, targetLength)) {
                                    trimOrPadByteArray(
                                            returnVariable,
                                            targetLength,
                                            byteArrayTerm,
                                            thenWriter);
                                } else {
                                    thenWriter.assignStmt(returnVariable, byteArrayTerm);
                                }
                            },
                            elseWriter ->
                                    trimOrPadByteArray(
                                            returnVariable,
                                            targetLength,
                                            byteArrayTerm,
                                            elseWriter))
                    .toString();
        }
    }
}
