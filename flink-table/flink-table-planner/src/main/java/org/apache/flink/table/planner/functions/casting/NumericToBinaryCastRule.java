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

import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.planner.codegen.CodeGenUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import static org.apache.flink.table.planner.codegen.CodeGenUtils.newName;
import static org.apache.flink.table.planner.functions.casting.BinaryToBinaryCastRule.couldPad;
import static org.apache.flink.table.planner.functions.casting.BinaryToBinaryCastRule.trimOrPadByteArray;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.EMPTY_STR_LITERAL;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.arrayLength;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.constructorCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.stringConcat;

/** {@link LogicalTypeFamily#NUMERIC} to {@link LogicalTypeFamily#CHARACTER_STRING} cast rule. */
public class NumericToBinaryCastRule
        extends AbstractNullAwareCodeGeneratorCastRule<Number, byte[]> {

    static final NumericToBinaryCastRule INSTANCE = new NumericToBinaryCastRule();

    protected NumericToBinaryCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.INTEGER_NUMERIC)
                        .input(LogicalTypeFamily.APPROXIMATE_NUMERIC)
                        .input(LogicalTypeRoot.DECIMAL)
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
        final CastRuleUtils.CodeWriter writer = new CastRuleUtils.CodeWriter();
        final String resultStringTerm = newName("resultString");
        final String inputstr = stringConcat(EMPTY_STR_LITERAL, inputTerm);
        writer.declStmt(BinaryStringData.class, resultStringTerm);
        writer.assignStmt(resultStringTerm, constructorCall(BinaryStringData.class, inputstr));

        if (context.legacyBehaviour()) {
            return writer.assignStmt(returnVariable, methodCall(resultStringTerm, "toBytes"))
                    .toString();
        } else {
            final int targetLength = LogicalTypeChecks.getLength(targetLogicalType);
            final String byteArrayTerm = CodeGenUtils.newName("byteArrayTerm");

            return writer.declStmt(
                            byte[].class, byteArrayTerm, methodCall(resultStringTerm, "toBytes"))
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
