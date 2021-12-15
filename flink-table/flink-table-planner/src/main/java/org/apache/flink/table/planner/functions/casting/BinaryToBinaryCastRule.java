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
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

import java.util.Arrays;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.arrayLength;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.ternaryOperator;

/** {@link LogicalTypeFamily#BINARY_STRING} to {@link LogicalTypeFamily#BINARY_STRING} cast rule. */
class BinaryToBinaryCastRule extends AbstractExpressionCodeGeneratorCastRule<byte[], byte[]> {

    static final BinaryToBinaryCastRule INSTANCE = new BinaryToBinaryCastRule();

    private BinaryToBinaryCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.BINARY_STRING)
                        .target(LogicalTypeFamily.BINARY_STRING)
                        .build());
    }

    /* Example generated code for BINARY(2):

    // legacy behavior
    ((byte[])(inputValue))

    // new behavior
    ((((byte[])(inputValue)).length <= 2) ? (((byte[])(inputValue))) : (java.util.Arrays.copyOfRange(((byte[])(inputValue)), 0, 2)))

    */

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        int inputLength = LogicalTypeChecks.getLength(inputLogicalType);
        int targetLength = LogicalTypeChecks.getLength(targetLogicalType);

        if (context.legacyBehaviour()) {
            return inputTerm;
        } else {
            if (inputLength <= targetLength) {
                return inputTerm;
            } else {
                return ternaryOperator(
                        arrayLength(inputTerm) + " <= " + targetLength,
                        inputTerm,
                        staticCall(Arrays.class, "copyOfRange", inputTerm, 0, targetLength));
            }
        }
    }
}
