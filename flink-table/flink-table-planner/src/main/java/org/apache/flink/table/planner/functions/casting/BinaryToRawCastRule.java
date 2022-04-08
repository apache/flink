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

import org.apache.flink.table.data.binary.BinaryRawValueData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

/** {@link LogicalTypeFamily#BINARY_STRING} to {@link LogicalTypeRoot#RAW} to cast rule. */
class BinaryToRawCastRule extends AbstractNullAwareCodeGeneratorCastRule<byte[], Object> {

    static final BinaryToRawCastRule INSTANCE = new BinaryToRawCastRule();

    private BinaryToRawCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.BINARY_STRING)
                        .target(LogicalTypeRoot.RAW)
                        .build());
    }

    @Override
    public boolean canFail(LogicalType inputLogicalType, LogicalType targetLogicalType) {
        return true;
    }

    /* Example generated code for BINARY(3):

    isNull$0 = _myInputIsNull;
    if (!isNull$0) {
        result$1 = org.apache.flink.table.data.binary.BinaryRawValueData.fromBytes(_myInput);
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
        // Get serializer for RAW type
        return new CastRuleUtils.CodeWriter()
                .assignStmt(
                        returnVariable,
                        staticCall(BinaryRawValueData.class, "fromBytes", inputTerm))
                .toString();
    }
}
