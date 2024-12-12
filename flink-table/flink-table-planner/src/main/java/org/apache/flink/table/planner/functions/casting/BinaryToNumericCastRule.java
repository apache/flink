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
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;

import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.STRING_DATA_TO_BYTE;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.STRING_DATA_TO_DOUBLE;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.STRING_DATA_TO_FLOAT;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.STRING_DATA_TO_INT;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.STRING_DATA_TO_LONG;
import static org.apache.flink.table.planner.codegen.calls.BuiltInMethods.STRING_DATA_TO_SHORT;
import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

/** {@link LogicalTypeFamily#BINARY_STRING} to {@link LogicalTypeFamily#NUMERIC} cast rule. */
class BinaryToNumericCastRule extends AbstractExpressionCodeGeneratorCastRule<byte[], Number> {

    static final BinaryToNumericCastRule INSTANCE = new BinaryToNumericCastRule();

    protected BinaryToNumericCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.BINARY_STRING)
                        .target(LogicalTypeFamily.INTEGER_NUMERIC)
                        .target(LogicalTypeFamily.APPROXIMATE_NUMERIC)
                        .build());
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        final String inputstr = staticCall(BinaryStringData.class, "fromBytes", inputTerm);
        switch (targetLogicalType.getTypeRoot()) {
            case TINYINT:
                return staticCall(STRING_DATA_TO_BYTE(), inputstr);
            case SMALLINT:
                return staticCall(STRING_DATA_TO_SHORT(), inputstr);
            case INTEGER:
                return staticCall(STRING_DATA_TO_INT(), inputstr);
            case BIGINT:
                return staticCall(STRING_DATA_TO_LONG(), inputstr);
            case FLOAT:
                return staticCall(STRING_DATA_TO_FLOAT(), inputstr);
            case DOUBLE:
                return staticCall(STRING_DATA_TO_DOUBLE(), inputstr);
        }
        throw new IllegalArgumentException("This is a bug. Please file an issue.");
    }

    @Override
    public boolean canFail(LogicalType inputLogicalType, LogicalType targetLogicalType) {
        return super.canFail(inputLogicalType, targetLogicalType);
    }
}
