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

import org.apache.flink.table.runtime.functions.UuidCastUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.staticCall;

/**
 * {@link LogicalTypeFamily#BINARY_STRING} to {@link LogicalTypeRoot#UUID} cast rule, the inverse of
 * {@link UuidToBinaryCastRule}.
 *
 * <p>Reinterprets the bytes as a UUID via {@link UuidCastUtils#fromBytes(byte[])}, which requires
 * exactly 16 bytes. A different length fails {@code CAST}; since the rule {@link #canFail}, the
 * framework wraps the call in a {@code try/catch} that yields {@code null} for {@code TRY_CAST}.
 * Example generated code:
 *
 * <pre>
 * result$0 = UuidCastUtils.fromBytes(bytes$0);
 * </pre>
 */
class BinaryToUuidCastRule extends AbstractExpressionCodeGeneratorCastRule<byte[], byte[]> {

    static final BinaryToUuidCastRule INSTANCE = new BinaryToUuidCastRule();

    private BinaryToUuidCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeFamily.BINARY_STRING)
                        .target(LogicalTypeRoot.UUID)
                        .build());
    }

    @Override
    public boolean canFail(LogicalType inputLogicalType, LogicalType targetLogicalType) {
        return true;
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        return staticCall(UuidCastUtils.class, "fromBytes", inputTerm);
    }
}
