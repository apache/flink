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
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;

/**
 * {@link LogicalTypeRoot#UUID} to binary cast rule, the inverse of {@link BinaryToUuidCastRule}.
 *
 * <p>A UUID is its 16-byte big-endian encoding, so the internal {@code byte[]} is returned
 * unchanged. Only {@code BINARY(16)} and {@code BYTES} are supported targets; any other width would
 * pad or trim the value and thereby corrupt it. Example generated code:
 *
 * <pre>
 * result$0 = uuid$0;
 * </pre>
 */
class UuidToBinaryCastRule extends AbstractExpressionCodeGeneratorCastRule<byte[], byte[]> {

    static final UuidToBinaryCastRule INSTANCE = new UuidToBinaryCastRule();

    private UuidToBinaryCastRule() {
        super(
                CastRulePredicate.builder()
                        .predicate(
                                (input, target) ->
                                        input.is(LogicalTypeRoot.UUID) && isSupportedTarget(target))
                        .build());
    }

    private static boolean isSupportedTarget(LogicalType target) {
        if (target.is(LogicalTypeRoot.BINARY)) {
            return LogicalTypeChecks.getLength(target) == 16;
        }
        if (target.is(LogicalTypeRoot.VARBINARY)) {
            return LogicalTypeChecks.getLength(target) == VarBinaryType.MAX_LENGTH;
        }
        return false;
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        return inputTerm;
    }
}
