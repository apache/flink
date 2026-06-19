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
import org.apache.flink.types.bitmap.Bitmap;

import static org.apache.flink.table.planner.functions.casting.CastRuleUtils.methodCall;

/**
 * {@link LogicalTypeRoot#BITMAP} to BYTES cast rule.
 *
 * <p>Only casting to BYTES (unbounded VARBINARY) is supported. Casting to BINARY(n) or VARBINARY(n)
 * is intentionally disabled because trimming or padding would corrupt the serialized bitmap data.
 */
class BitmapToBinaryCastRule extends AbstractExpressionCodeGeneratorCastRule<Bitmap, byte[]> {

    static final BitmapToBinaryCastRule INSTANCE = new BitmapToBinaryCastRule();

    private BitmapToBinaryCastRule() {
        super(
                CastRulePredicate.builder()
                        .input(LogicalTypeRoot.BITMAP)
                        .target(VarBinaryType.BYTES_TYPE)
                        .build());
    }

    @Override
    public String generateExpression(
            CodeGeneratorCastRule.Context context,
            String inputTerm,
            LogicalType inputLogicalType,
            LogicalType targetLogicalType) {
        return methodCall(inputTerm, "toBytes");
    }
}
