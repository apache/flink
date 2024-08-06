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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.BinaryStringDataUtil;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#ENDS_WITH}. */
@Internal
public class EndsWithFunction extends BuiltInScalarFunction {

    public EndsWithFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.ENDS_WITH, context);
    }

    public @Nullable Boolean eval(@Nullable StringData expr, @Nullable StringData endExpr) {
        if (expr == null || endExpr == null) {
            return null;
        }
        if (BinaryStringDataUtil.isEmpty((BinaryStringData) endExpr)) {
            return true;
        }
        return ((BinaryStringData) expr).endsWith((BinaryStringData) endExpr);
    }

    public @Nullable Boolean eval(@Nullable byte[] expr, @Nullable byte[] endExpr) {
        if (expr == null || endExpr == null) {
            return null;
        }
        if (endExpr.length == 0) {
            return true;
        }
        return matchAtEnd(expr, endExpr);
    }

    private static boolean matchAtEnd(byte[] source, byte[] target) {
        int start = source.length - target.length;
        if (start < 0) {
            return false;
        }
        for (int i = start; i < source.length; i++) {
            if (source[i] != target[i - start]) {
                return false;
            }
        }
        return true;
    }
}
