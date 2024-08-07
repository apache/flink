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

/** Implementation of {@link BuiltInFunctionDefinitions#STARTS_WITH}. */
@Internal
public class StartsWithFunction extends BuiltInScalarFunction {

    public StartsWithFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.STARTS_WITH, context);
    }

    public @Nullable Boolean eval(@Nullable StringData expr, @Nullable StringData startExpr) {
        if (expr == null || startExpr == null) {
            return null;
        }
        if (BinaryStringDataUtil.isEmpty((BinaryStringData) startExpr)) {
            return true;
        }
        return ((BinaryStringData) expr).startsWith((BinaryStringData) startExpr);
    }

    public @Nullable Boolean eval(@Nullable byte[] expr, @Nullable byte[] startExpr) {
        if (expr == null || startExpr == null) {
            return null;
        }
        if (startExpr.length == 0) {
            return true;
        }
        return matchAtStart(expr, startExpr);
    }

    private static boolean matchAtStart(byte[] source, byte[] target) {
        if (source.length < target.length) {
            return false;
        }
        for (int i = 0; i < target.length; i++) {
            if (source[i] != target[i]) {
                return false;
            }
        }
        return true;
    }
}
