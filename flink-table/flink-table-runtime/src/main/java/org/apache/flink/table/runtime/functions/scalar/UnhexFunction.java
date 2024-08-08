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
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction.SpecializedContext;

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#UNHEX}. */
@Internal
public class UnhexFunction extends BuiltInScalarFunction {

    public UnhexFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.UNHEX, context);
    }

    public @Nullable byte[] eval(@Nullable StringData expr) {
        if (expr == null) {
            return null;
        }
        return unhex(expr.toBytes());
    }

    /** Inspired from {@link org.apache.flink.table.utils.EncodingUtils#decodeHex(String)}. */
    public static byte[] unhex(final byte[] bytes) {
        final byte[] out = new byte[(bytes.length + 1) >> 1];

        int i = bytes.length - 2;
        int j = out.length - 1;
        while (i >= 0) {
            int l = Character.digit(bytes[i], 16);
            int r = Character.digit(bytes[i + 1], 16);
            if (l == -1 || r == -1) {
                return null;
            }
            i -= 2;
            out[j--] = (byte) (((l << 4) | r) & 0xFF);
        }

        // length is odd and first byte is invalid
        if (i == -1 && Character.digit(bytes[0], 16) == -1) {
            return null;
        }

        return out;
    }
}
