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
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.table.runtime.functions.BaseConversionUtils;

import java.nio.charset.StandardCharsets;

/** Implementation of {@link BuiltInFunctionDefinitions#CONV}. */
@Internal
public class ConvFunction extends BuiltInScalarFunction {

    public ConvFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.CONV, context);
    }

    public static StringData eval(StringData input, Number fromBase, Number toBase) {
        if (input == null || fromBase == null || toBase == null) {
            return null;
        }
        byte[] bytes =
                BaseConversionUtils.conv(input.toBytes(), fromBase.intValue(), toBase.intValue());
        // fromBase cannot be negative, orElse it will return null.
        return bytes == null ? null : StringData.fromBytes(bytes);
    }

    public static StringData eval(Number input, Number fromBase, Number toBase) {
        if (input == null || fromBase == null || toBase == null) {
            return null;
        }
        byte[] bytes =
                BaseConversionUtils.conv(
                        String.valueOf(input.longValue()).getBytes(StandardCharsets.UTF_8),
                        fromBase.intValue(),
                        toBase.intValue());
        // fromBase cannot be negative, orElse it will return null.
        return bytes == null ? null : StringData.fromBytes(bytes);
    }
}
