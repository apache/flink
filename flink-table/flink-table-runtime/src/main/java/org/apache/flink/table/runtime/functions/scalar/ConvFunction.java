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
import org.apache.flink.table.runtime.functions.BaseConversionUtils;

import javax.annotation.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;

/** Implementation of {@link BuiltInFunctionDefinitions#CONV}. */
@Internal
public class ConvFunction extends BuiltInScalarFunction {

    public ConvFunction(SpecializedContext context) {
        super(BuiltInFunctionDefinitions.CONV, context);
    }

    public @Nullable StringData eval(
            @Nullable Number num, @Nullable Number fromBase, @Nullable Number toBase) {
        if (num == null || fromBase == null || toBase == null) {
            return null;
        }
        return BinaryStringData.fromString(
                BaseConversionUtils.conv(
                        String.valueOf(num.longValue()).getBytes(UTF_8),
                        fromBase.longValue(),
                        toBase.longValue()));
    }

    public @Nullable StringData eval(
            @Nullable StringData num, @Nullable Number fromBase, @Nullable Number toBase) {
        if (num == null || fromBase == null || toBase == null) {
            return null;
        }
        StringData trimNum =
                BinaryStringDataUtil.trim((BinaryStringData) num, BinaryStringData.fromString(" "));
        return BinaryStringData.fromString(
                BaseConversionUtils.conv(
                        trimNum.toBytes(), fromBase.longValue(), toBase.longValue()));
    }
}
