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

import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.types.variant.Variant;

import javax.annotation.Nullable;

/** Implementation of {@link BuiltInFunctionDefinitions#PARSE_JSON}. */
public class ParseJsonFunction extends BuiltInScalarFunction {

    public ParseJsonFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.PARSE_JSON, context);
    }

    public @Nullable Variant eval(@Nullable StringData jsonStr) {
        return eval(jsonStr, false);
    }

    public @Nullable Variant eval(@Nullable StringData jsonStr, boolean allowDuplicateKeys) {
        if (jsonStr == null) {
            return null;
        }

        try {
            return BinaryVariantInternalBuilder.parseJson(jsonStr.toString(), allowDuplicateKeys);
        } catch (Throwable e) {
            throw new TableRuntimeException(
                    String.format("Failed to parse json string: %s", jsonStr), e);
        }
    }
}
