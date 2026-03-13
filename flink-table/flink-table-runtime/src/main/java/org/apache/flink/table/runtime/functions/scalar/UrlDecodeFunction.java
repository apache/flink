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
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;

import javax.annotation.Nullable;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;

/**
 * Implementation of {@link BuiltInFunctionDefinitions#URL_DECODE} and {@link
 * BuiltInFunctionDefinitions#URL_DECODE_RECURSIVE}.
 */
@Internal
public class UrlDecodeFunction extends BuiltInScalarFunction {

    private static final int DEFAULT_MAX_DECODE_DEPTH = 10;

    private final boolean recursive;

    public UrlDecodeFunction(SpecializedFunction.SpecializedContext context) {
        super(resolveDefinition(context), context);
        this.recursive =
                context.getCallContext().getFunctionDefinition()
                        == BuiltInFunctionDefinitions.URL_DECODE_RECURSIVE;
    }

    private static BuiltInFunctionDefinition resolveDefinition(
            SpecializedFunction.SpecializedContext context) {
        if (context.getCallContext().getFunctionDefinition()
                == BuiltInFunctionDefinitions.URL_DECODE_RECURSIVE) {
            return BuiltInFunctionDefinitions.URL_DECODE_RECURSIVE;
        }
        return BuiltInFunctionDefinitions.URL_DECODE;
    }

    public @Nullable StringData eval(StringData value) {
        if (value == null) {
            return null;
        }
        if (!recursive) {
            try {
                return StringData.fromString(
                        URLDecoder.decode(value.toString(), StandardCharsets.UTF_8));
            } catch (RuntimeException e) {
                return null;
            }
        }

        // Recursive with default max depth
        return eval(value, DEFAULT_MAX_DECODE_DEPTH);
    }

    public @Nullable StringData eval(StringData value, Integer maxDepth) {
        if (value == null) {
            return null;
        }
        if (maxDepth == null || maxDepth <= 0) {
            maxDepth = DEFAULT_MAX_DECODE_DEPTH;
        }

        String currentValue = value.toString();
        String previousValue = currentValue;
        int iteration = 0;

        try {
            while (iteration < maxDepth) {
                previousValue = currentValue;
                currentValue = URLDecoder.decode(currentValue, StandardCharsets.UTF_8);
                iteration++;
                if (currentValue.equals(previousValue)) {
                    break;
                }
            }
            return StringData.fromString(currentValue);
        } catch (RuntimeException e) {
            // If we successfully decoded at least once, return the last successful result
            if (iteration > 0) {
                return StringData.fromString(previousValue);
            }
            return null;
        }
    }
}
