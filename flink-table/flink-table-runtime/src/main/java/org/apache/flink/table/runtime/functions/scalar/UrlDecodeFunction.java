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
 *
 * <h3>Non-recursive URL Decode</h3>
 *
 * <p>Performs a single-pass URL decoding of the input string.
 *
 * <ul>
 *   <li>Input: URL-encoded string (e.g., "Hello%20World")
 *   <li>Output: Decoded string (e.g., "Hello World")
 *   <li>Returns NULL if decoding fails
 * </ul>
 *
 * <h3>Recursive URL Decode</h3>
 *
 * <p>Recursively decodes a URL-encoded string until the result stabilizes or the maximum depth is
 * reached. This is useful for handling multi-encoded URLs (e.g., double or triple encoded).
 *
 * <p><b>Behavior:</b>
 *
 * <ul>
 *   <li>Decoding stops when the output equals the input (stable) or maxDepth iterations are reached
 *   <li>If at least one successful decoding has been performed, returns the last successfully
 *       decoded result
 *   <li>Returns NULL if the input is NULL
 *   <li>Returns the input value if no successful decoding occurs (e.g., first iteration fails)
 * </ul>
 *
 * <p><b>Parameters:</b>
 *
 * <ul>
 *   <li><b>value</b>: The URL-encoded string to decode (can be NULL)
 *   <li><b>maxDepth</b>: Maximum number of decode iterations (default: 10, must be > 0, must be a
 *       literal integer)
 * </ul>
 *
 * <p><b>Examples:</b>
 *
 * <pre>{@code
 * // Simple decode
 * URL_DECODE("Hello%20World") = "Hello World"
 *
 * // Double-encoded with default maxDepth
 * URL_DECODE_RECURSIVE("Hello%2520World") = "Hello World"
 *
 * // Triple-encoded with maxDepth=2
 * URL_DECODE_RECURSIVE("Hello%252520World", 2) = "Hello%20World"
 *
 * // Triple-encoded with maxDepth=3
 * URL_DECODE_RECURSIVE("Hello%252520World", 3) = "Hello World"
 * }</pre>
 *
 * <p><b>Usage Scenarios:</b>
 *
 * <ul>
 *   <li>Use {@code URL_DECODE} for simple, single-pass URL decoding
 *   <li>Use {@code URL_DECODE_RECURSIVE} for handling multi-encoded URLs in web crawling, data
 *       extraction, or log analysis scenarios
 * </ul>
 */
@Internal
public class UrlDecodeFunction extends BuiltInScalarFunction {

    /**
     * Default maximum decode depth. The value of 10 is chosen as a safe upper bound that: - Handles
     * most practical multi-encoding scenarios (double/triple encoding) - Prevents infinite loops
     * from circular encodings - Limits performance impact while providing sufficient flexibility
     * Users can override this with a custom maxDepth value if needed.
     */
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
        return evalRecursive(value, maxDepth);
    }

    /** Overload for {@code TINYINT} literal, coerced at compile time. */
    public @Nullable StringData eval(StringData value, Byte maxDepth) {
        return evalRecursive(value, maxDepth == null ? null : maxDepth.intValue());
    }

    /** Overload for {@code SMALLINT} literal, coerced at compile time. */
    public @Nullable StringData eval(StringData value, Short maxDepth) {
        return evalRecursive(value, maxDepth == null ? null : maxDepth.intValue());
    }

    /** Overload for {@code BIGINT} literal, coerced at compile time. */
    public @Nullable StringData eval(StringData value, Long maxDepth) {
        return evalRecursive(value, maxDepth == null ? null : maxDepth.intValue());
    }

    private @Nullable StringData evalRecursive(
            @Nullable StringData value, @Nullable Integer maxDepth) {
        if (value == null) {
            return null;
        }
        // maxDepth is validated at planning time and guaranteed to be positive
        // No defensive check needed here
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
