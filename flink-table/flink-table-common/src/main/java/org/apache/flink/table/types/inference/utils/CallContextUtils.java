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

package org.apache.flink.table.types.inference.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.TableSemantics;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.util.TimeUtils;

import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;

/** Helpers for reading and validating literal arguments of a {@link CallContext}. */
@Internal
public final class CallContextUtils {

    /**
     * Returns the table semantics of the table argument at {@code pos}, or throws a {@link
     * ValidationException} if the argument is not a table.
     */
    public static TableSemantics getTableSemanticsOrThrow(
            final CallContext callContext, final int pos) {
        return callContext
                .getTableSemantics(pos)
                .orElseThrow(
                        () ->
                                callContext.newValidationError(
                                        "The argument at position %d of function '%s' must be a "
                                                + "table argument.",
                                        pos, callContext.getName()));
    }

    /**
     * Returns the constant of {@code enumClass} whose lower-case name equals the string literal at
     * {@code pos}, or empty if the argument is NULL, not a literal, or names no constant.
     */
    public static <E extends Enum<E>> Optional<E> getEnumArgument(
            final CallContext callContext, final int pos, final Class<E> enumClass) {
        return callContext
                .getArgumentValue(pos, String.class)
                .flatMap(
                        value ->
                                Arrays.stream(enumClass.getEnumConstants())
                                        .filter(constant -> lowerCaseName(constant).equals(value))
                                        .findFirst());
    }

    /**
     * Returns an error message if the argument at {@code pos} is neither NULL nor a literal that
     * names a constant of {@code enumClass}, see {@link #getEnumArgument}.
     */
    public static <E extends Enum<E>> Optional<String> validateEnumArgument(
            final CallContext callContext,
            final int pos,
            final String argName,
            final String functionName,
            final Class<E> enumClass) {
        if (callContext.isArgumentNull(pos)) {
            return Optional.empty();
        }
        final String received;
        if (!callContext.isArgumentLiteral(pos)) {
            received = "a non-literal expression";
        } else {
            final Optional<String> value = callContext.getArgumentValue(pos, String.class);
            if (value.isEmpty() || getEnumArgument(callContext, pos, enumClass).isPresent()) {
                return Optional.empty();
            }
            received = String.format("%s => '%s'", argName, value.get());
        }
        final String expected =
                Arrays.stream(enumClass.getEnumConstants())
                        .map(
                                constant ->
                                        String.format(
                                                "%s => '%s'", argName, lowerCaseName(constant)))
                        .collect(Collectors.joining(" or "));
        return Optional.of(
                String.format(
                        "%s requires %s, but received %s.", functionName, expected, received));
    }

    /** Returns the {@code state_ttl} literal at {@code pos}, or empty if it is NULL or omitted. */
    public static Optional<Duration> getStateTtl(final CallContext callContext, final int pos) {
        return callContext.getArgumentValue(pos, Duration.class);
    }

    /**
     * Returns an error message if the {@code state_ttl} argument at {@code pos} is neither NULL nor
     * a non-negative constant interval.
     */
    public static Optional<String> validateStateTtl(final CallContext callContext, final int pos) {
        if (callContext.isArgumentNull(pos)) {
            return Optional.empty();
        }
        if (!callContext.isArgumentLiteral(pos)) {
            return Optional.of(
                    "The 'state_ttl' argument must be a constant INTERVAL literal, but received a "
                            + "non-literal expression. Use a literal such as INTERVAL '1' HOUR.");
        }
        return getStateTtl(callContext, pos)
                .filter(Duration::isNegative)
                .map(
                        stateTtl ->
                                String.format(
                                        "The 'state_ttl' argument must not be negative, but was "
                                                + "'%s'. Use INTERVAL '0' SECOND to retain state "
                                                + "indefinitely.",
                                        TimeUtils.formatWithHighestUnit(stateTtl)));
    }

    private static String lowerCaseName(final Enum<?> constant) {
        return constant.name().toLowerCase(Locale.ROOT);
    }

    private CallContextUtils() {}
}
