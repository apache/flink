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

package org.apache.flink.api.common.errors;

import org.apache.flink.api.common.errors.FlinkError.Parameter;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Checks the errors of a {@link FlinkError} enum, for a test per enum. */
public final class FlinkErrorTestUtils {

    private static final Pattern NAME = Pattern.compile("[A-Z][A-Z0-9]*(_[A-Z0-9]+)*");

    private static final Pattern PLACEHOLDER = Pattern.compile("\\{([a-zA-Z][a-zA-Z0-9]*)}");

    private static final Pattern SQL_STATE = Pattern.compile("[0-9A-Z]{5}");

    /**
     * Checks every error of the enum: the name is in upper snake case, there is a category, the
     * attribute values have the attribute's type, a SQLSTATE has five characters and doesn't signal
     * success, the placeholders of the template match the parameters, and the template renders with
     * sample arguments.
     *
     * @param errors The enum of errors
     * @param sampleArgument Returns an argument of the given parameter type
     */
    public static <E extends Enum<E> & FlinkError> void assertErrors(
            Class<E> errors, Function<Class<?>, Object> sampleArgument) {
        for (E error : errors.getEnumConstants()) {
            assertError(error, sampleArgument);
        }
    }

    private static void assertError(FlinkError error, Function<Class<?>, Object> sampleArgument) {
        final String name = error.name();
        assertThat(name).as("name of %s", name).matches(NAME);
        assertThat(error.getCategory()).as("category of %s", name).isNotNull();

        error.getAttributes()
                .forEach(
                        (attribute, value) ->
                                assertThat(value)
                                        .as("attribute %s of %s", attribute, name)
                                        .isInstanceOf(attribute.getType()));
        final Object sqlState = error.getAttributes().get(ErrorAttribute.SQLSTATE);
        if (sqlState != null) {
            assertThat((String) sqlState)
                    .as("SQLSTATE of %s", name)
                    .matches(SQL_STATE)
                    .doesNotStartWith("00");
        }

        final List<String> parameterNames =
                error.getParameters().stream().map(Parameter::getName).collect(Collectors.toList());
        assertThat(parameterNames).as("parameters of %s", name).doesNotHaveDuplicates();
        assertThat(placeholders(error.getMessageTemplate()))
                .as("placeholders of %s", name)
                .containsExactlyInAnyOrderElementsOf(new HashSet<>(parameterNames));

        final List<Object> arguments =
                error.getParameters().stream()
                        .map(parameter -> sampleArgument.apply(parameter.getType()))
                        .collect(Collectors.toList());
        assertThat(placeholders(error.format(arguments)))
                .as("placeholders in the message of %s", name)
                .isEmpty();
        final boolean hasSensitiveParameter =
                error.getParameters().stream().anyMatch(Parameter::isSensitive);
        assertThat(error.formatRedacted(arguments, "<redacted>").contains("<redacted>"))
                .as("redacted message of %s", name)
                .isEqualTo(hasSensitiveParameter);
    }

    private static Set<String> placeholders(String text) {
        final Set<String> placeholders = new HashSet<>();
        final Matcher matcher = PLACEHOLDER.matcher(text);
        while (matcher.find()) {
            placeholders.add(matcher.group(1));
        }
        return placeholders;
    }

    private FlinkErrorTestUtils() {}
}
