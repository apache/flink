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

import org.apache.flink.annotation.PublicEvolving;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A kind of error with a stable name, a category, attributes and a message template with typed
 * parameters.
 *
 * <p>Each component declares its errors in one enum that implements this interface, with one
 * constant per error. Implementations must be enums, because exceptions keep their error when they
 * are serialized and {@link FlinkThrowable#is(FlinkError)} compares errors by identity. Exceptions
 * reject errors that aren't enum constants.
 *
 * <p>The name, category, attributes and parameters of an error are part of the API. The message
 * template can change between versions.
 *
 * @see FlinkThrowable
 */
@PublicEvolving
public interface FlinkError extends Serializable {

    /** The name of the error, unique across Flink, such as {@code TABLE_NOT_FOUND}. */
    String name();

    /** Who has to act on the error. */
    ErrorCategory getCategory();

    /** Additional metadata, with a value of {@link ErrorAttribute#getType()} per attribute. */
    Map<ErrorAttribute, Object> getAttributes();

    /**
     * The message with a {@code {name}} placeholder per parameter, such as {@code "Table
     * {objectName} does not exist."}.
     */
    String getMessageTemplate();

    /** The parameters of the message template, in the order the arguments are passed. */
    List<Parameter> getParameters();

    /**
     * Checks that the arguments match the parameters in number and type. Arguments must not be
     * null.
     *
     * @throws IllegalArgumentException if the arguments don't match the parameters
     */
    default void validateArguments(List<Object> arguments) {
        final List<Parameter> parameters = getParameters();
        if (arguments.size() != parameters.size()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Error '%s' expects %d arguments, but got %d.",
                            name(), parameters.size(), arguments.size()));
        }
        for (int i = 0; i < parameters.size(); i++) {
            final Parameter parameter = parameters.get(i);
            final Object argument = arguments.get(i);
            if (!parameter.getType().isInstance(argument)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Error '%s' expects an argument of type %s for parameter '%s', but got %s.",
                                name(),
                                parameter.getType().getTypeName(),
                                parameter.getName(),
                                argument == null ? "null" : argument.getClass().getTypeName()));
            }
        }
    }

    /**
     * Returns the message template with each placeholder replaced by the {@link
     * String#valueOf(Object)} of its argument. This is the detail message of exceptions created
     * from this error.
     *
     * @throws IllegalArgumentException if the arguments don't match the parameters
     */
    default String format(List<Object> arguments) {
        validateArguments(arguments);
        return fillTemplate(arguments, null);
    }

    /**
     * Returns the message like {@link #format(List)}, but with the arguments of {@link
     * Parameter#isSensitive() sensitive} parameters replaced by the given text. Meant for logs.
     *
     * @throws IllegalArgumentException if the arguments don't match the parameters
     */
    default String formatRedacted(List<Object> arguments, String replacement) {
        Objects.requireNonNull(replacement, "Replacement must not be null.");
        validateArguments(arguments);
        return fillTemplate(arguments, replacement);
    }

    /**
     * Fills the template in a single pass, so that a placeholder inside an argument stays as it is.
     * A brace that doesn't start a placeholder of a parameter stays as it is too.
     */
    private String fillTemplate(List<Object> arguments, @Nullable String replacement) {
        final List<Parameter> parameters = getParameters();
        final String template = getMessageTemplate();
        final StringBuilder message = new StringBuilder(template.length());
        int position = 0;
        while (true) {
            final int open = template.indexOf('{', position);
            final int close = open < 0 ? -1 : template.indexOf('}', open);
            if (close < 0) {
                break;
            }
            final int index = indexOfParameter(parameters, template.substring(open + 1, close));
            if (index < 0) {
                message.append(template, position, open + 1);
                position = open + 1;
                continue;
            }
            final Parameter parameter = parameters.get(index);
            message.append(template, position, open);
            message.append(
                    replacement != null && parameter.isSensitive()
                            ? replacement
                            : String.valueOf(arguments.get(index)));
            position = close + 1;
        }
        return message.append(template, position, template.length()).toString();
    }

    private static int indexOfParameter(List<Parameter> parameters, String name) {
        for (int i = 0; i < parameters.size(); i++) {
            if (parameters.get(i).getName().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    /** A named and typed parameter of a message template. */
    @PublicEvolving
    final class Parameter {

        private static final Pattern NAME = Pattern.compile("[a-zA-Z][a-zA-Z0-9]*");

        private final String name;
        private final Class<?> type;
        private final boolean sensitive;

        private Parameter(String name, Class<?> type, boolean sensitive) {
            Objects.requireNonNull(name, "Parameter name must not be null.");
            Objects.requireNonNull(type, "Parameter type must not be null.");
            if (!NAME.matcher(name).matches()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Parameter name '%s' must consist of letters and digits, "
                                        + "starting with a letter.",
                                name));
            }
            if (!Serializable.class.isAssignableFrom(type)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Parameter '%s' must have a serializable type, but has %s.",
                                name, type.getTypeName()));
            }
            // The constructors of exceptions take the cause and the arguments as varargs, so a
            // single array or exception argument would bind to those instead of the parameter.
            if (type.isArray() || Throwable.class.isAssignableFrom(type)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Parameter '%s' must not have an array or exception type, but has %s.",
                                name, type.getTypeName()));
            }
            this.name = name;
            this.type = type;
            this.sensitive = sensitive;
        }

        /**
         * A parameter whose arguments never contain user data.
         *
         * @param name The name in the {@code {name}} placeholder
         * @param type The type of the arguments, which must be {@link Serializable} and not an
         *     array or an exception
         */
        public static Parameter of(String name, Class<?> type) {
            return new Parameter(name, type, false);
        }

        /**
         * A parameter whose arguments can contain user data, such as identifiers or literals from a
         * statement. {@link FlinkError#formatRedacted(List, String)} replaces them.
         *
         * @param name The name in the {@code {name}} placeholder
         * @param type The type of the arguments, which must be {@link Serializable} and not an
         *     array or an exception
         */
        public static Parameter sensitive(String name, Class<?> type) {
            return new Parameter(name, type, true);
        }

        public String getName() {
            return name;
        }

        public Class<?> getType() {
            return type;
        }

        public boolean isSensitive() {
            return sensitive;
        }
    }
}
