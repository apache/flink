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

package org.apache.flink.table.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;

/**
 * A path that points at exactly one place in a value, such as {@code $.user.email} or {@code
 * $['items'][0]}.
 *
 * <p>The syntax is the part of the JSON functions' path syntax that names a single place: no
 * wildcards, filters, slices, negative indices, functions or {@code lax} and {@code strict} modes.
 * Every path that this class accepts means the same in the JSON functions. A path starts with
 * {@code $}, the whole value, followed by any number of steps:
 *
 * <ul>
 *   <li>{@code .name} is a field. The name cannot contain whitespace or one of the characters
 *       {@code . [ ] ( ) * ' " \}.
 *   <li>{@code ['name']} and {@code ["name"]} are a field with any name. Inside the quotes, a
 *       backslash followed by a backslash or a quote stands for that character.
 *   <li>{@code [n]} is the array element at the 0-based index {@code n}.
 * </ul>
 *
 * <p>Trailing spaces are ignored, as in the JSON functions, so a space-padded {@code CHAR(n)} path
 * works.
 */
@Internal
public final class SimplePath implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final String SYNTAX =
            "A path is '$' followed by the steps .name, ['name'], [\"name\"] or [index], "
                    + "without wildcards, filters, slices, negative indices or functions. "
                    + "Use brackets for a name that has whitespace or one of the characters "
                    + ". [ ] ( ) * ' \" \\ in it.";

    private static final String NAME_STOP_CHARACTERS = ".[]()*'\"\\";

    private static final Pattern PATH_MODE_PREFIX =
            Pattern.compile("\\s*(strict|lax)\\s.*", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private final String path;

    private final List<Step> steps;

    private SimplePath(final String path, final List<Step> steps) {
        this.path = path;
        this.steps = steps;
    }

    /**
     * Parses a path.
     *
     * @throws ValidationException if the path is malformed or uses syntax that can point at more
     *     than one place
     */
    public static SimplePath parse(final String path) {
        Preconditions.checkNotNull(path, "Path must not be null.");
        return new Parser(path).parse();
    }

    /** Returns the steps after {@code $}, so the root path has none. */
    public List<Step> getSteps() {
        return steps;
    }

    @Override
    public String toString() {
        return path;
    }

    // --------------------------------------------------------------------------------------------

    /** A step to a field of an object or to an element of an array. */
    @Internal
    public static final class Step implements Serializable {

        private static final long serialVersionUID = 1L;

        private final @Nullable String fieldName;

        private final int index;

        private Step(final @Nullable String fieldName, final int index) {
            this.fieldName = fieldName;
            this.index = index;
        }

        static Step field(final String fieldName) {
            return new Step(Preconditions.checkNotNull(fieldName), -1);
        }

        static Step index(final int index) {
            Preconditions.checkArgument(index >= 0, "Index must not be negative.");
            return new Step(null, index);
        }

        public boolean isField() {
            return fieldName != null;
        }

        public String getFieldName() {
            Preconditions.checkState(fieldName != null, "Step is not a field.");
            return fieldName;
        }

        public int getIndex() {
            Preconditions.checkState(fieldName == null, "Step is not an index.");
            return index;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Step that = (Step) o;
            return index == that.index && Objects.equals(fieldName, that.fieldName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(fieldName, index);
        }

        @Override
        public String toString() {
            if (fieldName == null) {
                return "[" + index + "]";
            }
            return "['" + fieldName.replace("\\", "\\\\").replace("'", "\\'") + "']";
        }
    }

    // --------------------------------------------------------------------------------------------

    private static final class Parser {

        private final String path;

        // The end of the path without trailing spaces.
        private final int end;

        private int pos;

        private Parser(final String path) {
            this.path = path;
            int end = path.length();
            while (end > 0 && path.charAt(end - 1) == ' ') {
                end--;
            }
            this.end = end;
        }

        private SimplePath parse() {
            if (!path.startsWith("$")) {
                if (PATH_MODE_PREFIX.matcher(path).matches()) {
                    throw error("the 'lax' and 'strict' modes are not supported");
                }
                throw error("it must start with '$'");
            }
            pos = 1;
            final List<Step> steps = new ArrayList<>();
            while (pos < end) {
                final char c = path.charAt(pos);
                if (c == '.') {
                    pos++;
                    steps.add(Step.field(readName()));
                } else if (c == '[') {
                    pos++;
                    steps.add(readBracketStep());
                } else {
                    throw error(String.format("unexpected '%s' after '%s'", c, consumed()));
                }
            }
            return new SimplePath(path, List.copyOf(steps));
        }

        private String readName() {
            final int start = pos;
            while (pos < end && isNameCharacter(path.charAt(pos))) {
                pos++;
            }
            if (pos == start) {
                throw error(String.format("expected a field name after '%s'", consumed()));
            }
            return path.substring(start, pos);
        }

        private Step readBracketStep() {
            final String prefix = path.substring(0, pos - 1);
            final char c = pos < end ? path.charAt(pos) : 0;
            final Step step;
            if (c == '\'' || c == '"') {
                pos++;
                step = Step.field(readQuotedName(c, prefix));
            } else if (isDigit(c)) {
                step = Step.index(readIndex());
            } else {
                throw error(
                        String.format("expected an index or a quoted name after '%s'", consumed()));
            }
            if (pos >= end || path.charAt(pos) != ']') {
                throw error(String.format("expected ']' after '%s'", consumed()));
            }
            pos++;
            return step;
        }

        private String readQuotedName(final char quote, final String prefix) {
            final StringBuilder name = new StringBuilder();
            while (pos < end) {
                final char c = path.charAt(pos);
                if (c == quote) {
                    pos++;
                    return name.toString();
                }
                if (c == '\\') {
                    final char escaped = pos + 1 < end ? path.charAt(pos + 1) : 0;
                    if (escaped != '\\' && escaped != '\'' && escaped != '"') {
                        throw error(
                                String.format(
                                        "unsupported escape sequence in the name after '%s'. "
                                                + "Only a backslash or a quote can follow a "
                                                + "backslash",
                                        prefix));
                    }
                    name.append(escaped);
                    pos += 2;
                } else {
                    name.append(c);
                    pos++;
                }
            }
            throw error(String.format("the name after '%s' has no closing quote", prefix));
        }

        private int readIndex() {
            final int start = pos;
            while (pos < end && isDigit(path.charAt(pos))) {
                pos++;
            }
            try {
                return Integer.parseInt(path.substring(start, pos));
            } catch (NumberFormatException e) {
                throw error(
                        String.format(
                                "the index after '%s' is larger than %d",
                                path.substring(0, start), Integer.MAX_VALUE));
            }
        }

        private String consumed() {
            return path.substring(0, pos);
        }

        private ValidationException error(final String reason) {
            return new ValidationException(
                    String.format("Invalid path '%s': %s. %s", path, reason, SYNTAX));
        }

        private static boolean isDigit(final char c) {
            return c >= '0' && c <= '9';
        }

        private static boolean isNameCharacter(final char c) {
            return NAME_STOP_CHARACTERS.indexOf(c) < 0
                    && !Character.isWhitespace(c)
                    && !Character.isSpaceChar(c)
                    && !Character.isISOControl(c);
        }
    }
}
