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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.utils.SimplePath.Step;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.stream.Stream;

import static org.apache.flink.table.utils.SimplePath.Step.field;
import static org.apache.flink.table.utils.SimplePath.Step.index;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

/** Tests for {@link SimplePath}. */
class SimplePathTest {

    private static Stream<Arguments> validPaths() {
        return Stream.of(
                arguments("$", List.of()),
                arguments("$.items[0].qty", List.of(field("items"), index(0), field("qty"))),
                arguments("$.a.b", List.of(field("a"), field("b"))),
                arguments("$[007]", List.of(index(7))),
                arguments("$[2147483647]", List.of(index(Integer.MAX_VALUE))),
                arguments("$[\"a.b\"]", List.of(field("a.b"))),
                arguments("$['it\\'s']", List.of(field("it's"))),
                // The padding of a CHAR(n) value.
                arguments("$.a   ", List.of(field("a"))));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("validPaths")
    void testParse(final String path, final List<Step> steps) {
        assertThat(SimplePath.parse(path).getSteps()).isEqualTo(steps);
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @CsvSource(
            delimiter = '|',
            quoteCharacter = '`',
            value = {
                "` $.a`         | it must start with '$'",
                "lax $.a        | the 'lax' and 'strict' modes are not supported",
                "$.*            | expected a field name after '$.'",
                "$[-1]          | expected an index or a quoted name after '$['",
                "$[0,1]         | expected ']' after '$[0'",
                "$['a\\']       | the name after '$' has no closing quote",
                "$.x['a\\nb']   | unsupported escape sequence in the name after '$.x'",
                "$[2147483648]  | the index after '$[' is larger than 2147483647"
            })
    void testParseFails(final String path, final String reason) {
        assertThatThrownBy(() -> SimplePath.parse(path))
                .isInstanceOf(ValidationException.class)
                .hasMessageStartingWith(String.format("Invalid path '%s': %s.", path, reason));
    }

    @ParameterizedTest(name = "[{index}] U+{0}")
    @ValueSource(chars = {']', '(', ')', '*', '\'', '"', '\\', ' ', '\t', '\u00A0', '\u0001'})
    void testFieldNameStopsAt(final char c) {
        final String path = "$.a" + c + "b";
        assertThatThrownBy(() -> SimplePath.parse(path))
                .isInstanceOf(ValidationException.class)
                .hasMessageStartingWith(
                        String.format("Invalid path '%s': unexpected '%s' after '$.a'.", path, c));
    }

    @Test
    void testErrorMessageExplainsTheSyntax() {
        assertThatThrownBy(() -> SimplePath.parse("$.a b"))
                .hasMessage(
                        "Invalid path '$.a b': unexpected ' ' after '$.a'. A path is '$' followed "
                                + "by the steps .name, ['name'], [\"name\"] or [index], without "
                                + "wildcards, filters, slices, negative indices or functions. "
                                + "Use brackets for a name that has whitespace or one of the "
                                + "characters . [ ] ( ) * ' \" \\ in it.");
    }

    @Test
    void testSerializable() throws Exception {
        final SimplePath path = SimplePath.parse("$.items[0]['sku']");

        final SimplePath copy =
                InstantiationUtil.clone(path, SimplePathTest.class.getClassLoader());

        assertThat(copy.getSteps()).isEqualTo(path.getSteps());
        assertThat(copy).hasToString("$.items[0]['sku']");
    }
}
