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

package org.apache.flink.table.client.cli;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.client.config.SqlClientOptions;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.StatementResult;

import org.apache.commons.lang3.tuple.Pair;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.commons.lang3.tuple.Pair.of;
import static org.apache.flink.table.client.cli.PromptHandlerTest.TestSpec.forPromptPattern;
import static org.assertj.core.api.Assertions.assertThat;
import static org.jline.utils.AttributedStyle.DEFAULT;
import static org.jline.utils.AttributedStyle.RED;
import static org.junit.jupiter.params.provider.Arguments.of;

/** Tests for {@link PromptHandler}. */
class PromptHandlerTest {
    private static final String CURRENT_CATALOG = "current_catalog";
    private static final String CURRENT_DATABASE = "current_database";
    private static final Map<String, SimpleDateFormat> FORMATTER_CACHE = new HashMap<>();

    private static final Terminal TERMINAL;

    static {
        try {
            TERMINAL = TerminalBuilder.terminal();
            FORMATTER_CACHE.put("D", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS", Locale.ROOT));
            FORMATTER_CACHE.put("m", new SimpleDateFormat("mm", Locale.ROOT));
            FORMATTER_CACHE.put("o", new SimpleDateFormat("MM", Locale.ROOT));
            FORMATTER_CACHE.put("O", new SimpleDateFormat("MMM", Locale.ROOT));
            FORMATTER_CACHE.put("P", new SimpleDateFormat("a", Locale.ROOT));
            FORMATTER_CACHE.put("r", new SimpleDateFormat("hh:mm", Locale.ROOT));
            FORMATTER_CACHE.put("R", new SimpleDateFormat("HH:mm", Locale.ROOT));
            FORMATTER_CACHE.put("s", new SimpleDateFormat("ss", Locale.ROOT));
            FORMATTER_CACHE.put("w", new SimpleDateFormat("d", Locale.ROOT));
            FORMATTER_CACHE.put("W", new SimpleDateFormat("E", Locale.ROOT));
            FORMATTER_CACHE.put("y", new SimpleDateFormat("yy", Locale.ROOT));
            FORMATTER_CACHE.put("Y", new SimpleDateFormat("yyyy", Locale.ROOT));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("promptSupplier")
    public void promptTest(TestSpec spec) {
        Map<String, String> map = new HashMap<>();
        map.put(SqlClientOptions.VERBOSE.key(), Boolean.FALSE.toString());
        map.put(SqlClientOptions.PROMPT.key(), spec.promptPattern);
        map.put("my_prop", "my_prop_value");
        map.put("my_another_prop", "my_another_prop_value");
        map.put("test", "my_prop_value");
        PromptHandler dumbPromptHandler = getDumbPromptHandler(map);

        SimpleDateFormat sdf;
        if (spec.promptPattern.length() > 1
                && (sdf = FORMATTER_CACHE.get(spec.promptPattern.substring(1))) != null) {
            final Date start;
            final Date parsed;
            final Date end;
            try {
                start = sdf.parse(sdf.format(new Date()));
                parsed = sdf.parse(dumbPromptHandler.getPrompt());
                end = sdf.parse(sdf.format(new Date()));
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
            assertThat(parsed).isAfterOrEqualTo(start).isBeforeOrEqualTo(end);
        } else {
            assertThat(dumbPromptHandler.getPrompt()).isEqualTo(spec.expectedPromptValue);
        }
    }

    static Stream<Arguments> promptSupplier() {
        return Stream.of(
                of(forPromptPattern(toAnsi("")).expectedPromptValue("")),
                of(forPromptPattern(toAnsi("simple_prompt")).expectedPromptValue("simple_prompt")),
                of(forPromptPattern(toAnsi("\\")).expectedPromptValue("")),
                of(forPromptPattern(toAnsi("\\\\")).expectedPromptValue("\\")),
                of(forPromptPattern(toAnsi("\\\\\\")).expectedPromptValue("\\")),
                of(forPromptPattern(toAnsi("\\\\\\\\")).expectedPromptValue("\\\\")),
                of(
                        forPromptPattern(toAnsi(of("", DEFAULT.foreground(RED).bold())))
                                .expectedPromptValue("")),
                of(forPromptPattern(toAnsi("\\d")).expectedPromptValue(CURRENT_DATABASE)),
                of(forPromptPattern(toAnsi("\\c")).expectedPromptValue(CURRENT_CATALOG)),
                of(
                        forPromptPattern(
                                        toAnsi(
                                                of("\\", DEFAULT),
                                                of(
                                                        "my prompt",
                                                        DEFAULT.foreground(AttributedStyle.GREEN)
                                                                .underline())))
                                .expectedPromptValue(
                                        toAnsi(
                                                of(
                                                        "my prompt",
                                                        DEFAULT.foreground(AttributedStyle.GREEN)
                                                                .underline())))),
                // property value in prompt
                of(
                        forPromptPattern(toAnsi("\\:my_prop\\:>"))
                                .propertyKey("my_prop")
                                .propertyValue("my_prop_value")
                                .expectedPromptValue("my_prop_value>")),
                // escaping of backslash \
                of(
                        forPromptPattern(
                                        toAnsi(
                                                of(
                                                        "\\\\[b:y,italic\\]\\:test\\:\\[default\\]>",
                                                        DEFAULT)))
                                .propertyKey("test")
                                .propertyValue("my_prop_value")
                                .expectedPromptValue("\\[b:y,italic]my_prop_value>")),
                // not specified \X will be handled as X
                of(forPromptPattern(toAnsi("\\X>")).expectedPromptValue("X>")),
                // if any of patterns \[...\], \{...\}, \:...\: not closed it will be handled as \X
                of(
                        forPromptPattern(toAnsi("\\{ \\:my_another_prop\\:\\[default\\]>"))
                                .propertyKey("my_another_prop")
                                .propertyValue("my_another_prop_value")
                                .expectedPromptValue("{ my_another_prop_value>")),
                of(
                        forPromptPattern(toAnsi("\\[default]\\:my_another_prop\\:>"))
                                .propertyKey("my_another_prop")
                                .propertyValue("my_another_prop_value")
                                .expectedPromptValue("[default]my_another_prop_value>")),
                // No need for expected value for date time since each time it could be different
                of(forPromptPattern(toAnsi("\\D"))),
                of(forPromptPattern(toAnsi("\\m"))),
                of(forPromptPattern(toAnsi("\\o"))),
                of(forPromptPattern(toAnsi("\\O"))),
                of(forPromptPattern(toAnsi("\\P"))),
                of(forPromptPattern(toAnsi("\\r"))),
                of(forPromptPattern(toAnsi("\\R"))),
                of(forPromptPattern(toAnsi("\\s"))),
                of(forPromptPattern(toAnsi("\\w"))),
                of(forPromptPattern(toAnsi("\\W"))),
                of(forPromptPattern(toAnsi("\\y"))),
                of(forPromptPattern(toAnsi("\\Y"))));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidPromptSupplier")
    void promptTestForInvalidInput(TestSpec spec) {
        PromptHandler promptHandler =
                getDumbPromptHandler(
                        Collections.singletonMap(
                                SqlClientOptions.VERBOSE.key(), Boolean.FALSE.toString()));
        assertThat(promptHandler.getPrompt())
                .as(SqlClientOptions.PROMPT.defaultValue())
                .isEqualTo(SqlClientOptions.PROMPT.defaultValue());
    }

    static Stream<Arguments> invalidPromptSupplier() {
        return Stream.of(
                of(
                        forPromptPattern(toAnsi("\\d"))
                                .expectedPromptValue(SqlClientOptions.PROMPT.defaultValue())),
                of(
                        forPromptPattern(toAnsi("\\c"))
                                .expectedPromptValue(SqlClientOptions.PROMPT.defaultValue())));
    }

    private static PromptHandler getDumbPromptHandler(Map<String, String> map) {
        return new PromptHandler(
                new Executor() {

                    @Override
                    public void configureSession(String statement) {}

                    @Override
                    public ReadableConfig getSessionConfig() throws SqlExecutionException {
                        return Configuration.fromMap(map);
                    }

                    @Override
                    public StatementResult executeStatement(String statement) {
                        return null;
                    }

                    @Override
                    public List<String> completeStatement(String statement, int position) {
                        return null;
                    }

                    @Override
                    public String getCurrentCatalog() {
                        return CURRENT_CATALOG;
                    }

                    @Override
                    public String getCurrentDatabase() {
                        return CURRENT_DATABASE;
                    }

                    @Override
                    public void close() {}
                },
                () -> TERMINAL);
    }

    // ------------------------------------------------------------------------------------------

    static class TestSpec {
        String expectedPromptValue;
        boolean isDatePattern;

        final String promptPattern;

        String auxiliaryPropertyKey;

        String auxiliaryPropertyValue;

        private TestSpec(String promptPattern) {
            this.promptPattern = promptPattern;
        }

        static TestSpec forPromptPattern(String promptPattern) {
            return new TestSpec(promptPattern);
        }

        TestSpec expectedPromptValue(String expectedPromptValue) {
            this.expectedPromptValue = expectedPromptValue;
            return this;
        }

        TestSpec propertyKey(String propertyKey) {
            this.auxiliaryPropertyKey = propertyKey;
            return this;
        }

        TestSpec propertyValue(String propertyValue) {
            this.auxiliaryPropertyValue = propertyValue;
            return this;
        }

        TestSpec setDatePattern(boolean datePattern) {
            isDatePattern = datePattern;
            return this;
        }

        public String toString() {
            return "Expected: "
                    + expectedPromptValue
                    + ", pattern: "
                    + promptPattern
                    + ", propertyKey: "
                    + auxiliaryPropertyKey
                    + ", propertyValue: "
                    + auxiliaryPropertyValue;
        }
    }

    private static Arguments params(String expectedAnsi, String promptPattern) {
        return params(expectedAnsi, promptPattern, null, null);
    }

    private static Arguments params(
            String expectedAnsi, String promptPattern, String propName, String propValue) {
        return of(expectedAnsi, promptPattern, propName, propValue);
    }

    private static String toAnsi(String input) {
        return toAnsi(of(input, DEFAULT));
    }

    private static String toAnsi(Pair<String, AttributedStyle> pair) {
        AttributedStringBuilder builder = new AttributedStringBuilder();
        builder.append(pair.getLeft(), pair.getRight());
        return builder.toAnsi();
    }

    private static String toAnsi(
            Pair<String, AttributedStyle> pair1, Pair<String, AttributedStyle> pair2) {
        AttributedStringBuilder builder = new AttributedStringBuilder();
        builder.append(pair1.getLeft(), pair1.getRight());
        builder.append(pair2.getLeft(), pair2.getRight());
        return builder.toAnsi();
    }
}
