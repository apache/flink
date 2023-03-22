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

package org.apache.flink.table.client.cli.parser;

import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.client.gateway.Executor;

import org.jline.reader.LineReader;
import org.jline.reader.impl.DefaultHighlighter;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.table.client.cli.CliClient.COLOR_SCHEMA_VAR;

/** Sql Client syntax highlighter. */
public class SqlClientSyntaxHighlighter extends DefaultHighlighter {
    private static final Logger LOG = LoggerFactory.getLogger(SqlClientSyntaxHighlighter.class);
    private static Set<String> FLINK_KEYWORD_SET;
    private static Set<Character> FLINK_KEYWORD_CHARACTER_SET;

    static {
        try (InputStream is =
                SqlClientSyntaxHighlighter.class.getResourceAsStream("/keywords.properties")) {
            Properties props = new Properties();
            props.load(is);
            FLINK_KEYWORD_SET =
                    Collections.unmodifiableSet(
                            Arrays.stream(props.get("default").toString().split(";"))
                                    .collect(Collectors.toSet()));
            FLINK_KEYWORD_CHARACTER_SET =
                    FLINK_KEYWORD_SET.stream()
                            .flatMap(t -> t.chars().mapToObj(c -> (char) c))
                            .collect(Collectors.toSet());
        } catch (IOException e) {
            LOG.error("Exception: ", e);
            FLINK_KEYWORD_SET = Collections.emptySet();
        }
    }

    private final Executor executor;

    public SqlClientSyntaxHighlighter(Executor executor) {
        this.executor = executor;
    }

    @Override
    public AttributedString highlight(LineReader reader, String buffer) {
        final Object colorSchemeOrdinal = reader.getVariable(COLOR_SCHEMA_VAR);
        SyntaxHighlightStyle.BuiltInStyle style =
                SyntaxHighlightStyle.BuiltInStyle.fromOrdinal(
                        colorSchemeOrdinal == null ? 0 : (Integer) colorSchemeOrdinal);
        if (style == SyntaxHighlightStyle.BuiltInStyle.DEFAULT) {
            return super.highlight(reader, buffer);
        }
        final String dialectName =
                executor.getSessionConfig().get(TableConfigOptions.TABLE_SQL_DIALECT);
        final SqlDialect dialect =
                SqlDialect.HIVE.name().equalsIgnoreCase(dialectName)
                        ? SqlDialect.HIVE
                        : SqlDialect.DEFAULT;
        return getHighlightedOutput(buffer, style.getHighlightStyle(), dialect);
    }

    static AttributedString getHighlightedOutput(
            String buffer, SyntaxHighlightStyle style, SqlDialect dialect) {
        final Set<Character> stateStartSymbols =
                Arrays.stream(State.values())
                        .map(t -> t.getStart(dialect).charAt(0))
                        .collect(Collectors.toSet());
        final AttributedStringBuilder highlightedOutput = new AttributedStringBuilder();
        State currentParseState = null;
        int counter = 0;
        StringBuilder word = new StringBuilder();
        for (int i = 0; i < buffer.length(); i++) {
            final char c = buffer.charAt(i);
            if (currentParseState == null) {
                if (stateStartSymbols.contains(c)) {
                    for (State s : State.STATE_LIST) {
                        final String stateStart = s.getStart(dialect);
                        if (buffer.regionMatches(i, stateStart, 0, stateStart.length())) {
                            handleWord(
                                    word,
                                    highlightedOutput,
                                    currentParseState,
                                    style,
                                    true,
                                    dialect);
                            s.getStyleSetter().accept(highlightedOutput, style);
                            highlightedOutput.append(stateStart);
                            counter++;
                            currentParseState = s;
                            i += stateStart.length() - 1;
                            break;
                        }
                    }
                }
                if (currentParseState == null) {
                    if (!FLINK_KEYWORD_CHARACTER_SET.contains(Character.toUpperCase(c))) {
                        handleWord(
                                word, highlightedOutput, currentParseState, style, true, dialect);
                        highlightedOutput.append(c);
                    } else {
                        word.append(c);
                    }
                }
            } else {
                word.append(c);
                final String stateEnd = currentParseState.getEnd(dialect);
                if (buffer.regionMatches(i, stateEnd, 0, stateEnd.length())) {
                    counter--;
                    if (counter == 0) {
                        handleWord(
                                word, highlightedOutput, currentParseState, style, true, dialect);
                        i += stateEnd.length() - 1;
                        currentParseState = null;
                    }
                }
            }
        }
        handleWord(word, highlightedOutput, currentParseState, style, false, dialect);
        return highlightedOutput.toAttributedString();
    }

    private static void handleWord(
            StringBuilder word,
            AttributedStringBuilder sb,
            State currentState,
            SyntaxHighlightStyle style,
            boolean turnOffHighlight,
            SqlDialect dialect) {
        final String wordStr = word.toString();
        if (currentState == null) {
            if (FLINK_KEYWORD_SET.contains(wordStr.toUpperCase(Locale.ROOT))) {
                sb.style(style.getKeywordStyle());
            } else {
                sb.style(style.getDefaultStyle());
            }
            sb.append(wordStr);
        } else if (turnOffHighlight) {
            sb.append(wordStr);
            final String stateEnd = currentState.getEnd(dialect);
            if (stateEnd.length() > 1) {
                sb.append(stateEnd.substring(1));
            }
        } else {
            sb.append(wordStr);
        }
        word.setLength(0);
        sb.style(style.getDefaultStyle());
    }

    /** State of parser while preparing highlighted output. */
    private enum State {
        QUOTED(
                1,
                (dialect) -> "'",
                (dialect) -> "'",
                (asb, style) -> asb.style(style.getQuotedStyle())),
        SQL_QUOTED_IDENTIFIER(
                2,
                (dialect) -> dialect == SqlDialect.HIVE ? "\"" : "`",
                (dialect) -> dialect == SqlDialect.HIVE ? "\"" : "`",
                (asb, style) -> asb.style(style.getSqlIdentifierStyle())),
        ONE_LINE_COMMENTED(
                3,
                (dialect) -> "--",
                (dialect) -> "\n",
                (asb, style) -> asb.style(style.getCommentStyle())),
        MULTILINE_COMMENTED(
                5,
                (dialect) -> "/*",
                (dialect) -> "*/",
                (asb, style) -> asb.style(style.getCommentStyle())),
        HINTED(
                4,
                (dialect) -> "/*+",
                (dialect) -> "*/",
                (asb, style) -> asb.style(style.getHintStyle()));

        private final Function<SqlDialect, String> start;
        private final Function<SqlDialect, String> end;

        private final int order;

        private final BiConsumer<AttributedStringBuilder, SyntaxHighlightStyle> styleSetter;

        private static final List<State> STATE_LIST =
                Arrays.stream(State.values())
                        .sorted(Comparator.comparingInt(o -> o.order))
                        .collect(Collectors.toList());

        State(
                int order,
                Function<SqlDialect, String> start,
                Function<SqlDialect, String> end,
                BiConsumer<AttributedStringBuilder, SyntaxHighlightStyle> styleSetter) {
            this.start = start;
            this.end = end;
            this.order = order;
            this.styleSetter = styleSetter;
        }

        public BiConsumer<AttributedStringBuilder, SyntaxHighlightStyle> getStyleSetter() {
            return styleSetter;
        }

        public String getStart(SqlDialect dialect) {
            return start.apply(dialect);
        }

        public String getEnd(SqlDialect dialect) {
            return end.apply(dialect);
        }
    }
}
