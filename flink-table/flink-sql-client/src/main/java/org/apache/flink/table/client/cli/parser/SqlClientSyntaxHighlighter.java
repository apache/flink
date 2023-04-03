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
import org.apache.flink.table.client.config.SqlClientOptions;
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

/** Sql Client syntax highlighter. */
public class SqlClientSyntaxHighlighter extends DefaultHighlighter {
    private static final Logger LOG = LoggerFactory.getLogger(SqlClientSyntaxHighlighter.class);
    private static Set<String> keywordSet;

    static {
        try (InputStream is =
                SqlClientSyntaxHighlighter.class.getResourceAsStream("/keywords.properties")) {
            Properties props = new Properties();
            props.load(is);
            keywordSet =
                    Collections.unmodifiableSet(
                            Arrays.stream(props.get("default").toString().split(";"))
                                    .collect(Collectors.toSet()));
        } catch (IOException e) {
            LOG.error("Exception: ", e);
            keywordSet = Collections.emptySet();
        }
    }

    private final Executor executor;

    public SqlClientSyntaxHighlighter(Executor executor) {
        this.executor = executor;
    }

    @Override
    public AttributedString highlight(LineReader reader, String buffer) {
        final SyntaxHighlightStyle.BuiltInStyle style =
                SyntaxHighlightStyle.BuiltInStyle.fromString(
                        executor.getSessionConfig()
                                .get(SqlClientOptions.DISPLAY_DEFAULT_COLOR_SCHEMA));

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
        final AttributedStringBuilder highlightedOutput = new AttributedStringBuilder();
        State prevParseState = State.DEFAULT;
        State currentParseState = State.DEFAULT;
        final StringBuilder word = new StringBuilder();
        for (int i = 0; i < buffer.length(); i++) {
            final char currentChar = buffer.charAt(i);
            if (prevParseState == State.DEFAULT) {
                currentParseState = State.computeStateAt(buffer, i, dialect);
                if (currentParseState == State.DEFAULT) {
                    if (isPartOfWord(currentChar)) {
                        word.append(currentChar);
                    } else {
                        handleWord(word, highlightedOutput, currentParseState, style);
                        highlightedOutput.append(currentChar);
                        word.setLength(0);
                    }
                } else {
                    handleWord(word, highlightedOutput, State.DEFAULT, style);
                    currentParseState.styleSetter.accept(highlightedOutput, style);
                    highlightedOutput.append(currentParseState.start);
                    i += currentParseState.start.length() - 1;
                }
            } else {
                if (currentParseState.isEndOfState(buffer, i)) {
                    highlightedOutput
                            .append(word)
                            .append(currentParseState.end)
                            .style(style.getDefaultStyle());
                    word.setLength(0);
                    i += currentParseState.end.length() - 1;
                    currentParseState = State.DEFAULT;
                } else {
                    word.append(currentChar);
                }
            }
            prevParseState = currentParseState;
        }
        handleWord(word, highlightedOutput, currentParseState, style);
        return highlightedOutput.toAttributedString();
    }

    private static boolean isPartOfWord(char c) {
        return Character.isLetterOrDigit(c) || c == '_' || c == '$';
    }

    private static void handleWord(
            StringBuilder word,
            AttributedStringBuilder highlightedOutput,
            State currentState,
            SyntaxHighlightStyle style) {
        final String wordStr = word.toString();
        if (currentState == State.DEFAULT) {
            if (keywordSet.contains(wordStr.toUpperCase(Locale.ROOT))) {
                highlightedOutput.style(style.getKeywordStyle());
            } else {
                highlightedOutput.style(style.getDefaultStyle());
            }
        }
        highlightedOutput.append(wordStr).style(style.getDefaultStyle());
        word.setLength(0);
    }

    /**
     * State of parser while preparing highlighted output. This class represents a state machine.
     *
     * <pre>
     *      MultiLine Comment           Single Line Comment
     *           |                              |
     *   (&#47;*,*&#47;) |                              | (--, \n)
     *           *------------Default-----------*
     *                        |    |
     *                        |    |
     *           *------------*    *------------*
     *  (&#47;*+,*&#47;) |            |    |            | (', ')
     *           |            |    |            |
     *         Hint           |    |          String
     *                        |    |
     *                        |    |
     *           *------------*    *------------*
     *    (&quot;, &quot;) |                              | (`, `)
     *           |                              |
     *     Hive Identifier           Flink Default Identifier
     * </pre>
     */
    private enum State {
        QUOTED(1, "'", "'", dialect -> true, (asb, style) -> asb.style(style.getQuotedStyle())),
        SQL_QUOTED_IDENTIFIER(
                2,
                "`",
                "`",
                (dialect) -> dialect == SqlDialect.DEFAULT || dialect == null,
                (asb, style) -> asb.style(style.getSqlIdentifierStyle())),
        HIVE_SQL_QUOTED_IDENTIFIER(
                2,
                "\"",
                "\"",
                (dialect) -> dialect == SqlDialect.HIVE,
                (asb, style) -> asb.style(style.getSqlIdentifierStyle())),
        ONE_LINE_COMMENTED(
                3, "--", "\n", dialect -> true, (asb, style) -> asb.style(style.getCommentStyle())),
        HINTED(4, "/*+", "*/", dialect -> true, (asb, style) -> asb.style(style.getHintStyle())),
        MULTILINE_COMMENTED(
                5, "/*", "*/", dialect -> true, (asb, style) -> asb.style(style.getCommentStyle())),
        DEFAULT(
                Integer.MAX_VALUE,
                null,
                null,
                dialect -> true,
                (asb, style) -> asb.style(style.getDefaultStyle()));

        private final String start;
        private final String end;
        private final Function<SqlDialect, Boolean> condition;

        private final int order;

        private final BiConsumer<AttributedStringBuilder, SyntaxHighlightStyle> styleSetter;

        private static final List<State> STATE_LIST_WITHOUT_DEFAULT =
                Arrays.stream(State.values())
                        .filter(t -> t != DEFAULT)
                        .sorted(Comparator.comparingInt(o -> o.order))
                        .collect(Collectors.toList());
        private static final Set<Character> STATE_START_SYMBOLS =
                Arrays.stream(State.values())
                        .filter(t -> t != DEFAULT)
                        .map(t -> t.start.charAt(0))
                        .collect(Collectors.toSet());

        State(
                int order,
                String start,
                String end,
                Function<SqlDialect, Boolean> condition,
                BiConsumer<AttributedStringBuilder, SyntaxHighlightStyle> styleSetter) {
            this.start = start;
            this.end = end;
            this.order = order;
            this.condition = condition;
            this.styleSetter = styleSetter;
        }

        static State computeStateAt(String input, int pos, SqlDialect dialect) {
            final char currentChar = input.charAt(pos);
            if (!STATE_START_SYMBOLS.contains(currentChar)) {
                return DEFAULT;
            }
            for (State state : STATE_LIST_WITHOUT_DEFAULT) {
                if (state.condition.apply(dialect)
                        && state.start.regionMatches(0, input, pos, state.start.length())) {
                    return state;
                }
            }
            return DEFAULT;
        }

        boolean isEndOfState(String input, int pos) {
            if (this == DEFAULT) {
                return false;
            }
            return end.length() > 0 && input.regionMatches(pos, end, 0, end.length());
        }
    }
}
