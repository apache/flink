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

import javax.annotation.Nullable;

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
    private static Set<Character> keywordCharacterSet;

    static {
        try (InputStream is =
                SqlClientSyntaxHighlighter.class.getResourceAsStream("/keywords.properties")) {
            Properties props = new Properties();
            props.load(is);
            keywordSet =
                    Collections.unmodifiableSet(
                            Arrays.stream(props.get("default").toString().split(";"))
                                    .collect(Collectors.toSet()));
            keywordCharacterSet =
                    keywordSet.stream()
                            .flatMap(t -> t.chars().mapToObj(c -> (char) c))
                            .collect(Collectors.toSet());
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
        State currentParseState = null;
        StringBuilder word = new StringBuilder();
        for (int i = 0; i < buffer.length(); i++) {
            final char currentChar = buffer.charAt(i);
            if (currentParseState == null) {
                currentParseState = State.computeStateAt(buffer, i, dialect);
                if (currentParseState == null) {
                    if (!keywordCharacterSet.contains(Character.toUpperCase(currentChar))) {
                        handleWord(word, highlightedOutput, currentParseState, style, true);
                        highlightedOutput.append(currentChar);
                    } else {
                        word.append(currentChar);
                    }
                } else {
                    handleWord(word, highlightedOutput, null, style, true);
                    currentParseState.getStyleSetter().accept(highlightedOutput, style);
                    highlightedOutput.append(currentParseState.getStart());
                    i += currentParseState.getStart().length() - 1;
                }
            } else {
                word.append(currentChar);
                final String stateEnd = currentParseState.getEnd();
                if (buffer.regionMatches(i, stateEnd, 0, stateEnd.length())) {
                    handleWord(word, highlightedOutput, currentParseState, style, true);
                    i += stateEnd.length() - 1;
                    currentParseState = null;
                }
            }
        }
        handleWord(word, highlightedOutput, currentParseState, style, false);
        return highlightedOutput.toAttributedString();
    }

    private static void handleWord(
            StringBuilder word,
            AttributedStringBuilder sb,
            @Nullable State currentState,
            SyntaxHighlightStyle style,
            boolean turnOffHighlight) {
        final String wordStr = word.toString();
        if (currentState == null) {
            if (keywordSet.contains(wordStr.toUpperCase(Locale.ROOT))) {
                sb.style(style.getKeywordStyle());
            } else {
                sb.style(style.getDefaultStyle());
            }
            sb.append(wordStr);
        } else if (turnOffHighlight) {
            sb.append(wordStr);
            final String stateEnd = currentState.getEnd();
            if (stateEnd.length() > 1) {
                sb.append(stateEnd.substring(1));
            }
        } else {
            sb.append(wordStr);
        }
        word.setLength(0);
        sb.style(style.getDefaultStyle());
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
        MULTILINE_COMMENTED(
                5, "/*", "*/", dialect -> true, (asb, style) -> asb.style(style.getCommentStyle())),
        HINTED(4, "/*+", "*/", dialect -> true, (asb, style) -> asb.style(style.getHintStyle()));

        private final String start;
        private final String end;
        private final Function<SqlDialect, Boolean> condition;

        private final int order;

        private final BiConsumer<AttributedStringBuilder, SyntaxHighlightStyle> styleSetter;

        private static final List<State> STATE_LIST =
                Arrays.stream(State.values())
                        .sorted(Comparator.comparingInt(o -> o.order))
                        .collect(Collectors.toList());
        private static final Set<Character> STATE_START_SYMBOLS =
                Arrays.stream(State.values())
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

        public BiConsumer<AttributedStringBuilder, SyntaxHighlightStyle> getStyleSetter() {
            return styleSetter;
        }

        public String getStart() {
            return start;
        }

        public String getEnd() {
            return end;
        }

        public static State computeStateAt(String input, int pos, SqlDialect dialect) {
            final char currentChar = input.charAt(pos);
            if (!STATE_START_SYMBOLS.contains(currentChar)) {
                return null;
            }
            for (State state : STATE_LIST) {
                if (state.condition.apply(dialect)
                        && state.start.regionMatches(0, input, pos, state.start.length())) {
                    return state;
                }
            }
            return null;
        }
    }
}
