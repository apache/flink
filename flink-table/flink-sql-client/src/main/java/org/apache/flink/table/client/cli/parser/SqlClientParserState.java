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

import org.jline.utils.AttributedStringBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * State of parser used while preparing highlighted output, multiline parsing. This class represents
 * a state machine.
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
public enum SqlClientParserState {
    QUOTED("'", "'", dialect -> true, (asb, style) -> asb.style(style.getQuotedStyle())),
    SQL_QUOTED_IDENTIFIER(
            "`",
            "`",
            (dialect) -> dialect == SqlDialect.DEFAULT || dialect == null,
            (asb, style) -> asb.style(style.getSqlIdentifierStyle())),
    HIVE_SQL_QUOTED_IDENTIFIER(
            "\"",
            "\"",
            (dialect) -> dialect == SqlDialect.HIVE,
            (asb, style) -> asb.style(style.getSqlIdentifierStyle())),
    ONE_LINE_COMMENTED(
            "--", "\n", dialect -> true, (asb, style) -> asb.style(style.getCommentStyle())),
    HINTED("/*+", "*/", dialect -> true, (asb, style) -> asb.style(style.getHintStyle())),
    MULTILINE_COMMENTED(
            "/*", "*/", dialect -> true, (asb, style) -> asb.style(style.getCommentStyle())),

    // DEFAULT state should be the last one in this list since it is kind a fallback.
    DEFAULT(null, null, dialect -> true, (asb, style) -> asb.style(style.getDefaultStyle()));

    private final String start;
    private final String end;
    private final Function<SqlDialect, Boolean> condition;

    private final BiConsumer<AttributedStringBuilder, SyntaxHighlightStyle> styleSetter;

    private static final List<SqlClientParserState> STATE_LIST_WITHOUT_DEFAULT =
            Arrays.stream(SqlClientParserState.values())
                    .filter(t -> t != DEFAULT)
                    .collect(Collectors.toList());
    private static final Set<Character> STATE_START_SYMBOLS =
            Arrays.stream(SqlClientParserState.values())
                    .filter(t -> t != DEFAULT)
                    .map(t -> t.start.charAt(0))
                    .collect(Collectors.toSet());

    SqlClientParserState(
            String start,
            String end,
            Function<SqlDialect, Boolean> condition,
            BiConsumer<AttributedStringBuilder, SyntaxHighlightStyle> styleSetter) {
        this.start = start;
        this.end = end;
        this.condition = condition;
        this.styleSetter = styleSetter;
    }

    static SqlClientParserState computeStateAt(String input, int pos, SqlDialect dialect) {
        final char currentChar = input.charAt(pos);
        if (!STATE_START_SYMBOLS.contains(currentChar)) {
            return DEFAULT;
        }
        for (SqlClientParserState state : STATE_LIST_WITHOUT_DEFAULT) {
            if (state.condition.apply(dialect)
                    && state.start.regionMatches(0, input, pos, state.start.length())) {
                return state;
            }
        }
        return DEFAULT;
    }

    public static SqlClientParserState computeCurrentStateAtTheEndOfLine(
            String line, SqlDialect dialect) {
        SqlClientParserState prevParseState = SqlClientParserState.DEFAULT;
        SqlClientParserState currentParseState = SqlClientParserState.DEFAULT;
        for (int i = 0; i < line.length(); i++) {
            if (prevParseState == SqlClientParserState.DEFAULT) {
                currentParseState = SqlClientParserState.computeStateAt(line, i, dialect);
                if (currentParseState != SqlClientParserState.DEFAULT) {
                    i += currentParseState.getStart().length() - 1;
                }
            } else {
                if (currentParseState.isEndMarkerOfState(line, i)) {
                    currentParseState = SqlClientParserState.DEFAULT;
                }
            }
            prevParseState = currentParseState;
        }
        return currentParseState;
    }

    /**
     * Returns whether at current {@code pos} of {@code input} there is {@code end} marker of the
     * state. In case {@code end} marker is null it returns false.
     *
     * @param input a string to look at
     * @param pos a position to check if anything matches to {@code end} starting from this position
     * @return whether end marker of the current state is reached of false case of end marker of
     *     current state is null.
     */
    boolean isEndMarkerOfState(String input, int pos) {
        if (end == null) {
            return false;
        }
        return end.length() > 0 && input.regionMatches(pos, end, 0, end.length());
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
}
