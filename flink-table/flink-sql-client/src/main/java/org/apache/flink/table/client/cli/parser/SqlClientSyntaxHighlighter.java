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

import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.sql.parser.impl.FlinkSqlParserImplConstants;
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

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/** Sql Client syntax highlighter. */
public class SqlClientSyntaxHighlighter extends DefaultHighlighter {
    private static final Logger LOG = LoggerFactory.getLogger(SqlClientSyntaxHighlighter.class);
    private static final Set<String> KEYWORDS =
            Collections.unmodifiableSet(
                    Arrays.stream(FlinkSqlParserImplConstants.tokenImage)
                            .map(t -> t.replaceAll("\"", ""))
                            .collect(Collectors.toSet()));

    private final Executor executor;

    public SqlClientSyntaxHighlighter(Executor executor) {
        this.executor = executor;
    }

    @Override
    public AttributedString highlight(LineReader reader, String buffer) {
        ReadableConfig configuration = executor.getSessionConfig();
        final SyntaxHighlightStyle.BuiltInStyle style =
                SyntaxHighlightStyle.BuiltInStyle.fromString(
                        configuration.get(SqlClientOptions.DISPLAY_DEFAULT_COLOR_SCHEMA));

        if (style == SyntaxHighlightStyle.BuiltInStyle.DEFAULT) {
            return super.highlight(reader, buffer);
        }
        final String dialectName = configuration.get(TableConfigOptions.TABLE_SQL_DIALECT);
        final SqlDialect dialect =
                SqlDialect.HIVE.name().equalsIgnoreCase(dialectName)
                        ? SqlDialect.HIVE
                        : SqlDialect.DEFAULT;
        return getHighlightedOutput(buffer, style.getHighlightStyle(), dialect);
    }

    static AttributedString getHighlightedOutput(
            String buffer, SyntaxHighlightStyle style, SqlDialect dialect) {
        final AttributedStringBuilder highlightedOutput = new AttributedStringBuilder();
        SqlClientParserState prevParseState = SqlClientParserState.DEFAULT;
        SqlClientParserState currentParseState = SqlClientParserState.DEFAULT;
        final StringBuilder word = new StringBuilder();
        for (int i = 0; i < buffer.length(); i++) {
            final char currentChar = buffer.charAt(i);
            if (prevParseState == SqlClientParserState.DEFAULT) {
                currentParseState = SqlClientParserState.computeStateAt(buffer, i, dialect);
                if (currentParseState == SqlClientParserState.DEFAULT) {
                    if (isPartOfWord(currentChar)) {
                        word.append(currentChar);
                    } else {
                        handleWord(word, highlightedOutput, currentParseState, style);
                        highlightedOutput.append(currentChar);
                        word.setLength(0);
                    }
                } else {
                    handleWord(word, highlightedOutput, SqlClientParserState.DEFAULT, style);
                    currentParseState.getStyleSetter().accept(highlightedOutput, style);
                    highlightedOutput.append(currentParseState.getStart());
                    i += currentParseState.getStart().length() - 1;
                }
            } else {
                if (currentParseState.isEndMarkerOfState(buffer, i)) {
                    highlightedOutput
                            .append(word)
                            .append(currentParseState.getEnd())
                            .style(style.getDefaultStyle());
                    word.setLength(0);
                    i += currentParseState.getEnd().length() - 1;
                    currentParseState = SqlClientParserState.DEFAULT;
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
            SqlClientParserState currentState,
            SyntaxHighlightStyle style) {
        final String wordStr = word.toString();
        if (currentState == SqlClientParserState.DEFAULT) {
            if (KEYWORDS.contains(wordStr.toUpperCase(Locale.ROOT))) {
                highlightedOutput.style(style.getKeywordStyle());
            } else {
                highlightedOutput.style(style.getDefaultStyle());
            }
        }
        highlightedOutput.append(wordStr).style(style.getDefaultStyle());
        word.setLength(0);
    }
}
