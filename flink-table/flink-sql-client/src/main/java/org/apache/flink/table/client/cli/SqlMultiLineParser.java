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

import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.operations.Operation;

import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.jline.reader.SyntaxError;
import org.jline.reader.impl.DefaultParser;

import java.util.List;
import java.util.Optional;

/**
 * Multi-line parser for parsing an arbitrary number of SQL lines until a line ends with ';'.
 *
 * <p>Quoting and escaping are disabled for now.
 */
class SqlMultiLineParser extends DefaultParser {

    private static final String STATEMENT_DELIMITER = ";"; // a statement should end with `;`
    private static final String LINE_DELIMITER = "\n";
    private static final String NEW_LINE_PROMPT = ""; // results in simple '>' output

    /** Sql command parser. */
    private final SqlCommandParser parser;
    /** Exception caught in parsing. */
    private Throwable parseException = null;
    /** Operation parsed. */
    private Operation parsedOperation = null;
    /** Command read from terminal. */
    private String command;

    public SqlMultiLineParser(SqlCommandParser parser) {
        this.parser = parser;
        setEscapeChars(null);
        setQuoteChars(null);
    }

    @Override
    public ParsedLine parse(String line, int cursor, ParseContext context) {
        if (context != ParseContext.ACCEPT_LINE) {
            return parseInternal(line, cursor, context);
        }
        if (!line.trim().endsWith(STATEMENT_DELIMITER)) {
            throw new EOFError(-1, -1, "New line without EOF character.", NEW_LINE_PROMPT);
        }
        try {
            command = line;
            parseException = null;
            // try to parse the line read
            parsedOperation = parser.parseCommand(line).orElse(null);
        } catch (SqlExecutionException e) {
            if (e.getCause() instanceof SqlParserEOFException) {
                throw new EOFError(-1, -1, "The statement is incomplete.", NEW_LINE_PROMPT);
            }
            // cache the exception so that we can print details in the terminal.
            parseException = e;
            throw new SyntaxError(-1, -1, e.getMessage());
        }
        return parseInternal(line, cursor, context);
    }

    public static String formatSqlFile(String content) {
        String trimmed = content.trim();
        StringBuilder formatted = new StringBuilder();
        formatted.append(trimmed);
        if (!trimmed.endsWith(STATEMENT_DELIMITER)) {
            formatted.append(STATEMENT_DELIMITER);
        }
        formatted.append(LINE_DELIMITER);
        return formatted.toString();
    }

    private SqlArgumentList parseInternal(String line, int cursor, ParseContext context) {
        final ArgumentList parsedLine = (ArgumentList) super.parse(line, cursor, context);
        return new SqlArgumentList(
                parsedLine.line(),
                parsedLine.words(),
                parsedLine.wordIndex(),
                parsedLine.wordCursor(),
                parsedLine.cursor(),
                null,
                parsedLine.rawWordCursor(),
                parsedLine.rawWordLength());
    }

    /**
     * Gets operation parsed from current command read by LineReader. If the command read is
     * invalid, throw the exception from parser so that we can print details of the exception in
     * client.
     */
    public Optional<Operation> getParsedOperation() throws Throwable {
        if (parseException != null) {
            throw parseException;
        }
        return Optional.ofNullable(parsedOperation);
    }

    public String getCommand() {
        return command;
    }

    private class SqlArgumentList extends DefaultParser.ArgumentList {

        public SqlArgumentList(
                String line,
                List<String> words,
                int wordIndex,
                int wordCursor,
                int cursor,
                String openingQuote,
                int rawWordCursor,
                int rawWordLength) {

            super(
                    line,
                    words,
                    wordIndex,
                    wordCursor,
                    cursor,
                    openingQuote,
                    rawWordCursor,
                    rawWordLength);
        }

        @Override
        public CharSequence escape(CharSequence candidate, boolean complete) {
            // escaping is skipped for now because it highly depends on the context in the SQL
            // statement
            // e.g. 'INS(ERT INTO)' should not be quoted but 'SELECT * FROM T(axi Rides)' should
            return candidate;
        }
    }
}
