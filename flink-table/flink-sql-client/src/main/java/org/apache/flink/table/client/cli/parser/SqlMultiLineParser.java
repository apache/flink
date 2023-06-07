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
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.SqlParserEOFException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.client.cli.CliClient;
import org.apache.flink.table.client.cli.Printer;
import org.apache.flink.table.client.config.ResultMode;
import org.apache.flink.table.client.config.SqlClientOptions;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.StatementResult;

import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.jline.reader.SyntaxError;
import org.jline.reader.impl.DefaultParser;

import java.util.List;

import static org.apache.flink.table.client.config.ResultMode.TABLEAU;
import static org.apache.flink.table.client.config.SqlClientOptions.EXECUTION_RESULT_MODE;

/**
 * Multi-line parser for parsing an arbitrary number of SQL lines until a line ends with ';'.
 *
 * <p>Quoting and escaping are disabled for now.
 */
public class SqlMultiLineParser extends DefaultParser {

    private static final String STATEMENT_DELIMITER = ";"; // a statement should end with `;`
    private static final String LINE_DELIMITER = "\n";
    private static final String NEW_LINE_PROMPT = ""; // results in simple '>' output

    /** Sql command parser. */
    private final SqlCommandParser parser;
    /** Mode of the CliClient. */
    private final CliClient.ExecutionMode mode;
    /** Sql command executor. */
    private final Executor executor;
    /** Exception caught in parsing. */
    private SqlExecutionException parseException = null;
    /** Result printer. */
    private Printer printer;

    public SqlMultiLineParser(
            SqlCommandParser parser, Executor executor, CliClient.ExecutionMode mode) {
        this.parser = parser;
        this.mode = mode;
        this.executor = executor;
        setEscapeChars(null);
        setQuoteChars(null);
    }

    @Override
    public ParsedLine parse(String line, int cursor, ParseContext context) {
        if (context != ParseContext.ACCEPT_LINE) {
            return parseInternal(line, cursor, context);
        }
        ReadableConfig configuration = executor.getSessionConfig();
        final String dialectName = configuration.get(TableConfigOptions.TABLE_SQL_DIALECT);
        final SqlDialect dialect =
                SqlDialect.HIVE.name().equalsIgnoreCase(dialectName)
                        ? SqlDialect.HIVE
                        : SqlDialect.DEFAULT;
        SqlClientParserState currentParseState =
                SqlClientParserState.computeCurrentStateAtTheEndOfLine(line, dialect);
        if (currentParseState != SqlClientParserState.DEFAULT
                || !line.trim().endsWith(STATEMENT_DELIMITER)) {
            throw new EOFError(-1, -1, "New line without EOF character.", NEW_LINE_PROMPT);
        }
        try {
            parseException = null;
            Command command = parser.parseStatement(line).orElse(null);
            if (command == null) {
                throw new EOFError(-1, -1, "New line without EOF character.", NEW_LINE_PROMPT);
            }
            switch (command) {
                case QUIT:
                    printer = Printer.createQuitCommandPrinter();
                    break;
                case CLEAR:
                    printer = Printer.createClearCommandPrinter();
                    break;
                case HELP:
                    printer = Printer.createHelpCommandPrinter();
                    break;
                default:
                    {
                        if (mode == CliClient.ExecutionMode.INITIALIZATION) {
                            executor.configureSession(line);
                            printer = Printer.createInitializationCommandPrinter();
                        } else {
                            StatementResult result = executor.executeStatement(line);
                            ReadableConfig sessionConfig = executor.getSessionConfig();
                            if (mode == CliClient.ExecutionMode.NON_INTERACTIVE_EXECUTION
                                    && result.isQueryResult()
                                    && sessionConfig.get(SqlClientOptions.EXECUTION_RESULT_MODE)
                                            != ResultMode.TABLEAU) {
                                throw new SqlExecutionException(
                                        String.format(
                                                "In non-interactive mode, it only supports to use %s as value of %s when execute query. "
                                                        + "Please add 'SET %s=%s;' in the sql file.",
                                                TABLEAU,
                                                EXECUTION_RESULT_MODE.key(),
                                                EXECUTION_RESULT_MODE.key(),
                                                TABLEAU));
                            }
                            printer = Printer.createStatementCommandPrinter(result, sessionConfig);
                        }
                        break;
                    }
            }
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

    public Printer getPrinter() {
        if (parseException != null) {
            throw parseException;
        }
        return printer;
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
