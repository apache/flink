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

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.cli.parser.SqlCommandParserImpl;
import org.apache.flink.table.client.cli.parser.SqlMultiLineParser;
import org.apache.flink.table.client.cli.utils.SqlParserHelper;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.StatementResult;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.util.CloseableIterator;

import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC;
import static org.apache.flink.table.api.internal.StaticResultProvider.SIMPLE_ROW_DATA_TO_STRING_CONVERTER;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_SQL_EXECUTION_ERROR;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link CliClient}. */
class CliClientTest {

    private static final String INSERT_INTO_STATEMENT =
            "INSERT INTO MyTable SELECT * FROM MyOtherTable";
    private static final String INSERT_OVERWRITE_STATEMENT =
            "INSERT OVERWRITE MyTable SELECT * FROM MyOtherTable";
    private static final String ORIGIN_SQL = "SELECT pos\t FROM source_table;\n";
    private static final String SQL_WITHOUT_COMPLETER = "SELECT pos FROM source_table;";
    private static final String SQL_WITH_COMPLETER = "SELECT POSITION  FROM source_table;";

    @Test
    void testUpdateSubmission() throws Exception {
        verifyUpdateSubmission(INSERT_INTO_STATEMENT, false, false);
        verifyUpdateSubmission(INSERT_OVERWRITE_STATEMENT, false, false);
    }

    @Test
    void testFailedUpdateSubmission() throws Exception {
        // fail at executor
        verifyUpdateSubmission(INSERT_INTO_STATEMENT, true, true);
        verifyUpdateSubmission(INSERT_OVERWRITE_STATEMENT, true, true);
    }

    @Test
    void testExecuteSqlFile() throws Exception {
        MockExecutor executor = new MockExecutor();
        executeSqlFromContent(
                executor,
                String.join(
                        ";\n",
                        Arrays.asList(
                                INSERT_INTO_STATEMENT, "", INSERT_OVERWRITE_STATEMENT, "\n")));
        assertThat(executor.receivedStatement).contains(INSERT_OVERWRITE_STATEMENT);
    }

    @Test
    void testExecuteSqlFileWithoutSqlCompleter() throws Exception {
        MockExecutor executor = new MockExecutor(new SqlParserHelper(), false);
        executeSqlFromContent(executor, ORIGIN_SQL);
        assertThat(executor.receivedStatement).contains(SQL_WITHOUT_COMPLETER);
    }

    @Test
    void testExecuteSqlInteractiveWithSqlCompleter() throws Exception {
        final MockExecutor mockExecutor = new MockExecutor(new SqlParserHelper(), false);

        InputStream inputStream = new ByteArrayInputStream(ORIGIN_SQL.getBytes());
        OutputStream outputStream = new ByteArrayOutputStream(256);
        try (Terminal terminal = new DumbTerminal(inputStream, outputStream);
                CliClient client =
                        new CliClient(() -> terminal, mockExecutor, historyTempFile(), null)) {
            client.executeInInteractiveMode();
            assertThat(mockExecutor.receivedStatement).contains(SQL_WITH_COMPLETER);
        }
    }

    @Test
    void testSqlCompletion() throws IOException {
        verifySqlCompletion("", 0, Arrays.asList("CLEAR", "HELP", "EXIT", "QUIT", "RESET", "SET"));
        verifySqlCompletion("SELE", 4, Collections.emptyList());
        verifySqlCompletion("QU", 2, Collections.singletonList("QUIT"));
        verifySqlCompletion("qu", 2, Collections.singletonList("QUIT"));
        verifySqlCompletion("  qu", 2, Collections.singletonList("QUIT"));
        verifySqlCompletion("set ", 3, Collections.emptyList());
        verifySqlCompletion("show t ", 6, Collections.emptyList());
        verifySqlCompletion("show ", 4, Collections.emptyList());
        verifySqlCompletion("show modules", 12, Collections.emptyList());
    }

    @Test
    void testHistoryFile() throws Exception {
        final MockExecutor mockExecutor = new MockExecutor();

        InputStream inputStream = new ByteArrayInputStream("help;\nuse catalog cat;\n".getBytes());
        Path historyFilePath = historyTempFile();
        try (Terminal terminal =
                        new DumbTerminal(inputStream, new TerminalUtils.MockOutputStream());
                CliClient client =
                        new CliClient(() -> terminal, mockExecutor, historyFilePath, null)) {
            client.executeInInteractiveMode();
            List<String> content = Files.readAllLines(historyFilePath);
            assertThat(content).hasSize(2);
            assertThat(content.get(0)).contains("help");
            assertThat(content.get(1)).contains("use catalog cat");
        }
    }

    @Test
    void testGetEOFinNonInteractiveMode() throws Exception {
        final List<String> statements =
                Arrays.asList("DESC MyOtherTable;", "SHOW TABLES"); // meet EOF
        String content = String.join("\n", statements);

        final MockExecutor mockExecutor = new MockExecutor();

        executeSqlFromContent(mockExecutor, content);
        // execute the last commands
        assertThat(mockExecutor.receivedStatement).contains(statements.get(1));
    }

    @Test
    void testUnknownStatementInNonInteractiveMode() throws Exception {
        final List<String> statements =
                Arrays.asList(
                        "ERT INTO MyOtherTable VALUES (1, 101), (2, 102);",
                        "DESC MyOtherTable;",
                        "SHOW TABLES;");
        String content = String.join("\n", statements);

        final MockExecutor mockExecutor = new MockExecutor();

        executeSqlFromContent(mockExecutor, content);
        // don't execute other commands
        assertThat(statements.get(0)).isEqualTo(mockExecutor.receivedStatement);
    }

    @Test
    void testFailedExecutionInNonInteractiveMode() throws Exception {
        final List<String> statements =
                Arrays.asList(
                        "INSERT INTO MyOtherTable VALUES (1, 101), (2, 102);",
                        "DESC MyOtherTable;",
                        "SHOW TABLES;");
        String content = String.join("\n", statements);

        final MockExecutor mockExecutor = new MockExecutor();
        mockExecutor.failExecution = true;

        executeSqlFromContent(mockExecutor, content);
        // don't execute other commands
        assertThat(statements.get(0)).isEqualTo(mockExecutor.receivedStatement);
    }

    @Test
    void testIllegalResultModeInNonInteractiveMode() throws Exception {
        // When client executes sql file, it requires sql-client.execution.result-mode = tableau;
        // Therefore, it will get execution error and stop executing the sql follows the illegal
        // statement.
        final List<String> statements =
                Arrays.asList(
                        "SELECT * FROM MyOtherTable;",
                        "HELP;",
                        "INSERT INTO MyOtherTable VALUES (1, 101), (2, 102);",
                        "DESC MyOtherTable;",
                        "SHOW TABLES;");

        String content = String.join("\n", statements);

        final MockExecutor mockExecutor = new MockExecutor();

        String output = executeSqlFromContent(mockExecutor, content);
        assertThat(output)
                .contains(
                        "In non-interactive mode, it only supports to use TABLEAU as value of "
                                + "sql-client.execution.result-mode when execute query. Please add "
                                + "'SET sql-client.execution.result-mode=TABLEAU;' in the sql file.");
    }

    @Test
    void testCancelExecutionInNonInteractiveMode() throws Exception {
        // add "\n" with quit to trigger commit the line
        final List<String> statements =
                Arrays.asList(
                        "HELP;",
                        "CREATE TABLE tbl( -- comment\n"
                                + "-- comment with ;\n"
                                + "id INT,\n"
                                + "name STRING\n"
                                + ") WITH (\n"
                                + "  'connector' = 'values'\n"
                                + ");\n",
                        "INSERT INTO \n" + "MyOtherTable VALUES (1, 101), (2, 102);",
                        "DESC MyOtherTable;",
                        "SHOW TABLES;",
                        "QUIT;\n");

        // use table.dml-sync to keep running
        // therefore in non-interactive mode, the last executed command is INSERT INTO
        final int hookIndex = 2;

        String content = String.join("\n", statements);

        final MockExecutor mockExecutor = new MockExecutor(new SqlParserHelper(), true);

        Path historyFilePath = historyTempFile();

        OutputStream outputStream = new ByteArrayOutputStream(256);

        try (CliClient client =
                new CliClient(
                        () -> TerminalUtils.createDumbTerminal(outputStream),
                        mockExecutor,
                        historyFilePath,
                        null)) {
            Thread thread = new Thread(() -> client.executeInNonInteractiveMode(content));
            thread.start();

            while (!mockExecutor.isAwait) {
                Thread.sleep(10);
            }

            thread.interrupt();

            while (thread.isAlive()) {
                Thread.sleep(10);
            }
            assertThat(outputStream.toString())
                    .contains("java.lang.InterruptedException: sleep interrupted");
        }

        // read the last executed statement
        assertThat(statements.get(hookIndex)).isEqualTo(mockExecutor.receivedStatement.trim());
    }

    @Test
    void testCancelExecutionInteractiveMode() throws Exception {
        final MockExecutor mockExecutor = new MockExecutor(new SqlParserHelper(), true);

        Path historyFilePath = historyTempFile();
        InputStream inputStream = new ByteArrayInputStream("SELECT 1;\nHELP;\n ".getBytes());
        OutputStream outputStream = new ByteArrayOutputStream(248);

        try (Terminal terminal = TerminalUtils.createDumbTerminal(inputStream, outputStream);
                CliClient client =
                        new CliClient(() -> terminal, mockExecutor, historyFilePath, null)) {
            CheckedThread thread =
                    new CheckedThread() {
                        @Override
                        public void go() {
                            client.executeInInteractiveMode();
                        }
                    };
            thread.start();

            while (!mockExecutor.isAwait) {
                Thread.sleep(10);
            }

            terminal.raise(Terminal.Signal.INT);
            CommonTestUtils.waitUntilCondition(
                    () -> outputStream.toString().contains(CliStrings.MESSAGE_HELP));
            // Prevent NPE when closing the terminal. See FLINK-33116 for more information.
            thread.sync();
        }
    }

    // --------------------------------------------------------------------------------------------

    private void verifyUpdateSubmission(
            String statement, boolean failExecution, boolean testFailure) throws Exception {
        final MockExecutor mockExecutor = new MockExecutor();
        mockExecutor.failExecution = failExecution;

        String result = executeSqlFromContent(mockExecutor, statement);

        if (testFailure) {
            assertThat(result).contains(MESSAGE_SQL_EXECUTION_ERROR);
        } else {
            assertThat(result).doesNotContain(MESSAGE_SQL_EXECUTION_ERROR);
            assertThat(SqlMultiLineParser.formatSqlFile(statement))
                    .isEqualTo(SqlMultiLineParser.formatSqlFile(mockExecutor.receivedStatement));
        }
    }

    private void verifySqlCompletion(String statement, int position, List<String> expectedHints)
            throws IOException {
        final MockExecutor mockExecutor = new MockExecutor();

        final SqlCompleter completer = new SqlCompleter(mockExecutor);
        final SqlMultiLineParser parser =
                new SqlMultiLineParser(
                        new SqlCommandParserImpl(),
                        mockExecutor,
                        CliClient.ExecutionMode.INTERACTIVE_EXECUTION);

        try (Terminal terminal = TerminalUtils.createDumbTerminal()) {
            final LineReader reader = LineReaderBuilder.builder().terminal(terminal).build();

            final ParsedLine parsedLine =
                    parser.parse(statement, position, Parser.ParseContext.COMPLETE);
            final List<Candidate> candidates = new ArrayList<>();
            final List<String> results = new ArrayList<>();
            completer.complete(reader, parsedLine, candidates);
            candidates.forEach(item -> results.add(item.value()));

            assertThat(results.containsAll(expectedHints)).isTrue();

            assertThat(statement).isEqualTo(mockExecutor.receivedStatement);
            assertThat(position).isEqualTo(mockExecutor.receivedPosition);
        }
    }

    private Path historyTempFile() throws IOException {
        return File.createTempFile("history", "tmp").toPath();
    }

    private String executeSqlFromContent(MockExecutor executor, String content) throws IOException {
        OutputStream outputStream = new ByteArrayOutputStream(256);
        try (CliClient client =
                new CliClient(
                        () -> TerminalUtils.createDumbTerminal(outputStream),
                        executor,
                        historyTempFile(),
                        null)) {
            client.executeInNonInteractiveMode(content);
        }
        return outputStream.toString();
    }

    // --------------------------------------------------------------------------------------------

    private static class MockExecutor implements Executor {

        public boolean failExecution;

        public volatile boolean isSync;
        public volatile boolean isAwait = false;
        public String receivedStatement;
        public int receivedPosition;
        public final Configuration configuration;
        private final SqlParserHelper helper;

        public MockExecutor() {
            this(new SqlParserHelper(), false);
        }

        public MockExecutor(SqlParserHelper helper, boolean isSync) {
            this.helper = helper;
            this.configuration = new Configuration();
            this.isSync = isSync;
            configuration.set(TABLE_DML_SYNC, isSync);
            helper.registerTables();
        }

        @Override
        public void configureSession(String statement) {}

        @Override
        public ReadableConfig getSessionConfig() {
            return configuration;
        }

        @Override
        public Map<String, String> getSessionConfigMap() {
            return configuration.toMap();
        }

        @Override
        public StatementResult executeStatement(String statement) {
            receivedStatement = statement;
            if (failExecution) {
                throw new SqlExecutionException("Fail execution.");
            }
            Operation operation;
            try {
                operation = helper.getSqlParser().parse(statement).get(0);
            } catch (Exception e) {
                throw new SqlExecutionException("Failed to parse statement.", e);
            }
            if (operation instanceof ModifyOperation || operation instanceof QueryOperation) {
                if (isSync) {
                    isAwait = true;
                    try {
                        Thread.sleep(60_000L);
                    } catch (InterruptedException e) {
                        throw new SqlExecutionException("Fail to execute", e);
                    }
                }

                return new StatementResult(
                        ResolvedSchema.of(Column.physical("result", DataTypes.BIGINT())),
                        CloseableIterator.adapterForIterator(
                                Collections.singletonList((RowData) GenericRowData.of(-1L))
                                        .iterator()),
                        operation instanceof QueryOperation,
                        ResultKind.SUCCESS_WITH_CONTENT,
                        JobID.generate(),
                        SIMPLE_ROW_DATA_TO_STRING_CONVERTER);
            } else {
                return new StatementResult(
                        TableResultImpl.TABLE_RESULT_OK.getResolvedSchema(),
                        TableResultImpl.TABLE_RESULT_OK.collectInternal(),
                        false,
                        ResultKind.SUCCESS,
                        null);
            }
        }

        @Override
        public List<String> completeStatement(String statement, int position) {
            receivedStatement = statement;
            receivedPosition = position;
            return Arrays.asList(helper.getSqlParser().getCompletionHints(statement, position));
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
