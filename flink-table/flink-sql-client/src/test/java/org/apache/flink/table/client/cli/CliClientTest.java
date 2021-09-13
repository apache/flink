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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.environment.TestingJobClient;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.client.cli.utils.SqlParserHelper;
import org.apache.flink.table.client.cli.utils.TestTableResult;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.client.gateway.context.DefaultContext;
import org.apache.flink.table.client.gateway.context.SessionContext;
import org.apache.flink.table.operations.ModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.TestLogger;

import org.jline.reader.Candidate;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.api.config.TableConfigOptions.TABLE_DML_SYNC;
import static org.apache.flink.table.client.cli.CliClient.DEFAULT_TERMINAL_FACTORY;
import static org.apache.flink.table.client.cli.CliStrings.MESSAGE_SQL_EXECUTION_ERROR;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link CliClient}. */
public class CliClientTest extends TestLogger {

    private static final String INSERT_INTO_STATEMENT =
            "INSERT INTO MyTable SELECT * FROM MyOtherTable";
    private static final String INSERT_OVERWRITE_STATEMENT =
            "INSERT OVERWRITE MyTable SELECT * FROM MyOtherTable";

    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testUpdateSubmission() throws Exception {
        verifyUpdateSubmission(INSERT_INTO_STATEMENT, false, false);
        verifyUpdateSubmission(INSERT_OVERWRITE_STATEMENT, false, false);
    }

    @Test
    public void testFailedUpdateSubmission() throws Exception {
        // fail at executor
        verifyUpdateSubmission(INSERT_INTO_STATEMENT, true, true);
        verifyUpdateSubmission(INSERT_OVERWRITE_STATEMENT, true, true);
    }

    @Test
    public void testExecuteSqlFile() throws Exception {
        MockExecutor executor = new MockExecutor();
        executeSqlFromContent(
                executor,
                String.join(
                        ";\n",
                        Arrays.asList(
                                INSERT_INTO_STATEMENT, "", INSERT_OVERWRITE_STATEMENT, "\n")));
        assertEquals(INSERT_OVERWRITE_STATEMENT, executor.receivedStatement);
    }

    @Test
    public void testSqlCompletion() throws IOException {
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
    public void testHistoryFile() throws Exception {
        final MockExecutor mockExecutor = new MockExecutor();
        String sessionId = mockExecutor.openSession("test-session");

        InputStream inputStream = new ByteArrayInputStream("help;\nuse catalog cat;\n".getBytes());
        Path historyFilePath = historyTempFile();
        try (Terminal terminal =
                        new DumbTerminal(inputStream, new TerminalUtils.MockOutputStream());
                CliClient client =
                        new CliClient(
                                () -> terminal, sessionId, mockExecutor, historyFilePath, null)) {
            client.executeInInteractiveMode();
            List<String> content = Files.readAllLines(historyFilePath);
            assertEquals(2, content.size());
            assertTrue(content.get(0).contains("help"));
            assertTrue(content.get(1).contains("use catalog cat"));
        }
    }

    @Test
    public void testGetEOFinNonInteractiveMode() throws Exception {
        final List<String> statements =
                Arrays.asList("DESC MyOtherTable;", "SHOW TABLES"); // meet EOF
        String content = String.join("\n", statements);

        final MockExecutor mockExecutor = new MockExecutor();

        executeSqlFromContent(mockExecutor, content);
        // execute the last commands
        assertTrue(statements.get(1).contains(mockExecutor.receivedStatement));
    }

    @Test
    public void testUnknownStatementInNonInteractiveMode() throws Exception {
        final List<String> statements =
                Arrays.asList(
                        "ERT INTO MyOtherTable VALUES (1, 101), (2, 102);",
                        "DESC MyOtherTable;",
                        "SHOW TABLES;");
        String content = String.join("\n", statements);

        final MockExecutor mockExecutor = new MockExecutor();

        executeSqlFromContent(mockExecutor, content);
        // don't execute other commands
        assertTrue(statements.get(0).contains(mockExecutor.receivedStatement));
    }

    @Test
    public void testFailedExecutionInNonInteractiveMode() throws Exception {
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
        assertTrue(statements.get(0).contains(mockExecutor.receivedStatement));
    }

    @Test
    public void testIllegalResultModeInNonInteractiveMode() throws Exception {
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
        assertThat(
                output,
                containsString(
                        "In non-interactive mode, it only supports to use TABLEAU as value of "
                                + "sql-client.execution.result-mode when execute query. Please add "
                                + "'SET sql-client.execution.result-mode=TABLEAU;' in the sql file."));
    }

    @Test
    public void testIllegalStatementInInitFile() throws Exception {
        final List<String> statements =
                Arrays.asList(
                        "CREATE TABLE source (a int, b string) with ( 'connector' = 'values');",
                        "INSERT INTO MyOtherTable VALUES (1, 101), (2, 102);",
                        "DESC MyOtherTable;",
                        "SHOW TABLES;");

        String content = String.join("\n", statements);

        final MockExecutor mockExecutor = new MockExecutor();
        String sessionId = mockExecutor.openSession("test-session");
        CliClient cliClient =
                new CliClient(DEFAULT_TERMINAL_FACTORY, sessionId, mockExecutor, historyTempFile());

        assertFalse("Should fail", cliClient.executeInitialization(content));
    }

    @Test(timeout = 10000)
    public void testCancelExecutionInNonInteractiveMode() throws Exception {
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

        final MockExecutor mockExecutor = new MockExecutor();
        mockExecutor.isSync = true;

        String sessionId = mockExecutor.openSession("test-session");

        Path historyFilePath = historyTempFile();

        OutputStream outputStream = new ByteArrayOutputStream(256);

        try (CliClient client =
                new CliClient(
                        () -> TerminalUtils.createDumbTerminal(outputStream),
                        sessionId,
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
            assertTrue(
                    outputStream
                            .toString()
                            .contains("java.lang.InterruptedException: sleep interrupted"));
        }

        // read the last executed statement
        assertTrue(statements.get(hookIndex).contains(mockExecutor.receivedStatement));
    }

    @Test
    public void testCancelExecutionInteractiveMode() throws Exception {
        final MockExecutor mockExecutor = new MockExecutor();
        mockExecutor.isSync = true;

        String sessionId = mockExecutor.openSession("test-session");
        Path historyFilePath = historyTempFile();
        InputStream inputStream =
                new ByteArrayInputStream("SET 'key'='value';\nSELECT 1;\nSET;\n ".getBytes());
        OutputStream outputStream = new ByteArrayOutputStream(248);

        try (CliClient client =
                new CliClient(
                        () -> TerminalUtils.createDumbTerminal(inputStream, outputStream),
                        sessionId,
                        mockExecutor,
                        historyFilePath,
                        null)) {
            Thread thread =
                    new Thread(
                            () -> {
                                try {
                                    client.executeInInteractiveMode();
                                } catch (Exception ignore) {
                                }
                            });
            thread.start();

            while (!mockExecutor.isAwait) {
                Thread.sleep(10);
            }

            client.getTerminal().raise(Terminal.Signal.INT);
            CommonTestUtils.waitUntilCondition(
                    () -> outputStream.toString().contains("'key' = 'value'"),
                    Deadline.fromNow(Duration.ofMillis(10000)));
        }
    }

    // --------------------------------------------------------------------------------------------

    private void verifyUpdateSubmission(
            String statement, boolean failExecution, boolean testFailure) throws Exception {
        final MockExecutor mockExecutor = new MockExecutor();
        mockExecutor.failExecution = failExecution;

        String result = executeSqlFromContent(mockExecutor, statement);

        if (testFailure) {
            assertTrue(result.contains(MESSAGE_SQL_EXECUTION_ERROR));
        } else {
            assertFalse(result.contains(MESSAGE_SQL_EXECUTION_ERROR));
            assertEquals(statement, mockExecutor.receivedStatement);
        }
    }

    private void verifySqlCompletion(String statement, int position, List<String> expectedHints)
            throws IOException {
        final MockExecutor mockExecutor = new MockExecutor();
        String sessionId = mockExecutor.openSession("test-session");

        final SqlCompleter completer = new SqlCompleter(sessionId, mockExecutor);
        final SqlMultiLineParser parser = new SqlMultiLineParser();

        try (Terminal terminal = TerminalUtils.createDumbTerminal()) {
            final LineReader reader = LineReaderBuilder.builder().terminal(terminal).build();

            final ParsedLine parsedLine =
                    parser.parse(statement, position, Parser.ParseContext.COMPLETE);
            final List<Candidate> candidates = new ArrayList<>();
            final List<String> results = new ArrayList<>();
            completer.complete(reader, parsedLine, candidates);
            candidates.forEach(item -> results.add(item.value()));

            assertTrue(results.containsAll(expectedHints));

            assertEquals(statement, mockExecutor.receivedStatement);
            assertEquals(position, mockExecutor.receivedPosition);
        }
    }

    private Path historyTempFile() throws IOException {
        return File.createTempFile("history", "tmp").toPath();
    }

    private String executeSqlFromContent(MockExecutor executor, String content) throws IOException {
        String sessionId = executor.openSession("test-session");
        OutputStream outputStream = new ByteArrayOutputStream(256);
        try (CliClient client =
                new CliClient(
                        () -> TerminalUtils.createDumbTerminal(outputStream),
                        sessionId,
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

        public volatile boolean isSync = false;
        public volatile boolean isAwait = false;
        public String receivedStatement;
        public int receivedPosition;
        private final Map<String, SessionContext> sessionMap = new HashMap<>();
        private final SqlParserHelper helper = new SqlParserHelper();

        @Override
        public void start() throws SqlExecutionException {}

        @Override
        public String openSession(@Nullable String sessionId) throws SqlExecutionException {
            Configuration configuration = new Configuration();
            configuration.set(TABLE_DML_SYNC, isSync);

            DefaultContext defaultContext =
                    new DefaultContext(
                            Collections.emptyList(),
                            configuration,
                            Collections.singletonList(new DefaultCLI()));
            SessionContext context = SessionContext.create(defaultContext, sessionId);
            sessionMap.put(sessionId, context);
            helper.registerTables();
            return sessionId;
        }

        @Override
        public void closeSession(String sessionId) throws SqlExecutionException {}

        @Override
        public Map<String, String> getSessionConfigMap(String sessionId)
                throws SqlExecutionException {
            return this.sessionMap.get(sessionId).getConfigMap();
        }

        @Override
        public ReadableConfig getSessionConfig(String sessionId) throws SqlExecutionException {
            SessionContext context = this.sessionMap.get(sessionId);
            return context.getReadableConfig();
        }

        @Override
        public void resetSessionProperties(String sessionId) throws SqlExecutionException {}

        @Override
        public void resetSessionProperty(String sessionId, String key)
                throws SqlExecutionException {}

        @Override
        public void setSessionProperty(String sessionId, String key, String value)
                throws SqlExecutionException {
            SessionContext context = this.sessionMap.get(sessionId);
            context.set(key, value);
        }

        @Override
        public TableResult executeOperation(String sessionId, Operation operation)
                throws SqlExecutionException {
            if (failExecution) {
                throw new SqlExecutionException("Fail execution.");
            }
            if (operation instanceof ModifyOperation) {
                if (isSync) {
                    isAwait = true;
                    try {
                        Thread.sleep(60_000L);
                    } catch (InterruptedException e) {
                        throw new SqlExecutionException("Fail to execute", e);
                    }
                }
                return new TestTableResult(
                        new TestingJobClient(),
                        ResultKind.SUCCESS_WITH_CONTENT,
                        ResolvedSchema.of(Column.physical("result", DataTypes.BIGINT())),
                        CloseableIterator.adapterForIterator(
                                Collections.singletonList(Row.of(-1L)).iterator()));
            }
            return TestTableResult.TABLE_RESULT_OK;
        }

        @Override
        public TableResult executeModifyOperations(
                String sessionId, List<ModifyOperation> operations) throws SqlExecutionException {
            if (failExecution) {
                throw new SqlExecutionException("Fail execution.");
            }
            if (isSync) {
                isAwait = true;
                try {
                    Thread.sleep(60_000L);
                } catch (InterruptedException e) {
                    throw new SqlExecutionException("Fail to execute", e);
                }
            }
            return new TestTableResult(
                    new TestingJobClient(),
                    ResultKind.SUCCESS_WITH_CONTENT,
                    ResolvedSchema.of(Column.physical("result", DataTypes.BIGINT())),
                    CloseableIterator.adapterForIterator(
                            Collections.singletonList(Row.of(-1L)).iterator()));
        }

        @Override
        public Operation parseStatement(String sessionId, String statement)
                throws SqlExecutionException {
            receivedStatement = statement;

            try {
                return helper.getSqlParser().parse(statement).get(0);
            } catch (Exception ex) {
                throw new SqlExecutionException("Parse error: " + statement, ex);
            }
        }

        @Override
        public List<String> completeStatement(String sessionId, String statement, int position) {
            receivedStatement = statement;
            receivedPosition = position;
            return Arrays.asList(helper.getSqlParser().getCompletionHints(statement, position));
        }

        @Override
        public ResultDescriptor executeQuery(String sessionId, QueryOperation query)
                throws SqlExecutionException {
            if (isSync) {
                isAwait = true;
                try {
                    Thread.sleep(60_000L);
                } catch (InterruptedException e) {
                    throw new SqlExecutionException("Fail to execute", e);
                }
            }
            return null;
        }

        @Override
        public TypedResult<List<Row>> retrieveResultChanges(String sessionId, String resultId)
                throws SqlExecutionException {
            return null;
        }

        @Override
        public TypedResult<Integer> snapshotResult(String sessionId, String resultId, int pageSize)
                throws SqlExecutionException {
            return null;
        }

        @Override
        public List<Row> retrieveResultPage(String resultId, int page)
                throws SqlExecutionException {
            return null;
        }

        @Override
        public void cancelQuery(String sessionId, String resultId) throws SqlExecutionException {
            // nothing to do
        }

        @Override
        public void addJar(String sessionId, String jarUrl) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public void removeJar(String sessionId, String jarUrl) {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public List<String> listJars(String sessionId) {
            throw new UnsupportedOperationException("Not implemented.");
        }
    }
}
