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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.cli.utils.SqlParserHelper;
import org.apache.flink.table.client.cli.utils.TerminalUtils;
import org.apache.flink.table.client.cli.utils.TerminalUtils.MockOutputStream;
import org.apache.flink.table.client.cli.utils.TestTableResult;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
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
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for the {@link CliClient}. */
public class CliClientTest extends TestLogger {

    private static final String INSERT_INTO_STATEMENT =
            "INSERT INTO MyTable SELECT * FROM MyOtherTable";
    private static final String INSERT_OVERWRITE_STATEMENT =
            "INSERT OVERWRITE MyTable SELECT * FROM MyOtherTable";
    private static final String SELECT_STATEMENT = "SELECT * FROM MyOtherTable";
    private static final Row SHOW_ROW = new Row(1);

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

        // fail early in client
        verifyUpdateSubmission(SELECT_STATEMENT, false, true);
    }

    @Test
    public void testSqlCompletion() throws IOException {
        verifySqlCompletion(
                "", 0, Arrays.asList("SOURCE", "QUIT;", "RESET;"), Collections.emptyList());
        verifySqlCompletion(
                "SELE", 5, Collections.singletonList("HintA"), Collections.singletonList("QUIT;"));
        verifySqlCompletion(
                "SOUR", 5, Collections.singletonList("SOURCE"), Collections.singletonList("QUIT;"));
        verifySqlCompletion(
                "SOUR", 0, Collections.singletonList("SOURCE"), Collections.singletonList("QUIT;"));
        verifySqlCompletion(
                "QU", 2, Collections.singletonList("QUIT;"), Collections.singletonList("SELECT"));
        verifySqlCompletion(
                "qu", 2, Collections.singletonList("QUIT;"), Collections.singletonList("SELECT"));
        verifySqlCompletion(
                "  qu", 2, Collections.singletonList("QUIT;"), Collections.singletonList("SELECT"));
        verifySqlCompletion("set ", 3, Collections.emptyList(), Collections.singletonList("SET"));
        verifySqlCompletion(
                "show t ", 6, Collections.emptyList(), Collections.singletonList("SET"));
        verifySqlCompletion(
                "show",
                7,
                Collections.singletonList("MODULES;"),
                Collections.singletonList("QUIT;"));
        verifySqlCompletion(
                "show ",
                4,
                Collections.singletonList("MODULES;"),
                Collections.singletonList("QUIT;"));
        verifySqlCompletion(
                "show modules", 13, Collections.emptyList(), Collections.singletonList("QUIT;"));
    }

    @Test
    public void testUseNonExistingDB() throws Exception {
        TestingExecutor executor =
                new TestingExecutorBuilder()
                        .setExecuteSqlConsumer(
                                (ignored1, ignored2) -> {
                                    throw new SqlExecutionException("mocked exception");
                                })
                        .build();
        InputStream inputStream = new ByteArrayInputStream("use db;\n".getBytes());
        SessionContext session = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(session);

        try (Terminal terminal = new DumbTerminal(inputStream, new MockOutputStream());
                CliClient client =
                        new CliClient(terminal, sessionId, executor, historyTempFile())) {
            client.open();
            assertThat(executor.getNumExecuteSqlCalls(), is(1));
        }
    }

    @Test
    public void testUseNonExistingCatalog() throws Exception {
        TestingExecutor executor =
                new TestingExecutorBuilder()
                        .setExecuteSqlConsumer(
                                (ignored1, ignored2) -> {
                                    throw new SqlExecutionException("mocked exception");
                                })
                        .build();

        InputStream inputStream = new ByteArrayInputStream("use catalog cat;\n".getBytes());
        SessionContext sessionContext = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(sessionContext);

        try (Terminal terminal = new DumbTerminal(inputStream, new MockOutputStream());
                CliClient client =
                        new CliClient(terminal, sessionId, executor, historyTempFile())) {
            client.open();
            assertThat(executor.getNumExecuteSqlCalls(), is(1));
        }
    }

    @Test
    public void testCreateTableWithInvalidDdl() throws Exception {
        TestingExecutor executor = new TestingExecutorBuilder().build();

        // proctimee() is invalid
        InputStream inputStream =
                new ByteArrayInputStream("create table tbl(a int, b as proctimee());\n".getBytes());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(256);
        SessionContext sessionContext = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(sessionContext);

        try (Terminal terminal = new DumbTerminal(inputStream, outputStream);
                CliClient client =
                        new CliClient(terminal, sessionId, executor, historyTempFile())) {
            client.open();
            String output = new String(outputStream.toByteArray());
            assertTrue(output.contains("No match found for function signature proctimee()"));
        }
    }

    @Test
    public void testUseCatalogAndShowCurrentCatalog() throws Exception {
        TestingExecutor executor =
                new TestingExecutorBuilder()
                        .setExecuteSqlConsumer(
                                (ignored1, sql) -> {
                                    if (sql.toLowerCase().equals("use catalog cat")) {
                                        return TestTableResult.TABLE_RESULT_OK;
                                    } else if (sql.toLowerCase().equals("show current catalog")) {
                                        SHOW_ROW.setField(0, "cat");
                                        return new TestTableResult(
                                                ResultKind.SUCCESS_WITH_CONTENT,
                                                TableSchema.builder()
                                                        .field(
                                                                "current catalog name",
                                                                DataTypes.STRING())
                                                        .build(),
                                                CloseableIterator.ofElement(SHOW_ROW, ele -> {}));
                                    } else {
                                        throw new SqlExecutionException(
                                                "unexpected sql statement: " + sql);
                                    }
                                })
                        .build();

        String output = testExecuteSql(executor, "use catalog cat;");
        assertThat(executor.getNumExecuteSqlCalls(), is(1));
        assertFalse(output.contains("unexpected catalog name"));

        output = testExecuteSql(executor, "show current catalog;");
        assertThat(executor.getNumExecuteSqlCalls(), is(2));
        assertTrue(output.contains("cat"));
    }

    @Test
    public void testUseDatabaseAndShowCurrentDatabase() throws Exception {
        TestingExecutor executor =
                new TestingExecutorBuilder()
                        .setExecuteSqlConsumer(
                                (ignored1, sql) -> {
                                    if (sql.toLowerCase().equals("use db")) {
                                        return TestTableResult.TABLE_RESULT_OK;
                                    } else if (sql.toLowerCase().equals("show current database")) {
                                        SHOW_ROW.setField(0, "db");
                                        return new TestTableResult(
                                                ResultKind.SUCCESS_WITH_CONTENT,
                                                TableSchema.builder()
                                                        .field(
                                                                "current database name",
                                                                DataTypes.STRING())
                                                        .build(),
                                                CloseableIterator.ofElement(SHOW_ROW, ele -> {}));
                                    } else {
                                        throw new SqlExecutionException(
                                                "unexpected database name: db");
                                    }
                                })
                        .build();
        String output = testExecuteSql(executor, "use db;");
        assertThat(executor.getNumExecuteSqlCalls(), is(1));
        assertFalse(output.contains("unexpected database name"));

        output = testExecuteSql(executor, "show current database;");
        assertThat(executor.getNumExecuteSqlCalls(), is(2));
        assertTrue(output.contains("db"));
    }

    @Test
    public void testHistoryFile() throws Exception {
        final SessionContext context = new SessionContext("test-session", new Environment());
        final MockExecutor mockExecutor = new MockExecutor();
        String sessionId = mockExecutor.openSession(context);

        InputStream inputStream = new ByteArrayInputStream("help;\nuse catalog cat;\n".getBytes());
        Path historyFilePath = historyTempFile();
        try (Terminal terminal = new DumbTerminal(inputStream, new MockOutputStream());
                CliClient client =
                        new CliClient(terminal, sessionId, mockExecutor, historyFilePath)) {
            client.open();
            List<String> content = Files.readAllLines(historyFilePath);
            assertEquals(2, content.size());
            assertTrue(content.get(0).contains("help"));
            assertTrue(content.get(1).contains("use catalog cat"));
        }
    }

    @Test
    public void testSetSessionPropertyWithException() throws Exception {
        TestingExecutor executor =
                new TestingExecutorBuilder()
                        .setSessionPropertiesFunction(
                                (ignored1, ignored2, ignored3) -> {
                                    throw new SqlExecutionException(
                                            "Property 'parallelism' must be a integer value but was: 10a");
                                })
                        .build();
        String output = testExecuteSql(executor, "set execution.parallelism = 10a;");
        assertThat(executor.getNumSetSessionPropertyCalls(), is(1));
        assertTrue(output.contains("Property 'parallelism' must be a integer value but was: 10a"));
    }

    @Test
    public void testResetSessionPropertiesWithException() throws Exception {
        TestingExecutor executor =
                new TestingExecutorBuilder()
                        .resetSessionPropertiesFunction(
                                (ignored1) -> {
                                    throw new SqlExecutionException("Failed to reset.");
                                })
                        .build();
        String output = testExecuteSql(executor, "reset;");
        assertThat(executor.getNumResetSessionPropertiesCalls(), is(1));
        assertTrue(output.contains("Failed to reset."));
    }

    @Test
    public void testCreateCatalog() throws Exception {
        TestingExecutor executor =
                new TestingExecutorBuilder()
                        .setExecuteSqlConsumer((s, s2) -> TestTableResult.TABLE_RESULT_OK)
                        .build();
        testExecuteSql(executor, "create catalog c1 with('type'='generic_in_memory');");
        assertThat(executor.getNumExecuteSqlCalls(), is(1));
    }

    @Test
    public void testDropCatalog() throws Exception {
        TestingExecutor executor =
                new TestingExecutorBuilder()
                        .setExecuteSqlConsumer((s, s2) -> TestTableResult.TABLE_RESULT_OK)
                        .build();
        testExecuteSql(executor, "drop catalog c1;");
        assertThat(executor.getNumExecuteSqlCalls(), is(1));
    }

    // --------------------------------------------------------------------------------------------

    /** execute a sql statement and return the terminal output as string. */
    private String testExecuteSql(TestingExecutor executor, String sql) throws IOException {
        InputStream inputStream = new ByteArrayInputStream((sql + "\n").getBytes());
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(256);
        SessionContext sessionContext = new SessionContext("test-session", new Environment());
        String sessionId = executor.openSession(sessionContext);

        try (Terminal terminal = new DumbTerminal(inputStream, outputStream);
                CliClient client =
                        new CliClient(terminal, sessionId, executor, historyTempFile())) {
            client.open();
            return new String(outputStream.toByteArray());
        }
    }

    private void verifyUpdateSubmission(
            String statement, boolean failExecution, boolean testFailure) throws Exception {
        final SessionContext context = new SessionContext("test-session", new Environment());

        final MockExecutor mockExecutor = new MockExecutor();
        String sessionId = mockExecutor.openSession(context);
        mockExecutor.failExecution = failExecution;

        try (CliClient client =
                new CliClient(
                        TerminalUtils.createDummyTerminal(),
                        sessionId,
                        mockExecutor,
                        historyTempFile())) {
            if (testFailure) {
                assertFalse(client.submitUpdate(statement));
            } else {
                assertTrue(client.submitUpdate(statement));
                assertEquals(statement, mockExecutor.receivedStatement);
                assertEquals(context, mockExecutor.receivedContext);
            }
        }
    }

    private void verifySqlCompletion(
            String statement,
            int position,
            List<String> expectedHints,
            List<String> notExpectedHints)
            throws IOException {
        final SessionContext context = new SessionContext("test-session", new Environment());
        final MockExecutor mockExecutor = new MockExecutor();
        String sessionId = mockExecutor.openSession(context);

        final SqlCompleter completer = new SqlCompleter(sessionId, mockExecutor);
        final SqlMultiLineParser parser = new SqlMultiLineParser();

        try (Terminal terminal = TerminalUtils.createDummyTerminal()) {
            final LineReader reader = LineReaderBuilder.builder().terminal(terminal).build();

            final ParsedLine parsedLine =
                    parser.parse(statement, position, Parser.ParseContext.COMPLETE);
            final List<Candidate> candidates = new ArrayList<>();
            final List<String> results = new ArrayList<>();
            completer.complete(reader, parsedLine, candidates);
            candidates.forEach(item -> results.add(item.value()));

            assertTrue(results.containsAll(expectedHints));

            assertEquals(statement, mockExecutor.receivedStatement);
            assertEquals(context, mockExecutor.receivedContext);
            assertEquals(position, mockExecutor.receivedPosition);
            assertTrue(results.contains("HintA"));
            assertTrue(results.contains("Hint B"));

            results.retainAll(notExpectedHints);
            assertEquals(0, results.size());
        }
    }

    private Path historyTempFile() throws IOException {
        return File.createTempFile("history", "tmp").toPath();
    }

    // --------------------------------------------------------------------------------------------

    private static class MockExecutor implements Executor {

        public boolean failExecution;

        public SessionContext receivedContext;
        public String receivedStatement;
        public int receivedPosition;
        private final Map<String, SessionContext> sessionMap = new HashMap<>();
        private final SqlParserHelper helper = new SqlParserHelper();

        @Override
        public void start() throws SqlExecutionException {}

        @Override
        public String openSession(SessionContext session) throws SqlExecutionException {
            String sessionId = UUID.randomUUID().toString();
            sessionMap.put(sessionId, session);
            helper.registerTables();
            return sessionId;
        }

        @Override
        public void closeSession(String sessionId) throws SqlExecutionException {}

        @Override
        public Map<String, String> getSessionProperties(String sessionId)
                throws SqlExecutionException {
            return null;
        }

        @Override
        public void resetSessionProperties(String sessionId) throws SqlExecutionException {}

        @Override
        public void setSessionProperty(String sessionId, String key, String value)
                throws SqlExecutionException {}

        @Override
        public TableResult executeSql(String sessionId, String statement)
                throws SqlExecutionException {
            return TestTableResult.TABLE_RESULT_OK;
        }

        @Override
        public List<String> listModules(String sessionId) throws SqlExecutionException {
            return null;
        }

        @Override
        public org.apache.flink.table.delegation.Parser getSqlParser(String sessionId) {
            return helper.getSqlParser();
        }

        @Override
        public List<String> completeStatement(String sessionId, String statement, int position) {
            receivedContext = sessionMap.get(sessionId);
            receivedStatement = statement;
            receivedPosition = position;
            return Arrays.asList("HintA", "Hint B");
        }

        @Override
        public ResultDescriptor executeQuery(String sessionId, String query)
                throws SqlExecutionException {
            return null;
        }

        @Override
        public TypedResult<List<Tuple2<Boolean, Row>>> retrieveResultChanges(
                String sessionId, String resultId) throws SqlExecutionException {
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
        public ProgramTargetDescriptor executeUpdate(String sessionId, String statement)
                throws SqlExecutionException {
            receivedContext = sessionMap.get(sessionId);
            receivedStatement = statement;
            if (failExecution) {
                throw new SqlExecutionException("Fail execution.");
            }
            JobID jobID = JobID.generate();
            return new ProgramTargetDescriptor(jobID);
        }
    }
}
