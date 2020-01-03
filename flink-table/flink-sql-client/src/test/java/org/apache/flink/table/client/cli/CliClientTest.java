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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.cli.utils.TerminalUtils;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.config.entries.ViewEntry;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.types.Row;
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
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * Tests for the {@link CliClient}.
 */
public class CliClientTest extends TestLogger {

	private static final String INSERT_INTO_STATEMENT = "INSERT INTO MyTable SELECT * FROM MyOtherTable";
	private static final String INSERT_OVERWRITE_STATEMENT = "INSERT OVERWRITE MyTable SELECT * FROM MyOtherTable";
	private static final String SELECT_STATEMENT = "SELECT * FROM MyOtherTable";

	@Test
	public void testUpdateSubmission() {
		verifyUpdateSubmission(INSERT_INTO_STATEMENT, false, false);
		verifyUpdateSubmission(INSERT_OVERWRITE_STATEMENT, false, false);
	}

	@Test
	public void testFailedUpdateSubmission() {
		// fail at executor
		verifyUpdateSubmission(INSERT_INTO_STATEMENT, true, true);
		verifyUpdateSubmission(INSERT_OVERWRITE_STATEMENT, true, true);

		// fail early in client
		verifyUpdateSubmission(SELECT_STATEMENT, false, true);
	}

	@Test
	public void testSqlCompletion() throws IOException {
		verifySqlCompletion("", 0, Arrays.asList("SELECT", "QUIT;", "RESET;"), Collections.emptyList());
		verifySqlCompletion("SELEC", 5, Collections.singletonList("SELECT"), Collections.singletonList("QUIT;"));
		verifySqlCompletion("SELE", 0, Collections.singletonList("SELECT"), Collections.singletonList("QUIT;"));
		verifySqlCompletion("QU", 2, Collections.singletonList("QUIT;"), Collections.singletonList("SELECT"));
		verifySqlCompletion("qu", 2, Collections.singletonList("QUIT;"), Collections.singletonList("SELECT"));
		verifySqlCompletion("  qu", 2, Collections.singletonList("QUIT;"), Collections.singletonList("SELECT"));
		verifySqlCompletion("set ", 3, Collections.emptyList(), Collections.singletonList("SET"));
		verifySqlCompletion("show t ", 6, Collections.emptyList(), Collections.singletonList("SET"));
	}

	@Test
	public void testUseNonExistingDB() throws Exception {
		Executor executor = mock(Executor.class);
		doThrow(new SqlExecutionException("mocked exception")).when(executor).useDatabase(any(), any());
		InputStream inputStream = new ByteArrayInputStream("use db;\n".getBytes());
		// don't care about the output
		OutputStream outputStream = new OutputStream() {
			@Override
			public void write(int b) throws IOException {
			}
		};
		SessionContext session = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(session);

		CliClient cliClient = null;
		try (Terminal terminal = new DumbTerminal(inputStream, outputStream)) {
			cliClient = new CliClient(terminal, sessionId, executor);
			cliClient.open();
			verify(executor).useDatabase(any(), any());
		} finally {
			if (cliClient != null) {
				cliClient.close();
			}
		}
	}

	@Test
	public void testUseNonExistingCatalog() throws Exception {
		Executor executor = mock(Executor.class);
		doThrow(new SqlExecutionException("mocked exception")).when(executor).useCatalog(any(), any());
		InputStream inputStream = new ByteArrayInputStream("use catalog cat;\n".getBytes());
		// don't care about the output
		OutputStream outputStream = new OutputStream() {
			@Override
			public void write(int b) throws IOException {
			}
		};
		CliClient cliClient = null;
		SessionContext sessionContext = new SessionContext("test-session", new Environment());
		String sessionId = executor.openSession(sessionContext);

		try (Terminal terminal = new DumbTerminal(inputStream, outputStream)) {
			cliClient = new CliClient(terminal, sessionId, executor);
			cliClient.open();
			verify(executor).useCatalog(any(), any());
		} finally {
			if (cliClient != null) {
				cliClient.close();
			}
		}
	}

	// --------------------------------------------------------------------------------------------

	private void verifyUpdateSubmission(String statement, boolean failExecution, boolean testFailure) {
		final SessionContext context = new SessionContext("test-session", new Environment());

		final MockExecutor mockExecutor = new MockExecutor();
		String sessionId = mockExecutor.openSession(context);
		mockExecutor.failExecution = failExecution;

		CliClient cli = null;
		try {
			cli = new CliClient(TerminalUtils.createDummyTerminal(), sessionId, mockExecutor);
			if (testFailure) {
				assertFalse(cli.submitUpdate(statement));
			} else {
				assertTrue(cli.submitUpdate(statement));
				assertEquals(statement, mockExecutor.receivedStatement);
				assertEquals(context, mockExecutor.receivedContext);
			}
		} finally {
			if (cli != null) {
				cli.close();
			}
		}
	}

	private void verifySqlCompletion(String statement, int position, List<String> expectedHints, List<String> notExpectedHints) throws IOException {
		final SessionContext context = new SessionContext("test-session", new Environment());
		final MockExecutor mockExecutor = new MockExecutor();
		String sessionId = mockExecutor.openSession(context);

		final SqlCompleter completer = new SqlCompleter(sessionId, mockExecutor);
		final SqlMultiLineParser parser = new SqlMultiLineParser();

		try (Terminal terminal = TerminalUtils.createDummyTerminal()) {
			final LineReader reader = LineReaderBuilder.builder().terminal(terminal).build();

			final ParsedLine parsedLine = parser.parse(statement, position, Parser.ParseContext.COMPLETE);
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

	// --------------------------------------------------------------------------------------------

	private static class MockExecutor implements Executor {

		public boolean failExecution;

		public SessionContext receivedContext;
		public String receivedStatement;
		public int receivedPosition;
		private final Map<String, SessionContext> sessionMap = new HashMap<>();

		@Override
		public void start() throws SqlExecutionException {
			// nothing to do
		}

		@Override
		public String openSession(SessionContext session) throws SqlExecutionException {
			String sessionId = UUID.randomUUID().toString();
			sessionMap.put(sessionId, session);
			return sessionId;
		}

		@Override
		public void closeSession(String sessionId) throws SqlExecutionException {

		}

		@Override
		public Map<String, String> getSessionProperties(String sessionId) throws SqlExecutionException {
			return null;
		}

		@Override
		public void resetSessionProperties(String sessionId) throws SqlExecutionException {

		}

		@Override
		public void setSessionProperty(String sessionId, String key, String value) throws SqlExecutionException {

		}

		@Override
		public void addView(String sessionId, String name, String query) throws SqlExecutionException {

		}

		@Override
		public void removeView(String sessionId, String name) throws SqlExecutionException {

		}

		@Override
		public Map<String, ViewEntry> listViews(String sessionId) throws SqlExecutionException {
			return null;
		}

		@Override
		public List<String> listCatalogs(String sessionId) throws SqlExecutionException {
			return null;
		}

		@Override
		public List<String> listDatabases(String sessionId) throws SqlExecutionException {
			return null;
		}

		@Override
		public void createTable(String sessionId, String ddl) throws SqlExecutionException {

		}

		@Override
		public void dropTable(String sessionId, String ddl) throws SqlExecutionException {

		}

		@Override
		public List<String> listTables(String sessionId) throws SqlExecutionException {
			return null;
		}

		@Override
		public List<String> listUserDefinedFunctions(String sessionId) throws SqlExecutionException {
			return null;
		}

		@Override
		public List<String> listFunctions(String sessionId) throws SqlExecutionException {
			return null;
		}

		@Override
		public List<String> listModules(String sessionId) throws SqlExecutionException {
			return null;
		}

		@Override
		public void useCatalog(String sessionId, String catalogName) throws SqlExecutionException {

		}

		@Override
		public void useDatabase(String sessionId, String databaseName) throws SqlExecutionException {

		}

		@Override
		public TableSchema getTableSchema(String sessionId, String name) throws SqlExecutionException {
			return null;
		}

		@Override
		public String explainStatement(String sessionId, String statement) throws SqlExecutionException {
			return null;
		}

		@Override
		public List<String> completeStatement(String sessionId, String statement, int position) {
			receivedContext = sessionMap.get(sessionId);
			receivedStatement = statement;
			receivedPosition = position;
			return Arrays.asList("HintA", "Hint B");
		}

		@Override
		public ResultDescriptor executeQuery(String sessionId, String query) throws SqlExecutionException {
			return null;
		}

		@Override
		public TypedResult<List<Tuple2<Boolean, Row>>> retrieveResultChanges(String sessionId, String resultId) throws SqlExecutionException {
			return null;
		}

		@Override
		public TypedResult<Integer> snapshotResult(String sessionId, String resultId, int pageSize) throws SqlExecutionException {
			return null;
		}

		@Override
		public List<Row> retrieveResultPage(String resultId, int page) throws SqlExecutionException {
			return null;
		}

		@Override
		public void cancelQuery(String sessionId, String resultId) throws SqlExecutionException {
			// nothing to do
		}

		@Override
		public ProgramTargetDescriptor executeUpdate(String sessionId, String statement) throws SqlExecutionException {
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
