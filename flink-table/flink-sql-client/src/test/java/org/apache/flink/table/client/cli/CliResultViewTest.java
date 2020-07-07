/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.client.cli;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.client.cli.utils.TerminalUtils;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.types.Row;

import org.jline.utils.AttributedString;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

/**
 * Contains basic tests for the {@link CliResultView}.
 */
public class CliResultViewTest {

	@Test
	public void testTableResultViewKeepJobResult() throws Exception {
		testResultViewClearResult(TypedResult.endOfStream(), true, 0);
	}

	@Test
	public void testTableResultViewClearEmptyResult() throws Exception {
		testResultViewClearResult(TypedResult.empty(), true, 1);
	}

	@Test
	public void testTableResultViewClearPayloadResult() throws Exception {
		testResultViewClearResult(TypedResult.payload(1), true, 1);
	}

	@Test
	public void testChangelogResultViewKeepJobResult() throws Exception {
		testResultViewClearResult(TypedResult.endOfStream(), false, 0);
	}

	@Test
	public void testChangelogResultViewClearEmptyResult() throws Exception {
		testResultViewClearResult(TypedResult.empty(), false, 1);
	}

	@Test
	public void testChangelogResultViewClearPayloadResult() throws Exception {
		testResultViewClearResult(TypedResult.payload(Collections.emptyList()), false, 1);
	}

	private void testResultViewClearResult(TypedResult<?> typedResult, boolean isTableMode, int expectedCancellationCount) throws Exception {
		final CountDownLatch cancellationCounterLatch = new CountDownLatch(expectedCancellationCount);
		final SessionContext session = new SessionContext("test-session", new Environment());
		final MockExecutor executor = new MockExecutor(typedResult, cancellationCounterLatch);
		String sessionId = executor.openSession(session);
		final ResultDescriptor descriptor = new ResultDescriptor(
				"result-id",
				TableSchema.builder().field("Null Field", Types.STRING()).build(),
				false,
				false);

		Thread resultViewRunner = null;
		CliClient cli = null;
		try {
			cli = new CliClient(
					TerminalUtils.createDummyTerminal(),
					sessionId,
					executor,
					File.createTempFile("history", "tmp").toPath());
			resultViewRunner = new Thread(new TestingCliResultView(cli, descriptor, isTableMode));
			resultViewRunner.start();
		} finally {
			if (resultViewRunner != null && !resultViewRunner.isInterrupted()) {
				resultViewRunner.interrupt();
			}
			if (cli != null) {
				cli.close();
			}
		}

		assertTrue(
			"Invalid number of cancellations.",
			cancellationCounterLatch.await(10, TimeUnit.SECONDS));
	}

	private static final class MockExecutor implements Executor {

		private final TypedResult<?> typedResult;
		private final CountDownLatch cancellationCounter;

		public MockExecutor(TypedResult<?> typedResult, CountDownLatch cancellationCounter) {
			this.typedResult = typedResult;
			this.cancellationCounter = cancellationCounter;
		}

		@Override
		public void start() throws SqlExecutionException {
			// do nothing
		}

		@Override
		public String openSession(SessionContext session) throws SqlExecutionException {
			return UUID.randomUUID().toString();
		}

		@Override
		public void closeSession(String sessionId) throws SqlExecutionException {
			// do nothing
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
		public TableResult executeSql(String sessionId, String statement) throws SqlExecutionException {
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
		public Parser getSqlParser(String sessionId) {
			return null;
		}

		@Override
		public List<String> completeStatement(String sessionId, String statement, int position) {
			return null;
		}

		@Override
		public ResultDescriptor executeQuery(String sessionId, String query) throws SqlExecutionException {
			return null;
		}

		@Override
		@SuppressWarnings("unchecked")
		public TypedResult<List<Tuple2<Boolean, Row>>> retrieveResultChanges(String sessionId, String resultId) throws SqlExecutionException {
			return (TypedResult<List<Tuple2<Boolean, Row>>>) typedResult;
		}

		@Override
		@SuppressWarnings("unchecked")
		public TypedResult<Integer> snapshotResult(String sessionId, String resultId, int pageSize) throws SqlExecutionException {
			return (TypedResult<Integer>) typedResult;
		}

		@Override
		public List<Row> retrieveResultPage(String resultId, int page) throws SqlExecutionException {
			return Collections.singletonList(new Row(1));
		}

		@Override
		public void cancelQuery(String sessionId, String resultId) throws SqlExecutionException {
			cancellationCounter.countDown();
		}

		@Override
		public ProgramTargetDescriptor executeUpdate(String sessionId, String statement) throws SqlExecutionException {
			return null;
		}
	}

	private static final class TestingCliResultView implements Runnable {

		private final CliResultView realResultView;

		public TestingCliResultView(
			CliClient client,
			ResultDescriptor descriptor,
			boolean isTableMode) {

			if (isTableMode) {
				realResultView = new TestingCliTableResultView(client, descriptor);
			} else {
				realResultView = new TestingCliChangelogResultView(client, descriptor);
			}
		}

		@Override
		public void run() {
			realResultView.open();
		}
	}

	private static class TestingCliChangelogResultView extends CliChangelogResultView {

		public TestingCliChangelogResultView(CliClient client, ResultDescriptor resultDescriptor) {
			super(client, resultDescriptor);
		}

		@Override
		protected List<AttributedString> computeMainHeaderLines() {
			return Collections.emptyList();
		}
	}

	private static class TestingCliTableResultView extends CliTableResultView {

		public TestingCliTableResultView(CliClient client, ResultDescriptor resultDescriptor) {
			super(client, resultDescriptor);
		}

		@Override
		protected List<AttributedString> computeMainHeaderLines() {
			return Collections.emptyList();
		}
	}
}
