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
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.Executor;
import org.apache.flink.table.client.gateway.ProgramTargetDescriptor;
import org.apache.flink.table.client.gateway.ResultDescriptor;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.SqlExecutionException;
import org.apache.flink.table.client.gateway.TypedResult;
import org.apache.flink.types.Row;

import org.jline.utils.AttributedString;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 * Contains basic tests for the {@link CliResultView}.
 */
public class CliResultViewTest {

	@Test
	public void tesTableResultViewKeepJobResult() throws Exception {
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

	private void testResultViewClearResult(TypedResult typedResult, boolean isTableMode, int expectedCount) throws Exception {
		final AtomicInteger counter = new AtomicInteger(0);
		final CountDownLatch signal = new CountDownLatch(1);
		final SessionContext session = new SessionContext("test-session", new Environment());
		final MockExecutor executor = new MockExecutor(
			counter, typedResult, signal);

		ResultDescriptor descriptor = executor.executeQuery(session, "");

		Thread cliTableResultViewRunner = null;
		try {
			cliTableResultViewRunner = new Thread(new TestingCliResultView(executor, session, descriptor, isTableMode));
			cliTableResultViewRunner.start();
		} catch (SqlExecutionException e) {
			fail("testResultViewClearResult failed : " + e.getMessage());
		} finally {
			if (cliTableResultViewRunner != null && !cliTableResultViewRunner.isInterrupted()) {
				cliTableResultViewRunner.interrupt();
			}
		}

		signal.await(3, TimeUnit.SECONDS);
		assertEquals(expectedCount, counter.get());
	}

	private static final class MockExecutor implements Executor {

		private final AtomicInteger counter;
		private final TypedResult typedResult;
		private final CountDownLatch countDownLatch;

		public MockExecutor(AtomicInteger counter, TypedResult typedResult, CountDownLatch countDownLatch) {
			this.counter = counter;
			this.typedResult = typedResult;
			this.countDownLatch = countDownLatch;
		}

		@Override
		public void start() throws SqlExecutionException {

		}

		@Override
		public Map<String, String> getSessionProperties(SessionContext session) throws SqlExecutionException {
			return null;
		}

		@Override
		public List<String> listTables(SessionContext session) throws SqlExecutionException {
			return null;
		}

		@Override
		public List<String> listUserDefinedFunctions(SessionContext session) throws SqlExecutionException {
			return null;
		}

		@Override
		public TableSchema getTableSchema(SessionContext session, String name) throws SqlExecutionException {
			return null;
		}

		@Override
		public String explainStatement(SessionContext session, String statement) throws SqlExecutionException {
			return null;
		}

		@Override
		public List<String> completeStatement(SessionContext session, String statement, int position) {
			return null;
		}

		@Override
		public ResultDescriptor executeQuery(SessionContext session, String query) throws SqlExecutionException {
			return new ResultDescriptor("", mock(TableSchema.class), false);
		}

		@Override
		public TypedResult<List<Tuple2<Boolean, Row>>> retrieveResultChanges(SessionContext session, String resultId) throws SqlExecutionException {
			return this.typedResult;
		}

		@Override
		public TypedResult<Integer> snapshotResult(SessionContext session, String resultId, int pageSize) throws SqlExecutionException {
			return this.typedResult;
		}

		@Override
		public List<Row> retrieveResultPage(String resultId, int page) throws SqlExecutionException {
			return Collections.singletonList(new Row(1));
		}

		@Override
		public void cancelQuery(SessionContext session, String resultId) throws SqlExecutionException {
			counter.incrementAndGet();
			countDownLatch.countDown();
		}

		@Override
		public ProgramTargetDescriptor executeUpdate(SessionContext session, String statement) throws SqlExecutionException {
			return null;
		}

		@Override
		public void validateSession(SessionContext session) throws SqlExecutionException {

		}

		@Override
		public void stop(SessionContext session) {

		}
	}

	private static final class TestingCliResultView implements Runnable {

		private final CliResultView realResultView;

		public TestingCliResultView(
			Executor executor,
			SessionContext context,
			ResultDescriptor descriptor,
			boolean isTableMode) {

			if (isTableMode) {
				realResultView = new TestingCliTableResultView(new CliClient(context, executor), descriptor);
			} else {
				realResultView = new TestingCliChangelogResultView(new CliClient(context, executor), descriptor);
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
			return Collections.EMPTY_LIST;
		}
	}

	private static class TestingCliTableResultView extends CliTableResultView {

		public TestingCliTableResultView(CliClient client, ResultDescriptor resultDescriptor) {
			super(client, resultDescriptor);
		}

		@Override
		protected List<AttributedString> computeMainHeaderLines() {
			return Collections.EMPTY_LIST;
		}
	}

}
