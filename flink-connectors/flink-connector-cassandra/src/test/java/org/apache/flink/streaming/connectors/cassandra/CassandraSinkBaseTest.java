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

package org.apache.flink.streaming.connectors.cassandra;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.queryablestate.FutureUtils;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Preconditions;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.util.concurrent.ListenableFuture;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for the {@link CassandraSinkBase}.
 */
public class CassandraSinkBaseTest {

	private static final long DEFAULT_TEST_TIMEOUT = 5000;

	@Test(expected = NoHostAvailableException.class)
	public void testHostNotFoundErrorHandling() throws Exception {
		CassandraSinkBase base = new CassandraSinkBase(new ClusterBuilder() {
			@Override
			protected Cluster buildCluster(Cluster.Builder builder) {
				return builder
					.addContactPoint("127.0.0.1")
					.withoutJMXReporting()
					.withoutMetrics().build();
			}
		}, new NoOpCassandraFailureHandler()) {
			@Override
			public ListenableFuture send(Object value) {
				return null;
			}
		};

		base.open(new Configuration());
	}

	@Test(timeout = DEFAULT_TEST_TIMEOUT)
	public void testSuccessfulPath() throws Exception {
		try (TestCassandraSink casSinkFunc = createOpenedTestCassandraSink()) {
			casSinkFunc.setResultFuture(ResultSetFutures.fromCompletableFuture(CompletableFuture.completedFuture(null)));
			casSinkFunc.invoke("hello");

			Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());
		}
	}

	@Test(timeout = DEFAULT_TEST_TIMEOUT)
	public void testThrowErrorOnClose() throws Exception {
		TestCassandraSink casSinkFunc = new TestCassandraSink();

		casSinkFunc.open(new Configuration());

		Exception cause = new RuntimeException();
		casSinkFunc.setResultFuture(ResultSetFutures.fromCompletableFuture(FutureUtils.getFailedFuture(cause)));
		casSinkFunc.invoke("hello");
		try {
			casSinkFunc.close();

			Assert.fail("Close should have thrown an exception.");
		} catch (IOException e) {
			Assert.assertEquals(cause, e.getCause());
			Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());
		}
	}

	@Test(timeout = DEFAULT_TEST_TIMEOUT)
	public void testThrowErrorOnInvoke() throws Exception {
		try (TestCassandraSink casSinkFunc = createOpenedTestCassandraSink()) {
			Exception cause = new RuntimeException();
			casSinkFunc.setResultFuture(ResultSetFutures.fromCompletableFuture(FutureUtils.getFailedFuture(cause)));

			casSinkFunc.invoke("hello");

			try {
				casSinkFunc.invoke("world");
				Assert.fail("Sending of second value should have failed.");
			} catch (IOException e) {
				Assert.assertEquals(cause, e.getCause());
				Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());
			}
		}
	}

	@Test(timeout = DEFAULT_TEST_TIMEOUT)
	public void testIgnoreError() throws Exception {
		Exception cause = new RuntimeException();
		CassandraFailureHandler failureHandler = failure -> Assert.assertEquals(cause, failure);

		try (TestCassandraSink casSinkFunc = createOpenedTestCassandraSink(failureHandler)) {

			casSinkFunc.setResultFuture(ResultSetFutures.fromCompletableFuture(FutureUtils.getFailedFuture(cause)));

			casSinkFunc.invoke("hello");
			casSinkFunc.invoke("world");
		}
	}

	@Test(timeout = DEFAULT_TEST_TIMEOUT)
	public void testThrowErrorOnSnapshot() throws Exception {
		TestCassandraSink casSinkFunc = new TestCassandraSink();

		try (OneInputStreamOperatorTestHarness<String, Object> testHarness = createOpenedTestHarness(casSinkFunc)) {
			Exception cause = new RuntimeException();
			casSinkFunc.setResultFuture(ResultSetFutures.fromCompletableFuture(FutureUtils.getFailedFuture(cause)));

			casSinkFunc.invoke("hello");

			try {
				testHarness.snapshot(123L, 123L);

				Assert.fail();
			} catch (Exception e) {
				Assert.assertTrue(e.getCause() instanceof IOException);
				Assert.assertEquals(cause, e.getCause().getCause());
				Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());
			}
		}
	}

	@Test(timeout = DEFAULT_TEST_TIMEOUT)
	public void testWaitForPendingUpdatesOnSnapshot() throws Exception {
		TestCassandraSink casSinkFunc = new TestCassandraSink();

		try (OneInputStreamOperatorTestHarness<String, Object> testHarness = createOpenedTestHarness(casSinkFunc)) {
			CompletableFuture<ResultSet> completableFuture = new CompletableFuture<>();
			ResultSetFuture resultSetFuture = ResultSetFutures.fromCompletableFuture(completableFuture);
			casSinkFunc.setResultFuture(resultSetFuture);

			casSinkFunc.invoke("hello");
			Assert.assertEquals(1, casSinkFunc.getNumOfPendingRecords());

			final CountDownLatch latch = new CountDownLatch(1);
			Thread t = new CheckedThread("Flink-CassandraSinkBaseTest") {
				@Override
				public void go() throws Exception {
					testHarness.snapshot(123L, 123L);
					latch.countDown();
				}
			};
			t.start();
			while (t.getState() != Thread.State.WAITING) {
				Thread.sleep(5);
			}

			Assert.assertEquals(1, casSinkFunc.getNumOfPendingRecords());
			completableFuture.complete(null);
			latch.await();
			Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());
		}
	}

	@Test(timeout = DEFAULT_TEST_TIMEOUT)
	public void testWaitForPendingUpdatesOnClose() throws Exception {
		TestCassandraSink casSinkFunc = new TestCassandraSink();

		try (OneInputStreamOperatorTestHarness<String, Object> testHarness = createOpenedTestHarness(casSinkFunc)) {

			CompletableFuture<ResultSet> completableFuture = new CompletableFuture<>();
			ResultSetFuture resultSetFuture = ResultSetFutures.fromCompletableFuture(completableFuture);
			casSinkFunc.setResultFuture(resultSetFuture);

			casSinkFunc.invoke("hello");
			Assert.assertEquals(1, casSinkFunc.getNumOfPendingRecords());

			final CountDownLatch latch = new CountDownLatch(1);
			Thread t = new CheckedThread("Flink-CassandraSinkBaseTest") {
				@Override
				public void go() throws Exception {
					testHarness.close();
					latch.countDown();
				}
			};
			t.start();
			while (t.getState() != Thread.State.WAITING) {
				Thread.sleep(5);
			}

			Assert.assertEquals(1, casSinkFunc.getNumOfPendingRecords());
			completableFuture.complete(null);
			latch.await();
			Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());
		}
	}

	private TestCassandraSink createOpenedTestCassandraSink() {
		final TestCassandraSink testCassandraSink = new TestCassandraSink();
		testCassandraSink.open(new Configuration());
		return testCassandraSink;
	}

	private TestCassandraSink createOpenedTestCassandraSink(CassandraFailureHandler failureHandler) {
		final TestCassandraSink testCassandraSink = new TestCassandraSink(failureHandler);
		testCassandraSink.open(new Configuration());
		return testCassandraSink;
	}

	private OneInputStreamOperatorTestHarness<String, Object> createOpenedTestHarness(
		TestCassandraSink testCassandraSink) throws Exception {
		final StreamSink<String> testStreamSink = new StreamSink<>(testCassandraSink);
		final OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(testStreamSink);
		testHarness.open();
		return testHarness;
	}

	private static class TestCassandraSink extends CassandraSinkBase<String, ResultSet> implements AutoCloseable {

		private static final ClusterBuilder builder;
		private static final Cluster cluster;
		private static final Session session;

		static {
			cluster = mock(Cluster.class);

			session = mock(Session.class);
			when(cluster.connect()).thenReturn(session);

			builder = new ClusterBuilder() {
				@Override
				protected Cluster buildCluster(Cluster.Builder builder) {
					return cluster;
				}
			};
		}

		private ResultSetFuture result;

		TestCassandraSink() {
			super(builder, new NoOpCassandraFailureHandler());
		}

		TestCassandraSink(CassandraFailureHandler failureHandler) {
			super(builder, failureHandler);
		}

		void setResultFuture(ResultSetFuture result) {
			Preconditions.checkNotNull(result);
			this.result = result;
		}

		@Override
		public ListenableFuture<ResultSet> send(String value) {
			return result;
		}
	}
}
