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
import org.apache.flink.core.testutils.MultiShotLatch;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.util.concurrent.ListenableFuture;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for the {@link CassandraSinkBase}.
 */
public class CassandraSinkBaseTest {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraSinkBaseTest.class);

	private static final String DUMMY_QUERY_STMT = "CQL_Dummy_Stmt";

	private static final String DUMMY_MESSAGE = "Dummy_msg";

	/**
	 * Test ensures a NoHostAvailableException would be thrown if a contact point added does not exist.
	 */
	@Test(expected = NoHostAvailableException.class)
	public void testCasHostNotFoundErrorHandling() throws Exception {
		CassandraSinkBase base = new DummyCassandraSinkBase<>(new ClusterBuilder() {
			@Override
			protected Cluster buildCluster(Cluster.Builder builder) {
				return builder
					.addContactPoint("127.0.0.1")
					.withoutJMXReporting()
					.withoutMetrics().build();
			}
		});

		base.open(new Configuration());
	}

	/**
	 * Test ensures the message could be delivered successfully to sink.
	 */
	@Test
	public void testSimpleSuccessfulPath() throws Exception {
		ClusterBuilder builder = mock(ClusterBuilder.class);
		MockCassandraSinkBase casSinkFunc = new MockCassandraSinkBase(builder);
		casSinkFunc.open(new Configuration());

		casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.IMMEDIATE_SUCCESS);
		verify(casSinkFunc.getMockSession(), times(1)).executeAsync(any(String.class));

		casSinkFunc.close();

		Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());
	}

	/**
	 * Test ensures that an asyncError would be thrown on close() if previously message delivery failed.
	 */
	@Test
	public void testAsyncErrorThrownOnClose() throws Exception {
		ClusterBuilder builder = mock(ClusterBuilder.class);
		MockCassandraSinkBase casSinkFunc = new MockCassandraSinkBase(builder);

		casSinkFunc.open(new Configuration());

		casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.IMMEDIATE_FAILURE);
		try {
			casSinkFunc.close();

			Assert.fail();
		} catch (IOException e) {
			//expected async error from close()

			Assert.assertTrue(e.getMessage().contains("Error while sending value"));
			Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());
			verify(casSinkFunc.getMockSession(), times(1)).executeAsync(any(String.class));
		}

	}

	/**
	 * Test ensures that an asyncError would be thrown on invoke() if previously message delivery failed.
	 */
	//Exception would have been thrown from invoke(), but asyncError was not set null, hence it was rethrown in close()
	@Ignore
	@Test
	public void testAsyncErrorThrownOnInvoke() throws Exception {
		ClusterBuilder builder = mock(ClusterBuilder.class);
		MockCassandraSinkBase casSinkFunc = new MockCassandraSinkBase(builder);

		casSinkFunc.open(new Configuration());

		casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.IMMEDIATE_FAILURE);
		try {
			casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.IMMEDIATE_SUCCESS);

			Assert.fail();
		} catch (IOException e) {
			//expected async error thrown from invoke()

			Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());

			casSinkFunc.close();
		}
	}

	/**
	 * Test ensures that an asyncError would be thrown when checkpoint performs if previously message delivery failed.
	 */
	@Test
	public void testAsyncErrorThrownOnCheckpoint() throws Exception {
		ClusterBuilder builder = mock(ClusterBuilder.class);
		MockCassandraSinkBase casSinkFunc = new MockCassandraSinkBase(builder);

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(casSinkFunc));

		testHarness.open();

		casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.IMMEDIATE_FAILURE);

		try {
			testHarness.snapshot(123L, 123L);

			Assert.fail();
		} catch (Exception e) {
			//expected async error from snapshotState()

			Assert.assertTrue(e.getCause() instanceof IllegalStateException);
			Assert.assertTrue(e.getCause().getMessage().contains("Failed to send data to Cassandra"));
			Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());
			verify(casSinkFunc.getMockSession(), times(1)).executeAsync(any(String.class));

			casSinkFunc.close();
		}
	}

	/**
	 * Test ensures that CassandraSinkBase would flush all in-flight message on close().
	 * Add a 5000 ms timeout in case logic breaks.
	 */
	@Test(timeout = 5000)
	public void testFlushOnPendingRecordsOnCloseWithSuccessfulMessage() throws Exception {
		ClusterBuilder builder = mock(ClusterBuilder.class);

		MockCassandraSinkBase casSinkFunc = new MockCassandraSinkBase(builder, true, true);

		//Launch another thread to invoke(msg)
		CheckedThread msgThread = new CheckedThread() {
			@Override
			public void go() throws Exception {
				casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.IMMEDIATE_SUCCESS);
			}
		};
		msgThread.start();

		// the message should eventually get blocked until flushing was triggered
		while (msgThread.getState() != Thread.State.WAITING) {
			Thread.sleep(10);
		}

		Assert.assertEquals(1, casSinkFunc.getNumOfPendingRecords());
		casSinkFunc.simulateFlush();

		// all in-flight message were flushed
		Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());
		verify(casSinkFunc.getMockSession(), times(1)).executeAsync(any(String.class));
		casSinkFunc.close();
	}

	/**
	 * Test ensures that CassandraSinkBase would flush all in-flight message when checkpoint perform
	 * Add a 5000 ms timeout in case logic breaks.
	 */
	@Test(timeout = 5000)
	public void testFlushOnPendingRecordsOnCheckpoint() throws Exception {
		ClusterBuilder builder = mock(ClusterBuilder.class);
		MockCassandraSinkBase casSinkFunc = new MockCassandraSinkBase(builder, true, true);
		OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(casSinkFunc));

		testHarness.open();
		casSinkFunc.open(new Configuration());

		//Launch another thread to invoke(msg)
		CheckedThread msgThread = new CheckedThread() {
			@Override
			public void go() throws Exception {
				casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.IMMEDIATE_SUCCESS);
			}
		};
		msgThread.start();

		// the 1st message should eventually get blocked until flushing was triggered
		while (msgThread.getState() != Thread.State.WAITING) {
			Thread.sleep(10);
		}

		Assert.assertEquals(1, casSinkFunc.getNumOfPendingRecords());
		verify(casSinkFunc.getMockSession(), times(1)).executeAsync(any(String.class));

		//Launch another thread to invoke(msg)
		CheckedThread snapshotThread = new CheckedThread() {
			@Override
			public void go() throws Exception {
				testHarness.snapshot(123L, 123L);
			}
		};
		snapshotThread.start();

		//snapshot thread will eventually get blocked since there are still pending records
		while (msgThread.getState() != Thread.State.WAITING) {
			Thread.sleep(10);
		}

		// until all pending records were flushed to sink
		casSinkFunc.simulateFlush();

		Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());
	}

	/**
	 * Test ensures that CassandraSinkBase would NOT flush all in-flight message when checkpoint performs.
	 * Add a 5000 ms timeout in case logic breaks.
	 */
	@Test(timeout = 5000)
	public void testDoNotFlushOnPendingRecordsOnCheckpoint() throws Exception {
		ClusterBuilder builder = mock(ClusterBuilder.class);

		MockCassandraSinkBase casSinkFunc = new MockCassandraSinkBase(builder, false, true);

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(casSinkFunc));

		testHarness.open();

		casSinkFunc.open(new Configuration());

		//Launch another thread to invoke(msg)
		CheckedThread msgThread = new CheckedThread() {
			@Override
			public void go() throws Exception {
				casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.IMMEDIATE_SUCCESS);
			}
		};
		msgThread.start();

		// the 1st message should eventually get blocked until flushing was triggered
		while (msgThread.getState() != Thread.State.WAITING) {
			Thread.sleep(10);
		}

		Assert.assertEquals(1, casSinkFunc.getNumOfPendingRecords());

		verify(casSinkFunc.getMockSession(), times(1)).executeAsync(any(String.class));

		// This operation will not block thread
		testHarness.snapshot(123L, 123L);

		Assert.assertEquals(1, casSinkFunc.getNumOfPendingRecords());
	}

	////////////////////////////////
	// Utilities
	///////////////////////////////

	private enum PredeterminedResult {
		IMMEDIATE_SUCCESS,
		IMMEDIATE_FAILURE,
	}

	private static class DummyCassandraSinkBase<IN, V> extends CassandraSinkBase<IN, V> {

		@SuppressWarnings("unchecked")
		DummyCassandraSinkBase(ClusterBuilder clusterBuilder) {
			super(clusterBuilder, true);
		}

		@Override
		public ListenableFuture<V> send(IN value) {
			return (ListenableFuture<V>) session.executeAsync(DUMMY_QUERY_STMT);
		}

	}

	private static class MockCassandraSinkBase extends CassandraSinkBase<String, ResultSet> {

		private transient MultiShotLatch flushLatch;

		/**
		 * A message callback to wait indeifinitely on flush latch until a manual flush is triggered.
		 * Only for testing purposes.
		 */
		private class FlushLatchedMessageCallback extends DefaultMessageCallback<ResultSet> {
			@Override
			public void onSuccess(ResultSet ignored) {
				try {
					flushLatch.await();
				} catch (InterruptedException e) {
					log.error("latch await() failed: ", e);
				}

				super.onSuccess(ignored);
			}

			@Override
			public void onFailure(Throwable t) {
				try {
					flushLatch.await();
				} catch (InterruptedException e) {
					log.error("latch await() failed: ", e);
				}

				super.onFailure(t);
			}
		}

		MockCassandraSinkBase(ClusterBuilder clusterBuilder, boolean flushOnCheckpoint, boolean isWaitOnMsgCallback) {
			super(clusterBuilder, flushOnCheckpoint);

			cluster = mock(Cluster.class);
			session = mock(Session.class);
			when(builder.getCluster()).thenReturn(cluster);
			when(cluster.connect()).thenReturn(session);

			this.flushLatch = new MultiShotLatch();

			//Overwrite the default message callback
			if (isWaitOnMsgCallback) {
				this.callback = new FlushLatchedMessageCallback();
			}
		}

		MockCassandraSinkBase(ClusterBuilder clusterBuilder, boolean flushOnCheckpoint) {
			this(clusterBuilder, flushOnCheckpoint, false);
		}

		MockCassandraSinkBase(ClusterBuilder clusterBuilder) {
			this(clusterBuilder, true);
		}

		/**
		 * Intends to trigger all awaited message threads.
		 *
		 * @throws InterruptedException
		 */
		protected void simulateFlush() throws InterruptedException {
			flushLatch.trigger();
			this.flushPending();
		}

		public void invokeWithImmediateResult(String value, PredeterminedResult result) throws Exception {
			when(session.executeAsync(DUMMY_QUERY_STMT)).thenAnswer(new Answer<ResultSetFuture>() {
					@Override
					public ResultSetFuture answer(InvocationOnMock invocationOnMock) throws Throwable {
						ResultSetFuture predeterminedFuture = null;

						switch (result) {
							case IMMEDIATE_FAILURE:
								predeterminedFuture = ResultSetFutures.immediateFailedFuture(new IllegalStateException("Immediate Failure!"));
								break;
							//If not specified, set result to Successful
							default:
							case IMMEDIATE_SUCCESS:
								predeterminedFuture = ResultSetFutures.immediateFuture(null);
								break;
						}

						log.info("Invoke with {} of {}", value, result.name());

						return predeterminedFuture;
					}
				}
			);

			invoke(value);

		}

		public Session getMockSession() {
			return this.session;
		}

		@Override
		public ListenableFuture<ResultSet> send(String value) {
			return (ListenableFuture<ResultSet>) session.executeAsync(DUMMY_QUERY_STMT);
		}

	}
}
