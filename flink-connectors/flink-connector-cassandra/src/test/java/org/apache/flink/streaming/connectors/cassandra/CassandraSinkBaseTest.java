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
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.google.common.util.concurrent.ListenableFuture;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for the {@link CassandraSinkBase}.
 */
public class CassandraSinkBaseTest {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraSinkBaseTest.class);

	private static final String DUMMY_QUERY_STMT = "CQL_Dummy_Stmt";

	private static final String DUMMY_MESSAGE = "Dummy_msg";

	private static final int MAX_THREAD_NUM = 3;

	private static final int MAX_THREAD_POOL_SIZE = 2;

	@BeforeClass
	public static void doSetUp() throws Exception {

	}

	@BeforeClass
	public static void doTearDown() throws Exception {

	}

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
		base.close();
	}


	///////////////////////
	// Single Thread Test
	///////////////////////

	/**
	 * Test ensures the message could be delivered successfully to sink.
	 */
	@Test
	public void testSimpleSuccessfulPath() throws Exception {
		ClusterBuilder builder = mock(ClusterBuilder.class);
		MockCassandraSinkBase casSinkFunc = new MockCassandraSinkBase<>(builder);
		casSinkFunc.setFlushOnCheckpoint(true);
		casSinkFunc.open(new Configuration());

		casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.IMMEDIATE_SUCCESS);

		casSinkFunc.close();

		//Final pending updates should be zero
		Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());
	}

	/**
	 * Test ensures that an asyncError would be thrown on close() if previously message delivery failed.
	 */
	@Test
	public void testAsyncErrorThrownOnClose() throws Exception {
		ClusterBuilder builder = mock(ClusterBuilder.class);
		MockCassandraSinkBase casSinkFunc = new MockCassandraSinkBase<>(builder);
		casSinkFunc.setFlushOnCheckpoint(true);

		casSinkFunc.open(new Configuration());

		casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.IMMEDIATE_FAILURE);
		try {

			casSinkFunc.close();
		} catch (IOException e) {
			//expected async error from close()

			Assert.assertTrue(e.getMessage().contains("Error while sending value"));

			//Final pending updates should be zero
			Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());

			//done
			return;
		}

		Assert.fail();
	}

	/**
	 * Test ensures that an asyncError would be thrown on invoke() if previously message delivery failed.
	 */
	//TODO: should unitfy error handling logic in CassandraSinkBase
	//Exception would have been thrown from invoke(), but asyncError was not set null, hence it was rethrown in close()
	@Ignore
	@Test
	public void testAsyncErrorThrownOnInvoke() throws Exception {
		ClusterBuilder builder = mock(ClusterBuilder.class);
		MockCassandraSinkBase casSinkFunc = new MockCassandraSinkBase<>(builder);
		casSinkFunc.setFlushOnCheckpoint(true);

		casSinkFunc.open(new Configuration());

		casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.IMMEDIATE_FAILURE);
		try {
			casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.IMMEDIATE_SUCCESS);
		} catch (IOException e) {
			//expected async error thrown from invoke()

			//Final pending updates should be zero
			Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());

			casSinkFunc.close();

			//done
			return;
		}

		Assert.fail();
	}

	/**
	 * Test ensures that an asyncError would be thrown when checkpoint performs if previously message delivery failed.
	 */
	@Test
	public void testAsyncErrorThrownOnCheckpoint() throws Exception {
		ClusterBuilder builder = mock(ClusterBuilder.class);
		MockCassandraSinkBase casSinkFunc = new MockCassandraSinkBase<>(builder);
		casSinkFunc.setFlushOnCheckpoint(true);

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(casSinkFunc));

		testHarness.open();

		casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.IMMEDIATE_FAILURE);

		try {
			testHarness.snapshot(123L, 123L);
		} catch (Exception e) {
			//expected async error from snapshotState()

			Assert.assertTrue(e.getCause() instanceof IllegalStateException);
			Assert.assertTrue(e.getCause().getMessage().contains("Failed to send data to Cassandra"));

			//Final pending updates should be zero
			Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());

			casSinkFunc.close();

			//done
			return;
		}

		Assert.fail();
	}

	///////////////////////
	// Multi Thread Test
	///////////////////////

	/**
	 * Test ensures that CassandraSinkBase would flush all in-flight message on close(), accompanied with concurrent
	 * message delivery successfully via a thraedpool.
	 */
	@Test
	public void testFlushOnPendingRecordsOnCloseWithSuccessfulMessage() throws Exception {
		ClusterBuilder builder = mock(ClusterBuilder.class);

		ExecutorService threadPool = Executors.newFixedThreadPool(MAX_THREAD_POOL_SIZE);
		MockCassandraSinkBase casSinkFunc = new MockCassandraSinkBase<>(builder);
		casSinkFunc.setFlushOnCheckpoint(true);

		casSinkFunc.open(new Configuration());

		for (int i = 0; i < MAX_THREAD_NUM; i++) {
			threadPool.submit(() -> {
				try {

					casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.DELAYED_SUCCESS);
				} catch (Exception e) {
					LOG.error("Error while dispatching sending message to Cassandra sink => {} ", e);
				}
			});
		}

		//wait until the first message has been dispatched and invoked
		Thread.sleep(500);

		casSinkFunc.close();

		threadPool.shutdown();
		threadPool.awaitTermination(10, TimeUnit.SECONDS);

		//Final pending updates should be zero
		Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());
	}

	/**
	 * Test ensures that CassandraSinkBase would flush all in-flight message when checkpoint performs, accompanied
	 * with concurrent message delivery successfully via a thraedpool.
	 */
	@Test
	public void testFlushOnPendingRecordsOnCheckpoint() throws Exception {
		ClusterBuilder builder = mock(ClusterBuilder.class);

		ExecutorService threadPool = Executors.newFixedThreadPool(MAX_THREAD_POOL_SIZE);
		MockCassandraSinkBase casSinkFunc = new MockCassandraSinkBase<>(builder);
		casSinkFunc.setFlushOnCheckpoint(true);

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
				new OneInputStreamOperatorTestHarness<>(new StreamSink<>(casSinkFunc));

		testHarness.open();

		casSinkFunc.open(new Configuration());

		for (int i = 0; i < MAX_THREAD_NUM; i++) {
			threadPool.submit(() -> {
				try {

					casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.DELAYED_SUCCESS);
				} catch (Exception e) {
					LOG.error("Error while dispatching sending message to Cassandra sink => {} ", e);
				}
			});
			Thread.sleep(500);
		}

		//wait until the first message has been dispatched and invoked
		Thread.sleep(500);

		testHarness.snapshot(123L, 123L);

		threadPool.shutdown();
		threadPool.awaitTermination(10, TimeUnit.SECONDS);

		Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());

		casSinkFunc.close();
	}

	/**
	 * Test ensures that CassandraSinkBase would NOT flush all in-flight message when checkpoint performs.
	 */
	@Test
	public void testDoNotFlushOnPendingRecordsOnCheckpoint() throws Exception {
		ClusterBuilder builder = mock(ClusterBuilder.class);

		ExecutorService threadPool = Executors.newFixedThreadPool(MAX_THREAD_POOL_SIZE);
		MockCassandraSinkBase casSinkFunc = new MockCassandraSinkBase<>(builder);
		casSinkFunc.setFlushOnCheckpoint(false);

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(casSinkFunc));

		testHarness.open();

		casSinkFunc.open(new Configuration());

		for (int i = 0; i < MAX_THREAD_NUM; i++) {
			threadPool.submit(() -> {
				try {

					casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.DELAYED_SUCCESS);
				} catch (Exception e) {
					LOG.error("Error while dispatching sending message to Cassandra sink => {} ", e);
				}
			});
			Thread.sleep(500);
		}

		//wait until the first message has been dispatched and invoked
		Thread.sleep(500);

		testHarness.snapshot(123L, 123L);
		//Final pending records # > 0
		Assert.assertTrue(casSinkFunc.getNumOfPendingRecords() > 0);

		threadPool.shutdown();
		threadPool.awaitTermination(10, TimeUnit.SECONDS);

		casSinkFunc.close();
	}

	/**
	 * Test ensures that CassandraSinkBase would flush all in-flight message on close(), accompanied with concurrent
	 * thread dispatched message failure via a thraedpool.
	 */
	@Test
	public void testFlushOnPendingRecordsOnCloseWithFailedMessage() throws Exception {
		ClusterBuilder builder = mock(ClusterBuilder.class);

		ExecutorService threadPool = Executors.newFixedThreadPool(MAX_THREAD_POOL_SIZE);
		MockCassandraSinkBase casSinkFunc = new MockCassandraSinkBase<>(builder);
		casSinkFunc.setFlushOnCheckpoint(true);

		casSinkFunc.open(new Configuration());
		try {
			for (int i = 0; i < MAX_THREAD_NUM; i++) {
				threadPool.submit(() -> {
					try {

						casSinkFunc.invokeWithImmediateResult(DUMMY_MESSAGE, PredeterminedResult.DELAYED_FAILURE);
					} catch (Exception e) {
						LOG.error("Error while dispatching sending message to Cassandra sink => {} ", e);
					}

				});
			}
			//wait until the first message has been dispatched and invoked
			Thread.sleep(500);

			casSinkFunc.close();
		} catch (IOException e) {
			//expected async error from close()
		} finally {
			//wait for all message dispatching threads to end
			threadPool.shutdown();
			threadPool.awaitTermination(10, TimeUnit.SECONDS);
		}

		//Final pending updates should be zero
		Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());
	}

	////////////////////////////////
	// Utilities
	///////////////////////////////

	private enum PredeterminedResult {
		IMMEDIATE_SUCCESS,
		IMMEDIATE_FAILURE,
		IMMEDIATE_CANCELLATION,
		DELAYED_SUCCESS,
		DELAYED_FAILURE
	}

	private static class DummyCassandraSinkBase<IN, V> extends CassandraSinkBase<IN, V> {

		@SuppressWarnings("unchecked")
		DummyCassandraSinkBase(ClusterBuilder clusterBuilder) {
			super(clusterBuilder);
		}

		@Override
		public ListenableFuture<V> send(IN value) {
			return (ListenableFuture<V>) session.executeAsync(DUMMY_QUERY_STMT);
		}

	}

	private static class MockCassandraSinkBase<IN> extends CassandraSinkBase<IN, ResultSet> {

		@SuppressWarnings("unchecked")
		MockCassandraSinkBase(ClusterBuilder clusterBuilder) {
			super(clusterBuilder);

			cluster = mock(Cluster.class);
			session = mock(Session.class);
			when(builder.getCluster()).thenReturn(cluster);
			when(cluster.connect()).thenReturn(session);
		}

		public void invokeWithImmediateResult(IN value, PredeterminedResult result) throws Exception {
			when(session.executeAsync(DUMMY_QUERY_STMT)).thenAnswer(new Answer<ResultSetFuture>() {
					@Override
					public ResultSetFuture answer(InvocationOnMock invocationOnMock) throws Throwable {
						ResultSetFuture predeterminedFuture = null;

						switch (result) {
							case IMMEDIATE_FAILURE:
								predeterminedFuture = ResultSetFutures.immediateFailedFuture(new IllegalStateException("Immediate Failure!"));
								break;

							case IMMEDIATE_CANCELLATION:
								predeterminedFuture = ResultSetFutures.immediateCancelledFuture();
								break;

							case DELAYED_FAILURE:
								predeterminedFuture = ResultSetFutures.delayedFailedFuture(new IllegalStateException("Delayed Failure!"));
								break;

							case DELAYED_SUCCESS:
								predeterminedFuture = ResultSetFutures.delayedFuture(null);
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

		@Override
		public ListenableFuture<ResultSet> send(IN value) {
			return (ListenableFuture<ResultSet>) session.executeAsync(DUMMY_QUERY_STMT);
		}

	}

	public static void main(String[] args) throws Exception {

	}

}
