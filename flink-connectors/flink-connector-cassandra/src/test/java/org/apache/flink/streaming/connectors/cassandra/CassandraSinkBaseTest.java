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

import static org.mockito.Mockito.mock;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for the {@link CassandraSinkBase}.
 */
public class CassandraSinkBaseTest {

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
		}) {
			@Override
			public ListenableFuture send(Object value) {
				return null;
			}
		};

		base.open(new Configuration());
	}

	@Test(timeout = 5000)
	public void testSuccessfulPath() throws Exception {
		TestCassandraSink casSinkFunc = new TestCassandraSink();
		casSinkFunc.open(new Configuration());

		casSinkFunc.setResultFuture(ResultSetFutures.fromCompletableFuture(CompletableFuture.completedFuture(null)));
		casSinkFunc.invoke("hello");

		Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());

		casSinkFunc.close();
	}

	@Test(timeout = 5000)
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

	@Test(timeout = 5000)
	public void testThrowErrorOnInvoke() throws Exception {
		TestCassandraSink casSinkFunc = new TestCassandraSink();

		casSinkFunc.open(new Configuration());

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

	@Test(timeout = 5000)
	public void testThrowErrorOnSnapshot() throws Exception {
		TestCassandraSink casSinkFunc = new TestCassandraSink();

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(casSinkFunc));

		testHarness.open();

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

		testHarness.close();
	}

	@Test(timeout = 5000)
	public void testWaitForPendingUpdatesOnSnapshot() throws Exception {
		TestCassandraSink casSinkFunc = new TestCassandraSink();

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(casSinkFunc));

		testHarness.open();

		CompletableFuture<ResultSet> completableFuture = new CompletableFuture<>();
		ResultSetFuture resultSetFuture = ResultSetFutures.fromCompletableFuture(completableFuture);
		casSinkFunc.setResultFuture(resultSetFuture);

		casSinkFunc.invoke("hello");
		Assert.assertEquals(1, casSinkFunc.getNumOfPendingRecords());

		Thread t = new CheckedThread("Flink-CassandraSinkBaseTest") {
			@Override
			public void go() throws Exception {
				testHarness.snapshot(123L, 123L);
			}
		};
		t.start();
		while (t.getState() != Thread.State.WAITING) {
			Thread.sleep(5);
		}

		Assert.assertEquals(1, casSinkFunc.getNumOfPendingRecords());
		completableFuture.complete(null);
		Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());

		testHarness.close();
	}

	@Test(timeout = 5000)
	public void testWaitForPendingUpdatesOnClose() throws Exception {
		TestCassandraSink casSinkFunc = new TestCassandraSink();

		OneInputStreamOperatorTestHarness<String, Object> testHarness =
			new OneInputStreamOperatorTestHarness<>(new StreamSink<>(casSinkFunc));

		testHarness.open();

		CompletableFuture<ResultSet> completableFuture = new CompletableFuture<>();
		ResultSetFuture resultSetFuture = ResultSetFutures.fromCompletableFuture(completableFuture);
		casSinkFunc.setResultFuture(resultSetFuture);

		casSinkFunc.invoke("hello");
		Assert.assertEquals(1, casSinkFunc.getNumOfPendingRecords());

		Thread t = new CheckedThread("Flink-CassandraSinkBaseTest") {
			@Override
			public void go() throws Exception {
				testHarness.close();
			}
		};
		t.start();
		while (t.getState() != Thread.State.WAITING) {
			Thread.sleep(5);
		}

		Assert.assertEquals(1, casSinkFunc.getNumOfPendingRecords());
		completableFuture.complete(null);
		Assert.assertEquals(0, casSinkFunc.getNumOfPendingRecords());
	}

	private static class TestCassandraSink extends CassandraSinkBase<String, ResultSet> {

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
			super(builder);
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
