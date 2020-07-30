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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalService;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Tests for the error handling in case of a suspended connection to the ZooKeeper instance.
 */
public class ZooKeeperLeaderElectionConnectionHandlingTest extends TestLogger {

	private TestingServer testingServer;

	private Configuration config;

	private CuratorFramework zooKeeperClient;

	@Before
	public void before() throws Exception {
		testingServer = new TestingServer();

		config = new Configuration();
		config.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
		config.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, testingServer.getConnectString());

		zooKeeperClient = ZooKeeperUtils.startCuratorFramework(config);
	}

	@After
	public void after() throws Exception {
		stopTestServer();

		if (zooKeeperClient != null) {
			zooKeeperClient.close();
			zooKeeperClient = null;
		}
	}

	@Test
	public void testConnectionSuspendedHandlingDuringInitialization() throws Exception {
		QueueLeaderElectionListener queueLeaderElectionListener = new QueueLeaderElectionListener(1, Duration.ofMillis(50));

		ZooKeeperLeaderRetrievalService testInstance = ZooKeeperUtils.createLeaderRetrievalService(zooKeeperClient, config);
		testInstance.start(queueLeaderElectionListener);

		// do the testing
		CompletableFuture<String> firstAddress = queueLeaderElectionListener.next();
		assertThat("No results are expected, yet, since no leader was elected.", firstAddress, is(nullValue()));

		stopTestServer();

		CompletableFuture<String> secondAddress = queueLeaderElectionListener.next();
		assertThat("No result is expected since there was no leader elected before stopping the server, yet.", secondAddress, is(nullValue()));
	}

	@Test
	public void testConnectionSuspendedHandling() throws Exception {
		String leaderAddress = "localhost";
		LeaderElectionService leaderElectionService = ZooKeeperUtils.createLeaderElectionService(zooKeeperClient, config);
		TestingContender contender = new TestingContender(leaderAddress, leaderElectionService);
		leaderElectionService.start(contender);

		QueueLeaderElectionListener queueLeaderElectionListener = new QueueLeaderElectionListener(2);

		ZooKeeperLeaderRetrievalService testInstance = ZooKeeperUtils.createLeaderRetrievalService(zooKeeperClient, config);
		testInstance.start(queueLeaderElectionListener);

		// do the testing
		CompletableFuture<String> firstAddress = queueLeaderElectionListener.next();
		assertThat("The first result is expected to be the initially set leader address.", firstAddress.get(), is(leaderAddress));

		stopTestServer();

		CompletableFuture<String> secondAddress = queueLeaderElectionListener.next();
		assertThat("The next result must not be missing.", secondAddress, is(notNullValue()));
		assertThat("The next result is expected to be null.", secondAddress.get(), is(nullValue()));
	}

	private void stopTestServer() throws IOException {
		if (testingServer != null) {
			testingServer.stop();
			testingServer = null;
		}
	}

	private static class QueueLeaderElectionListener implements LeaderRetrievalListener {

		private final BlockingQueue<CompletableFuture<String>> queue;
		private final Duration timeout;

		public QueueLeaderElectionListener(int expectedCalls) {
			this(expectedCalls, null);
		}

		public QueueLeaderElectionListener(int expectedCalls, Duration timeout) {
			this.queue = new ArrayBlockingQueue<>(expectedCalls);
			this.timeout = timeout;
		}

		@Override
		public void notifyLeaderAddress(String leaderAddress, UUID leaderSessionID) {
			try {
				if (timeout == null) {
					queue.put(CompletableFuture.completedFuture(leaderAddress));
				} else {
					queue.offer(CompletableFuture.completedFuture(leaderAddress), timeout.toMillis(), TimeUnit.MILLISECONDS);
				}
			} catch (InterruptedException e) {
				throw new IllegalStateException(e);
			}
		}

		public CompletableFuture<String> next() {
			try {
				if (timeout == null) {
					return queue.take();
				} else {
					return this.queue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
				}
			} catch (InterruptedException e) {
				throw new IllegalStateException(e);
			}
		}

		@Override
		public void handleError(Exception exception) {
			throw new UnsupportedOperationException("handleError(Exception) shouldn't have been called, but it was triggered anyway.", exception);
		}
	}
}
