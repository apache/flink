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

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriver;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalEventHandler;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriver;
import org.apache.flink.runtime.rpc.DirectlyFailingFatalErrorHandler;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;

import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
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

	private final FatalErrorHandler fatalErrorHandler = DirectlyFailingFatalErrorHandler.INSTANCE;

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
		closeTestServer();

		if (zooKeeperClient != null) {
			zooKeeperClient.close();
			zooKeeperClient = null;
		}
	}

	@Test
	public void testConnectionSuspendedHandlingDuringInitialization() throws Exception {
		final QueueLeaderElectionListener queueLeaderElectionListener = new QueueLeaderElectionListener(1);

		LeaderRetrievalDriver leaderRetrievalDriver = null;
		try {
			leaderRetrievalDriver = ZooKeeperUtils
				.createLeaderRetrievalDriverFactory(zooKeeperClient, config)
				.createLeaderRetrievalDriver(queueLeaderElectionListener, fatalErrorHandler);

			// do the testing
			final CompletableFuture<String> firstAddress = queueLeaderElectionListener.next(Duration.ofMillis(50));
			assertThat("No results are expected, yet, since no leader was elected.", firstAddress, is(nullValue()));

			closeTestServer();

			// QueueLeaderElectionListener will be notified with an empty leader when ZK connection is suspended
			final CompletableFuture<String> secondAddress = queueLeaderElectionListener.next();
			assertThat("The next result must not be missing.", secondAddress, is(notNullValue()));
			assertThat("The next result is expected to be null.", secondAddress.get(), is(nullValue()));
		} finally {
			if (leaderRetrievalDriver != null) {
				leaderRetrievalDriver.close();
			}
		}
	}

	@Test
	public void testConnectionSuspendedHandling() throws Exception {
		final String retrievalPath = "/testConnectionSuspendedHandling/leaderAddress";
		final String leaderAddress = "localhost";

		final QueueLeaderElectionListener queueLeaderElectionListener = new QueueLeaderElectionListener(1);
		LeaderRetrievalDriver leaderRetrievalDriver = null;
		try {
			leaderRetrievalDriver = new ZooKeeperLeaderRetrievalDriver(
				zooKeeperClient, retrievalPath, queueLeaderElectionListener, fatalErrorHandler);

			writeLeaderInformationToZooKeeper(retrievalPath, leaderAddress, UUID.randomUUID());

			// do the testing
			CompletableFuture<String> firstAddress = queueLeaderElectionListener.next();
			assertThat("The first result is expected to be the initially set leader address.", firstAddress.get(), is(leaderAddress));

			closeTestServer();

			CompletableFuture<String> secondAddress = queueLeaderElectionListener.next();
			assertThat("The next result must not be missing.", secondAddress, is(notNullValue()));
			assertThat("The next result is expected to be null.", secondAddress.get(), is(nullValue()));
		} finally {
			if (leaderRetrievalDriver != null) {
				leaderRetrievalDriver.close();
			}
		}
	}

	@Test
	public void testSameLeaderAfterReconnectTriggersListenerNotification() throws Exception {
		final String retrievalPath = "/testSameLeaderAfterReconnectTriggersListenerNotification/leaderAddress";
		final QueueLeaderElectionListener queueLeaderElectionListener = new QueueLeaderElectionListener(1);
		LeaderRetrievalDriver leaderRetrievalDriver = null;
		try {
			leaderRetrievalDriver = new ZooKeeperLeaderRetrievalDriver(
				zooKeeperClient, retrievalPath, queueLeaderElectionListener, fatalErrorHandler);

			final String leaderAddress = "foobar";
			final UUID sessionId = UUID.randomUUID();
			writeLeaderInformationToZooKeeper(retrievalPath, leaderAddress, sessionId);

			// pop new leader
			queueLeaderElectionListener.next();

			testingServer.stop();

			final CompletableFuture<String> connectionSuspension = queueLeaderElectionListener.next();

			// wait until the ZK connection is suspended
			connectionSuspension.join();

			testingServer.restart();

			// new old leader information should be announced
			final CompletableFuture<String> connectionReconnect = queueLeaderElectionListener.next();
			assertThat(connectionReconnect.get(), is(leaderAddress));
		} finally {
			if (leaderRetrievalDriver != null) {
				leaderRetrievalDriver.close();
			}
		}
	}

	private void writeLeaderInformationToZooKeeper(
			String retrievalPath,
			String leaderAddress,
			UUID sessionId) throws Exception {
		final byte[] data = createLeaderInformation(leaderAddress, sessionId);
		if (zooKeeperClient.checkExists().forPath(retrievalPath) != null) {
			zooKeeperClient.setData().forPath(retrievalPath, data);
		} else {
			zooKeeperClient.create().creatingParentsIfNeeded().forPath(retrievalPath, data);
		}
	}

	private byte[] createLeaderInformation(String leaderAddress, UUID sessionId) throws IOException {
		try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final ObjectOutputStream oos = new ObjectOutputStream(baos)) {

			oos.writeUTF(leaderAddress);
			oos.writeObject(sessionId);

			oos.flush();

			return baos.toByteArray();
		}
	}

	@Test
	public void testNewLeaderAfterReconnectTriggersListenerNotification() throws Exception {
		final String retrievalPath = "/testNewLeaderAfterReconnectTriggersListenerNotification/leaderAddress";
		final QueueLeaderElectionListener queueLeaderElectionListener = new QueueLeaderElectionListener(1);

		LeaderRetrievalDriver leaderRetrievalDriver = null;
		try {
			leaderRetrievalDriver = new ZooKeeperLeaderRetrievalDriver(
				zooKeeperClient, retrievalPath, queueLeaderElectionListener, fatalErrorHandler);

			final String leaderAddress = "foobar";
			final UUID sessionId = UUID.randomUUID();
			writeLeaderInformationToZooKeeper(retrievalPath, leaderAddress, sessionId);

			// pop new leader
			queueLeaderElectionListener.next();

			testingServer.stop();

			final CompletableFuture<String> connectionSuspension = queueLeaderElectionListener.next();

			// wait until the ZK connection is suspended
			connectionSuspension.join();

			testingServer.restart();

			final String newLeaderAddress = "barfoo";
			final UUID newSessionId = UUID.randomUUID();
			writeLeaderInformationToZooKeeper(retrievalPath, newLeaderAddress, newSessionId);

			// check that we find the new leader information eventually
			CommonTestUtils.waitUntilCondition(
				() -> {
					final CompletableFuture<String> afterConnectionReconnect = queueLeaderElectionListener.next();
					return afterConnectionReconnect.get().equals(newLeaderAddress);
				},
				Deadline.fromNow(Duration.ofSeconds(30L)));
		} finally {
			if (leaderRetrievalDriver != null) {
				leaderRetrievalDriver.close();
			}
		}
	}

	private void closeTestServer() throws IOException {
		if (testingServer != null) {
			testingServer.close();
			testingServer = null;
		}
	}

	private static class QueueLeaderElectionListener implements LeaderRetrievalEventHandler {

		private final BlockingQueue<CompletableFuture<String>> queue;

		public QueueLeaderElectionListener(int expectedCalls) {
			this.queue = new ArrayBlockingQueue<>(expectedCalls);
		}

		@Override
		public void notifyLeaderAddress(LeaderInformation leaderInformation) {
			final String leaderAddress = leaderInformation.getLeaderAddress();
			try {
				queue.put(CompletableFuture.completedFuture(leaderAddress));
			} catch (InterruptedException e) {
				throw new IllegalStateException(e);
			}
		}

		public CompletableFuture<String> next() {
			return next(null);
		}

		public CompletableFuture<String> next(@Nullable Duration timeout) {
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
	}
}
