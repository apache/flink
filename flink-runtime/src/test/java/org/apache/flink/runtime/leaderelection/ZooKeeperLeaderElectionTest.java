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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.test.TestingServer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalService;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.TestLogger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.Promise;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class ZooKeeperLeaderElectionTest extends TestLogger {
	private TestingServer testingServer;

	private Configuration configuration;

	private CuratorFramework client;

	private static final String TEST_URL = "akka//user/jobmanager";
	private static final FiniteDuration timeout = new FiniteDuration(200, TimeUnit.SECONDS);

	private static Logger LOG = LoggerFactory.getLogger(ZooKeeperLeaderElectionTest.class);

	@Before
	public void before() {
		try {
			testingServer = new TestingServer();
		} catch (Exception e) {
			throw new RuntimeException("Could not start ZooKeeper testing cluster.", e);
		}

		configuration = new Configuration();

		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, testingServer.getConnectString());
		configuration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");

		client = ZooKeeperUtils.startCuratorFramework(configuration);
	}

	@After
	public void after() throws IOException {
		if (client != null) {
			client.close();
			client = null;
		}

		if (testingServer != null) {
			testingServer.stop();
			testingServer = null;
		}
	}

	/**
	 * Tests that the ZooKeeperLeaderElection/RetrievalService return both the correct URL.
	 */
	@Test
	public void testZooKeeperLeaderElectionRetrieval() throws Exception {
		ZooKeeperLeaderElectionService leaderElectionService = null;
		ZooKeeperLeaderRetrievalService leaderRetrievalService = null;

		try {
			leaderElectionService = ZooKeeperUtils.createLeaderElectionService(client, configuration);
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(client, configuration);

			TestingContender contender = new TestingContender(TEST_URL, leaderElectionService);
			TestingListener listener = new TestingListener();

			leaderElectionService.start(contender);
			leaderRetrievalService.start(listener);

			contender.waitForLeader(timeout.toMillis());

			assertTrue(contender.isLeader());
			assertEquals(leaderElectionService.getLeaderSessionID(), contender.getLeaderSessionID());

			listener.waitForNewLeader(timeout.toMillis());

			assertEquals(TEST_URL, listener.getAddress());
			assertEquals(leaderElectionService.getLeaderSessionID(), listener.getLeaderSessionID());

		} finally {
			if (leaderElectionService != null) {
				leaderElectionService.stop();
			}

			if (leaderRetrievalService != null) {
				leaderRetrievalService.stop();
			}
		}
	}

	/**
	 * Tests repeatedly the reelection of still available LeaderContender. After a contender has
	 * been elected as the leader, it is removed. This forces the ZooKeeperLeaderElectionService
	 * to elect a new leader.
	 */
	@Test
	public void testZooKeeperReelection() throws Exception {
		Deadline deadline = new FiniteDuration(5, TimeUnit.MINUTES).fromNow();

		int num = 10;

		ZooKeeperLeaderElectionService[] leaderElectionService = new ZooKeeperLeaderElectionService[num];
		TestingContender[] contenders = new TestingContender[num];
		ZooKeeperLeaderRetrievalService leaderRetrievalService = null;

		TestingListener listener = new TestingListener();

		try {
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(client, configuration);

			LOG.debug("Start leader retrieval service for the TestingListener.");

			leaderRetrievalService.start(listener);

			for (int i = 0; i < num; i++) {
				leaderElectionService[i] = ZooKeeperUtils.createLeaderElectionService(client, configuration);
				contenders[i] = new TestingContender(TEST_URL + "_" + i, leaderElectionService[i]);

				LOG.debug("Start leader election service for contender #{}.", i);

				leaderElectionService[i].start(contenders[i]);
			}

			String pattern = TEST_URL + "_" + "(\\d+)";
			Pattern regex = Pattern.compile(pattern);

			int numberSeenLeaders = 0;

			while (deadline.hasTimeLeft() && numberSeenLeaders < num) {
				LOG.debug("Wait for new leader #{}.", numberSeenLeaders);
				String address = listener.waitForNewLeader(deadline.timeLeft().toMillis());

				Matcher m = regex.matcher(address);

				if (m.find()) {
					int index = Integer.parseInt(m.group(1));

					TestingContender contender = contenders[index];

					// check that the retrieval service has retrieved the correct leader
					if (address.equals(contender.getAddress()) && listener.getLeaderSessionID().equals(contender.getLeaderSessionID())) {
						// kill the election service of the leader
						LOG.debug("Stop leader election service of contender #{}.", numberSeenLeaders);
						leaderElectionService[index].stop();
						leaderElectionService[index] = null;

						numberSeenLeaders++;
					}
				} else {
					fail("Did not find the leader's index.");
				}
			}

			assertFalse("Did not complete the leader reelection in time.", deadline.isOverdue());
			assertEquals(num, numberSeenLeaders);

		} finally {
			if (leaderRetrievalService != null) {
				leaderRetrievalService.stop();
			}

			for (ZooKeeperLeaderElectionService electionService : leaderElectionService) {
				if (electionService != null) {
					electionService.stop();
				}
			}
		}
	}

	/**
	 * Tests the repeated reelection of {@link LeaderContender} once the current leader dies.
	 * Furthermore, it tests that new LeaderElectionServices can be started later on and that they
	 * successfully register at ZooKeeper and take part in the leader election.
	 */
	@Test
	public void testZooKeeperReelectionWithReplacement() throws Exception {
		int num = 3;
		int numTries = 30;

		ZooKeeperLeaderElectionService[] leaderElectionService = new ZooKeeperLeaderElectionService[num];
		TestingContender[] contenders = new TestingContender[num];
		ZooKeeperLeaderRetrievalService leaderRetrievalService = null;

		TestingListener listener = new TestingListener();

		try {
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(client, configuration);

			leaderRetrievalService.start(listener);

			for (int i = 0; i < num; i++) {
				leaderElectionService[i] = ZooKeeperUtils.createLeaderElectionService(client, configuration);
				contenders[i] = new TestingContender(TEST_URL + "_" + i + "_0", leaderElectionService[i]);

				leaderElectionService[i].start(contenders[i]);
			}

			String pattern = TEST_URL + "_" + "(\\d+)" + "_" + "(\\d+)";
			Pattern regex = Pattern.compile(pattern);

			for (int i = 0; i < numTries; i++) {
				listener.waitForNewLeader(timeout.toMillis());

				String address = listener.getAddress();

				Matcher m = regex.matcher(address);

				if (m.find()) {
					int index = Integer.parseInt(m.group(1));
					int lastTry = Integer.parseInt(m.group(2));

					assertEquals(listener.getLeaderSessionID(), contenders[index].getLeaderSessionID());

					// stop leader election service = revoke leadership
					leaderElectionService[index].stop();
					// create new leader election service which takes part in the leader election
					leaderElectionService[index] = ZooKeeperUtils.createLeaderElectionService(client, configuration);
					contenders[index] = new TestingContender(
							TEST_URL + "_" + index + "_" + (lastTry + 1),
							leaderElectionService[index]);

					leaderElectionService[index].start(contenders[index]);
				} else {
					throw new Exception("Did not find the leader's index.");
				}
			}

		} finally {
			if (leaderRetrievalService != null) {
				leaderRetrievalService.stop();
			}

			for (ZooKeeperLeaderElectionService electionService : leaderElectionService) {
				if (electionService != null) {
					electionService.stop();
				}
			}
		}
	}

	/**
	 * Tests that the current leader is notified when his leader connection information in ZooKeeper
	 * are overwritten. The leader must re-establish the correct leader connection information in
	 * ZooKeeper.
	 */
	@Test
	public void testMultipleLeaders() throws Exception {
		final String FAULTY_CONTENDER_URL = "faultyContender";
		final String leaderPath = "/leader";

		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_LEADER_PATH, leaderPath);

		ZooKeeperLeaderElectionService leaderElectionService = null;
		ZooKeeperLeaderRetrievalService leaderRetrievalService = null;
		ZooKeeperLeaderRetrievalService leaderRetrievalService2 = null;
		TestingListener listener = new TestingListener();
		TestingListener listener2 = new TestingListener();
		TestingContender contender;

		try {
			leaderElectionService = ZooKeeperUtils.createLeaderElectionService(client, configuration);
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(client, configuration);
			leaderRetrievalService2 = ZooKeeperUtils.createLeaderRetrievalService(client, configuration);

			contender = new TestingContender(TEST_URL, leaderElectionService);

			leaderElectionService.start(contender);
			leaderRetrievalService.start(listener);

			listener.waitForNewLeader(timeout.toMillis());

			assertEquals(listener.getLeaderSessionID(), contender.getLeaderSessionID());
			assertEquals(TEST_URL, listener.getAddress());

			CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration);

			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(baos);

			oos.writeUTF(FAULTY_CONTENDER_URL);
			oos.writeObject(null);

			oos.close();

			// overwrite the current leader address, the leader should notice that and correct it
			boolean dataWritten = false;

			while(!dataWritten) {
				client.delete().forPath(leaderPath);

				try {
					client.create().forPath(leaderPath, baos.toByteArray());

					dataWritten = true;
				} catch (KeeperException.NodeExistsException e) {
					// this can happen if the leader election service was faster
				}
			}

			leaderRetrievalService2.start(listener2);

			listener2.waitForNewLeader(timeout.toMillis());

			if (FAULTY_CONTENDER_URL.equals(listener2.getAddress())) {
				listener2.waitForNewLeader(timeout.toMillis());
			}

			assertEquals(listener2.getLeaderSessionID(), contender.getLeaderSessionID());
			assertEquals(listener2.getAddress(), contender.getAddress());

		} finally {
			if (leaderElectionService != null) {
				leaderElectionService.stop();
			}

			if (leaderRetrievalService != null) {
				leaderRetrievalService.stop();
			}

			if (leaderRetrievalService2 != null) {
				leaderRetrievalService2.stop();
			}
		}
	}

	/**
	 *  Test that errors in the {@link LeaderElectionService} are correctly forwarded to the
	 *  {@link LeaderContender}.
	 */
	@Test
	public void testExceptionForwarding() throws Exception {
		ZooKeeperLeaderElectionService leaderElectionService = null;
		ZooKeeperLeaderRetrievalService leaderRetrievalService = null;
		TestingListener listener = new TestingListener();
		TestingContender testingContender;

		CuratorFramework client;
		final CreateBuilder mockCreateBuilder = mock(CreateBuilder.class, Mockito.RETURNS_DEEP_STUBS);
		final Exception testException = new Exception("Test exception");

		try {
			client = spy(ZooKeeperUtils.startCuratorFramework(configuration));

			Answer<CreateBuilder> answer = new Answer<CreateBuilder>() {
				private int counter = 0;

				@Override
				public CreateBuilder answer(InvocationOnMock invocation) throws Throwable {
					counter++;

					// at first we have to create the leader latch, there it mustn't fail yet
					if (counter < 2) {
						return (CreateBuilder) invocation.callRealMethod();
					} else {
						return mockCreateBuilder;
					}
				}
			};

			doAnswer(answer).when(client).create();

			when(
				mockCreateBuilder
				.creatingParentsIfNeeded()
				.withMode(Matchers.any(CreateMode.class))
				.forPath(anyString(), any(byte[].class))).thenThrow(testException);

			leaderElectionService = new ZooKeeperLeaderElectionService(client, "/latch", "/leader");
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(client, configuration);

			testingContender = new TestingContender(TEST_URL, leaderElectionService);

			leaderElectionService.start(testingContender);
			leaderRetrievalService.start(listener);

			testingContender.waitForError(timeout.toMillis());

			assertNotNull(testingContender.getError());
			assertEquals(testException, testingContender.getError().getCause());
		} finally {
			if (leaderElectionService != null) {
				leaderElectionService.stop();
			}

			if (leaderRetrievalService != null) {
				leaderRetrievalService.stop();
			}
		}
	}

	/**
	 * Tests that there is no information left in the ZooKeeper cluster after the ZooKeeper client
	 * has terminated. In other words, checks that the ZooKeeperLeaderElection service uses
	 * ephemeral nodes.
	 */
	@Test
	public void testEphemeralZooKeeperNodes() throws Exception {
		ZooKeeperLeaderElectionService leaderElectionService;
		ZooKeeperLeaderRetrievalService leaderRetrievalService = null;
		TestingContender testingContender;
		TestingListener listener;

		CuratorFramework client = null;
		CuratorFramework client2 = null;
		NodeCache cache = null;

		try {
			client = ZooKeeperUtils.startCuratorFramework(configuration);
			client2 = ZooKeeperUtils.startCuratorFramework(configuration);

			leaderElectionService = ZooKeeperUtils.createLeaderElectionService(client, configuration);
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(client2, configuration);
			testingContender = new TestingContender(TEST_URL, leaderElectionService);
			listener = new TestingListener();

			final String leaderPath = configuration.getString(HighAvailabilityOptions.HA_ZOOKEEPER_LEADER_PATH);
			cache = new NodeCache(client2, leaderPath);

			ExistsCacheListener existsListener = new ExistsCacheListener(cache);
			DeletedCacheListener deletedCacheListener = new DeletedCacheListener(cache);

			cache.getListenable().addListener(existsListener);
			cache.start();

			leaderElectionService.start(testingContender);

			testingContender.waitForLeader(timeout.toMillis());

			Future<Boolean> existsFuture = existsListener.nodeExists();

			Await.result(existsFuture, timeout);

			cache.getListenable().addListener(deletedCacheListener);

			leaderElectionService.stop();

			// now stop the underlying client
			client.close();

			Future<Boolean> deletedFuture = deletedCacheListener.nodeDeleted();

			// make sure that the leader node has been deleted
			Await.result(deletedFuture, timeout);

			leaderRetrievalService.start(listener);

			try {
				listener.waitForNewLeader(1000L);

				fail("TimeoutException was expected because there is no leader registered and " +
						"thus there shouldn't be any leader information in ZooKeeper.");
			} catch (TimeoutException e) {
				//that was expected
			}
		} finally {
			if(leaderRetrievalService != null) {
				leaderRetrievalService.stop();
			}

			if (cache != null) {
				cache.close();
			}

			if (client2 != null) {
				client2.close();
			}
		}
	}

	public static class ExistsCacheListener implements NodeCacheListener {

		final Promise<Boolean> existsPromise = new scala.concurrent.impl.Promise.DefaultPromise<>();

		final NodeCache cache;

		public ExistsCacheListener(final NodeCache cache) {
			this.cache = cache;
		}

		public Future<Boolean> nodeExists() {
			return existsPromise.future();
		}

		@Override
		public void nodeChanged() throws Exception {
			ChildData data = cache.getCurrentData();

			if (data != null && !existsPromise.isCompleted()) {
				existsPromise.success(true);
				cache.getListenable().removeListener(this);
			}
		}
	}

	public static class DeletedCacheListener implements NodeCacheListener {

		final Promise<Boolean> deletedPromise = new scala.concurrent.impl.Promise.DefaultPromise<>();

		final NodeCache cache;

		public DeletedCacheListener(final NodeCache cache) {
			this.cache = cache;
		}

		public Future<Boolean> nodeDeleted() {
			return deletedPromise.future();
		}

		@Override
		public void nodeChanged() throws Exception {
			ChildData data = cache.getCurrentData();

			if (data == null && !deletedPromise.isCompleted()) {
				deletedPromise.success(true);
				cache.getListenable().removeListener(this);
			}
		}
	}
}
