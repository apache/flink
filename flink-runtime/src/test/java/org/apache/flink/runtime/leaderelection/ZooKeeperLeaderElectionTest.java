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
import org.apache.curator.framework.api.ProtectACLCreateModePathAndBytesable;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.test.TestingCluster;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalService;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.TestLogger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.Promise;
import scala.concurrent.duration.FiniteDuration;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class ZooKeeperLeaderElectionTest extends TestLogger {
	private TestingCluster testingCluster;
	private static final String TEST_URL = "akka//user/jobmanager";
	private static final FiniteDuration timeout = new FiniteDuration(20000, TimeUnit.MILLISECONDS);

	@Before
	public void before() {
		testingCluster = new TestingCluster(3);

		try {
			testingCluster.start();
		} catch (Exception e) {
			throw new RuntimeException("Could not start ZooKeeper testing cluster.", e);
		}
	}

	@After
	public void after() {
		try {
			testingCluster.stop();
		} catch (Exception e) {
			throw new RuntimeException("Could not stop ZooKeeper testing cluster.", e);
		}

		testingCluster = null;
	}

	/**
	 * Tests that the ZooKeeperLeaderElection/RetrievalService return both the correct URL.
	 */
	@Test
	public void testZooKeeperLeaderElectionRetrieval() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(ConfigConstants.ZOOKEEPER_QUORUM_KEY, testingCluster.getConnectString());
		configuration.setString(ConfigConstants.RECOVERY_MODE, "zookeeper");

		ZooKeeperLeaderElectionService leaderElectionService = null;
		ZooKeeperLeaderRetrievalService leaderRetrievalService = null;

		try {
			leaderElectionService = ZooKeeperUtils.createLeaderElectionService(configuration);
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(configuration);

			TestingContender contender = new TestingContender(TEST_URL, leaderElectionService);
			TestingListener listener = new TestingListener();

			leaderElectionService.start(contender);
			leaderRetrievalService.start(listener);

			contender.waitForLeader(timeout.toMillis());

			assertTrue(contender.isLeader());
			assertEquals(leaderElectionService.getLeaderSessionID(), contender.getLeaderSessionID());

			listener.waitForLeader(timeout.toMillis());

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
		Configuration configuration = new Configuration();
		configuration.setString(ConfigConstants.ZOOKEEPER_QUORUM_KEY, testingCluster.getConnectString());
		configuration.setString(ConfigConstants.RECOVERY_MODE, "zookeeper");

		int num = 100;

		ZooKeeperLeaderElectionService[] leaderElectionService = new ZooKeeperLeaderElectionService[num];
		TestingContender[] contenders = new TestingContender[num];
		ZooKeeperLeaderRetrievalService leaderRetrievalService = null;

		TestingListener listener = new TestingListener();

		try {
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(configuration);

			leaderRetrievalService.start(listener);

			for (int i = 0; i < num; i++) {
				leaderElectionService[i] = ZooKeeperUtils.createLeaderElectionService(configuration);
				contenders[i] = new TestingContender(TEST_URL + "_" + i, leaderElectionService[i]);

				leaderElectionService[i].start(contenders[i]);
			}

			String pattern = TEST_URL + "_" + "(\\d+)";
			Pattern regex = Pattern.compile(pattern);

			for (int i = 0; i < num; i++) {
				listener.waitForNewLeader(timeout.toMillis());

				String address = listener.getAddress();

				Matcher m = regex.matcher(address);

				if (m.find()) {
					int index = Integer.parseInt(m.group(1));

					// check that the leader session ID of the listeners and the leader are equal
					assertEquals(listener.getLeaderSessionID(), contenders[index].getLeaderSessionID());
					assertEquals(TEST_URL + "_" + index, listener.getAddress());

					// kill the election service of the leader
					leaderElectionService[index].stop();
					leaderElectionService[index] = null;
				} else {
					fail("Did not find the leader's index.");
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
	 * Tests the repeated reelection of {@link LeaderContender} once the current leader dies.
	 * Furthermore, it tests that new LeaderElectionServices can be started later on and that they
	 * successfully register at ZooKeeper and take part in the leader election.
	 */
	@Test
	public void testZooKeeperReelectionWithReplacement() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(ConfigConstants.ZOOKEEPER_QUORUM_KEY, testingCluster.getConnectString());
		configuration.setString(ConfigConstants.RECOVERY_MODE, "zookeeper");

		int num = 3;
		int numTries = 30;

		ZooKeeperLeaderElectionService[] leaderElectionService = new ZooKeeperLeaderElectionService[num];
		TestingContender[] contenders = new TestingContender[num];
		ZooKeeperLeaderRetrievalService leaderRetrievalService = null;

		TestingListener listener = new TestingListener();

		try {
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(configuration);

			leaderRetrievalService.start(listener);

			for (int i = 0; i < num; i++) {
				leaderElectionService[i] = ZooKeeperUtils.createLeaderElectionService(configuration);
				contenders[i] = new TestingContender(TEST_URL + "_" + i + "_0", leaderElectionService[i]);

				leaderElectionService[i].start(contenders[i]);
			}

			String pattern = TEST_URL + "_" + "(\\d+)" + "_" + "(\\d+)";
			Pattern regex = Pattern.compile(pattern);

			for (int i = 0; i < numTries; i++) {
				listener.waitForLeader(timeout.toMillis());

				String address = listener.getAddress();

				Matcher m = regex.matcher(address);

				if (m.find()) {
					int index = Integer.parseInt(m.group(1));
					int lastTry = Integer.parseInt(m.group(2));

					assertEquals(listener.getLeaderSessionID(), contenders[index].getLeaderSessionID());

					// clear the current leader of the listener
					listener.clear();

					// stop leader election service = revoke leadership
					leaderElectionService[index].stop();
					// create new leader election service which takes part in the leader election
					leaderElectionService[index] = ZooKeeperUtils.createLeaderElectionService(configuration);
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

		Configuration configuration = new Configuration();
		configuration.setString(ConfigConstants.ZOOKEEPER_QUORUM_KEY, testingCluster.getConnectString());
		configuration.setString(ConfigConstants.RECOVERY_MODE, "zookeeper");
		configuration.setString(ConfigConstants.ZOOKEEPER_LEADER_PATH, leaderPath);

		ZooKeeperLeaderElectionService leaderElectionService = null;
		ZooKeeperLeaderRetrievalService leaderRetrievalService = null;
		TestingListener listener = new TestingListener();
		TestingContender contender;

		try {
			leaderElectionService = ZooKeeperUtils.createLeaderElectionService(configuration);
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(configuration);

			contender = new TestingContender(TEST_URL, leaderElectionService);

			leaderElectionService.start(contender);
			leaderRetrievalService.start(listener);

			listener.waitForLeader(timeout.toMillis());

			assertEquals(listener.getLeaderSessionID(), contender.getLeaderSessionID());
			assertEquals(TEST_URL, listener.getAddress());

			listener.clear();

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

			listener.waitForLeader(timeout.toMillis());

			if (FAULTY_CONTENDER_URL.equals(listener.getAddress())) {
				listener.clear();
				listener.waitForLeader(timeout.toMillis());
			}

			assertEquals(listener.getLeaderSessionID(), contender.getLeaderSessionID());
			assertEquals(listener.getAddress(), contender.getAddress());

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
	 *  Test that errors in the {@link LeaderElectionService} are correctly forwarded to the
	 *  {@link LeaderContender}.
	 */
	@Test
	public void testExceptionForwarding() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(ConfigConstants.ZOOKEEPER_QUORUM_KEY, testingCluster.getConnectString());
		configuration.setString(ConfigConstants.RECOVERY_MODE, "zookeeper");

		ZooKeeperLeaderElectionService leaderElectionService = null;
		ZooKeeperLeaderRetrievalService leaderRetrievalService = null;
		TestingListener listener = new TestingListener();
		TestingContender testingContender;

		CuratorFramework client;
		final CreateBuilder mockCreateBuilder = mock(CreateBuilder.class);
		final ProtectACLCreateModePathAndBytesable<String> mockCreateParentsIfNeeded = mock (ProtectACLCreateModePathAndBytesable.class);
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

			when(mockCreateBuilder.creatingParentsIfNeeded()).thenReturn(mockCreateParentsIfNeeded);
			when(mockCreateParentsIfNeeded.withMode(Matchers.any(CreateMode.class))).thenReturn(mockCreateParentsIfNeeded);
			when(mockCreateParentsIfNeeded.forPath(Matchers.any(String.class),  Matchers.any(byte[].class))).thenThrow(testException);

			leaderElectionService = new ZooKeeperLeaderElectionService(client, "/latch", "/leader");
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(configuration);

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
	 * Tests that there is no information left in the ZooKeeper cluster after all JobManagers
	 * have terminated. In other words, checks that the ZooKeeperLeaderElection service uses
	 * ephemeral nodes.
	 */
	@Test
	public void testEphemeralZooKeeperNodes() throws Exception {
		Configuration configuration = new Configuration();
		configuration.setString(ConfigConstants.ZOOKEEPER_QUORUM_KEY, testingCluster.getConnectString());
		configuration.setString(ConfigConstants.RECOVERY_MODE, "zookeeper");

		ZooKeeperLeaderElectionService leaderElectionService;
		ZooKeeperLeaderRetrievalService leaderRetrievalService = null;
		TestingContender testingContender;
		TestingListener listener;

		CuratorFramework client = null;
		NodeCache cache = null;

		try {
			leaderElectionService = ZooKeeperUtils.createLeaderElectionService(configuration);
			leaderRetrievalService = ZooKeeperUtils.createLeaderRetrievalService(configuration);
			testingContender = new TestingContender(TEST_URL, leaderElectionService);
			listener = new TestingListener();

			client = ZooKeeperUtils.startCuratorFramework(configuration);
			final String leaderPath = configuration.getString(ConfigConstants.ZOOKEEPER_LEADER_PATH,
					ConfigConstants.DEFAULT_ZOOKEEPER_LEADER_PATH);
			cache = new NodeCache(client, leaderPath);

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

			Future<Boolean> deletedFuture = deletedCacheListener.nodeDeleted();

			// make sure that the leader node has been deleted
			Await.result(deletedFuture, timeout);

			leaderRetrievalService.start(listener);

			try {
				listener.waitForLeader(1000);

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

			if (client != null) {
				client.close();
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
