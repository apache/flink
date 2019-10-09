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
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalService;
import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for the {@link ZooKeeperLeaderElectionService} and the {@link ZooKeeperLeaderRetrievalService}.
 */
public class ZooKeeperLeaderElectionTest extends TestLogger {

	private static Logger LOG = LoggerFactory.getLogger(ZooKeeperLeaderElectionTest.class);
	private static final ZooKeeperTestEnvironment ZOOKEEPER = new ZooKeeperTestEnvironment(1);

	private static final String TEST_URL = "akka//user/jobmanager";
	private static final long timeout = 200L * 1000L;

	@Rule
	public TestName testName = new TestName();

	@After
	public void after() throws Exception {
		ZOOKEEPER.deleteAll();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		ZOOKEEPER.shutdown();
	}

	private ZooKeeperLeaderElectionService createLeaderElectionService() {
		return new ZooKeeperLeaderElectionService(ZOOKEEPER.getClient(), testName.getMethodName());
	}

	private ZooKeeperLeaderRetrievalService createLeaderRetrievalService() {
		return new ZooKeeperLeaderRetrievalService(ZOOKEEPER.getClient(), testName.getMethodName());
	}

	/**
	 * Tests that the ZooKeeperLeaderElection/RetrievalService return both the correct URL.
	 */
	@Test
	public void testZooKeeperLeaderElectionRetrieval() throws Exception {
		ZooKeeperLeaderElectionService leaderElectionService = null;
		ZooKeeperLeaderRetrievalService leaderRetrievalService = null;

		try {
			leaderElectionService = createLeaderElectionService();
			leaderRetrievalService = createLeaderRetrievalService();

			TestingContender contender = new TestingContender(TEST_URL, leaderElectionService);
			TestingListener listener = new TestingListener();

			leaderElectionService.start(contender);
			leaderRetrievalService.start(listener);

			contender.waitForLeader(timeout);
			assertTrue(contender.isLeader());

			listener.waitForNewLeader(timeout);
			assertEquals(TEST_URL, listener.getAddress());

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
		Deadline deadline = Deadline.fromNow(Duration.ofMinutes(5L));

		int num = 10;

		ZooKeeperLeaderElectionService[] leaderElectionService = new ZooKeeperLeaderElectionService[num];
		TestingContender[] contenders = new TestingContender[num];
		ZooKeeperLeaderRetrievalService leaderRetrievalService = null;

		TestingListener listener = new TestingListener();

		try {
			leaderRetrievalService = createLeaderRetrievalService();

			LOG.debug("Start leader retrieval service for the TestingListener.");

			leaderRetrievalService.start(listener);

			for (int i = 0; i < num; i++) {
				leaderElectionService[i] = createLeaderElectionService();
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
			leaderRetrievalService = createLeaderRetrievalService();

			leaderRetrievalService.start(listener);

			for (int i = 0; i < num; i++) {
				leaderElectionService[i] = createLeaderElectionService();
				contenders[i] = new TestingContender(TEST_URL + "_" + i + "_0", leaderElectionService[i]);

				leaderElectionService[i].start(contenders[i]);
			}

			String pattern = TEST_URL + "_" + "(\\d+)" + "_" + "(\\d+)";
			Pattern regex = Pattern.compile(pattern);

			for (int i = 0; i < numTries; i++) {
				listener.waitForNewLeader(timeout);

				String address = listener.getAddress();

				Matcher m = regex.matcher(address);

				if (m.find()) {
					int index = Integer.parseInt(m.group(1));
					int lastTry = Integer.parseInt(m.group(2));

					assertEquals(listener.getLeaderSessionID(), contenders[index].getLeaderSessionID());

					// stop leader election service = revoke leadership
					leaderElectionService[index].stop();
					// create new leader election service which takes part in the leader election
					leaderElectionService[index] = createLeaderElectionService();
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
	 *  Test that errors in the {@link LeaderElectionService} are correctly forwarded to the
	 *  {@link LeaderContender}.
	 */
	@Test
	public void testExceptionForwarding() throws Exception {
		ZooKeeperLeaderElectionService leaderElectionService = null;
		ZooKeeperLeaderRetrievalService leaderRetrievalService = null;
		TestingListener listener = new TestingListener();
		TestingContender testingContender;

		FlinkRuntimeException testException = new FlinkRuntimeException("Test exception");

		try {
			leaderElectionService = createLeaderElectionService();
			leaderRetrievalService = createLeaderRetrievalService();

			testingContender = new TestingContender(TEST_URL, leaderElectionService) {
				@Override
				public String getAddress() {
					throw testException;
				}
			};

			leaderElectionService.start(testingContender);
			leaderRetrievalService.start(listener);

			testingContender.waitForError(timeout);

			assertNotNull(testingContender.getError());
			assertEquals(testException, testingContender.getError());
		} finally {
			if (leaderElectionService != null) {
				leaderElectionService.stop();
			}

			if (leaderRetrievalService != null) {
				leaderRetrievalService.stop();
			}
		}
	}

	@Test
	public void testMultipleLeaders() throws Exception {
		LeaderElectionService leaderElectionService1 = createLeaderElectionService();
		LeaderElectionService leaderElectionService2 = createLeaderElectionService();
		LeaderRetrievalService leaderRetrievalService = createLeaderRetrievalService();

		try {
			String leaderAddress1 = "0-109-18792";
			String leaderAddress2 = "8-109-18793";

			TestingContender contender1 = new TestingContender(leaderAddress1, leaderElectionService1);
			TestingContender contender2 = new TestingContender(leaderAddress2, leaderElectionService2);
			TestingListener retriever = new TestingListener();

			leaderRetrievalService.start(retriever);
			leaderElectionService1.start(contender1);

			retriever.waitForNewLeader(2000L);
			assertThat(retriever.getAddress(), is(leaderAddress1));

			leaderElectionService2.start(contender2);
			leaderElectionService1.stop();

			retriever.waitForNewLeader(2000L);
			assertThat(retriever.getAddress(), is(leaderAddress2));
		} finally {
			leaderRetrievalService.stop();
			leaderElectionService2.stop();
			leaderElectionService1.stop();
		}
	}

	@Test
	@SuppressWarnings("OptionalGetWithoutIsPresent")
	public void testAccess() throws Exception {
		ZooKeeperLeaderElectionService leaderElectionService = createLeaderElectionService();

		try {
			TestingContender listener = new TestingContender("foo", leaderElectionService);
			leaderElectionService.start(listener);
			listener.waitForLeader(2000L);

			LeaderStore leaderStore = leaderElectionService.getLeaderStore();
			assertNotNull(leaderStore);

			String name = "/jane";
			byte[] value1 = "0-109-18792".getBytes(Charset.defaultCharset());
			assertFalse(leaderStore.exists(name));
			assertFalse(leaderStore.get(name).isPresent());
			assertFalse(leaderStore.getChildren(name).isPresent());

			leaderStore.add(name, value1);
			assertTrue(leaderStore.exists(name));
			assertArrayEquals(value1, leaderStore.get(name).get());
			assertTrue(leaderStore.getChildren(name).get().isEmpty());

			String childName1 = "/jane/bob";
			byte[] childValue1 = "2-12719-179".getBytes(Charset.defaultCharset());
			leaderStore.add(childName1, childValue1);
			assertTrue(leaderStore.exists(childName1));
			assertArrayEquals(childValue1, leaderStore.get(childName1).get());
			assertTrue(leaderStore.getChildren(childName1).get().isEmpty());
			assertTrue(leaderStore.getChildren(name).get().contains(childName1));

			String childName2 = "/jane/alice";
			byte[] childValue2 = "8-1210-1202".getBytes(Charset.defaultCharset());
			leaderStore.add(childName2, childValue2);
			assertTrue(leaderStore.exists(childName2));
			assertArrayEquals(childValue2, leaderStore.get(childName2).get());
			assertTrue(leaderStore.getChildren(childName2).get().isEmpty());
			assertTrue(leaderStore.getChildren(name).get().contains(childName2));

			String childName3 = "/jane/carol";
			leaderStore.add(childName3, null);
			assertTrue(leaderStore.exists(childName3));
			assertArrayEquals(LeaderStore.EMPTY_DATA, leaderStore.get(childName3).get());
			byte[] childValue3 = "0-80469-1202".getBytes(Charset.defaultCharset());
			leaderStore.update(childName3, childValue3);
			assertArrayEquals(childValue3, leaderStore.get(childName3).get());
			leaderStore.update(childName3, null);
			assertArrayEquals(LeaderStore.EMPTY_DATA, leaderStore.get(childName3).get());

			leaderStore.remove(childName1);
			assertFalse(leaderStore.exists(childName1));
			assertFalse(leaderStore.get(childName1).isPresent());
			assertFalse(leaderStore.getChildren(name).get().contains(childName1));

		} finally {
			leaderElectionService.stop();
		}
	}
}
