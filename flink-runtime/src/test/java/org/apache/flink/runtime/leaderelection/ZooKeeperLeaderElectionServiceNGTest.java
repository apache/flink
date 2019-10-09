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

import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalService;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.nio.charset.Charset;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * Unit tests for {@link ZooKeeperLeaderElectionServiceNG}.
 */
public class ZooKeeperLeaderElectionServiceNGTest extends TestLogger {
	private static final ZooKeeperTestEnvironment ZOOKEEPER = new ZooKeeperTestEnvironment(1);

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

	private ZooKeeperLeaderElectionServiceNG createLeaderElectionService() {
		return new ZooKeeperLeaderElectionServiceNG(ZOOKEEPER.getClient(), testName.getMethodName());
	}

	private LeaderRetrievalService createLeaderRetrievalService() {
		return new ZooKeeperLeaderRetrievalService(
			ZOOKEEPER.getClient(),
			ZooKeeperUtils.getLeaderInfoPath(testName.getMethodName()));
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
		ZooKeeperLeaderElectionServiceNG leaderElectionService = createLeaderElectionService();

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
