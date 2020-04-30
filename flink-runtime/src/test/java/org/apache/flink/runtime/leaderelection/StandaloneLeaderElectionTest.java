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

import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class StandaloneLeaderElectionTest extends TestLogger {
	private static final String TEST_URL = "akka://users/jobmanager";

	/**
	 * Tests that the standalone leader election and retrieval service return the same leader
	 * URL.
	 */
	@Test
	public void testStandaloneLeaderElectionRetrieval() throws Exception {
		StandaloneLeaderElectionService leaderElectionService = new StandaloneLeaderElectionService();
		StandaloneLeaderRetrievalService leaderRetrievalService = new StandaloneLeaderRetrievalService(TEST_URL);
		TestingContender contender = new TestingContender(TEST_URL, leaderElectionService);
		TestingListener testingListener = new TestingListener();

		try {
			leaderElectionService.start(contender);
			leaderRetrievalService.start(testingListener);

			contender.waitForLeader(1000l);

			assertTrue(contender.isLeader());
			assertEquals(HighAvailabilityServices.DEFAULT_LEADER_ID, contender.getLeaderSessionID());

			testingListener.waitForNewLeader(1000l);

			assertEquals(TEST_URL, testingListener.getAddress());
			assertEquals(HighAvailabilityServices.DEFAULT_LEADER_ID, testingListener.getLeaderSessionID());
		} finally {
			leaderElectionService.stop();
			leaderRetrievalService.stop();
		}
	}
}
