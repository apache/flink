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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.state.LocalStateHandle;
import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import scala.concurrent.duration.Deadline;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Tests for basic {@link CompletedCheckpointStore} contract and ZooKeeper state handling.
 */
public class ZooKeeperCompletedCheckpointStoreITCase extends CompletedCheckpointStoreTest {

	private final static ZooKeeperTestEnvironment ZooKeeper = new ZooKeeperTestEnvironment(1);

	private final static String CheckpointsPath = "/checkpoints";

	@AfterClass
	public static void tearDown() throws Exception {
		if (ZooKeeper != null) {
			ZooKeeper.shutdown();
		}
	}

	@Before
	public void cleanUp() throws Exception {
		ZooKeeper.deleteAll();
	}

	@Override
	protected CompletedCheckpointStore createCompletedCheckpoints(int maxNumberOfCheckpointsToRetain,
			ClassLoader userLoader) throws Exception {

		return new ZooKeeperCompletedCheckpointStore(maxNumberOfCheckpointsToRetain, userLoader,
				ZooKeeper.createClient(), CheckpointsPath, new LocalStateHandle
				.LocalStateHandleProvider<CompletedCheckpoint>());
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * Tests that older checkpoints are cleaned up at startup.
	 */
	@Test
	public void testRecover() throws Exception {
		CompletedCheckpointStore checkpoints = createCompletedCheckpoints(3, ClassLoader
				.getSystemClassLoader());

		TestCheckpoint[] expected = new TestCheckpoint[] {
				createCheckpoint(0), createCheckpoint(1), createCheckpoint(2)
		};

		// Add multiple checkpoints
		checkpoints.addCheckpoint(expected[0]);
		checkpoints.addCheckpoint(expected[1]);
		checkpoints.addCheckpoint(expected[2]);

		// All three should be in ZK
		assertEquals(3, ZooKeeper.getClient().getChildren().forPath(CheckpointsPath).size());

		// Recover
		checkpoints.recover();

		// Only the latest one should be in ZK
		Deadline deadline = new FiniteDuration(1, TimeUnit.MINUTES).fromNow();

		// Retry this operation, because removal is asynchronous
		while (deadline.hasTimeLeft() && ZooKeeper.getClient()
				.getChildren().forPath(CheckpointsPath).size() != 1) {

			Thread.sleep(Math.min(100, deadline.timeLeft().toMillis()));
		}

		assertEquals(1, ZooKeeper.getClient().getChildren().forPath(CheckpointsPath).size());
		assertEquals(expected[2], checkpoints.getLatestCheckpoint());
	}
}
