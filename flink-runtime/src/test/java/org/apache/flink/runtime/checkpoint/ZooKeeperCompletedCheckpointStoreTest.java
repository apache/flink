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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperResource;
import org.apache.flink.util.TestLogger;

import org.apache.curator.framework.CuratorFramework;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link ZooKeeperCompletedCheckpointStore}.
 */
public class ZooKeeperCompletedCheckpointStoreTest extends TestLogger {

	@ClassRule
	public static ZooKeeperResource zooKeeperResource = new ZooKeeperResource();

	@Test
	public void testPathConversion() {
		final long checkpointId = 42L;

		final String path = ZooKeeperCompletedCheckpointStore.checkpointIdToPath(checkpointId);

		assertEquals(checkpointId, ZooKeeperCompletedCheckpointStore.pathToCheckpointId(path));
	}

	/**
	 * Tests that subsumed checkpoints are discarded.
	 */
	@Test
	public void testDiscardingSubsumedCheckpoints() throws Exception {
		final SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		final Configuration configuration = new Configuration();
		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());

		final CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration);
		final ZooKeeperCompletedCheckpointStore checkpointStore = createZooKeeperCheckpointStore(client);

		try {
			final CompletedCheckpointStoreTest.TestCompletedCheckpoint checkpoint1 = CompletedCheckpointStoreTest.createCheckpoint(0, sharedStateRegistry);

			checkpointStore.addCheckpoint(checkpoint1);
			assertThat(checkpointStore.getAllCheckpoints(), Matchers.contains(checkpoint1));

			final CompletedCheckpointStoreTest.TestCompletedCheckpoint checkpoint2 = CompletedCheckpointStoreTest.createCheckpoint(1, sharedStateRegistry);
			checkpointStore.addCheckpoint(checkpoint2);
			final List<CompletedCheckpoint> allCheckpoints = checkpointStore.getAllCheckpoints();
			assertThat(allCheckpoints, Matchers.contains(checkpoint2));
			assertThat(allCheckpoints, Matchers.not(Matchers.contains(checkpoint1)));

			// verify that the subsumed checkpoint is discarded
			CompletedCheckpointStoreTest.verifyCheckpointDiscarded(checkpoint1);
		} finally {
			client.close();
		}
	}

	/**
	 * Tests that checkpoints are discarded when the completed checkpoint store is shut
	 * down with a globally terminal state.
	 */
	@Test
	public void testDiscardingCheckpointsAtShutDown() throws Exception {
		final SharedStateRegistry sharedStateRegistry = new SharedStateRegistry();
		final Configuration configuration = new Configuration();
		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());

		final CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration);
		final ZooKeeperCompletedCheckpointStore checkpointStore = createZooKeeperCheckpointStore(client);

		try {
			final CompletedCheckpointStoreTest.TestCompletedCheckpoint checkpoint1 = CompletedCheckpointStoreTest.createCheckpoint(0, sharedStateRegistry);

			checkpointStore.addCheckpoint(checkpoint1);
			assertThat(checkpointStore.getAllCheckpoints(), Matchers.contains(checkpoint1));

			checkpointStore.shutdown(JobStatus.FINISHED);

			// verify that the checkpoint is discarded
			CompletedCheckpointStoreTest.verifyCheckpointDiscarded(checkpoint1);
		} finally {
			client.close();
		}
	}

	@Nonnull
	private ZooKeeperCompletedCheckpointStore createZooKeeperCheckpointStore(CuratorFramework client) throws Exception {
		return new ZooKeeperCompletedCheckpointStore(
			1,
			client,
			"/checkpoints",
			new TestingRetrievableStateStorageHelper<>(),
			Executors.directExecutor());
	}

}
