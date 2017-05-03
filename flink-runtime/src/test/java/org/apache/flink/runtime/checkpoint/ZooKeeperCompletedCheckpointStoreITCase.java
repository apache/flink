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

import org.apache.curator.framework.CuratorFramework;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.zookeeper.RetrievableStateStorageHelper;
import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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
	protected AbstractCompletedCheckpointStore createCompletedCheckpoints(
			int maxNumberOfCheckpointsToRetain) throws Exception {

		return new ZooKeeperCompletedCheckpointStore(maxNumberOfCheckpointsToRetain,
			ZooKeeper.createClient(), CheckpointsPath, new RetrievableStateStorageHelper<CompletedCheckpoint>() {
			@Override
			public RetrievableStateHandle<CompletedCheckpoint> store(CompletedCheckpoint state) throws Exception {
				return new HeapRetrievableStateHandle<>(state);
			}
		}, Executors.directExecutor());
	}

	// ---------------------------------------------------------------------------------------------

	/**
	 * Tests that older checkpoints are not cleaned up right away when recovering. Only after
	 * another checkpointed has been completed the old checkpoints exceeding the number of
	 * checkpoints to retain will be removed.
	 */
	@Test
	public void testRecover() throws Exception {
		AbstractCompletedCheckpointStore checkpoints = createCompletedCheckpoints(3);

		TestCompletedCheckpoint[] expected = new TestCompletedCheckpoint[] {
				createCheckpoint(0), createCheckpoint(1), createCheckpoint(2)
		};

		// Add multiple checkpoints
		checkpoints.addCheckpoint(expected[0]);
		checkpoints.addCheckpoint(expected[1]);
		checkpoints.addCheckpoint(expected[2]);

		verifyCheckpointRegistered(expected[0].getOperatorStates().values(), checkpoints.sharedStateRegistry);
		verifyCheckpointRegistered(expected[1].getOperatorStates().values(), checkpoints.sharedStateRegistry);
		verifyCheckpointRegistered(expected[2].getOperatorStates().values(), checkpoints.sharedStateRegistry);

		// All three should be in ZK
		assertEquals(3, ZooKeeper.getClient().getChildren().forPath(CheckpointsPath).size());
		assertEquals(3, checkpoints.getNumberOfRetainedCheckpoints());

		resetCheckpoint(expected[0].getOperatorStates().values());
		resetCheckpoint(expected[1].getOperatorStates().values());
		resetCheckpoint(expected[2].getOperatorStates().values());

		// Recover TODO!!! clear registry!
		checkpoints.recover();

		assertEquals(3, ZooKeeper.getClient().getChildren().forPath(CheckpointsPath).size());
		assertEquals(3, checkpoints.getNumberOfRetainedCheckpoints());
		assertEquals(expected[2], checkpoints.getLatestCheckpoint());

		List<CompletedCheckpoint> expectedCheckpoints = new ArrayList<>(3);
		expectedCheckpoints.add(expected[1]);
		expectedCheckpoints.add(expected[2]);
		expectedCheckpoints.add(createCheckpoint(3));

		checkpoints.addCheckpoint(expectedCheckpoints.get(2));

		List<CompletedCheckpoint> actualCheckpoints = checkpoints.getAllCheckpoints();

		assertEquals(expectedCheckpoints, actualCheckpoints);

		for (CompletedCheckpoint actualCheckpoint : actualCheckpoints) {
			verifyCheckpointRegistered(actualCheckpoint.getOperatorStates().values(), checkpoints.sharedStateRegistry);
		}
	}

	/**
	 * Tests that shutdown discards all checkpoints.
	 */
	@Test
	public void testShutdownDiscardsCheckpoints() throws Exception {
		CuratorFramework client = ZooKeeper.getClient();

		CompletedCheckpointStore store = createCompletedCheckpoints(1);
		TestCompletedCheckpoint checkpoint = createCheckpoint(0);

		store.addCheckpoint(checkpoint);
		assertEquals(1, store.getNumberOfRetainedCheckpoints());
		assertNotNull(client.checkExists().forPath(CheckpointsPath + "/" + checkpoint.getCheckpointID()));

		store.shutdown(JobStatus.FINISHED);
		assertEquals(0, store.getNumberOfRetainedCheckpoints());
		assertNull(client.checkExists().forPath(CheckpointsPath + "/" + checkpoint.getCheckpointID()));

		store.recover();

		assertEquals(0, store.getNumberOfRetainedCheckpoints());
	}

	/**
	 * Tests that suspends keeps all checkpoints (as they can be recovered
	 * later by the ZooKeeper store).
	 */
	@Test
	public void testSuspendKeepsCheckpoints() throws Exception {
		CuratorFramework client = ZooKeeper.getClient();

		CompletedCheckpointStore store = createCompletedCheckpoints(1);
		TestCompletedCheckpoint checkpoint = createCheckpoint(0);

		store.addCheckpoint(checkpoint);
		assertEquals(1, store.getNumberOfRetainedCheckpoints());
		assertNotNull(client.checkExists().forPath(CheckpointsPath + "/" + checkpoint.getCheckpointID()));

		store.shutdown(JobStatus.SUSPENDED);

		assertEquals(0, store.getNumberOfRetainedCheckpoints());
		assertNotNull(client.checkExists().forPath(CheckpointsPath + "/" + checkpoint.getCheckpointID()));

		// Recover again
		store.recover();

		CompletedCheckpoint recovered = store.getLatestCheckpoint();
		assertEquals(checkpoint, recovered);
	}

	static class HeapRetrievableStateHandle<T extends Serializable> implements RetrievableStateHandle<T> {

		private static final long serialVersionUID = -268548467968932L;

		public HeapRetrievableStateHandle(T state) {
			this.state = state;
		}

		private T state;

		@Override
		public T retrieveState() throws Exception {
			return state;
		}

		@Override
		public void discardState() throws Exception {
			state = null;
		}

		@Override
		public long getStateSize() {
			return 0;
		}
	}
}
