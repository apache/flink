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

package org.apache.flink.runtime.zookeeper;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.TestingLongStateHandleHelper;
import org.apache.flink.runtime.persistence.TestingLongStateHandleHelper.LongRetrievableStateHandle;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

/**
 * Tests for basic {@link ZooKeeperStateHandleStore} behaviour.
 *
 * <p> Tests include:
 * <ul>
 * <li>Expected usage of operations</li>
 * <li>Correct ordering of ZooKeeper and state handle operations</li>
 * </ul>
 */
public class ZooKeeperStateHandleStoreTest extends TestLogger {

	private static final ZooKeeperTestEnvironment ZOOKEEPER = new ZooKeeperTestEnvironment(1);

	@AfterClass
	public static void tearDown() throws Exception {
		if (ZOOKEEPER != null) {
			ZOOKEEPER.shutdown();
		}
	}

	@Before
	public void cleanUp() throws Exception {
		ZOOKEEPER.deleteAll();
	}

	/**
	 * Tests add operation with lock.
	 */
	@Test
	public void testAddAndLock() throws Exception {
		final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();
		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
			ZOOKEEPER.getClient(), longStateStorage);

		// Config
		final String pathInZooKeeper = "/testAdd";
		final Long state = 1239712317L;

		// Test
		store.addAndLock(pathInZooKeeper, state);

		// Verify
		// State handle created
		assertEquals(1, store.getAllAndLock().size());
		assertEquals(state, store.getAndLock(pathInZooKeeper).retrieveState());

		// Path created and is persistent
		Stat stat = ZOOKEEPER.getClient().checkExists().forPath(pathInZooKeeper);
		assertNotNull(stat);
		assertEquals(0, stat.getEphemeralOwner());

		List<String> children = ZOOKEEPER.getClient().getChildren().forPath(pathInZooKeeper);

		// there should be one child which is the lock
		assertEquals(1, children.size());

		stat = ZOOKEEPER.getClient().checkExists().forPath(pathInZooKeeper + '/' + children.get(0));
		assertNotNull(stat);

		// check that the child is an ephemeral node
		assertNotEquals(0, stat.getEphemeralOwner());

		// Data is equal
		@SuppressWarnings("unchecked")
		Long actual = ((RetrievableStateHandle<Long>) InstantiationUtil.deserializeObject(
				ZOOKEEPER.getClient().getData().forPath(pathInZooKeeper),
				ClassLoader.getSystemClassLoader())).retrieveState();

		assertEquals(state, actual);
	}

	/**
	 * Tests that an existing path throws an Exception.
	 */
	@Test(expected = Exception.class)
	public void testAddAlreadyExistingPath() throws Exception {
		final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZOOKEEPER.getClient(), stateHandleProvider);

		ZOOKEEPER.getClient().create().forPath("/testAddAlreadyExistingPath");

		store.addAndLock("/testAddAlreadyExistingPath", 1L);

		// writing to the state storage should have succeeded
		assertEquals(1, stateHandleProvider.getStateHandles());

		// the created state handle should have been cleaned up if the add operation failed
		assertEquals(1, stateHandleProvider.getStateHandles().get(0).getNumberOfDiscardCalls());
	}

	/**
	 * Tests that the created state handle is discarded if ZooKeeper create fails.
	 */
	@Test
	public void testAddDiscardStateHandleAfterFailure() throws Exception {
		// Setup
		final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

		CuratorFramework client = spy(ZOOKEEPER.getClient());
		when(client.inTransaction().create()).thenThrow(new RuntimeException("Expected test Exception."));

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				client, stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testAddDiscardStateHandleAfterFailure";
		final Long state = 81282227L;

		try {
			// Test
			store.addAndLock(pathInZooKeeper, state);
			fail("Did not throw expected exception");
		}
		catch (Exception ignored) {
		}

		// Verify
		// State handle created and discarded
		assertEquals(1, stateHandleProvider.getStateHandles().size());
		assertEquals(state, stateHandleProvider.getStateHandles().get(0).retrieveState());
		assertEquals(1, stateHandleProvider.getStateHandles().get(0).getNumberOfDiscardCalls());
	}

	/**
	 * Tests that a state handle is replaced.
	 */
	@Test
	public void testReplace() throws Exception {
		// Setup
		final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZOOKEEPER.getClient(), stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testReplace";
		final Long initialState = 30968470898L;
		final Long replaceState = 88383776661L;

		// Test
		store.addAndLock(pathInZooKeeper, initialState);
		store.replace(pathInZooKeeper, IntegerResourceVersion.valueOf(0), replaceState);

		// Verify
		// State handles created
		assertEquals(2, stateHandleProvider.getStateHandles().size());
		assertEquals(initialState, stateHandleProvider.getStateHandles().get(0).retrieveState());
		assertEquals(replaceState, stateHandleProvider.getStateHandles().get(1).retrieveState());

		// Path created and is persistent
		Stat stat = ZOOKEEPER.getClient().checkExists().forPath(pathInZooKeeper);
		assertNotNull(stat);
		assertEquals(0, stat.getEphemeralOwner());

		// Data is equal
		@SuppressWarnings("unchecked")
		Long actual = ((RetrievableStateHandle<Long>) InstantiationUtil.deserializeObject(
				ZOOKEEPER.getClient().getData().forPath(pathInZooKeeper),
				ClassLoader.getSystemClassLoader())).retrieveState();

		assertEquals(replaceState, actual);
	}

	/**
	 * Tests that a non existing path throws an Exception.
	 */
	@Test(expected = Exception.class)
	public void testReplaceNonExistingPath() throws Exception {
		final RetrievableStateStorageHelper<Long> stateStorage = new TestingLongStateHandleHelper();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZOOKEEPER.getClient(), stateStorage);

		store.replace("/testReplaceNonExistingPath", IntegerResourceVersion.valueOf(0), 1L);
	}

	/**
	 * Tests that the replace state handle is discarded if ZooKeeper setData fails.
	 */
	@Test
	public void testReplaceDiscardStateHandleAfterFailure() throws Exception {
		// Setup
		final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

		CuratorFramework client = spy(ZOOKEEPER.getClient());
		when(client.setData()).thenThrow(new RuntimeException("Expected test Exception."));

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				client, stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testReplaceDiscardStateHandleAfterFailure";
		final Long initialState = 30968470898L;
		final Long replaceState = 88383776661L;

		// Test
		store.addAndLock(pathInZooKeeper, initialState);

		try {
			store.replace(pathInZooKeeper, IntegerResourceVersion.valueOf(0), replaceState);
			fail("Did not throw expected exception");
		}
		catch (Exception ignored) {
		}

		// Verify
		// State handle created and discarded
		assertEquals(2, stateHandleProvider.getStateHandles().size());
		assertEquals(initialState, stateHandleProvider.getStateHandles().get(0).retrieveState());
		assertEquals(replaceState, stateHandleProvider.getStateHandles().get(1).retrieveState());
		assertEquals(1, stateHandleProvider.getStateHandles().get(1).getNumberOfDiscardCalls());

		// Initial value
		@SuppressWarnings("unchecked")
		Long actual = ((RetrievableStateHandle<Long>) InstantiationUtil.deserializeObject(
				ZOOKEEPER.getClient().getData().forPath(pathInZooKeeper),
				ClassLoader.getSystemClassLoader())).retrieveState();

		assertEquals(initialState, actual);
	}

	/**
	 * Tests get operation.
	 */
	@Test
	public void testGetAndExists() throws Exception {
		// Setup
		final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZOOKEEPER.getClient(), stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testGetAndExists";
		final Long state = 311222268470898L;

		// Test
		assertThat(store.exists(pathInZooKeeper).isExisting(), is(false));

		store.addAndLock(pathInZooKeeper, state);
		RetrievableStateHandle<Long> actual = store.getAndLock(pathInZooKeeper);

		// Verify
		assertEquals(state, actual.retrieveState());
		assertTrue(store.exists(pathInZooKeeper).getValue() >= 0);
	}

	/**
	 * Tests that a non existing path throws an Exception.
	 */
	@Test(expected = Exception.class)
	public void testGetNonExistingPath() throws Exception {
		final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZOOKEEPER.getClient(), stateHandleProvider);

		store.getAndLock("/testGetNonExistingPath");
	}

	/**
	 * Tests that all added state is returned.
	 */
	@Test
	public void testGetAll() throws Exception {
		// Setup
		final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZOOKEEPER.getClient(), stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testGetAll";

		final Set<Long> expected = new HashSet<>();
		expected.add(311222268470898L);
		expected.add(132812888L);
		expected.add(27255442L);
		expected.add(11122233124L);

		// Test
		for (long val : expected) {
			store.addAndLock(pathInZooKeeper + val, val);
		}

		for (Tuple2<RetrievableStateHandle<Long>, String> val : store.getAllAndLock()) {
			assertTrue(expected.remove(val.f0.retrieveState()));
		}
		assertEquals(0, expected.size());
	}

	/**
	 * Tests that the state is returned sorted.
	 */
	@Test
	public void testGetAllSortedByName() throws Exception {
		// Setup
		final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZOOKEEPER.getClient(), stateHandleProvider);

		// Config
		final String basePath = "/testGetAllSortedByName";

		final Long[] expected = new Long[] {
				311222268470898L, 132812888L, 27255442L, 11122233124L };

		// Test
		for (long val : expected) {
			final String pathInZooKeeper = String.format("%s%016d", basePath, val);
			store.addAndLock(pathInZooKeeper, val);
		}

		List<Tuple2<RetrievableStateHandle<Long>, String>> actual = store.getAllAndLock();
		assertEquals(expected.length, actual.size());

		// bring the elements in sort order
		Arrays.sort(expected);
		Collections.sort(actual, Comparator.comparing(o -> o.f1));

		for (int i = 0; i < expected.length; i++) {
			assertEquals(expected[i], actual.get(i).f0.retrieveState());
		}
	}

	/**
	 * Tests that state handles are correctly removed.
	 */
	@Test
	public void testRemove() throws Exception {
		// Setup
		final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZOOKEEPER.getClient(), stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testRemove";
		final Long state = 27255442L;

		store.addAndLock(pathInZooKeeper, state);

		final int numberOfGlobalDiscardCalls = LongRetrievableStateHandle.getNumberOfGlobalDiscardCalls();

		// Test
		store.releaseAndTryRemove(pathInZooKeeper);

		// Verify discarded
		assertEquals(0, ZOOKEEPER.getClient().getChildren().forPath("/").size());
		assertEquals(numberOfGlobalDiscardCalls + 1, LongRetrievableStateHandle.getNumberOfGlobalDiscardCalls());
	}

	/** Tests that all state handles are correctly discarded. */
	@Test
	public void testReleaseAndTryRemoveAll() throws Exception {
		// Setup
		final TestingLongStateHandleHelper stateHandleProvider = new TestingLongStateHandleHelper();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZOOKEEPER.getClient(), stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testDiscardAll";

		final Set<Long> expected = new HashSet<>();
		expected.add(311222268470898L);
		expected.add(132812888L);
		expected.add(27255442L);
		expected.add(11122233124L);

		// Test
		for (long val : expected) {
			store.addAndLock(pathInZooKeeper + val, val);
		}

		store.releaseAndTryRemoveAll();

		// Verify all discarded
		assertEquals(0, ZOOKEEPER.getClient().getChildren().forPath("/").size());
	}

	/**
	 * Tests that the ZooKeeperStateHandleStore can handle corrupted data by releasing and trying to remove the
	 * respective ZooKeeper ZNodes.
	 */
	@Test
	public void testCorruptedData() throws Exception {
		final TestingLongStateHandleHelper stateStorage = new TestingLongStateHandleHelper();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
			ZOOKEEPER.getClient(),
			stateStorage);

		final Collection<Long> input = new HashSet<>();
		input.add(1L);
		input.add(2L);
		input.add(3L);

		for (Long aLong : input) {
			store.addAndLock("/" + aLong, aLong);
		}

		// corrupt one of the entries
		ZOOKEEPER.getClient().setData().forPath("/" + 2, new byte[2]);

		List<Tuple2<RetrievableStateHandle<Long>, String>> allEntries = store.getAllAndLock();

		Collection<Long> expected = new HashSet<>(input);
		expected.remove(2L);

		Collection<Long> actual = new HashSet<>(expected.size());

		for (Tuple2<RetrievableStateHandle<Long>, String> entry : allEntries) {
			actual.add(entry.f0.retrieveState());
		}

		assertEquals(expected, actual);
	}

	/**
	 * FLINK-6612
	 *
	 * Tests that a concurrent delete operation cannot succeed if another instance holds a lock on the specified
	 * node.
	 */
	@Test
	public void testConcurrentDeleteOperation() throws Exception {
		final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();

		ZooKeeperStateHandleStore<Long> zkStore1 = new ZooKeeperStateHandleStore<>(
			ZOOKEEPER.getClient(),
			longStateStorage);

		ZooKeeperStateHandleStore<Long> zkStore2 = new ZooKeeperStateHandleStore<>(
			ZOOKEEPER.getClient(),
			longStateStorage);

		final String statePath = "/state";

		zkStore1.addAndLock(statePath, 42L);
		RetrievableStateHandle<Long> stateHandle = zkStore2.getAndLock(statePath);

		// this should not remove the referenced node because we are still holding a state handle
		// reference via zkStore2
		zkStore1.releaseAndTryRemove(statePath);

		// sanity check
		assertEquals(42L, (long) stateHandle.retrieveState());

		Stat nodeStat = ZOOKEEPER.getClient().checkExists().forPath(statePath);

		assertNotNull("NodeStat should not be null, otherwise the referenced node does not exist.", nodeStat);

		zkStore2.releaseAndTryRemove(statePath);

		nodeStat = ZOOKEEPER.getClient().checkExists().forPath(statePath);

		assertNull("NodeState should be null, because the referenced node should no longer exist.", nodeStat);
	}

	/**
	 * FLINK-6612
	 *
	 * Tests that getAndLock removes a created lock if the RetrievableStateHandle cannot be retrieved
	 * (e.g. deserialization problem).
	 */
	@Test
	public void testLockCleanupWhenGetAndLockFails() throws Exception {
		final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();

		ZooKeeperStateHandleStore<Long> zkStore1 = new ZooKeeperStateHandleStore<>(
			ZOOKEEPER.getClient(),
			longStateStorage);

		ZooKeeperStateHandleStore<Long> zkStore2 = new ZooKeeperStateHandleStore<>(
			ZOOKEEPER.getClient(),
			longStateStorage);

		final String path = "/state";

		zkStore1.addAndLock(path, 42L);

		final byte[] corruptedData = {1, 2};

		// corrupt the data
		ZOOKEEPER.getClient().setData().forPath(path, corruptedData);

		try {
			zkStore2.getAndLock(path);
			fail("Should fail because we cannot deserialize the node's data");
		} catch (IOException ignored) {
			// expected to fail
		}

		// check that there is no lock node left
		String lockNodePath = zkStore2.getLockPath(path);

		Stat stat = ZOOKEEPER.getClient().checkExists().forPath(lockNodePath);

		// zkStore2 should not have created a lock node
		assertNull("zkStore2 should not have created a lock node.", stat);

		Collection<String> children = ZOOKEEPER.getClient().getChildren().forPath(path);

		// there should be exactly one lock node from zkStore1
		assertEquals(1, children.size());

		zkStore1.releaseAndTryRemove(path);

		stat = ZOOKEEPER.getClient().checkExists().forPath(path);

		assertNull("The state node should have been removed.", stat);
	}

	/**
	 * FLINK-6612
	 *
	 * Tests that lock nodes will be released if the client dies.
	 */
	@Test
	public void testLockCleanupWhenClientTimesOut() throws Exception {
		final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();

		Configuration configuration = new Configuration();
		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, ZOOKEEPER.getConnectString());
		configuration.setInteger(HighAvailabilityOptions.ZOOKEEPER_SESSION_TIMEOUT, 100);
		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT, "timeout");

		try (CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration);
			CuratorFramework client2 = ZooKeeperUtils.startCuratorFramework(configuration)) {

			ZooKeeperStateHandleStore<Long> zkStore = new ZooKeeperStateHandleStore<>(
				client,
				longStateStorage);

			final String path = "/state";

			zkStore.addAndLock(path, 42L);

			// this should delete all ephemeral nodes
			client.close();

			Stat stat = client2.checkExists().forPath(path);

			// check that our state node still exists
			assertNotNull(stat);

			Collection<String> children = client2.getChildren().forPath(path);

			// check that the lock node has been released
			assertEquals(0, children.size());
		}
	}

	/**
	 * FLINK-6612
	 *
	 * Tests that we can release a locked state handles in the ZooKeeperStateHandleStore.
	 */
	@Test
	public void testRelease() throws Exception {
		final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();

		ZooKeeperStateHandleStore<Long> zkStore = new ZooKeeperStateHandleStore<>(
			ZOOKEEPER.getClient(),
			longStateStorage);

		final String path = "/state";

		zkStore.addAndLock(path, 42L);

		final String lockPath = zkStore.getLockPath(path);

		Stat stat = ZOOKEEPER.getClient().checkExists().forPath(lockPath);

		assertNotNull("Expected an existing lock", stat);

		zkStore.release(path);

		stat = ZOOKEEPER.getClient().checkExists().forPath(path);

		// release should have removed the lock child
		assertEquals("Expected no lock nodes as children", 0, stat.getNumChildren());

		zkStore.releaseAndTryRemove(path);

		stat = ZOOKEEPER.getClient().checkExists().forPath(path);

		assertNull("State node should have been removed.", stat);
	}

	/**
	 * FLINK-6612
	 *
	 * Tests that we can release all locked state handles in the ZooKeeperStateHandleStore
	 */
	@Test
	public void testReleaseAll() throws Exception {
		final TestingLongStateHandleHelper longStateStorage = new TestingLongStateHandleHelper();

		ZooKeeperStateHandleStore<Long> zkStore = new ZooKeeperStateHandleStore<>(
			ZOOKEEPER.getClient(),
			longStateStorage);

		final Collection<String> paths = Arrays.asList("/state1", "/state2", "/state3");

		for (String path : paths) {
			zkStore.addAndLock(path, 42L);
		}

		for (String path : paths) {
			Stat stat = ZOOKEEPER.getClient().checkExists().forPath(zkStore.getLockPath(path));

			assertNotNull("Expecte and existing lock.", stat);
		}

		zkStore.releaseAll();

		for (String path : paths) {
			Stat stat = ZOOKEEPER.getClient().checkExists().forPath(path);

			assertEquals(0, stat.getNumChildren());
		}

		zkStore.releaseAndTryRemoveAll();

		Stat stat = ZOOKEEPER.getClient().checkExists().forPath("/");

		assertEquals(0, stat.getNumChildren());
	}

	@Test
	public void testRemoveAllHandlesShouldRemoveAllPaths() throws Exception {
		final ZooKeeperStateHandleStore<Long> zkStore = new ZooKeeperStateHandleStore<>(
			ZooKeeperUtils.useNamespaceAndEnsurePath(ZOOKEEPER.getClient(), "/path"),
			new TestingLongStateHandleHelper());

		zkStore.addAndLock("/state", 1L);
		zkStore.clearEntries();

		assertThat(zkStore.getAllHandles(), is(empty()));
	}
}
