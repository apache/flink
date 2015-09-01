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

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.runtime.state.StateHandleProvider;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
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
public class ZooKeeperStateHandleStoreITCase extends TestLogger {

	private final static ZooKeeperTestEnvironment ZooKeeper = new ZooKeeperTestEnvironment(1);

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

	/**
	 * Tests add operation with default {@link CreateMode}.
	 */
	@Test
	public void testAdd() throws Exception {
		// Setup
		LongStateHandleProvider stateHandleProvider = new LongStateHandleProvider();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZooKeeper.getClient(), stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testAdd";
		final Long state = 1239712317L;

		// Test
		store.add(pathInZooKeeper, state);

		// Verify
		// State handle created
		assertEquals(1, stateHandleProvider.getStateHandles().size());
		assertEquals(state, stateHandleProvider.getStateHandles().get(0).getState(null));

		// Path created and is persistent
		Stat stat = ZooKeeper.getClient().checkExists().forPath(pathInZooKeeper);
		assertNotNull(stat);
		assertEquals(0, stat.getEphemeralOwner());

		// Data is equal
		@SuppressWarnings("unchecked")
		Long actual = ((StateHandle<Long>) InstantiationUtil.deserializeObject(
				ZooKeeper.getClient().getData().forPath(pathInZooKeeper),
				ClassLoader.getSystemClassLoader())).getState(null);

		assertEquals(state, actual);
	}

	/**
	 * Tests that {@link CreateMode} is respected.
	 */
	@Test
	public void testAddWithCreateMode() throws Exception {
		LongStateHandleProvider stateHandleProvider = new LongStateHandleProvider();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZooKeeper.getClient(), stateHandleProvider);

		// Config
		Long state = 3457347234L;

		CreateMode[] modes = CreateMode.values();
		for (int i = 0; i < modes.length; i++) {
			CreateMode mode = modes[i];
			state += i;

			String pathInZooKeeper = "/testAddWithCreateMode" + mode.name();

			// Test
			store.add(pathInZooKeeper, state, mode);

			if (mode.isSequential()) {
				// Figure out the sequential ID
				List<String> paths = ZooKeeper.getClient().getChildren().forPath("/");
				for (String p : paths) {
					if (p.startsWith("testAddWithCreateMode" + mode.name())) {
						pathInZooKeeper = "/" + p;
						break;
					}
				}
			}

			// Verify
			// State handle created
			assertEquals(i + 1, stateHandleProvider.getStateHandles().size());
			assertEquals(state, stateHandleProvider.getStateHandles().get(i).getState(null));

			// Path created
			Stat stat = ZooKeeper.getClient().checkExists().forPath(pathInZooKeeper);

			assertNotNull(stat);

			// Is ephemeral or persistent
			if (mode.isEphemeral()) {
				assertTrue(stat.getEphemeralOwner() != 0);
			}
			else {
				assertEquals(0, stat.getEphemeralOwner());
			}

			// Data is equal
			@SuppressWarnings("unchecked")
			Long actual = ((StateHandle<Long>) InstantiationUtil.deserializeObject(
					ZooKeeper.getClient().getData().forPath(pathInZooKeeper),
					ClassLoader.getSystemClassLoader())).getState(null);

			assertEquals(state, actual);
		}
	}

	/**
	 * Tests that an existing path throws an Exception.
	 */
	@Test(expected = Exception.class)
	public void testAddAlreadyExistingPath() throws Exception {
		LongStateHandleProvider stateHandleProvider = new LongStateHandleProvider();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZooKeeper.getClient(), stateHandleProvider);

		ZooKeeper.getClient().create().forPath("/testAddAlreadyExistingPath");

		store.add("/testAddAlreadyExistingPath", 1L);
	}

	/**
	 * Tests that the created state handle is discarded if ZooKeeper create fails.
	 */
	@Test
	public void testAddDiscardStateHandleAfterFailure() throws Exception {
		// Setup
		LongStateHandleProvider stateHandleProvider = new LongStateHandleProvider();

		CuratorFramework client = spy(ZooKeeper.getClient());
		when(client.create()).thenThrow(new RuntimeException("Expected test Exception."));

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				client, stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testAddDiscardStateHandleAfterFailure";
		final Long state = 81282227L;

		try {
			// Test
			store.add(pathInZooKeeper, state);
			fail("Did not throw expected exception");
		}
		catch (Exception ignored) {
		}

		// Verify
		// State handle created and discarded
		assertEquals(1, stateHandleProvider.getStateHandles().size());
		assertEquals(state, stateHandleProvider.getStateHandles().get(0).getState(null));
		assertEquals(1, stateHandleProvider.getStateHandles().get(0).getNumberOfDiscardCalls());
	}

	/**
	 * Tests that a state handle is replaced.
	 */
	@Test
	public void testReplace() throws Exception {
		// Setup
		LongStateHandleProvider stateHandleProvider = new LongStateHandleProvider();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZooKeeper.getClient(), stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testReplace";
		final Long initialState = 30968470898L;
		final Long replaceState = 88383776661L;

		// Test
		store.add(pathInZooKeeper, initialState);
		store.replace(pathInZooKeeper, 0, replaceState);

		// Verify
		// State handles created
		assertEquals(2, stateHandleProvider.getStateHandles().size());
		assertEquals(initialState, stateHandleProvider.getStateHandles().get(0).getState(null));
		assertEquals(replaceState, stateHandleProvider.getStateHandles().get(1).getState(null));

		// Path created and is persistent
		Stat stat = ZooKeeper.getClient().checkExists().forPath(pathInZooKeeper);
		assertNotNull(stat);
		assertEquals(0, stat.getEphemeralOwner());

		// Data is equal
		@SuppressWarnings("unchecked")
		Long actual = ((StateHandle<Long>) InstantiationUtil.deserializeObject(
				ZooKeeper.getClient().getData().forPath(pathInZooKeeper),
				ClassLoader.getSystemClassLoader())).getState(null);

		assertEquals(replaceState, actual);
	}

	/**
	 * Tests that a non existing path throws an Exception.
	 */
	@Test(expected = Exception.class)
	public void testReplaceNonExistingPath() throws Exception {
		StateHandleProvider<Long> stateHandleProvider = new LongStateHandleProvider();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZooKeeper.getClient(), stateHandleProvider);

		store.replace("/testReplaceNonExistingPath", 0, 1L);
	}

	/**
	 * Tests that the replace state handle is discarded if ZooKeeper setData fails.
	 */
	@Test
	public void testReplaceDiscardStateHandleAfterFailure() throws Exception {
		// Setup
		LongStateHandleProvider stateHandleProvider = new LongStateHandleProvider();

		CuratorFramework client = spy(ZooKeeper.getClient());
		when(client.setData()).thenThrow(new RuntimeException("Expected test Exception."));

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				client, stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testReplaceDiscardStateHandleAfterFailure";
		final Long initialState = 30968470898L;
		final Long replaceState = 88383776661L;

		// Test
		store.add(pathInZooKeeper, initialState);

		try {
			store.replace(pathInZooKeeper, 0, replaceState);
			fail("Did not throw expected exception");
		}
		catch (Exception ignored) {
		}

		// Verify
		// State handle created and discarded
		assertEquals(2, stateHandleProvider.getStateHandles().size());
		assertEquals(initialState, stateHandleProvider.getStateHandles().get(0).getState(null));
		assertEquals(replaceState, stateHandleProvider.getStateHandles().get(1).getState(null));
		assertEquals(1, stateHandleProvider.getStateHandles().get(1).getNumberOfDiscardCalls());

		// Initial value
		@SuppressWarnings("unchecked")
		Long actual = ((StateHandle<Long>) InstantiationUtil.deserializeObject(
				ZooKeeper.getClient().getData().forPath(pathInZooKeeper),
				ClassLoader.getSystemClassLoader())).getState(null);

		assertEquals(initialState, actual);
	}

	/**
	 * Tests get operation.
	 */
	@Test
	public void testGetAndExists() throws Exception {
		// Setup
		LongStateHandleProvider stateHandleProvider = new LongStateHandleProvider();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZooKeeper.getClient(), stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testGetAndExists";
		final Long state = 311222268470898L;

		// Test
		assertEquals(-1, store.exists(pathInZooKeeper));

		store.add(pathInZooKeeper, state);
		StateHandle<Long> actual = store.get(pathInZooKeeper);

		// Verify
		assertEquals(state, actual.getState(null));
		assertTrue(store.exists(pathInZooKeeper) >= 0);
	}

	/**
	 * Tests that a non existing path throws an Exception.
	 */
	@Test(expected = Exception.class)
	public void testGetNonExistingPath() throws Exception {
		LongStateHandleProvider stateHandleProvider = new LongStateHandleProvider();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZooKeeper.getClient(), stateHandleProvider);

		store.get("/testGetNonExistingPath");
	}

	/**
	 * Tests that all added state is returned.
	 */
	@Test
	public void testGetAll() throws Exception {
		// Setup
		LongStateHandleProvider stateHandleProvider = new LongStateHandleProvider();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZooKeeper.getClient(), stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testGetAll";

		final Set<Long> expected = new HashSet<>();
		expected.add(311222268470898L);
		expected.add(132812888L);
		expected.add(27255442L);
		expected.add(11122233124L);

		// Test
		for (long val : expected) {
			store.add(pathInZooKeeper, val, CreateMode.PERSISTENT_SEQUENTIAL);
		}

		for (Tuple2<StateHandle<Long>, String> val : store.getAll()) {
			assertTrue(expected.remove(val.f0.getState(null)));
		}
		assertEquals(0, expected.size());
	}

	/**
	 * Tests that the state is returned sorted.
	 */
	@Test
	public void testGetAllSortedByName() throws Exception {
		// Setup
		LongStateHandleProvider stateHandleProvider = new LongStateHandleProvider();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZooKeeper.getClient(), stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testGetAllSortedByName";

		final Long[] expected = new Long[] {
				311222268470898L, 132812888L, 27255442L, 11122233124L };

		// Test
		for (long val : expected) {
			store.add(pathInZooKeeper, val, CreateMode.PERSISTENT_SEQUENTIAL);
		}

		List<Tuple2<StateHandle<Long>, String>> actual = store.getAllSortedByName();
		assertEquals(expected.length, actual.size());

		for (int i = 0; i < expected.length; i++) {
			assertEquals(expected[i], actual.get(i).f0.getState(null));
		}
	}

	/**
	 * Tests that state handles are correctly removed.
	 */
	@Test
	public void testRemove() throws Exception {
		// Setup
		LongStateHandleProvider stateHandleProvider = new LongStateHandleProvider();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZooKeeper.getClient(), stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testRemove";
		final Long state = 27255442L;

		store.add(pathInZooKeeper, state);

		// Test
		store.remove(pathInZooKeeper);

		// Verify discarded
		assertEquals(0, ZooKeeper.getClient().getChildren().forPath("/").size());
	}

	/**
	 * Tests that state handles are correctly removed with a callback.
	 */
	@Test
	public void testRemoveWithCallback() throws Exception {
		// Setup
		LongStateHandleProvider stateHandleProvider = new LongStateHandleProvider();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZooKeeper.getClient(), stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testRemoveWithCallback";
		final Long state = 27255442L;

		store.add(pathInZooKeeper, state);

		final CountDownLatch sync = new CountDownLatch(1);
		BackgroundCallback callback = mock(BackgroundCallback.class);
		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				sync.countDown();
				return null;
			}
		}).when(callback).processResult(eq(ZooKeeper.getClient()), any(CuratorEvent.class));

		// Test
		store.remove(pathInZooKeeper, callback);

		// Verify discarded and callback called
		assertEquals(0, ZooKeeper.getClient().getChildren().forPath("/").size());

		sync.await();

		verify(callback, times(1))
				.processResult(eq(ZooKeeper.getClient()), any(CuratorEvent.class));
	}

	/**
	 * Tests that state handles are correctly discarded.
	 */
	@Test
	public void testRemoveAndDiscardState() throws Exception {
		// Setup
		LongStateHandleProvider stateHandleProvider = new LongStateHandleProvider();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZooKeeper.getClient(), stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testDiscard";
		final Long state = 27255442L;

		store.add(pathInZooKeeper, state);

		// Test
		store.removeAndDiscardState(pathInZooKeeper);

		// Verify discarded
		assertEquals(0, ZooKeeper.getClient().getChildren().forPath("/").size());
	}

	/** Tests that all state handles are correctly discarded. */
	@Test
	public void testRemoveAndDiscardAllState() throws Exception {
		// Setup
		LongStateHandleProvider stateHandleProvider = new LongStateHandleProvider();

		ZooKeeperStateHandleStore<Long> store = new ZooKeeperStateHandleStore<>(
				ZooKeeper.getClient(), stateHandleProvider);

		// Config
		final String pathInZooKeeper = "/testDiscardAll";

		final Set<Long> expected = new HashSet<>();
		expected.add(311222268470898L);
		expected.add(132812888L);
		expected.add(27255442L);
		expected.add(11122233124L);

		// Test
		for (long val : expected) {
			store.add(pathInZooKeeper, val, CreateMode.PERSISTENT_SEQUENTIAL);
		}

		store.removeAndDiscardAllState();

		// Verify all discarded
		assertEquals(0, ZooKeeper.getClient().getChildren().forPath("/").size());
	}

	// ---------------------------------------------------------------------------------------------
	// Simple test helpers
	// ---------------------------------------------------------------------------------------------

	private static class LongStateHandleProvider implements StateHandleProvider<Long> {

		private static final long serialVersionUID = 4572084854499402276L;

		private final List<LongStateHandle> stateHandles = new ArrayList<>();

		@Override
		public StateHandle<Long> createStateHandle(Long state) {
			LongStateHandle stateHandle = new LongStateHandle(state);
			stateHandles.add(stateHandle);

			return stateHandle;
		}

		public List<LongStateHandle> getStateHandles() {
			return stateHandles;
		}
	}

	private static class LongStateHandle implements StateHandle<Long> {

		private static final long serialVersionUID = -3555329254423838912L;

		private final Long state;

		private int numberOfDiscardCalls;

		public LongStateHandle(Long state) {
			this.state = state;
		}

		@Override
		public Long getState(ClassLoader ignored) throws Exception {
			return state;
		}

		@Override
		public void discardState() throws Exception {
			numberOfDiscardCalls++;
		}

		public int getNumberOfDiscardCalls() {
			return numberOfDiscardCalls;
		}
	}
}
