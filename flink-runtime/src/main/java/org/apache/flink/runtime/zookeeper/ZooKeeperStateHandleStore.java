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
import org.apache.curator.utils.ZKPaths;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.InstantiationUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * State handles backed by ZooKeeper.
 *
 * <p>Added state is persisted via {@link RetrievableStateHandle RetrievableStateHandles},
 * which in turn are written to ZooKeeper. This level of indirection is necessary to keep the
 * amount of data in ZooKeeper small. ZooKeeper is build for data in the KB range whereas
 * state can grow to multiple MBs.
 *
 * <p>State modifications require some care, because it is possible that certain failures bring
 * the state handle backend and ZooKeeper out of sync.
 *
 * <p>ZooKeeper holds the ground truth about state handles, i.e. the following holds:
 *
 * <pre>
 * State handle in ZooKeeper =&gt; State handle exists
 * </pre>
 *
 * But not:
 *
 * <pre>
 * State handle exists =&gt; State handle in ZooKeeper
 * </pre>
 *
 * There can be lingering state handles when failures happen during operation. They
 * need to be cleaned up manually (see <a href="https://issues.apache.org/jira/browse/FLINK-2513">
 * FLINK-2513</a> about a possible way to overcome this).
 *
 * @param <T> Type of state
 */
public class ZooKeeperStateHandleStore<T extends Serializable> {

	public static Logger LOG = LoggerFactory.getLogger(ZooKeeperStateHandleStore.class);

	/** Curator ZooKeeper client */
	private final CuratorFramework client;

	private final RetrievableStateStorageHelper<T> storage;

	private final Executor executor;

	/**
	 * Creates a {@link ZooKeeperStateHandleStore}.
	 *
	 * @param client              The Curator ZooKeeper client. <strong>Important:</strong> It is
	 *                            expected that the client's namespace ensures that the root
	 *                            path is exclusive for all state handles managed by this
	 *                            instance, e.g. <code>client.usingNamespace("/stateHandles")</code>
	 * @param storage to persist the actual state and whose returned state handle is then written
	 *                to ZooKeeper
	 * @param executor to run the ZooKeeper callbacks
	 */
	public ZooKeeperStateHandleStore(
		CuratorFramework client,
		RetrievableStateStorageHelper<T> storage,
		Executor executor) {

		this.client = checkNotNull(client, "Curator client");
		this.storage = checkNotNull(storage, "State storage");
		this.executor = checkNotNull(executor);
	}

	/**
	 * Creates a state handle and stores it in ZooKeeper with create mode {@link
	 * CreateMode#PERSISTENT}.
	 *
	 * @see #add(String, T, CreateMode)
	 */
	public RetrievableStateHandle<T> add(String pathInZooKeeper, T state) throws Exception {
		return add(pathInZooKeeper, state, CreateMode.PERSISTENT);
	}

	/**
	 * Creates a state handle and stores it in ZooKeeper.
	 *
	 * <p><strong>Important</strong>: This will <em>not</em> store the actual state in
	 * ZooKeeper, but create a state handle and store it in ZooKeeper. This level of indirection
	 * makes sure that data in ZooKeeper is small.
	 *
	 * @param pathInZooKeeper Destination path in ZooKeeper (expected to *not* exist yet and
	 *                        start with a '/')
	 * @param state           State to be added
	 * @param createMode      The create mode for the new path in ZooKeeper
	 *
	 * @return The Created {@link RetrievableStateHandle}.
	 * @throws Exception If a ZooKeeper or state handle operation fails
	 */
	public RetrievableStateHandle<T> add(
			String pathInZooKeeper,
			T state,
			CreateMode createMode) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");
		checkNotNull(state, "State");

		RetrievableStateHandle<T> storeHandle = storage.store(state);

		boolean success = false;

		try {
			// Serialize the state handle. This writes the state to the backend.
			byte[] serializedStoreHandle = InstantiationUtil.serializeObject(storeHandle);

			// Write state handle (not the actual state) to ZooKeeper. This is expected to be
			// smaller than the state itself. This level of indirection makes sure that data in
			// ZooKeeper is small, because ZooKeeper is designed for data in the KB range, but
			// the state can be larger.
			client.create().withMode(createMode).forPath(pathInZooKeeper, serializedStoreHandle);

			success = true;
			return storeHandle;
		}
		finally {
			if (!success) {
				// Cleanup the state handle if it was not written to ZooKeeper.
				if (storeHandle != null) {
					storeHandle.discardState();
				}
			}
		}
	}

	/**
	 * Replaces a state handle in ZooKeeper and discards the old state handle.
	 *
	 * @param pathInZooKeeper Destination path in ZooKeeper (expected to exist and start with a '/')
	 * @param expectedVersion Expected version of the node to replace
	 * @param state           The new state to replace the old one
	 * @throws Exception If a ZooKeeper or state handle operation fails
	 */
	public void replace(String pathInZooKeeper, int expectedVersion, T state) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");
		checkNotNull(state, "State");

		RetrievableStateHandle<T> oldStateHandle = get(pathInZooKeeper);

		RetrievableStateHandle<T> newStateHandle = storage.store(state);

		boolean success = false;

		try {
			// Serialize the new state handle. This writes the state to the backend.
			byte[] serializedStateHandle = InstantiationUtil.serializeObject(newStateHandle);

			// Replace state handle in ZooKeeper.
			client.setData()
					.withVersion(expectedVersion)
					.forPath(pathInZooKeeper, serializedStateHandle);
			success = true;
		} finally {
			if(success) {
				oldStateHandle.discardState();
			} else {
				newStateHandle.discardState();
			}
		}

	}

	/**
	 * Returns the version of the node if it exists or <code>-1</code> if it doesn't.
	 *
	 * @param pathInZooKeeper Path in ZooKeeper to check
	 * @return Version of the ZNode if the path exists, <code>-1</code> otherwise.
	 * @throws Exception If the ZooKeeper operation fails
	 */
	public int exists(String pathInZooKeeper) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");

		Stat stat = client.checkExists().forPath(pathInZooKeeper);

		if (stat != null) {
			return stat.getVersion();
		}

		return -1;
	}

	/**
	 * Gets a state handle from ZooKeeper.
	 *
	 * @param pathInZooKeeper Path in ZooKeeper to get the state handle from (expected to
	 *                        exist and start with a '/').
	 * @return The state handle
	 * @throws Exception If a ZooKeeper or state handle operation fails
	 */
	@SuppressWarnings("unchecked")
	public RetrievableStateHandle<T> get(String pathInZooKeeper) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");

		byte[] data;

		try {
			data = client.getData().forPath(pathInZooKeeper);
		} catch (Exception e) {
			throw new Exception("Failed to retrieve state handle data under " + pathInZooKeeper +
				" from ZooKeeper.", e);
		}

		try {
			return InstantiationUtil.deserializeObject(data, Thread.currentThread().getContextClassLoader());
		} catch (IOException | ClassNotFoundException e) {
			throw new IOException("Failed to deserialize state handle from ZooKeeper data from " +
				pathInZooKeeper + '.', e);
		}
	}

	/**
	 * Return a list of all valid paths for state handles.
	 *
	 * @return List of valid state handle paths in ZooKeeper
	 * @throws Exception if a ZooKeeper operation fails
	 */
	public Collection<String> getAllPaths() throws Exception {
		final String path = "/";

		while(true) {
			Stat stat = client.checkExists().forPath(path);

			if (stat == null) {
				return Collections.emptyList();
			} else {
				try {
					return client.getChildren().forPath(path);
				} catch (KeeperException.NoNodeException ignored) {
					// Concurrent deletion, retry
				}
			}
		}
	}

	/**
	 * Gets all available state handles from ZooKeeper.
	 *
	 * <p>If there is a concurrent modification, the operation is retried until it succeeds.
	 *
	 * @return All state handles from ZooKeeper.
	 * @throws Exception If a ZooKeeper or state handle operation fails
	 */
	@SuppressWarnings("unchecked")
	public List<Tuple2<RetrievableStateHandle<T>, String>> getAll() throws Exception {
		final List<Tuple2<RetrievableStateHandle<T>, String>> stateHandles = new ArrayList<>();

		boolean success = false;

		retry:
		while (!success) {
			stateHandles.clear();

			Stat stat = client.checkExists().forPath("/");
			if (stat == null) {
				break; // Node does not exist, done.
			} else {
				// Initial cVersion (number of changes to the children of this node)
				int initialCVersion = stat.getCversion();

				List<String> children = client.getChildren().forPath("/");

				for (String path : children) {
					path = "/" + path;

					try {
						final RetrievableStateHandle<T> stateHandle = get(path);
						stateHandles.add(new Tuple2<>(stateHandle, path));
					} catch (KeeperException.NoNodeException ignored) {
						// Concurrent deletion, retry
						continue retry;
					} catch (IOException ioException) {
						LOG.warn("Could not get all ZooKeeper children. Node {} contained " +
							"corrupted data. Ignoring this node.", path, ioException);
					}
				}

				int finalCVersion = client.checkExists().forPath("/").getCversion();

				// Check for concurrent modifications
				success = initialCVersion == finalCVersion;
			}
		}

		return stateHandles;
	}


	/**
	 * Gets all available state handles from ZooKeeper sorted by name (ascending).
	 *
	 * <p>If there is a concurrent modification, the operation is retried until it succeeds.
	 *
	 * @return All state handles in ZooKeeper.
	 * @throws Exception If a ZooKeeper or state handle operation fails
	 */
	@SuppressWarnings("unchecked")
	public List<Tuple2<RetrievableStateHandle<T>, String>> getAllSortedByName() throws Exception {
		final List<Tuple2<RetrievableStateHandle<T>, String>> stateHandles = new ArrayList<>();

		boolean success = false;

		retry:
		while (!success) {
			stateHandles.clear();

			Stat stat = client.checkExists().forPath("/");
			if (stat == null) {
				break; // Node does not exist, done.
			} else {
				// Initial cVersion (number of changes to the children of this node)
				int initialCVersion = stat.getCversion();

				List<String> children = ZKPaths.getSortedChildren(
						client.getZookeeperClient().getZooKeeper(),
						ZKPaths.fixForNamespace(client.getNamespace(), "/"));

				for (String path : children) {
					path = "/" + path;

					try {
						final RetrievableStateHandle<T> stateHandle = get(path);
						stateHandles.add(new Tuple2<>(stateHandle, path));
					} catch (KeeperException.NoNodeException ignored) {
						// Concurrent deletion, retry
						continue retry;
					} catch (IOException ioException) {
						LOG.warn("Could not get all ZooKeeper children. Node {} contained " +
							"corrupted data. Ignoring this node.", path, ioException);
					}
				}

				int finalCVersion = client.checkExists().forPath("/").getCversion();

				// Check for concurrent modifications
				success = initialCVersion == finalCVersion;
			}
		}

		return stateHandles;
	}

	/**
	 * Removes a state handle from ZooKeeper.
	 *
	 * <p><strong>Important</strong>: this does not discard the state handle. If you want to
	 * discard the state handle call {@link #removeAndDiscardState(String)}.
	 *
	 * @param pathInZooKeeper Path of state handle to remove (expected to start with a '/')
	 * @throws Exception If the ZooKeeper operation fails
	 */
	public void remove(String pathInZooKeeper) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");

		client.delete().deletingChildrenIfNeeded().forPath(pathInZooKeeper);
	}

	/**
	 * Removes a state handle from ZooKeeper asynchronously.
	 *
	 * <p><strong>Important</strong>: this does not discard the state handle. If you want to
	 * discard the state handle call {@link #removeAndDiscardState(String)}.
	 *
	 * @param pathInZooKeeper Path of state handle to remove (expected to start with a '/')
	 * @param callback        The callback after the operation finishes
	 * @throws Exception If the ZooKeeper operation fails
	 */
	public void remove(String pathInZooKeeper, BackgroundCallback callback) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");
		checkNotNull(callback, "Background callback");

		client.delete().deletingChildrenIfNeeded().inBackground(callback, executor).forPath(pathInZooKeeper);
	}

	/**
	 * Discards a state handle and removes it from ZooKeeper.
	 *
	 * <p>If you only want to remove the state handle in ZooKeeper call {@link #remove(String)}.
	 *
	 * @param pathInZooKeeper Path of state handle to discard (expected to start with a '/')
	 * @throws Exception If the ZooKeeper or state handle operation fails
	 */
	public void removeAndDiscardState(String pathInZooKeeper) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");

		RetrievableStateHandle<T> stateHandle = get(pathInZooKeeper);

		// Delete the state handle from ZooKeeper first
		client.delete().deletingChildrenIfNeeded().forPath(pathInZooKeeper);

		// Discard the state handle only after it has been successfully deleted from ZooKeeper.
		// Otherwise we might enter an illegal state after failures (with a state handle in
		// ZooKeeper, which has already been discarded).
		stateHandle.discardState();
	}

	/**
	 * Discards all available state handles and removes them from ZooKeeper.
	 *
	 * @throws Exception If a ZooKeeper or state handle operation fails
	 */
	public void removeAndDiscardAllState() throws Exception {
		final List<Tuple2<RetrievableStateHandle<T>, String>> allStateHandles = getAll();

		ZKPaths.deleteChildren(
				client.getZookeeperClient().getZooKeeper(),
				ZKPaths.fixForNamespace(client.getNamespace(), "/"),
				false);

		// Discard the state handles only after they have been successfully deleted from ZooKeeper.
		for (Tuple2<RetrievableStateHandle<T>, String> stateHandleAndPath : allStateHandles) {
			stateHandleAndPath.f0.discardState();
		}
	}
}
