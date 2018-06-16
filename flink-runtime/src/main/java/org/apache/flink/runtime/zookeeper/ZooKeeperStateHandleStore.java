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
import org.apache.curator.framework.api.CuratorEventType;
import org.apache.curator.utils.ZKPaths;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Class which stores state via the provided {@link RetrievableStateStorageHelper} and writes the
 * returned state handle to ZooKeeper. The ZooKeeper node can be locked by creating an ephemeral
 * child and only allowing the deletion of the ZooKeeper node if it does not have any children.
 * That way we protect concurrent accesses from different ZooKeeperStateHandleStore instances.
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

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperStateHandleStore.class);

	/** Curator ZooKeeper client */
	private final CuratorFramework client;

	private final RetrievableStateStorageHelper<T> storage;

	private final Executor executor;

	/** Lock node name of this ZooKeeperStateHandleStore. The name should be unique among all other state handle stores. */
	private final String lockNode;

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

		// Generate a unique lock node name
		lockNode = UUID.randomUUID().toString();
	}

	/**
	 * Creates a state handle, stores it in ZooKeeper and locks it. A locked node cannot be removed by
	 * another {@link ZooKeeperStateHandleStore} instance as long as this instance remains connected
	 * to ZooKeeper.
	 *
	 * <p><strong>Important</strong>: This will <em>not</em> store the actual state in
	 * ZooKeeper, but create a state handle and store it in ZooKeeper. This level of indirection
	 * makes sure that data in ZooKeeper is small.
	 *
	 * <p>The operation will fail if there is already an node under the given path
	 *
	 * @param pathInZooKeeper Destination path in ZooKeeper (expected to *not* exist yet)
	 * @param state           State to be added
	 *
	 * @return The Created {@link RetrievableStateHandle}.
	 * @throws Exception If a ZooKeeper or state handle operation fails
	 */
	public RetrievableStateHandle<T> addAndLock(
			String pathInZooKeeper,
			T state) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");
		checkNotNull(state, "State");

		final String path = normalizePath(pathInZooKeeper);

		RetrievableStateHandle<T> storeHandle = storage.store(state);

		boolean success = false;

		try {
			// Serialize the state handle. This writes the state to the backend.
			byte[] serializedStoreHandle = InstantiationUtil.serializeObject(storeHandle);

			// Write state handle (not the actual state) to ZooKeeper. This is expected to be
			// smaller than the state itself. This level of indirection makes sure that data in
			// ZooKeeper is small, because ZooKeeper is designed for data in the KB range, but
			// the state can be larger.
			// Create the lock node in a transaction with the actual state node. That way we can prevent
			// race conditions with a concurrent delete operation.
			client.inTransaction().create().withMode(CreateMode.PERSISTENT).forPath(path, serializedStoreHandle)
				.and().create().withMode(CreateMode.EPHEMERAL).forPath(getLockPath(path))
				.and().commit();

			success = true;
			return storeHandle;
		}
		catch (KeeperException.NodeExistsException e) {
			throw new ConcurrentModificationException("ZooKeeper unexpectedly modified", e);
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

		final String path = normalizePath(pathInZooKeeper);

		RetrievableStateHandle<T> oldStateHandle = get(path, false);

		RetrievableStateHandle<T> newStateHandle = storage.store(state);

		boolean success = false;

		try {
			// Serialize the new state handle. This writes the state to the backend.
			byte[] serializedStateHandle = InstantiationUtil.serializeObject(newStateHandle);

			// Replace state handle in ZooKeeper.
			client.setData()
					.withVersion(expectedVersion)
					.forPath(path, serializedStateHandle);
			success = true;
		} catch (KeeperException.NoNodeException e) {
			throw new ConcurrentModificationException("ZooKeeper unexpectedly modified", e);
		} finally {
			if (success) {
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

		final String path = normalizePath(pathInZooKeeper);

		Stat stat = client.checkExists().forPath(path);

		if (stat != null) {
			return stat.getVersion();
		}

		return -1;
	}

	/**
	 * Gets the {@link RetrievableStateHandle} stored in the given ZooKeeper node and locks it. A
	 * locked node cannot be removed by another {@link ZooKeeperStateHandleStore} instance as long
	 * as this instance remains connected to ZooKeeper.
	 *
	 * @param pathInZooKeeper Path to the ZooKeeper node which contains the state handle
	 * @return The retrieved state handle from the specified ZooKeeper node
	 * @throws IOException Thrown if the method failed to deserialize the stored state handle
	 * @throws Exception Thrown if a ZooKeeper operation failed
	 */
	public RetrievableStateHandle<T> getAndLock(String pathInZooKeeper) throws Exception {
		return get(pathInZooKeeper, true);
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
	 * Gets all available state handles from ZooKeeper and locks the respective state nodes.
	 *
	 * <p>If there is a concurrent modification, the operation is retried until it succeeds.
	 *
	 * @return All state handles from ZooKeeper.
	 * @throws Exception If a ZooKeeper or state handle operation fails
	 */
	@SuppressWarnings("unchecked")
	public List<Tuple2<RetrievableStateHandle<T>, String>> getAllAndLock() throws Exception {
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
						final RetrievableStateHandle<T> stateHandle = getAndLock(path);
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
	 * Gets all available state handles from ZooKeeper sorted by name (ascending) and locks the
	 * respective state nodes. The result tuples contain the retrieved state and the path to the
	 * node in ZooKeeper.
	 *
	 * <p>If there is a concurrent modification, the operation is retried until it succeeds.
	 *
	 * @return All state handles in ZooKeeper.
	 * @throws Exception If a ZooKeeper or state handle operation fails
	 */
	@SuppressWarnings("unchecked")
	public List<Tuple2<RetrievableStateHandle<T>, String>> getAllSortedByNameAndLock() throws Exception {
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
						final RetrievableStateHandle<T> stateHandle = getAndLock(path);
						stateHandles.add(new Tuple2<>(stateHandle, path));
					} catch (KeeperException.NoNodeException ignored) {
						// Concurrent deletion, retry
						continue retry;
					} catch (IOException ioException) {
						LOG.warn("Could not get all ZooKeeper children. Node {} contained " +
							"corrupted data. Releasing and trying to remove this node.", path, ioException);

						releaseAndTryRemove(path);
					}
				}

				int finalCVersion = client.checkExists().forPath("/").getCversion();

				// Check for concurrent modifications
				success = initialCVersion == finalCVersion;

				// we don't have to release all locked nodes in case of a concurrent modification, because we
				// will retrieve them in the next iteration again.
			}
		}

		return stateHandles;
	}

	/**
	 * Releases the lock for the given state node and tries to remove the state node if it is no longer locked.
	 * The deletion of the state node is executed asynchronously.
	 *
	 * <p><strong>Important</strong>: This also discards the stored state handle after the given action
	 * has been executed.
	 *
	 * @param pathInZooKeeper Path of state handle to remove (expected to start with a '/')
	 * @throws Exception If the ZooKeeper operation fails
	 */
	public void releaseAndTryRemove(String pathInZooKeeper) throws Exception {
		releaseAndTryRemove(pathInZooKeeper, null);
	}

	/**
	 * Releases the lock for the given state node and tries to remove the state node if it is no longer locked.
	 * The deletion of the state node is executed asynchronously. After the state node has been deleted, the given
	 * callback is called with the {@link RetrievableStateHandle} of the deleted state node.
	 *
	 * <p><strong>Important</strong>: This also discards the stored state handle after the given action
	 * has been executed.
	 *
	 * @param pathInZooKeeper Path of state handle to remove
	 * @param callback The callback to execute after a successful deletion. Null if no action needs to be executed.
	 * @throws Exception If the ZooKeeper operation fails
	 */
	public void releaseAndTryRemove(
			String pathInZooKeeper,
			@Nullable final RemoveCallback<T> callback) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");

		final String path = normalizePath(pathInZooKeeper);

		RetrievableStateHandle<T> stateHandle = null;

		try {
			stateHandle = get(path, false);
		} catch (Exception e) {
			LOG.warn("Could not retrieve the state handle from node " + path + '.', e);
		}

		release(pathInZooKeeper);

		final BackgroundCallback backgroundCallback = new RemoveBackgroundCallback<>(stateHandle, callback, path);

		client.delete().inBackground(backgroundCallback, executor).forPath(path);
	}

	/**
	 * Releases all lock nodes of this ZooKeeperStateHandleStores and tries to remove all state nodes which
	 * are not locked anymore.
	 *
	 * <p>The delete operation is executed asynchronously
	 *
	 * @throws Exception if the delete operation fails
	 */
	public void releaseAndTryRemoveAll() throws Exception {
		Collection<String> children = getAllPaths();

		Exception exception = null;

		for (String child : children) {
			try {
				releaseAndTryRemove('/' + child);
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}
		}

		if (exception != null) {
			throw new Exception("Could not properly release and try removing all state nodes.", exception);
		}
	}

	/**
	 * Releases the lock from the node under the given ZooKeeper path. If no lock exists, then nothing happens.
	 *
	 * @param pathInZooKeeper Path describing the ZooKeeper node
	 * @throws Exception if the delete operation of the lock node fails
	 */
	public void release(String pathInZooKeeper) throws Exception {
		final String path = normalizePath(pathInZooKeeper);

		try {
			client.delete().forPath(getLockPath(path));
		} catch (KeeperException.NoNodeException ignored) {
			// we have never locked this node
		} catch (Exception e) {
			throw new Exception("Could not release the lock: " + getLockPath(pathInZooKeeper) + '.', e);
		}
	}

	/**
	 * Releases all lock nodes of this ZooKeeperStateHandleStore.
	 *
	 * @throws Exception if the delete operation of a lock file fails
	 */
	public void releaseAll() throws Exception {
		Collection<String> children = getAllPaths();

		Exception exception = null;

		for (String child: children) {
			try {
				release(child);
			} catch (Exception e) {
				exception = ExceptionUtils.firstOrSuppressed(e, exception);
			}
		}

		if (exception != null) {
			throw new Exception("Could not properly release all state nodes.", exception);
		}
	}

	// ---------------------------------------------------------------------------------------------------------
	// Protected methods
	// ---------------------------------------------------------------------------------------------------------

	/**
	 * Returns the path for the lock node relative to the given path.
	 *
	 * @param rootPath Root path under which the lock node shall be created
	 * @return Path for the lock node
	 */
	protected String getLockPath(String rootPath) {
		return rootPath + '/' + lockNode;
	}

	// ---------------------------------------------------------------------------------------------------------
	// Private methods
	// ---------------------------------------------------------------------------------------------------------

	/**
	 * Gets a state handle from ZooKeeper and optionally locks it.
	 *
	 * @param pathInZooKeeper Path in ZooKeeper to get the state handle from
	 * @param lock True if we should lock the node; otherwise false
	 * @return The state handle
	 * @throws IOException Thrown if the method failed to deserialize the stored state handle
	 * @throws Exception Thrown if a ZooKeeper operation failed
	 */
	@SuppressWarnings("unchecked")
	private RetrievableStateHandle<T> get(String pathInZooKeeper, boolean lock) throws Exception {
		checkNotNull(pathInZooKeeper, "Path in ZooKeeper");

		final String path = normalizePath(pathInZooKeeper);

		if (lock) {
			// try to lock the node
			try {
				client.create().withMode(CreateMode.EPHEMERAL).forPath(getLockPath(path));
			} catch (KeeperException.NodeExistsException ignored) {
				// we have already created the lock
			} catch (KeeperException.NoNodeException e) {
				throw new Exception("Cannot lock the node " + path + " since it does not exist.", e);
			}
		}

		boolean success = false;

		try {
			byte[] data;

			try {
				data = client.getData().forPath(path);
			} catch (Exception e) {
				throw new Exception("Failed to retrieve state handle data under " + path +
					" from ZooKeeper.", e);
			}

			try {
				RetrievableStateHandle<T> retrievableStateHandle = InstantiationUtil.deserializeObject(
					data,
					Thread.currentThread().getContextClassLoader());

				success = true;

				return retrievableStateHandle;
			} catch (IOException | ClassNotFoundException e) {
				throw new IOException("Failed to deserialize state handle from ZooKeeper data from " +
					path + '.', e);
			}
		} finally {
			if (!success && lock) {
				// release the lock
				release(path);
			}
		}
	}

	/**
	 * Makes sure that every path starts with a "/"
	 *
	 * @param path Path to normalize
	 * @return Normalized path such that it starts with a "/"
	 */
	private static String normalizePath(String path) {
		if (path.startsWith("/")) {
			return path;
		} else {
			return '/' + path;
		}
	}

	// ---------------------------------------------------------------------------------------------------------
	// Utility classes
	// ---------------------------------------------------------------------------------------------------------

	/**
	 * Callback which is executed when removing a node from ZooKeeper. The callback will call the given
	 * {@link RemoveCallback} if it is not null. Afterwards, it will discard the given {@link RetrievableStateHandle}
	 * if it is not null.
	 *
	 * @param <T> Type of the value stored in the RetrievableStateHandle
	 */
	private static final class RemoveBackgroundCallback<T extends Serializable> implements BackgroundCallback {
		@Nullable
		private final RetrievableStateHandle<T> stateHandle;

		@Nullable
		private final RemoveCallback<T> callback;

		private final String pathInZooKeeper;

		private RemoveBackgroundCallback(
			@Nullable RetrievableStateHandle<T> stateHandle,
			@Nullable RemoveCallback<T> callback,
			String pathInZooKeeper) {

			this.stateHandle = stateHandle;
			this.callback = callback;
			this.pathInZooKeeper = Preconditions.checkNotNull(pathInZooKeeper);
		}

		@Override
		public void processResult(CuratorFramework client, CuratorEvent event) throws Exception {
			try {
				if (event.getType() == CuratorEventType.DELETE) {
					final KeeperException.Code resultCode = KeeperException.Code.get(event.getResultCode());

					if (resultCode == KeeperException.Code.OK) {
						Exception exception = null;

						if (null != callback) {
							try {
								callback.apply(stateHandle);
							} catch (Throwable e) {
								exception = new Exception("Could not execute delete action for node " +
									pathInZooKeeper + '.', e);
							}
						}

						if (stateHandle != null) {
							try {
								// Discard the state handle
								stateHandle.discardState();
							} catch (Throwable e) {
								Exception newException = new Exception("Could not discard state handle of node " +
									pathInZooKeeper + '.', e);

								if (exception == null) {
									exception = newException;
								} else {
									exception.addSuppressed(newException);
								}
							}
						}

						if (exception != null) {
							throw exception;
						}
					} else if (resultCode == KeeperException.Code.NOTEMPTY) {
						// Could not delete the node because it still contains children/locks
						LOG.debug("Could not delete node " + pathInZooKeeper + " because it is still locked.");
					} else {
						throw new IllegalStateException("Unexpected result code " +
							resultCode.name() + " in '" + event + "' callback.");
					}
				} else {
					throw new IllegalStateException("Unexpected event type " +
						event.getType() + " in '" + event + "' callback.");
				}
			} catch (Exception e) {
				LOG.warn("Failed to run callback for delete operation on node " + pathInZooKeeper + '.', e);
			}

		}
	}

	/**
	 * Callback interface for remove calls
	 */
	public interface RemoveCallback<T extends Serializable> {
		/**
		 * Callback method. The parameter can be null if the {@link RetrievableStateHandle} could not be retrieved
		 * from ZooKeeper.
		 *
		 * @param value RetrievableStateHandle retrieved from ZooKeeper, null if it was not retrievable
		 * @throws FlinkException If the callback failed
		 */
		void apply(@Nullable RetrievableStateHandle<T> value) throws FlinkException;
	}
}
