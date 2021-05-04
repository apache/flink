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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.persistence.IntegerResourceVersion;
import org.apache.flink.runtime.persistence.PossibleInconsistentStateException;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.StateHandleStore;
import org.apache.flink.runtime.state.RetrievableStateHandle;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.curator4.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator4.org.apache.curator.utils.ZKPaths;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.runtime.util.StateHandleStoreUtils.deserialize;
import static org.apache.flink.runtime.util.StateHandleStoreUtils.serializeOrDiscard;
import static org.apache.flink.shaded.guava18.com.google.common.collect.Sets.newHashSet;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Class which stores state via the provided {@link RetrievableStateStorageHelper} and writes the
 * returned state handle to ZooKeeper. The ZooKeeper node can be locked by creating an ephemeral
 * child and only allowing the deletion of the ZooKeeper node if it does not have any children. That
 * way we protect concurrent accesses from different ZooKeeperStateHandleStore instances.
 *
 * <p>Added state is persisted via {@link RetrievableStateHandle RetrievableStateHandles}, which in
 * turn are written to ZooKeeper. This level of indirection is necessary to keep the amount of data
 * in ZooKeeper small. ZooKeeper is build for data in the KB range whereas state can grow to
 * multiple MBs.
 *
 * <p>State modifications require some care, because it is possible that certain failures bring the
 * state handle backend and ZooKeeper out of sync.
 *
 * <p>ZooKeeper holds the ground truth about state handles, i.e. the following holds:
 *
 * <pre>
 * State handle in ZooKeeper =&gt; State handle exists
 * </pre>
 *
 * <p>But not:
 *
 * <pre>
 * State handle exists =&gt; State handle in ZooKeeper
 * </pre>
 *
 * <p>There can be lingering state handles when failures happen during operation. They need to be
 * cleaned up manually (see <a href="https://issues.apache.org/jira/browse/FLINK-2513">
 * FLINK-2513</a> about a possible way to overcome this).
 *
 * @param <T> Type of state
 */
public class ZooKeeperStateHandleStore<T extends Serializable>
        implements StateHandleStore<T, IntegerResourceVersion> {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperStateHandleStore.class);

    @VisibleForTesting
    static final Set<Class<? extends KeeperException>> PRE_COMMIT_EXCEPTIONS =
            newHashSet(
                    KeeperException.NodeExistsException.class,
                    KeeperException.BadArgumentsException.class,
                    KeeperException.NoNodeException.class,
                    KeeperException.NoAuthException.class,
                    KeeperException.BadVersionException.class,
                    KeeperException.AuthFailedException.class,
                    KeeperException.InvalidACLException.class,
                    KeeperException.SessionMovedException.class,
                    KeeperException.NotReadOnlyException.class);

    /** Curator ZooKeeper client. */
    private final CuratorFramework client;

    private final RetrievableStateStorageHelper<T> storage;

    /**
     * Lock node name of this ZooKeeperStateHandleStore. The name should be unique among all other
     * state handle stores.
     */
    private final String lockNode;

    /**
     * Creates a {@link ZooKeeperStateHandleStore}.
     *
     * @param client The Curator ZooKeeper client. <strong>Important:</strong> It is expected that
     *     the client's namespace ensures that the root path is exclusive for all state handles
     *     managed by this instance, e.g. <code>client.usingNamespace("/stateHandles")</code>
     * @param storage to persist the actual state and whose returned state handle is then written to
     *     ZooKeeper
     */
    public ZooKeeperStateHandleStore(
            CuratorFramework client, RetrievableStateStorageHelper<T> storage) {

        this.client = checkNotNull(client, "Curator client");
        this.storage = checkNotNull(storage, "State storage");

        // Generate a unique lock node name
        lockNode = UUID.randomUUID().toString();
    }

    /**
     * Creates a state handle, stores it in ZooKeeper and locks it. A locked node cannot be removed
     * by another {@link ZooKeeperStateHandleStore} instance as long as this instance remains
     * connected to ZooKeeper.
     *
     * <p><strong>Important</strong>: This will <em>not</em> store the actual state in ZooKeeper,
     * but create a state handle and store it in ZooKeeper. This level of indirection makes sure
     * that data in ZooKeeper is small.
     *
     * <p>The operation will fail if there is already an node under the given path
     *
     * @param pathInZooKeeper Destination path in ZooKeeper (expected to *not* exist yet)
     * @param state State to be added
     * @return The Created {@link RetrievableStateHandle}.
     * @throws PossibleInconsistentStateException if the write-to-ZooKeeper operation failed. This
     *     indicates that it's not clear whether the new state was successfully written to ZooKeeper
     *     or not. Proper error handling has to be applied on the caller's side.
     * @throws Exception If a ZooKeeper or state handle operation fails
     */
    @Override
    public RetrievableStateHandle<T> addAndLock(String pathInZooKeeper, T state)
            throws PossibleInconsistentStateException, Exception {
        checkNotNull(pathInZooKeeper, "Path in ZooKeeper");
        checkNotNull(state, "State");

        final String path = normalizePath(pathInZooKeeper);

        RetrievableStateHandle<T> storeHandle = storage.store(state);

        byte[] serializedStoreHandle = serializeOrDiscard(storeHandle);

        try {
            writeStoreHandleTransactionally(path, serializedStoreHandle);
            return storeHandle;
        } catch (Exception e) {
            if (indicatesPossiblyInconsistentState(e)) {
                throw new PossibleInconsistentStateException(e);
            }

            // in any other failure case: discard the state
            storeHandle.discardState();

            // We wrap the exception here so that it could be caught in DefaultJobGraphStore
            throw ExceptionUtils.findThrowable(e, KeeperException.NodeExistsException.class)
                    .map(
                            nee ->
                                    new AlreadyExistException(
                                            "ZooKeeper node " + path + " already exists.", nee))
                    .orElseThrow(() -> e);
        }
    }

    // this method is provided for the sole purpose of easier testing
    @VisibleForTesting
    protected void writeStoreHandleTransactionally(String path, byte[] serializedStoreHandle)
            throws Exception {
        // Write state handle (not the actual state) to ZooKeeper. This is expected to be
        // smaller than the state itself. This level of indirection makes sure that data in
        // ZooKeeper is small, because ZooKeeper is designed for data in the KB range, but
        // the state can be larger.
        // Create the lock node in a transaction with the actual state node. That way we can
        // prevent race conditions with a concurrent delete operation.
        client.inTransaction()
                .create()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, serializedStoreHandle)
                .and()
                .create()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(getLockPath(path))
                .and()
                .commit();
    }

    /**
     * Replaces a state handle in ZooKeeper and discards the old state handle.
     *
     * @param pathInZooKeeper Destination path in ZooKeeper (expected to exist and start with a '/')
     * @param expectedVersion Expected version of the node to replace
     * @param state The new state to replace the old one
     * @throws Exception If a ZooKeeper or state handle operation fails
     */
    @Override
    public void replace(String pathInZooKeeper, IntegerResourceVersion expectedVersion, T state)
            throws Exception {
        checkNotNull(pathInZooKeeper, "Path in ZooKeeper");
        checkNotNull(state, "State");

        final String path = normalizePath(pathInZooKeeper);

        RetrievableStateHandle<T> oldStateHandle = get(path, false);

        RetrievableStateHandle<T> newStateHandle = storage.store(state);

        final byte[] serializedStateHandle = serializeOrDiscard(newStateHandle);

        // initialize flags to serve the failure case
        boolean discardOldState = false;
        boolean discardNewState = true;
        try {
            setStateHandle(path, serializedStateHandle, expectedVersion.getValue());

            // swap subject for deletion in case of success
            discardOldState = true;
            discardNewState = false;
        } catch (Exception e) {
            if (indicatesPossiblyInconsistentState(e)) {
                // it's unclear whether the state handle metadata was written to ZooKeeper -
                // hence, we don't discard any data
                discardNewState = false;
                throw new PossibleInconsistentStateException(e);
            }

            // We wrap the exception here so that it could be caught in DefaultJobGraphStore
            throw ExceptionUtils.findThrowable(e, KeeperException.NoNodeException.class)
                    .map(
                            nnee ->
                                    new NotExistException(
                                            "ZooKeeper node " + path + " does not exist.", nnee))
                    .orElseThrow(() -> e);
        } finally {
            if (discardOldState) {
                oldStateHandle.discardState();
            }

            if (discardNewState) {
                newStateHandle.discardState();
            }
        }
    }

    // this method is provided for the sole purpose of easier testing
    @VisibleForTesting
    protected void setStateHandle(String path, byte[] serializedStateHandle, int expectedVersion)
            throws Exception {
        // Replace state handle in ZooKeeper.
        client.setData().withVersion(expectedVersion).forPath(path, serializedStateHandle);
    }

    private boolean indicatesPossiblyInconsistentState(Exception e) {
        return !PRE_COMMIT_EXCEPTIONS.contains(e.getClass());
    }

    /**
     * Returns the version of the node if it exists or <code>-1</code> if it doesn't.
     *
     * @param pathInZooKeeper Path in ZooKeeper to check
     * @return Version of the ZNode if the path exists, <code>-1</code> otherwise.
     * @throws Exception If the ZooKeeper operation fails
     */
    @Override
    public IntegerResourceVersion exists(String pathInZooKeeper) throws Exception {
        checkNotNull(pathInZooKeeper, "Path in ZooKeeper");

        final String path = normalizePath(pathInZooKeeper);

        Stat stat = client.checkExists().forPath(path);

        if (stat != null) {
            return IntegerResourceVersion.valueOf(stat.getVersion());
        }

        return IntegerResourceVersion.notExisting();
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
    @Override
    public RetrievableStateHandle<T> getAndLock(String pathInZooKeeper) throws Exception {
        return get(pathInZooKeeper, true);
    }

    /**
     * Return a list of all valid paths for state handles.
     *
     * @return List of valid state handle paths in ZooKeeper
     * @throws Exception if a ZooKeeper operation fails
     */
    @Override
    public Collection<String> getAllHandles() throws Exception {
        final String path = "/";

        while (true) {
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
    @Override
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
                        LOG.warn(
                                "Could not get all ZooKeeper children. Node {} contained "
                                        + "corrupted data. Ignoring this node.",
                                path,
                                ioException);
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
     * Releases the lock for the given state node and tries to remove the state node if it is no
     * longer locked. It returns the {@link RetrievableStateHandle} stored under the given state
     * node if any.
     *
     * @param pathInZooKeeper Path of state handle to remove
     * @return True if the state handle could be released
     * @throws Exception If the ZooKeeper operation or discarding the state handle fails
     */
    @Override
    public boolean releaseAndTryRemove(String pathInZooKeeper) throws Exception {
        checkNotNull(pathInZooKeeper, "Path in ZooKeeper");

        final String path = normalizePath(pathInZooKeeper);

        RetrievableStateHandle<T> stateHandle = null;

        try {
            stateHandle = get(path, false);
        } catch (Exception e) {
            LOG.warn("Could not retrieve the state handle from node {}.", path, e);
        }

        release(pathInZooKeeper);

        try {
            client.delete().forPath(path);
        } catch (KeeperException.NotEmptyException ignored) {
            LOG.debug("Could not delete znode {} because it is still locked.", path);
            return false;
        }

        if (stateHandle != null) {
            stateHandle.discardState();
        }

        return true;
    }

    /**
     * Releases all lock nodes of this ZooKeeperStateHandleStores and tries to remove all state
     * nodes which are not locked anymore.
     *
     * <p>The delete operation is executed asynchronously
     *
     * @throws Exception if the delete operation fails
     */
    @Override
    public void releaseAndTryRemoveAll() throws Exception {
        Collection<String> children = getAllHandles();

        Exception exception = null;

        for (String child : children) {
            try {
                releaseAndTryRemove('/' + child);
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        if (exception != null) {
            throw new Exception(
                    "Could not properly release and try removing all state nodes.", exception);
        }
    }

    /**
     * Releases the lock from the node under the given ZooKeeper path. If no lock exists, then
     * nothing happens.
     *
     * @param pathInZooKeeper Path describing the ZooKeeper node
     * @throws Exception if the delete operation of the lock node fails
     */
    @Override
    public void release(String pathInZooKeeper) throws Exception {
        final String path = normalizePath(pathInZooKeeper);

        try {
            client.delete().forPath(getLockPath(path));
        } catch (KeeperException.NoNodeException ignored) {
            // we have never locked this node
        } catch (Exception e) {
            throw new Exception(
                    "Could not release the lock: " + getLockPath(pathInZooKeeper) + '.', e);
        }
    }

    /**
     * Releases all lock nodes of this ZooKeeperStateHandleStore.
     *
     * @throws Exception if the delete operation of a lock file fails
     */
    @Override
    public void releaseAll() throws Exception {
        Collection<String> children = getAllHandles();

        Exception exception = null;

        for (String child : children) {
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

    /**
     * Recursively deletes all children.
     *
     * @throws Exception ZK errors
     */
    @Override
    public void clearEntries() throws Exception {
        final String path = "/" + client.getNamespace();
        LOG.info("Removing {} from ZooKeeper", path);
        ZKPaths.deleteChildren(client.getZookeeperClient().getZooKeeper(), path, true);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "{namespace='" + client.getNamespace() + "'}";
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
            } catch (KeeperException.NoNodeException ex) {
                // We could run into this exception because the parent node does not exist when we
                // are trying to lock.
                // We wrap the exception here so that it could be caught in DefaultJobGraphStore
                throw new NotExistException("ZooKeeper node " + path + " does not exist.", ex);
            }
        }

        boolean success = false;

        try {
            byte[] data = client.getData().forPath(path);

            RetrievableStateHandle<T> retrievableStateHandle = deserialize(data);

            success = true;

            return retrievableStateHandle;
        } catch (KeeperException.NoNodeException ex) {
            // We wrap the exception here so that it could be caught in DefaultJobGraphStore
            throw new NotExistException("ZooKeeper node " + path + " does not exist.", ex);
        } catch (IOException | ClassNotFoundException e) {
            throw new IOException(
                    "Failed to deserialize state handle from ZooKeeper data from " + path + '.', e);
        } finally {
            if (!success && lock) {
                // release the lock
                release(path);
            }
        }
    }

    /**
     * Makes sure that every path starts with a "/".
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
}
