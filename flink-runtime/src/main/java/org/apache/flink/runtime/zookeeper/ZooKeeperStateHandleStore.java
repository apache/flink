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
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.utils.ZKPaths;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.runtime.util.StateHandleStoreUtils.deserialize;
import static org.apache.flink.runtime.util.StateHandleStoreUtils.serializeOrDiscard;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Class which stores state via the provided {@link RetrievableStateStorageHelper} and writes the
 * returned state handle to ZooKeeper. The ZooKeeper node can be locked by creating an ephemeral
 * child in the lock sub-path. The implementation only allows the deletion of the ZooKeeper node if
 * the lock sub-path has no children. That way we protect concurrent accesses from different
 * ZooKeeperStateHandleStore instances.
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
 * State handle in ZooKeeper and not marked for deletion =&gt; State handle exists
 * </pre>
 *
 * <pre>
 * State handle in ZooKeeper and marked for deletion =&gt; State handle might or might not be deleted, yet
 * </pre>
 *
 * <pre>
 * State handle not in ZooKeeper =&gt; State handle does not exist
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

    /** Pre-commit exceptions that don't imply data inconsistency. */
    private static final Set<Class<? extends KeeperException>> SAFE_PRE_COMMIT_EXCEPTIONS =
            new HashSet<>(
                    Arrays.asList(
                            KeeperException.NodeExistsException.class,
                            KeeperException.BadArgumentsException.class,
                            KeeperException.NoNodeException.class,
                            KeeperException.NoAuthException.class,
                            KeeperException.BadVersionException.class,
                            KeeperException.AuthFailedException.class,
                            KeeperException.InvalidACLException.class,
                            KeeperException.SessionMovedException.class,
                            KeeperException.NotReadOnlyException.class));

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
     * <p>The operation will fail if there is already a node under the given path.
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
        final Optional<Stat> maybeStat = getStat(path);

        if (maybeStat.isPresent()) {
            if (isNotMarkedForDeletion(maybeStat.get())) {
                throw new AlreadyExistException(
                        String.format("ZooKeeper node %s already exists.", path));
            }

            Preconditions.checkState(
                    releaseAndTryRemove(path),
                    "The state is marked for deletion and, therefore, should be deletable.");
        }

        final RetrievableStateHandle<T> storeHandle = storage.store(state);
        final byte[] serializedStoreHandle = serializeOrDiscard(storeHandle);
        try {
            writeStoreHandleTransactionally(path, serializedStoreHandle);
            return storeHandle;
        } catch (KeeperException.NodeExistsException e) {
            // Transactions are not idempotent in the curator version we're currently using, so it
            // is actually possible that we've re-tried a transaction that has already succeeded.
            // We've ensured that the node hasn't been present prior executing the transaction, so
            // we can assume that this is a result of the retry mechanism.
            return storeHandle;
        } catch (Exception e) {
            if (indicatesPossiblyInconsistentState(e)) {
                throw new PossibleInconsistentStateException(e);
            }
            // In case of any other failure, discard the state and rethrow the exception.
            storeHandle.discardState();
            throw e;
        }
    }

    // this method is provided for the sole purpose of easier testing
    @VisibleForTesting
    void writeStoreHandleTransactionally(String path, byte[] serializedStoreHandle)
            throws Exception {
        // Write state handle (not the actual state) to ZooKeeper. This is expected to be smaller
        // than the state itself. This level of indirection makes sure that data in ZooKeeper is
        // small, because ZooKeeper is designed for data in the KB range, but the state can be
        // larger. Create the lock node in a transaction with the actual state node. That way we can
        // prevent race conditions with a concurrent delete operation.
        client.inTransaction()
                .create()
                .withMode(CreateMode.PERSISTENT)
                .forPath(path, serializedStoreHandle)
                .and()
                .create()
                .withMode(CreateMode.PERSISTENT)
                .forPath(getRootLockPath(path))
                .and()
                .create()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(getInstanceLockPath(path))
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
     * @throws IllegalStateException if the replace operation shall be performed on a node that is
     *     not locked for this specific instance.
     */
    @Override
    public void replace(String pathInZooKeeper, IntegerResourceVersion expectedVersion, T state)
            throws Exception {
        checkNotNull(pathInZooKeeper, "Path in ZooKeeper");
        checkNotNull(state, "State");

        final String path = normalizePath(pathInZooKeeper);

        checkState(
                hasLock(path),
                "'{}' is only allowed to be replaced if the instance has a lock on this node.",
                path);

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
        // Replace state handle in ZooKeeper. We use idempotent set here to avoid a scenario, where
        // we retry an update, because we didn't receive a proper acknowledgement due to temporary
        // connection loss. Without idempotent flag this would result in a BadVersionException,
        // because the version on server no longer matches our expected version. With this flag,
        // when curator receives BadVersionException internally, it checks whether the content on
        // the server matches our intended update and its version is our expectedVersion + 1.
        client.setData()
                .idempotent()
                .withVersion(expectedVersion)
                .forPath(path, serializedStateHandle);
    }

    private boolean indicatesPossiblyInconsistentState(Exception e) {
        return !SAFE_PRE_COMMIT_EXCEPTIONS.contains(e.getClass());
    }

    /**
     * Returns the version of the node if it exists and is not marked for deletion or <code>-1
     * </code>.
     *
     * @param pathInZooKeeper Path in ZooKeeper to check
     * @return Version of the ZNode if the path exists and is not marked for deletion, <code>-1
     *     </code> otherwise.
     * @throws Exception If the ZooKeeper operation fails
     */
    @Override
    public IntegerResourceVersion exists(String pathInZooKeeper) throws Exception {
        checkNotNull(pathInZooKeeper, "Path in ZooKeeper");

        return getStat(pathInZooKeeper)
                .filter(ZooKeeperStateHandleStore::isNotMarkedForDeletion)
                .map(stat -> IntegerResourceVersion.valueOf(stat.getVersion()))
                .orElse(IntegerResourceVersion.notExisting());
    }

    private static boolean isNotMarkedForDeletion(Stat stat) {
        return stat.getNumChildren() > 0;
    }

    private Optional<Stat> getStat(String path) throws Exception {
        final String normalizedPath = normalizePath(path);
        return Optional.ofNullable(client.checkExists().forPath(normalizedPath));
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
                    LOG.debug(
                            "Unable to get all handles, retrying (ZNode was likely deleted concurrently: {})",
                            ignored.getMessage());
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
    @Override
    public List<Tuple2<RetrievableStateHandle<T>, String>> getAllAndLock() throws Exception {
        return getAllAndLock(parentNodePath -> client.getChildren().forPath(parentNodePath));
    }

    @VisibleForTesting
    List<Tuple2<RetrievableStateHandle<T>, String>> getAllAndLock(
            FunctionWithException<String, List<String>, Exception> getNodeChildren)
            throws Exception {
        final List<Tuple2<RetrievableStateHandle<T>, String>> stateHandles = new ArrayList<>();

        final String rootPath = "/";
        boolean success = false;

        while (!success) {
            stateHandles.clear();

            Stat stat = client.checkExists().forPath(rootPath);
            if (stat == null) {
                break; // Node does not exist, done.
            } else {
                // Initial cVersion (number of changes to the children of this node)
                int initialCVersion = stat.getCversion();

                final List<String> children = getNodeChildren.apply(rootPath);

                for (String path : children) {
                    path = rootPath + path;

                    try {
                        final RetrievableStateHandle<T> stateHandle = getAndLock(path);
                        stateHandles.add(new Tuple2<>(stateHandle, path));
                    } catch (NotExistException ignored) {
                        // The node is subject for deletion which can mean two things:
                        // 1. The state is marked for deletion: The cVersion of the node does not
                        // necessarily change. We're not interested in the state anymore, anyway.
                        // Therefore, this error can be ignored.
                        // 2. An actual concurrent deletion is going on. The child node is gone.
                        // That would affect the cVersion of the parent node and, as a consequence,
                        // would trigger a restart the logic through the while loop.
                    } catch (IOException ioException) {
                        LOG.warn(
                                "Could not get all ZooKeeper children. Node {} contained "
                                        + "corrupted data. Ignoring this node.",
                                path,
                                ioException);
                    }
                }

                int finalCVersion = client.checkExists().forPath(rootPath).getCversion();

                // Check for concurrent modifications
                success = initialCVersion == finalCVersion;
            }
        }

        return stateHandles;
    }

    /**
     * Releases the lock for the given state node and tries to remove the state node if it is no
     * longer locked.
     *
     * @param pathInZooKeeper Path of state handle to remove
     * @return {@code true} if the state handle could be deleted; {@code false}, if the handle is
     *     locked by another connection.
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
            deleteIfExists(getRootLockPath(path));
        } catch (KeeperException.NotEmptyException ignored) {
            LOG.debug(
                    "Could not delete znode {} because it is still locked.", getRootLockPath(path));
            return false;
        }

        if (stateHandle != null) {
            stateHandle.discardState();
        }

        // we can now commit the deletion by removing the parent node
        deleteIfExists(path);

        return true;
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
        final String lockPath = getInstanceLockPath(path);
        try {
            deleteIfExists(lockPath);
        } catch (Exception e) {
            throw new Exception("Could not release the lock: " + lockPath + '.', e);
        }
    }

    private void deleteIfExists(String path) throws Exception {
        final String normalizedPath = normalizePath(path);
        try {
            client.delete().forPath(normalizedPath);
        } catch (KeeperException.NoNodeException ignored) {
            LOG.debug(
                    "ZNode '{}' is already marked for deletion. Command is ignored.",
                    normalizedPath);
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
     * Checks whether a lock is created for this instance on the passed ZooKeeper node.
     *
     * @param rootPath The node that shall be checked.
     * @return {@code true} if the lock exists; {@code false} otherwise.
     */
    private boolean hasLock(String rootPath) throws Exception {
        final String normalizedRootPath = normalizePath(rootPath);
        try {
            return client.checkExists().forPath(getInstanceLockPath(normalizedRootPath)) != null;
        } catch (KeeperException.NoNodeException e) {
            // this is the case if the node is marked for deletion or already deleted
            return false;
        }
    }

    /**
     * Returns the path for the lock node relative to the given path.
     *
     * @param rootPath Root path under which the lock node shall be created
     * @return Path for the lock node
     */
    @VisibleForTesting
    String getInstanceLockPath(String rootPath) {
        return getRootLockPath(rootPath) + '/' + lockNode;
    }

    /**
     * Returns the sub-path for lock nodes of the corresponding node (referred to through the passed
     * {@code rooPath}. The returned sub-path collects the lock nodes for the {@code rootPath}'s
     * node. The {@code rootPath} is marked for deletion if the sub-path for lock nodes is deleted.
     */
    @VisibleForTesting
    static String getRootLockPath(String rootPath) {
        return rootPath + "/locks";
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
    private RetrievableStateHandle<T> get(String pathInZooKeeper, boolean lock) throws Exception {
        checkNotNull(pathInZooKeeper, "Path in ZooKeeper");

        final String path = normalizePath(pathInZooKeeper);

        if (lock) {
            // try to lock the node
            try {
                client.create().withMode(CreateMode.EPHEMERAL).forPath(getInstanceLockPath(path));
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
