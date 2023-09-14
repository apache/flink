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

package org.apache.flink.runtime.util;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.SecurityOptions;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DefaultCompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.DefaultCompletedCheckpointStoreUtils;
import org.apache.flink.runtime.checkpoint.DefaultLastStateConnectionStateListener;
import org.apache.flink.runtime.checkpoint.ZooKeeperCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.ZooKeeperCheckpointStoreUtil;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.zookeeper.CuratorFrameworkWithUnhandledErrorListener;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.RestoreMode;
import org.apache.flink.runtime.jobmanager.DefaultJobGraphStore;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.ZooKeeperJobGraphStoreUtil;
import org.apache.flink.runtime.jobmanager.ZooKeeperJobGraphStoreWatcher;
import org.apache.flink.runtime.leaderelection.LeaderInformation;
import org.apache.flink.runtime.leaderretrieval.DefaultLeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalDriverFactory;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriver;
import org.apache.flink.runtime.leaderretrieval.ZooKeeperLeaderRetrievalDriverFactory;
import org.apache.flink.runtime.persistence.RetrievableStateStorageHelper;
import org.apache.flink.runtime.persistence.filesystem.FileSystemStateStorageHelper;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.state.SharedStateRegistryFactory;
import org.apache.flink.runtime.zookeeper.ZooKeeperStateHandleStore;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.function.RunnableWithException;

import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFramework;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.api.ACLProvider;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.api.UnhandledErrorListener;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.imps.DefaultACLProvider;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.recipes.cache.TreeCacheSelector;
import org.apache.flink.shaded.curator5.org.apache.curator.framework.state.SessionConnectionStateErrorPolicy;
import org.apache.flink.shaded.curator5.org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.CreateMode;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.KeeperException;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.ZooDefs;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.ACL;
import org.apache.flink.shaded.zookeeper3.org.apache.zookeeper.data.Stat;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Class containing helper functions to interact with ZooKeeper. */
public class ZooKeeperUtils {

    private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperUtils.class);

    /** The prefix of the submitted job graph file. */
    public static final String HA_STORAGE_SUBMITTED_JOBGRAPH_PREFIX = "submittedJobGraph";

    /** The prefix of the completed checkpoint file. */
    public static final String HA_STORAGE_COMPLETED_CHECKPOINT = "completedCheckpoint";

    /** The prefix of the resource manager node. */
    public static final String RESOURCE_MANAGER_NODE = "resource_manager";

    private static final String DISPATCHER_NODE = "dispatcher";

    private static final String LEADER_NODE = "leader";

    private static final String REST_SERVER_NODE = "rest_server";

    private static final String LEADER_LATCH_NODE = "latch";

    private static final String CONNECTION_INFO_NODE = "connection_info";

    public static String getLeaderPathForJob(JobID jobId) {
        return generateZookeeperPath(getJobsPath(), getPathForJob(jobId));
    }

    public static String getJobsPath() {
        return "/jobs";
    }

    private static String getCheckpointsPath() {
        return "/checkpoints";
    }

    public static String getCheckpointIdCounterPath() {
        return "/checkpoint_id_counter";
    }

    public static String getLeaderPath() {
        return generateZookeeperPath(LEADER_NODE);
    }

    public static String getDispatcherNode() {
        return DISPATCHER_NODE;
    }

    public static String getResourceManagerNode() {
        return RESOURCE_MANAGER_NODE;
    }

    public static String getRestServerNode() {
        return REST_SERVER_NODE;
    }

    public static String getLeaderLatchPath() {
        return generateZookeeperPath(LEADER_LATCH_NODE);
    }

    public static String getLeaderPath(String suffix) {
        return generateZookeeperPath(LEADER_NODE, suffix);
    }

    public static String generateConnectionInformationPath(String path) {
        return generateZookeeperPath(path, CONNECTION_INFO_NODE);
    }

    public static boolean isConnectionInfoPath(String path) {
        return path.endsWith(CONNECTION_INFO_NODE);
    }

    public static String generateLeaderLatchPath(String path) {
        return generateZookeeperPath(path, LEADER_LATCH_NODE);
    }

    /**
     * Starts a {@link CuratorFramework} instance and connects it to the given ZooKeeper quorum.
     *
     * @param configuration {@link Configuration} object containing the configuration values
     * @param fatalErrorHandler {@link FatalErrorHandler} fatalErrorHandler to handle unexpected
     *     errors of {@link CuratorFramework}
     * @return {@link CuratorFrameworkWithUnhandledErrorListener} instance
     */
    public static CuratorFrameworkWithUnhandledErrorListener startCuratorFramework(
            Configuration configuration, FatalErrorHandler fatalErrorHandler) {
        checkNotNull(configuration, "configuration");
        String zkQuorum = configuration.getValue(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM);

        if (zkQuorum == null || StringUtils.isBlank(zkQuorum)) {
            throw new RuntimeException(
                    "No valid ZooKeeper quorum has been specified. "
                            + "You can specify the quorum via the configuration key '"
                            + HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM.key()
                            + "'.");
        }

        int sessionTimeout =
                configuration.getInteger(HighAvailabilityOptions.ZOOKEEPER_SESSION_TIMEOUT);

        int connectionTimeout =
                configuration.getInteger(HighAvailabilityOptions.ZOOKEEPER_CONNECTION_TIMEOUT);

        int retryWait = configuration.getInteger(HighAvailabilityOptions.ZOOKEEPER_RETRY_WAIT);

        int maxRetryAttempts =
                configuration.getInteger(HighAvailabilityOptions.ZOOKEEPER_MAX_RETRY_ATTEMPTS);

        String root = configuration.getValue(HighAvailabilityOptions.HA_ZOOKEEPER_ROOT);

        String namespace = configuration.getValue(HighAvailabilityOptions.HA_CLUSTER_ID);

        boolean disableSaslClient =
                configuration.getBoolean(SecurityOptions.ZOOKEEPER_SASL_DISABLE);

        ACLProvider aclProvider;

        ZkClientACLMode aclMode = ZkClientACLMode.fromConfig(configuration);

        if (disableSaslClient && aclMode == ZkClientACLMode.CREATOR) {
            String errorMessage =
                    "Cannot set ACL role to "
                            + ZkClientACLMode.CREATOR
                            + "  since SASL authentication is "
                            + "disabled through the "
                            + SecurityOptions.ZOOKEEPER_SASL_DISABLE.key()
                            + " property";
            LOG.warn(errorMessage);
            throw new IllegalConfigurationException(errorMessage);
        }

        if (aclMode == ZkClientACLMode.CREATOR) {
            LOG.info("Enforcing creator for ZK connections");
            aclProvider = new SecureAclProvider();
        } else {
            LOG.info("Enforcing default ACL for ZK connections");
            aclProvider = new DefaultACLProvider();
        }

        String rootWithNamespace = generateZookeeperPath(root, namespace);

        LOG.info("Using '{}' as Zookeeper namespace.", rootWithNamespace);

        boolean ensembleTracking =
                configuration.getBoolean(HighAvailabilityOptions.ZOOKEEPER_ENSEMBLE_TRACKING);

        final CuratorFrameworkFactory.Builder curatorFrameworkBuilder =
                CuratorFrameworkFactory.builder()
                        .connectString(zkQuorum)
                        .sessionTimeoutMs(sessionTimeout)
                        .connectionTimeoutMs(connectionTimeout)
                        .retryPolicy(new ExponentialBackoffRetry(retryWait, maxRetryAttempts))
                        // Curator prepends a '/' manually and throws an Exception if the
                        // namespace starts with a '/'.
                        .namespace(trimStartingSlash(rootWithNamespace))
                        .ensembleTracker(ensembleTracking)
                        .aclProvider(aclProvider);

        if (configuration.get(HighAvailabilityOptions.ZOOKEEPER_TOLERATE_SUSPENDED_CONNECTIONS)) {
            curatorFrameworkBuilder.connectionStateErrorPolicy(
                    new SessionConnectionStateErrorPolicy());
        }
        return startCuratorFramework(curatorFrameworkBuilder, fatalErrorHandler);
    }

    /**
     * Starts a {@link CuratorFramework} instance and connects it to the given ZooKeeper quorum from
     * a builder.
     *
     * @param builder {@link CuratorFrameworkFactory.Builder} A builder for curatorFramework.
     * @param fatalErrorHandler {@link FatalErrorHandler} fatalErrorHandler to handle unexpected
     *     errors of {@link CuratorFramework}
     * @return {@link CuratorFrameworkWithUnhandledErrorListener} instance
     */
    @VisibleForTesting
    public static CuratorFrameworkWithUnhandledErrorListener startCuratorFramework(
            CuratorFrameworkFactory.Builder builder, FatalErrorHandler fatalErrorHandler) {
        CuratorFramework cf = builder.build();
        UnhandledErrorListener unhandledErrorListener =
                (message, throwable) -> {
                    LOG.error(
                            "Unhandled error in curator framework, error message: {}",
                            message,
                            throwable);
                    // The exception thrown in UnhandledErrorListener will be caught by
                    // CuratorFramework. So we mostly trigger exit process or interact with main
                    // thread to inform the failure in FatalErrorHandler.
                    fatalErrorHandler.onFatalError(throwable);
                };
        cf.getUnhandledErrorListenable().addListener(unhandledErrorListener);
        cf.start();
        return new CuratorFrameworkWithUnhandledErrorListener(cf, unhandledErrorListener);
    }

    /** Returns whether {@link HighAvailabilityMode#ZOOKEEPER} is configured. */
    public static boolean isZooKeeperRecoveryMode(Configuration flinkConf) {
        return HighAvailabilityMode.fromConfig(flinkConf).equals(HighAvailabilityMode.ZOOKEEPER);
    }

    /**
     * Returns the configured ZooKeeper quorum (and removes whitespace, because ZooKeeper does not
     * tolerate it).
     */
    public static String getZooKeeperEnsemble(Configuration flinkConf)
            throws IllegalConfigurationException {

        String zkQuorum = flinkConf.getValue(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM);

        if (zkQuorum == null || StringUtils.isBlank(zkQuorum)) {
            throw new IllegalConfigurationException("No ZooKeeper quorum specified in config.");
        }

        // Remove all whitespace
        zkQuorum = zkQuorum.replaceAll("\\s+", "");

        return zkQuorum;
    }

    /**
     * Creates a {@link DefaultLeaderRetrievalService} instance with {@link
     * ZooKeeperLeaderRetrievalDriver}.
     *
     * @param client The {@link CuratorFramework} ZooKeeper client to use
     * @return {@link DefaultLeaderRetrievalService} instance.
     */
    public static DefaultLeaderRetrievalService createLeaderRetrievalService(
            final CuratorFramework client) {
        return createLeaderRetrievalService(client, "", new Configuration());
    }

    /**
     * Creates a {@link DefaultLeaderRetrievalService} instance with {@link
     * ZooKeeperLeaderRetrievalDriver}.
     *
     * @param client The {@link CuratorFramework} ZooKeeper client to use
     * @param path The path for the leader retrieval
     * @param configuration configuration for further config options
     * @return {@link DefaultLeaderRetrievalService} instance.
     */
    public static DefaultLeaderRetrievalService createLeaderRetrievalService(
            final CuratorFramework client, final String path, final Configuration configuration) {
        return new DefaultLeaderRetrievalService(
                createLeaderRetrievalDriverFactory(client, path, configuration));
    }

    /**
     * Creates a {@link LeaderRetrievalDriverFactory} implemented by ZooKeeper.
     *
     * @param client The {@link CuratorFramework} ZooKeeper client to use
     * @return {@link LeaderRetrievalDriverFactory} instance.
     */
    public static ZooKeeperLeaderRetrievalDriverFactory createLeaderRetrievalDriverFactory(
            final CuratorFramework client) {
        return createLeaderRetrievalDriverFactory(client, "");
    }

    /**
     * Creates a {@link LeaderRetrievalDriverFactory} implemented by ZooKeeper.
     *
     * @param client The {@link CuratorFramework} ZooKeeper client to use
     * @param path The parent path that shall be used by the client.
     * @return {@link LeaderRetrievalDriverFactory} instance.
     */
    public static ZooKeeperLeaderRetrievalDriverFactory createLeaderRetrievalDriverFactory(
            final CuratorFramework client, String path) {
        return createLeaderRetrievalDriverFactory(client, path, new Configuration());
    }

    /**
     * Creates a {@link LeaderRetrievalDriverFactory} implemented by ZooKeeper.
     *
     * @param client The {@link CuratorFramework} ZooKeeper client to use
     * @param path The path for the leader zNode
     * @param configuration configuration for further config options
     * @return {@link LeaderRetrievalDriverFactory} instance.
     */
    public static ZooKeeperLeaderRetrievalDriverFactory createLeaderRetrievalDriverFactory(
            final CuratorFramework client, final String path, final Configuration configuration) {
        final ZooKeeperLeaderRetrievalDriver.LeaderInformationClearancePolicy
                leaderInformationClearancePolicy;

        if (configuration.get(HighAvailabilityOptions.ZOOKEEPER_TOLERATE_SUSPENDED_CONNECTIONS)) {
            leaderInformationClearancePolicy =
                    ZooKeeperLeaderRetrievalDriver.LeaderInformationClearancePolicy
                            .ON_LOST_CONNECTION;
        } else {
            leaderInformationClearancePolicy =
                    ZooKeeperLeaderRetrievalDriver.LeaderInformationClearancePolicy
                            .ON_SUSPENDED_CONNECTION;
        }

        return new ZooKeeperLeaderRetrievalDriverFactory(
                client, path, leaderInformationClearancePolicy);
    }

    public static void writeLeaderInformationToZooKeeper(
            LeaderInformation leaderInformation,
            CuratorFramework curatorFramework,
            BooleanSupplier hasLeadershipCheck,
            String connectionInformationPath)
            throws Exception {
        final byte[] data;

        if (leaderInformation.isEmpty()) {
            data = null;
        } else {
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final ObjectOutputStream oos = new ObjectOutputStream(baos);

            oos.writeUTF(leaderInformation.getLeaderAddress());
            oos.writeObject(leaderInformation.getLeaderSessionID());

            oos.close();

            data = baos.toByteArray();
        }

        boolean dataWritten = false;

        while (!dataWritten && hasLeadershipCheck.getAsBoolean()) {
            Stat stat = curatorFramework.checkExists().forPath(connectionInformationPath);

            if (stat != null) {
                long owner = stat.getEphemeralOwner();
                long sessionID =
                        curatorFramework.getZookeeperClient().getZooKeeper().getSessionId();

                if (owner == sessionID) {
                    try {
                        curatorFramework.setData().forPath(connectionInformationPath, data);

                        dataWritten = true;
                    } catch (KeeperException.NoNodeException noNode) {
                        // node was deleted in the meantime
                    }
                } else {
                    try {
                        curatorFramework.delete().forPath(connectionInformationPath);
                    } catch (KeeperException.NoNodeException noNode) {
                        // node was deleted in the meantime --> try again
                    }
                }
            } else {
                try {
                    curatorFramework
                            .create()
                            .creatingParentsIfNeeded()
                            .withMode(CreateMode.EPHEMERAL)
                            .forPath(connectionInformationPath, data);

                    dataWritten = true;
                } catch (KeeperException.NodeExistsException nodeExists) {
                    // node has been created in the meantime --> try again
                }
            }
        }
    }

    public static LeaderInformation readLeaderInformation(byte[] data)
            throws IOException, ClassNotFoundException {
        if (data != null && data.length > 0) {
            final ByteArrayInputStream bais = new ByteArrayInputStream(data);
            final String leaderAddress;
            final UUID leaderSessionID;

            try (final ObjectInputStream ois = new ObjectInputStream(bais)) {
                leaderAddress = ois.readUTF();
                leaderSessionID = (UUID) ois.readObject();
            }

            return LeaderInformation.known(leaderSessionID, leaderAddress);
        } else {
            return LeaderInformation.empty();
        }
    }

    /**
     * Creates a {@link DefaultJobGraphStore} instance with {@link ZooKeeperStateHandleStore},
     * {@link ZooKeeperJobGraphStoreWatcher} and {@link ZooKeeperJobGraphStoreUtil}.
     *
     * @param client The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration {@link Configuration} object
     * @return {@link DefaultJobGraphStore} instance
     * @throws Exception if the submitted job graph store cannot be created
     */
    public static JobGraphStore createJobGraphs(
            CuratorFramework client, Configuration configuration) throws Exception {

        checkNotNull(configuration, "Configuration");

        RetrievableStateStorageHelper<JobGraph> stateStorage =
                createFileSystemStateStorage(configuration, HA_STORAGE_SUBMITTED_JOBGRAPH_PREFIX);

        // ZooKeeper submitted jobs root dir
        String zooKeeperJobsPath =
                configuration.getString(HighAvailabilityOptions.HA_ZOOKEEPER_JOBGRAPHS_PATH);

        // Ensure that the job graphs path exists
        client.newNamespaceAwareEnsurePath(zooKeeperJobsPath).ensure(client.getZookeeperClient());

        // All operations will have the path as root
        CuratorFramework facade = client.usingNamespace(client.getNamespace() + zooKeeperJobsPath);

        final String zooKeeperFullJobsPath = client.getNamespace() + zooKeeperJobsPath;

        final ZooKeeperStateHandleStore<JobGraph> zooKeeperStateHandleStore =
                new ZooKeeperStateHandleStore<>(facade, stateStorage);

        final PathChildrenCache pathCache = new PathChildrenCache(facade, "/", false);

        return new DefaultJobGraphStore<>(
                zooKeeperStateHandleStore,
                new ZooKeeperJobGraphStoreWatcher(pathCache),
                ZooKeeperJobGraphStoreUtil.INSTANCE);
    }

    /**
     * Creates a {@link DefaultCompletedCheckpointStore} instance with {@link
     * ZooKeeperStateHandleStore}.
     *
     * @param client The {@link CuratorFramework} ZooKeeper client to use
     * @param configuration {@link Configuration} object
     * @param maxNumberOfCheckpointsToRetain The maximum number of checkpoints to retain
     * @param executor to run ZooKeeper callbacks
     * @param restoreMode the mode in which the job is being restored
     * @return {@link DefaultCompletedCheckpointStore} instance
     * @throws Exception if the completed checkpoint store cannot be created
     */
    public static CompletedCheckpointStore createCompletedCheckpoints(
            CuratorFramework client,
            Configuration configuration,
            int maxNumberOfCheckpointsToRetain,
            SharedStateRegistryFactory sharedStateRegistryFactory,
            Executor ioExecutor,
            Executor executor,
            RestoreMode restoreMode)
            throws Exception {

        checkNotNull(configuration, "Configuration");

        RetrievableStateStorageHelper<CompletedCheckpoint> stateStorage =
                createFileSystemStateStorage(configuration, HA_STORAGE_COMPLETED_CHECKPOINT);

        final ZooKeeperStateHandleStore<CompletedCheckpoint> completedCheckpointStateHandleStore =
                createZooKeeperStateHandleStore(client, getCheckpointsPath(), stateStorage);
        Collection<CompletedCheckpoint> completedCheckpoints =
                DefaultCompletedCheckpointStoreUtils.retrieveCompletedCheckpoints(
                        completedCheckpointStateHandleStore, ZooKeeperCheckpointStoreUtil.INSTANCE);
        final CompletedCheckpointStore zooKeeperCompletedCheckpointStore =
                new DefaultCompletedCheckpointStore<>(
                        maxNumberOfCheckpointsToRetain,
                        completedCheckpointStateHandleStore,
                        ZooKeeperCheckpointStoreUtil.INSTANCE,
                        completedCheckpoints,
                        sharedStateRegistryFactory.create(
                                ioExecutor, completedCheckpoints, restoreMode),
                        executor);
        LOG.info(
                "Initialized {} in '{}' with {}.",
                DefaultCompletedCheckpointStore.class.getSimpleName(),
                completedCheckpointStateHandleStore,
                getCheckpointsPath());
        return zooKeeperCompletedCheckpointStore;
    }

    /** Returns the JobID as a String (with leading slash). */
    public static String getPathForJob(JobID jobId) {
        checkNotNull(jobId, "Job ID");
        return String.format("/%s", jobId);
    }

    /**
     * Creates an instance of {@link ZooKeeperStateHandleStore}.
     *
     * @param client ZK client
     * @param path Path to use for the client namespace
     * @param stateStorage RetrievableStateStorageHelper that persist the actual state and whose
     *     returned state handle is then written to ZooKeeper
     * @param <T> Type of state
     * @return {@link ZooKeeperStateHandleStore} instance
     * @throws Exception ZK errors
     */
    public static <T extends Serializable>
            ZooKeeperStateHandleStore<T> createZooKeeperStateHandleStore(
                    final CuratorFramework client,
                    final String path,
                    final RetrievableStateStorageHelper<T> stateStorage)
                    throws Exception {
        return new ZooKeeperStateHandleStore<>(
                useNamespaceAndEnsurePath(client, path), stateStorage);
    }

    /**
     * Creates a {@link ZooKeeperCheckpointIDCounter} instance.
     *
     * @param client The {@link CuratorFramework} ZooKeeper client to use
     * @return {@link ZooKeeperCheckpointIDCounter} instance
     */
    public static ZooKeeperCheckpointIDCounter createCheckpointIDCounter(CuratorFramework client) {
        return new ZooKeeperCheckpointIDCounter(
                client, new DefaultLastStateConnectionStateListener());
    }

    /**
     * Creates a {@link FileSystemStateStorageHelper} instance.
     *
     * @param configuration {@link Configuration} object
     * @param prefix Prefix for the created files
     * @param <T> Type of the state objects
     * @return {@link FileSystemStateStorageHelper} instance
     * @throws IOException if file system state storage cannot be created
     */
    public static <T extends Serializable>
            FileSystemStateStorageHelper<T> createFileSystemStateStorage(
                    Configuration configuration, String prefix) throws IOException {

        return new FileSystemStateStorageHelper<>(
                HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration),
                prefix);
    }

    /** Creates a ZooKeeper path of the form "/a/b/.../z". */
    public static String generateZookeeperPath(String... paths) {
        return Arrays.stream(paths)
                .map(ZooKeeperUtils::trimSlashes)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.joining("/", "/", ""));
    }

    /**
     * Splits the given ZooKeeper path into its parts.
     *
     * @param path path to split
     * @return splited path
     */
    public static String[] splitZooKeeperPath(String path) {
        return path.split("/");
    }

    public static String trimStartingSlash(String path) {
        return path.startsWith("/") ? path.substring(1) : path;
    }

    private static String trimSlashes(String input) {
        int left = 0;
        int right = input.length() - 1;

        while (left <= right && input.charAt(left) == '/') {
            left++;
        }

        while (right >= left && input.charAt(right) == '/') {
            right--;
        }

        if (left <= right) {
            return input.substring(left, right + 1);
        } else {
            return "";
        }
    }

    /**
     * Returns a facade of the client that uses the specified namespace, and ensures that all nodes
     * in the path exist.
     *
     * @param client ZK client
     * @param path the new namespace
     * @return ZK Client that uses the new namespace
     * @throws Exception ZK errors
     */
    public static CuratorFramework useNamespaceAndEnsurePath(
            final CuratorFramework client, final String path) throws Exception {
        checkNotNull(client, "client must not be null");
        checkNotNull(path, "path must not be null");

        // Ensure that the checkpoints path exists
        client.newNamespaceAwareEnsurePath(path).ensure(client.getZookeeperClient());

        // All operations will have the path as root
        final String newNamespace = generateZookeeperPath(client.getNamespace(), path);
        return client.usingNamespace(
                // Curator prepends a '/' manually and throws an Exception if the
                // namespace starts with a '/'.
                trimStartingSlash(newNamespace));
    }

    /**
     * Creates a {@link TreeCache} that only observes a specific node.
     *
     * @param client ZK client
     * @param pathToNode full path of the node to observe
     * @param nodeChangeCallback callback to run if the node has changed
     * @return tree cache
     */
    public static TreeCache createTreeCache(
            final CuratorFramework client,
            final String pathToNode,
            final RunnableWithException nodeChangeCallback) {
        final TreeCache cache =
                createTreeCache(
                        client, pathToNode, ZooKeeperUtils.treeCacheSelectorForPath(pathToNode));

        cache.getListenable().addListener(createTreeCacheListener(nodeChangeCallback));

        return cache;
    }

    public static TreeCache createTreeCache(
            final CuratorFramework client,
            final String pathToNode,
            final TreeCacheSelector selector) {
        return TreeCache.newBuilder(client, pathToNode)
                .setCacheData(true)
                .setCreateParentNodes(false)
                .setSelector(selector)
                // see FLINK-32204 for further details on why the task rejection shouldn't
                // be enforced here
                .setExecutor(Executors.newDirectExecutorServiceWithNoOpShutdown())
                .build();
    }

    @VisibleForTesting
    static TreeCacheListener createTreeCacheListener(RunnableWithException nodeChangeCallback) {
        return (ignored, event) -> {
            // only notify listener if nodes have changed
            // connection issues are handled separately from the cache
            switch (event.getType()) {
                case NODE_ADDED:
                case NODE_UPDATED:
                case NODE_REMOVED:
                    nodeChangeCallback.run();
            }
        };
    }

    /**
     * Returns a {@link TreeCacheSelector} that only accepts a specific node.
     *
     * @param fullPath node to accept
     * @return tree cache selector
     */
    private static TreeCacheSelector treeCacheSelectorForPath(String fullPath) {
        return new TreeCacheSelector() {
            @Override
            public boolean traverseChildren(String childPath) {
                return false;
            }

            @Override
            public boolean acceptChild(String childPath) {
                return fullPath.equals(childPath);
            }
        };
    }

    /** Secure {@link ACLProvider} implementation. */
    public static class SecureAclProvider implements ACLProvider {
        @Override
        public List<ACL> getDefaultAcl() {
            return ZooDefs.Ids.CREATOR_ALL_ACL;
        }

        @Override
        public List<ACL> getAclForPath(String path) {
            return ZooDefs.Ids.CREATOR_ALL_ACL;
        }
    }

    /** ZooKeeper client ACL mode enum. */
    public enum ZkClientACLMode {
        CREATOR,
        OPEN;

        /**
         * Return the configured {@link ZkClientACLMode}.
         *
         * @param config The config to parse
         * @return Configured ACL mode or the default defined by {@link
         *     HighAvailabilityOptions#ZOOKEEPER_CLIENT_ACL} if not configured.
         */
        public static ZkClientACLMode fromConfig(Configuration config) {
            String aclMode = config.getString(HighAvailabilityOptions.ZOOKEEPER_CLIENT_ACL);
            if (aclMode == null || aclMode.equalsIgnoreCase(OPEN.name())) {
                return OPEN;
            } else if (aclMode.equalsIgnoreCase(CREATOR.name())) {
                return CREATOR;
            } else {
                String message = "Unsupported ACL option: [" + aclMode + "] provided";
                LOG.error(message);
                throw new IllegalConfigurationException(message);
            }
        }
    }

    public static void deleteZNode(CuratorFramework curatorFramework, String path)
            throws Exception {
        curatorFramework.delete().idempotent().deletingChildrenIfNeeded().forPath(path);
    }

    /** Private constructor to prevent instantiation. */
    private ZooKeeperUtils() {
        throw new RuntimeException();
    }
}
